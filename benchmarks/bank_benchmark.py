"""Clean OpenRCA Bank benchmark evaluation runner using DiagnosisEngine and GroundTruthOracle."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping

from adapters.bank_adapter import BankAdapter
from adapters.bank_library import (
    build_bank_registry,
    get_bank_contrast_playbook,
    get_bank_seed_playbook,
)
from e4.controller.library import LibraryManager, Playbook
from e4.engine import DiagnosisEngine
from e4.oracle import GroundTruthOracle
from e4.sandbox.runner import DEFAULT_TIMEOUT_SECONDS, SubprocessRunner


def load_pinned_inference_config() -> dict[str, Any]:
    cfg_path = Path("config/gemma_m3_inference.json")
    if cfg_path.is_file():
        try:
            return json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"temperature": 1.0, "top_p": 0.95, "top_k": 64, "max_generation_tokens": 8192}


class LocalLlamaClient:
    """HTTP client querying local llama-server chat endpoint with profile tracking."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:19250",
        model_name: str = "e4-gemma-m3",
        seed: int = 42,
        temperature: float = 1.0,
        top_p: float = 0.95,
        top_k: int = 64,
        max_tokens: int = 8192,
        timeout: float = 300.0,
        system_message: str = "",
        context_tokens: int = 32768,
        reasoning_mode: str = "on",
        reasoning_budget: int = 2048,
        preflight: bool = True,
        response_format: Mapping[str, str] | None = None,
        call_journal_path: Path | str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.model_name = model_name
        self.seed = seed
        self.temperature = temperature
        self.top_p = top_p
        self.top_k = top_k
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.system_message = system_message
        self.context_tokens = context_tokens
        self.reasoning_mode = reasoning_mode
        self.reasoning_budget = reasoning_budget
        self.preflight = preflight
        self.response_format = dict(response_format or {"type": "json_object"})
        self.call_journal_path = Path(call_journal_path) if call_journal_path else None
        self.total_requests = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.usage_complete = True
        self.finish_reasons: list[str] = []
        self.calls: list[dict[str, Any]] = []
        # Filled by resumable orchestration before each exact request; this is
        # durable in the same fsynced call record so replay stays request-scoped.
        self.request_context: dict[str, Any] = {}

    @property
    def usage(self) -> dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "prompt_tokens": self.total_prompt_tokens,
            "completion_tokens": self.total_completion_tokens,
            "total_tokens": self.total_prompt_tokens + self.total_completion_tokens,
            "usage_complete": self.usage_complete,
            "finish_reasons": list(self.finish_reasons),
            "protocol_constrained_json": self.response_format == {"type": "json_object"},
        }

    def complete(self, prompt: str) -> str:
        started = time.time()
        messages = ([{"role": "system", "content": self.system_message}]
                    if self.system_message else [])
        messages.append({"role": "user", "content": prompt})
        token_preflight: dict[str, Any] = {"status": "not_requested"}
        if self.preflight:
            try:
                rendered = self._post("/apply-template", {"messages": messages,
                    "add_generation_prompt": True,
                    "chat_template_kwargs": {"enable_thinking": self.reasoning_mode.lower()
                        in {"on", "enabled", "true"}}})
                rendered_prompt = rendered.get("prompt") if isinstance(rendered, Mapping) else None
                if not isinstance(rendered_prompt, str):
                    raise ValueError("/apply-template response has no rendered prompt")
                tokenized = self._post("/tokenize", {"content": rendered_prompt,
                    "add_special": True, "parse_special": True})
                tokens = tokenized.get("tokens") if isinstance(tokenized, Mapping) else None
                if not isinstance(tokens, list):
                    raise ValueError("/tokenize response has no token list")
                token_preflight = {"status": "ok", "rendered_prompt_tokens": len(tokens),
                                   "context_tokens": self.context_tokens,
                                   "requested_generation_tokens": self.max_tokens,
                                   "reasoning_budget": self.reasoning_budget}
                if len(tokens) + self.max_tokens > self.context_tokens:
                    raise ValueError("rendered prompt plus maximum generation exceeds context")
            except Exception as exc:
                self._record_call({"status": "preflight_error", "prompt": prompt,
                                   "error_type": type(exc).__name__, "error": str(exc),
                                   "token_preflight": token_preflight,
                                   "elapsed_seconds": time.time() - started})
                raise RuntimeError(f"LocalLlamaClient token preflight failed: {exc}") from exc
        req_body = {
            "model": self.model_name,
            "messages": messages,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "top_k": self.top_k,
            "seed": self.seed,
            "max_tokens": self.max_tokens,
            "response_format": self.response_format,
            "chat_template_kwargs": {"enable_thinking": self.reasoning_mode.lower() in {"on", "enabled", "true"}},
        }
        self.total_requests += 1
        req = urllib.request.Request(
            f"{self.base_url}/v1/chat/completions",
            data=json.dumps(req_body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                choice = data["choices"][0]
                reason = choice.get("finish_reason", "stop")
                self.finish_reasons.append(reason)
                usage_info = data.get("usage", {})
                self.total_prompt_tokens += usage_info.get("prompt_tokens", 0)
                self.total_completion_tokens += usage_info.get("completion_tokens", 0)
                content = choice["message"].get("content", "")
                self._record_call({"status": "ok", "prompt": prompt, "response": content,
                                   "finish_reason": reason, "usage": usage_info,
                                   "usage_complete": True,
                                   "token_preflight": token_preflight,
                                   "elapsed_seconds": time.time() - started})
                return content
        except Exception as exc:
            # The server may have generated text before the connection or
            # response failed, so zero reported tokens must not imply zero cost.
            self.usage_complete = False
            error_info: dict[str, Any] = {}
            if isinstance(exc, urllib.error.HTTPError):
                error_info["http_status"] = exc.code
                try:
                    error_info["http_error_body"] = exc.read().decode("utf-8", errors="replace")[:4000]
                except Exception:
                    pass
            self._record_call({"status": "request_error", "prompt": prompt,
                               "error_type": type(exc).__name__, "error": str(exc),
                               "usage_complete": False,
                               **error_info,
                               "token_preflight": token_preflight,
                               "elapsed_seconds": time.time() - started})
            raise RuntimeError(f"LocalLlamaClient request failed: {exc}") from exc

    def _post(self, path: str, payload: dict[str, Any]) -> Any:
        req = urllib.request.Request(f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _record_call(self, record: dict[str, Any]) -> None:
        """Keep call metadata in memory and durably append each completed call."""
        record = {**record, "request_context": dict(self.request_context)}
        self.calls.append(record)
        if self.call_journal_path is None:
            return
        self.call_journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.call_journal_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())


def load_ground_truth(gt_path: Path) -> dict[str, list[str]]:
    if not gt_path.is_file():
        raise FileNotFoundError(f"Ground truth file not found: {gt_path}")
    mapping: dict[str, list[str]] = {}
    for line in gt_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        cid = row["incident_id"]
        targets = [item["component"] for item in row.get("ground_truth", [])]
        mapping[cid] = targets
    if not mapping:
        raise ValueError(f"Ground truth file {gt_path} contains no valid entries")
    return mapping


def run_benchmark(
    cases_dir: Path,
    ground_truth_path: Path,
    model_client: Any,
    case_ids: list[str] | None = None,
    max_attempts: int = 3,
    profile: dict[str, Any] | None = None,
    on_result: Any = None,
    deadline_epoch: float | None = None,
    operator_timeout_seconds: float | None = None,
    operator_memory_limit_mb: int | None = None,
    library: LibraryManager | None = None,
    adapter_factory: Callable[[Path], Any] | None = None,
    resume_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    gt_map = load_ground_truth(ground_truth_path)
    oracle = GroundTruthOracle(labels_by_case=gt_map, top_k=3, max_attempts=max_attempts)

    if library is None:
        # Preserve legacy defaults for all existing callers.
        reg = build_bank_registry()
        lib = LibraryManager(registry=reg)
        seed_pb = get_bank_seed_playbook()
        contrast_pb = get_bank_contrast_playbook()
        lib.add_playbook(Playbook(name=seed_pb["name"], description=seed_pb["description"], dag=seed_pb["dag"]))
        lib.add_playbook(Playbook(name=contrast_pb["name"], description=contrast_pb["description"], dag=contrast_pb["dag"]))
    else:
        lib = library

    sandbox_runner = None
    if operator_timeout_seconds is not None or operator_memory_limit_mb is not None:
        sandbox_runner = SubprocessRunner(
            timeout_seconds=(operator_timeout_seconds if operator_timeout_seconds is not None
                             else DEFAULT_TIMEOUT_SECONDS),
            memory_limit_mb=(operator_memory_limit_mb if operator_memory_limit_mb is not None
                             else 512),
        )
    engine = DiagnosisEngine(model=model_client, oracle=oracle, library=lib, runner=sandbox_runner,
                             max_attempts=max_attempts)

    all_case_dirs = sorted([d for d in cases_dir.iterdir() if d.is_dir() and (d / "alert.json").is_file()])
    if case_ids is not None:
        if not case_ids:
            raise ValueError("Requested case_ids list is empty")
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("Requested case_ids must be unique")
        by_name = {directory.name: directory for directory in all_case_dirs}
        missing = [case_id for case_id in case_ids if case_id not in by_name]
        if missing:
            raise ValueError(f"Requested case directories are missing: {missing}")
        all_case_dirs = [by_name[case_id] for case_id in case_ids]

    if not all_case_dirs:
        raise ValueError(f"No valid case directories found in {cases_dir}")

    results: list[dict[str, Any]] = list(resume_results or [])
    processed_ids = {
        row.get("case_id") for row in results if row.get("status") != "not_started"
    }
    hits = 0
    stage_counts = {"select": 0, "compose": 0, "serve": 0}
    terminal_stage_counts = {"select": 0, "compose": 0, "evolve": 0,
                             "serve": 0, "error": 0, "not_started": 0}

    for case_index, case_dir in enumerate(all_case_dirs):
        cid = case_dir.name
        if cid in processed_ids:
            continue
        if deadline_epoch is not None and time.time() >= deadline_epoch:
            for pending in all_case_dirs[case_index:]:
                results.append({"case_id": pending.name, "status": "not_started",
                    "stage": None, "confirmed": False, "error_category": "deadline_stop",
                    "error": "Global benchmark deadline reached before this case started.",
                    "total_calls": 0, "call_metadata": [], "usage_delta": {}})
                terminal_stage_counts["not_started"] += 1
                if on_result:
                    on_result(results[-1])
            break
        before_calls = len(getattr(model_client, "calls", []))
        before_usage = dict(getattr(model_client, "usage", {}))
        try:
            # The engine/library persists only within this invocation. The
            # three-seed driver invokes this once per five-case shard.
            adapter = adapter_factory(case_dir) if adapter_factory is not None else BankAdapter(case_dir)
            diag = engine.diagnose(adapter, case_id=cid)
        except Exception as exc:
            category = classify_case_error(exc, model_client, before_calls)
            outage = category == "backend_outage"
            call_log = list(getattr(model_client, "calls", []))[before_calls:]
            usage_after = dict(getattr(model_client, "usage", {}))
            results.append({"case_id": cid, "status": "error", "stage": None,
                "confirmed": False, "error_category": category,
                "error_type": type(exc).__name__, "error": str(exc),
                "total_calls": len(call_log), "call_metadata": call_log,
                "usage_delta": usage_delta(before_usage, usage_after, call_log)})
            if on_result:
                on_result(results[-1])
            terminal_stage_counts["error"] += 1
            if outage:
                for pending in all_case_dirs[case_index + 1:]:
                    results.append({"case_id": pending.name, "status": "not_started",
                        "stage": None, "confirmed": False,
                        "error_category": "backend_outage_stop",
                        "error": "Earlier model endpoint outage stopped this independent chain.",
                        "total_calls": 0, "call_metadata": [], "usage_delta": {}})
                    if on_result:
                        on_result(results[-1])
                    terminal_stage_counts["not_started"] += 1
                break
            continue
        call_log = list(getattr(model_client, "calls", []))[before_calls:]
        usage_after = dict(getattr(model_client, "usage", {}))
        caught_outage = classify_call_error(call_log[-1]) if call_log else None
        if caught_outage == "backend_outage":
            last_call = call_log[-1]
            results.append({"case_id": cid, "status": "error", "stage": None,
                "confirmed": False, "error_category": "backend_outage",
                "error_type": last_call.get("error_type", "BackendRequestError"),
                "error": last_call.get("error", "Model backend failed during a caught stage."),
                "partial_diagnosis": {"status": diag.status, "stage": diag.stage,
                    "top_candidates": [c.as_dict() for c in diag.top_candidates],
                    "total_calls": diag.total_calls, "history": list(diag.history)},
                "total_calls": len(call_log), "call_metadata": call_log,
                "usage_delta": usage_delta(before_usage, usage_after, call_log)})
            terminal_stage_counts["error"] += 1
            if on_result:
                on_result(results[-1])
            for pending in all_case_dirs[case_index + 1:]:
                results.append({"case_id": pending.name, "status": "not_started",
                    "stage": None, "confirmed": False,
                    "error_category": "backend_outage_stop",
                    "error": "Earlier model endpoint outage stopped this independent chain.",
                    "total_calls": 0, "call_metadata": [], "usage_delta": {}})
                terminal_stage_counts["not_started"] += 1
                if on_result:
                    on_result(results[-1])
            break
        is_hit = diag.status == "confirmed"
        if is_hit:
            hits += 1
            if diag.stage in stage_counts:
                stage_counts[diag.stage] += 1
        if diag.stage in terminal_stage_counts:
            terminal_stage_counts[diag.stage] += 1
        results.append({
            "case_id": cid,
            "status": diag.status,
            "stage": diag.stage,
            "confirmed": is_hit,
            "top_candidates": [c.as_dict() for c in diag.top_candidates],
            "total_calls": diag.total_calls,
            "call_metadata": call_log,
            "usage_delta": usage_delta(before_usage, usage_after, call_log),
            "history": list(diag.history),
        })
        if on_result:
            on_result(results[-1])

    results_by_id: dict[str, dict[str, Any]] = {}
    for row in results:
        case_id = row.get("case_id")
        if case_id in results_by_id:
            raise ValueError(f"Duplicate benchmark result for case {case_id!r}")
        results_by_id[case_id] = row
    results = [results_by_id[cid] for cid in [path.name for path in all_case_dirs] if cid in results_by_id]
    total = len(all_case_dirs)
    hits = sum(row.get("status") == "confirmed" for row in results)
    stage_counts = {stage: sum(row.get("status") == "confirmed" and row.get("stage") == stage for row in results)
                    for stage in ("select", "compose", "serve")}
    terminal_stage_counts = {
        stage: sum(row.get("stage") == stage for row in results)
        for stage in ("select", "compose", "evolve", "serve")
    }
    terminal_stage_counts["error"] = sum(row.get("status") == "error" for row in results)
    terminal_stage_counts["not_started"] = sum(row.get("status") == "not_started" for row in results)
    acc = (hits / total) if total > 0 else 0.0
    stage_prefix_counts = {
        "select": stage_counts["select"],
        "compose": stage_counts["select"] + stage_counts["compose"],
        "serve": sum(stage_counts.values()),
    }

    return {
        "profile": profile or {},
        "usage": getattr(model_client, "usage", {}),
        "total_cases": total,
        "hits": hits,
        "accuracy": acc,
        "stage_breakdown": stage_counts,
        "stage_prefix_counts": stage_prefix_counts,
        "terminal_stage_breakdown": terminal_stage_counts,
        "results": results,
    }


def usage_delta(before: Mapping[str, Any], after: Mapping[str, Any],
                calls: list[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Per-case cost metadata without attempting to estimate hidden reasoning."""
    delta: dict[str, Any] = {}
    for key in ("total_requests", "prompt_tokens", "completion_tokens", "total_tokens"):
        if key in before or key in after:
            delta[key] = max(0, int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0))
    delta["usage_complete"] = not any(call.get("status") == "request_error" for call in (calls or []))
    return delta


def classify_call_error(call: Mapping[str, Any]) -> str | None:
    """Classify the most recent failed API call, including failures caught in the controller."""
    if call.get("status") not in {"request_error", "preflight_error"}:
        return None
    message = (str(call.get("error", "")) + " " +
               str(call.get("http_error_body", ""))).lower()
    if "exceeds context" in message or "context length" in message:
        return "full_context_error"
    try:
        status = int(call.get("http_status", 0) or 0)
    except (TypeError, ValueError):
        status = 0
    if status >= 500:
        return "backend_outage"
    error_type = str(call.get("error_type", ""))
    if error_type in {"TimeoutError", "Timeout", "URLError", "ConnectionError",
                      "ConnectionRefusedError", "ConnectionResetError", "BrokenPipeError",
                      "RemoteDisconnected", "socket.timeout", "OSError", "gaierror"}:
        return "backend_outage"
    return "model_request_error"


def classify_case_error(exc: BaseException, client: Any, before_calls: int) -> str:
    """Use observed client failures and exception types; never report a hit on error."""
    calls = list(getattr(client, "calls", []))[before_calls:]
    if calls and calls[-1].get("status") in {"request_error", "preflight_error"}:
        call_category = classify_call_error(calls[-1])
        if call_category == "backend_outage":
            return call_category
        if call_category == "full_context_error":
            return call_category
        chain: BaseException | None = exc
        while chain is not None:
            if isinstance(chain, urllib.error.HTTPError):
                return "backend_outage" if chain.code >= 500 else "model_request_error"
            if isinstance(chain, (TimeoutError, socket.timeout, urllib.error.URLError,
                                  ConnectionError, OSError)):
                return "backend_outage"
            chain = chain.__cause__ or chain.__context__
        return "model_request_error"
    if isinstance(exc, (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError)):
        return "typed_source_or_input_error"
    return "execution_error"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run E4 OpenRCA Bank benchmark evaluation")
    parser.add_argument("--cases-dir", default="data/prepared/bank/public/cases", help="Path to prepared cases")
    parser.add_argument("--split", choices=["scored", "dev"], default="scored", help="Benchmark split")
    parser.add_argument("--model-url", default="http://127.0.0.1:19250", help="llama-server endpoint URL")
    parser.add_argument("--model-name", default="e4-gemma-m3", help="Model name identifier")
    parser.add_argument("--model-type", choices=["llama", "dummy"], default="llama", help="Model client type")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for model sampling")
    parser.add_argument("--temperature", type=float, default=1.0, help="Sampling temperature")
    parser.add_argument("--top-p", type=float, default=0.95, help="Top-p nucleus sampling")
    parser.add_argument("--top-k", type=int, default=64, help="Top-k sampling (frozen: 64)")
    parser.add_argument("--max-tokens", type=int, default=8192, help="Maximum generated tokens")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of cases to evaluate")
    args = parser.parse_args()

    gt_file = Path(f"data/prepared/bank/evaluator/{args.split}_ground_truth.jsonl")
    cases_root = Path(args.cases_dir)

    print(f"Evaluating {args.split} incidents from {cases_root} with seed {args.seed}...")
    profile_info = {
        "seed": args.seed,
        "temperature": args.temperature,
        "top_p": args.top_p,
        "top_k": args.top_k,
        "max_tokens": args.max_tokens,
        "model_name": args.model_name,
    }
    if args.model_type == "llama":
        client: Any = LocalLlamaClient(
            base_url=args.model_url,
            model_name=args.model_name,
            seed=args.seed,
            temperature=args.temperature,
            top_p=args.top_p,
            top_k=args.top_k,
            max_tokens=args.max_tokens,
        )
    else:
        class DummyModel:
            def __init__(self) -> None:
                self.usage = {"total_requests": 0}

            def complete(self, prompt: str) -> str:
                self.usage["total_requests"] += 1
                return json.dumps({
                    "selected_playbook": "bank.starter.metric_contrast",
                    "parameter_bindings": {},
                })
        client = DummyModel()

    gt_map = load_ground_truth(gt_file)
    case_ids = list(gt_map.keys())
    if args.limit:
        case_ids = case_ids[:args.limit]

    summary = run_benchmark(
        cases_root,
        gt_file,
        client,
        case_ids=case_ids,
        profile=profile_info,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
