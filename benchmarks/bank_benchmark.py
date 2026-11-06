"""Clean OpenRCA Bank benchmark evaluation runner using DiagnosisEngine and GroundTruthOracle."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from adapters.bank_adapter import BankAdapter
from adapters.bank_library import (
    build_bank_registry,
    get_bank_contrast_playbook,
    get_bank_seed_playbook,
)
from e4.controller.library import LibraryManager, Playbook
from e4.engine import DiagnosisEngine
from e4.oracle import GroundTruthOracle


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
    ):
        self.base_url = base_url.rstrip("/")
        self.model_name = model_name
        self.seed = seed
        self.temperature = temperature
        self.top_p = top_p
        self.top_k = top_k
        self.max_tokens = max_tokens
        self.timeout = timeout
        self.total_requests = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.finish_reasons: list[str] = []

    @property
    def usage(self) -> dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "prompt_tokens": self.total_prompt_tokens,
            "completion_tokens": self.total_completion_tokens,
            "total_tokens": self.total_prompt_tokens + self.total_completion_tokens,
            "finish_reasons": list(self.finish_reasons),
        }

    def complete(self, prompt: str) -> str:
        self.total_requests += 1
        req_body = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": self.temperature,
            "top_p": self.top_p,
            "top_k": self.top_k,
            "seed": self.seed,
            "max_tokens": self.max_tokens,
        }
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
                return content
        except Exception as exc:
            raise RuntimeError(f"LocalLlamaClient request failed: {exc}") from exc


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
) -> dict[str, Any]:
    gt_map = load_ground_truth(ground_truth_path)
    oracle = GroundTruthOracle(labels_by_case=gt_map, top_k=3, max_attempts=max_attempts)

    # Initialize library with Bank baseline playbooks
    reg = build_bank_registry()
    lib = LibraryManager(registry=reg)
    seed_pb = get_bank_seed_playbook()
    contrast_pb = get_bank_contrast_playbook()
    lib.add_playbook(
        Playbook(
            name=seed_pb["name"],
            description=seed_pb["description"],
            dag=seed_pb["dag"],
        )
    )
    lib.add_playbook(
        Playbook(
            name=contrast_pb["name"],
            description=contrast_pb["description"],
            dag=contrast_pb["dag"],
        )
    )

    engine = DiagnosisEngine(
        model=model_client,
        oracle=oracle,
        library=lib,
        max_attempts=max_attempts,
    )

    all_case_dirs = sorted([d for d in cases_dir.iterdir() if d.is_dir() and (d / "alert.json").is_file()])
    if case_ids is not None:
        if not case_ids:
            raise ValueError("Requested case_ids list is empty")
        all_case_dirs = [d for d in all_case_dirs if d.name in case_ids]

    if not all_case_dirs:
        raise ValueError(f"No valid case directories found in {cases_dir}")

    results: list[dict[str, Any]] = []
    hits = 0
    stage_counts = {"select": 0, "compose": 0, "serve": 0}

    for case_dir in all_case_dirs:
        cid = case_dir.name
        adapter = BankAdapter(case_dir)
        diag = engine.diagnose(adapter, case_id=cid)
        is_hit = diag.status == "confirmed"
        if is_hit:
            hits += 1
            if diag.stage in stage_counts:
                stage_counts[diag.stage] += 1

        results.append({
            "case_id": cid,
            "status": diag.status,
            "stage": diag.stage,
            "confirmed": is_hit,
            "top_candidates": [c.as_dict() for c in diag.top_candidates],
            "total_calls": diag.total_calls,
        })

    total = len(all_case_dirs)
    acc = (hits / total) if total > 0 else 0.0

    return {
        "profile": profile or {},
        "usage": getattr(model_client, "usage", {}),
        "total_cases": total,
        "hits": hits,
        "accuracy": acc,
        "stage_breakdown": stage_counts,
        "results": results,
    }


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
