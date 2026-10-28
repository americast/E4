"""Local inference smoke and throughput harness; uses only the Python standard library."""

from __future__ import annotations

import argparse
import json
import os
import resource
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from e4.paths import ROOT

CONFIG = ROOT / "config/models/smoke.json"
SYSTEM = "Return only the requested result. Do not include markdown fences."
JSON_PROMPT = ('Return one JSON object with keys "status" and "count". Use values '
               '"ok" and 3, respectively.')
OPERATOR_PROMPT = (
    "Write a Python function named operator(data) that returns the sum of the integer values "
    "in data['values']. Return only Python source, no fences."
)


def read_config() -> dict[str, Any]:
    cfg = json.loads(CONFIG.read_text())
    if os.environ.get("E4_BASE_URL"):
        cfg["base_url"] = os.environ["E4_BASE_URL"]
    return cfg


def request_json(url: str, payload: dict[str, Any] | None = None, timeout: int = 10) -> Any:
    body = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def chat(cfg: dict[str, Any], prompt: str, max_tokens: int, timeout: int) -> tuple[str, str, dict, float, dict]:
    payload = {
        "model": cfg["model_alias"],
        "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt}],
        "temperature": cfg["temperature"], "top_p": cfg["top_p"], "top_k": cfg["top_k"],
        "max_tokens": max_tokens, "seed": cfg["seed"],
        "chat_template_kwargs": {"enable_thinking": True},
    }
    start = time.perf_counter()
    result = request_json(cfg["base_url"].rstrip("/") + "/v1/chat/completions",
                          payload, timeout=timeout)
    elapsed = time.perf_counter() - start
    choice = result["choices"][0]
    message = choice.get("message", {})
    # llama.cpp versions differ in how they expose reasoning content. Preserve both fields.
    content = message.get("content") or ""
    reasoning = message.get("reasoning_content") or message.get("reasoning") or ""
    metadata = {"finish_reason": choice.get("finish_reason"),
                "timings": result.get("timings"), "id": result.get("id")}
    return content, reasoning, result.get("usage", {}), elapsed, metadata


def extract_python(text: str) -> str:
    match = re.search(r"```(?:python)?\s*\n(.*?)```", text, flags=re.I | re.S)
    source = match.group(1) if match else text
    source = source.strip()
    if not source:
        raise ValueError("model returned empty Python source")
    return source + "\n"


def parse_json_answer(text: str, finish_reason: str | None) -> dict[str, Any]:
    if finish_reason in {"length", "max_tokens"}:
        raise ValueError("model response was truncated by the generation limit")
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("model JSON response must be an object")
    return parsed


def _limit_operator_child() -> None:
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_CPU, (3, 3))
    resource.setrlimit(resource.RLIMIT_FSIZE, (1024 * 1024, 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))


def validate_and_run_operator(source: str, data: dict[str, Any], timeout: int = 5) -> Any:
    """Run a tiny smoke operator in a bounded child process with a clean environment."""
    import ast
    tree = ast.parse(source)
    funcs = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    if len(funcs) != 1 or funcs[0].name != "operator" or isinstance(funcs[0], ast.AsyncFunctionDef):
        raise ValueError("expected exactly one synchronous operator(data) function")
    # This check is a smoke-task guard, not a security sandbox for production-generated code.
    allowed_nodes = (ast.Module, ast.FunctionDef, ast.arguments, ast.arg, ast.Return, ast.Expr,
                     ast.Call, ast.Name, ast.Load, ast.Store, ast.Subscript, ast.Constant,
                     ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.List, ast.Tuple,
                     ast.Dict, ast.UnaryOp, ast.USub)
    for node in ast.walk(tree):
        if not isinstance(node, allowed_nodes):
            raise ValueError(f"operator contains disallowed syntax: {type(node).__name__}")
        if isinstance(node, ast.Call) and not (
            isinstance(node.func, ast.Name) and node.func.id == "sum"
        ):
            raise ValueError("operator may call only the sum builtin")
        if isinstance(node, ast.Name) and node.id not in {"operator", "data", "sum"}:
            raise ValueError(f"operator uses an unapproved name: {node.id}")
    harness = (
        "import json,sys\n"
        + source +
        "\ndata=json.load(sys.stdin)\n"
        "result=operator(data)\n"
        "print(json.dumps(result))\n"
    )
    env = {"PATH": os.defpath, "PYTHONIOENCODING": "utf-8"}
    completed = subprocess.run([sys.executable, "-I", "-c", harness], input=json.dumps(data),
                               text=True, capture_output=True, timeout=timeout, env=env,
                               cwd=ROOT, preexec_fn=_limit_operator_child)
    if completed.returncode:
        raise RuntimeError(f"operator subprocess failed: {completed.stderr[-1000:]}")
    return json.loads(completed.stdout)


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


class GpuSampler:
    """Sample device-wide used/free memory while inference calls are running."""

    def __init__(self) -> None:
        self.samples: list[dict[str, int]] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            try:
                result = subprocess.run(
                    ["nvidia-smi", "--query-gpu=index,memory.used,memory.free,utilization.gpu",
                     "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=3
                )
                for line in result.stdout.splitlines():
                    fields = [part.strip() for part in line.split(",")]
                    if len(fields) == 4:
                        self.samples.append(dict(gpu_index=int(fields[0]), used_mib=int(fields[1]),
                                                 free_mib=int(fields[2]), utilization_pct=int(fields[3])))
            except Exception:
                pass
            self.stop_event.wait(0.5)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        self.thread.join(timeout=4)
        if not self.samples:
            return {"sample_count": 0, "peak_used_mib": None, "minimum_free_mib": None}
        return {"sample_count": len(self.samples),
                "peak_used_mib": max(s["used_mib"] for s in self.samples),
                "minimum_free_mib": min(s["free_mib"] for s in self.samples),
                "samples": self.samples}


def run(artifact: Path, timeout: int) -> int:
    cfg = read_config()
    manifest = json.loads((ROOT / "config/models/gemma_manifest.json").read_text())
    started = datetime.now(timezone.utc).isoformat()
    try:
        health = request_json(cfg["base_url"].rstrip("/") + "/health", timeout=10)
    except Exception as exc:
        print(f"llama-server unavailable at {cfg['base_url']}: {exc}", file=sys.stderr)
        return 2
    base = {"started_utc": started, "model_alias": cfg["model_alias"],
            "model_path": cfg["model_path"], "context_tokens": cfg["context_tokens"],
            "model_repository": manifest.get("repository"),
            "model_revision": manifest.get("revision"), "model_sha256": manifest.get("sha256"),
            "server_health": health}
    sampler = GpuSampler()
    sampler.start()
    failed = False
    for label, prompt, limit in (("json", JSON_PROMPT, cfg["max_generation_tokens"]),
                                 ("operator", OPERATOR_PROMPT, cfg["max_generation_tokens"])):
        record = dict(base, smoke_case=label)
        try:
            text, reasoning, usage, elapsed, metadata = chat(cfg, prompt, limit, timeout)
            record.update({"ok": True, "latency_seconds": elapsed, "usage": usage,
                           "response": text, "reasoning": reasoning, "response_meta": metadata})
            if label == "json":
                parsed = parse_json_answer(text, metadata["finish_reason"])
                record["parsed_json"] = parsed
                record["assertion"] = parsed.get("status") == "ok" and parsed.get("count") == 3
                if not record["assertion"]:
                    raise ValueError("JSON response did not match requested values")
            else:
                source = extract_python(text)
                checks = []
                for input_data, expected in (({"values": [2, 3, 5]}, 10),
                                             ({"values": [-4, 7]}, 3),
                                             ({"values": []}, 0),
                                             ({"values": [11]}, 11)):
                    value = validate_and_run_operator(source, input_data)
                    checks.append({"input": input_data, "expected": expected, "actual": value,
                                   "passed": value == expected})
                record.update({"operator_source": source, "operator_checks": checks,
                               "assertion": all(item["passed"] for item in checks)})
                if not record["assertion"]:
                    raise ValueError("generated operator failed one or more independent inputs")
            output_tokens = usage.get("completion_tokens") or usage.get("output_tokens")
            record["output_tokens_per_second"] = (
                output_tokens / elapsed if isinstance(output_tokens, (int, float)) and elapsed > 0 else None
            )
        except Exception as exc:
            failed = True
            record.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        append_jsonl(artifact, record)
        print(json.dumps({k: record.get(k) for k in
                          ("smoke_case", "ok", "latency_seconds", "usage", "output_tokens_per_second", "error")}))
    gpu = sampler.stop()
    append_jsonl(artifact, dict(base, smoke_case="run_summary", gpu_memory=gpu,
                                ok=not failed, finished_utc=datetime.now(timezone.utc).isoformat()))
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path,
                        default=ROOT / "artifacts/smoke/smoke.jsonl")
    parser.add_argument("--timeout", type=int, default=None,
                        help="per-request timeout in seconds (defaults to config)")
    args = parser.parse_args()
    cfg = read_config()
    return run(args.artifact, args.timeout or cfg["request_timeout_seconds"])


if __name__ == "__main__":
    raise SystemExit(main())
