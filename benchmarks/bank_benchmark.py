"""Clean OpenRCA Bank benchmark evaluation runner using DiagnosisEngine and GroundTruthOracle."""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from adapters.bank_adapter import BankAdapter
from adapters.bank_library import build_bank_registry, get_bank_seed_playbook
from e4.controller.library import LibraryManager, Playbook
from e4.engine import DiagnosisEngine
from e4.oracle import GroundTruthOracle


class LocalLlamaClient:
    """HTTP client querying local llama-server chat endpoint."""

    def __init__(self, base_url: str = "http://127.0.0.1:19250", model_name: str = "e4-gemma-m3", timeout: float = 300.0):
        self.base_url = base_url.rstrip("/")
        self.model_name = model_name
        self.timeout = timeout

    def complete(self, prompt: str) -> str:
        req = urllib.request.Request(
            f"{self.base_url}/v1/chat/completions",
            data=json.dumps({
                "model": self.model_name,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
                "max_tokens": 4096,
            }).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data["choices"][0]["message"].get("content", "")
        except Exception as exc:
            return ""


def load_ground_truth(gt_path: Path) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    if not gt_path.is_file():
        return mapping
    for line in gt_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        cid = row["incident_id"]
        targets = [item["component"] for item in row.get("ground_truth", [])]
        mapping[cid] = targets
    return mapping


def run_benchmark(
    cases_dir: Path,
    ground_truth_path: Path,
    model_client: Any,
    case_ids: list[str] | None = None,
    max_attempts: int = 3,
) -> dict[str, Any]:
    gt_map = load_ground_truth(ground_truth_path)
    oracle = GroundTruthOracle(labels_by_case=gt_map, top_k=3, max_attempts=max_attempts)

    # Initialize library with Bank baseline
    reg = build_bank_registry()
    lib = LibraryManager(registry=reg)
    seed_pb = get_bank_seed_playbook()
    lib.add_playbook(
        Playbook(
            name=seed_pb["name"],
            description=seed_pb["description"],
            dag=seed_pb["dag"],
        )
    )

    engine = DiagnosisEngine(
        model=model_client,
        oracle=oracle,
        library=lib,
        max_attempts=max_attempts,
    )

    all_case_dirs = sorted([d for d in cases_dir.iterdir() if d.is_dir() and (d / "alert.json").is_file()])
    if case_ids:
        all_case_dirs = [d for d in all_case_dirs if d.name in case_ids]

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
    parser.add_argument("--model-type", choices=["llama", "dummy"], default="llama", help="Model client type")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of cases to evaluate")
    args = parser.parse_args()

    gt_file = Path(f"data/prepared/bank/evaluator/{args.split}_ground_truth.jsonl")
    cases_root = Path(args.cases_dir)

    print(f"Evaluating {args.split} incidents from {cases_root}...")
    if args.model_type == "llama":
        client: Any = LocalLlamaClient(base_url=args.model_url)
    else:
        class DummyModel:
            def complete(self, prompt: str) -> str:
                return json.dumps({
                    "selected_playbook": "bank.starter.metric_consensus",
                    "parameter_bindings": {},
                })
        client = DummyModel()

    gt_map = load_ground_truth(gt_file)
    case_ids = list(gt_map.keys())
    if args.limit:
        case_ids = case_ids[:args.limit]

    summary = run_benchmark(cases_root, gt_file, client, case_ids=case_ids)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
