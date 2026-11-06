"""Run one independently checkpointed five-case Bank chain for a frozen seed."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import threading
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.bank_benchmark import (
    LocalLlamaClient,
    load_ground_truth,
    load_pinned_inference_config,
    run_benchmark,
)

ROOT = Path(__file__).resolve().parents[1]
SEEDS = (42, 43, 44)
SHARDS = (0, 1)
CONFIG_PATH = ROOT / "config/gemma_m3_inference.json"
SAMPLE_PATH = ROOT / "config/bank_sample.json"
GT_PATH = ROOT / "data/prepared/bank/evaluator/scored_ground_truth.jsonl"
CASES_DIR = ROOT / "data/prepared/bank/public/cases"


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def _case_ids(shard: int) -> list[str]:
    sample = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    ids = list(sample.get("selection", {}).get("scored_ids", []))
    rows = [row["incident_id"] for row in sample["scored_cases"]]
    if ids != rows:
        raise ValueError("selection.scored_ids and scored_cases order differ in bank_sample.json")
    if len(ids) != 10 or len(set(ids)) != 10:
        raise ValueError("config/bank_sample.json must define exactly ten unique scored Bank IDs")
    return ids[shard * 5:(shard + 1) * 5]


def run_chain(
    *, seed: int, shard: int, run_root: Path, model_url: str,
    model_name: str | None = None, timeout: float | None = None,
    deadline_utc: str | None = None,
) -> dict[str, Any]:
    if seed not in SEEDS:
        raise ValueError(f"seed must be one of {SEEDS}")
    if shard not in SHARDS:
        raise ValueError("shard must be 0 or 1")
    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=False)
    cfg = load_pinned_inference_config()
    ids = _case_ids(shard)
    gt = load_ground_truth(GT_PATH)
    missing = [cid for cid in ids if cid not in gt]
    if missing:
        raise ValueError(f"ground-truth file is missing requested sample IDs: {missing}")
    absent = [cid for cid in ids if not (CASES_DIR / cid / "alert.json").is_file()]
    if absent:
        raise FileNotFoundError(f"prepared case inputs are missing: {absent}")

    config_bytes = CONFIG_PATH.read_bytes()
    try:
        config_label = str(CONFIG_PATH.resolve().relative_to(ROOT))
    except ValueError:
        config_label = str(CONFIG_PATH.resolve())
    profile = {
        "config_path": config_label,
        "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "model_name": model_name or cfg.get("model_alias", "e4-gemma-m3"),
        "model_endpoint": model_url,
        "seed": seed,
        "temperature": float(cfg.get("temperature", 1.0)),
        "top_p": float(cfg.get("top_p", 0.95)),
        "top_k": int(cfg.get("top_k", 64)),
        "max_generation_tokens": int(cfg.get("max_generation_tokens", 8192)),
        "context_tokens": int(cfg.get("context_tokens", 32768)),
        "reasoning_mode": str(cfg.get("reasoning_mode", "on")),
        "reasoning_budget": int(cfg.get("reasoning_budget", 2048)),
        "generation_reserve_tokens": int(cfg.get("max_generation_tokens", 8192)),
        "protocol_constrained_json": True,
        "response_format": {"type": "json_object"},
        "system_message": str(cfg.get("system_message", "")),
        "timeout_seconds": float(timeout or cfg.get("request_timeout_seconds", 600)),
        "shard": shard,
        "case_ids": ids,
        "call_journal": "model_calls.jsonl",
        "operator_timeout_seconds": 120,
        "operator_memory_limit_mb": 512,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "deadline_utc": deadline_utc,
    }
    # The study fixes these sampling settings and reserves the full generation
    # budget for reasoning. Keep the seed as the only varying model parameter.
    if (profile["temperature"], profile["top_p"], profile["top_k"],
            profile["max_generation_tokens"], profile["context_tokens"]) != (1.0, 0.95, 64, 8192, 32768):
        raise ValueError("pinned inference config differs from frozen benchmark settings")
    client = LocalLlamaClient(
        base_url=model_url, model_name=profile["model_name"], seed=seed,
        temperature=profile["temperature"], top_p=profile["top_p"],
        top_k=profile["top_k"], max_tokens=profile["max_generation_tokens"],
        timeout=profile["timeout_seconds"], system_message=profile["system_message"],
        context_tokens=profile["context_tokens"], reasoning_mode=profile["reasoning_mode"],
        reasoning_budget=profile["reasoning_budget"], preflight=True,
        response_format=profile["response_format"],
        call_journal_path=run_root / profile["call_journal"],
    )
    _atomic_json(run_root / "provenance.json", profile)
    checkpoint = run_root / "checkpoint.json"
    collected: list[dict[str, Any]] = []

    def save_case(row: dict[str, Any]) -> None:
        collected.append(row)
        _atomic_json(checkpoint, {
            "status": "running", "profile": profile,
            "completed_slots": len(collected), "total_slots": len(ids),
            "results": collected, "usage": client.usage,
            "updated_utc": datetime.now(timezone.utc).isoformat(),
        })

    deadline_epoch = None
    if deadline_utc:
        deadline_epoch = datetime.fromisoformat(deadline_utc.replace("Z", "+00:00")).timestamp()
    summary = run_benchmark(CASES_DIR, GT_PATH, client, case_ids=ids,
                            max_attempts=3, profile=profile, on_result=save_case,
                            deadline_epoch=deadline_epoch,
                            operator_timeout_seconds=profile["operator_timeout_seconds"],
                            operator_memory_limit_mb=profile["operator_memory_limit_mb"])
    summary["status"] = "complete" if all(r["status"] not in {"error", "not_started"}
                                           for r in summary["results"]) else "incomplete"
    summary["profile"] = profile
    summary["finished_utc"] = datetime.now(timezone.utc).isoformat()
    _atomic_json(run_root / "result.json", summary)
    _atomic_json(checkpoint, summary)
    return summary


def aggregate_run_roots(run_roots: list[Path], *, allow_incomplete: bool = False) -> dict[str, Any]:
    """Combine the six seed/shard result files without dropping failed slots."""
    if len(run_roots) > len(SEEDS) * len(SHARDS) or (
            not allow_incomplete and len(run_roots) != len(SEEDS) * len(SHARDS)):
        raise ValueError("aggregation requires all six seed/shard run roots")
    expected_ids = _case_ids(0) + _case_ids(1)
    by_chain: dict[tuple[int, int], dict[str, Any]] = {}
    for root in run_roots:
        result_path = Path(root) / "result.json"
        if not result_path.is_file():
            raise FileNotFoundError(f"chain result is missing: {result_path}")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        profile = result.get("profile", {})
        key = (int(profile.get("seed", -1)), int(profile.get("shard", -1)))
        if key in by_chain:
            raise ValueError(f"duplicate seed/shard result: {key}")
        if key[0] not in SEEDS or key[1] not in SHARDS:
            raise ValueError(f"invalid seed/shard provenance in {result_path}")
        observed_ids = [row.get("case_id") for row in result.get("results", [])]
        shard_ids = expected_ids[key[1] * 5:(key[1] + 1) * 5]
        if observed_ids != shard_ids:
            raise ValueError(f"case order/slots do not match frozen shard for {key}")
        by_chain[key] = result
    expected_chains = {(seed, shard) for seed in SEEDS for shard in SHARDS}
    if not allow_incomplete and set(by_chain) != expected_chains:
        raise ValueError(f"missing chains: {sorted(expected_chains - set(by_chain))}")

    per_seed: dict[str, Any] = {}
    endpoint_totals: dict[str, dict[str, int]] = {}
    all_complete = True
    for seed in SEEDS:
        seed_rows: list[dict[str, Any]] = []
        missing_shards = [shard for shard in SHARDS if (seed, shard) not in by_chain]
        usage_sums = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        usage_complete = not missing_shards
        for shard in SHARDS:
            if (seed, shard) not in by_chain:
                continue
            chain = by_chain[(seed, shard)]
            seed_rows.extend(chain["results"])
            chain_usage = chain.get("usage", {})
            for key in usage_sums:
                usage_sums[key] += int(chain_usage.get(key, 0) or 0)
            usage_complete = usage_complete and bool(chain_usage.get("usage_complete", True))
            endpoint = str(chain.get("profile", {}).get("model_endpoint", "unknown"))
            counters = endpoint_totals.setdefault(endpoint,
                {"confirmed": 0, "slots": 0, "completed": 0, "errors": 0})
            for row in chain["results"]:
                counters["slots"] += 1
                if row.get("status") == "confirmed":
                    counters["confirmed"] += 1
                if row.get("status") in {"error", "not_started"}:
                    counters["errors"] += 1
                else:
                    counters["completed"] += 1
        confirmed = sum(row.get("status") == "confirmed" for row in seed_rows)
        errors = sum(row.get("status") in {"error", "not_started"} for row in seed_rows)
        complete = len(seed_rows) == 10 and errors == 0 and not missing_shards
        all_complete = all_complete and complete
        confirmed_stage_counts = {"select": 0, "compose": 0, "serve": 0}
        terminal_stage_counts = {"select": 0, "compose": 0, "evolve": 0,
                                 "serve": 0, "error": 0, "not_started": 0}
        for row in seed_rows:
            stage = row.get("stage") or (row.get("partial_diagnosis") or {}).get("stage")
            if stage in terminal_stage_counts:
                terminal_stage_counts[stage] += 1
            if row.get("status") == "confirmed" and stage in confirmed_stage_counts:
                confirmed_stage_counts[stage] += 1
            if row.get("status") == "error":
                terminal_stage_counts["error"] += 1
            elif row.get("status") == "not_started":
                terminal_stage_counts["not_started"] += 1
        prefix_successes = {
            "select": confirmed_stage_counts["select"],
            "compose": confirmed_stage_counts["select"] + confirmed_stage_counts["compose"],
            "serve": sum(confirmed_stage_counts.values()),
        }
        completed_cases = len(seed_rows) - errors
        per_seed[str(seed)] = {"status": "complete" if complete else "incomplete",
            "planned_cases": 10, "completed_cases": completed_cases,
            "reported_slots": len(seed_rows), "missing_shards": missing_shards,
            "error_or_not_started_slots": errors, "hits": confirmed,
            "accuracy_over_planned": confirmed / 10,
            "confirmed_stage_breakdown": confirmed_stage_counts,
            "stage_prefix_successes": prefix_successes,
            "terminal_stage_counts": terminal_stage_counts,
            "usage_sum": usage_sums,
            "usage_complete": usage_complete,
            "mean_reported_tokens_per_planned_case": usage_sums["total_tokens"] / 10,
            "mean_reported_tokens_per_completed_case": (
                usage_sums["total_tokens"] / completed_cases if completed_cases else None),
            "results": seed_rows}
    for counters in endpoint_totals.values():
        counters["accuracy_over_planned"] = counters["confirmed"] / counters["slots"] if counters["slots"] else 0.0
    return {"status": "complete" if all_complete else "incomplete",
        "seeds": list(SEEDS), "planned_cases_per_seed": 10,
        "per_seed": per_seed, "per_endpoint": endpoint_totals}


def run_campaign(*, campaign_root: Path, local_url: str, wingtip_url: str,
                 deadline_utc: str | None = None) -> dict[str, Any]:
    """Run six isolated seed/shard chains using one sequential worker per endpoint."""
    campaign_root = campaign_root.resolve()
    campaign_root.mkdir(parents=True, exist_ok=False)
    assignments = {
        "local": [(42, 0), (43, 1), (44, 0)],
        "wingtip": [(42, 1), (43, 0), (44, 1)],
    }
    endpoints = {"local": local_url, "wingtip": wingtip_url}
    chains: dict[str, dict[str, Any]] = {}
    for endpoint, items in assignments.items():
        for seed, shard in items:
            chain_id = f"{endpoint}-seed{seed}-shard{shard}"
            chain_root = campaign_root / "chains" / endpoint / f"seed{seed}_shard{shard}"
            chains[chain_id] = {"endpoint": endpoint, "seed": seed, "shard": shard,
                "run_root": str(chain_root), "status": "queued"}
    manifest: dict[str, Any] = {
        "status": "running", "started_utc": datetime.now(timezone.utc).isoformat(),
        "deadline_utc": deadline_utc, "local_url": local_url, "wingtip_url": wingtip_url,
        "chain_design": "Each chain starts from frozen Bank starter playbooks; no cross-chain handoff.",
        "assignments": assignments, "chains": chains,
    }
    manifest_path = campaign_root / "campaign_manifest.json"
    lock = threading.Lock()

    def save_manifest(mutate: Any = None) -> None:
        with lock:
            if mutate is not None:
                mutate()
            manifest["updated_utc"] = datetime.now(timezone.utc).isoformat()
            _atomic_json(manifest_path, manifest)

    save_manifest()

    def endpoint_worker(endpoint: str, items: list[tuple[int, int]]) -> None:
        stop_endpoint = False
        for seed, shard in items:
            chain_id = f"{endpoint}-seed{seed}-shard{shard}"
            chain = chains[chain_id]
            if stop_endpoint:
                save_manifest(lambda: chain.update({"status": "not_started_backend_outage",
                    "error": "A prior chain reported this endpoint offline."}))
                continue
            save_manifest(lambda: chain.update({"status": "running",
                "started_utc": datetime.now(timezone.utc).isoformat()}))
            try:
                summary = run_chain(seed=seed, shard=shard,
                    run_root=Path(chain["run_root"]), model_url=endpoints[endpoint],
                    deadline_utc=deadline_utc)
                has_outage = any(row.get("error_category") == "backend_outage"
                    for row in summary.get("results", []))
                update = {"status": summary.get("status", "incomplete"),
                    "result_path": str(Path(chain["run_root"]) / "result.json"),
                    "finished_utc": datetime.now(timezone.utc).isoformat(),
                    "backend_outage": has_outage}
                if has_outage:
                    update["error"] = "Model endpoint outage stopped this chain."
                    stop_endpoint = True
                save_manifest(lambda: chain.update(update))
            except Exception as exc:
                checkpoint = Path(chain["run_root"]) / "checkpoint.json"
                update = {"status": "crashed", "error_type": type(exc).__name__,
                    "error": str(exc), "finished_utc": datetime.now(timezone.utc).isoformat()}
                if checkpoint.is_file():
                    update["checkpoint_path"] = str(checkpoint)
                result_path = Path(chain["run_root"]) / "result.json"
                if result_path.is_file():
                    update["result_path"] = str(result_path)
                save_manifest(lambda: chain.update(update))

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="bank-endpoint") as pool:
        futures = [pool.submit(endpoint_worker, endpoint, items)
                   for endpoint, items in assignments.items()]
        for future in futures:
            future.result()

    result_roots = [Path(chain["run_root"]) for chain in chains.values()
                    if (Path(chain["run_root"]) / "result.json").is_file()]
    errors = [{"chain_id": chain_id, "status": chain["status"],
               "error": chain.get("error"), "checkpoint_path": chain.get("checkpoint_path")}
              for chain_id, chain in chains.items() if chain["status"] != "complete"]
    try:
        combined = aggregate_run_roots(result_roots, allow_incomplete=True)
    except Exception as exc:
        combined = {"status": "incomplete", "aggregation_error": str(exc)}
        errors.append({"chain_id": "aggregation", "status": "error", "error": str(exc)})
    combined.update({"campaign_status": "incomplete" if errors else "complete",
                     "campaign_errors": errors,
                     "campaign_manifest": str(manifest_path)})
    if errors:
        combined["status"] = "incomplete"
    combined_path = campaign_root / "combined_result.json"
    _atomic_json(combined_path, combined)
    save_manifest(lambda: manifest.update({"status": "incomplete" if errors else "complete",
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "combined_result_path": str(combined_path), "errors": errors}))
    return combined


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-root", type=Path,
                        help="Run all six fixed seed/shard chains with two endpoint workers")
    parser.add_argument("--local-url", default="http://127.0.0.1:19250")
    parser.add_argument("--wingtip-url", default=None)
    parser.add_argument("--seed", type=int, choices=SEEDS)
    parser.add_argument("--shard", type=int, choices=SHARDS,
                        help="0 or 1; each shard has a fresh independent five-case library chain")
    parser.add_argument("--run-root", type=Path,
                        help="New output directory for this seed/shard; must not already exist")
    parser.add_argument("--model-url", default="http://127.0.0.1:19250",
                        help="OpenAI-compatible llama-server base URL, including SSH-forwarded endpoints")
    parser.add_argument("--model-name", default=None)
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument("--deadline-utc", default=None,
                        help="Optional global ISO-8601 UTC deadline; case boundaries stop cleanly at expiry")
    args = parser.parse_args()
    if args.campaign_root is not None:
        if not args.wingtip_url:
            parser.error("--campaign-root requires --wingtip-url")
        summary = run_campaign(campaign_root=args.campaign_root,
            local_url=args.local_url, wingtip_url=args.wingtip_url,
            deadline_utc=args.deadline_utc)
        print(json.dumps({"status": summary["status"],
            "campaign_status": summary["campaign_status"],
            "campaign_root": str(args.campaign_root),
            "error_count": len(summary.get("campaign_errors", []))}, sort_keys=True))
        return
    if args.seed is None or args.shard is None or args.run_root is None:
        parser.error("single-chain mode requires --seed, --shard, and --run-root")
    summary = run_chain(seed=args.seed, shard=args.shard, run_root=args.run_root,
                        model_url=args.model_url, model_name=args.model_name,
                        timeout=args.timeout, deadline_utc=args.deadline_utc)
    # Never write prompts, model text, or reasoning to terminal output.
    print(json.dumps({"status": summary["status"], "seed": args.seed,
                      "shard": args.shard, "run_root": str(args.run_root),
                      "completed_slots": len(summary["results"]),
                      "accuracy": summary.get("accuracy")}, sort_keys=True))


if __name__ == "__main__":
    main()
