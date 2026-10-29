#!/usr/bin/env python3
"""Run one split-aware M5 E4 case through the isolated M4 controller."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
for import_root in (ROOT / "src", ROOT):  # e4 lives in src; scripts.* at the repo root
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from scripts.bank.diagnose import main as supervisor_main


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _finite_nonnegative(value):
    return (float(value) if type(value) in (int, float) and math.isfinite(float(value))
            and value >= 0 else None)


def _stage_rows(run_dir: Path, diagnostic: dict) -> list:
    stages = diagnostic.get("stages")
    if isinstance(stages, list):
        return stages
    names = ("stage1_select", "stage2_compose", "stage3_synthesize")
    output = []
    for name in names:
        path = run_dir / "diagnostic" / "attempts" / name / "stage_record.json"
        output.append(_read_json(path) if path.is_file() else None)
    return output


def _fallback_slots(run_dir: Path, diagnostic: dict, *, case_timeout: bool) -> list[dict]:
    stages = _stage_rows(run_dir, diagnostic)
    names = ("stage1_select", "stage2_compose", "stage3_synthesize")
    slots = []
    for index, name in enumerate(names):
        stage = stages[index] if index < len(stages) else None
        started = (run_dir / "diagnostic" / "attempts" / name).is_dir()
        if isinstance(stage, dict) and stage.get("status") in {"accepted", "rejected"}:
            slots.append({"attempt": index + 1, "status": "submitted",
                          "accepted": stage["status"] == "accepted", "valid": None,
                          "correct_rank": None})
        elif isinstance(stage, dict):
            slots.append({"attempt": index + 1, "status": "failed", "accepted": False,
                          "valid": False, "correct_rank": None,
                          "validation_error": stage.get("status")})
        elif case_timeout and started:
            slots.append({"attempt": index + 1, "status": "failed", "accepted": False,
                          "valid": False, "correct_rank": None,
                          "validation_error": "case_wall_timeout"})
        else:
            slots.append({"attempt": index + 1, "status": "not_run", "accepted": False,
                          "valid": None, "correct_rank": None})
    return slots


def _case_cost(run_dir: Path, diagnostic: dict, supervisor: dict) -> dict:
    from e4.comparison.metrics import COST_FIELDS
    cost = {field: None for field in COST_FIELDS}
    calls = diagnostic.get("model_calls", {})
    if isinstance(calls, dict):
        if type(calls.get("model_call_count")) is int:
            cost["model_calls"] = calls["model_call_count"]
        cost["prompt_tokens"] = calls.get("prompt_tokens")
        cost["completion_tokens"] = calls.get("completion_tokens")
        cost["model_seconds"] = _finite_nonnegative(calls.get("model_elapsed_seconds"))
    cost["wall_seconds"] = _finite_nonnegative(supervisor.get("supervisor_wall_seconds"))
    audit = run_dir / "diagnostic" / "audit" / "public_query_calls.jsonl"
    query_budget = diagnostic.get("query_budget", {})
    requests = query_budget.get("requests") if isinstance(query_budget, dict) else None
    if audit.is_file():
        rows = []
        try:
            rows = [json.loads(line) for line in audit.read_text(encoding="utf-8").splitlines()
                    if line.strip()]
        except (OSError, json.JSONDecodeError):
            rows = []
        good = [row for row in rows if isinstance(row, dict) and row.get("status") == "success"]
        if len(good) == len(rows):
            scanned = [_finite_nonnegative(row.get("source_bytes_scanned")) for row in good]
            returned = [_finite_nonnegative(row.get("returned_bytes")) for row in good]
            if all(value is not None for value in scanned):
                cost["logical_bytes_scanned"] = sum(scanned)
            if all(value is not None for value in returned):
                cost["returned_bytes"] = sum(returned)
    elif requests == 0:
        cost["logical_bytes_scanned"] = 0
        cost["returned_bytes"] = 0
    # The executor does not report CPU time or physical disk reads. Leave those,
    # model loading, and per-case model startup unknown rather than inferring them.
    return cost


def _write_case_result(args: argparse.Namespace, exit_code: int) -> dict | None:
    configured_root = os.environ.get("E4_RUN_DIR")
    if not configured_root:
        return None
    run_dir = Path(args.run_dir)
    if not run_dir.is_absolute():
        run_dir = Path(configured_root) / run_dir
    run_dir = run_dir.resolve()
    if not run_dir.is_dir():
        return None
    diagnostic = _read_json(run_dir / "diagnostic" / "diagnostic_result.json")
    supervisor = _read_json(run_dir / "supervisor_manifest.json")
    timeout = supervisor.get("case_wall_timeout") is True
    summary_slots = supervisor.get("attempt_slots")
    slots = (summary_slots if isinstance(summary_slots, list) and len(summary_slots) == 3
             else _fallback_slots(run_dir, diagnostic, case_timeout=timeout))
    accepted = any(isinstance(slot, dict) and slot.get("accepted") is True for slot in slots)
    diagnostic_status = diagnostic.get("status")
    terminal = (diagnostic_status in {"accepted", "exhausted_or_failed"} or
                timeout or accepted)
    classification = "diagnostic_complete" if terminal else "infrastructure_incomplete"
    if accepted:
        status = "accepted"
    elif timeout:
        status = "case_wall_timeout"
    elif diagnostic_status:
        status = diagnostic_status
    else:
        status = "infrastructure_incomplete"
    error = None
    if classification == "infrastructure_incomplete":
        error_doc = diagnostic.get("error")
        if isinstance(error_doc, dict):
            error = error_doc.get("message") or error_doc.get("code")
        if not error:
            error = ("supervisor failed before producing a diagnostic result" if exit_code
                     else "diagnostic result artifact is missing")
    library_path = run_dir / "diagnostic" / "library"
    result = {
        "schema_version": "e4-m5-case-result/1",
        "case_id": args.case_id,
        "split": args.split,
        "status": status,
        "diagnostic_status": diagnostic_status,
        "diagnostic_error": diagnostic.get("error"),
        "terminal_classification": classification,
        "infrastructure_error": error,
        "attempts": slots,
        "cost": _case_cost(run_dir, diagnostic, supervisor),
        "library_root": str(library_path) if library_path.is_dir() else None,
        "diagnostic_result_path": str(run_dir / "diagnostic" / "diagnostic_result.json")
            if diagnostic else None,
        "supervisor_exit_code": exit_code,
        "case_wall_timeout": timeout,
    }
    target = run_dir / "m5_case_result.json"
    if target.exists():
        raise FileExistsError("M5 E4 case result already exists")
    target.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True,
                                 indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--split", choices=("dev", "scored"), required=True)
    parser.add_argument("--run-dir", type=Path, required=True,
                        help="fresh child directory of E4_RUN_DIR")
    parser.add_argument("--library-root", type=Path, required=True)
    parser.add_argument("--experiment-manifest", type=Path,
                        help="required for scored runs; frozen after development")
    parser.add_argument("--task-view-root", type=Path,
                        help="orchestrator-staged public telemetry snapshot (required for scored)")
    parser.add_argument("--trusted-evaluator-socket", type=Path,
                        help="shared host evaluator socket (required for scored)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.split == "scored" and (args.experiment_manifest is None or
            args.task_view_root is None or args.trusted_evaluator_socket is None):
        raise SystemExit("scored runs require --experiment-manifest, --task-view-root, and --trusted-evaluator-socket")
    forwarded = ["--mode", "m5-diagnostic", "--case-id", args.case_id,
                 "--split", args.split, "--run-dir", str(args.run_dir),
                 "--library-root", str(args.library_root)]
    if args.experiment_manifest is not None:
        forwarded += ["--experiment-manifest", str(args.experiment_manifest)]
    if args.task_view_root is not None:
        forwarded += ["--task-view-root", str(args.task_view_root)]
    if args.trusted_evaluator_socket is not None:
        forwarded += ["--trusted-evaluator-socket", str(args.trusted_evaluator_socket)]
    exit_code = supervisor_main(forwarded)
    result = _write_case_result(args, exit_code)
    if result and result["terminal_classification"] == "diagnostic_complete":
        return 0
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

