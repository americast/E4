#!/usr/bin/env python3
"""Build an artifact-only report for the three-seed frozen E4 rerun.

The report reads only queue/freeze metadata, trusted combined aggregates, and
request journals. It writes no case-level outcomes, labels, prompts, or ledger
rows. Unknown request usage remains null rather than being imputed as zero.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[4]
LAUNCH = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from artifacts.escalation_fixes.implementation_20261026.launch.prepare_frozen_rerun import (
    ALLOWED_SEEDS, validate_launch_bundle,
)
from artifacts.seed_variability.launch.pilot_report import (
    extract_aggregate_fields, _iter_jsonl_selected, _SelectiveJSON,
)


STAGE_KEYS = ("stage1_prefix_successes_of_10", "stage2_prefix_successes_of_10",
    "stage3_prefix_successes_of_10")
STAGE_NAMES = ("retrieval", "adaptation", "evolve")
STAGE_RUNTIME_NAMES = ("stage1_select", "stage2_compose", "stage3_synthesize")
E4_JOURNAL_FIELDS = (
    ("call_id",), ("m4_stage",), ("http_status",),
    ("request_payload", "seed"), ("request_payload", "temperature"),
    ("request_payload", "top_p"), ("request_payload", "top_k"),
    ("request_payload", "max_tokens"),
    ("usage", "prompt_tokens"), ("usage", "input_tokens"),
    ("usage", "completion_tokens"), ("usage", "output_tokens"),
    ("error", "code"), ("generation_elapsed_seconds",),
    ("preflight_calls", "*", "endpoint"), ("preflight_calls", "*", "http_status"),
    ("preflight_calls", "*", "error"),
)
REPORTER_RELATIVE_PATH = "artifacts/escalation_fixes/implementation_20261026/launch/report_frozen_reruns.py"


class RerunReportError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _path(path: str | Path, label: str, *, must_exist: bool = True) -> Path:
    raw = Path(path)
    if raw.is_symlink():
        raise RerunReportError(f"refusing symlinked {label}")
    resolved = raw.resolve(strict=must_exist)
    if not resolved.is_relative_to(ROOT.resolve()):
        raise RerunReportError(f"{label} escapes workspace root")
    return resolved


def _json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RerunReportError(f"cannot read {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise RerunReportError(f"{label} must be a JSON object")
    return value


def _nonnegative_int(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise RerunReportError(f"{label} must be a nonnegative integer")
    return value


def _historical_inputs(path: Path) -> dict[str, Any]:
    historical = _json_object(path, "historical report")
    design = historical.get("pilot_design")
    per_seed = historical.get("per_seed")
    if (historical.get("schema_version") != "e4-seed-variability-report/1" or
            not isinstance(design, Mapping) or design.get("distinct_incidents") != 10 or
            design.get("dataset_seed") != 42 or design.get("same_incidents_repeated_each_seed") is not True or
            not isinstance(per_seed, Mapping)):
        raise RerunReportError("historical report is not a compatible ten-incident pilot aggregate")
    values: dict[str, Any] = {}
    for seed in ALLOWED_SEEDS:
        row = per_seed.get(str(seed))
        if not isinstance(row, Mapping):
            raise RerunReportError(f"historical report lacks seed{seed}")
        e4, t2 = row.get("E4"), row.get("T2")
        if not isinstance(e4, Mapping) or not isinstance(t2, Mapping):
            raise RerunReportError(f"historical report lacks seed{seed} system aggregates")
        prefix = e4.get("prefix_successes")
        if not isinstance(prefix, Mapping):
            raise RerunReportError(f"historical E4 seed{seed} lacks prefix counts")
        stage_counts = {name: _nonnegative_int(prefix.get(key), f"historical seed{seed} {name}")
            for name, key in zip(STAGE_NAMES, STAGE_KEYS)}
        t2_correct = _nonnegative_int(t2.get("correct_count"), f"historical T2 seed{seed} correct count")
        e4_correct = _nonnegative_int(e4.get("correct_count"), f"historical E4 seed{seed} correct count")
        if any(count > 10 for count in (*stage_counts.values(), t2_correct, e4_correct)):
            raise RerunReportError(f"historical seed{seed} aggregate exceeds the fixed denominator")
        values[str(seed)] = {"E4": {"case_count": 10, "correct_count": e4_correct,
                "accuracy": e4_correct / 10, "prefix_successes": stage_counts,
                "cost": _cost_projection(e4)},
            "T2": {"case_count": 10, "correct_count": t2_correct,
                "accuracy": t2_correct / 10, "cost": _cost_projection(t2)}}
    return {"values": values, "path": path.relative_to(ROOT).as_posix(),
        "sha256": _sha(path), "report_id": historical.get("report_id")}


def _cost_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    prompt = row.get("prompt_tokens")
    completion = row.get("completion_tokens")
    total = row.get("token_total")
    for key, value in (("prompt_tokens", prompt), ("completion_tokens", completion),
                       ("token_total", total)):
        if value is not None:
            _nonnegative_int(value, f"historical cost {key}")
    complete = (prompt is not None and completion is not None and total is not None and
        total == prompt + completion)
    if not complete:
        prompt = completion = total = None
    return {"prompt_tokens": prompt, "completion_tokens": completion,
        "token_total": total, "usage_complete": complete,
        "controller_attempts": row.get("controller_attempts"),
        "http_requests": row.get("http_requests")}


def _validate_manifest_and_queue(manifest_path: Path, queue_path: Path) -> tuple[
        dict[str, Any], dict[str, Any], dict[int, dict[str, Any]]]:
    from artifacts.escalation_fixes.implementation_20261026.launch import (
        prepare_frozen_rerun as rerun,
    )
    manifest = _json_object(manifest_path, "rerun manifest")
    queue = _json_object(queue_path, "queue status")
    if (manifest.get("schema_version") != "e4-paper-starter-frozen-rerun/1" or
            manifest.get("status") != "prepared_no_model_launch"):
        raise RerunReportError("rerun manifest is not a sealed frozen-rerun plan")
    manifest_sha = _sha(manifest_path)
    if (queue.get("schema_version") != "e4-frozen-rerun-queue/1" or
            queue.get("status") != "complete" or
            queue.get("rerun_manifest_sha256") != manifest_sha or
            queue.get("rerun_manifest_path") != manifest_path.relative_to(ROOT).as_posix()):
        raise RerunReportError("queue is incomplete or not linked to this exact rerun manifest")
    seeds = manifest.get("model_inference_seeds")
    if seeds != list(ALLOWED_SEEDS):
        raise RerunReportError("rerun must contain the fixed ordered 42/43/44 seed set")
    if manifest.get("helper_source_sha256", {}).get(REPORTER_RELATIVE_PATH) != _sha(Path(__file__)):
        raise RerunReportError("reporter source does not match the rerun's sealed helper hash")
    verified: dict[int, dict[str, Any]] = {}
    for seed in ALLOWED_SEEDS:
        _manifest, seed_row, _metadata_root = rerun.validate_launch_bundle(manifest_path, seed)
        queue_seed = queue.get("seeds", {}).get(str(seed))
        if not isinstance(queue_seed, Mapping) or queue_seed.get("seed") != seed:
            raise RerunReportError(f"queue status lacks seed{seed}")
        if queue_seed.get("last_event") != "seed_complete" or not isinstance(queue_seed.get("combined"), Mapping):
            raise RerunReportError(f"seed{seed} has no completed combined audit")
        if (queue_seed.get("model_run_root") != seed_row.get("model_run_root") or
                queue_seed.get("model_run_id") != seed_row.get("model_run_id")):
            raise RerunReportError(f"seed{seed} queue model-run identity differs from its seal")
        model_root = _path(ROOT / seed_row["model_run_root"], f"seed{seed} model-run root")
        run_doc = _json_object(model_root / "run.json", f"seed{seed} launcher run receipt")
        outer = _json_object(model_root / "outer-supervisor.json", f"seed{seed} supervisor receipt")
        launch = _json_object(model_root / "launch.json", f"seed{seed} model launch receipt")
        if (run_doc.get("run_id") != seed_row["model_run_id"] or run_doc.get("ok") is not True or
                run_doc.get("exit_code") != 0 or outer.get("exit_code") != 0 or
                outer.get("bubblewrap_exit_code") != 0 or
                launch.get("profile_sha256") != seed_row["profile_sha256"] or
                launch.get("model_sha256") != manifest.get("model_checkpoint_sha256")):
            raise RerunReportError(f"seed{seed} launcher run pins do not verify")
        handoff = _json_object(model_root / "launch_handoff.json", f"seed{seed} local handoff")
        if (handoff.get("status") != "complete" or handoff.get("run_root") != seed_row["model_run_root"] or
                handoff.get("freeze_manifest_sha256") != seed_row["derived_freeze_sha256"]):
            raise RerunReportError(f"seed{seed} local handoff does not match its derived freeze")
        audit_dir = _path(ROOT / handoff["audit_dir"], f"seed{seed} audit directory")
        audit_manifest_path = audit_dir / "audit_manifest.json"
        audit_manifest = _json_object(audit_manifest_path, f"seed{seed} audit manifest")
        if (audit_manifest.get("freeze_manifest_sha256") != seed_row["derived_freeze_sha256"] or
                audit_manifest.get("case_ids") != manifest.get("case_order") or
                audit_manifest.get("shard_groups") != manifest.get("shard_groups") or
                audit_manifest.get("model_profile_sha256") != seed_row["profile_sha256"] or
                audit_manifest.get("initial_library_snapshot_id") != manifest.get("initial_library_snapshot_id")):
            raise RerunReportError(f"seed{seed} audit manifest differs from its sealed plan")
        shard_reports: dict[str, Any] = {}
        for shard in ("local", "wingtip"):
            shard_state_path = audit_dir / f"bank10_{shard}" / "bank10_status.json"
            shard_state = _json_object(shard_state_path, f"seed{seed} {shard} shard status")
            expected_cases = manifest["shard_groups"][shard]
            actual_rows = shard_state.get("completed_cases")
            if (shard_state.get("status") != "complete" or
                    shard_state.get("audit_manifest_sha256") != _sha(audit_manifest_path) or
                    [row.get("case_id") for row in actual_rows or []] != expected_cases or
                    not actual_rows or actual_rows[0].get("input_library_snapshot_id") !=
                        manifest.get("initial_library_snapshot_id")):
                raise RerunReportError(f"seed{seed} {shard} shard lacks the complete fixed-order fresh chain")
            proof_path = model_root / f"{shard}_request_seed_proof.json"
            proof = _json_object(proof_path, f"seed{seed} {shard} request-seed proof")
            if (proof.get("seed") != seed or proof.get("shard") != shard or
                    proof.get("all_request_seeds_match") is not True or
                    proof.get("profile_sha256") != seed_row["profile_sha256"] or
                    proof.get("initial_library_snapshot_id") != manifest["initial_library_snapshot_id"] or
                    proof.get("case_count") != 5 or proof.get("request_count", 0) <= 0):
                raise RerunReportError(f"seed{seed} {shard} actual request-seed proof differs from its seal")
            proof_cases = proof.get("cases")
            if (not isinstance(proof_cases, list) or len(proof_cases) != 5 or
                    [case.get("case_index") for case in proof_cases] != [1, 2, 3, 4, 5]):
                raise RerunReportError(f"seed{seed} {shard} request proof does not cover five ordered cases")
            if sum(case.get("request_count", 0) for case in proof_cases) != proof.get("request_count"):
                raise RerunReportError(f"seed{seed} {shard} request-seed proof has an inconsistent count")
            if any(request.get("request_seed") != seed for case in proof_cases
                    for request in case.get("requests", [])):
                raise RerunReportError(f"seed{seed} {shard} proof contains a mismatched outgoing request seed")
            shard_reports[shard] = {"state_path": shard_state_path.relative_to(ROOT).as_posix(),
                "state_sha256": _sha(shard_state_path),
                "request_proof_path": proof_path.relative_to(ROOT).as_posix(),
                "request_proof_sha256": _sha(proof_path),
                "request_count": proof["request_count"],
                "case_count": 5}
        combined = queue_seed["combined"]
        combined_path = _path(ROOT / combined["combined_result_path"], f"seed{seed} combined result")
        if _sha(combined_path) != combined.get("combined_result_sha256"):
            raise RerunReportError(f"seed{seed} combined aggregate hash differs from queue status")
        verified[seed] = {"seed_row": seed_row, "model_run_root": model_root,
            "audit_dir": audit_dir, "audit_manifest_path": audit_manifest_path,
            "audit_manifest_sha256": _sha(audit_manifest_path), "shards": shard_reports,
            "combined_path": combined_path, "combined_sha256": _sha(combined_path)}
    return manifest, queue, verified


def _read_current_aggregate(path: Path) -> dict[str, Any]:
    fields = [("status",), ("case_count",), ("case_ids",),
        ("cumulative_top3_prefix_successes",)]
    rows = extract_aggregate_fields(path, fields)
    if (rows.get(("status",)) != "complete" or rows.get(("case_count",)) != 10 or
            not isinstance(rows.get(("cumulative_top3_prefix_successes",)), Mapping) or
            not isinstance(rows.get(("case_ids",)), list) or len(rows[("case_ids",)]) != 10):
        raise RerunReportError("combined E4 aggregate is not a ten-case completed result")
    counts = {name: _nonnegative_int(rows[("cumulative_top3_prefix_successes",)].get(key),
            f"current {name} prefix") for name, key in zip(STAGE_NAMES, STAGE_KEYS)}
    if any(value > 10 for value in counts.values()):
        raise RerunReportError("combined E4 prefix count exceeds ten")
    stage_shape = {"cases": {"*": {"stage_slots": {"*": {
        "stage": {"__terminal__": True}, "status": {"__terminal__": True},
        "valid": {"__terminal__": True}}}}}}
    parser = _SelectiveJSON(path)
    try:
        selected = parser.select(stage_shape)
        parser.finish()
    finally:
        parser.close()
    case_rows = selected.get("cases") if isinstance(selected, Mapping) else None
    if not isinstance(case_rows, list) or len(case_rows) != 10:
        raise RerunReportError("combined E4 aggregate lacks ten case stage summaries")
    stage_counts = {name: Counter() for name in STAGE_NAMES}
    for case in case_rows:
        slots = case.get("stage_slots") if isinstance(case, Mapping) else None
        if not isinstance(slots, list) or len(slots) != 3:
            raise RerunReportError("case stage-slot summaries do not have exactly three slots")
        for index, slot in enumerate(slots):
            if not isinstance(slot, Mapping):
                raise RerunReportError("case stage-slot summary is malformed")
            stage, status, valid = slot.get("stage"), slot.get("status"), slot.get("valid")
            if stage != STAGE_RUNTIME_NAMES[index] or not isinstance(status, str):
                raise RerunReportError("case stage slots are not ordered according to the fixed pipeline")
            stage_counts[STAGE_NAMES[index]][status] += 1
            if status == "submitted" and valid is False:
                stage_counts[STAGE_NAMES[index]]["invalid_submitted"] += 1
    return {"case_count": 10, "correct_count": counts["evolve"],
        "accuracy": counts["evolve"] / 10, "prefix_successes": counts,
        "stage_status_counts": {stage: dict(sorted(counts.items()))
            for stage, counts in stage_counts.items()}}


def _cost_from_e4_journals(audit_dir: Path, seed: int, profile: Mapping[str, Any],
                           case_order: Sequence[str], shard_groups: Mapping[str, Sequence[str]]) -> dict[str, Any]:
    fields = (("call_id",), ("m4_stage",), ("http_status",),
        ("request_payload", "seed"), ("request_payload", "temperature"),
        ("request_payload", "top_p"), ("request_payload", "top_k"),
        ("request_payload", "max_tokens"),
        ("usage", "prompt_tokens"), ("usage", "input_tokens"),
        ("usage", "completion_tokens"), ("usage", "output_tokens"),
        ("error", "code"), ("generation_elapsed_seconds",))
    by_stage = {stage: {"controller_attempts": 0, "http_requests": 0,
        "prompt_tokens": 0, "completion_tokens": 0, "usage_complete": True}
        for stage in STAGE_RUNTIME_NAMES}
    total_attempts = total_http = missing_journals = empty_journals = 0
    preflight_no_http = 0
    request_hashes: list[str] = []
    complete = True
    for shard in ("local", "wingtip"):
        case_ids = list(shard_groups[shard])
        for case_index, case_id in enumerate(case_ids, 1):
            journal = audit_dir / f"bank10_{shard}" / f"case{case_index:02d}" / "diagnostic" / "model_calls.jsonl"
            if journal.is_symlink() or not journal.is_file():
                missing_journals += 1
                complete = False
                continue
            request_hashes.append(_sha(journal))
            journal_attempts = 0
            for row in _iter_jsonl_selected(journal, fields):
                journal_attempts += 1
                call_id = row.get(("call_id",))
                stage = row.get(("m4_stage",))
                if not isinstance(call_id, str) or not call_id or stage not in by_stage:
                    raise RerunReportError(f"seed{seed} journal has malformed call or stage metadata")
                payload_seed = row.get(("request_payload", "seed"))
                if (payload_seed != seed or row.get(("request_payload", "temperature")) != profile.get("temperature") or
                        row.get(("request_payload", "top_p")) != profile.get("top_p") or
                        row.get(("request_payload", "top_k")) != profile.get("top_k")):
                    raise RerunReportError(f"seed{seed} outgoing E4 request differs from its sealed profile")
                max_tokens = row.get(("request_payload", "max_tokens"))
                if type(max_tokens) is not int or not 1 <= max_tokens <= profile.get("max_generation_tokens", 0):
                    raise RerunReportError(f"seed{seed} outgoing E4 max-token limit differs from its profile")
                bucket = by_stage[stage]
                bucket["controller_attempts"] += 1
                total_attempts += 1
                status = row.get(("http_status",))
                if status is None:
                    preflight_no_http += 1
                    bucket["usage_complete"] = False
                    complete = False
                    continue
                total_http += 1
                bucket["http_requests"] += 1
                prompt = row.get(("usage", "prompt_tokens"))
                if prompt is None:
                    prompt = row.get(("usage", "input_tokens"))
                completion = row.get(("usage", "completion_tokens"))
                if completion is None:
                    completion = row.get(("usage", "output_tokens"))
                if type(prompt) is not int or type(completion) is not int or min(prompt, completion) < 0:
                    bucket["usage_complete"] = False
                    complete = False
                    continue
                bucket["prompt_tokens"] += prompt
                bucket["completion_tokens"] += completion
            if journal_attempts == 0:
                empty_journals += 1
                complete = False
    stage_costs = {}
    cumulative_prompt = cumulative_completion = 0
    cumulative_known = True
    for stage, display in zip(STAGE_RUNTIME_NAMES, STAGE_NAMES):
        row = by_stage[stage]
        if not row["usage_complete"]:
            cumulative_known = False
        if cumulative_known:
            cumulative_prompt += row["prompt_tokens"]
            cumulative_completion += row["completion_tokens"]
        stage_costs[display] = {"controller_attempts": row["controller_attempts"],
            "http_requests": row["http_requests"], "usage_complete": row["usage_complete"],
            "prompt_tokens": row["prompt_tokens"] if row["usage_complete"] else None,
            "completion_tokens": row["completion_tokens"] if row["usage_complete"] else None,
            "cumulative_total_tokens": cumulative_prompt + cumulative_completion if cumulative_known else None}
    return {"controller_attempts": total_attempts, "http_requests": total_http,
        "preflight_or_non_http_attempts": preflight_no_http,
        "missing_journal_count": missing_journals, "empty_journal_count": empty_journals,
        "prompt_tokens": sum(row["prompt_tokens"] for row in by_stage.values()) if complete else None,
        "completion_tokens": sum(row["completion_tokens"] for row in by_stage.values()) if complete else None,
        "token_total": sum(row["prompt_tokens"] + row["completion_tokens"] for row in by_stage.values()) if complete else None,
        "usage_complete": complete and total_attempts > 0,
        "request_journal_count": len(request_hashes),
        "request_journal_sha256s": request_hashes, "stage_costs": stage_costs}


def _summary(values: Sequence[int | float | None]) -> dict[str, Any]:
    known = [float(value) for value in values if type(value) in (int, float)]
    return {"mean": sum(known) / len(known) if known else None,
        "observed_range": ({"minimum": min(known), "maximum": max(known)} if known else None),
        "available_count": len(known), "expected_count": len(values)}


def build_report(rerun_manifest_path: Path, queue_status_path: Path,
                 historical_report_path: Path | None = None) -> tuple[dict[str, Any], str]:
    manifest_path = _path(rerun_manifest_path, "rerun manifest")
    queue_path = _path(queue_status_path, "rerun queue status")
    manifest, queue, verified = _validate_manifest_and_queue(manifest_path, queue_path)
    if manifest.get("model_inference_seeds") != list(ALLOWED_SEEDS):
        raise RerunReportError("rerun manifest does not bind the fixed three-seed plan")
    pinned_history_path = _path(manifest.get("historical_report_path", ""),
        "sealed historical seed report")
    if historical_report_path is not None:
        history_path = _path(historical_report_path, "historical seed report")
        if history_path != pinned_history_path:
            raise RerunReportError("requested historical comparison differs from the rerun seal")
    else:
        history_path = pinned_history_path
    if _sha(history_path) != manifest.get("historical_report_sha256"):
        raise RerunReportError("historical comparison report differs from its rerun seal")
    historical = _historical_inputs(history_path)
    results: dict[str, Any] = {}
    provenance: dict[str, Any] = {}
    for seed in ALLOWED_SEEDS:
        verified_seed = verified[seed]
        seed_row = verified_seed["seed_row"]
        audit_dir = verified_seed["audit_dir"]
        manifest_audit_path = verified_seed["audit_manifest_path"]
        combined_path = verified_seed["combined_path"]
        aggregate_fields = extract_aggregate_fields(combined_path, [
            ("status",), ("case_count",), ("case_ids",),
            ("cumulative_top3_prefix_successes",),
            ("cases", "*", "stage_slots", "*", "stage"),
            ("cases", "*", "stage_slots", "*", "status"),
            ("cases", "*", "stage_slots", "*", "valid")])
        if (aggregate_fields.get(("status",)) != "complete" or
                aggregate_fields.get(("case_count",)) != 10 or
                aggregate_fields.get(("case_ids",)) != manifest.get("case_order")):
            raise RerunReportError(f"seed{seed} aggregate does not preserve all ten ordered cases")
        current = _read_current_aggregate(combined_path)
        profile_path = _path(ROOT / seed_row["profile_path"], f"seed{seed} profile")
        profile = _json_object(profile_path, f"seed{seed} profile")
        if profile.get("seed") != seed or _sha(profile_path) != seed_row["profile_sha256"]:
            raise RerunReportError(f"seed{seed} profile SHA/seed differs from rerun seal")
        current_cost = _cost_from_e4_journals(audit_dir, seed, profile,
            manifest["case_order"], manifest["shard_groups"])
        prior_e4 = historical["values"][str(seed)]["E4"]
        prior_t2 = historical["values"][str(seed)]["T2"]
        t2_cost = prior_t2["cost"]
        e4_cost = current_cost
        ratio_t2 = (100.0 * e4_cost["token_total"] / t2_cost["token_total"]
            if e4_cost["token_total"] is not None and t2_cost["token_total"] is not None and
               t2_cost["token_total"] > 0 else None)
        ratio_prior_e4 = (100.0 * e4_cost["token_total"] / prior_e4["cost"]["token_total"]
            if e4_cost["token_total"] is not None and prior_e4["cost"]["token_total"] is not None and
               prior_e4["cost"]["token_total"] > 0 else None)
        results[str(seed)] = {"case_count": 10,
            "current_e4": current, "historical_e4": prior_e4,
            "historical_t2": prior_t2,
            "current_e4_cost": e4_cost,
            "historical_e4_cost": prior_e4["cost"],
            "historical_t2_cost": t2_cost,
            "current_e4_cost_as_percent_of_historical_same_seed_t2": ratio_t2,
            "current_e4_cost_as_percent_of_historical_same_seed_e4": ratio_prior_e4}
        provenance[str(seed)] = {"model_run_id": seed_row["model_run_id"],
            "model_run_root": seed_row["model_run_root"],
            "derived_freeze_path": seed_row["derived_freeze_path"],
            "derived_freeze_sha256": seed_row["derived_freeze_sha256"],
            "profile_path": seed_row["profile_path"], "profile_sha256": seed_row["profile_sha256"],
            "audit_manifest_path": manifest_audit_path.relative_to(ROOT).as_posix(),
            "audit_manifest_sha256": _sha(manifest_audit_path),
            "combined_result_path": combined_path.relative_to(ROOT).as_posix(),
            "combined_result_sha256": _sha(combined_path),
            "local_shard": queue_seed.get("local_status_path"),
            "wingtip_shard": queue_seed.get("wingtip_status_path"),
            "request_journal_sha256s": current_cost["request_journal_sha256s"]}
    summary = {"current_e4_accuracy": _summary([results[str(seed)]["current_e4"]["accuracy"] for seed in ALLOWED_SEEDS]),
        "historical_e4_accuracy": _summary([results[str(seed)]["historical_e4"]["accuracy"] for seed in ALLOWED_SEEDS]),
        "historical_t2_accuracy": _summary([results[str(seed)]["historical_t2"]["accuracy"] for seed in ALLOWED_SEEDS]),
        "current_e4_input_tokens": _summary([results[str(seed)]["current_e4_cost"]["prompt_tokens"] for seed in ALLOWED_SEEDS]),
        "current_e4_output_tokens": _summary([results[str(seed)]["current_e4_cost"]["completion_tokens"] for seed in ALLOWED_SEEDS]),
        "current_e4_total_tokens": _summary([results[str(seed)]["current_e4_cost"]["token_total"] for seed in ALLOWED_SEEDS]),
        "historical_t2_total_tokens": _summary([results[str(seed)]["historical_t2_cost"]["token_total"] for seed in ALLOWED_SEEDS]),
        "current_e4_cost_percent_historical_t2": _summary([results[str(seed)]["current_e4_cost_as_percent_of_historical_same_seed_t2"] for seed in ALLOWED_SEEDS])}
    report = {"schema_version": "e4-frozen-rerun-results/1",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "design": {"distinct_incidents": 10, "dataset_seed": 42, "inference_seeds": list(ALLOWED_SEEDS),
            "same_cases_repeated_each_seed": True, "exploratory_only": True,
            "claims": "descriptive repeated-run engineering evidence; no significance, equivalence, or noninferiority claim"},
        "denominator": {"case_count_per_seed": 10, "failures_remain_in_denominator": True},
        "current_e4_method": "current frozen rerun aggregate; cumulative top-3 prefix counts correspond to retrieval, adaptation, evolve",
        "historical_baseline_source": {"path": historical["path"], "sha256": historical["sha256"],
            "report_id": historical["report_id"]},
        "t2_comparison": "historical same-seed T2 aggregates and ledger-derived cost as recorded in the immutable pilot report; T2 is not rerun here",
        "cost_definition": "sum of actual E4 request-ledger usage across all attempts including repair/failure requests; missing usage remains null; no reservation budget imputation",
        "rerun_manifest": {"path": manifest_path.relative_to(ROOT).as_posix(),
            "sha256": _sha(manifest_path), "freeze_path": manifest["new_freeze_path"],
            "freeze_sha256": manifest["new_freeze_sha256"],
            "source_archive_path": manifest["source_archive_path"],
            "source_archive_sha256": manifest["source_archive_sha256"],
            "helper_source_sha256": manifest["helper_source_sha256"],
            "config_sha256": manifest["config_sha256"]},
        "queue": {"path": queue_path.relative_to(ROOT).as_posix(),
            "pre_report_status_sha256": _sha(queue_path), "status": queue["status"],
            "report_receipt_is_appended_after_report_generation": True},
        "per_seed": results, "summary": summary, "sources": provenance,
        "published_case_identifiers": False, "evaluator_labels_read_by_reporter": False,
        "prompt_or_response_text_written": False}
    return report, render_markdown(report)


def _fmt_count(value: Any, denominator: int = 10) -> str:
    return "unknown" if type(value) is not int else f"{value}/{denominator}"


def _fmt_tokens(value: Any) -> str:
    return "unknown" if type(value) is not int else f"{value:,}"


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = ["# Frozen three-seed rerun results", "",
        "Exploratory repeated-run engineering evidence over the same ten incidents; the three seeds are not independent holdout samples. No significance, equivalence, or noninferiority claim is made.", "",
        "Failures remain in each ten-case denominator. Historical E4 and T2 values are read from the immutable prior report; T2 is not rerun in this revision.", "",
        "| Seed | E4 retrieval/adaptation/evolve cumulative top-3 | Historical E4 retrieval/adaptation/evolve | Historical T2 top-3 | Current E4 input+output tokens | Historical T2 input+output tokens | Current E4 / same-seed T2 |",
        "|---:|---:|---:|---:|---:|---:|---:|"]
    for seed in ALLOWED_SEEDS:
        row = report["per_seed"][str(seed)]
        current = row["current_e4"]["prefix_successes"]
        historical = row["historical_e4"]["prefix_successes"]
        current_prefix = "/".join(str(current[name]) for name in STAGE_NAMES)
        historical_prefix = "/".join(str(historical[name]) for name in STAGE_NAMES)
        e4_tokens = _fmt_tokens(row["current_e4_cost"]["token_total"])
        t2_tokens = _fmt_tokens(row["historical_t2_cost"]["token_total"])
        ratio = row["current_e4_cost_as_percent_of_historical_same_seed_t2"]
        ratio_text = "unknown" if ratio is None else f"{ratio:.1f}%"
        lines.append(f"| {seed} | {current_prefix} | {historical_prefix} | {_fmt_count(row['historical_t2']['correct_count'])} | {e4_tokens} | {t2_tokens} | {ratio_text} |")
    lines.extend(["", "## Stage-slot failures", "",
        "Counts are aggregated over the ten case slots per seed; no case-level outcomes are included.", "",
        "| Seed | Retrieval failed | Adaptation failed | Evolve failed | E4 request usage complete |",
        "|---:|---:|---:|---:|---:|"])
    for seed in ALLOWED_SEEDS:
        row = report["per_seed"][str(seed)]
        failures = row["current_e4"].get("stage_status_counts", {})
        cells = [str(failures.get(stage, {}).get("failed", 0)) for stage in STAGE_NAMES]
        usage = "yes" if row["current_e4_cost"]["usage_complete"] else "unknown/incomplete"
        lines.append(f"| {seed} | {cells[0]} | {cells[1]} | {cells[2]} | {usage} |")
    lines.extend(["", "## Descriptive summary", "",
        "| Measure | Mean | Observed range | Coverage |", "|---|---:|---:|---:|"])
    labels = (("Current E4 top-3 accuracy", "current_e4_accuracy", "percent"),
        ("Historical E4 top-3 accuracy", "historical_e4_accuracy", "percent"),
        ("Historical T2 top-3 accuracy", "historical_t2_accuracy", "percent"),
        ("Current E4 input+output tokens", "current_e4_total_tokens", "tokens"),
        ("Historical T2 input+output tokens", "historical_t2_total_tokens", "tokens"),
        ("Current E4 cost / historical same-seed T2", "current_e4_cost_percent_historical_t2", "ratio"))
    for label, key, style in labels:
        item=report["summary"][key]; mean=item["mean"]; observed=item["observed_range"]
        if mean is None or observed is None:
            mean_text=range_text="unknown"
        elif style=="percent":
            mean_text=f"{mean*100:.1f}%"; range_text=f"{observed['minimum']*100:.1f}%–{observed['maximum']*100:.1f}%"
        elif style=="tokens":
            mean_text=f"{mean:,.0f}"; range_text=f"{observed['minimum']:,.0f}–{observed['maximum']:,.0f}"
        else:
            mean_text=f"{mean:.1f}%"; range_text=f"{observed['minimum']:.1f}%–{observed['maximum']:.1f}%"
        lines.append(f"| {label} | {mean_text} | {range_text} | {item['available_count']}/{item['expected_count']} |")
    lines.extend(["", "Request-ledger usage includes all recorded calls, including failures and repairs. If any request lacks observed usage, its cost remains unknown; no output uses a reserved-generation estimate.", ""])
    return "\n".join(lines)


def write_immutable_report(rerun_manifest_path: Path, queue_status_path: Path,
        historical_report_path: Path | None = None, *, report_id: str | None = None,
        output_root: Path | None = None) -> dict[str, Any]:
    report_id = report_id or (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4))
    if not report_id or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in report_id):
        raise RerunReportError("report ID contains unsupported characters")
    report, markdown = build_report(rerun_manifest_path, queue_status_path, historical_report_path)
    parent = Path(output_root) if output_root is not None else ROOT / "artifacts/escalation_fixes/implementation_20261026/reports"
    if parent.is_symlink():
        raise RerunReportError("report output root may not be a symlink")
    parent.mkdir(parents=True, exist_ok=True)
    parent = parent.resolve(strict=True)
    if not parent.is_relative_to(ROOT.resolve()):
        raise RerunReportError("report output root escapes workspace")
    final = parent / report_id
    if final.exists() or final.is_symlink():
        raise FileExistsError(f"preserving existing report directory {final}")
    pending = parent / ("." + report_id + ".pending-" + secrets.token_hex(4))
    pending.mkdir(mode=0o700, exist_ok=False)
    output_json, output_md = pending / "rerun_results.json", pending / "rerun_results.md"
    _write_new(output_json, json.dumps(report, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n")
    _write_new(output_md, markdown)
    os.rename(pending, final)
    output_json, output_md = final / output_json.name, final / output_md.name
    return {"status": "written", "report_dir": final.relative_to(ROOT).as_posix(),
        "json_path": output_json.relative_to(ROOT).as_posix(), "json_sha256": _sha(output_json),
        "markdown_path": output_md.relative_to(ROOT).as_posix(), "markdown_sha256": _sha(output_md)}


def _write_new(path: Path, content: str) -> None:
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
    with os.fdopen(fd,"w",encoding="utf-8") as stream:
        stream.write(content);stream.flush();os.fsync(stream.fileno())


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rerun-manifest",required=True,type=Path)
    parser.add_argument("--queue-status",required=True,type=Path)
    parser.add_argument("--historical-report",type=Path,
        help="optional exact historical report path; if omitted, use the rerun manifest pin")
    parser.add_argument("--report-id")
    parser.add_argument("--output-root",type=Path)
    args=parser.parse_args()
    print(json.dumps(write_immutable_report(args.rerun_manifest,args.queue_status,
        args.historical_report,report_id=args.report_id,output_root=args.output_root),sort_keys=True))
