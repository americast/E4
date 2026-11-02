#!/usr/bin/env python3
"""Run one exploratory E4-only revision against the immutable recorded Terminus-2 baseline."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.m5_comparison import COST_FIELDS, paired_comparison
from e4core.m4_library import load_library_snapshot
from scripts.run_bank_comparison import (_check_preflight_receipt, _run_job,
    _preflight_scored, _sha, _verify_library_lineage, _write_checkpoint, _write_json_once,
    )
from e4core.m5_tasks import load_bank_case_view


BASELINE_COMPARISON_RELATIVE = Path(
    "artifacts/m3/model_runs/bankm6i2/m6_comparison/comparison_result.json")
BASELINE_COMPARISON_SHA256 = "07d374ea333aacc7b38ef69efb1c98d3c3ddf47093774614c5e4a47d1c457bfd"
BASELINE_MANIFEST_RELATIVE = Path(
    "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json")
BASELINE_MANIFEST_SHA256 = "a4bb8489736cd9b01b72128b974db7e975334c6dd06f407b7e0f252bf96a68c7"
BASELINE_SOURCE_ARCHIVE_RELATIVE = Path(
    "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_source.tar.gz")
BASELINE_SOURCE_ARCHIVE_SHA256 = "14b00ba6c7f028d91ede80abe8b01bb255de626e9ba2a372b1532be5319040ae"
REVISION_METADATA_SCHEMA = "e4-exploratory-revision/1"
REVISION_MODE = "exploratory_post_analysis_revision"
ALLOWED_RUNTIME_DELTA_PATHS = {
    "policy_name", "policy_rationale",
    "query_and_generation.maximum_operator_input_bytes",
    "query_and_generation.generated_operator_cpu_seconds",
    "query_and_generation.generated_operator_wall_seconds",
}


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _dotted_get(document: Mapping[str, Any], dotted: str) -> Any:
    value: Any = document
    for part in dotted.split("."):
        if not isinstance(value, Mapping) or part not in value:
            raise ValueError(f"runtime policy is missing {dotted}")
        value = value[part]
    return value


def _dotted_get_optional(document: Mapping[str, Any], dotted: str) -> Any:
    value: Any = document
    for part in dotted.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return None
        value = value[part]
    return value


def _dotted_set(document: dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    parent = document
    for part in parts[:-1]:
        child = parent.get(part)
        if not isinstance(child, dict):
            raise ValueError(f"runtime policy is missing {dotted}")
        parent = child
    parent[parts[-1]] = value


def _dotted_delete(document: dict[str, Any], dotted: str) -> None:
    parts = dotted.split(".")
    parent = document
    for part in parts[:-1]:
        child = parent.get(part)
        if not isinstance(child, dict):
            return
        parent = child
    parent.pop(parts[-1], None)


def _validate_runtime_delta(baseline_runtime: Mapping[str, Any],
                            revision_runtime: Mapping[str, Any]) -> dict[str, Any]:
    """Require the documented E4 generated-worker resource changes and no other policy drift."""
    normalized = copy.deepcopy(dict(revision_runtime))
    baseline_values = {path: _dotted_get_optional(baseline_runtime, path)
                       for path in ALLOWED_RUNTIME_DELTA_PATHS}
    # The original policy relied on the generic worker's implicit 2 CPU-second
    # default and had no policy_rationale field. Record that effective default while
    # preserving the archived policy's actual missing-key shape for the diff.
    baseline_effective_values = dict(baseline_values)
    baseline_effective_values[
        "query_and_generation.generated_operator_cpu_seconds"] = (
            baseline_values["query_and_generation.generated_operator_cpu_seconds"]
            if baseline_values["query_and_generation.generated_operator_cpu_seconds"] is not None
            else 2)
    revision_values = {path: _dotted_get(revision_runtime, path)
                       for path in ALLOWED_RUNTIME_DELTA_PATHS}
    for path in ALLOWED_RUNTIME_DELTA_PATHS:
        if baseline_values[path] is None:
            _dotted_delete(normalized, path)
        else:
            _dotted_set(normalized, path, baseline_values[path])
    if normalized != dict(baseline_runtime):
        raise ValueError("E4 runtime policy differs outside the documented sandbox resource allowance")
    resource_paths = {
        "query_and_generation.maximum_operator_input_bytes": (2 * 1024 * 1024, 16 * 1024 * 1024),
        "query_and_generation.generated_operator_cpu_seconds": (baseline_effective_values[
            "query_and_generation.generated_operator_cpu_seconds"], 10),
        "query_and_generation.generated_operator_wall_seconds": (5, 15),
    }
    for path, (old, new) in resource_paths.items():
        observed_old = baseline_values[path]
        if observed_old is None and path.endswith("generated_operator_cpu_seconds"):
            observed_old = baseline_effective_values[path]
        if observed_old != old or revision_values[path] != new:
            raise ValueError(f"documented E4 sandbox change differs at {path}")
    if (revision_values["policy_name"] != "bank-m4-controller-input16m-cpu10-wall15-v3" or
            not isinstance(revision_values["policy_rationale"], str) or
            "2 MiB" not in revision_values["policy_rationale"] or
            "10 CPU seconds" not in revision_values["policy_rationale"] or
            "15 wall seconds" not in revision_values["policy_rationale"]):
        raise ValueError("revised E4 runtime policy does not document its sandbox resource change")
    return {"baseline_values": baseline_effective_values,
            "baseline_explicit_values": baseline_values,
            "revision_values": revision_values,
            "allowed_changed_paths": sorted(ALLOWED_RUNTIME_DELTA_PATHS)}


def _validate_revision_metadata(revision_manifest: Mapping[str, Any], *,
                                baseline_result_path: Path,
                                baseline_manifest_path: Path) -> Mapping[str, Any]:
    metadata = revision_manifest.get("revision_metadata")
    if (not isinstance(metadata, Mapping) or
            metadata.get("schema_version") != REVISION_METADATA_SCHEMA or
            metadata.get("mode") != REVISION_MODE or
            metadata.get("terminus_reused") is not True or
            metadata.get("contemporaneous") is not False):
        raise ValueError("new freeze must identify a noncontemporaneous exploratory E4-only revision")
    result_ref = metadata.get("baseline_comparison", {})
    manifest_ref = metadata.get("baseline_manifest", {})
    if (not isinstance(result_ref, Mapping) or not isinstance(manifest_ref, Mapping) or
            result_ref.get("path") != BASELINE_COMPARISON_RELATIVE.as_posix() or
            result_ref.get("sha256") != BASELINE_COMPARISON_SHA256 or
            manifest_ref.get("path") != BASELINE_MANIFEST_RELATIVE.as_posix() or
            manifest_ref.get("sha256") != BASELINE_MANIFEST_SHA256):
        raise ValueError("revision manifest does not pin the approved immutable Terminus baseline")
    if (baseline_result_path.resolve() != (ROOT / BASELINE_COMPARISON_RELATIVE).resolve() or
            baseline_manifest_path.resolve() != (ROOT / BASELINE_MANIFEST_RELATIVE).resolve()):
        raise ValueError("only the approved immutable baseline paths may be reused")
    note = metadata.get("sandbox_delta_note")
    if not isinstance(note, str) or not all(word in note.casefold() for word in
            ("e4", "sandbox", "terminus", "unchanged")):
        raise ValueError("revision manifest must document the E4-only sandbox change and unchanged Terminus baseline")
    return metadata


def _validate_revision_contract(revision_manifest: Mapping[str, Any],
                                baseline_manifest: Mapping[str, Any],
                                baseline_result: Mapping[str, Any], *,
                                baseline_runtime: Mapping[str, Any],
                                revision_runtime: Mapping[str, Any]) -> dict[str, Any]:
    """Compare only frozen public/run contracts; never reads private evaluator labels."""
    case_ids = baseline_manifest.get("case_ids")
    if (not isinstance(case_ids, list) or len(case_ids) != 10 or
            revision_manifest.get("case_ids") != case_ids or
            baseline_result.get("case_ids") != case_ids or
            baseline_result.get("experiment_manifest_sha256") != BASELINE_MANIFEST_SHA256):
        raise ValueError("revision must reuse the baseline's exact ordered ten-case allowlist")
    if (revision_manifest.get("split") != "scored" or
            baseline_manifest.get("split") != "scored" or
            revision_manifest.get("seed") != baseline_manifest.get("seed") or
            revision_manifest.get("systems") != ["E4", "Terminus-2"]):
        raise ValueError("revision must retain the scored split, seed, and common frozen manifest header")
    if (revision_manifest.get("public_inputs") != baseline_manifest.get("public_inputs") or
            revision_manifest.get("model") != baseline_manifest.get("model")):
        raise ValueError("revision changed public inputs or the frozen model/profile/checkpoint")
    old_budget = baseline_manifest.get("resource_budgets", {})
    new_budget = revision_manifest.get("resource_budgets", {})
    for field in ("case_wall_seconds", "aggregate_generated_tokens", "terminus_max_turns"):
        if new_budget.get(field) != old_budget.get(field):
            raise ValueError(f"revision changed frozen {field}")
    if baseline_manifest.get("paired_execution_order") != revision_manifest.get("paired_execution_order"):
        raise ValueError("revision changed the fixed case ordering")
    runtime_delta = _validate_runtime_delta(baseline_runtime, revision_runtime)
    term_jobs = baseline_result.get("case_jobs", {})
    if not isinstance(term_jobs, Mapping):
        raise ValueError("immutable baseline has no Terminus case-job ledger")
    for case_id in case_ids:
        item = term_jobs.get(case_id, {}).get("Terminus-2", {})
        if item.get("status") != "complete" or item.get("complete") is not True:
            raise ValueError(f"immutable Terminus baseline is incomplete for {case_id}")
        if not isinstance(item.get("result_path"), str) or not _is_sha256(item.get("result_sha256")):
            raise ValueError(f"immutable Terminus result hash is missing for {case_id}")
    return {"case_ids": list(case_ids), "runtime_policy_delta": runtime_delta}


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _load_baseline_terminus_cases(baseline_result: Mapping[str, Any], *,
                                  case_ids: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    result_hashes: dict[str, dict[str, str]] = {}
    for case_id in case_ids:
        job = baseline_result["case_jobs"][case_id]["Terminus-2"]
        result_path = Path(job["result_path"]).resolve(strict=True)
        if result_path.is_symlink() or _sha(result_path) != job["result_sha256"]:
            raise ValueError(f"immutable Terminus paired-case result changed for {case_id}")
        result_doc = _json(result_path)
        if result_doc.get("case_id") != case_id or result_doc.get("system") != "Terminus-2":
            raise ValueError(f"immutable Terminus paired-case result identity is invalid for {case_id}")
        source_path = Path(result_doc.get("source_result_path", "")).resolve(strict=True)
        source_hash = result_doc.get("source_result_sha256")
        if (source_path.is_symlink() or not _is_sha256(source_hash) or
                _sha(source_path) != source_hash):
            raise ValueError(f"immutable Terminus source-result hash changed for {case_id}")
        attempts = result_doc.get("attempts")
        cost = result_doc.get("cost")
        if not isinstance(attempts, list) or len(attempts) != 3 or not isinstance(cost, Mapping):
            raise ValueError(f"immutable Terminus case record is incomplete for {case_id}")
        cases[case_id] = {"attempts": attempts, "cost": dict(cost)}
        result_hashes[case_id] = {"paired_case_result_sha256": job["result_sha256"],
                                  "source_result_sha256": source_hash}
    return cases, result_hashes


def build_revision_metadata(*, root: Path = ROOT,
                            baseline_result_path: Path | None = None,
                            baseline_manifest_path: Path | None = None) -> dict[str, Any]:
    """Build metadata only after validating the pinned baseline and exact runtime delta."""
    root = Path(root).resolve(strict=True)
    result_path, manifest_path = _baseline_paths(root, baseline_result_path,
                                                 baseline_manifest_path)
    result = _json(result_path)
    baseline_manifest = _json(manifest_path)
    archive_path = root / BASELINE_SOURCE_ARCHIVE_RELATIVE
    if _sha(archive_path) != BASELINE_SOURCE_ARCHIVE_SHA256:
        raise ValueError("immutable baseline source archive hash mismatch")
    import tarfile
    with tarfile.open(archive_path, "r:gz") as archive:
        member = archive.extractfile("config/m4_runtime.json")
        if member is None:
            raise ValueError("baseline source archive omits its runtime policy")
        baseline_runtime = json.loads(member.read().decode("utf-8"))
    revision_runtime = _json(root / "config/m4_runtime.json")
    runtime_delta = _validate_runtime_delta(baseline_runtime, revision_runtime)
    case_ids = baseline_manifest.get("case_ids", [])
    if (len(case_ids) != 10 or result.get("case_ids") != case_ids or
            result.get("experiment_manifest_sha256") != BASELINE_MANIFEST_SHA256):
        raise ValueError("immutable baseline result does not match its frozen ten-case manifest")
    _load_baseline_terminus_cases(result, case_ids=case_ids)
    return {"schema_version": REVISION_METADATA_SCHEMA, "mode": REVISION_MODE,
        "terminus_reused": True, "contemporaneous": False,
        "baseline_comparison": {"path": BASELINE_COMPARISON_RELATIVE.as_posix(),
                                 "sha256": BASELINE_COMPARISON_SHA256},
        "baseline_manifest": {"path": BASELINE_MANIFEST_RELATIVE.as_posix(),
                              "sha256": BASELINE_MANIFEST_SHA256},
        "sandbox_delta_note": (
            "E4 generated-operator sandbox resources changed within the documented policy; "
            "model, profile, checkpoint, public inputs, acceptance, token and wall budgets are "
            "unchanged. Terminus-2 results are reused unchanged from the immutable baseline."),
        "runtime_policy_delta": runtime_delta}


def _baseline_paths(root: Path, result_override: Path | None,
                    manifest_override: Path | None) -> tuple[Path, Path]:
    result = (result_override or root / BASELINE_COMPARISON_RELATIVE).resolve(strict=True)
    manifest = (manifest_override or root / BASELINE_MANIFEST_RELATIVE).resolve(strict=True)
    if result != (root / BASELINE_COMPARISON_RELATIVE).resolve() or _sha(result) != BASELINE_COMPARISON_SHA256:
        raise ValueError("immutable baseline comparison artifact hash/path mismatch")
    if manifest != (root / BASELINE_MANIFEST_RELATIVE).resolve() or _sha(manifest) != BASELINE_MANIFEST_SHA256:
        raise ValueError("immutable baseline frozen manifest hash/path mismatch")
    return result, manifest


def _preflight_revision(*, manifest_path: Path, receipt_path: Path,
                        baseline_result_path: Path | None = None,
                        baseline_manifest_path: Path | None = None,
                        root: Path = ROOT) -> dict[str, Any]:
    manifest_path = Path(manifest_path).resolve(strict=True)
    baseline_result_path, baseline_manifest_path = _baseline_paths(
        root, baseline_result_path, baseline_manifest_path)
    revision_manifest = _json(manifest_path)
    _validate_revision_metadata(revision_manifest, baseline_result_path=baseline_result_path,
                                baseline_manifest_path=baseline_manifest_path)
    baseline_manifest = _json(baseline_manifest_path)
    baseline_result = _json(baseline_result_path)
    source_archive = root / BASELINE_SOURCE_ARCHIVE_RELATIVE
    if _sha(source_archive) != BASELINE_SOURCE_ARCHIVE_SHA256:
        raise ValueError("immutable baseline source archive hash mismatch")
    import tarfile
    with tarfile.open(source_archive, "r:gz") as archive:
        member = archive.extractfile("config/m4_runtime.json")
        if member is None:
            raise ValueError("baseline source archive omits its runtime policy")
        baseline_runtime = json.loads(member.read().decode("utf-8"))
    revision_runtime = _json(root / "config/m4_runtime.json")
    contract = _validate_revision_contract(revision_manifest, baseline_manifest, baseline_result,
        baseline_runtime=baseline_runtime, revision_runtime=revision_runtime)
    if revision_manifest["revision_metadata"].get("runtime_policy_delta") != contract["runtime_policy_delta"]:
        raise ValueError("revision metadata runtime policy delta does not match the pinned source policies")
    case_ids = contract["case_ids"]
    checked_manifest, checked_ids = _preflight_scored(manifest_path, root)
    if checked_manifest != revision_manifest or checked_ids != case_ids:
        raise ValueError("standard scored preflight differs from exploratory revision contract")
    _load_baseline_terminus_cases(baseline_result, case_ids=case_ids)
    receipt = {"schema_version": "e4-m5-preflight/1",
        "experiment_manifest_sha256": _sha(manifest_path), "case_ids": case_ids,
        "public_inputs": revision_manifest["public_inputs"],
        "revision_baseline_comparison_sha256": BASELINE_COMPARISON_SHA256,
        "revision_baseline_manifest_sha256": BASELINE_MANIFEST_SHA256,
        "revision_runtime_policy_delta": contract["runtime_policy_delta"]}
    target = Path(receipt_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    _write_json_once(target, receipt)
    return {"status": "preflight_passed", "case_ids": case_ids,
            "experiment_manifest_sha256": receipt["experiment_manifest_sha256"],
            "baseline_comparison_sha256": BASELINE_COMPARISON_SHA256,
            "baseline_manifest_sha256": BASELINE_MANIFEST_SHA256,
            "receipt": str(target)}


def run_e4_revision(*, manifest_path: Path, receipt_path: Path, run_root: Path,
                    baseline_result_path: Path | None = None,
                    baseline_manifest_path: Path | None = None,
                    harbor_python: Path | None = None, root: Path = ROOT) -> dict[str, Any]:
    manifest_path = Path(manifest_path).resolve(strict=True)
    receipt_path = Path(receipt_path).resolve(strict=True)
    baseline_result_path, baseline_manifest_path = _baseline_paths(
        root, baseline_result_path, baseline_manifest_path)
    manifest = _json(manifest_path)
    metadata = _validate_revision_metadata(manifest,
        baseline_result_path=baseline_result_path, baseline_manifest_path=baseline_manifest_path)
    baseline_manifest = _json(baseline_manifest_path)
    baseline_result = _json(baseline_result_path)
    source_archive = root / BASELINE_SOURCE_ARCHIVE_RELATIVE
    if _sha(source_archive) != BASELINE_SOURCE_ARCHIVE_SHA256:
        raise ValueError("immutable baseline source archive hash mismatch")
    import tarfile
    with tarfile.open(source_archive, "r:gz") as archive:
        member = archive.extractfile("config/m4_runtime.json")
        if member is None:
            raise ValueError("baseline source archive omits its runtime policy")
        baseline_runtime = json.loads(member.read().decode("utf-8"))
    revision_runtime = _json(root / "config/m4_runtime.json")
    contract = _validate_revision_contract(manifest, baseline_manifest, baseline_result,
        baseline_runtime=baseline_runtime, revision_runtime=revision_runtime)
    if metadata.get("runtime_policy_delta") != contract["runtime_policy_delta"]:
        raise ValueError("revision metadata runtime policy delta changed after preflight")
    checked_manifest, checked_ids = _preflight_scored(manifest_path, root)
    if checked_manifest != manifest or checked_ids != contract["case_ids"]:
        raise ValueError("scored manifest differs from the revision preflight contract")
    _load_baseline_terminus_cases(baseline_result, case_ids=contract["case_ids"])
    _check_preflight_receipt(receipt_path, manifest_path, manifest,
        contract["case_ids"], schema_version="e4-m5-preflight/1")
    receipt = _json(receipt_path)
    if (receipt.get("revision_baseline_comparison_sha256") != BASELINE_COMPARISON_SHA256 or
            receipt.get("revision_baseline_manifest_sha256") != BASELINE_MANIFEST_SHA256):
        raise ValueError("preflight receipt does not pin the immutable comparison baseline")
    launcher_root = Path(os.environ.get("E4_RUN_DIR", "")).resolve()
    raw_run_root = Path(run_root)
    run_root = (raw_run_root if raw_run_root.is_absolute() else launcher_root / raw_run_root).resolve()
    if not launcher_root.is_dir() or launcher_root not in run_root.parents:
        raise ValueError("revision output must be a fresh child of E4_RUN_DIR")
    if run_root.exists():
        raise FileExistsError("revision output directory must be fresh")
    run_root.mkdir(parents=True, exist_ok=False)

    case_ids = list(manifest["case_ids"])
    term_cases, term_hashes = _load_baseline_terminus_cases(baseline_result, case_ids=case_ids)
    initial_library = Path(manifest["local_pins"]["initial_library_path"]).resolve(strict=True)
    initial_id = manifest["local_pins"]["initial_library_snapshot_id"]
    if load_library_snapshot(initial_library).snapshot_id != initial_id:
        raise ValueError("revision must start from its freshly pinned pristine bootstrap library")
    harbor_python = Path(harbor_python or root / "artifacts/m5/runtime/harbor-python/bin/python").resolve()
    if not harbor_python.is_file():
        raise FileNotFoundError(f"pinned Harbor Python runtime is missing: {harbor_python}")

    jobs: dict[str, Any] = {}
    e4_cases: dict[str, dict[str, Any]] = {}
    current_library = initial_library
    checkpoint = {"schema_version": "e4-exploratory-revision-checkpoint/1",
        "run_id": manifest["run_id"], "status": "running",
        "experiment_manifest": str(manifest_path),
        "experiment_manifest_sha256": _sha(manifest_path),
        "baseline_comparison_sha256": BASELINE_COMPARISON_SHA256,
        "baseline_manifest_sha256": BASELINE_MANIFEST_SHA256,
        "case_ids": case_ids, "jobs": jobs}
    checkpoint_path = run_root / "checkpoint.json"
    _write_checkpoint(checkpoint_path, checkpoint)
    for index, case_id in enumerate(case_ids):
        task_root = run_root / "public_cases" / case_id
        task_root.parent.mkdir(parents=True, exist_ok=True)
        source_view = load_bank_case_view(case_id, split="scored", root=root)
        staged_view = load_bank_case_view(case_id, split="scored", root=root, staged_root=task_root)
        if (staged_view.telemetry_file_sha256 != source_view.telemetry_file_sha256 or
                staged_view.component_ids != source_view.component_ids or
                staged_view.alert_sha256 != source_view.alert_sha256):
            raise ValueError("staged revision public task view changed during trusted validation")
        case_root = run_root / "jobs" / case_id / "E4" / "attempt-01"
        jobs[case_id] = {"status": "running", "attempt_number": 1,
                         "artifact_root": str(case_root),
                         "library_before": str(current_library)}
        _write_checkpoint(checkpoint_path, checkpoint)
        job = _run_job(system="E4", case_id=case_id, split="scored", run_root=run_root,
            manifest_path=manifest_path, library_root=current_library,
            harbor_python=harbor_python, budgets=manifest["resource_budgets"], root=root,
            attempt_number=1, task_root=task_root)
        if not job.get("complete"):
            jobs[case_id] = {"status": "infrastructure_incomplete", **job,
                             "attempt_number": 1}
            checkpoint["status"] = "incomplete"
            checkpoint["incomplete_case_id"] = case_id
            _write_checkpoint(checkpoint_path, checkpoint)
            summary_path = run_root / "incomplete_summary.json"
            _write_json_once(summary_path, {"schema_version": "e4-exploratory-revision-incomplete/1",
                "run_id": manifest["run_id"], "case_ids": case_ids,
                "completed_case_ids": list(e4_cases), "incomplete_case_id": case_id,
                "jobs": jobs})
            return {"status": "incomplete", "run_id": manifest["run_id"],
                    "completed_case_ids": list(e4_cases), "incomplete_case_id": case_id,
                    "incomplete_summary": str(summary_path)}
        case_result = _json(Path(job["result_path"]))
        e4_cases[case_id] = {"attempts": case_result["attempts"], "cost": case_result["cost"]}
        jobs[case_id] = {"status": "complete", **job,
                         "attempt_number": 1, "library_before": str(current_library)}
        if job.get("library_root"):
            next_library = Path(job["library_root"]).resolve(strict=True)
            _verify_library_lineage(next_library, initial_id)
            current_library = next_library
            jobs[case_id]["library_after"] = str(current_library)
        _write_checkpoint(checkpoint_path, checkpoint)

    zero_bootstrap = {field: 0 for field in COST_FIELDS}
    comparison = paired_comparison({"E4_revision": e4_cases,
                                    "Terminus-2_recorded": term_cases},
        expected_case_ids=case_ids,
        bootstrap_costs={"E4_revision": manifest.get("bootstrap_cost", {}),
                         "Terminus-2_recorded": zero_bootstrap})
    result = {**comparison, "split": "scored", "status": "exploratory_revision_complete",
        "comparison_type": REVISION_MODE, "contemporaneous": False,
        "terminus_execution": "reused_immutable_recorded_baseline_without_rerun",
        "revision_run_id": manifest["run_id"],
        "experiment_manifest_sha256": _sha(manifest_path),
        "baseline_comparison_path": str(baseline_result_path),
        "baseline_comparison_sha256": BASELINE_COMPARISON_SHA256,
        "baseline_manifest_path": str(baseline_manifest_path),
        "baseline_manifest_sha256": BASELINE_MANIFEST_SHA256,
        "terminus_case_hashes": term_hashes,
        "runtime_policy_delta": receipt.get("revision_runtime_policy_delta"),
        "sandbox_delta_note": metadata["sandbox_delta_note"],
        "limitations": ["exploratory post-analysis revision on previously evaluated cases",
                        "Terminus-2 is reused from the immutable original run; results are not contemporaneous",
                        "E4-only sandbox resource changes are part of this revision"],
        "case_jobs": jobs}
    result_path = run_root / "e4_revision_comparison_result.json"
    _write_json_once(result_path, result)
    checkpoint.update({"status": "complete", "comparison_result": str(result_path),
                       "comparison_result_sha256": _sha(result_path)})
    _write_checkpoint(checkpoint_path, checkpoint)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-manifest", type=Path, required=True)
    parser.add_argument("--preflight-receipt", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path,
                        help="fresh child of E4_RUN_DIR (defaults to e4_revision during execution)")
    parser.add_argument("--baseline-result", type=Path)
    parser.add_argument("--baseline-manifest", type=Path)
    parser.add_argument("--harbor-python", type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.preflight_only:
            result = _preflight_revision(manifest_path=args.experiment_manifest,
                receipt_path=args.preflight_receipt, baseline_result_path=args.baseline_result,
                baseline_manifest_path=args.baseline_manifest)
        else:
            result = run_e4_revision(manifest_path=args.experiment_manifest,
                receipt_path=args.preflight_receipt, run_root=args.run_dir or Path("e4_revision"),
                baseline_result_path=args.baseline_result,
                baseline_manifest_path=args.baseline_manifest,
                harbor_python=args.harbor_python)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result.get("status") in {"preflight_passed", "exploratory_revision_complete"} else 2
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"revision runner stopped before completion: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
