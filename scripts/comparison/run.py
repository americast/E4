#!/usr/bin/env python3
"""Run paired M5 development readiness or the authorized M6 scored comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
UNIX_SOCKET_PATH_MAX_BYTES = 107
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.comparison.metrics import paired_comparison
from e4.comparison.manifest import (validate_development_manifest,
                                    validate_frozen_manifest)
from e4.comparison.evaluator import (EvaluationSession, TrustedFeedbackServer,
                                     load_development_truth_ids, load_scored_truth_ids)
from e4.comparison.tasks import load_bank_case_view
from e4.agent.library import load_library_snapshot


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _trusted_socket_path(run_root: Path) -> Path:
    """Reuse one short, run-local trusted socket path across sequential child jobs."""
    path = Path(run_root).resolve() / "s"
    encoded = os.fsencode(str(path))
    if len(encoded) > UNIX_SOCKET_PATH_MAX_BYTES:
        raise ValueError("E4_RUN_DIR plus comparison run directory is too long for a trusted Unix socket; choose a shorter --run-dir")
    return path


def _write_checkpoint(path: Path, data: dict[str, Any]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=path.name + ".pending.", dir=path.parent)
    temporary = Path(temporary_name)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(data, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _write_json_once(path: Path, data: Any) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(data, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _verify_library_lineage(path: Path, initial_snapshot_id: str) -> str:
    snapshot = load_library_snapshot(path)
    current = snapshot
    seen: set[str] = set()
    while current.snapshot_id != initial_snapshot_id:
        if current.snapshot_id in seen:
            raise ValueError("E4 library lineage contains a cycle")
        seen.add(current.snapshot_id)
        parent_id = current.parent_snapshot_id
        if not parent_id:
            for event in reversed(current.document.get("lineage", ())):
                if event.get("event") == "snapshot_created" and event.get("parent_snapshot_id"):
                    parent_id = event["parent_snapshot_id"]
                    break
        if not parent_id:
            raise ValueError("E4 case library does not descend from the frozen initial snapshot")
        current = load_library_snapshot(current.path.parent / f"{parent_id}.json")
    return snapshot.snapshot_id


def _load_completed_job(job: dict[str, Any], *, system: str, case_id: str) -> dict[str, Any] | None:
    """Return a sealed checkpoint result; completed diagnosis failures are reusable."""
    if job.get("status") != "complete":
        return None
    result_path = Path(job["result_path"])
    if not result_path.is_file() or _sha(result_path) != job.get("result_sha256"):
        raise ValueError(f"completed {system}/{case_id} checkpoint result changed")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if not isinstance(result, dict) or result.get("case_id") != case_id:
        raise ValueError(f"completed {system}/{case_id} checkpoint has invalid contents")
    return result


def _resume_e4_library(jobs: dict[str, Any], case_ids: list[str],
                       initial_library: Path, initial_snapshot_id: str) -> Path:
    """Recover the latest completed E4 child in fixed case order, independent of pair order."""
    current = initial_library
    for case_id in case_ids:
        prior = jobs.get(case_id, {}).get("E4", {})
        if prior.get("status") != "complete" or not prior.get("library_root"):
            continue
        candidate = Path(prior["library_root"]).resolve()
        if not candidate.is_dir():
            raise ValueError(f"completed E4/{case_id} checkpoint library is unavailable")
        _verify_library_lineage(candidate, initial_snapshot_id)
        current = candidate
    return current


def _incomplete_job_keys(case_ids: list[str],
                         cases: dict[str, dict[str, Any]]) -> list[tuple[str, str]]:
    return [(case_id, system) for system in ("E4", "Terminus-2") for case_id in case_ids
            if case_id not in cases.get(system, {})]


def _normalise_slots(system: str, result: dict[str, Any], session: EvaluationSession) -> list[dict[str, Any]]:
    source_slots = result.get("attempt_slots", result.get("attempts"))
    if not isinstance(source_slots, list) or len(source_slots) != 3:
        raise ValueError(f"{system} result must preserve exactly three diagnosis slots")
    ledger = session.trusted_ledger()
    ledger_index = 0
    slots: list[dict[str, Any]] = []
    for index, source in enumerate(source_slots, 1):
        if not isinstance(source, dict) or source.get("attempt") != index:
            raise ValueError(f"{system} attempt slots are not ordered 1..3")
        status = source.get("status")
        accepted = source.get("accepted")
        if status not in {"submitted", "failed", "not_run"} or type(accepted) is not bool:
            raise ValueError(f"{system} attempt slot is malformed")
        normalized = {"attempt": index, "status": status, "accepted": accepted,
                      "valid": source.get("valid"), "correct_rank": None}
        if status == "submitted":
            if ledger_index >= len(ledger):
                raise ValueError(f"{system} submitted slot has no trusted feedback ledger row")
            trusted = ledger[ledger_index]
            ledger_index += 1
            if trusted["accepted"] is not accepted:
                raise ValueError(f"{system} slot feedback differs from trusted evaluator record")
            normalized["valid"] = trusted["valid"]
            normalized["correct_rank"] = trusted["correct_rank"]
        slots.append(normalized)
    if ledger_index != len(ledger):
        raise ValueError(f"{system} trusted evaluator has unaccounted submissions")
    return slots


def _case_cost(result: dict[str, Any] | None) -> dict[str, Any]:
    from e4.comparison.metrics import COST_FIELDS
    supplied = result.get("cost", {}) if isinstance(result, dict) else {}
    if not isinstance(supplied, dict):
        supplied = {}
    if not supplied and isinstance(result, dict) and isinstance(result.get("model_calls"), dict):
        calls = result["model_calls"]
        supplied = {"model_calls": calls.get("model_call_count"),
                    "prompt_tokens": calls.get("prompt_tokens"),
                    "completion_tokens": calls.get("completion_tokens"),
                    "model_seconds": calls.get("model_elapsed_seconds"),
                    "wall_seconds": result.get("wall_seconds")}
    if isinstance(result, dict):
        for field in COST_FIELDS:
            if field not in supplied and field in result:
                supplied[field] = result[field]
    return {field: supplied.get(field) for field in COST_FIELDS}


def _completed_diagnostic_result(system: str, result: dict[str, Any]) -> bool:
    classification = result.get("terminal_classification")
    if classification == "diagnostic_complete":
        return True
    if classification == "infrastructure_incomplete":
        return False
    if system == "Terminus-2":
        return result.get("agent_setup_completed") is not False
    return isinstance(result.get("attempt_slots"), list) and len(result["attempt_slots"]) == 3


def _run_child(command: list[str], *, cwd: Path, stdout: Any, stderr: Any,
               timeout_seconds: int) -> int:
    process = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=stderr,
                               start_new_session=True)
    try:
        return process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        raise


def _command_for(system: str, *, case_id: str, split: str, job_root: Path,
                 output_root: Path, task_root: Path, socket_path: Path,
                 manifest_path: Path, library_root: Path, harbor_python: Path,
                 root: Path, budgets: dict[str, Any]) -> list[str]:
    if system == "E4":
        return [sys.executable, str(root / "scripts/comparison/e4_case.py"),
                "--case-id", case_id, "--split", split,
                "--run-dir", str(output_root), "--library-root", str(library_root),
                "--experiment-manifest", str(manifest_path),
                "--task-view-root", str(task_root),
                "--trusted-evaluator-socket", str(socket_path)]
    turns = budgets.get("terminus_max_turns")
    if type(turns) is not int or turns <= 0:
        turns = 32
    token_budget = budgets.get("aggregate_generated_tokens")
    if type(token_budget) is not int or token_budget <= 0:
        token_budget = 73_728
    wall_budget = budgets.get("case_wall_seconds")
    if type(wall_budget) is not int or wall_budget <= 0:
        wall_budget = 7_200
    return [str(harbor_python), str(root / "scripts/comparison/terminus_case.py"),
            "--case-id", case_id, "--split", split, "--run-dir", str(output_root),
            "--experiment-manifest", str(manifest_path), "--task-view-root", str(task_root),
            "--trusted-evaluator-socket", str(socket_path),
            "--validated-manifest-sha256", _sha(manifest_path),
            "--max-turns", str(turns),
            "--generated-token-budget", str(token_budget),
            "--case-wall-seconds", str(wall_budget)]


def _run_job(*, system: str, case_id: str, split: str, run_root: Path,
             manifest_path: Path, library_root: Path, harbor_python: Path,
             budgets: dict[str, Any], root: Path, attempt_number: int,
             task_root: Path) -> dict[str, Any]:
    job_root = run_root / "jobs" / case_id / system / f"attempt-{attempt_number:02d}"
    job_root.mkdir(parents=True, exist_ok=False)
    started = __import__("time").monotonic()
    output_root = job_root / "output"
    # Only one paired case job is active at once. The feedback context removes the
    # socket before the next job reuses this compact run-local path.
    socket_path = _trusted_socket_path(run_root)
    view = load_bank_case_view(case_id, split=split, root=root)
    # The comparison orchestrator stages one immutable public view before either system
    # starts. Both systems receive this same host-visible path for the paired case.
    staged_root = task_root.resolve(strict=True)
    if not staged_root.is_dir():
        raise ValueError("paired public task view is unavailable")
    from e4.comparison.e4_runs import validate_staged_task_view
    validate_staged_task_view(staged_root, root=root, case_id=case_id, split=split)
    if split == "dev":
        truth_ids = load_development_truth_ids(root=root, case_id=case_id)
    else:
        truth_ids = load_scored_truth_ids(root=root, case_id=case_id,
                                          experiment_manifest=manifest_path)
    evaluation = EvaluationSession(case_id=case_id,
        allowed_component_ids=view.component_ids, truth_ids=truth_ids)
    harbor_runtime = harbor_python
    if not harbor_runtime.is_file():
        raise FileNotFoundError(f"pinned Harbor Python runtime is missing: {harbor_runtime}")
    command = _command_for(system, case_id=case_id, split=split, job_root=job_root,
                           output_root=output_root, task_root=task_root,
                           socket_path=socket_path, manifest_path=manifest_path,
                           library_root=library_root, harbor_python=harbor_runtime,
                           root=root, budgets=budgets)
    timeout_seconds = int(budgets["case_wall_seconds"])
    exit_code: int | None = None
    error = None
    protocol_failure = None
    try:
        with TrustedFeedbackServer(socket_path, evaluation) as feedback:
            with (job_root / "stdout.log").open("x", encoding="utf-8") as stdout, \
                    (job_root / "stderr.log").open("x", encoding="utf-8") as stderr:
                exit_code = _run_child(command, cwd=root, stdout=stdout, stderr=stderr,
                                       timeout_seconds=timeout_seconds)
            protocol_failure = feedback.failure
    except subprocess.TimeoutExpired:
        error = "case process exceeded its frozen outer wall allowance"
    except (OSError, ValueError, RuntimeError) as exc:
        error = f"{type(exc).__name__}: {exc}"
    elapsed = __import__("time").monotonic() - started
    trusted_path = job_root / "trusted_evaluation.json"
    _write_json_once(trusted_path, evaluation.trusted_ledger())
    result_path = output_root / ("m5_case_result.json" if system == "E4" else "terminus_result.json")
    result: dict[str, Any] | None = None
    if result_path.is_file():
        try:
            value = json.loads(result_path.read_text(encoding="utf-8"))
            result = value if isinstance(value, dict) else None
        except json.JSONDecodeError:
            result = None
    if result is not None and system == "E4":
        supervisor_path = output_root / "supervisor_manifest.json"
        if supervisor_path.is_file():
            supervisor = json.loads(supervisor_path.read_text(encoding="utf-8"))
            result.setdefault("attempt_slots", supervisor.get("attempt_slots"))
        slots_path = output_root / "diagnostic" / "m5_attempt_slots.json"
        if result.get("attempt_slots") is None and slots_path.is_file():
            result["attempt_slots"] = json.loads(slots_path.read_text(encoding="utf-8"))
        result["library_root"] = str(output_root / "diagnostic" / "library")
    if result is None:
        # An interrupted infrastructure attempt is retained and can be retried only
        # from a new invocation with --resume-from; it is never silently counted complete.
        return {"status": "infrastructure_interrupted", "exit_code": exit_code,
                "error": error or "runner result artifact is unavailable", "result_path": str(result_path),
                "artifact_root": str(job_root), "elapsed_seconds": elapsed,
                "trusted_evaluation_path": str(trusted_path),
                "trusted_evaluation_sha256": _sha(trusted_path),
                "cost": {field: (elapsed if field == "wall_seconds" else None)
                         for field in __import__("e4.comparison.metrics",
                             fromlist=["COST_FIELDS"]).COST_FIELDS},
                "complete": False}
    result_cost = _case_cost(result)
    result_cost["wall_seconds"] = elapsed
    if protocol_failure:
        return {"status": "feedback_protocol_failure", "exit_code": exit_code,
                "error": protocol_failure, "result_path": str(result_path),
                "artifact_root": str(job_root), "elapsed_seconds": elapsed,
                "trusted_evaluation_path": str(trusted_path),
                "trusted_evaluation_sha256": _sha(trusted_path), "cost": result_cost,
                "complete": False}
    if not _completed_diagnostic_result(system, result):
        return {"status": "infrastructure_interrupted", "exit_code": exit_code,
                "error": result.get("infrastructure_error") or "runner setup did not reach a diagnosis-ready state",
                "result_path": str(result_path), "artifact_root": str(job_root),
                "elapsed_seconds": elapsed, "trusted_evaluation_path": str(trusted_path),
                "trusted_evaluation_sha256": _sha(trusted_path), "cost": result_cost,
                "complete": False}
    try:
        slots = _normalise_slots(system, result, evaluation)
    except ValueError as exc:
        contract_path = job_root / "result_contract_failure.json"
        _write_json_once(contract_path, {"system": system, "case_id": case_id,
            "error": str(exc), "result_path": str(result_path)})
        return {"status": "result_contract_failure", "exit_code": exit_code,
                "error": str(exc), "artifact_root": str(job_root),
                "contract_failure_path": str(contract_path), "complete": False}
    result_doc = {"case_id": case_id, "system": system, "status": result.get("status"),
                  "attempts": slots, "cost": result_cost,
                  "source_result_path": str(result_path), "source_result_sha256": _sha(result_path),
                  "artifact_root": str(job_root), "process_exit_code": exit_code,
                  "process_error": error, "trusted_evaluation_path": str(trusted_path),
                  "trusted_evaluation_sha256": _sha(trusted_path),
                  "runner_result": result}
    output_path = job_root / "comparison_case_result.json"
    _write_json_once(output_path, result_doc)
    return {"status": "complete", "result_path": str(output_path),
            "result_sha256": _sha(output_path), "artifact_root": str(job_root),
            "library_root": str(output_root / "diagnostic" / "library") if system == "E4" else None,
            "cost": result_cost, "complete": True}


def _preflight_scored(manifest_path: Path, root: Path) -> tuple[dict[str, Any], list[str]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    case_ids = manifest.get("case_ids")
    if manifest.get("split") != "scored" or not isinstance(case_ids, list) or len(case_ids) != 10:
        raise ValueError("M6 requires the exact ten-case scored freeze manifest")
    for case_id in case_ids:
        validate_frozen_manifest(manifest_path, root=root, case_id=case_id, split="scored",
            initial_library_path=Path(manifest["local_pins"]["initial_library_path"]))
    order = manifest.get("paired_execution_order")
    expected_order = {case_id: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
                      for index, case_id in enumerate(case_ids)}
    if order != expected_order:
        raise ValueError("frozen paired execution order differs from the fixed alternating schedule")
    return manifest, case_ids


def _preflight_development(manifest_path: Path, root: Path) -> tuple[dict[str, Any], list[str]]:
    manifest = validate_development_manifest(manifest_path, root=root)
    return manifest, list(manifest["case_ids"])


def _check_preflight_receipt(receipt_path: Path, manifest_path: Path,
                             manifest: dict[str, Any], case_ids: list[str], *,
                             schema_version: str) -> None:
    receipt = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
    if (receipt.get("schema_version") != schema_version or
            receipt.get("experiment_manifest_sha256") != _sha(manifest_path) or
            receipt.get("case_ids") != case_ids or
            receipt.get("public_inputs") != manifest.get("public_inputs")):
        raise ValueError("preflight receipt does not match the manifest and selected public inputs")


def run_scored_experiment(*, manifest_path: Path, run_root: Path,
                          root: Path = ROOT, resume_from: Path | None = None,
                          harbor_python: Path | None = None,
                          preflight_receipt: Path | None = None) -> dict[str, Any]:
    """Execute the one fixed paired seed-42 pass under the trusted model launcher."""
    manifest_path = Path(manifest_path).resolve(strict=True)
    manifest, case_ids = _preflight_scored(manifest_path, root)
    if preflight_receipt is None:
        raise ValueError("scored execution requires a successful pre-model preflight receipt")
    _check_preflight_receipt(preflight_receipt, manifest_path, manifest, case_ids,
                             schema_version="e4-m5-preflight/1")
    run_root = Path(run_root).resolve()
    launcher_root = Path(os.environ.get("E4_RUN_DIR", "")).resolve()
    if not launcher_root.is_dir() or launcher_root not in run_root.parents:
        raise ValueError("comparison output must be a fresh child of E4_RUN_DIR")
    if run_root.exists():
        raise FileExistsError("comparison output directory must be fresh")
    run_root.mkdir(parents=True, exist_ok=False)
    manifest_sha = _sha(manifest_path)
    jobs: dict[str, Any] = {}
    if resume_from is not None:
        previous = Path(resume_from).resolve(strict=True)
        previous_state = json.loads((previous / "checkpoint.json").read_text(encoding="utf-8"))
        if previous_state.get("experiment_manifest_sha256") != manifest_sha:
            raise ValueError("cannot resume a different frozen experiment")
        jobs = previous_state.get("jobs", {})
    checkpoint = {"schema_version": "e4-m5-run-checkpoint/1", "run_id": manifest["run_id"],
                  "experiment_manifest": str(manifest_path),
                  "experiment_manifest_sha256": manifest_sha, "case_ids": case_ids,
                  "status": "running", "jobs": jobs,
                  "created_from_checkpoint": str(resume_from) if resume_from else None}
    checkpoint_path = run_root / "checkpoint.json"
    _write_checkpoint(checkpoint_path, checkpoint)
    budget_cfg = manifest["resource_budgets"]
    initial_library = Path(manifest["local_pins"]["initial_library_path"]).resolve()
    initial_id = manifest["local_pins"]["initial_library_snapshot_id"]
    current_library = _resume_e4_library(jobs, case_ids, initial_library, initial_id)
    harbor_python = Path(harbor_python or root / "artifacts/m5/runtime/harbor-python/bin/python").resolve()
    cases: dict[str, dict[str, Any]] = {"E4": {}, "Terminus-2": {}}
    for case_index, case_id in enumerate(case_ids):
        case_state = jobs.setdefault(case_id, {})
        order = manifest["paired_execution_order"][case_id]
        task_root = run_root / "public_cases" / case_id
        if not task_root.exists():
            # Stage and pin the host-visible public input once for the pair, before
            # launching either model. It lives in the fresh run directory and cannot
            # be rewritten by the second system.
            task_root.parent.mkdir(parents=True, exist_ok=True)
            source_view = load_bank_case_view(case_id, split="scored", root=root)
            staged_view = load_bank_case_view(case_id, split="scored", root=root,
                                              staged_root=task_root)
            if (staged_view.telemetry_file_sha256 != source_view.telemetry_file_sha256 or
                    staged_view.component_ids != source_view.component_ids or
                    staged_view.alert_sha256 != source_view.alert_sha256):
                raise ValueError("staged task view changed during trusted validation")
        elif not task_root.is_dir():
            raise ValueError("paired public task view path is not a directory")
        for short_name in order:
            system = "E4" if short_name == "e4" else "Terminus-2"
            previous_job = case_state.get(system, {})
            if system == "E4" and any(
                    jobs.get(prior_case, {}).get("E4", {}).get("status") != "complete"
                    for prior_case in case_ids[:case_index]):
                case_state.setdefault("E4", {"status": "pending_sequential_library"})
                _write_checkpoint(checkpoint_path, checkpoint)
                continue
            completed_result = _load_completed_job(previous_job, system=system, case_id=case_id)
            if completed_result is not None:
                data = completed_result
                cases[system][case_id] = data
                if system == "E4" and previous_job.get("library_root"):
                    current_library = Path(previous_job["library_root"]).resolve()
                continue
            if previous_job.get("status") == "feedback_protocol_failure":
                raise RuntimeError("cannot resume after a trusted feedback protocol failure")
            if previous_job.get("status") == "result_contract_failure":
                raise RuntimeError("cannot resume after a result-contract failure; fix code and freeze a new run")
            history = list(previous_job.get("history", []))
            if previous_job.get("status") in {"running", "infrastructure_interrupted"}:
                history.append({key: value for key, value in previous_job.items() if key != "history"})
            attempt_number = len(history) + 1
            attempt_root = run_root / "jobs" / case_id / system / f"attempt-{attempt_number:02d}"
            case_state[system] = {"status": "running", "history": history,
                                  "attempt_number": attempt_number,
                                  "artifact_root": str(attempt_root)}
            _write_checkpoint(checkpoint_path, checkpoint)
            job = _run_job(system=system, case_id=case_id, split="scored", run_root=run_root,
                manifest_path=manifest_path, library_root=current_library, harbor_python=harbor_python,
                budgets=budget_cfg, root=root, attempt_number=attempt_number,
                task_root=task_root)
            # Leave incomplete infrastructure attempts pinned in their first attempt
            # directory; a resumed invocation may retry in a newly allocated directory.
            if not job["complete"]:
                case_state[system] = {"status": "infrastructure_interrupted", **job,
                                      "history": history}
                _write_checkpoint(checkpoint_path, checkpoint)
                if job.get("status") == "feedback_protocol_failure":
                    checkpoint["status"] = "failed_feedback_protocol"
                    _write_checkpoint(checkpoint_path, checkpoint)
                    raise RuntimeError("trusted feedback protocol failed; stopping paired experiment")
                if job.get("status") == "result_contract_failure":
                    checkpoint["status"] = "failed_result_contract"
                    _write_checkpoint(checkpoint_path, checkpoint)
                    raise RuntimeError("result contract failed; stopping paired experiment")
                continue
            result_path = Path(job["result_path"])
            result_doc = json.loads(result_path.read_text(encoding="utf-8"))
            case_state[system] = {"status": "complete", **job, "history": history}
            cases[system][case_id] = result_doc
            from e4.comparison.metrics import COST_FIELDS
            all_costs = [item.get("cost", {}) for item in history] + [job.get("cost", {})]
            result_doc["cost"] = {
                field: (sum(record[field] for record in all_costs)
                        if all(isinstance(record, dict) and type(record.get(field)) in (int, float)
                               and __import__("math").isfinite(record[field]) and record[field] >= 0
                               for record in all_costs) else None)
                for field in COST_FIELDS}
            aggregate_case_path = Path(job["artifact_root"]) / "paired_case_result.json"
            _write_json_once(aggregate_case_path, result_doc)
            job["result_path"] = str(aggregate_case_path)
            job["result_sha256"] = _sha(aggregate_case_path)
            case_state[system].update({"result_path": job["result_path"],
                                       "result_sha256": job["result_sha256"]})
            if system == "E4" and job.get("library_root"):
                candidate = Path(job["library_root"]).resolve()
                if candidate.is_dir():
                    _verify_library_lineage(candidate, initial_id)
                    current_library = candidate
                    case_state[system]["library_root"] = str(candidate)
                else:
                    job["library_root"] = None
                    case_state[system]["library_root"] = None
            _write_checkpoint(checkpoint_path, checkpoint)
    incomplete_jobs = _incomplete_job_keys(case_ids, cases)
    if incomplete_jobs:
        checkpoint["status"] = "incomplete_infrastructure"
        checkpoint["incomplete_jobs"] = [
            {"case_id": case_id, "system": system,
             "status": jobs.get(case_id, {}).get(system, {}).get("status", "pending")}
            for case_id, system in incomplete_jobs]
        incomplete_path = run_root / "incomplete_summary.json"
        _write_json_once(incomplete_path, {"schema_version": "e4-m5-incomplete-run/1",
            "run_id": manifest["run_id"], "experiment_manifest_sha256": manifest_sha,
            "incomplete_jobs": checkpoint["incomplete_jobs"], "case_jobs": jobs})
        checkpoint["incomplete_summary"] = str(incomplete_path)
        checkpoint["incomplete_summary_sha256"] = _sha(incomplete_path)
        _write_checkpoint(checkpoint_path, checkpoint)
        return {"schema_version": "e4-m5-incomplete-run/1", "status": "incomplete",
                "run_id": manifest["run_id"], "case_ids": case_ids,
                "experiment_manifest_sha256": manifest_sha,
                "incomplete_jobs": checkpoint["incomplete_jobs"], "case_jobs": jobs}
    # Terminus has no cross-case bootstrap; zero is measured absence of that operation.
    empty_bootstrap = {field: 0 for field in __import__("e4.comparison.metrics",
        fromlist=["COST_FIELDS"]).COST_FIELDS}
    comparison = paired_comparison(cases, expected_case_ids=case_ids,
        bootstrap_costs={"E4": manifest.get("bootstrap_cost", {}),
                         "Terminus-2": empty_bootstrap})
    launcher_record: dict[str, Any] = {}
    launcher_path = launcher_root / "launch.json"
    if launcher_path.is_file():
        try:
            launcher_record = json.loads(launcher_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            launcher_record = {}
    shared_server_load = launcher_record.get("server_load_seconds")
    if (type(shared_server_load) not in (int, float) or shared_server_load < 0 or
            not __import__("math").isfinite(float(shared_server_load))):
        shared_server_load = None
    result = {**comparison, "split": "scored", "run_id": manifest["run_id"],
              "experiment_manifest_sha256": manifest_sha,
              "bootstrap_cost_separate": {
                  name: summary["bootstrap_cost_separate"]
                  for name, summary in comparison["systems"].items()},
              "execution_order": manifest["paired_execution_order"],
              "shared_model_server": {
                  "load_seconds": shared_server_load,
                  "measurement_source": "E4_RUN_DIR/launch.json server_load_seconds" if shared_server_load is not None else None,
              },
              "case_jobs": jobs}
    result_path = run_root / "comparison_result.json"
    _write_json_once(result_path, result)
    checkpoint["status"] = "complete"
    checkpoint["comparison_result"] = str(result_path)
    checkpoint["comparison_result_sha256"] = _sha(result_path)
    _write_checkpoint(checkpoint_path, checkpoint)
    return result


def run_development_experiment(*, manifest_path: Path, run_root: Path,
                               preflight_receipt: Path, root: Path = ROOT,
                               harbor_python: Path | None = None) -> dict[str, Any]:
    """Run the fixed public two-case paired M5 readiness pass after a label-free preflight."""
    manifest_path = Path(manifest_path).resolve(strict=True)
    manifest, case_ids = _preflight_development(manifest_path, root)
    _check_preflight_receipt(preflight_receipt, manifest_path, manifest, case_ids,
                             schema_version="e4-m5-development-preflight/1")
    run_root = Path(run_root).resolve()
    launcher_root = Path(os.environ.get("E4_RUN_DIR", "")).resolve()
    if not launcher_root.is_dir() or launcher_root not in run_root.parents:
        raise ValueError("development comparison output must be a child of E4_RUN_DIR")
    if run_root.exists():
        raise FileExistsError("development comparison output directory must be fresh")
    run_root.mkdir(parents=True, exist_ok=False)
    manifest_sha = _sha(manifest_path)
    budgets = manifest["resource_budgets"]
    initial_library = Path(manifest["local_pins"]["initial_library_path"]).resolve()
    initial_id = manifest["local_pins"]["initial_library_snapshot_id"]
    current_library = initial_library
    harbor_python = Path(harbor_python or root / "artifacts/m5/runtime/harbor-python/bin/python").resolve()
    jobs: dict[str, Any] = {}
    cases: dict[str, dict[str, Any]] = {"E4": {}, "Terminus-2": {}}
    checkpoint = {"schema_version": "e4-m5-development-checkpoint/1",
                  "run_id": manifest["run_id"], "split": "dev",
                  "experiment_manifest": str(manifest_path),
                  "experiment_manifest_sha256": manifest_sha,
                  "case_ids": case_ids, "status": "running", "jobs": jobs}
    checkpoint_path = run_root / "checkpoint.json"
    _write_checkpoint(checkpoint_path, checkpoint)
    for case_index, case_id in enumerate(case_ids):
        task_root = run_root / "public_cases" / case_id
        task_root.parent.mkdir(parents=True, exist_ok=True)
        source_view = load_bank_case_view(case_id, split="dev", root=root)
        staged_view = load_bank_case_view(case_id, split="dev", root=root,
                                          staged_root=task_root)
        if (staged_view.telemetry_file_sha256 != source_view.telemetry_file_sha256 or
                staged_view.component_ids != source_view.component_ids or
                staged_view.alert_sha256 != source_view.alert_sha256):
            raise ValueError("staged development view changed during trusted validation")
        for short_name in manifest["paired_execution_order"][case_id]:
            system = "E4" if short_name == "e4" else "Terminus-2"
            if system == "E4" and any(
                    jobs.get(prior, {}).get("E4", {}).get("status") != "complete"
                    for prior in case_ids[:case_index]):
                jobs.setdefault(case_id, {})[system] = {"status": "pending_sequential_library"}
                continue
            case_state = jobs.setdefault(case_id, {})
            job_root = run_root / "jobs" / case_id / system / "attempt-01"
            case_state[system] = {"status": "running", "attempt_number": 1,
                                  "artifact_root": str(job_root)}
            _write_checkpoint(checkpoint_path, checkpoint)
            job = _run_job(system=system, case_id=case_id, split="dev", run_root=run_root,
                manifest_path=manifest_path, library_root=current_library,
                harbor_python=harbor_python, budgets=budgets, root=root,
                attempt_number=1, task_root=task_root)
            if not job["complete"]:
                case_state[system] = {"status": "infrastructure_interrupted", **job}
                _write_checkpoint(checkpoint_path, checkpoint)
                continue
            result_doc = json.loads(Path(job["result_path"]).read_text(encoding="utf-8"))
            case_state[system] = {"status": "complete", **job}
            cases[system][case_id] = result_doc
            if system == "E4" and job.get("library_root"):
                candidate = Path(job["library_root"]).resolve()
                if candidate.is_dir():
                    _verify_library_lineage(candidate, initial_id)
                    current_library = candidate
                    case_state[system]["library_root"] = str(candidate)
            _write_checkpoint(checkpoint_path, checkpoint)
    incomplete = _incomplete_job_keys(case_ids, cases)
    if incomplete:
        details = [{"case_id": case_id, "system": system,
                    "status": jobs.get(case_id, {}).get(system, {}).get("status", "pending")}
                   for case_id, system in incomplete]
        target = run_root / "incomplete_summary.json"
        _write_json_once(target, {"schema_version": "e4-m5-development-incomplete/1",
            "run_id": manifest["run_id"], "experiment_manifest_sha256": manifest_sha,
            "incomplete_jobs": details, "case_jobs": jobs})
        checkpoint.update({"status": "incomplete_infrastructure", "incomplete_jobs": details,
                           "incomplete_summary": str(target),
                           "incomplete_summary_sha256": _sha(target)})
        _write_checkpoint(checkpoint_path, checkpoint)
        return {"schema_version": "e4-m5-development-incomplete/1", "status": "incomplete",
                "run_id": manifest["run_id"], "case_ids": case_ids,
                "experiment_manifest_sha256": manifest_sha, "incomplete_jobs": details,
                "case_jobs": jobs}
    comparison = paired_comparison(cases, expected_case_ids=case_ids)
    result = {**comparison, "split": "dev", "status": "development_complete",
              "run_id": manifest["run_id"], "experiment_manifest_sha256": manifest_sha,
              "execution_order": manifest["paired_execution_order"], "case_jobs": jobs,
              "limitations": ["fixed two-case development readiness evidence only",
                              "resource budgets remain pending measurement"]}
    result_path = run_root / "development_comparison_result.json"
    _write_json_once(result_path, result)
    checkpoint.update({"status": "development_complete", "comparison_result": str(result_path),
                       "comparison_result_sha256": _sha(result_path)})
    _write_checkpoint(checkpoint_path, checkpoint)
    return result


def _read_cases(path: Path) -> dict[str, Any]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or not isinstance(doc.get("cases"), dict):
        raise ValueError(f"{path} must contain a top-level cases object")
    return doc["cases"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split", choices=("dev", "scored"), default="scored")
    parser.add_argument("--input", action="append", default=[], metavar="SYSTEM=PATH",
                        help="case artifact JSON; each case includes attempts and cost")
    parser.add_argument("--experiment-manifest", type=Path)
    parser.add_argument("--output", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight-only", action="store_true",
                      help="verify manifest pins and fixed public cases before model startup")
    mode.add_argument("--execute", action="store_true",
                      help="run the paired dev readiness or scored M6 pass under scripts/model/run.py")
    parser.add_argument("--run-dir", type=Path,
                        help="fresh child of E4_RUN_DIR for the paired execution")
    parser.add_argument("--resume-from", type=Path,
                        help="continue pending cases from a matching prior checkpoint")
    parser.add_argument("--harbor-python", type=Path,
                        help="pinned Python 3.13 Harbor environment interpreter")
    parser.add_argument("--preflight-receipt", type=Path,
                        help="receipt produced by --preflight-only before starting Gemma")
    args = parser.parse_args()
    if args.preflight_only or args.execute:
        if args.experiment_manifest is None:
            parser.error("preflight/execution requires --experiment-manifest")
        if args.preflight_only:
            if args.split == "scored":
                manifest, case_ids = _preflight_scored(args.experiment_manifest.resolve(), ROOT)
                receipt_schema = "e4-m5-preflight/1"
            else:
                manifest, case_ids = _preflight_development(args.experiment_manifest.resolve(), ROOT)
                receipt_schema = "e4-m5-development-preflight/1"
            if args.preflight_receipt is None:
                parser.error("--preflight-only requires --preflight-receipt to preserve the validation receipt")
            receipt = {"schema_version": receipt_schema,
                       "experiment_manifest_sha256": _sha(args.experiment_manifest.resolve()),
                       "case_ids": case_ids, "public_inputs": manifest["public_inputs"]}
            receipt_path = args.preflight_receipt.resolve()
            receipt_path.parent.mkdir(parents=True, exist_ok=True)
            with receipt_path.open("x", encoding="utf-8") as stream:
                json.dump(receipt, stream, ensure_ascii=False, sort_keys=True, indent=2)
                stream.write("\n")
            print(json.dumps({"status": "preflight_passed", "split": args.split,
                              "case_count": len(case_ids),
                              "case_ids": case_ids,
                              "experiment_manifest_sha256": _sha(args.experiment_manifest),
                              "receipt": str(receipt_path)},
                             sort_keys=True))
            return 0
        launcher_dir = Path(os.environ.get("E4_RUN_DIR", "")).resolve()
        if not launcher_dir.is_dir():
            parser.error("--execute must run under scripts/model/run.py with E4_RUN_DIR set")
        if args.preflight_receipt is None:
            parser.error("--execute requires the successful preflight receipt")
        run_dir = args.run_dir or (launcher_dir / ("m6_comparison" if args.split == "scored"
                                                   else "m5_development"))
        if args.split == "scored":
            result = run_scored_experiment(manifest_path=args.experiment_manifest,
                run_root=run_dir, root=ROOT, resume_from=args.resume_from,
                harbor_python=args.harbor_python, preflight_receipt=args.preflight_receipt)
        else:
            if args.resume_from is not None:
                parser.error("development readiness runs use a fresh run directory and do not resume")
            result = run_development_experiment(manifest_path=args.experiment_manifest,
                run_root=run_dir, root=ROOT, harbor_python=args.harbor_python,
                preflight_receipt=args.preflight_receipt)
        if result.get("status") == "incomplete":
            print(json.dumps({"status": "incomplete", "run_dir": str(run_dir),
                              "incomplete_jobs": result["incomplete_jobs"]}, sort_keys=True))
            return 2
        print(json.dumps({"status": result.get("status", "complete"),
                          "split": args.split, "run_dir": str(run_dir),
                          "accuracy": {name: result["systems"][name]["accuracy_at_3_3"]
                                       for name in result["systems"]}}, sort_keys=True))
        return 0
    if not args.input or args.output is None:
        parser.error("aggregation requires at least two --input SYSTEM=PATH entries and --output")
    sample = json.loads((ROOT / "config/bank/sample.json").read_text(encoding="utf-8"))
    if args.split == "scored":
        if args.experiment_manifest is None:
            parser.error("--experiment-manifest is required for scored comparison")
        frozen = json.loads(args.experiment_manifest.read_text(encoding="utf-8"))
        expected_ids = frozen.get("case_ids", [])
        if frozen.get("split") != "scored":
            parser.error("experiment manifest does not describe the scored split")
        for case_id in expected_ids:
            validate_frozen_manifest(args.experiment_manifest, root=ROOT, case_id=case_id,
                split="scored", initial_library_path=Path(frozen["local_pins"]["initial_library_path"]))
    else:
        expected_ids = [row["incident_id"] for row in sample.get("development_cases", [])]
    systems = {}
    for item in args.input:
        if "=" not in item:
            parser.error("each --input must be SYSTEM=PATH")
        name, filename = item.split("=", 1)
        if not name or name in systems:
            parser.error("comparison system names must be nonempty and unique")
        systems[name] = _read_cases(Path(filename))
    result = paired_comparison(systems, expected_case_ids=expected_ids)
    result["split"] = args.split
    if args.split == "scored":
        result["experiment_manifest"] = str(args.experiment_manifest)
        result["experiment_manifest_sha256"] = __import__("hashlib").sha256(
            args.experiment_manifest.read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"output": str(args.output), "case_count": len(expected_ids),
                      "systems": list(systems)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
