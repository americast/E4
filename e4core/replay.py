"""Offline transcript replay for corrected synthetic benchmark oracles."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import statistics
import time
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from unittest.mock import patch

from .benchmark import (ROOT, checker_sha256, run_task, runtime_context, sha256_file,
                        summarize_outcomes, validate_suite)
from .model_client import LocalModelClient, ModelCall
from .telecom_ops import telecom_registry


CAPTURE_SUITE_SHA256 = "1e0b7d91e6a8d8e7f7dca673b8361f8fe776d91a178a4b3f2651a0f2ba291cac"
CAPTURE_CHECKER_SHA256 = "aa88d8e49be2bdf7f7f9bdbee5feccf4c393e69271d24245494b57112481c023"
CAPTURE_SOURCE_ARCHIVE_SHA256 = "dc101d028c0d27b7ece5d669cb4fb9477cf85e6e5ec969c3080d637dbd65184b"
V2_SUITE_PATH = ROOT / "config/m3_synthetic_suite_v2.json"


class ReplayError(RuntimeError):
    pass


def _jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ReplayError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
        if type(row) is not dict:
            raise ReplayError(f"expected JSON object at {path}:{line_number}")
        rows.append(row)
    return rows


def _hash_tree(directory: Path) -> dict[str, str]:
    files = sorted(path for path in directory.rglob("*") if path.is_file())
    return {path.relative_to(directory).as_posix(): sha256_file(path) for path in files}


def _helper_hashes() -> dict[str, str]:
    paths = (Path(__file__).resolve(), ROOT / "scripts/replay_m3_benchmark.py")
    return {path.relative_to(ROOT).as_posix(): sha256_file(path) for path in paths}


def _number(value: Any) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, ValueError):
        return False


def _audit_equal(actual: Any, expected: Any) -> bool:
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual is expected
    if _number(actual) and _number(expected):
        return math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-12)
    if actual is None or expected is None or isinstance(actual, str) or isinstance(expected, str):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(_audit_equal(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(_audit_equal(actual[k], expected[k])
                                                        for k in actual)
    return type(actual) is type(expected) and actual == expected


def _independent_operator_result(task_id: str, case: Mapping[str, Any]) -> dict[str, Any]:
    inputs, params, runtime = case["inputs"], case["params"], case["runtime"]
    if task_id == "operator_01_metric_mean_support":
        rows = [row["value"] for row in inputs["rows"]
                if row["entity"] == params["entity"] and row["metric"] == params["metric"]
                and _number(row["value"])]
        mean = statistics.mean(rows) if len(rows) >= runtime["minimum_support"] else None
        return {"mean": mean, "support": len(rows)}
    if task_id == "operator_02_trace_failure_denominator":
        values = [span["success"] for span in inputs["spans"]
                  if span["component"] == params["component"] and type(span["success"]) is bool]
        failures = sum(value is False for value in values)
        return {"failure_rate": failures / len(values) if values else None,
                "failures": failures, "denominator": len(values)}
    if task_id == "operator_03_trace_local_parent_edges":
        spans = inputs["spans"]
        known = {(span["trace_id"], span["id"]) for span in spans}
        resolved = unresolved = 0
        for span in spans:
            parent = span["parent_id"]
            if span["component"] != params["component"] or parent in (None, ""):
                continue
            if (span["trace_id"], parent) in known:
                resolved += 1
            else:
                unresolved += 1
        return {"resolved": resolved, "unresolved": unresolved}
    if task_id == "operator_04_median_window_contrast":
        delta = statistics.median(inputs["alert"]) - statistics.median(inputs["reference"])
        return {"delta": delta, "exceeds": delta > runtime["delta_threshold"]}
    if task_id == "operator_05_fixed_time_blocks":
        width = params["block_minutes"] * 60000
        groups: dict[int, list[float]] = {}
        for sample in inputs["samples"]:
            value = sample["value"]
            if not _number(value):
                continue
            index = math.floor((sample["timestamp_ms"] - runtime["window_start_ms"]) / width)
            groups.setdefault(index, []).append(value)
        indices = sorted(groups)
        return {"block_indices": indices,
                "medians": [statistics.median(groups[index]) for index in indices]}
    if task_id == "operator_06_runtime_view_read_only":
        return {"scaled_sum": sum(inputs["values"]) * params["multiplier"],
                "depth": runtime["dag_depth"], "seed": runtime["seed"]}
    raise ReplayError(f"no independent oracle implementation for {task_id}")


def audit_operator_oracles(suite: Mapping[str, Any]) -> dict[str, Any]:
    """Independently recompute each frozen operator fixture from its public inputs."""
    checks = []
    for task in suite["tasks"]:
        if task["kind"] != "operator":
            continue
        for index, case in enumerate(task["checks"]):
            calculated = _independent_operator_result(task["id"], case)
            if not _audit_equal(calculated, case["expected"]):
                checks.append({"task_id": task["id"], "case_index": index,
                               "independent_result": calculated,
                               "stored_expected": case["expected"]})
    return {"operator_check_count": sum(len(task["checks"]) for task in suite["tasks"]
                                         if task["kind"] == "operator"),
            "mismatch_count": len(checks), "mismatches": checks}


class TranscriptClient:
    """Returns captured calls after verifying every replayed request byte-for-byte."""

    def __init__(self, rows: Sequence[Mapping[str, Any]], profile: Mapping[str, Any],
                 base_url: str, context_tokens: int, max_generation_tokens: int):
        self.rows = list(rows)
        self.position = 0
        self.model_client = LocalModelClient(profile, base_url=base_url,
                                             context_tokens=context_tokens)
        self.model_client.max_generation_tokens = int(max_generation_tokens)
        self.verified_prompts = 0
        self.verified_payloads = 0

    def complete(self, prompt: str, *, task_id: str, attempt: int,
                 max_tokens: int | None = None) -> ModelCall:
        if self.position >= len(self.rows):
            raise ReplayError(f"replay requested extra model call {task_id} attempt {attempt}")
        row = self.rows[self.position]
        self.position += 1
        if row.get("record_type") != "model_call":
            raise ReplayError("transcript row has an unexpected record_type")
        if row.get("task_id") != task_id or row.get("attempt") != attempt:
            raise ReplayError(f"call order mismatch: expected {task_id}/{attempt}, got "
                              f"{row.get('task_id')}/{row.get('attempt')}")
        request_payload = row.get("request_payload")
        messages = request_payload.get("messages") if isinstance(request_payload, dict) else None
        if (not isinstance(messages, list) or len(messages) != 2 or
                messages[1].get("role") != "user" or messages[1].get("content") != prompt):
            raise ReplayError(f"rendered prompt mismatch for {task_id} attempt {attempt}")
        prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        if row.get("prompt_sha256") != prompt_sha:
            raise ReplayError(f"prompt hash mismatch for {task_id} attempt {attempt}")
        self.verified_prompts += 1
        generated = self.model_client.make_payload(prompt, max_tokens=max_tokens)
        if _canonical_payload(generated) != _canonical_payload(request_payload):
            raise ReplayError(f"request/profile payload mismatch for {task_id} attempt {attempt}")
        self.verified_payloads += 1
        model_fields = {field.name for field in fields(ModelCall)}
        missing = model_fields - set(row)
        if missing:
            raise ReplayError(f"transcript call missing ModelCall fields: {sorted(missing)}")
        values = {name: row[name] for name in model_fields}
        values["preflight_calls"] = tuple(values["preflight_calls"])
        return ModelCall(**values)

    def require_consumed(self) -> None:
        if self.position != len(self.rows):
            raise ReplayError(f"unused model transcript rows: {len(self.rows) - self.position}")


def _selection_costs(source_summary: Mapping[str, Any], gpu: Mapping[str, Any],
                     source_run: Path) -> dict[str, Any]:
    gpu_samples = gpu.get("samples", [])
    free_values = [sample["free_mib"] for sample in gpu_samples if type(sample.get("free_mib")) is int]
    used_values = [sample["used_mib"] for sample in gpu_samples if type(sample.get("used_mib")) is int]
    return {"source_summary_path": str(source_run / "m3_benchmark_summary.json"),
            "elapsed_seconds": source_summary.get("elapsed_seconds"),
            "input_tokens": source_summary.get("input_tokens"),
            "output_tokens": source_summary.get("output_tokens"),
            "gpu_memory_path": str(source_run / "gpu-memory.json"),
            "gpu_memory_sha256": sha256_file(source_run / "gpu-memory.json"),
            "gpu_sample_count": len(gpu_samples),
            "minimum_free_mib": min(free_values) if free_values else None,
            "peak_used_mib": max(used_values) if used_values else None,
            "reserve_mib": gpu.get("reserve_mib"),
            "reserve_satisfied": gpu.get("reserve_satisfied")}


def _write_json(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True,
                                allow_nan=False) + "\n")


def _canonical_payload(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReplayError(f"request payload is not finite JSON: {exc}") from exc


def replay_captured_run(capture_dir: Path, suite_path: Path,
                        output_dir: Path) -> dict[str, Any]:
    """Replay all tasks from a completed capture without making model requests."""
    thread_environment = {key: os.environ.get(key) for key in
                          ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                           "TOKENIZERS_PARALLELISM")}
    if thread_environment != {"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                             "MKL_NUM_THREADS": "1", "TOKENIZERS_PARALLELISM": "false"}:
        raise ReplayError("replay must use the captured launcher’s numerical thread limits")
    capture_dir = capture_dir.resolve(strict=True)
    output_dir = output_dir.resolve()
    allowed_capture_root = (ROOT / "artifacts/m3/model_runs").resolve()
    allowed_output_root = (ROOT / "artifacts/m3/offline_replays").resolve()
    if not capture_dir.is_relative_to(allowed_capture_root):
        raise ReplayError("capture directory must be under artifacts/m3/model_runs")
    if not output_dir.is_relative_to(allowed_output_root):
        raise ReplayError("replay output must be under artifacts/m3/offline_replays")
    if output_dir == capture_dir or output_dir.is_relative_to(capture_dir):
        raise ReplayError("replay output cannot be inside its source capture")
    suite_path = suite_path.resolve(strict=True)
    if not suite_path.is_relative_to(ROOT.resolve()):
        raise ReplayError("corrected suite must be inside the project root")

    source_hashes_before = _hash_tree(capture_dir)
    required = ("m3_benchmark_manifest.json", "m3_model_calls.jsonl",
                "m3_task_results.jsonl", "m3_benchmark_summary.json", "gpu-memory.json")
    missing = [name for name in required if not (capture_dir / name).is_file()]
    if missing:
        raise ReplayError(f"capture is incomplete; missing {missing}")
    source_manifest = json.loads((capture_dir / required[0]).read_text())
    source_summary = json.loads((capture_dir / "m3_benchmark_summary.json").read_text())
    gpu = json.loads((capture_dir / "gpu-memory.json").read_text())
    if (source_manifest.get("suite_id") != "m3-synthetic-v2" or
            source_manifest.get("suite_sha256") != CAPTURE_SUITE_SHA256 or
            source_manifest.get("checker_sha256") != CAPTURE_CHECKER_SHA256):
        raise ReplayError("source capture does not match the frozen v2 suite/checker")
    archive = ROOT / "artifacts/m3/capture_source_v2.tar.gz"
    if not archive.is_file() or sha256_file(archive) != CAPTURE_SOURCE_ARCHIVE_SHA256:
        raise ReplayError("frozen source archive is missing or has the wrong SHA-256")

    suite = json.loads(suite_path.read_text())
    tasks = validate_suite(suite)
    if suite.get("suite_id") != "m3-synthetic-v3":
        raise ReplayError("replay requires corrected m3-synthetic-v3")
    if sha256_file(V2_SUITE_PATH) != CAPTURE_SUITE_SHA256:
        raise ReplayError("frozen v2 suite is missing or has changed")
    v2_suite = json.loads(V2_SUITE_PATH.read_text())
    projected_v3 = copy.deepcopy(v2_suite)
    projected_v3["schema_version"] = "e4-m3-synthetic-suite/3"
    projected_v3["suite_id"] = "m3-synthetic-v3"
    median_task = next(task for task in projected_v3["tasks"]
                       if task["id"] == "operator_04_median_window_contrast")
    median_task["checks"][1]["expected"]["delta"] = 2.0
    if suite != projected_v3:
        raise ReplayError("v3 must differ from frozen v2 only by version/id and the corrected median oracle")
    old_oracle_audit = audit_operator_oracles(v2_suite)
    old_mismatches = old_oracle_audit["mismatches"]
    if (old_oracle_audit["operator_check_count"] != 19 or len(old_mismatches) != 1 or
            old_mismatches[0]["task_id"] != "operator_04_median_window_contrast" or
            old_mismatches[0]["case_index"] != 1 or
            old_mismatches[0]["independent_result"].get("delta") != 2 or
            old_mismatches[0]["stored_expected"].get("delta") != 1.0):
        raise ReplayError(f"frozen v2 oracle audit differs from reviewed discrepancy: {old_oracle_audit}")
    oracle_audit = audit_operator_oracles(suite)
    if oracle_audit["operator_check_count"] != 19 or oracle_audit["mismatch_count"] != 0:
        raise ReplayError(f"corrected operator oracle audit failed: {oracle_audit}")
    expected_task_ids = [task["id"] for task in tasks]
    if source_manifest.get("task_ids") != expected_task_ids or source_manifest.get("task_count") != 20:
        raise ReplayError("capture task order/count does not match corrected suite")

    captured_task_rows = _jsonl(capture_dir / "m3_task_results.jsonl")
    calls = _jsonl(capture_dir / "m3_model_calls.jsonl")
    if [row.get("task_id") for row in captured_task_rows] != expected_task_ids:
        raise ReplayError("captured task result order/count does not match corrected suite")
    if source_summary.get("task_count") != 20:
        raise ReplayError("capture summary is not a complete 20-task run")
    # run_task accepts the exact same task contract while the corrected file's
    # new suite ID/version remain recorded separately in replay provenance.
    validation_view = copy.deepcopy(suite)
    validation_view["schema_version"] = "e4-m3-synthetic-suite/2"
    validation_view["suite_id"] = "m3-synthetic-v2"
    tasks = validate_suite(validation_view)

    for task_id in expected_task_ids:
        rows_for_task = [row for row in calls if row.get("task_id") == task_id]
        if not rows_for_task or [row.get("attempt") for row in rows_for_task] != list(
                range(1, len(rows_for_task) + 1)):
            raise ReplayError(f"captured attempts are missing or out of order for {task_id}")
    grouped_order = []
    for row in calls:
        task_id = row.get("task_id")
        if not grouped_order or grouped_order[-1] != task_id:
            grouped_order.append(task_id)
    if grouped_order != expected_task_ids:
        raise ReplayError("captured model call order does not match task order")

    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite replay output: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir()
    client = TranscriptClient(calls, source_manifest["profile"], source_manifest["base_url"],
                              int(source_manifest["context_tokens"]),
                              int(source_manifest["max_generation_tokens"]))
    runtime = runtime_context()
    registry = telecom_registry()
    outcomes = []
    source_task_by_id = {row["task_id"]: row for row in captured_task_rows}
    task_results_path = output_dir / "m3_task_results.jsonl"
    replay_calls_path = output_dir / "m3_model_calls.jsonl"
    validation_started = time.perf_counter()
    with patch("urllib.request.urlopen", side_effect=AssertionError("offline replay attempted HTTP")):
        for task in tasks:
            outcome = run_task(task, registry=registry, runtime=runtime, client=client,
                               run_dir=output_dir)
            outcomes.append(outcome)
            source_task = source_task_by_id[task["id"]]
            result_row = outcome.as_dict()
            result_row["replay_validation_elapsed_seconds"] = result_row["elapsed_seconds"]
            for cost_key in ("elapsed_seconds", "input_tokens", "output_tokens"):
                result_row[cost_key] = source_task[cost_key]
            result_row["selection_cost_source"] = str(capture_dir / "m3_task_results.jsonl")
            with task_results_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(result_row, ensure_ascii=False, separators=(",", ":"),
                                        allow_nan=False) + "\n")
            with replay_calls_path.open("a", encoding="utf-8") as stream:
                for call_data in outcome.calls:
                    row = {"record_type": "model_call", **call_data}
                    stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"),
                                            allow_nan=False) + "\n")
                stream.flush()
    validation_wall = time.perf_counter() - validation_started
    client.require_consumed()
    if client.verified_prompts != len(calls) or client.verified_payloads != len(calls):
        raise ReplayError("not every captured prompt and request payload was verified")
    if _hash_tree(capture_dir) != source_hashes_before:
        raise ReplayError("source capture artifacts changed during replay")

    corrected_summary = summarize_outcomes(outcomes)
    corrected_summary["replay_validation_wall_seconds"] = validation_wall
    corrected_summary["elapsed_seconds"] = source_summary["elapsed_seconds"]
    corrected_summary["input_tokens"] = source_summary["input_tokens"]
    corrected_summary["output_tokens"] = source_summary["output_tokens"]
    corrected_summary["selection_costs"] = _selection_costs(source_summary, gpu, capture_dir)
    corrected_summary["selection_cost_source"] = "original model capture; replay validation wall excluded"
    original_categories = source_summary.get("per_category", {})
    for category, values in corrected_summary.get("per_category", {}).items():
        source_values = original_categories.get(category, {})
        for key in ("elapsed_seconds", "input_tokens", "output_tokens"):
            if key in source_values:
                values[key] = source_values[key]
        values["replay_validation_wall_seconds"] = sum(
            outcome.elapsed_seconds for outcome in outcomes if outcome.kind == category)
    corrected_summary.update({"suite_id": suite["suite_id"],
                              "suite_sha256": sha256_file(suite_path),
                              "checker_sha256": checker_sha256()[0],
                              "capture_run_id": capture_dir.name,
                              "finished_utc": datetime.now(timezone.utc).isoformat(),
                              "record_type": "m3-offline-replay-summary/1"})
    # Keep a byte-for-byte copy of the original calls as the replay transcript.
    # Reconstructed rows are independently emitted beside it for provenance.
    source_call_copy = output_dir / "source_m3_model_calls.jsonl"
    with source_call_copy.open("xb") as stream:
        stream.write((capture_dir / "m3_model_calls.jsonl").read_bytes())
    _write_json(output_dir / "m3_replay_summary.json", corrected_summary)
    replay_manifest = {
        "record_type": "m3-offline-replay-manifest/1",
        "capture_run_id": capture_dir.name,
        "capture_path": str(capture_dir),
        "capture_source_artifact_sha256": source_hashes_before,
        "capture_source_archive": str(archive),
        "capture_source_archive_sha256": CAPTURE_SOURCE_ARCHIVE_SHA256,
        "capture_suite_id": source_manifest["suite_id"],
        "capture_suite_sha256": source_manifest["suite_sha256"],
        "capture_checker_sha256": source_manifest["checker_sha256"],
        "corrected_suite_id": suite["suite_id"],
        "corrected_suite_path": str(suite_path),
        "corrected_suite_sha256": sha256_file(suite_path),
        "checker_sha256": corrected_summary["checker_sha256"],
        "replay_helper_sha256": _helper_hashes(),
        "task_count": len(tasks), "captured_call_count": len(calls),
        "verified_prompt_count": client.verified_prompts,
        "verified_request_payload_count": client.verified_payloads,
        "network_request_count": 0,
        "thread_environment": thread_environment,
        "operator_oracle_audit": oracle_audit,
        "v2_operator_oracle_audit": old_oracle_audit,
        "selection_costs": corrected_summary["selection_costs"],
        "replay_validation_wall_seconds": validation_wall,
        "source_artifact_hashes_after": _hash_tree(capture_dir),
        "task_results_file": str(task_results_path),
        "replayed_model_calls_file": str(replay_calls_path),
        "source_transcript_copy": str(source_call_copy),
    }
    _write_json(output_dir / "m3_replay_manifest.json", replay_manifest)
    return corrected_summary
