"""Label-blind first-development adapter run for model-generated E4 DAGs."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping

from e4.adapters.telecom import ROOT, load_case
from e4.synthetic.benchmark import REPAIR_LIMIT, StaticTaskError, parse_strict_json_object, sha256_file
from e4.dsl.cache import CacheStore, implementation_fingerprint
from e4.dsl.verifier import DAGValidationError, verify_dag
from e4.dsl.executor import SerialExecutor, execution_result_to_dict
from e4.models.analysis_model import AnalysisModel
from e4.models.client import ModelClient
from e4.dsl.runtime import build_case_runtime
from e4.operators.telecom.operators import telecom_registry


def select_primary_kpi(telemetry: Any, policy_path: Path | None = None) -> dict[str, Any]:
    """Choose a paired public KPI by the frozen support-first rule."""
    policy_path = policy_path or ROOT / "config/telecom/kpi_policy.json"
    policy = json.loads(policy_path.read_text())
    minimum = int(policy["minimum_support_per_window"])
    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    root_values: dict[str, dict[str, dict[str, list[float]]]] = {
        name: defaultdict(lambda: defaultdict(list)) for name in windows}
    root_status: dict[str, dict[str, dict[str, list[bool]]]] = {
        name: defaultdict(lambda: defaultdict(list)) for name in windows}
    for span in telemetry.spans:
        window_name = next((name for name, window in windows.items()
                            if window.contains(span.start_ms_utc)), None)
        if window_name is None or span.call_type.casefold() != "osb" or span.parent_id is not None:
            continue
        if span.logical_service is None:
            continue
        identifier = span.logical_service.identifier
        trace_id = span.trace_id
        try:
            value = float(span.elapsed_raw)
        except (TypeError, ValueError):
            value = float("nan")
        if span.success_status == "valid":
            root_status[window_name][identifier][trace_id].append(span.success is False)
        if value == value and abs(value) != float("inf"):
            root_values[window_name][identifier][trace_id].append(value)
    root_options = sorted(set(root_values["reference"]) & set(root_values["alert"]))
    root_options = [identifier for identifier in root_options
                    if min(sum(bool(root_values[name][identifier][trace])
                               for trace in root_values[name][identifier]) for name in windows) >= minimum]
    if root_options:
        identifier = min(root_options, key=lambda item: (-min(len(root_values[name][item]) for name in windows), item.casefold()))
        values_by_window = {name: [median(values) for _, values in sorted(root_values[name][identifier].items())]
                            for name in windows}
        return {"kpi_id": "root_osb_elapsedTime_mean", "entity": identifier,
                "selection_reason": "highest paired valid root OSB trace support; ties use canonical service ID",
                "aggregation": "per-trace root elapsedTime; mean/median/q90 reported separately",
                "units": "undeclared source elapsedTime units; within-field comparison only",
                "windows": {name: _summary(values_by_window[name]) for name in windows}}

    app_by_service: dict[str, dict[str, dict[int, list[float]]]] = {
        name: defaultdict(lambda: defaultdict(list)) for name in windows
    }
    app_request: dict[str, dict[str, dict[int, list[float]]]] = {
        name: defaultdict(lambda: defaultdict(list)) for name in windows
    }
    for point in telemetry.metrics:
        if point.table != "metric/metric_app.csv":
            continue
        window_name = next((name for name, window in windows.items()
                            if window.contains(point.timestamp_ms_utc)), None)
        if window_name is None or point.value is None:
            continue
        if point.metric_name == "avg_time":
            app_by_service[window_name][point.entity.identifier][point.timestamp_ms_utc].append(point.value)
        elif point.metric_name == "num":
            app_request[window_name][point.entity.identifier][point.timestamp_ms_utc].append(point.value)
    app_options = sorted(set(app_by_service["reference"]) & set(app_by_service["alert"]))
    app_options = [identifier for identifier in app_options
                   if min(len(app_by_service[name][identifier]) for name in windows) >= minimum]
    if app_options:
        identifier = min(app_options, key=lambda item: (-min(len(app_by_service[name][item]) for name in windows), item.casefold()))
        return {"kpi_id": "app_avg_time_mean", "entity": identifier,
                "selection_reason": "highest paired unique timestamp support; ties use canonical service ID",
                "aggregation": "median duplicate service/time rows, then mean/median/q90 over unique timestamps",
                "units": "undeclared source avg_time units; within-field comparison only",
                "median_request_count_by_window": {name: median([median(values)
                    for values in app_request[name][identifier].values()]) if app_request[name][identifier] else 0.0
                    for name in windows},
                "windows": {name: _summary([median(values) for _, values in sorted(app_by_service[name][identifier].items())])
                            for name in windows}}

    status_options = sorted(set(root_status["reference"]) & set(root_status["alert"]))
    status_options = [identifier for identifier in status_options
                      if min(len(root_status[name][identifier]) for name in windows) >= minimum]
    if status_options:
        identifier = min(status_options, key=lambda item: (-min(len(root_status[name][item]) for name in windows), item.casefold()))
        return {"kpi_id": "root_osb_failure_fraction", "entity": identifier,
                "selection_reason": "paired valid root OSB status support after latency options abstain",
                "aggregation": "failures divided by valid success/failure observations",
                "windows": {name: {"failures": sum(root_status[name][identifier]),
                                    "support": len(root_status[name][identifier]),
                                    "fraction": mean(mean(values) for values in root_status[name][identifier].values())}
                            for name in windows}}
    return {"kpi_id": None, "selection_reason": "no public KPI satisfied paired support",
            "minimum_support_per_window": minimum, "windows": {}}


def build_development_prompt(case_id: str, alert: Mapping[str, Any],
                             runtime: Any, kpi: Mapping[str, Any],
                             catalog: Mapping[str, Any], registry: Any) -> str:
    operators = registry.signatures()
    public_entities = [{"id": identifier, **dict(values)}
                       for identifier, values in sorted(catalog.get("entities", {}).items())]
    public_context = {
        "case_id": case_id,
        "alert_text": alert["alert_text"],
        "timezone": alert["timezone"],
        "reference_window_utc_ms": [alert["analysis_window_start_epoch_ms_utc"],
                                    alert["alert_window_start_epoch_ms_utc"]],
        "alert_window_utc_ms": [alert["alert_window_start_epoch_ms_utc"],
                                alert["alert_window_end_epoch_ms_utc"]],
        "selected_kpi": dict(kpi),
        "deployment_catalog": public_entities,
        "telemetry_summary": {"metric_tables": ["metric_app", "metric_container", "metric_middleware",
                                                  "metric_node", "metric_service"],
                              "trace_table": "trace_span", "trace_roles": ["executing component",
                                                                              "logical serviceName",
                                                                              "database dsName"],
                              "parent_identity": "(traceId,id); pid is local to the trace",
                              "timestamps": "UTC integer milliseconds; case windows are half-open"},
    }
    schema = json.loads((ROOT / "config/runtime/schema.json").read_text())
    seed_library = json.loads((ROOT / "config/telecom/seed_playbooks.json").read_text())
    input_schemas = {
        "metric_app": ["serviceName", "startTime", "avg_time", "num", "succee_num", "succee_rate"],
        "metric_container/middleware/node/service": ["itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id"],
        "trace_span": ["callType", "startTime", "elapsedTime", "success", "traceId", "id", "pid", "cmdb_id", "dsName", "serviceName"],
    }
    return ("Design a valid analysis DAG for this unlabeled Telecom development case using only the "
            "registered operators below. Do not guess a root cause or component. Return one strict JSON "
            "object exactly shaped as {schema_version: 'e4-dag/1', nodes: [{id, operator, version, "
            "inputs, params, bindings}], return: {node}}; each node has exactly those six fields. Nodes "
            "are ordered and references point only to earlier nodes. The return must be a RankedAnswer "
            "from answers.combine. No prose or code. Each metrics.aggregate and "
            "traces.aggregate node accepts exactly one window parameter (reference or alert) and returns "
            "one single-window Aggregate. Comparison scorers take explicit reference and alert aggregates. "
            "Use runtime bindings where useful; explicit parameter values override bindings. Share the "
            "candidate set across scorers. Every CandidateSet must feed at least one scorer directly, and "
            "the final answers.combine node must consume every terminal score branch. Do not output data "
            "or predictions in the DAG.\n\nREGISTERED OPERATORS\n" +
            json.dumps(operators, ensure_ascii=False, sort_keys=True) +
            "\n\nPUBLISHED RUNTIME SCHEMA\n" + json.dumps(schema, ensure_ascii=False, sort_keys=True) +
            "\n\nALLOWLISTED RUNTIME VALUES\n" + json.dumps(runtime.as_dict(), ensure_ascii=False, sort_keys=True) +
            "\n\nGENERIC SEED DAG STRUCTURES (examples only, no answer labels)\n" +
            json.dumps(seed_library["examples"], ensure_ascii=False, sort_keys=True) +
            "\n\nTELEMETRY INPUT COLUMNS\n" + json.dumps(input_schemas, ensure_ascii=False, sort_keys=True) +
            "\n\nPUBLIC CASE CONTEXT\n" + json.dumps(public_context, ensure_ascii=False, sort_keys=True))


def run_development_dag(case_id: str, split: str, *, model: AnalysisModel,
                        run_dir: Path | None = None, root: Path = ROOT) -> dict[str, Any]:
    run_dir = run_dir or Path(os.environ.get("E4_RUN_DIR", root / "artifacts/m3/dev"))
    run_dir.mkdir(parents=True, exist_ok=True)
    status_path = run_dir / "phase_status.json"
    wall_started = time.perf_counter()

    def status(phase: str, state: str, error: Mapping[str, Any] | None = None) -> None:
        payload = {"phase": phase, "status": state}
        if error is not None:
            payload["error"] = dict(error)
        status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                                          sort_keys=True, allow_nan=False) + "\n")

    reserved = ("dev_manifest.json", "model_calls.jsonl", "model_dag.json", "development_run.json")
    if any((run_dir / name).exists() for name in reserved):
        raise FileExistsError(f"development run artifacts already exist under {run_dir}")
    try:
        case_dir = root / "data/prepared/telecom/public/cases" / case_id
        alert_path = case_dir / "alert.json"
        catalog_path = root / "data/prepared/telecom/public/deployment_catalog.json"
        alert = json.loads(alert_path.read_text())
        catalog = json.loads(catalog_path.read_text())
        runtime = build_case_runtime(alert)
        registry = telecom_registry()
        status("input_load", "running")
        load_started = time.perf_counter()
        telemetry = load_case(case_id, split, root=root)
        load_seconds = time.perf_counter() - load_started
        kpi = select_primary_kpi(telemetry)
        prompt = build_development_prompt(case_id, alert, runtime, kpi, catalog, registry)
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
        profile = model.document
        telemetry_load = {"loader_seconds": load_seconds,
                          "file_bytes": sum(telemetry.file_bytes.values()),
                          "file_bytes_by_path": dict(telemetry.file_bytes),
                          "data_sha256": telemetry.data_sha256,
                          "file_sha256": dict(telemetry.file_sha256),
                          "cache_replay_policy": "same in-memory Telemetry object reused for cold/warm/disabled cache executions"}
        manifest = {"schema_version": "e4-m3-development-manifest/1", "case_id": case_id,
                    "split": split, "model_alias": profile.get("model_alias"),
                    "profile_sha256": sha256_file(model.path),
                    "alert_sha256": sha256_file(alert_path), "catalog_sha256": sha256_file(catalog_path),
                    "input_manifest_sha256": sha256_file(root / "config/telecom/input_manifest.json"),
                    "telemetry_load": telemetry_load, "selected_kpi": kpi,
                    "kpi_policy_sha256": sha256_file(root / "config/telecom/kpi_policy.json"),
                    "seed_library_sha256": sha256_file(ROOT / "config/telecom/seed_playbooks.json"),
                    "runtime_schema_sha256": sha256_file(ROOT / "config/runtime/schema.json"),
                    "development_source_sha256": sha256_file(ROOT / "src/e4/agent/telecom/develop.py"),
                    "model_client_source_sha256": sha256_file(ROOT / "src/e4/models/client.py"),
                    "runner_script_sha256": sha256_file(ROOT / "scripts/telecom/develop.py"),
                    "implementation_fingerprint": implementation_fingerprint(),
                    "runtime": runtime.as_dict(), "prompt_sha256": prompt_hash,
                    "started_utc": datetime.now(timezone.utc).isoformat(),
                    "maximum_static_repairs": REPAIR_LIMIT}
        manifest_path = run_dir / "dev_manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2,
                                            sort_keys=True, allow_nan=False) + "\n")
        calls_path = run_dir / "model_calls.jsonl"
        calls_path.write_text("")
        client = ModelClient(model)
        repair_prompt = prompt
        repairs = 0
        static_errors: list[dict[str, Any]] = []
        fenced = False
        document = None
        verified = None
        while True:
            status("model_call", "running")
            call = client.complete(repair_prompt, task_id=f"dev_{case_id}", attempt=repairs + 1)
            with calls_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(call.as_dict(), ensure_ascii=False,
                                        separators=(",", ":"), allow_nan=False) + "\n")
            if call.error:
                failure = dict(call.error)
                status("model_call", "failed", failure)
                raise RuntimeError("model request failed: " + json.dumps(failure, ensure_ascii=False))
            if call.response_metadata.get("finish_reason") in {"length", "max_tokens"}:
                failure = {"code": "truncated_output",
                           "message": "model DAG response was truncated; semantic retries are forbidden"}
                status("model_call", "failed", failure)
                raise RuntimeError(failure["message"])
            content = call.content or ""
            try:
                document, fenced = parse_strict_json_object(content)
                verified = verify_dag(document, registry, runtime,
                    max_nodes=int(runtime.snapshot.values["max_nodes"]),
                    max_depth=int(runtime.snapshot.values["dag_depth"]))
                break
            except (StaticTaskError, DAGValidationError) as exc:
                error = (exc.as_dict() if isinstance(exc, StaticTaskError) else
                         {"code": exc.code, "message": str(exc), "node_id": exc.node_id,
                          "details": dict(exc.details)})
                static_errors.append(error)
                if repairs >= REPAIR_LIMIT:
                    failure = {"code": "static_validation_failed",
                               "repairs": repairs, "errors": static_errors}
                    status("static_validation", "failed", failure)
                    raise RuntimeError(json.dumps(failure, ensure_ascii=False))
                repairs += 1
                repair_prompt = (prompt + "\n\nStatic JSON/DAG validation error; repair only the syntax, "
                    "schema, or graph issue. Do not use diagnostic outcomes or propose a diagnosis.\n" +
                    json.dumps(error, ensure_ascii=False, sort_keys=True) +
                    "\n\nPrevious final artifact:\n" + content +
                    "\n\nReturn the corrected JSON object only.")
        assert document is not None and verified is not None
        document_path = run_dir / "model_dag.json"
        document_path.write_text(json.dumps(document, ensure_ascii=False, indent=2,
            sort_keys=True, allow_nan=False) + "\n")
        manifest["static_repairs"] = repairs
        manifest["static_validation_errors"] = static_errors
        manifest["dag_sha256"] = sha256_file(document_path)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2,
                                            sort_keys=True, allow_nan=False) + "\n")
        status("execution", "running")
    except Exception as exc:
        previous = json.loads(status_path.read_text()) if status_path.is_file() else {}
        if previous.get("status") != "failed":
            failure = {"code": getattr(exc, "code", "development_run_failed"), "message": str(exc)}
            status("development", "failed", failure)
        raise

    cache = CacheStore(run_dir / "core_cache", enabled=True)
    executor = SerialExecutor(registry, cache)
    external_nodes = [node.node_id for node in verified.nodes if node.operator.external_input]
    if len(external_nodes) != 1:
        failure = {"code": "external_source_count", "message": "DAG must contain exactly one case.telemetry source"}
        status("execution", "failed", failure)
        raise RuntimeError(failure["message"])
    external_values = {external_nodes[0]: telemetry}
    try:
        started = time.perf_counter()
        first = executor.execute_verified(verified, runtime, external_values, cache_enabled=True)
        first_seconds = time.perf_counter() - started
        started = time.perf_counter()
        warm = executor.execute_verified(verified, runtime, external_values, cache_enabled=True)
        warm_seconds = time.perf_counter() - started
        started = time.perf_counter()
        fresh = executor.execute_verified(verified, runtime, external_values, cache_enabled=False)
        fresh_seconds = time.perf_counter() - started
        if first.answer != warm.answer or first.answer != fresh.answer:
            raise RuntimeError("cached/warm/fresh executions produced different answers")
    except Exception as exc:
        failure = {"code": "execution_failed", "message": str(exc)}
        status("execution", "failed", failure)
        raise
    record = {
        "schema_version": "e4-m3-development-run/1", "case_id": case_id, "split": split,
        "model_alias": profile.get("model_alias"), "model_call_id": call.call_id,
        "prompt_sha256": prompt_hash, "model_content_sha256": hashlib.sha256((call.content or "").encode()).hexdigest(),
        "fenced_json_normalized": fenced, "dag_sha256": hashlib.sha256(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "runtime": runtime.as_dict(), "selected_kpi": kpi,
        "telemetry_load": telemetry_load,
        "wall_seconds_including_load_and_model_call": time.perf_counter() - wall_started,
        "model_call_seconds": call.elapsed_seconds,
        "loader_seconds": load_seconds,
        "execution_seconds_cache_cold": first_seconds,
        "execution_seconds_cache_warm": warm_seconds,
        "execution_seconds_cache_disabled": fresh_seconds,
        "cache_hits_cold": first.cache_hits, "cache_hits_warm": warm.cache_hits,
        "cache_hits_disabled": fresh.cache_hits,
        "cold_execution": execution_result_to_dict(first),
        "warm_execution": execution_result_to_dict(warm),
        "cache_disabled_execution": execution_result_to_dict(fresh),
        "implementation_fingerprint": implementation_fingerprint(),
    }
    (run_dir / "development_run.json").write_text(json.dumps(record, ensure_ascii=False,
        indent=2, sort_keys=True, allow_nan=False) + "\n")
    status("complete", "passed")
    return record


def _summary(values: list[float]) -> dict[str, Any]:
    import numpy as np
    return {"support": len(values), "mean": mean(values), "median": median(values),
            "q90": float(np.quantile(values, 0.9, method="linear")),
            "min": min(values), "max": max(values)}


def replay_saved_development_dag(case_id: str, split: str, dag_path: Path, *,
                                run_dir: Path, root: Path = ROOT,
                                verify_cache_parity: bool = False) -> dict[str, Any]:
    """Replay a saved model DAG without model calls; parity is opt-in."""
    run_dir.mkdir(parents=True, exist_ok=True)
    alert_path = root / "data/prepared/telecom/public/cases" / case_id / "alert.json"
    alert = json.loads(alert_path.read_text())
    runtime = build_case_runtime(alert)
    registry = telecom_registry()
    document = json.loads(dag_path.read_text())
    verified = verify_dag(document, registry, runtime,
        max_nodes=int(runtime.snapshot.values["max_nodes"]),
        max_depth=int(runtime.snapshot.values["dag_depth"]))
    load_started = time.perf_counter()
    telemetry = load_case(case_id, split, root=root)
    load_seconds = time.perf_counter() - load_started
    external_nodes = [node.node_id for node in verified.nodes if node.operator.external_input]
    if len(external_nodes) != 1:
        raise ValueError("saved DAG must contain exactly one telemetry external source")
    external = {external_nodes[0]: telemetry}
    executor = SerialExecutor(registry, CacheStore(run_dir / "core_cache", enabled=True))
    results, timings = [], []
    modes = (True, True, False) if verify_cache_parity else (True,)
    for enabled in modes:
        started = time.perf_counter()
        result = executor.execute_verified(verified, runtime, external, cache_enabled=enabled)
        timings.append(time.perf_counter() - started)
        results.append(result)
    if verify_cache_parity and any(result.answer != results[0].answer for result in results[1:]):
        raise RuntimeError("saved-DAG cache replays disagree")
    record = {"schema_version": "e4-m3-development-replay/1", "case_id": case_id,
              "split": split, "dag_path": str(dag_path.resolve()),
              "dag_sha256": sha256_file(dag_path), "data_sha256": telemetry.data_sha256,
              "file_sha256": dict(telemetry.file_sha256), "file_bytes": sum(telemetry.file_bytes.values()),
              "telemetry_load_seconds": load_seconds,
              "loader_seconds": load_seconds,
              "cache_parity_requested": verify_cache_parity,
              "cache_parity_passed": (all(result.answer == results[0].answer for result in results[1:])
                                       if verify_cache_parity else None),
              "execution_seconds": timings,
              "cache_hits": [result.cache_hits for result in results],
              "executions": [execution_result_to_dict(result) for result in results],
              "implementation_fingerprint": implementation_fingerprint()}
    (run_dir / "development_replay.json").write_text(json.dumps(record, ensure_ascii=False,
        indent=2, sort_keys=True, allow_nan=False) + "\n")
    return record
