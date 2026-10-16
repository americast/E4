#!/usr/bin/env python3
"""Run one label-blind Bank development DAG and verify cache replay parity."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
from urllib.parse import urlsplit
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.bank_adapter import BankAdapterError, BankCase, load_bank_case, sha256_file
from e4core.bank_ops import bank_registry
from e4core.benchmark import REPAIR_LIMIT, StaticTaskError, parse_strict_json_object
from e4core.cache import CacheStore, implementation_fingerprint
from e4core.dag import DAGValidationError, VerifiedDAG, verify_dag
from e4core.executor import (ExecutionFailure, SerialExecutor, execution_result_to_dict)
from e4core.model_client import LocalModelClient, ModelCall
from e4core.numerics import percentile
from e4core.runtime import RuntimeContext, build_case_runtime


BANK_SYSTEM_PROMPT = (
    "Analyze the supplied public Bank telemetry using the registered E4 operators. "
    "Return only the requested final artifact. Use no external tools, private labels, "
    "or unstated data; preserve exact source identifiers and units."
)


class BankDevelopmentError(RuntimeError):
    pass


class BankModelClient(LocalModelClient):
    """Local chat client with Bank system wording and unchanged frozen request controls."""

    def make_payload(self, prompt: str, *, max_tokens: int | None = None) -> dict[str, Any]:
        payload = super().make_payload(prompt, max_tokens=max_tokens)
        payload["messages"][0]["content"] = BANK_SYSTEM_PROMPT
        return payload


def _json_write_exclusive(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True,
                                allow_nan=False) + "\n")


def _json_write_replace(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True,
                               allow_nan=False) + "\n", encoding="utf-8")


def _status(path: Path, phase: str, state: str,
            error: Mapping[str, Any] | None = None) -> None:
    result: dict[str, Any] = {"phase": phase, "status": state,
                              "updated_utc": datetime.now(timezone.utc).isoformat()}
    if error is not None:
        result["error"] = dict(error)
    _json_write_replace(path, result)


def select_primary_bank_kpi(telemetry: Any, policy_path: Path) -> dict[str, Any]:
    """Support-first label-blind selection over an explicit context-only KPI allowlist."""
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise BankDevelopmentError("unsupported Bank development KPI policy")
    minimum = int(policy["minimum_support_per_window"])
    allowed = set(policy.get("primary_metric_allowlist", ()))
    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    buckets: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    for point in telemetry.metrics:
        policy_key = f"{point.table.rsplit('/', 1)[-1].removesuffix('.csv')}.{point.metric_name}"
        if policy_key not in allowed or point.value is None or not math.isfinite(point.value):
            continue
        if point.entity.role != "app_kpi":
            continue
        window_name = next((name for name, window in windows.items()
                            if window.contains(point.timestamp_ms_utc)), None)
        if window_name is not None:
            buckets[(window_name, point.metric_name, point.entity.identifier,
                     point.timestamp_ms_utc)].append(float(point.value))
    grouped: dict[tuple[str, str, str], list[tuple[int, float]]] = defaultdict(list)
    for (window_name, field, tc, timestamp), values in buckets.items():
        grouped[(window_name, field, tc)].append((timestamp, float(median(values))))
    candidates = []
    for field in sorted(allowed):
        source, metric_name = field.split(".", 1)
        if source != "metric_app":
            continue
        identifiers = sorted({tc for window_name, name, tc in grouped
                              if name == metric_name and window_name == "reference"} &
                             {tc for window_name, name, tc in grouped
                              if name == metric_name and window_name == "alert"},
                             key=lambda value: (value.casefold(), value))
        for tc in identifiers:
            before = sorted(value for _, value in grouped[("reference", metric_name, tc)])
            after = sorted(value for _, value in grouped[("alert", metric_name, tc)])
            if min(len(before), len(after)) < minimum:
                continue
            candidates.append({"kpi_id": field, "field": metric_name, "tc_id": tc,
                "role": "app_kpi_context_only", "reference_support": len(before),
                "alert_support": len(after), "paired_support": min(len(before), len(after)),
                "reference": {"median": float(median(before)), "mean": float(mean(before)),
                              "q90": float(percentile(before, 0.9))},
                "alert": {"median": float(median(after)), "mean": float(mean(after)),
                          "q90": float(percentile(after, 0.9))},
                "unit": "undeclared", "component_mapping": "none",
                "interpretation": "mean-response-time-style working interpretation from mrt field name only"})
    candidates.sort(key=lambda item: (-item["paired_support"], item["tc_id"].casefold(),
                                      item["tc_id"], item["kpi_id"]))
    if not candidates:
        return {"kpi_id": None, "selection_reason":
                "no allowlisted app KPI/tc pair met the frozen paired timestamp support rule; no resource fallback",
                "minimum_support_per_window": minimum,
                "policy_sha256": sha256_file(policy_path),
                "source_policy": policy.get("selection_provenance")}
    selected = candidates[0]
    return {**selected,
            "selection_reason": policy["selection"],
            "minimum_support_per_window": minimum,
            "policy_sha256": sha256_file(policy_path),
            "source_policy": policy.get("selection_provenance")}


def build_bank_development_prompt(case: BankCase, runtime: RuntimeContext,
                                  kpi: Mapping[str, Any], registry: Any,
                                  *, root: Path = ROOT) -> str:
    telemetry = case.telemetry
    schema = json.loads((root / "config/runtime_schema.json").read_text(encoding="utf-8"))
    component_ids = sorted(telemetry.deployment_catalog,
                           key=lambda value: (value.casefold(), value))
    official = list(case.deployment_catalog.get("official_candidate_components", []))
    metric_policy = json.loads((root / "config/bank_metric_kind_policy.json").read_text(encoding="utf-8"))
    kpi_policy = json.loads((root / "config/bank_dev_kpi_policy.json").read_text(encoding="utf-8"))
    seed_library = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    seed_examples = materialize_bank_seed_examples(seed_library, registry)
    metric_allowlist = sorted(metric_policy["source_semantics"]["metric_container"]["ranking_allowlist"])
    metric_fields = sorted({point.metric_name for point in telemetry.metrics
                            if point.table in {"bank/metric_app.csv", "bank/metric_container.csv"}})
    public_context = {
        "incident_id": case.telemetry.case_id,
        "alert_text": case.alert["alert_text"],
        "timezone": case.alert["timezone"],
        "reference_window_utc_ms": [telemetry.reference_window.start_ms_utc,
                                    telemetry.reference_window.end_ms_utc],
        "alert_window_utc_ms": [telemetry.alert_window.start_ms_utc,
                                 telemetry.alert_window.end_ms_utc],
        "selected_app_context_kpi": dict(kpi),
        "fixed_primary_kpi_rule": {"allowlist": kpi_policy.get("primary_metric_allowlist", []),
                                   "selection": kpi_policy.get("selection"),
                                   "unit_policy": kpi_policy.get("unit_policy")},
        "explicitly_rankable_container_kpi_names": metric_allowlist,
        "exact_observed_component_ids": component_ids,
        "official_candidate_names_without_alias_mapping": official,
        "component_identity_coverage": dict(case.provenance["component_identity_coverage"]),
        "telemetry_quality_counts": dict(telemetry.quality_counts),
        "observed_metric_field_names": metric_fields,
        "log_volume_summary": dict(case.log_summary),
    }
    prompt = (
        "Design a valid analysis DAG for this label-blind Bank development case. Use only the registered "
        "Bank operators and exact observed identifiers. Return one strict JSON object exactly shaped as "
        "{schema_version:'e4-dag/1', nodes:[{id,operator,version,inputs,params,bindings}], return:{node}}; "
        "each node has exactly those six fields. Nodes are ordered and references point only to earlier "
        "nodes. The return must be a RankedAnswer from bank.answers.combine. No prose, code, root-cause "
        "text, or category prediction. Use exact cmdb_id strings; do not alias docker IDs to named pods "
        "or invent vertical host/service/database relationships. `tc` is an opaque app KPI identity, not a "
        "component mapping. Bank trace spans have no success or call-type field. Duration units are "
        "undeclared; compare only the same duration field without converting units. Log counts are raw "
        "records per one-minute bucket; zero means no rows recorded in the provided clip, not healthy "
        "logging. Raw log value text is retained in the input artifact and is not parsed into error/severity "
        "labels. Container KPI scoring is restricted to explicitly listed names and two-sided same-field "
        "contrasts. App `mrt` is provided only as selected context and is not ranked as a component."
        "\n\nREGISTERED BANK OPERATORS\n" +
        json.dumps(registry.signatures(), ensure_ascii=False, sort_keys=True) +
        "\n\nPUBLISHED RUNTIME SCHEMA\n" + json.dumps(schema, ensure_ascii=False, sort_keys=True) +
        "\n\nALLOWLISTED RUNTIME VALUES\n" + json.dumps(runtime.as_dict(), ensure_ascii=False, sort_keys=True) +
        "\n\nBANK CSV INPUT SCHEMAS\n" + json.dumps({
            "metric/metric_app.csv": ["timestamp", "rr", "sr", "cnt", "mrt", "tc"],
            "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
            "trace/trace_span.csv": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
            "log/log_service.csv": ["log_id", "timestamp", "cmdb_id", "log_name", "value"]},
            ensure_ascii=False, sort_keys=True) +
        "\n\nFIXED BANK STRUCTURAL EXAMPLES (no targets or outcomes)\n" +
        json.dumps(seed_examples, ensure_ascii=False, sort_keys=True) +
        "\n\nSOURCE-DATE TIME UNITS\n" + json.dumps(case.alert["source_date_timestamp_units"],
                                                         ensure_ascii=False, sort_keys=True) +
        "\n\nPUBLIC CASE CONTEXT\n" + json.dumps(public_context, ensure_ascii=False,
                                                         sort_keys=True, allow_nan=False))
    return prompt


def materialize_bank_seed_examples(seed_library: Mapping[str, Any], registry: Any) -> dict[str, Any]:
    """Resolve policy-fingerprinted operator versions in the public seed examples."""
    versions = {item["name"]: item["version"] for item in registry.signatures()}
    examples = seed_library.get("examples")
    if not isinstance(examples, Mapping):
        raise BankDevelopmentError("Bank seed library examples must be an object")
    materialized: dict[str, Any] = {}
    for name, example in examples.items():
        value = json.loads(json.dumps(example))
        for node in value.get("nodes", []):
            operator_name = node.get("operator")
            if operator_name not in versions:
                raise BankDevelopmentError(f"Bank seed {name} uses unknown operator {operator_name}")
            node["version"] = versions[operator_name]
        materialized[str(name)] = value
    return materialized


def _validate_bank_graph(document: Mapping[str, Any], verified: VerifiedDAG,
                         registry: Any) -> None:
    sources = [node for node in verified.nodes if node.operator.external_input]
    candidates = [node for node in verified.nodes if node.operator.name == "bank.candidates.generate"]
    rankers = [node for node in verified.nodes if node.operator.name == "bank.answers.combine"]
    scorers = [node for node in verified.nodes if node.operator.output_type == "ScoredCandidates"]
    if len(sources) != 1 or sources[0].operator.name != "case.telemetry":
        raise StaticTaskError("bank_graph_source", "Bank DAG must have exactly one case.telemetry source")
    if len(candidates) != 1 or len(rankers) != 1:
        raise StaticTaskError("bank_graph_shared_nodes", "Bank DAG must share one candidate generator and final ranker")
    scorer_ids = {node.node_id for node in scorers}
    rank_inputs = set(rankers[0].inputs.get("scores", ()))
    if rank_inputs != scorer_ids:
        raise StaticTaskError("bank_graph_terminal_scores", "final Bank ranker must consume every terminal scorer exactly once",
                              {"expected": sorted(scorer_ids), "actual": sorted(rank_inputs)})
    candidate_id = candidates[0].node_id
    for node in scorers:
        if candidate_id not in node.inputs.get("candidates", ()):
            raise StaticTaskError("bank_graph_candidate_sharing",
                                  f"scorer {node.node_id} must consume the shared candidate set")


def _task_prompt_with_repair(base_prompt: str, previous: str,
                             error: Mapping[str, Any]) -> str:
    # Static schema/graph diagnostics only. Runtime/scoring outcomes are never
    # included in a model repair prompt.
    diagnostic = {key: error[key] for key in ("code", "message", "node_id") if key in error}
    return (base_prompt + "\n\nStatic JSON/DAG validation error; repair only the syntax, schema, or graph issue. "
            "No diagnostic execution outcomes are available for repair.\n" +
            json.dumps(diagnostic, ensure_ascii=False, sort_keys=True) +
            "\n\nPrevious final artifact:\n" + previous +
            "\n\nReturn the corrected JSON object only.")


def run_bank_development(case_id: str, *, profile_path: Path = ROOT / "config/gemma_m3_inference.json",
                         run_dir: Path | None = None, root: Path = ROOT,
                         client: BankModelClient | None = None) -> dict[str, Any]:
    profile_path = profile_path.resolve(strict=True)
    if not profile_path.is_relative_to(root.resolve()):
        raise BankDevelopmentError("Bank inference profile must resolve inside the project root")
    configured_profile = os.environ.get("E4_MODEL_PROFILE")
    if not configured_profile:
        raise BankDevelopmentError("E4_MODEL_PROFILE from the approved model launcher is required")
    if Path(configured_profile).resolve(strict=True) != profile_path:
        raise BankDevelopmentError("Bank inference profile must match E4_MODEL_PROFILE from the approved model launcher")
    base_url = os.environ.get("E4_BASE_URL")
    parsed_base_url = urlsplit(base_url or "")
    if (parsed_base_url.scheme not in {"http", "https"} or
            parsed_base_url.hostname not in {"localhost", "127.0.0.1", "::1"} or
            parsed_base_url.port is None):
        raise BankDevelopmentError("E4_BASE_URL from the approved model launcher must use a local endpoint")
    configured = os.environ.get("E4_RUN_DIR")
    if not configured:
        raise BankDevelopmentError("E4_RUN_DIR from the approved model launcher is required")
    launcher_run_dir = Path(configured).resolve(strict=True)
    if run_dir is None:
        run_dir = launcher_run_dir / "development" / case_id
    elif not run_dir.is_absolute():
        run_dir = launcher_run_dir / run_dir
    run_dir = run_dir.resolve()
    if run_dir == launcher_run_dir or not run_dir.is_relative_to(launcher_run_dir):
        raise BankDevelopmentError("Bank run artifacts must stay inside a child directory of E4_RUN_DIR")
    sample_path = root / "config/bank_sample.json"
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    dev_ids = {item["incident_id"] for item in sample.get("development_cases", [])}
    if case_id not in dev_ids:
        raise BankDevelopmentError("only frozen Bank development cases may be run")
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    # The launcher owns the already-existing outer run directory. Give this
    # development task a fresh, isolated child beneath its sole writable mount.
    run_dir.mkdir(exist_ok=False)
    reserved = ("phase_status.json", "model_calls.jsonl", "dev_manifest.json",
                "model_dag.json", "development_run.json")
    if any((run_dir / name).exists() for name in reserved):
        raise FileExistsError(f"Bank development artifacts already exist under {run_dir}")
    status_path = run_dir / "phase_status.json"
    calls_path = run_dir / "model_calls.jsonl"
    manifest_path = run_dir / "dev_manifest.json"
    start = time.perf_counter()
    _status(status_path, "input_load", "running")

    try:
        loader_started = time.perf_counter()
        case = load_bank_case(case_id, "dev", root=root)
        loader_seconds = time.perf_counter() - loader_started
        telemetry = case.telemetry
        runtime = build_case_runtime(case.alert,
            schema_path=ROOT / "config/runtime_schema.json",
            config_path=ROOT / "config/core_runtime.json")
        registry = bank_registry(root=root)
        kpi = select_primary_bank_kpi(telemetry, root / "config/bank_dev_kpi_policy.json")
        bank_seeds = materialize_bank_seed_examples(
            json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8")), registry)
        prompt = build_bank_development_prompt(case, runtime, kpi, registry, root=root)
        prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        model = client or BankModelClient(profile, base_url=base_url)
        manifest = {
            "schema_version": "e4-bank-development-manifest/1",
            "run_id": run_dir.name, "case_id": case_id, "split": "dev",
            "model_alias": profile.get("model_alias"), "profile_path": str(profile_path),
            "profile_sha256": sha256_file(profile_path), "profile": profile,
            "request_endpoint": model.base_url, "context_tokens": model.context_tokens,
            "max_generation_tokens": model.max_generation_tokens,
            "bank_input_provenance": dict(case.provenance),
            "data_sha256": telemetry.data_sha256,
            "file_sha256": dict(telemetry.file_sha256),
            "file_bytes": dict(telemetry.file_bytes),
            "metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
            "dev_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
            "bank_sample_sha256": sha256_file(sample_path),
            "runtime_schema_sha256": sha256_file(ROOT / "config/runtime_schema.json"),
            "core_runtime_sha256": sha256_file(ROOT / "config/core_runtime.json"),
            "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json"),
            "bank_seed_examples_presented": sorted(bank_seeds),
            "adapter_source_sha256": sha256_file(ROOT / "e4core/bank_adapter.py"),
            "operator_source_sha256": sha256_file(ROOT / "e4core/bank_ops.py"),
            "runner_source_sha256": sha256_file(Path(__file__).resolve()),
            "model_client_source_sha256": sha256_file(ROOT / "e4core/model_client.py"),
            "implementation_fingerprint": implementation_fingerprint(),
            "runtime": runtime.as_dict(), "selected_primary_kpi": kpi,
            "component_identity_coverage": dict(case.provenance["component_identity_coverage"]),
            "log_summary": dict(case.log_summary),
            "prompt_sha256": prompt_sha, "prompt_characters": len(prompt),
            "system_prompt_sha256": hashlib.sha256(BANK_SYSTEM_PROMPT.encode()).hexdigest(),
            "static_repair_limit": REPAIR_LIMIT,
            "repair_policy": "static JSON/DAG syntax and schema only; no execution/scoring feedback",
            "started_utc": datetime.now(timezone.utc).isoformat(),
        }
        _json_write_exclusive(manifest_path, manifest)
        with calls_path.open("x", encoding="utf-8"):
            pass
        _status(status_path, "model_call", "running")
        try:
            document = None
            verified = None
            repaired_prompt = prompt
            previous = ""
            repair_count = 0
            static_errors: list[Mapping[str, Any]] = []
            while True:
                call = model.complete(repaired_prompt, task_id=f"bank_dev_{case_id}",
                                      attempt=repair_count + 1)
                with calls_path.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(call.as_dict(), ensure_ascii=False,
                                            separators=(",", ":"), allow_nan=False) + "\n")
                    stream.flush()
                if call.error:
                    raise BankDevelopmentError(f"Bank model request failed: {call.error}")
                if call.response_metadata.get("finish_reason") in {"length", "max_tokens"}:
                    raise BankDevelopmentError("Bank DAG output was truncated; no semantic repair is permitted")
                previous = call.content or ""
                try:
                    document, _fenced = parse_strict_json_object(previous)
                    verified = verify_dag(document, registry, runtime,
                        max_nodes=int(runtime.snapshot.values["max_nodes"]),
                        max_depth=int(runtime.snapshot.values["dag_depth"]))
                    _validate_bank_graph(document, verified, registry)
                    break
                except (StaticTaskError, DAGValidationError) as exc:
                    error = (exc.as_dict() if isinstance(exc, StaticTaskError) else exc.as_dict())
                    static_errors.append(error)
                    if repair_count >= REPAIR_LIMIT:
                        raise BankDevelopmentError(json.dumps({"code": "static_validation_failed",
                            "errors": static_errors, "repairs": repair_count}, sort_keys=True))
                    repair_count += 1
                    repaired_prompt = _task_prompt_with_repair(prompt, previous, error)
            assert document is not None and verified is not None
            dag_path = run_dir / "model_dag.json"
            _json_write_exclusive(dag_path, document)
            _status(status_path, "execution", "running")
            source_ids = [node.node_id for node in verified.nodes if node.operator.external_input]
            external = {source_ids[0]: telemetry}
            executor = SerialExecutor(registry, CacheStore(run_dir / "core_cache", enabled=True))
            executions = []
            timings = []
            for enabled in (True, True, False):
                tick = time.perf_counter()
                result = executor.execute_verified(verified, runtime, external,
                                                   cache_enabled=enabled)
                timings.append(time.perf_counter() - tick)
                executions.append(result)
            answers_equal = all(result.answer == executions[0].answer for result in executions[1:])
            digests_equal = all(
                [record.output_digest for record in result.records] ==
                [record.output_digest for record in executions[0].records]
                for result in executions[1:])
            if not answers_equal or not digests_equal:
                raise BankDevelopmentError("cold/warm/cache-disabled Bank DAG outputs differ")
            result_record = {
                "schema_version": "e4-bank-development-run/1",
                "case_id": case_id, "split": "dev", "model_alias": profile.get("model_alias"),
                "model_call_ids": [row["call_id"] for row in _read_call_rows(calls_path)],
                "prompt_sha256": prompt_sha,
                "model_dag_sha256": sha256_file(dag_path),
                "exact_seed_example_matches": sorted(name for name, seed in bank_seeds.items()
                    if json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False) ==
                       json.dumps(seed, sort_keys=True, separators=(",", ":"), allow_nan=False)),
                "static_repairs": repair_count, "static_validation_errors": static_errors,
                "model_call_seconds": sum(row["elapsed_seconds"] for row in _read_call_rows(calls_path)),
                "replay_wall_seconds": time.perf_counter() - start,
                "loader_seconds": loader_seconds,
                "cache_enabled_execution_seconds": timings[0],
                "cache_warm_execution_seconds": timings[1],
                "cache_disabled_execution_seconds": timings[2],
                "cache_hits": [result.cache_hits for result in executions],
                "cache_parity_passed": True,
                "execution_records": [execution_result_to_dict(result) for result in executions],
                "implementation_fingerprint": implementation_fingerprint(),
                "telemetry_data_sha256": telemetry.data_sha256,
                "bank_input_provenance": dict(case.provenance),
                "cache_identity_policy": "Bank metric-policy/KPI-policy/input-manifest hashes are included in Telemetry data_sha256 and adapter_version",
            }
            _json_write_exclusive(run_dir / "development_run.json", result_record)
            _status(status_path, "complete", "passed")
            return {"manifest": manifest, "development_run": result_record,
                    "run_dir": str(run_dir)}
        except Exception as exc:
            error = {"code": getattr(exc, "code", "bank_development_failed"),
                     "message": str(exc)}
            if not (run_dir / "development_run.json").exists():
                _json_write_exclusive(run_dir / "development_run.json", {
                    "schema_version": "e4-bank-development-run/1", "case_id": case_id,
                    "status": "failed", "error": error,
                    "elapsed_seconds": time.perf_counter() - start,
                    "static_repairs": locals().get("repair_count", 0),
                    "static_validation_errors": locals().get("static_errors", [])})
            _status(status_path, "development", "failed", error)
            raise
    except Exception as exc:
        if not manifest_path.exists():
            _json_write_exclusive(manifest_path, {"schema_version": "e4-bank-development-manifest/1",
                "run_id": run_dir.name, "case_id": case_id, "split": "dev",
                "status": "failed_before_prompt", "error": str(exc)})
        raise


def _read_call_rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _fresh_child_run_dir(*, case_id: str, leaf: str, run_dir: Path | None,
                         root: Path) -> tuple[Path, Path]:
    configured = os.environ.get("E4_RUN_DIR")
    if not configured:
        raise BankDevelopmentError("E4_RUN_DIR is required to keep Bank artifacts under the launcher run mount")
    outer = Path(configured).resolve(strict=True)
    target = outer / "development" / case_id / leaf if run_dir is None else run_dir
    if not target.is_absolute():
        target = outer / target
    target = target.resolve()
    if target == outer or not target.is_relative_to(outer):
        raise BankDevelopmentError("Bank artifacts must remain inside a child directory of E4_RUN_DIR")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir(exist_ok=False)
    return target, outer


def _execute_for_parity(verified: VerifiedDAG, runtime: RuntimeContext,
                        telemetry: Any, cache_dir: Path, *, root: Path = ROOT) -> dict[str, Any]:
    source_id = next(node.node_id for node in verified.nodes if node.operator.external_input)
    external = {source_id: telemetry}
    executor = SerialExecutor(bank_registry(root=root), CacheStore(cache_dir, enabled=True))
    executions = []
    durations = []
    for enabled in (True, True, False):
        tick = time.perf_counter()
        executions.append(executor.execute_verified(verified, runtime, external,
                           cache_enabled=enabled))
        durations.append(time.perf_counter() - tick)
    answer_parity = all(result.answer == executions[0].answer for result in executions[1:])
    digest_parity = all(
        [record.output_digest for record in result.records] ==
        [record.output_digest for record in executions[0].records]
        for result in executions[1:])
    return {"cache_parity_passed": answer_parity and digest_parity,
            "answer_parity_passed": answer_parity,
            "output_digest_parity_passed": digest_parity,
            "cache_hits": [result.cache_hits for result in executions],
            "execution_seconds": durations,
            "execution_records": [execution_result_to_dict(result) for result in executions]}


def run_bank_controls(case_id: str, *, run_dir: Path | None = None,
                      root: Path = ROOT) -> dict[str, Any]:
    """CPU-only cache parity runs over every frozen Bank structural seed DAG."""
    target, _outer = _fresh_child_run_dir(case_id=case_id, leaf="controls",
                                          run_dir=run_dir, root=root)
    manifest_path = target / "controls_manifest.json"
    _json_write_exclusive(manifest_path, {"schema_version": "e4-bank-controls/1",
        "case_id": case_id, "split": "dev", "mode": "CPU-only fixed structural controls",
        "model_requests": 0, "model_profile_used": False,
        "status": "running", "started_utc": datetime.now(timezone.utc).isoformat()})
    try:
        case = load_bank_case(case_id, "dev", root=root)
        runtime = build_case_runtime(case.alert,
            schema_path=ROOT / "config/runtime_schema.json",
            config_path=ROOT / "config/core_runtime.json")
        registry = bank_registry(root=root)
        seed_path = root / "config/bank_seed_library.json"
        seed_library = json.loads(seed_path.read_text(encoding="utf-8"))
        examples = materialize_bank_seed_examples(seed_library, registry)
        runs = []
        for name, document in examples.items():
            verified = verify_dag(document, registry, runtime,
                max_nodes=int(runtime.snapshot.values["max_nodes"]),
                max_depth=int(runtime.snapshot.values["dag_depth"]))
            _validate_bank_graph(document, verified, registry)
            dag_path = target / f"{name}.dag.json"
            _json_write_exclusive(dag_path, document)
            parity = _execute_for_parity(verified, runtime, case.telemetry,
                                         target / "cache" / name, root=root)
            row = {"name": name, "dag_sha256": sha256_file(dag_path),
                   "exact_bank_seed_example": True,
                   "verified_node_count": verified.measured_node_count,
                   "verified_max_depth": verified.max_depth,
                   "telemetry_data_sha256": case.telemetry.data_sha256,
                   **parity}
            _json_write_exclusive(target / f"{name}.result.json", row)
            runs.append(row)
        passed = len(runs) == len(examples) and all(row["cache_parity_passed"] for row in runs)
        manifest = {"schema_version": "e4-bank-controls/1", "case_id": case_id,
            "split": "dev", "mode": "CPU-only fixed structural controls",
            "model_requests": 0, "model_profile_used": False,
            "bank_input_provenance": dict(case.provenance),
            "bank_seed_library_sha256": sha256_file(seed_path),
            "metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
            "dev_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
            "runs": runs, "status": "passed" if passed else "failed",
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _json_write_replace(manifest_path, manifest)
        if not passed:
            raise BankDevelopmentError("one or more fixed Bank seed DAGs failed cache parity")
        return {"manifest": manifest, "run_dir": str(target)}
    except Exception as exc:
        _json_write_replace(manifest_path, {"schema_version": "e4-bank-controls/1",
            "case_id": case_id, "split": "dev", "mode": "CPU-only fixed structural controls",
            "model_requests": 0, "status": "failed", "error": str(exc),
            "finished_utc": datetime.now(timezone.utc).isoformat()})
        raise


def replay_bank_development(case_id: str, dag_path: Path, *, run_dir: Path | None = None,
                            root: Path = ROOT) -> dict[str, Any]:
    """Replay one saved Bank DAG on a frozen public development case with no HTTP."""
    target, _outer = _fresh_child_run_dir(case_id=case_id, leaf="saved_dag_replay",
                                          run_dir=run_dir, root=root)
    source_path = dag_path.resolve(strict=True)
    if not source_path.is_relative_to(root.resolve()):
        raise BankDevelopmentError("saved DAG must be inside the project root")
    manifest_path = target / "replay_manifest.json"
    manifest = {"schema_version": "e4-bank-saved-dag-replay/1", "case_id": case_id,
        "split": "dev", "mode": "CPU-only saved DAG replay", "model_requests": 0,
        "saved_dag_path": str(source_path), "saved_dag_sha256": sha256_file(source_path),
        "status": "running", "started_utc": datetime.now(timezone.utc).isoformat()}
    _json_write_exclusive(manifest_path, manifest)
    try:
        case = load_bank_case(case_id, "dev", root=root)
        runtime = build_case_runtime(case.alert,
            schema_path=ROOT / "config/runtime_schema.json",
            config_path=ROOT / "config/core_runtime.json")
        registry = bank_registry(root=root)
        document = json.loads(source_path.read_text(encoding="utf-8"))
        verified = verify_dag(document, registry, runtime,
            max_nodes=int(runtime.snapshot.values["max_nodes"]),
            max_depth=int(runtime.snapshot.values["dag_depth"]))
        _validate_bank_graph(document, verified, registry)
        copied = target / "replayed.dag.json"
        _json_write_exclusive(copied, document)
        parity = _execute_for_parity(verified, runtime, case.telemetry, target / "cache", root=root)
        result = {**manifest, "status": "passed" if parity["cache_parity_passed"] else "failed",
            "bank_input_provenance": dict(case.provenance),
            "metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
            "dev_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
            "replayed_dag_sha256": sha256_file(copied), **parity,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _json_write_replace(manifest_path, result)
        if not parity["cache_parity_passed"]:
            raise BankDevelopmentError("saved Bank DAG failed cold/warm/disabled cache parity")
        return {"manifest": result, "run_dir": str(target)}
    except Exception as exc:
        manifest.update({"status": "failed", "error": str(exc),
                         "finished_utc": datetime.now(timezone.utc).isoformat()})
        _json_write_replace(manifest_path, manifest)
        raise


def _dag_fixture(registry: Any) -> dict[str, Any]:
    """Small structural DAG used only in development runner tests."""
    nodes = []
    def add(node_id: str, operator: str, inputs=None, params=None):
        nodes.append({"id": node_id, "operator": operator, "version": "1",
                      "inputs": inputs or {}, "params": params or {}, "bindings": {}})
    add("data", "case.telemetry")
    add("mref", "bank.metrics.aggregate", {"data": "data"}, {"window": "reference"})
    add("malt", "bank.metrics.aggregate", {"data": "data"}, {"window": "alert"})
    add("tref", "bank.traces.aggregate", {"data": "data"}, {"window": "reference"})
    add("talt", "bank.traces.aggregate", {"data": "data"}, {"window": "alert"})
    add("candidates", "bank.candidates.generate",
        {"data": "data", "aggregates": ["mref", "malt", "tref", "talt"]})
    add("metric_score", "bank.metrics.contrast",
        {"reference": "mref", "alert": "malt", "candidates": "candidates"})
    add("trace_score", "bank.traces.latency_contrast",
        {"reference": "tref", "alert": "talt", "candidates": "candidates"})
    add("log_score", "bank.logs.volume_contrast",
        {"reference": "mref", "alert": "malt", "candidates": "candidates"})
    add("answer", "bank.answers.combine", {"scores": ["metric_score", "trace_score", "log_score"]})
    return {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "answer"}}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--split", choices=("dev",), default="dev")
    parser.add_argument("--profile", type=Path, default=ROOT / "config/gemma_m3_inference.json")
    parser.add_argument("--run-dir", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--controls", action="store_true",
                      help="run CPU-only cache controls for all four Bank seed DAGs; make no model requests")
    mode.add_argument("--saved-dag", type=Path,
                      help="CPU-only replay of a saved DAG; make no model requests")
    args = parser.parse_args(argv)
    if not os.environ.get("E4_RUN_DIR"):
        parser.error("set E4_RUN_DIR to an existing writable run directory")
    try:
        if args.controls:
            result = run_bank_controls(args.case_id, run_dir=args.run_dir, root=ROOT)
            print(json.dumps({"case_id": args.case_id, "run_dir": result["run_dir"],
                              "mode": "controls", "runs": len(result["manifest"]["runs"])}, sort_keys=True))
            return 0
        if args.saved_dag:
            result = replay_bank_development(args.case_id, args.saved_dag,
                                             run_dir=args.run_dir, root=ROOT)
            print(json.dumps({"case_id": args.case_id, "run_dir": result["run_dir"],
                              "mode": "saved_dag_replay",
                              "cache_parity_passed": result["manifest"]["cache_parity_passed"]}, sort_keys=True))
            return 0
        result = run_bank_development(args.case_id, profile_path=args.profile,
                                      run_dir=args.run_dir, root=ROOT)
        print(json.dumps({"case_id": args.case_id, "run_dir": result["run_dir"],
                          "cache_parity_passed": result["development_run"]["cache_parity_passed"],
                          "repair_count": result["development_run"]["static_repairs"]}, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"Bank development failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
