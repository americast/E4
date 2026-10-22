"""Three-stage M4 Bank controller with bounded prompts and binary-only escalation."""

from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import re
import shutil
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable, Mapping

from .bank_adapter import BankCase, load_bank_case, sha256_file
from .bank_ops import bank_registry
from .benchmark import REPAIR_LIMIT, StaticTaskError, parse_strict_json_object
from .cache import CacheStore, canonical_json, implementation_fingerprint
from .contracts import (Aggregate, Candidate, CandidateSet, EntityRef, RankedAnswer,
                        ScoredCandidate, ScoredCandidates)
from .dag import DAGValidationError, VerifiedDAG, verify_dag
from .executor import (ExecutionFailure, NodeContext, SerialExecutor,
                       execution_result_to_dict)
from .m4_data import M4QueryBudget, bank_m4_analysis_weights, make_m4_registry
from .m4_generated import (generated_spec_contract_schema, register_generated_specs,
                           validate_generated_spec)
from .m4_library import (LibrarySnapshot, admit_after_accepted,
                         assert_library_registry_compatible,
                         canonicalize_playbook_graph, generated_specs_from_snapshot,
                         playbook_graph_hash, record_library_invocation,
                         snapshot_library)
from .model_client import LocalModelClient
from .registry import OperatorRegistry
from .runtime import RuntimeContext, build_case_runtime


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_M4_POLICY = ROOT / "config/m4_runtime.json"
MAX_CASE_DIGEST_BYTES = 32 * 1024
MAX_DIGEST_SERIES = 8
MAX_PROMPT_HISTORY = 3
M4_SYSTEM_PROMPT = (
    "Analyze the supplied public Bank telemetry using only the registered E4 contracts. "
    "Return only the requested JSON artifact. You have no external tools or private-label access."
)


class M4ControllerError(RuntimeError):
    def __init__(self, code: str, message: str, *, static: bool = False):
        super().__init__(message)
        self.code, self.static = code, static

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "static": self.static}


class M4ModelClient(LocalModelClient):
    """Use M4-specific system wording without changing the frozen model payload."""

    def make_payload(self, prompt: str, *, max_tokens: int | None = None) -> dict[str, Any]:
        payload = super().make_payload(prompt, max_tokens=max_tokens)
        payload["messages"][0]["content"] = M4_SYSTEM_PROMPT
        return payload


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _implementation_fingerprint() -> str:
    pinned = os.environ.get("E4_M4_IMPLEMENTATION_FINGERPRINT")
    if pinned is None:
        return implementation_fingerprint()
    if not re.fullmatch(r"[0-9a-f]{64}", pinned):
        raise M4ControllerError("source_fingerprint_env", "trusted source fingerprint must be a lowercase SHA-256")
    return pinned


def _write_json(path: Path, value: Any, *, exclusive: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "x" if exclusive else "w"
    with path.open(mode, encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True,
                                allow_nan=False) + "\n")


def _append_jsonl(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":"), allow_nan=False) + "\n")
        stream.flush()


def _policy(root: Path = ROOT, policy_path: Path | None = None) -> dict[str, Any]:
    path = policy_path or root / "config/m4_runtime.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") != "e4-m4-runtime-policy/1":
        raise M4ControllerError("m4_policy_version", "unsupported M4 runtime policy")
    serving = value.get("serving", {})
    protected = value.get("protected_runtime_values", {})
    query = value.get("query_and_generation", {})
    query_frozen = {"maximum_public_query_requests_per_case": 32,
        "maximum_public_query_rows_per_request": 256,
        "maximum_public_query_rows_per_case": 2048,
        "maximum_public_query_bytes_per_request": 512 * 1024,
        "maximum_public_query_bytes_per_case": 2 * 1024 * 1024,
        "maximum_public_query_string_bytes": 4096,
        "maximum_public_query_offset": 100_000,
        "maximum_source_bytes_per_query": 512 * 1024 * 1024,
        "maximum_projected_summary_rows_per_request": 256,
        "maximum_projected_summary_offset": 100_000,
        "maximum_projected_string_bytes": 4096,
        "maximum_projected_output_bytes": 512 * 1024,
        "maximum_operator_input_bytes": 2 * 1024 * 1024,
        "generated_operator_wall_seconds": 5,
        "generated_operator_memory_bytes": 512 * 1024 * 1024}
    if (serving.get("stages") != ["select", "compose", "synthesize"] or
            serving.get("maximum_diagnostic_attempts") != 3 or
            serving.get("maximum_new_operators") != 3 or
            serving.get("static_repair_limit_per_attempt") != 2 or
            serving.get("maximum_nodes") != 40 or
            serving.get("maximum_depth") != 12 or
            serving.get("maximum_leads") != 5 or serving.get("acceptance_top_k") != 3 or
            value.get("bootstrap", {}).get("static_repair_limit") != 2 or
            value.get("bootstrap", {}).get("maximum_new_operators") != 2 or
            value.get("bootstrap", {}).get("maximum_new_playbooks") != 4 or
            value.get("library", {}).get("total_operator_cap") != 19 or
            protected.get("max_nodes") != 40 or protected.get("dag_depth") != 12 or
            protected.get("lead_count") != 5 or protected.get("acceptance_top_k") != 3 or
            protected.get("seed") != 42 or protected.get("permutation_resamples") != 499 or
            any(query.get(key) != expected for key, expected in query_frozen.items())):
        raise M4ControllerError("m4_policy_frozen_values", "M4 stage or resource budgets differ from the authorized frozen policy")
    return value


def typed_contract_wire_schema() -> dict[str, Any]:
    """Exact cache-wire fields required by M4 generated typed outputs."""
    return {"envelope": "exact object {$type: contract_name, $fields: object with every listed field}",
        "nested_collections": {"tuple": "{'$tuple':[items...]}",
                               "mapping": "{'$map':{string_key: value,...}}"},
        "finite_json": "numbers must be finite; object details contain JSON values only",
        "contract_fields": "contract_fields(value, expected_type) accepts a matching {$type, $fields} wire envelope and returns a recursively read-only MappingProxyType/dict-like mapping; nested mappings are read-only and nested lists/tuples are immutable tuples. Do not mutate returned values. The helper unwraps $fields; callers must not access fields through value['$fields'] after calling it.",
        "evidence_requirements": {
            "allowed_native_sources": ["bank/metric_app.csv", "bank/metric_container.csv",
                "bank/trace_span.csv", "bank/log/log_service.csv",
                "bank/metric/metric_app.csv", "bank/metric/metric_container.csv",
                "bank/trace/trace_span.csv", "bank/log/log_service.csv"],
            "support": "every positive-ranked lead requires at least one evidence item with support > 0",
            "eligibility": "every positive-ranked lead requires details.ranking_eligible == true only after the operator's declared gates pass",
            "attribution": "evidence.contributing_entities must include the exact component EntityRef being scored",
            "statistics": "do not invent p-values, effects, supports, or statistical-test claims; include them only when computed from the supplied inputs with declared methods",
            "bank_category": "category must be null; no Bank fault-category classifier is supported"},
        "contracts": {
            "EntityRef": {"fields": {"role": "string; Bank component outputs use 'component'",
                                       "identifier": "exact observed component ID"}},
            "Window": {"fields": {"name": "string: reference or alert",
                                    "start_ms_utc": "integer", "end_ms_utc": "integer"}},
            "AggregateRow": {"fields": {"entity": "EntityRef envelope", "metric_key": "string",
                "window": "Window envelope", "statistic": "string", "value": "finite number or null",
                "support": "nonnegative integer", "source_tables": "tuple of strings",
                "contributing_entities": "tuple of EntityRef envelopes", "details": "mapping envelope of finite JSON data"}},
            "Aggregate": {"fields": {"window": "Window envelope", "rows": "tuple of AggregateRow envelopes",
                "source_tables": "tuple of strings", "metadata": "mapping envelope of finite JSON data"}},
            "Candidate": {"fields": {"entity": "EntityRef envelope", "rationale": "string"}},
            "CandidateSet": {"fields": {"candidates": "tuple of Candidate envelopes", "source": "string"}},
            "Evidence": {"fields": {"source": "string", "window": "Window envelope", "statistic": "string",
                "value": "finite number, string, boolean, or null", "contributing_entities": "tuple of EntityRef envelopes",
                "support": "nonnegative integer", "details": "mapping envelope of finite JSON data"}},
            "ScoredCandidate": {"fields": {"entity": "EntityRef envelope", "score": "finite number",
                "category": "null for Bank M4; no Bank category classifier is supported",
                "evidence": "tuple of Evidence envelopes", "analysis_family": "string or null"}},
            "ScoredCandidates": {"fields": {"candidates": "tuple of ScoredCandidate envelopes",
                "scorer": "string", "status": "ready or abstained",
                "status_details": "mapping envelope of finite JSON data"}}}}


def data_operator_contract_schema() -> dict[str, Any]:
    """Document the exact metadata layouts returned by native query and summary projection."""
    return {
        "bank.public.query": {
            "output": "Aggregate",
            "aggregate_rows": "always an empty tuple; native rows are in metadata.public_query.rows_data",
            "public_query": {
                "request_fields": {"table": "native table path", "window": "reference|alert",
                    "entity": "exact filter or null", "metric": "exact filter or null",
                    "offset": "nonnegative integer", "limit": "integer 1..256",
                    "fields": "selected native columns plus timestamp_ms_utc"},
                "rows_data": "list of native CSV row objects; selected source-column values remain strings, timestamp_ms_utc is integer UTC milliseconds; selected fields only",
                "coverage_fields": ["total_matching_rows", "returned_rows", "source_files",
                    "source_rows_scanned", "source_bytes_scanned", "source_bytes_hashed",
                    "case_metadata_bytes_hashed", "returned_bytes", "bytes_processed_definition",
                    "omitted_before_rows", "remaining_after_rows", "truncated", "page_complete", "complete"],
                "page_fields": {"next_offset": "integer for next page or null",
                    "source_header": "native table header list",
                    "canonical_extra_fields": ["timestamp_ms_utc"],
                    "source_hashes": "mapping of exact prepared relative paths, including alert.json, to SHA-256 hex",
                    "timestamp_units": "mapping of exact prepared relative paths to empirically pinned seconds|milliseconds",
                    "timestamp_field": "timestamp_ms_utc is normalized; source timestamp remains verbatim",
                    "value_semantics": "native values only; no inferred metric units or component aliases"},
                "table_columns": {
                    "metric/metric_app.csv": ["timestamp", "rr", "sr", "cnt", "mrt", "tc"],
                    "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
                    "trace/trace_span.csv": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
                    "log/log_service.csv": ["log_id", "timestamp", "cmdb_id", "log_name", "value"]}},
            "limits": {"maximum_rows_per_request": 256, "maximum_request_bytes": 512 * 1024,
                "maximum_offset": 100_000, "string_limit_utf8_bytes": 4096}},
        "bank.public.project": {
            "input_output": "Aggregate to Aggregate; input must be an existing full-window summary Aggregate, not a native query page or prior projection",
            "output": {"rows": "tuple of selected AggregateRows; details preserve scalar values and add projection_omitted_detail_keys; each row preserves full-window value, support, metric/entity/window/source/contributors",
                "metadata": "preserves scalar source metadata; adds projection and projection_omitted_metadata_keys",
                "metadata_projection_fields": ["source_aggregate_digest", "source_row_count",
                    "total_matching_row_count", "selected_row_count", "omitted_before", "remaining_after",
                    "complete", "offset", "limit", "next_offset", "source_fields_filter",
                    "entities_filter", "statistics_filter", "omitted_metadata_keys",
                    "omitted_detail_keys", "projection_policy", "source_serialized_bytes",
                    "projection_serialized_bytes"]},
            "selection": "filters exact source_fields and statistics, entity identifier trim+casefold only; offset/limit pages rows without recomputing or sampling statistics",
            "limits": {"maximum_rows_per_request": 256, "maximum_output_bytes": 512 * 1024,
                "maximum_offset": 100_000, "string_limit_utf8_bytes": 4096}}
    }


def generated_spec_shape_example() -> dict[str, Any]:
    """Return a neutral shape-only example, not an analytical operator proposal."""
    return {
        "name": "evolved.example_operator",
        "version": "1",
        "description": "Shape-only pass-through example; replace with data-driven logic.",
        "family": "example",
        "input_ports": [{"name": "data", "type": "Aggregate", "required": True,
                         "variadic": False, "minimum_items": 1, "maximum_items": None}],
        "output_type": "Aggregate",
        "parameters": {"scale": {"type": "number", "required": False,
            "nullable": False, "default": 1.0, "choices": [], "runtime_field": None,
            "description": "Illustrative declared parameter."}},
        "source": "def operator(inputs, params, runtime):\n    # Shape only; replace with data-driven logic.\n    return inputs['data']",
    }


def build_m4_runtime(case: Mapping[str, Any], *,
                     overrides: Mapping[str, Any] | None = None,
                     root: Path = ROOT,
                     policy_path: Path | None = None) -> RuntimeContext:
    """Build a typed case runtime while enforcing frozen protected values and override bounds."""
    policy = _policy(root, policy_path)
    serving = policy["serving"]
    protected = policy["protected_runtime_values"]
    core = json.loads((root / "config/core_runtime.json").read_text(encoding="utf-8"))
    for key, value in protected.items():
        if core.get(key) != value:
            raise M4ControllerError("protected_runtime_mismatch",
                                    f"core runtime {key} differs from frozen M4 policy")
    if not isinstance(overrides, (Mapping, type(None))):
        raise M4ControllerError("runtime_override_schema", "runtime_overrides must be an object", static=True)
    supplied = dict(overrides or {})
    if set(supplied) & set(protected):
        raise M4ControllerError("protected_runtime_override",
            f"protected runtime values cannot be changed: {sorted(set(supplied) & set(protected))}", static=True)
    allowed = policy["model_runtime_overrides"]
    unknown = set(supplied) - set(allowed)
    if unknown:
        raise M4ControllerError("unknown_runtime_override",
                                 f"undeclared runtime override(s): {sorted(unknown)}", static=True)

    runtime_values = dict(core)
    runtime_values.update(protected)
    weight_defaults = bank_m4_analysis_weights(root=root)
    runtime_values["analysis_weights"] = weight_defaults
    for name, value in supplied.items():
        spec = allowed[name]
        value_type = spec["type"]
        if value_type == "number_map":
            if not isinstance(value, Mapping):
                raise M4ControllerError("runtime_override_type", f"{name} must be a JSON object", static=True)
            keys = set(value)
            invalid_keys = keys - set(spec["allowed_keys"])
            if invalid_keys:
                raise M4ControllerError("runtime_override_key",
                    f"unsupported {name} key(s): {sorted(invalid_keys)}", static=True)
            if any(type(item) not in (int, float) or not math.isfinite(float(item)) or
                   float(item) < float(spec["minimum"]) or float(item) > float(spec["maximum"])
                   for item in value.values()):
                raise M4ControllerError("runtime_override_range",
                    f"{name} values must be finite and between {spec['minimum']} and {spec['maximum']}",
                    static=True)
            runtime_values[name] = bank_m4_analysis_weights(root=root, overrides=value)
            continue
        if value_type == "positive_integer":
            valid_type = type(value) is int and value > 0
        elif value_type == "number":
            valid_type = type(value) in (int, float) and math.isfinite(float(value))
        else:
            raise M4ControllerError("m4_policy_schema", f"unsupported override type {value_type}")
        if not valid_type:
            raise M4ControllerError("runtime_override_type", f"invalid value type for {name}", static=True)
        if "choices" in spec and value not in spec["choices"]:
            raise M4ControllerError("runtime_override_choice",
                f"{name} must be one of {spec['choices']}", static=True)
        if "minimum" in spec and float(value) < float(spec["minimum"]):
            raise M4ControllerError("runtime_override_range", f"{name} is below its frozen minimum", static=True)
        if "maximum" in spec and float(value) > float(spec["maximum"]):
            raise M4ControllerError("runtime_override_range", f"{name} exceeds its frozen maximum", static=True)
        runtime_values[name] = value
    block_minutes = int(runtime_values["block_minutes"])
    if runtime_values["minimum_independent_blocks"] > 30 // block_minutes:
        raise M4ControllerError("runtime_block_support",
            "minimum_independent_blocks exceeds the number of blocks in a 30-minute window", static=True)
    try:
        return build_case_runtime(case, schema_path=root / "config/runtime_schema.json",
                                  config_path=root / "config/core_runtime.json",
                                  overrides=runtime_values)
    except Exception as exc:
        raise M4ControllerError("runtime_context_invalid", str(exc), static=True) from exc


def public_case_digest(case: BankCase, *, root: Path = ROOT,
                       max_series: int = MAX_DIGEST_SERIES,
                       max_bytes: int = MAX_CASE_DIGEST_BYTES) -> dict[str, Any]:
    """Build a bounded, deterministic case digest from public telemetry only."""
    if type(max_series) is not int or max_series <= 0 or type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("digest limits must be positive integers")
    telemetry = case.telemetry
    metric_policy = json.loads((root / "config/bank_metric_kind_policy.json").read_text(encoding="utf-8"))
    metric_semantics = metric_policy.get("source_semantics", {})
    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    metric_groups: dict[tuple[str, str, str], dict[str, dict[int, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    for point in telemetry.metrics:
        if point.value is None or not math.isfinite(point.value):
            continue
        name = next((key for key, window in windows.items()
                     if window.contains(point.timestamp_ms_utc)), None)
        if name is None:
            continue
        metric_groups[(point.entity.role, point.entity.identifier,
                       f"{point.table}|{point.metric_name}|{point.item_id or ''}")][name][
                           point.timestamp_ms_utc].append(float(point.value))
    component_series = []
    app_series = []
    log_series = []
    for (role, entity, metric), by_window in sorted(metric_groups.items(),
            key=lambda item: (item[0][0], item[0][1].casefold(), item[0][1], item[0][2])):
        table, field_name, item_id = (metric.split("|", 2) + [""])[:3]
        if table.endswith("metric_app.csv"):
            declared_semantics = metric_semantics.get("metric_app", {}).get("fields", {}).get(field_name, {})
        elif table.endswith("metric_container.csv"):
            declared_semantics = metric_semantics.get("metric_container", {}).get("ranking_allowlist", {}).get(
                field_name, {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False})
        elif table.endswith("log_service.csv"):
            declared_semantics = metric_semantics.get("log_service", {})
        else:
            declared_semantics = {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False}
        row = {"role": role, "entity": entity, "metric": metric,
               "field_semantics": {key: declared_semantics.get(key) for key in
                   ("kind", "unit", "ranking_eligible", "provenance", "working_interpretation")
                   if key in declared_semantics}, "windows": {}}
        for window_name in ("reference", "alert"):
            timestamp_groups = by_window.get(window_name, {})
            timestamp_medians = [float(median(values)) for _timestamp, values in sorted(timestamp_groups.items())
                                 if values]
            values = sorted(timestamp_medians)
            raw_rows = sum(len(items) for items in timestamp_groups.values())
            if values:
                row["windows"][window_name] = {"support": len(values),
                    "raw_rows": raw_rows,
                    "duplicate_rows_collapsed": max(0, raw_rows - len(values)),
                    "median": float(median(values)), "mean": float(mean(values)),
                    "q90": _quantile(values, 0.9)}
            else:
                row["windows"][window_name] = {"support": 0, "raw_rows": raw_rows,
                    "duplicate_rows_collapsed": 0, "median": None,
                    "mean": None, "q90": None}
        ref_center = row["windows"]["reference"]["median"]
        alt_center = row["windows"]["alert"]["median"]
        if ref_center is not None and alt_center is not None:
            row["descriptive_selection_change_ratio"] = float(abs(alt_center - ref_center) /
                max(abs(ref_center), float(metric_policy.get("source_semantics", {}).get(
                    "normalization_floor", 1e-9))))
        else:
            row["descriptive_selection_change_ratio"] = None
        if role == "app_kpi":
            app_series.append(row)
        elif "log_service.csv" in metric:
            log_series.append(row)
        else:
            component_series.append(row)

    # This ratio controls only which public series enter the bounded digest;
    # it is descriptive input selection, not inferential effect or causality.
    # Exact identifiers provide stable label-blind tie-breakers.
    component_series.sort(key=lambda row: (
        -(row["descriptive_selection_change_ratio"] if row["descriptive_selection_change_ratio"] is not None else -1.0),
        row["entity"].casefold(), row["entity"], row["metric"]))
    app_series.sort(key=lambda row: (
        -min(row["windows"]["reference"]["support"], row["windows"]["alert"]["support"]),
        row["entity"].casefold(), row["entity"], row["metric"]))
    log_series.sort(key=lambda row: (
        -(row["descriptive_selection_change_ratio"] if row["descriptive_selection_change_ratio"] is not None else -1.0),
        row["entity"].casefold(), row["entity"], row["metric"]))
    kept_series = component_series[:max_series]
    kept_app_series = app_series[:max_series]
    kept_log_series = log_series[:max_series]
    app_kpi_context = _select_primary_app_kpi_context(case, root=root)

    trace_groups: dict[tuple[str, str], dict[str, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    for span in telemetry.spans:
        if span.executing_component is None or not span.trace_id or not span.span_id:
            continue
        window_name = next((key for key, window in windows.items()
                            if window.contains(span.start_ms_utc)), None)
        if window_name is None:
            continue
        try:
            value = float(span.elapsed_raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value >= 0:
            trace_groups[(span.executing_component.identifier,
                          "bank/trace_span.csv|duration")][window_name].setdefault(span.trace_id, []).append(value)
    traces = []
    for (entity, metric), by_window in sorted(trace_groups.items(),
            key=lambda item: (item[0][0].casefold(), item[0][0], item[0][1])):
        row = {"entity": entity, "metric": metric, "success_field": None,
               "duration_unit": "undeclared", "windows": {}}
        for window_name in ("reference", "alert"):
            values = [float(median(group)) for _trace, group in sorted(by_window[window_name].items()) if group]
            values.sort()
            row["windows"][window_name] = {"independent_trace_support": len(values),
                "median": float(median(values)) if values else None,
                "mean": float(mean(values)) if values else None,
                "q90": _quantile(values, 0.9) if values else None}
        traces.append(row)
    output = {
        "case_id": telemetry.case_id,
        "timezone": case.alert.get("timezone"),
        "reference_window_utc_ms": [telemetry.reference_window.start_ms_utc,
                                     telemetry.reference_window.end_ms_utc],
        "alert_window_utc_ms": [telemetry.alert_window.start_ms_utc,
                                 telemetry.alert_window.end_ms_utc],
        "source_timestamp_units_by_date": _plain(case.alert.get("source_date_timestamp_units", {})),
        "file_sha256": dict(sorted(telemetry.file_sha256.items())),
        "file_bytes": dict(sorted(telemetry.file_bytes.items())),
        "exact_observed_component_ids": sorted(telemetry.deployment_catalog,
            key=lambda item: (item.casefold(), item)),
        "available_metric_fields": sorted({point.metric_name for point in telemetry.metrics}),
        "primary_app_kpi_context": app_kpi_context,
        "primary_app_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "metric_digest_selection_policy": "component series ordered by absolute median change divided by max(abs(reference median), frozen normalization floor); descriptive display selection only, not an inferential effect or cause score; ties use casefolded then exact entity and metric ID",
        "component_identity_coverage": _plain(case.provenance.get("component_identity_coverage", {})),
        "quality_counts": dict(sorted(telemetry.quality_counts.items())),
        "log_summary": _plain(case.log_summary),
        "metric_series": kept_series,
        "metric_series_omitted": max(0, len(component_series) - len(kept_series)),
        "app_kpi_context_series": kept_app_series,
        "app_kpi_context_series_omitted": max(0, len(app_series) - len(kept_app_series)),
        "log_volume_series": kept_log_series,
        "log_volume_series_omitted": max(0, len(log_series) - len(kept_log_series)),
        "trace_series": traces[:max_series],
        "trace_series_omitted": max(0, len(traces) - max_series),
        "log_text_excerpts": [],
        "log_text_omission_reason": "raw text is queried through bounded bank.public.query pages and is not in the initial case digest",
    }
    encoded = json.dumps(output, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > max_bytes:
        output["component_identity_coverage"] = {"truncated": True}
        output["quality_counts"] = {}
        output["log_summary"] = {"truncated": True}
        output["metric_series"] = kept_series[:max(1, max_series // 2)]
        output["metric_series_omitted"] = max(0, len(component_series) - len(output["metric_series"]))
        output["app_kpi_context_series"] = kept_app_series[:max(1, max_series // 2)]
        output["app_kpi_context_series_omitted"] = max(0, len(app_series) - len(output["app_kpi_context_series"]))
        output["log_volume_series"] = kept_log_series[:max(1, max_series // 2)]
        output["log_volume_series_omitted"] = max(0, len(log_series) - len(output["log_volume_series"]))
        output["trace_series"] = traces[:max(1, max_series // 2)]
        output["trace_series_omitted"] = max(0, len(traces) - len(output["trace_series"]))
        encoded = json.dumps(output, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > max_bytes:
        raise M4ControllerError("case_digest_limit", "bounded public case digest exceeds its frozen byte limit")
    output["digest_sha256"] = hashlib.sha256(encoded).hexdigest()
    output["digest_bytes"] = len(encoded)
    output["metric_series_limit"] = max_series
    output["trace_series_limit"] = max_series
    return output


def _quantile(values: list[float], q: float) -> float:
    if not values:
        raise ValueError("quantile requires nonempty values")
    position = q * (len(values) - 1)
    lower = int(position)
    upper = min(len(values) - 1, lower + 1)
    fraction = position - lower
    return float(values[lower] + fraction * (values[upper] - values[lower]))


def _select_primary_app_kpi_context(case: BankCase, *, root: Path) -> dict[str, Any]:
    """Apply the frozen Bank support-first app-mrt context rule without fallback."""
    policy_path = root / "config/bank_dev_kpi_policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise M4ControllerError("bank_kpi_policy_version", "unsupported frozen Bank KPI context policy")
    minimum = int(policy["minimum_support_per_window"])
    allowed = set(policy.get("primary_metric_allowlist", ()))
    windows = {"reference": case.telemetry.reference_window,
               "alert": case.telemetry.alert_window}
    groups: dict[tuple[str, str, str], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    raw_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for point in case.telemetry.metrics:
        if point.table != "bank/metric_app.csv" or point.value is None or not math.isfinite(point.value):
            continue
        field = f"metric_app.{point.metric_name}"
        if field not in allowed or point.entity.role != "app_kpi":
            continue
        window_name = next((name for name, window in windows.items()
                            if window.contains(point.timestamp_ms_utc)), None)
        if window_name is None:
            continue
        key = (field, point.entity.identifier, window_name)
        groups[key][point.timestamp_ms_utc].append(float(point.value))
        raw_counts[key] += 1
    candidates = []
    identities = sorted({(field, tc) for field, tc, window in groups if window == "reference"} &
                        {(field, tc) for field, tc, window in groups if window == "alert"},
                        key=lambda item: (-min(len(groups[(item[0], item[1], name)])
                            for name in ("reference", "alert")), item[1].casefold(), item[1], item[0]))
    for field, tc in identities:
        values_by_window = {}
        for name in ("reference", "alert"):
            per_timestamp = [float(median(values)) for _, values in
                             sorted(groups[(field, tc, name)].items()) if values]
            values_by_window[name] = sorted(per_timestamp)
        support = {name: len(values_by_window[name]) for name in values_by_window}
        if min(support.values()) < minimum:
            continue
        candidates.append({"kpi_id": field, "field": field.split(".", 1)[1],
            "tc_id": tc, "role": "app_kpi_context_only", "reference_support": support["reference"],
            "alert_support": support["alert"], "paired_support": min(support.values()),
            "raw_rows": {name: raw_counts[(field, tc, name)] for name in support},
            "duplicate_rows_collapsed": {name: raw_counts[(field, tc, name)] - support[name]
                                         for name in support},
            "reference": {"median": float(median(values_by_window["reference"])),
                "mean": float(mean(values_by_window["reference"])),
                "q90": _quantile(values_by_window["reference"], 0.9)},
            "alert": {"median": float(median(values_by_window["alert"])),
                "mean": float(mean(values_by_window["alert"])),
                "q90": _quantile(values_by_window["alert"], 0.9)},
            "unit": "undeclared", "component_mapping": "none",
            "interpretation": "mean-response-time-style working interpretation from field name only"})
    if not candidates:
        return {"kpi_id": None, "selection_reason": "no allowlisted app KPI/tc pair met frozen support; no resource fallback",
            "minimum_support_per_window": minimum, "policy_sha256": sha256_file(policy_path),
            "source_policy": policy.get("selection_provenance")}
    candidates.sort(key=lambda item: (-item["paired_support"], item["tc_id"].casefold(),
                                      item["tc_id"], item["kpi_id"]))
    return {**candidates[0], "selection_reason": policy.get("selection"),
        "minimum_support_per_window": minimum, "policy_sha256": sha256_file(policy_path),
        "source_policy": policy.get("selection_provenance")}


def _topology(document: Mapping[str, Any]) -> dict[str, Any]:
    return {"nodes": [{"id": row["id"], "operator": row["operator"],
                        "version": row["version"], "inputs": _plain(row["inputs"])}
                       for row in document["nodes"]],
            "return": _plain(document["return"])}


def _materialize_seeds_for_registry(seed_library: Mapping[str, Any],
                                    registry: OperatorRegistry) -> dict[str, Any]:
    versions: dict[str, str] = {}
    for row in registry.signatures():
        if row["name"] == "bank.answers.combine":
            if row["version"].startswith("1+m4."):
                versions[row["name"]] = row["version"]
        elif row["name"] not in versions:
            versions[row["name"]] = row["version"]
    examples = seed_library.get("examples")
    if not isinstance(examples, Mapping):
        raise M4ControllerError("seed_schema", "Bank seed library examples must be an object")
    materialized = {}
    for name, doc in examples.items():
        value = json.loads(json.dumps(doc, ensure_ascii=False, allow_nan=False))
        for node in value.get("nodes", []):
            operator_name = node.get("operator")
            if operator_name not in versions:
                raise M4ControllerError("seed_operator_missing", f"no active registry version for {operator_name}")
            node["version"] = versions[operator_name]
        materialized[str(name)] = value
    return materialized


def _validate_graph(document: Mapping[str, Any], registry: OperatorRegistry,
                    runtime: RuntimeContext, *, stage: str,
                    new_operator_keys: set[str] | None = None) -> VerifiedDAG:
    try:
        verified = verify_dag(document, registry,
            runtime=runtime,
            max_nodes=int(runtime.snapshot.values["max_nodes"]),
            max_depth=int(runtime.snapshot.values["dag_depth"]))
    except DAGValidationError as exc:
        raise M4ControllerError("dag_static_invalid", str(exc), static=True) from exc
    external_nodes = [node for node in verified.nodes if node.operator.external_input]
    if len(external_nodes) != 1 or external_nodes[0].operator.name != "case.telemetry":
        raise M4ControllerError("dag_external_input", "M4 DAG must contain exactly one case.telemetry input", static=True)
    returned = next(node for node in verified.nodes if node.node_id == verified.return_node)
    if (returned.operator.name != "bank.answers.combine" or
            not returned.operator.version.startswith("1+m4.") or
            returned.operator.output_type != "RankedAnswer"):
        raise M4ControllerError("dag_return_type", "M4 DAG must return the versioned Bank M4 RankedAnswer combiner", static=True)
    if "limit" in returned.params and returned.params["limit"] > int(runtime.snapshot.values["lead_count"]):
        raise M4ControllerError("dag_lead_limit", "M4 DAG cannot return more leads than protected runtime.lead_count", static=True)
    if stage == "synthesize":
        reachable = new_operator_keys or set()
        used = {f"{node.operator.name}@{node.operator.version}" for node in verified.nodes}
        if not (used & reachable):
            raise M4ControllerError("stage3_new_operator_unreachable",
                "Stage 3 DAG must reach at least one newly proposed operator", static=True)
    return verified


def _apply_stage1_selection(selection: Mapping[str, Any], snapshot: LibrarySnapshot,
                            registry: OperatorRegistry) -> tuple[dict[str, Any], str]:
    expected = {"playbook", "runtime_overrides", "node_parameters", "node_bindings"}
    if not isinstance(selection, Mapping) or set(selection) != expected:
        raise M4ControllerError("stage1_schema", f"Stage 1 response must contain exactly {sorted(expected)}", static=True)
    name = selection["playbook"]
    if not isinstance(name, str) or name not in snapshot.playbooks:
        raise M4ControllerError("stage1_playbook_unknown", "Stage 1 must select one current library playbook", static=True)
    playbook = snapshot.playbooks[name]
    document = _plain(playbook["document"])
    before_topology = _topology(document)
    if not isinstance(selection["node_parameters"], Mapping) or not isinstance(selection["node_bindings"], Mapping):
        raise M4ControllerError("stage1_parameter_schema", "node_parameters/node_bindings must be objects", static=True)
    for node_id in set(selection["node_parameters"]) & set(selection["node_bindings"]):
        if not isinstance(selection["node_parameters"][node_id], Mapping) or not isinstance(selection["node_bindings"][node_id], Mapping):
            continue
        conflict = set(selection["node_parameters"][node_id]) & set(selection["node_bindings"][node_id])
        if conflict:
            raise M4ControllerError("stage1_parameter_binding_conflict",
                f"a node parameter cannot be overridden explicitly and by binding at once: {node_id}:{sorted(conflict)}",
                static=True)
    by_id = {node["id"]: node for node in document["nodes"]}
    for field_name in ("node_parameters", "node_bindings"):
        values = selection[field_name]
        unknown_nodes = set(values) - set(by_id)
        if unknown_nodes:
            raise M4ControllerError("stage1_node_unknown", f"unknown stored node IDs: {sorted(unknown_nodes)}", static=True)
        for node_id, mapping in values.items():
            if not isinstance(mapping, Mapping):
                raise M4ControllerError("stage1_parameter_schema", f"{field_name}.{node_id} must be an object", static=True)
            node = by_id[node_id]
            operator = registry.get(node["operator"], node["version"])
            if operator is None:
                raise M4ControllerError("stage1_operator_unknown", "stored playbook operator is unavailable", static=True)
            for key, value in mapping.items():
                if key not in operator.parameters:
                    raise M4ControllerError("stage1_parameter_unknown", f"undeclared parameter {key} on node {node_id}", static=True)
                if field_name == "node_parameters":
                    node["params"][key] = value
                    node["bindings"].pop(key, None)
                else:
                    if not isinstance(value, str) or not value.startswith("runtime."):
                        raise M4ControllerError("stage1_binding_schema", "runtime bindings must use runtime.<field>", static=True)
                    node["bindings"][key] = value
                    node["params"].pop(key, None)
    if _topology(document) != before_topology:
        raise M4ControllerError("stage1_topology_changed", "Stage 1 cannot add or change nodes, edges, operators, versions, or return node", static=True)
    return document, name


def _stage_prompt(stage: str, *, digest: Mapping[str, Any], registry: OperatorRegistry,
                  snapshot: LibrarySnapshot, runtime: RuntimeContext,
                  history: list[Mapping[str, Any]], policy: Mapping[str, Any],
                  query_budget: M4QueryBudget, include_case_digest: bool = True,
                  include_history: bool = True) -> str:
    from e4infra.bank_prep import BANK_OFFICIAL_CANDIDATE_COMPONENTS

    query_policy = policy["query_and_generation"]
    query_usage = {"requests_used": query_budget.requests,
        "requests_remaining": max(0, int(query_policy["maximum_public_query_requests_per_case"]) - query_budget.requests),
        "rows_returned": query_budget.rows,
        "rows_remaining": max(0, int(query_policy["maximum_public_query_rows_per_case"]) - query_budget.rows),
        "serialized_bytes_returned": query_budget.bytes,
        "bytes_remaining": max(0, int(query_policy["maximum_public_query_bytes_per_case"]) - query_budget.bytes)}
    common = (
        "You are running one Bank case in the frozen M4 experiment. Do not infer unsupported "
        "trace success, root/database roles, aliases, categories, or raw log severity. Bank duration units "
        "are undeclared. Exact observed IDs are case-local; never alias docker IDs to named pods. The app tc "
        "field is opaque and not a component. Use supplied runtime overrides only; do not invent threshold "
        "constants. The initial case summary is a bounded digest; raw rows can be inspected only with the "
        "registered bank.public.query operator, whose offsets and total per-case request/row/byte budgets "
        "are enforced outside your process. When an Aggregate's cached details exceed the worker's 2 MiB "
        "input limit, pass it through bank.public.project with declared source_fields/entities/statistics, "
        "offset 0..100000, and limit 1..256. Projection preserves existing full-window scalar summaries, support, "
        "units and identity; it drops non-scalar row details/metadata with explicit omitted-key and coverage records, "
        "does not recompute statistics, and is limited to 512 KiB/4096-byte strings. Never omit fields silently. "
        "Native query pages are already bounded and cannot be projected again. Use fresh bank.public.query pages "
        "when an algorithm needs raw row samples or raw log text. No label or prior gold-evaluation data is available."
    )
    common += (
        "\n\nOFFICIAL STATIC BANK CANDIDATE IDENTIFIERS (published schema vocabulary, not observations)\n" +
        json.dumps(list(BANK_OFFICIAL_CANDIDATE_COMPONENTS), ensure_ascii=False,
                   separators=(",", ":")) +
        "\nThese fixed source identifiers are public reference information only. They do not imply that an ID "
        "was observed in this case, do not provide aliases or mappings, and do not change the case-local observed "
        "component list or evidence requirements. Only rank exact observed component IDs supported by the selected case."
    )
    if include_case_digest:
        common += "\n\nCURRENT PUBLIC CASE DIGEST\n" + json.dumps(digest, ensure_ascii=False,
            sort_keys=True, separators=(",", ":"))
    common += (
        "\n\nPUBLIC QUERY AND GENERATED WORKER LIMITS\n" +
        json.dumps({"limits": query_policy, "current_case_usage": query_usage},
                   ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
        "\n\nCURRENT ALLOWLISTED RUNTIME VALUES\n" + json.dumps(runtime.as_dict(), ensure_ascii=False,
            sort_keys=True, separators=(",", ":")) +
        "\n\nRUNTIME OVERRIDE POLICY\n" + json.dumps(policy["model_runtime_overrides"],
            ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
        "\n\nREGISTERED OPERATOR SIGNATURES\n" + json.dumps(registry.signatures(),
            ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if include_history:
        common += ("\n\nPRIOR ATTEMPT HISTORY (diagnostic outputs and binary accepted flags only)\n" +
            json.dumps(history[-MAX_PROMPT_HISTORY:], ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False))
    if stage == "select":
        plays = [{"name": name, "graph_sha256": item["graph_sha256"],
                  "document": _plain(item["document"])}
                 for name, item in sorted(snapshot.playbooks.items())]
        return (common + "\n\nSTAGE 1: select and instantiate one stored playbook. Return exactly "
            "{playbook:string,runtime_overrides:object,node_parameters:object,node_bindings:object}. "
            "You may choose explicit values only for parameters declared by the stored nodes, or bind a "
            "declared parameter to an existing runtime field. For one parameter choose either an explicit value or a binding, not both; "
            "an explicit override replaces a stored binding, and a binding replaces a stored explicit value. The node IDs, operators, versions, input edges, "
            "variadic positions, and return node must remain exactly as stored. No new node or operator. "
            "Use empty objects when no override is needed.\n\nCURRENT STORED PLAYBOOKS\n" +
            json.dumps(plays, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if stage == "compose":
        return (common + "\n\nSTAGE 2: compose a new typed DAG using only the registered operators. Return exactly "
            "{dag:{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}}, "
            "runtime_overrides:{}}. Use one case.telemetry input and return the M4 Bank RankedAnswer combiner. "
            "Keep to max_nodes/dag_depth from protected runtime. No generated source in this stage.")
    if stage == "synthesize":
        return (common + "\n\nSTAGE 3: propose a focused new operator and compose a DAG that reaches it; prefer one operator, "
            "and propose more only if a distinct additional capability is justified. Three is a hard maximum, not a target. "
            "Return exactly {operators:[spec],dag:{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}},runtime_overrides:{}}. Every DAG node's inputs, params, and bindings values must be JSON objects/maps (use {} when empty); inputs maps port names to node IDs, params maps declared parameter names to JSON values, and bindings maps declared parameter names to runtime field names. The DAG return node MUST be the trusted versioned bank.answers.combine operator that produces RankedAnswer; return scored candidates from your new operator into that combiner, never return Aggregate, CandidateSet, or ScoredCandidates directly. "
            "Each operator spec has exactly {name,version,description,family,input_ports,output_type,parameters,source}; "
            "names use evolved.<lower_snake>, source defines operator(inputs,params,runtime). Inputs are a plain "
            "port-name mapping whose typed values are cache wire envelopes; use contract_fields(inputs['port'],'Aggregate') "
            "to read a typed contract's public fields as nested read-only mappings/tuples. contract_fields unwraps the $fields envelope and nested maps/arrays are read-only (maps behave as MappingProxyType, arrays as tuples); do not mutate them. Params are plain JSON mapping; "
            "runtime supports both runtime.field and runtime['field']. Output types are Aggregate, CandidateSet, or "
            "ScoredCandidates; return exact cache wire envelopes with $type/$fields and nested $map/$tuple wrappers. "
            "Example envelope only: {'$type':'CandidateSet','$fields':{'candidates':{'$tuple':[]},'source':'...'}}. "
            "Only builtins abs,all,any,bool,dict,enumerate,float,int,len,list,max,min,range,round,set,sorted,str,sum,tuple,zip,print,Exception,ValueError and imports math/statistics/itertools/collections are available; contract_fields is also available. "
            "read_case_json and read_catalog_json have no mounts for M4 generated operators and will fail if called. "
            "Filesystem, network, reflection, dynamic imports, classes, lambda expressions, try/except, and recursion are forbidden. "
            "Bounded for/while iteration over supplied lists or ranges is permitted and remains subject to worker CPU, wall, memory, and output limits. "
            "Nontrivial numeric constants must be declared parameters/runtime values. Do not encode component names, "
            "incident IDs, or expected outputs into code.\n\nEXACT TYPED OUTPUT ENVELOPES\n"
            "Aggregate: {'$type':'Aggregate','$fields':{'window':{'$type':'Window','$fields':{'name':'alert','start_ms_utc':0,'end_ms_utc':1}},'rows':{'$tuple':[]},'source_tables':{'$tuple':['bank/table.csv']},'metadata':{'$map':{}}}}. "
            "CandidateSet: {'$type':'CandidateSet','$fields':{'candidates':{'$tuple':[{'$type':'Candidate','$fields':{'entity':{'$type':'EntityRef','$fields':{'role':'component','identifier':'observed-id'}},'rationale':'...'}}]},'source':'...'}}. "
            "Nested mappings use $map, tuples use $tuple, nested contracts retain their $type/$fields envelope. Use category null; Bank has no supported generated fault-category classifier.\n\n"
            "EXACT CORE CONTRACT FIELD SCHEMA\n" +
            json.dumps(typed_contract_wire_schema(), ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")) +
            "\n\nEXACT GENERATED OPERATOR SPEC JSON SCHEMA\n" +
            json.dumps(generated_spec_contract_schema(), ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")) +
            "\n\nNEUTRAL GENERATED SPEC SHAPE EXAMPLE (not an analytical solution)\n" +
            json.dumps(generated_spec_shape_example(), ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")) +
            "\n\nNATIVE QUERY AND PROJECTION DATA LAYOUT\n" +
            json.dumps(data_operator_contract_schema(), ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")) +
            "\n\nNORMALIZED VERSION RULE\n"
            "DAG nodes may refer to each proposed spec's declared version; the controller deterministically replaces it "
            "with the content-addressed normalized version before verification and records the mapping.")
    raise ValueError(f"unknown M4 stage {stage}")


def _static_repair_prompt(base_prompt: str, previous: str, error: Mapping[str, Any]) -> str:
    diagnostic = {key: error[key] for key in ("code", "message", "node_id") if key in error}
    return (base_prompt + "\n\nSTATIC CONTRACT ERROR ONLY. Repair the JSON schema, declared operator contract, "
        "or typed DAG topology. No execution, ranking, or feedback details are provided.\n" +
        json.dumps(diagnostic, sort_keys=True, ensure_ascii=False, separators=(",", ":")) +
        "\n\nPrevious artifact:\n" + previous + "\n\nReturn the corrected JSON object only.")


def _normalize_overrides(response: Mapping[str, Any], case: BankCase,
                         root: Path, policy: Mapping[str, Any]) -> RuntimeContext:
    overrides = response.get("runtime_overrides")
    if not isinstance(overrides, Mapping):
        raise M4ControllerError("runtime_override_schema", "runtime_overrides must be an object", static=True)
    return build_m4_runtime(case.alert, overrides=overrides, root=root)


def _call_feedback(feedback: Any, case_id: str, answer: RankedAnswer) -> dict[str, bool]:
    callback = feedback.evaluate if hasattr(feedback, "evaluate") else feedback
    if not callable(callback):
        raise M4ControllerError("feedback_unavailable", "trusted binary feedback callback is required")
    result = callback(case_id, answer)
    if type(result) is not dict or set(result) != {"accepted"} or type(result["accepted"]) is not bool:
        raise M4ControllerError("feedback_contract", "feedback callback must return exactly {'accepted': bool}")
    return result


def validate_bank_answer(answer: RankedAnswer, case: BankCase, *, maximum_leads: int = 5) -> None:
    """Enforce exact case-local observed component membership and eligible evidence."""
    if not isinstance(answer, RankedAnswer) or len(answer.leads) > maximum_leads:
        raise M4ControllerError("answer_contract", "ranked output violates the frozen answer shape")
    observed = {identifier.strip().casefold(): identifier
                for identifier, metadata in case.telemetry.deployment_catalog.items()
                if isinstance(metadata, Mapping) and metadata.get("role", "component") == "component"}
    seen: set[str] = set()
    prior_score = math.inf
    allowed_sources = {
        "bank/metric_app.csv", "bank/metric_container.csv", "bank/trace_span.csv",
        "bank/log/log_service.csv", "bank/metric/metric_app.csv",
        "bank/metric/metric_container.csv", "bank/trace/trace_span.csv",
        "bank/log/log_service.csv",
    }
    for index, candidate in enumerate(answer.leads):
        identifier = candidate.entity.identifier.strip()
        canonical = identifier.casefold()
        if candidate.entity.role != "component" or canonical not in observed:
            raise M4ControllerError("answer_unobserved_component",
                f"ranked lead {index + 1} is not an exact observed Bank component")
        if canonical in seen:
            raise M4ControllerError("answer_duplicate_component", "ranked leads repeat a canonical component")
        seen.add(canonical)
        if candidate.score > prior_score:
            raise M4ControllerError("answer_order", "ranked leads are not in nonincreasing score order")
        prior_score = candidate.score
        eligible = [item for item in candidate.evidence
                    if item.details.get("ranking_eligible") is True and item.support > 0
                    and item.source in allowed_sources
                    and any(entity.role == "component" and
                            entity.identifier.strip().casefold() == canonical
                            for entity in item.contributing_entities)]
        if not eligible:
            raise M4ControllerError("answer_missing_evidence",
                f"ranked lead {index + 1} has no supported, source-tagged eligible evidence")


def _model_cost(calls: list[Mapping[str, Any]]) -> dict[str, Any]:
    prompt_tokens = 0
    completion_tokens = 0
    prompt_known = True
    completion_known = True
    for row in calls:
        usage = row.get("usage", {})
        prompt = usage.get("prompt_tokens", usage.get("input_tokens")) if isinstance(usage, Mapping) else None
        completion = usage.get("completion_tokens", usage.get("output_tokens")) if isinstance(usage, Mapping) else None
        if type(prompt) is int:
            prompt_tokens += prompt
        else:
            prompt_known = False
        if type(completion) is int:
            completion_tokens += completion
        else:
            completion_known = False
    return {"model_call_count": len(calls),
        "prompt_tokens": prompt_tokens if prompt_known else None,
        "completion_tokens": completion_tokens if completion_known else None,
        "model_elapsed_seconds": sum(float(row.get("elapsed_seconds", 0.0)) for row in calls),
        "generation_elapsed_seconds": sum(float(row.get("generation_elapsed_seconds", 0.0)) for row in calls),
        "static_repair_calls": sum(1 for row in calls if row.get("m4_static_repair") is True)}


def _bounded_answer_history(answer: RankedAnswer, *, lead_limit: int = 5,
                            evidence_per_lead: int = 2, max_bytes: int = 8192) -> dict[str, Any]:
    """Keep useful escalation context bounded while retaining a digest of full output."""
    raw = canonical_json(answer)
    leads = []
    for rank, candidate in enumerate(answer.leads[:lead_limit], start=1):
        evidence = []
        for item in candidate.evidence[:evidence_per_lead]:
            details = item.details
            evidence.append({"source": item.source, "window": item.window.name,
                "statistic": item.statistic, "value": item.value, "support": item.support,
                "details": {key: details[key] for key in (
                    "metric_key", "metric_kind", "normalized_effect", "p_value",
                    "direction", "unit", "ranking_eligible", "summary_statistic")
                    if key in details}})
        leads.append({"rank": rank, "component_id": candidate.entity.identifier,
            "score": candidate.score, "category": candidate.category,
            "analysis_family": candidate.analysis_family,
            "evidence_count": len(candidate.evidence), "evidence_excerpt": evidence,
            "evidence_omitted": max(0, len(candidate.evidence) - len(evidence))})
    output = {"lead_count": len(answer.leads), "leads": leads,
        "leads_omitted": max(0, len(answer.leads) - len(leads)),
        "full_answer_sha256": hashlib.sha256(raw).hexdigest(),
        "full_answer_bytes": len(raw), "history_is_truncated": True}
    while len(json.dumps(output, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")) > max_bytes and leads:
        removed = leads.pop()
        output["leads_omitted"] += 1
        output["leads"] = leads
        if leads:
            last = leads[-1]
            if last["evidence_excerpt"]:
                last["evidence_omitted"] += len(last["evidence_excerpt"])
                last["evidence_excerpt"] = []
    return output


def _snapshot_source_pins(snapshot: LibrarySnapshot, root: Path, policy_path: Path) -> None:
    provenance = snapshot.document.get("provenance", {})
    if not isinstance(provenance, Mapping):
        raise M4ControllerError("library_provenance", "library snapshot lacks source provenance")
    expected = {"implementation_fingerprint": _implementation_fingerprint(),
        "m4_runtime_policy_sha256": sha256_file(policy_path),
        "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json")}
    for key, value in expected.items():
        if provenance.get(key) != value:
            raise M4ControllerError("library_source_mismatch",
                f"bootstrap library {key} does not match the active frozen source")


def _stage1_doc(selection: Mapping[str, Any], snapshot: LibrarySnapshot,
                registry: OperatorRegistry, runtime: RuntimeContext) -> tuple[dict[str, Any], str]:
    doc, name = _apply_stage1_selection(selection, snapshot, registry)
    return doc, name


def _candidate_spec_mapping(specs: list[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[tuple[str, str], str]]:
    normalized: list[dict[str, Any]] = []
    mapping: dict[tuple[str, str], str] = {}
    names = set()
    for raw in specs:
        try:
            spec = validate_generated_spec(raw)
        except Exception as exc:
            raise M4ControllerError("generated_spec_static", str(exc), static=True) from exc
        key = (spec["name"], spec["declared_version"])
        if key in mapping or spec["name"] in names:
            raise M4ControllerError("generated_spec_duplicate", "Stage 3 specs must have unique names and declared versions", static=True)
        mapping[key] = spec["version"]
        names.add(spec["name"])
        normalized.append(spec)
    return normalized, mapping


def reject_identifier_literals(source: str, identifiers: set[str] | list[str] | tuple[str, ...]) -> None:
    """Reject generated code that embeds an exact case/entity answer as a string constant."""
    forbidden = {item.strip().casefold() for item in identifiers
                 if isinstance(item, str) and item.strip()}
    if not forbidden:
        return
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise M4ControllerError("generated_source_syntax", str(exc), static=True) from exc
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value.strip().casefold() in forbidden:
                raise M4ControllerError("generated_source_identifier_literal",
                    "generated source cannot embed an incident ID or exact observed component ID; use inputs/params",
                    static=True)


def _resolve_generated_versions(document: Any, mapping: Mapping[tuple[str, str], str]) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise M4ControllerError("dag_schema", "dag must be a JSON object", static=True)
    result = json.loads(json.dumps(document, ensure_ascii=False, allow_nan=False))
    if not isinstance(result.get("nodes"), list):
        raise M4ControllerError("dag_schema", "dag.nodes must be an array", static=True)
    for node in result["nodes"]:
        if not isinstance(node, dict):
            raise M4ControllerError("dag_schema", "each DAG node must be an object", static=True)
        key = (node.get("operator"), node.get("version"))
        if key in mapping:
            node["version"] = mapping[key]
    return result


def _verify_and_execute(document: Mapping[str, Any], registry: OperatorRegistry,
                        runtime: RuntimeContext, telemetry: Any, run_dir: Path,
                        *, stage: str, new_operator_keys: set[str] | None = None) -> tuple[VerifiedDAG, Any]:
    verified = _validate_graph(document, registry, runtime, stage=stage,
                               new_operator_keys=new_operator_keys)
    source_id = next(node.node_id for node in verified.nodes if node.operator.external_input)
    executor = SerialExecutor(registry, CacheStore(run_dir / "cache", enabled=True))
    try:
        result = executor.execute_verified(verified, runtime, {source_id: telemetry},
            case_id=telemetry.case_id, data_sha256=telemetry.data_sha256,
            file_sha256=telemetry.file_sha256, cache_enabled=True)
    except ExecutionFailure as exc:
        raise M4ControllerError("runtime_execution_failed", str(exc)) from exc
    return verified, result


def run_diagnostic(case_id: str, *, library_root: Path, run_dir: Path,
                   feedback: Any, profile_path: Path,
                   client: LocalModelClient | None = None,
                   root: Path = ROOT, policy_path: Path | None = None,
                   split: str = "dev") -> dict[str, Any]:
    """Run one three-stage Bank diagnosis; default remains frozen development-only."""
    started = time.perf_counter()
    root = root.resolve(strict=True)
    policy_path = policy_path or root / "config/m4_runtime.json"
    policy = _policy(root, policy_path)
    if run_dir.exists():
        raise FileExistsError(f"M4 run directory must be fresh: {run_dir}")
    run_dir.mkdir(parents=True)
    for folder in ("prompts", "attempts", "dags", "operators", "audit"):
        (run_dir / folder).mkdir()
    calls_path = run_dir / "model_calls.jsonl"
    calls_path.touch(exist_ok=False)
    status_path = run_dir / "controller_status.json"
    _write_json(status_path, {"phase": "public_input_load", "status": "running"})
    snapshot: LibrarySnapshot | None = None
    calls: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    used_operator_keys: set[str] = set()
    used_playbooks: set[str] = set()
    last_snapshot: LibrarySnapshot | None = None
    accepted = False
    final_answer: RankedAnswer | None = None
    stage_rows: list[dict[str, Any]] = []
    try:
        sample = json.loads((root / "config/bank_sample.json").read_text(encoding="utf-8"))
        if split not in {"dev", "scored"}:
            raise M4ControllerError("case_split", "diagnosis split must be dev or scored")
        case_key = "development_cases" if split == "dev" else "scored_cases"
        allowed_cases = {row["incident_id"] for row in sample.get(case_key, [])}
        if case_id not in allowed_cases:
            raise M4ControllerError("case_not_in_split", f"controller case is not in the frozen {split} allowlist")
        case = load_bank_case(case_id, split, root=root)
        digest = public_case_digest(case, root=root)

        source_snapshot = snapshot_library(library_root)
        local_library = run_dir / "library"
        shutil.copytree(source_snapshot.root, local_library)
        snapshot = snapshot_library(local_library)
        last_snapshot = snapshot
        _snapshot_source_pins(snapshot, root, policy_path)
        specs = generated_specs_from_snapshot(snapshot)
        query_budget = M4QueryBudget(run_dir / "audit")
        registry = make_m4_registry(case, specs, root=root, audit_dir=run_dir / "audit",
                                    shared_query_budget=query_budget)
        assert_library_registry_compatible(snapshot, registry)
        profile_path = profile_path.resolve(strict=True)
        if not profile_path.is_relative_to(root):
            raise M4ControllerError("profile_path", "M4 model profile must remain inside project root")
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        model = client or M4ModelClient(profile,
            base_url=os.environ.get("E4_BASE_URL"),
            request_timeout=float(policy["serving"]["model_timeout_seconds"]))
        run_doc = {"schema_version": "e4-m4-diagnostic-run/1", "case_id": case_id,
            "split": split, "profile_path": str(profile_path),
            "profile_sha256": sha256_file(profile_path), "profile": profile,
            "model_alias": profile.get("model_alias"),
            "implementation_fingerprint": _implementation_fingerprint(),
            "m4_runtime_policy_sha256": sha256_file(policy_path),
            "bootstrap_snapshot_id": snapshot.snapshot_id,
            "input_manifest_sha256": case.provenance.get("input_manifest_sha256"),
            "public_digest_sha256": digest["digest_sha256"],
            "public_digest_bytes": digest["digest_bytes"],
            "case_data_sha256": case.telemetry.data_sha256,
            "metric_policy_sha256": case.provenance.get("metric_policy_sha256"),
            "kpi_policy_sha256": case.provenance.get("dev_kpi_policy_sha256"),
            "source_hashes": dict(case.telemetry.file_sha256),
            "static_repairs_per_attempt": policy["serving"]["static_repair_limit_per_attempt"],
            "maximum_diagnostic_attempts": policy["serving"]["maximum_diagnostic_attempts"],
            "acceptance_contract": policy["serving"]["acceptance_contract"],
            "started_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "diagnostic_manifest.json", run_doc)
        _write_json(run_dir / "public_case_digest.json", digest)
        if model is None:
            raise M4ControllerError("model_unavailable", "M4 model client is unavailable")

        stage_names = policy["serving"]["stages"]
        for stage_index, stage in enumerate(stage_names, start=1):
            if time.perf_counter() - started > float(policy["serving"]["case_wall_seconds"]):
                stage_rows.append({"stage": stage, "status": "failed_budget",
                                   "error": {"code": "case_wall_budget", "message": "case wall-time budget exhausted"}})
                break
            runtime = build_m4_runtime(case.alert, root=root, policy_path=policy_path)
            base_prompt = _stage_prompt(stage, digest=digest, registry=registry,
                snapshot=snapshot, runtime=runtime, history=history, policy=policy,
                query_budget=query_budget)
            prompt = base_prompt
            repair_count = 0
            previous = ""
            static_errors: list[dict[str, Any]] = []
            document: dict[str, Any] | None = None
            verified: VerifiedDAG | None = None
            runtime_for_stage = runtime
            stage_name = f"stage{stage_index}_{stage}"
            stage_dir = run_dir / "attempts" / stage_name
            stage_dir.mkdir()
            _write_json(status_path, {"phase": stage_name, "status": "running"}, exclusive=False)

            while True:
                # A failed repair must never leave a previously verified candidate eligible to run.
                document = None
                verified = None
                playbook_name = None
                if time.perf_counter() - started > float(policy["serving"]["case_wall_seconds"]):
                    stage_rows.append({"stage": stage_name, "status": "failed_budget",
                        "static_repairs": repair_count,
                        "static_errors": static_errors,
                        "error": {"code": "case_wall_budget", "message": "case wall-time budget exhausted"}})
                    break
                if repair_count == 0:
                    call_attempt = 1
                else:
                    call_attempt = repair_count + 1
                prompt_path = run_dir / "prompts" / f"{stage_name}_call{call_attempt}.txt"
                prompt_path.write_text(prompt, encoding="utf-8")
                call = model.complete(prompt, task_id=f"m4_{case_id}_{stage_name}",
                                      attempt=call_attempt)
                call_doc = call.as_dict()
                call_doc["m4_stage"] = stage_name
                call_doc["m4_static_repair"] = repair_count > 0
                call_doc["prompt_file"] = str(prompt_path.relative_to(run_dir))
                calls.append(call_doc)
                _append_jsonl(calls_path, call_doc)
                if call.error:
                    stage_rows.append({"stage": stage_name, "status": "model_call_failed",
                        "static_repairs": repair_count, "static_errors": static_errors,
                        "call_id": call.call_id,
                        "error": call.error})
                    break
                if call.response_metadata.get("finish_reason") in {"length", "max_tokens"}:
                    stage_rows.append({"stage": stage_name, "status": "truncated",
                        "static_repairs": repair_count, "static_errors": static_errors,
                        "call_id": call.call_id,
                        "error": {"code": "model_output_truncated", "message": "response ended at token limit"}})
                    break
                previous = call.content or ""
                try:
                    response, _fenced = parse_strict_json_object(previous)
                    if stage == "select":
                        if set(response) != {"playbook", "runtime_overrides", "node_parameters", "node_bindings"}:
                            raise M4ControllerError("stage1_schema", "Stage 1 response keys differ from the frozen schema", static=True)
                        runtime_for_stage = _normalize_overrides(response, case, root, policy)
                        document, playbook_name = _stage1_doc(response, snapshot, registry, runtime_for_stage)
                        verified = _validate_graph(document, registry, runtime_for_stage, stage=stage)
                    elif stage == "compose":
                        if set(response) != {"dag", "runtime_overrides"}:
                            raise M4ControllerError("stage2_schema", "Stage 2 response keys differ from the frozen schema", static=True)
                        runtime_for_stage = _normalize_overrides(response, case, root, policy)
                        document = response["dag"]
                        verified = _validate_graph(document, registry, runtime_for_stage, stage=stage)
                        playbook_name = None
                    else:
                        if set(response) != {"operators", "dag", "runtime_overrides"}:
                            raise M4ControllerError("stage3_schema", "Stage 3 response keys differ from the frozen schema", static=True)
                        raw_specs = response["operators"]
                        if not isinstance(raw_specs, list) or len(raw_specs) > int(policy["serving"].get("maximum_new_operators", 3)):
                            raise M4ControllerError("stage3_operator_budget", "Stage 3 operator proposals exceed the frozen limit", static=True)
                        runtime_for_stage = _normalize_overrides(response, case, root, policy)
                        normalized_new_specs, version_map = _candidate_spec_mapping(raw_specs)
                        entity_literals = set(case.telemetry.deployment_catalog) | {case_id}
                        for generated_spec in normalized_new_specs:
                            reject_identifier_literals(generated_spec["source"], entity_literals)
                        new_keys = {f"{spec['name']}@{spec['version']}" for spec in normalized_new_specs}
                        resolved_doc = _resolve_generated_versions(response["dag"], version_map)
                        existing_keys = {f"{spec['name']}@{spec['version']}" for spec in specs}
                        if new_keys & existing_keys:
                            raise M4ControllerError("stage3_operator_collision", "Stage 3 proposed an already-retained operator version", static=True)
                        candidate_registry = make_m4_registry(case, [*specs, *normalized_new_specs],
                            root=root, audit_dir=run_dir / "audit",
                            shared_query_budget=query_budget)
                        candidate_document = resolved_doc
                        candidate_verified = _validate_graph(candidate_document, candidate_registry, runtime_for_stage,
                            stage=stage, new_operator_keys=new_keys)
                        # Validate against the exact registry that will execute it, without
                        # mutating the live registry until every static check has succeeded.
                        final_registry = make_m4_registry(case, specs, root=root,
                            audit_dir=run_dir / "audit", shared_query_budget=query_budget)
                        register_generated_specs(final_registry, normalized_new_specs,
                                                 audit_dir=run_dir / "audit")
                        candidate_verified = _validate_graph(candidate_document, final_registry, runtime_for_stage,
                            stage=stage, new_operator_keys=new_keys)
                        for generated_spec in normalized_new_specs:
                            safe_name = generated_spec["name"].replace(".", "_")
                            source_path = run_dir / "operators" / (
                                f"{safe_name}_{generated_spec['source_sha256'][:16]}.py")
                            source_path.write_text(generated_spec["source"], encoding="utf-8")
                            _write_json(source_path.with_suffix(".json"),
                                {key: value for key, value in generated_spec.items() if key != "source"})
                        response["normalized_operators"] = normalized_new_specs
                        response["declared_to_resolved_versions"] = [
                            {"name": name, "declared_version": declared, "resolved_version": resolved}
                            for (name, declared), resolved in sorted(version_map.items())]
                        registry = final_registry
                        document, verified = candidate_document, candidate_verified
                        playbook_name = None
                    assert document is not None and verified is not None
                    break
                except (StaticTaskError, DAGValidationError, M4ControllerError, ValueError) as exc:
                    document = None
                    verified = None
                    playbook_name = None
                    error = exc.as_dict() if hasattr(exc, "as_dict") else {
                        "code": getattr(exc, "code", "static_validation_failed"), "message": str(exc)}
                    if not getattr(exc, "static", True):
                        raise
                    static_errors.append(error)
                    if repair_count >= int(policy["serving"]["static_repair_limit_per_attempt"]):
                        stage_rows.append({"stage": stage_name, "status": "static_validation_failed",
                            "static_repairs": repair_count, "static_errors": static_errors,
                            "call_id": call.call_id})
                        break
                    repair_count += 1
                    repair_context = _stage_prompt(stage, digest={}, registry=registry,
                        snapshot=snapshot, runtime=runtime_for_stage, history=[], policy=policy,
                        query_budget=query_budget, include_case_digest=False, include_history=False)
                    prompt = _static_repair_prompt(repair_context, previous, error)

            if document is None or verified is None:
                _write_json(stage_dir / "stage_record.json", stage_rows[-1], exclusive=False)
                history.append({"stage": stage_name, "status": stage_rows[-1].get("status", "failed"),
                                "accepted": None})
                continue

            dag_path = run_dir / "dags" / f"{stage_name}.json"
            _write_json(dag_path, document)
            runtime_path = stage_dir / "runtime.json"
            _write_json(runtime_path, runtime_for_stage.as_dict())
            try:
                execution = SerialExecutor(registry, CacheStore(run_dir / "cache",
                    enabled=True)).execute_verified(verified, runtime_for_stage,
                    {next(node.node_id for node in verified.nodes if node.operator.external_input): case.telemetry},
                    case_id=case_id, data_sha256=case.telemetry.data_sha256,
                    file_sha256=case.telemetry.file_sha256, cache_enabled=True)
            except ExecutionFailure as exc:
                used_operator_keys.update(f"{record.operator}@{record.version}" for record in exc.records
                    if record.status in {"success", "failed"})
                result_record = {"stage": stage_name, "status": "runtime_execution_failed",
                    "static_repairs": repair_count, "static_errors": static_errors,
                    "dag_sha256": sha256_file(dag_path), "runtime": runtime_for_stage.as_dict(),
                    "execution": exc.as_dict()}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                history.append({"stage": stage_name, "status": "execution_failed",
                                "accepted": None, "runtime": runtime_for_stage.as_dict()})
                continue

            used_operator_keys.update(f"{node.operator.name}@{node.operator.version}" for node in verified.nodes)
            if playbook_name is not None:
                used_playbooks.add(playbook_name)
            execution_path = stage_dir / "execution.json"
            execution_doc = execution_result_to_dict(execution)
            _write_json(execution_path, execution_doc)
            if not isinstance(execution.answer, RankedAnswer):
                result_record = {"stage": stage_name, "status": "wrong_answer_type",
                    "static_repairs": repair_count, "dag_sha256": sha256_file(dag_path),
                    "execution": execution_doc}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                history.append({"stage": stage_name, "status": "invalid_answer", "accepted": None})
                continue
            try:
                validate_bank_answer(execution.answer, case,
                    maximum_leads=int(policy["serving"]["maximum_leads"]))
            except M4ControllerError as exc:
                result_record = {"stage": stage_name, "status": "invalid_answer_output",
                    "static_repairs": repair_count, "dag_sha256": sha256_file(dag_path),
                    "execution": execution_doc, "error": exc.as_dict()}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                history.append({"stage": stage_name, "status": "invalid_answer_output",
                                "accepted": None, "error_code": exc.code})
                continue
            try:
                feedback_result = _call_feedback(feedback, case_id, execution.answer)
            except Exception as exc:
                result_record = {"stage": stage_name, "status": "feedback_failed",
                    "static_repairs": repair_count, "dag_sha256": sha256_file(dag_path),
                    "execution": execution_doc,
                    "error": {"code": getattr(exc, "code", "feedback_failed"), "message": str(exc)}}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                raise M4ControllerError("feedback_failed", "trusted evaluator did not return a valid binary response") from exc
            accepted = feedback_result["accepted"]
            final_answer = execution.answer
            result_record = {"stage": stage_name, "status": "accepted" if accepted else "rejected",
                "accepted": accepted, "static_repairs": repair_count, "static_errors": static_errors,
                "dag_sha256": sha256_file(dag_path), "playbook_name": playbook_name,
                "runtime": runtime_for_stage.as_dict(), "execution": execution_doc,
                "generated_operator_keys": (sorted(new_keys) if stage == "synthesize" else []),
                "query_budget": {"requests": query_budget.requests, "rows": query_budget.rows,
                                 "bytes": query_budget.bytes}}
            stage_rows.append(result_record)
            _write_json(stage_dir / "stage_record.json", result_record)
            history.append({"stage": stage_name, "status": "executed", "accepted": accepted,
                "ranked_answer": _bounded_answer_history(execution.answer),
                "runtime_values": runtime_for_stage.as_dict()["values"]})

            if accepted:
                if stage == "synthesize":
                    graph_hash = playbook_graph_hash(document)
                    generated_playbook = {"name": f"accepted_m4_{graph_hash[:12]}",
                        "document": document,
                        "provenance": {"source": "accepted_stage3", "model_alias": profile.get("model_alias"),
                            "stage": stage_name, "verified_graph_sha256": graph_hash}}
                    reachable = {f"{node.operator.name}@{node.operator.version}" for node in verified.nodes}
                    admitted = admit_after_accepted(snapshot, response["normalized_operators"],
                        [generated_playbook], {"model_alias": profile.get("model_alias"),
                            "run_manifest_sha256": _canonical_hash(run_doc),
                            "implementation_fingerprint": _implementation_fingerprint()},
                        accepted=True, verified_reachable_operator_keys=reachable,
                        invocation_case_id=case_id, used_operator_keys=used_operator_keys,
                        used_playbook_names=used_playbooks | {generated_playbook["name"]},
                        verified_playbook_graph_hashes={graph_hash})
                    snapshot = admitted
                    last_snapshot = admitted
                else:
                    snapshot = record_library_invocation(snapshot,
                        invocation_case_id=case_id, used_operator_keys=used_operator_keys,
                        used_playbook_names=used_playbooks)
                    last_snapshot = snapshot
                break

        if not accepted and snapshot is not None:
            known = set(snapshot.operators)
            snapshot = record_library_invocation(snapshot, invocation_case_id=case_id,
                used_operator_keys=used_operator_keys & known,
                used_playbook_names=used_playbooks & set(snapshot.playbooks))
            last_snapshot = snapshot

        complete = {"schema_version": "e4-m4-diagnostic-run/1", "case_id": case_id,
            "status": "accepted" if accepted else "exhausted_or_failed",
            "accepted": accepted,
            "accepted_stage": next((row["stage"] for row in stage_rows if row.get("accepted") is True), None),
            "stages": stage_rows, "model_calls": _model_cost(calls),
            "query_budget": {"requests": query_budget.requests, "rows": query_budget.rows,
                             "bytes": query_budget.bytes},
            "library_before_snapshot_id": source_snapshot.snapshot_id,
            "library_after_snapshot_id": last_snapshot.snapshot_id if last_snapshot else None,
            "implementation_fingerprint": _implementation_fingerprint(),
            "wall_seconds": time.perf_counter() - started,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "diagnostic_result.json", complete)
        _write_json(status_path, {"phase": "complete", "status": complete["status"]}, exclusive=False)
        return {"result": complete, "snapshot": last_snapshot,
                "run_dir": str(run_dir), "answer": final_answer}
    except Exception as exc:
        error = {"code": getattr(exc, "code", "m4_controller_failed"), "message": str(exc)}
        failure = {"schema_version": "e4-m4-diagnostic-run/1", "case_id": case_id,
            "status": "failed", "accepted": False, "stages": stage_rows,
            "model_calls": _model_cost(calls), "error": error,
            "library_snapshot_id": last_snapshot.snapshot_id if last_snapshot else None,
            "wall_seconds": time.perf_counter() - started,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        if not (run_dir / "diagnostic_result.json").exists():
            _write_json(run_dir / "diagnostic_result.json", failure)
        _write_json(status_path, {"phase": "failed", "status": "failed", "error": error}, exclusive=False)
        raise
