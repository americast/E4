"""Version-2 extension worker for the trusted typed M4 operator IR.

This extension shares the immutable v1 kernel implementation via the read-only
worker bundle. It adds per-node availability/abstention tracking without
modifying the v1 interpreter or its source pin.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Literal, Mapping, cast

_BUNDLE = Path(__file__).resolve().parent
if __package__:
    from . import m4_operator_ir_worker as _v1
else:
    if str(_BUNDLE) not in sys.path:
        sys.path.insert(0, str(_BUNDLE))
    import m4_operator_ir_worker as _v1  # trusted, read-only bundle dependency

PROFILE = "typed-operator-ir-v2"
IR_SCHEMA = "e4-typed-operator-ir/2"
ENGINE_ABI = "e4-typed-operator-ir-v2-kernel-abi/1"
BASE_WORKER = _BUNDLE / "m4_operator_ir_worker.py"
BASE_WORKER_SHA256 = hashlib.sha256(BASE_WORKER.read_bytes()).hexdigest()

KERNELS = copy.deepcopy(_v1.KERNELS)
KERNELS.pop("series.select_field@1", None)
KERNELS["series.observed_kpi@1"] = {
    "inputs": {"reference": "Aggregate", "alert": "Aggregate"},
    "output": "KPISeries", "params": {}}
KERNELS["series.select_metric_rule@1"] = {
    "inputs": {"series": "ComponentSeries"}, "output": "ComponentSeries",
    "params": {"selector_ref": {"type": "string", "required": True}}}
KERNELS["series.window_association@1"] = {
    "inputs": {"pairs": "AlignedSeriesPairs"}, "output": "WindowSummaries",
    "params": {"method": {"type": "string", "required": True,
            "choices": ["spearman"]},
        "minimum_pairs": {"type": "positive_integer", "required": True,
            "runtime_field": "minimum_support"}}}
KERNELS["logs.pattern_counts@1"] = {
    "inputs": {"log_input": "OperatorIRLogInput", "candidates": "CandidateSet"},
    "output": "PatternCounts", "params": {}}
KERNELS["logs.select_pattern@1"] = {
    "inputs": {"counts": "PatternCounts"}, "output": "PatternCounts",
    "params": {"pattern_sha256": {"type": "string", "required": True},
        "log_name": {"type": "string", "required": True}}}
KERNELS["logs.exposure_rate@1"] = {
    "inputs": {"counts": "PatternCounts"}, "output": "WindowSummaries",
    "params": {"statistic": {"type": "string", "required": True,
            "choices": ["mean_rate", "variance_rate", "max_rate", "q90_rate"]},
        "minimum_blocks": {"type": "positive_integer", "required": True,
            "runtime_field": "minimum_independent_blocks"}}}
KERNELS["series.normalize_reference_scale@1"] = {
    "inputs": {"series": "ComponentSeries"}, "output": "NormalizedSeries",
    "params": {"block_minutes": {"type": "positive_integer", "required": True,
            "runtime_field": "block_minutes"},
        "minimum_blocks": {"type": "positive_integer", "required": True,
            "runtime_field": "minimum_independent_blocks"},
        "relative_scale_floor": {"type": "number", "required": True,
            "runtime_field": "relative_scale_floor"},
        "absolute_scale_floor": {"type": "number", "required": True,
            "runtime_field": "absolute_scale_floor"}}}
KERNELS["series.reference_quantile_exceedance@1"] = {
    "inputs": {"series": "NormalizedSeries"}, "output": "WindowSummaries",
    "params": {"tail_quantile": {"type": "number", "required": True,
            "runtime_field": "tail_quantile"}}}
KERNELS["series.sigma_exceedance@1"] = {
    "inputs": {"series": "NormalizedSeries"}, "output": "WindowSummaries",
    "params": {"sigma_multiple": {"type": "number", "required": True,
            "choices": [2.0, 3.0]}}}

_TRACE: list[dict[str, Any]] = []
_CURRENT: dict[str, Any] = {"index": 0, "program": None}
_BASE_INVOKE = _v1._invoke
_BASE_WIRE_SCORED = _v1._wire_scored
_SERIES_TRACE_SAMPLE_LIMIT = 32


def _safe_reason(reason: Any) -> str:
    if not isinstance(reason, str) or not reason:
        return "computation_unavailable"
    cleaned = "".join(char.lower() if char.isalnum() else "_" for char in reason)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")[:96] or "computation_unavailable"


def _availability(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        direct = value.get("availability")
        if isinstance(direct, Mapping):
            return direct
        upstream = value.get("upstream_abstention")
        if isinstance(upstream, Mapping):
            return {"computability_state": "not_computable",
                "ranking_state": "not_rankable", "upstream_abstention": upstream}
    return None


def _input_abstention(inputs: Mapping[str, Any]) -> Mapping[str, Any] | None:
    found: list[tuple[int, str, Mapping[str, Any]]] = []
    for port in sorted(inputs):
        row = _availability(inputs[port])
        if row and row.get("computability_state") == "not_computable":
            origin = row.get("upstream_abstention")
            if isinstance(origin, Mapping):
                origin_row = origin
            else:
                chain = row.get("chain", ())
                origin_row = {"code": row.get("reason_code", "upstream_not_computable"),
                    "origin_kernel": row.get("origin_kernel", "unknown"),
                    "origin_node": row.get("origin_node"), "chain": list(chain),
                    "origin_node_index": row.get("origin_node_index", 0)}
            index = origin_row.get("origin_node_index", 0)
            found.append((index if type(index) is int else 0, port, origin_row))
    return min(found, key=lambda item: (item[0], item[1]))[2] if found else None


def _reason_origin(result: Mapping[str, Any], kernel: str, node_id: str,
                   inputs: Mapping[str, Any]) -> tuple[str, Mapping[str, Any] | None]:
    prior = _input_abstention(inputs)
    if prior is not None:
        return str(prior.get("code", "upstream_not_computable")), prior
    code = _safe_reason(result.get("reason_code", result.get("reason")))
    return code, None


def _count_input_data(inputs: Mapping[str, Any]) -> bool:
    for value in inputs.values():
        if isinstance(value, Mapping):
            rows = value.get("rows")
            if isinstance(rows, list) and rows:
                return True
            fields = value.get("$fields")
            if isinstance(fields, Mapping):
                for child in fields.values():
                    if _count_input_data({"value": child}):
                        return True
            for child in value.values():
                if _count_input_data({"value": child}):
                    return True
        elif isinstance(value, list) and value:
            return True
    return False


def _empty_output(output_type: str, *, reason: str,
                  availability: Mapping[str, Any]) -> dict[str, Any]:
    if output_type == "ScoredCandidates":
        return {"status": "abstained", "reason": reason,
            "candidates": [], "availability": dict(availability)}
    return {"status": "abstained", "reason": reason,
        "rows": [], "availability": dict(availability)}


def _series_trace_summary(rows: list[Any]) -> dict[str, Any]:
    """Bound per-feature trace examples while pinning the full diagnostic rows."""
    encoded = json.dumps(rows, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    reason_counts: dict[str, int] = {}
    computability_counts = {"computable": 0, "not_computable": 0, "unknown": 0}
    support_counts: dict[str, int] = {}
    source_hashes: set[tuple[str, str]] = set()
    case_data_hashes: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        computability = ("computable" if row.get("computable") is True else
            "not_computable" if row.get("computable") is False else "unknown")
        computability_counts[computability] += 1
        reason = row.get("reason") or "none"
        reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1
        counts = row.get("support_counts", {})
        support_key = json.dumps(counts, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False)
        support_counts[support_key] = support_counts.get(support_key, 0) + 1
        for record_key in ("source_record", "alert_record"):
            record = row.get(record_key)
            if not isinstance(record, Mapping):
                continue
            hashes = record.get("source_hashes", {})
            if isinstance(hashes, Mapping):
                source_hashes.update((str(name), str(digest))
                    for name, digest in hashes.items())
            data_hash = record.get("case_data_sha256")
            if isinstance(data_hash, str):
                case_data_hashes.add(data_hash)
    support_histogram = [{"support_counts": json.loads(key), "rows": count}
        for key, count in sorted(support_counts.items())]
    return {"schema_version": "e4-v2-series-trace-summary/1",
        "total_rows": len(rows),
        "shown_rows": min(len(rows), _SERIES_TRACE_SAMPLE_LIMIT),
        "omitted_rows": max(0, len(rows) - _SERIES_TRACE_SAMPLE_LIMIT),
        "all_rows_sha256": hashlib.sha256(encoded).hexdigest(),
        "computability_counts": computability_counts,
        "reason_counts": dict(sorted(reason_counts.items())),
        "support_counts_histogram": support_histogram,
        "source_sha256": [{"source": name, "sha256": digest}
            for name, digest in sorted(source_hashes)],
        "case_data_sha256": sorted(case_data_hashes)}


def _support_details(kernel: str, result: Mapping[str, Any],
        inputs: Mapping[str, Any], trusted: Mapping[str, Any]) -> dict[str, Any]:
    """Keep exact support and constant-series facts alongside each node state."""
    rows = result.get("rows", [])
    details: dict[str, Any] = {}
    if kernel in {"series.primary_kpi", "series.observed_kpi"}:
        primary_minimum_value = trusted.get("primary_kpi_minimum_support")
        primary_minimum: int | None = (primary_minimum_value
            if type(primary_minimum_value) is int else None)
        candidates: list[dict[str, Any]] = []
        if not rows:
            allowlist = trusted.get("primary_metric_allowlist", [])
            try:
                raw_ref = _v1._aggregate(inputs["reference"], "reference")[0]
                raw_alt = _v1._aggregate(inputs["alert"], "alert")[0]
                summaries: dict[tuple[str, str], dict[str, int]] = {}
                for side, aggregate in (("reference", raw_ref), ("alert", raw_alt)):
                    for row in aggregate.get("rows", []):
                        if row.get("statistic") != "median":
                            continue
                        role, identifier, _key = _v1._metric_key(row)
                        source = row.get("details") or {}
                        field = f"metric_app.{source.get('source_field')}"
                        if (role == "app_kpi" and source.get("source_table") == "bank/metric_app.csv"
                                and field in allowlist):
                            summaries.setdefault((field, identifier), {})[side] = len(
                                _v1._samples(source))
                for (field, identifier), support in summaries.items():
                    candidates.append({"opaque_id": identifier, "field": field,
                        "reference_unique_timestamps": support.get("reference", 0),
                        "alert_unique_timestamps": support.get("alert", 0)})
                candidates.sort(key=lambda item: (-min(item["reference_unique_timestamps"],
                    item["alert_unique_timestamps"]), item["opaque_id"].casefold(),
                    item["opaque_id"], item["field"]))
            except (KeyError, TypeError, ValueError):
                candidates = []
        for row in rows[:128]:
            windows = {}
            for side in ("reference", "alert"):
                values = [value for _timestamp, value in row[side].get("samples", [])]
                windows[side] = {"finite_unique_timestamps": len(values),
                    "constant": len(set(values)) <= 1 if values else None}
            meets_policy = None
            if primary_minimum is not None:
                reference_support = cast(int, windows["reference"]["finite_unique_timestamps"])
                alert_support = cast(int, windows["alert"]["finite_unique_timestamps"])
                meets_policy = min(reference_support, alert_support) >= cast(int, primary_minimum)
            candidates.append({"opaque_id": row.get("key", (None, None))[1],
                "field": row["reference"].get("source_field"), "windows": windows,
                "meets_primary_policy": meets_policy})
        details.update({"minimum_primary_policy_support_per_window": primary_minimum,
            "selected_opaque_id": (rows[0].get("key", (None, None))[1] if rows else None),
            "kpi_support_by_candidate": candidates,
            "support_rows_omitted": max(0, len(rows) - 128)})
    elif kernel == "series.select_metric_rule":
        details["selector_ref"] = result.get("selector_ref")
        details["selector_resolution"] = result.get("selector_resolution")
        details["matched_field_count"] = result.get("matched_field_count", 0)
        details["selection_semantics"] = result.get("field_selection",
            "all exact observed matches; no first-match selection")
    elif kernel == "series.component_metrics":
        series_rows = []
        for row in rows[:256]:
            windows = {}
            for side in ("reference", "alert"):
                samples = row[side].get("samples", [])
                values = [value for _timestamp, value in samples]
                windows[side] = {"finite_unique_timestamps": len(samples),
                    "constant": len(set(values)) <= 1 if values else None}
            series_rows.append({"component_id": row["key"][1],
                "field": row["reference"].get("source_field"), "windows": windows})
        details.update({"series_support": series_rows,
            "support_rows_omitted": max(0, len(rows) - 256)})
    elif kernel in {"series.normalize_reference_scale",
                    "series.reference_quantile_exceedance", "series.sigma_exceedance"}:
        summary = _series_trace_summary(rows)
        details["series_results"] = [{
            "component_id": row.get("component_id"), "field": row.get("source_field"),
            "direction": row.get("direction") or (row.get("metric_semantics") or {}).get("direction"),
            "reference_support": row.get("windows", {}).get("reference", {}).get("support"),
            "alert_support": row.get("windows", {}).get("alert", {}).get("support"),
            "reference_center": row.get("reference_center"),
            "reference_mad": row.get("reference_mad"),
            "robust_reference_scale": row.get("robust_reference_scale"),
            "effective_scale": row.get("effective_scale"),
            "reference_exceedance_rate": row.get("reference_exceedance_rate"),
            "alert_exceedance_rate": row.get("alert_exceedance_rate"),
            "reference_exceedance_count": row.get("reference_exceedance_count"),
            "alert_exceedance_count": row.get("alert_exceedance_count"),
            "reference_threshold": row.get("reference_threshold"),
            "alert_threshold": row.get("alert_threshold"),
            "sigma_thresholds": row.get("thresholds"),
            "tail_quantile": row.get("tail_quantile"),
            "numeric_unit": row.get("details", {}).get("numeric_unit"),
            "source_unit": row.get("details", {}).get("source_unit"),
            "support_counts": dict(row.get("support_counts", {})),
            "exceedance_details": dict(row.get("details", {})),
            "computable": row.get("computable"),
            "reason": row.get("reason")}
            for row in rows[:_SERIES_TRACE_SAMPLE_LIMIT]]
        details["series_trace_summary"] = summary
        details["series_rows_omitted"] = summary["omitted_rows"]
    elif kernel == "series.align_kpi_both_windows":
        series_rows = []
        for row in rows[:256]:
            series = row["series"]
            ref_component = dict(series["reference"]["samples"])
            alt_component = dict(series["alert"]["samples"])
            ref_kpi = dict(row["kpi"]["reference"]["samples"])
            alt_kpi = dict(row["kpi"]["alert"]["samples"])
            ref_shared = sorted(set(ref_component) & set(ref_kpi))
            alt_shared = sorted(set(alt_component) & set(alt_kpi))
            pair_counts = row.get("reference_pair_count", len(ref_shared)), row.get(
                "alert_pair_count", len(alt_shared))
            series_rows.append({"component_id": row["key"][1],
                "field": series["reference"].get("source_field"),
                "kpi_id": row["kpi"]["reference"].get("entity"),
                "exact_shared_pairs": {"reference": pair_counts[0], "alert": pair_counts[1]},
                "constant_flags": {"reference": {
                    "component": len({ref_component[t] for t in ref_shared}) <= 1 if ref_shared else None,
                    "kpi": len({ref_kpi[t] for t in ref_shared}) <= 1 if ref_shared else None},
                    "alert": {
                    "component": len({alt_component[t] for t in alt_shared}) <= 1 if alt_shared else None,
                    "kpi": len({alt_kpi[t] for t in alt_shared}) <= 1 if alt_shared else None}}})
        details.update({"timestamp_scope": result.get("timestamp_scope"),
            "series_support": series_rows,
            "support_rows_omitted": max(0, len(rows) - 256)})
    elif kernel == "series.correlate":
        details["correlation_rows"] = [{
            "component_id": row.get("key", (None, None))[1],
            "correlation": row.get("details", {}).get("correlation"),
            "method": row.get("details", {}).get("method"),
            "exact_shared_pairs_by_window": dict(row.get("support_counts", {})),
            "confidence_state": row.get("details", {}).get("confidence_state", "unknown"),
            "causal_attribution": row.get("details", {}).get("causal_attribution", "not_established")}
            for row in rows[:256]]
        details["support_rows_omitted"] = max(0, len(rows) - 256)
    elif kernel == "series.window_association":
        details["association_rows"] = [{"component_id": row["key"][1],
            "reference_rho": row["details"].get("reference_rho"),
            "alert_rho": row["details"].get("alert_rho"),
            "correlation_change": row["details"].get("correlation_change"),
            "numeric_unit": row["details"].get("numeric_unit"),
            "source_units": dict(row["details"].get("source_units", {})),
            "support_counts": dict(row.get("support_counts", {})),
            "constant_by_window": row["details"].get("constant_by_window"),
            "confidence_state": row["details"].get("confidence_state"),
            "causal_attribution": row["details"].get("causal_attribution")}
            for row in rows[:256]]
        details["rows_omitted"] = max(0, len(rows) - 256)
    elif kernel in {"logs.pattern_counts", "logs.select_pattern", "logs.exposure_rate"}:
        pattern_rows = []
        for row in rows[:256]:
            source = row.get("source_record", row)
            summary = row.get("details", {}) if kernel == "logs.exposure_rate" else {}
            before = row.get("windows", {}).get("reference", {})
            after = row.get("windows", {}).get("alert", {})
            pattern_rows.append({"component_id": source.get("component_id"),
                "log_name": source.get("log_name"), "pattern_sha256": source.get("pattern_sha256"),
                "reference_pattern_rows": (summary.get("reference_pattern_rows")
                    if kernel == "logs.exposure_rate" else before.get("pattern_rows")),
                "reference_total_rows": (summary.get("reference_total_rows")
                    if kernel == "logs.exposure_rate" else before.get("total_rows")),
                "alert_pattern_rows": (summary.get("alert_pattern_rows")
                    if kernel == "logs.exposure_rate" else after.get("pattern_rows")),
                "alert_total_rows": (summary.get("alert_total_rows")
                    if kernel == "logs.exposure_rate" else after.get("total_rows")),
                "reference_exposure_rate": (summary.get("reference_exposure_rate")
                    if kernel == "logs.exposure_rate" else before.get("rate")),
                "reference_statistic_value": summary.get("reference_statistic_value"),
                "alert_exposure_rate": (summary.get("alert_exposure_rate")
                    if kernel == "logs.exposure_rate" else after.get("rate")),
                "alert_statistic_value": summary.get("alert_statistic_value"),
                "statistic": summary.get("statistic"),
                "ranking_state": summary.get("ranking_state", "unknown"),
                "reference_exact_blocks": (summary.get("reference_exact_blocks")
                    if kernel == "logs.exposure_rate" else len(before.get("block_denominators", {}))),
                "alert_exact_blocks": (summary.get("alert_exact_blocks")
                    if kernel == "logs.exposure_rate" else len(after.get("block_denominators", {}))),
                "numeric_unit": summary.get("numeric_unit"),
                "source_unit": summary.get("source_unit"),
                "support_counts": dict(row.get("support_counts", {})),
                "computable": row.get("computable")})
        details["patterns"] = pattern_rows
        details["pattern_rows_omitted"] = max(0, len(rows) - 256)
    return details


def _invoke_v2(kernel: str, inputs: Mapping[str, Any], params: Mapping[str, Any],
               runtime: Mapping[str, Any], trusted: Mapping[str, Any]) -> dict[str, Any]:
    index = int(_CURRENT["index"])
    nodes = _CURRENT.get("program", {}).get("nodes", [])
    node = nodes[index] if index < len(nodes) else {}
    _CURRENT["index"] = index + 1
    node_id = str(node.get("id", f"n{index}"))
    output_type = KERNELS[f"{kernel}@{node['version']}"]["output"]
    upstream = _input_abstention(inputs)
    if upstream is not None:
        origin = {**dict(upstream), "chain": [*list(upstream.get("chain", [])), kernel]}
        result = _empty_output(output_type, reason=str(origin.get("code", "upstream_not_computable")),
            availability={"data_state": "partial", "computability_state": "not_computable",
                "ranking_state": "not_rankable", "reason_code": origin["code"],
                "origin_kernel": origin.get("origin_kernel"), "origin_node": origin.get("origin_node"),
                "origin_node_index": origin.get("origin_node_index", 0),
                "upstream_abstention": origin})
        _TRACE.append({"node_id": node_id, "kernel": kernel,
            "availability": dict(result["availability"])})
        return result

    if kernel == "series.observed_kpi":
        result = _observed_kpi(inputs, trusted)
    elif kernel == "series.component_metrics":
        result = _component_series_v2(inputs)
    elif kernel == "series.select_metric_rule":
        result = _select_metric_rule(inputs["series"], params["selector_ref"], trusted)
    elif kernel == "series.align_kpi_both_windows":
        result = _align_kpi_both_windows_v2(inputs["components"], inputs["kpi"])
    elif kernel == "series.window_association":
        result = _window_association(inputs["pairs"], params["method"], params["minimum_pairs"])
    elif kernel == "series.normalize_reference_scale":
        result = _normalize_reference_scale(inputs["series"], params, runtime, trusted)
    elif kernel == "series.reference_quantile_exceedance":
        result = _reference_quantile_exceedance(inputs["series"], params["tail_quantile"])
    elif kernel == "series.sigma_exceedance":
        result = _sigma_exceedance(inputs["series"], params["sigma_multiple"])
    elif kernel == "logs.pattern_counts":
        result = _log_pattern_counts(inputs, trusted)
    elif kernel == "logs.select_pattern":
        result = _log_select_pattern(inputs["counts"], params)
    elif kernel == "logs.exposure_rate":
        result = _log_exposure_rate(inputs["counts"], params["statistic"],
            params["minimum_blocks"])
    elif kernel == "evidence.from_scores":
        for score in inputs.get("scores", {}).get("rows", []):
            source_record = score.get("source_record", {})
            if source_record.get("source_table") == "bank/log/log_service.csv":
                score.setdefault("details", {})["metric_rule_id"] = source_record.get("metric_rule_id")
                score["details"]["metric_rule_ref"] = source_record.get("metric_rule_ref")
        result = _BASE_INVOKE(kernel, inputs, params, runtime, trusted)
        for row in result.get("rows", []):
            if (row.get("source") == "bank/log/log_service.csv" and
                    row.get("details", {}).get("metric_field") == "log_pattern_exposure_rate"):
                details = row["details"]
                details["policy_provenance"] = "shared_exact_bank_metric_classifier"
                details["metric_rule_id"] = row.get("details", {}).get("metric_rule_id")
                details["metric_rule_ref"] = row.get("details", {}).get("metric_rule_ref")
    elif kernel == "series.correlate":
        # Correlation is a descriptive association feature. Its support and
        # confidence state travel separately; association is not causality.
        result = _BASE_INVOKE(kernel, inputs, params, runtime, trusted)
        for row in result.get("rows", []):
            row["details"]["ranking_state"] = "unknown"
            row["details"]["confidence_state"] = "not_assessed_by_this_kernel"
            row["details"]["causal_attribution"] = "not_established"
        result["ranking_state"] = "unknown"
    else:
        result = _BASE_INVOKE(kernel, inputs, params, runtime, trusted)

    if not isinstance(result, dict):
        _v1._fail("ir_kernel_output", f"{kernel} returned a non-object")
    status = result.get("status", "ready")
    if status == "abstained":
        code, prior = _reason_origin(result, kernel, node_id, inputs)
        if prior is None:
            upstream_row = {"code": code, "origin_kernel": kernel,
                "origin_node": node_id, "origin_node_index": index, "chain": [kernel]}
        else:
            upstream_row = {**dict(prior), "chain": [*list(prior.get("chain", [])), kernel]}
        availability = {"data_state": "partial" if _count_input_data(inputs) else "absent",
            "computability_state": "not_computable", "ranking_state": "not_rankable",
            "reason_code": code, "origin_kernel": upstream_row["origin_kernel"],
            "origin_node": upstream_row.get("origin_node"),
            "origin_node_index": upstream_row.get("origin_node_index", index),
            "upstream_abstention": upstream_row,
            "support_details": _support_details(kernel, result, inputs, trusted)}
        result["reason_code"] = code
        result["availability"] = availability
    else:
        availability = {"data_state": "present" if _count_input_data(inputs) or result.get("rows") else "unknown",
            "computability_state": "computable", "ranking_state": "unknown",
            "reason_code": None, "origin_kernel": kernel, "origin_node": node_id,
            "origin_node_index": index,
            "support_details": _support_details(kernel, result, inputs, trusted)}
        result["availability"] = availability
    _TRACE.append({"node_id": node_id, "kernel": kernel,
        "availability": dict(availability)})
    return result


def _observed_kpi(inputs: Mapping[str, Any], trusted: Mapping[str, Any]) -> dict[str, Any]:
    ref, _, ref_hashes, ref_data_sha = _v1._aggregate(inputs["reference"], "reference")
    alt, _, alt_hashes, alt_data_sha = _v1._aggregate(inputs["alert"], "alert")
    if ref_hashes != alt_hashes or ref_data_sha != alt_data_sha:
        _v1._fail("ir_provenance_pair", "reference and alert aggregates must share exact source and case-data pins")
    allowlist = trusted.get("primary_metric_allowlist")
    if not isinstance(allowlist, list) or not allowlist:
        _v1._fail("ir_trusted_policy", "observed KPI extraction lacks the trusted metric allowlist")
        raise ValueError("observed KPI extraction lacks the trusted metric allowlist")
    def collect(aggregate):
        rows = {}
        for row in aggregate.get("rows", []):
            if row.get("statistic") != "median":
                continue
            role, identifier, _key = _v1._metric_key(row)
            details = row.get("details") or {}
            field = f"metric_app.{details.get('source_field')}"
            if role != "app_kpi" or details.get("source_table") != "bank/metric_app.csv" or field not in allowlist:
                continue
            values = _v1._samples(details)
            if values:
                rows[(field, identifier)] = {"role": role, "entity": identifier,
                    "metric_key": row["metric_key"], "source_table": "bank/metric_app.csv",
                    "source_field": details.get("source_field"), "samples": values,
                    "support": len(values), "source_hashes": dict(aggregate["metadata"]["source_hashes"]),
                    "case_data_sha256": aggregate["metadata"].get("case_data_sha256"),
                    "window": dict(aggregate["window"])}
        return rows
    refs, alts = collect(ref), collect(alt)
    candidates = [(min(len(refs[key]["samples"]), len(alts[key]["samples"])), key)
        for key in set(refs) & set(alts)
        if refs[key]["samples"] and alts[key]["samples"]]
    if not candidates:
        return {"rows": [], "status": "abstained",
            "reason": "observed_kpi_not_present_with_finite_values_in_both_windows"}
    candidates.sort(key=lambda item: (-item[0], item[1][1].casefold(), item[1][1], item[1][0]))
    support, key = candidates[0]
    minimum = trusted.get("primary_kpi_minimum_support")
    eligible_by_primary_policy = (type(minimum) is int and
        min(len(refs[key]["samples"]), len(alts[key]["samples"])) >= minimum)
    return {"rows": [{"key": key, "reference": refs[key], "alert": alts[key],
        "support": support, "primary_policy_eligible": eligible_by_primary_policy}],
        "status": "ready", "selection_reason": "observed_support_first_separate_from_primary_policy",
        "primary_policy_minimum": minimum,
        "primary_policy_eligible": eligible_by_primary_policy}


def _align_kpi_both_windows_v2(components: Mapping[str, Any],
                               kpi: Mapping[str, Any]) -> dict[str, Any]:
    aligned = _v1._align_kpi_both_windows(components, kpi)
    for row in aligned.get("rows", []):
        series = row["series"]
        reference_component = dict(series["reference"]["samples"])
        alert_component = dict(series["alert"]["samples"])
        reference_kpi = dict(row["kpi"]["reference"]["samples"])
        alert_kpi = dict(row["kpi"]["alert"]["samples"])
        shared_reference = sorted(set(reference_component) & set(reference_kpi))
        shared_alert = sorted(set(alert_component) & set(alert_kpi))
        row["pairs_by_window"] = {
            "reference": [(timestamp, reference_component[timestamp], reference_kpi[timestamp])
                for timestamp in shared_reference],
            "alert": [(timestamp, alert_component[timestamp], alert_kpi[timestamp])
                for timestamp in shared_alert]}
        row["constant_by_window"] = {
            "reference": {"component": len({reference_component[ts] for ts in shared_reference}) <= 1
                    if shared_reference else None,
                "kpi": len({reference_kpi[ts] for ts in shared_reference}) <= 1
                    if shared_reference else None},
            "alert": {"component": len({alert_component[ts] for ts in shared_alert}) <= 1
                    if shared_alert else None,
                "kpi": len({alert_kpi[ts] for ts in shared_alert}) <= 1
                    if shared_alert else None}}
    return aligned


def _window_association(aligned: Mapping[str, Any], method: str,
                        minimum_pairs: int) -> dict[str, Any]:
    if method != "spearman" or type(minimum_pairs) is not int or minimum_pairs <= 0:
        _v1._fail("ir_window_association_parameters", "window association requires Spearman and positive support")
    output = []
    for row in aligned.get("rows", []):
        summaries = {}
        constants = row.get("constant_by_window", {})
        computable = True
        for window_name in ("reference", "alert"):
            pairs = row.get("pairs_by_window", {}).get(window_name, [])
            x, y = [pair[1] for pair in pairs], [pair[2] for pair in pairs]
            rho = None
            is_constant = constants.get(window_name, {})
            if (len(pairs) >= minimum_pairs and
                    is_constant.get("component") is False and is_constant.get("kpi") is False):
                rho = _v1._pearson(_v1._rank(x), _v1._rank(y))
            if rho is None:
                computable = False
            source = row["series"][window_name]
            summaries[window_name] = {"value": rho, "support": len(pairs),
                "window": source["window"], "record": source}
        ref_rho, alert_rho = summaries["reference"]["value"], summaries["alert"]["value"]
        output.append({"key": row["key"], "windows": summaries,
            "eligible": computable, "computable": computable,
            "kind": "descriptive_window_association", "source_record": row["series"]["reference"],
            "alert_record": row["series"]["alert"], "support": min(
                summaries["reference"]["support"], summaries["alert"]["support"]),
            "support_counts": {"reference_window_exact_pairs": summaries["reference"]["support"],
                "alert_window_exact_pairs": summaries["alert"]["support"]},
            "details": {"method": method, "reference_rho": ref_rho,
                "numeric_unit": "dimensionless",
                "source_units": {"component": row["series"]["reference"].get("unit", "undeclared"),
                    "kpi": row["kpi"]["reference"].get("unit", "undeclared")},
                "alert_rho": alert_rho,
                "correlation_change": (alert_rho - ref_rho if ref_rho is not None and
                    alert_rho is not None else None),
                "minimum_pairs_per_window": minimum_pairs,
                "constant_by_window": constants,
                "association_estimand": "descriptive change in exact-pair Spearman rho, alert minus reference",
                "confidence_state": "not_assessed_by_this_kernel",
                "causal_attribution": "not_established"}})
    ready = any(row["computable"] for row in output)
    return {"rows": output, "status": "ready" if ready else "abstained",
        "reason": None if ready else "window_association_under_supported_or_constant",
        "ranking_state": "unknown"}


def _component_series_v2(inputs: Mapping[str, Any]) -> dict[str, Any]:
    """Extract only trusted preclassified exact fields and supplied candidates."""
    rows = _v1._window_series(inputs["reference"], inputs["alert"],
        role="component", source_table="bank/metric_container.csv",
        only_eligible=True, candidates=inputs["candidates"])
    return {"rows": rows, "status": "ready" if rows else "abstained",
        "reason": None if rows else "no_exact_classified_component_metric_series"}


def _select_metric_rule(series: Mapping[str, Any], selector_ref: str,
        trusted: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(selector_ref, str) or not selector_ref:
        _v1._fail("ir_metric_selector", "metric selector must be a stable nonempty catalog reference")
    resolutions = trusted.get("metric_selector_resolutions", {})
    resolution = resolutions.get(selector_ref) if isinstance(resolutions, Mapping) else None
    if (not isinstance(resolution, Mapping) or
            resolution.get("selector_ref") != selector_ref or
            resolution.get("source_table") != "bank/metric_container.csv" or
            resolution.get("policy_sha256") != trusted.get("bank_metric_policy_sha256") or
            resolution.get("case_data_sha256") != trusted.get("case_data_sha256")):
        return {"rows": [], "status": "abstained",
            "reason": "metric_selector_resolution_missing_or_provenance_mismatch",
            "selector_ref": selector_ref}
    if resolution.get("status") == "unavailable":
        return {"rows": [], "status": "abstained",
            "reason": "metric_selector_unavailable_for_exact_case_fields",
            "selector_ref": selector_ref, "selector_resolution": dict(resolution)}
    matches = resolution.get("matches")
    if not isinstance(matches, list) or not matches:
        return {"rows": [], "status": "abstained",
            "reason": "metric_selector_has_no_exact_resolved_fields",
            "selector_ref": selector_ref}
    selected_fields = {row.get("field_name") for row in matches
        if isinstance(row, Mapping) and row.get("ranking_eligible") is True}
    if len(selected_fields) != len(matches):
        _v1._fail("ir_metric_selector_semantics", "selector resolution includes unrankable or malformed fields")
    rows = [row for row in series.get("rows", [])
        if row.get("reference", {}).get("source_field") in selected_fields]
    if not rows:
        return {"rows": [], "status": "abstained",
            "reason": "metric_selector_resolved_fields_have_no_candidate_series",
            "selector_ref": selector_ref,
            "selector_resolution": dict(resolution)}
    return {"rows": rows, "status": "ready", "reason": None,
        "selector_ref": selector_ref,
        "selector_resolution": dict(resolution),
        "matched_field_count": len(selected_fields),
        "field_selection": "all exact observed fields; no first-match selection"}


def _log_pattern_counts(inputs: Mapping[str, Any], trusted: Mapping[str, Any]) -> dict[str, Any]:
    log_input = inputs.get("log_input")
    if (not isinstance(log_input, Mapping) or log_input.get("$type") != "OperatorIRLogInput" or
            set(log_input) != {"$type", "status", "complete", "analysis_block_duration_ms",
                "summaries", "patterns", "block_denominators", "derived_count_semantics",
                "source_sha256", "case_data_sha256"}):
        _v1._fail("ir_log_input_type", "log kernel requires the exact trusted OperatorIRLogInput contract")
        raise ValueError("log kernel requires the exact trusted OperatorIRLogInput contract")
    log_input = cast(Mapping[str, Any], log_input)
    if log_input.get("status") != "available" or log_input.get("complete") is not True:
        return {"rows": [], "status": "abstained",
            "reason": "native_log_pattern_summary_unsupported_or_incomplete"}
    count_semantics = log_input.get("derived_count_semantics")
    if (not isinstance(count_semantics, Mapping) or
            count_semantics.get("source_table") != "bank/log/log_service.csv" or
            count_semantics.get("field_name") != "log_volume_count" or
            count_semantics.get("kind") != "event_count" or
            type(count_semantics.get("ranking_eligible")) is not bool or
            count_semantics.get("analysis_block_duration_ms") != log_input.get("analysis_block_duration_ms")):
        _v1._fail("ir_log_count_semantics", "denominator semantics are not the exact classifier-derived row count")
        raise ValueError("log denominator semantics are not classifier-derived")
    count_semantics = cast(Mapping[str, Any], count_semantics)
    source_semantics_sha = hashlib.sha256(json.dumps(dict(count_semantics), sort_keys=True,
        separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    semantics_map = trusted.get("observed_metric_semantics", {})
    exposure_semantics = (semantics_map.get(
        "bank/log/log_service.csv:log_pattern_exposure_rate")
        if isinstance(semantics_map, Mapping) else None)
    if (not isinstance(exposure_semantics, Mapping) or
            exposure_semantics.get("source_table") != "bank/log/log_service.csv" or
            exposure_semantics.get("field_name") != "log_pattern_exposure_rate" or
            exposure_semantics.get("kind") != "rate" or
            exposure_semantics.get("ranking_eligible") is not True or
            not isinstance(exposure_semantics.get("selector_ref"), str)):
        return {"rows": [], "status": "abstained",
            "reason": "log_pattern_exposure_semantics_unavailable_or_unrankable"}
    exposure_semantics_sha = hashlib.sha256(json.dumps(dict(exposure_semantics), sort_keys=True,
        separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    block_ms = log_input.get("analysis_block_duration_ms")
    if type(block_ms) is not int or block_ms <= 0:
        _v1._fail("ir_log_grain", "log summary block duration is invalid")
        raise ValueError("log summary block duration is invalid")
    block_ms = cast(int, block_ms)
    source_hashes = log_input.get("source_sha256")
    case_data_sha256 = log_input.get("case_data_sha256")
    if (not isinstance(case_data_sha256, str) or len(case_data_sha256) != 64 or
            any(char not in "0123456789abcdef" for char in case_data_sha256)):
        _v1._fail("ir_log_case_provenance", "log input lacks the trusted case-data hash")
    if (not isinstance(source_hashes, list) or not source_hashes or any(
            not isinstance(value, str) or len(value) != 64 or
            any(char not in "0123456789abcdef" for char in value)
            for value in source_hashes)):
        _v1._fail("ir_log_provenance", "log summaries lack source hashes")
        raise ValueError("log summaries lack source hashes")
    source_hashes = cast(list[str], source_hashes)
    summaries = log_input.get("summaries")
    patterns = log_input.get("patterns")
    block_rows = log_input.get("block_denominators")
    if not isinstance(summaries, list) or not isinstance(patterns, list) or not isinstance(block_rows, list):
        _v1._fail("ir_log_summary_shape", "log summaries, patterns, and denominators must be arrays")
        raise ValueError("log summary arrays are malformed")
    summaries = cast(list[Mapping[str, Any]], summaries)
    patterns = cast(list[Mapping[str, Any]], patterns)
    block_rows = cast(list[Mapping[str, Any]], block_rows)
    if (len(summaries) > _v1.MAX_SERIES or
            len(patterns) > _v1.MAX_SERIES * 32 or len(block_rows) > _v1.MAX_POINTS):
        _v1._fail("ir_log_summary_limit", "trusted log summary exceeds the V2 fixed row limits")
    candidates = _v1._fields(inputs.get("candidates"), "CandidateSet").get("candidates", [])
    candidate_ids = {_v1._canonical_entity(item.get("entity"))[1]
        for item in candidates if isinstance(item, Mapping)}
    summary_map: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for summary in summaries:
        if summary.get("$type") != "PaperLogSummary":
            _v1._fail("ir_log_summary_type", "log denominator is not a trusted PaperLogSummary")
            raise ValueError("log denominator is not a trusted PaperLogSummary")
        fields = {key: value for key, value in summary.items() if key != "$type"}
        window = fields.get("window", {})
        component, log_name = fields.get("component_id"), fields.get("log_name")
        window_name = window.get("name") if isinstance(window, Mapping) else None
        row_count, scanned = fields.get("row_count"), fields.get("source_rows_scanned")
        hashes = fields.get("source_sha256")
        if (not isinstance(component, str) or not isinstance(log_name, str) or
                window_name not in {"reference", "alert"} or type(row_count) is not int or
                row_count < 0 or type(scanned) is not int or scanned < row_count or
                not isinstance(hashes, list) or any(value not in source_hashes for value in hashes)):
            _v1._fail("ir_log_denominator", "full-window log denominator metadata is malformed")
            raise ValueError("full-window log denominator metadata is malformed")
        key = (component, log_name, window_name)
        if key in summary_map:
            _v1._fail("ir_log_denominator_duplicate", "duplicate full-window log denominator")
        summary_map[key] = fields

    denominator_map: dict[tuple[str, str, str, int], int] = {}
    for denominator in block_rows:
        if denominator.get("$type") != "OperatorIRLogBlockDenominator":
            _v1._fail("ir_log_block_type", "per-block denominator is not a trusted typed record")
            raise ValueError("per-block denominator is not a trusted typed record")
        fields = {key: value for key, value in denominator.items() if key != "$type"}
        window = fields.get("window", {})
        component, log_name = fields.get("component_id"), fields.get("log_name")
        window_name = window.get("name") if isinstance(window, Mapping) else None
        start, count = fields.get("block_start_ms_utc"), fields.get("row_count")
        if (not isinstance(component, str) or not isinstance(log_name, str) or
                window_name not in {"reference", "alert"} or type(start) is not int or
                type(count) is not int or count < 0):
            _v1._fail("ir_log_block_denominator", "per-block denominator fields are malformed")
            raise ValueError("per-block denominator fields are malformed")
        denominator_key = (component, log_name, window_name, cast(int, start))
        if denominator_key in denominator_map:
            _v1._fail("ir_log_block_duplicate", "duplicate per-block log denominator")
        denominator_map[denominator_key] = cast(int, count)
    for (component, log_name, window_name), summary in summary_map.items():
        total_from_blocks = sum(count for (comp, name, win, _start), count
            in denominator_map.items() if (comp, name, win) == (component, log_name, window_name))
        if total_from_blocks != summary.get("row_count"):
            _v1._fail("ir_log_denominator_coverage",
                "per-block event counts do not reconcile to the trusted full-window row denominator")

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for pattern in patterns:
        if pattern.get("$type") != "PaperLogPatternSummary":
            _v1._fail("ir_log_pattern_type", "pattern row is not a trusted PaperLogPatternSummary")
            raise ValueError("pattern row is not a trusted PaperLogPatternSummary")
        fields = {key: value for key, value in pattern.items() if key != "$type"}
        window = fields.get("window", {})
        component, log_name = fields.get("component_id"), fields.get("log_name")
        window_name = window.get("name") if isinstance(window, Mapping) else None
        start = fields.get("block_start_ms_utc")
        pattern_sha, tokens = fields.get("pattern_sha256"), fields.get("tokens")
        support, block_count = fields.get("row_support"), fields.get("block_row_count")
        pattern_hashes = fields.get("source_sha256")
        if (not isinstance(component, str) or not isinstance(log_name, str) or
                window_name not in {"reference", "alert"} or type(start) is not int or
                not isinstance(tokens, list) or not tokens or not isinstance(pattern_sha, str) or
                len(pattern_sha) != 64 or hashlib.sha256("\0".join(tokens).encode()).hexdigest() != pattern_sha or
                type(support) is not int or support <= 0 or type(block_count) is not int or
                block_count < support or not isinstance(pattern_hashes, list) or
                any(value not in source_hashes for value in pattern_hashes)):
            _v1._fail("ir_log_pattern", "normalized pattern row or provenance is malformed")
            raise ValueError("normalized pattern row or provenance is malformed")
        denominator_value = denominator_map.get((component, log_name, window_name, cast(int, start)))
        if (denominator_value is None or type(block_count) is not int or type(support) is not int or
                denominator_value != block_count or support > denominator_value):
            _v1._fail("ir_log_pattern_denominator", "pattern row does not match its full-window block denominator")
            raise ValueError("pattern row does not match its full-window block denominator")
        if (component, log_name, window_name) not in summary_map:
            _v1._fail("ir_log_pattern_window", "pattern lacks a full-window record denominator")
        if component not in candidate_ids:
            continue
        pattern_key = (component, log_name, pattern_sha)
        row = grouped.setdefault(pattern_key, {"tokens": list(tokens), "counts": {"reference": 0, "alert": 0},
            "blocks": {"reference": {}, "alert": {}}, "source_hashes": list(source_hashes)})
        if start in row["blocks"][window_name]:
            _v1._fail("ir_log_pattern_duplicate", "duplicate exact pattern/component/log/window/block row")
        row["counts"][window_name] += support
        row["blocks"][window_name][start] = support
    output = []
    for (component, log_name, pattern_sha), counts in sorted(grouped.items()):
        per_window = {}
        valid = True
        for window_name in ("reference", "alert"):
            summary_row = summary_map.get((component, log_name, window_name))
            window_row_count = int(summary_row["row_count"]) if summary_row else 0
            block_values = {start: value for (comp, name, win, start), value in denominator_map.items()
                if comp == component and name == log_name and win == window_name}
            if summary_row is None or window_row_count <= 0:
                valid = False
            per_window[window_name] = {"pattern_rows": counts["counts"][window_name],
                "total_rows": window_row_count, "rate": (counts["counts"][window_name] / window_row_count
                    if window_row_count > 0 else None),
                "window": summary_row["window"] if summary_row else None,
                "block_denominators": block_values,
                "block_pattern_counts": counts["blocks"][window_name]}
        reference_summary = summary_map.get((component, log_name, "reference"), {})
        alert_summary = summary_map.get((component, log_name, "alert"), {})
        def source_record(summary):
            window = summary.get("window", {})
            return {"role": "component", "entity": component,
                "source_table": "bank/log/log_service.csv",
                "source_field": "log_pattern_exposure_rate",
                "metric_kind": exposure_semantics["kind"],
                "unit": exposure_semantics["unit"],
                "metric_rule_id": exposure_semantics["rule_id"],
                "metric_rule_version": exposure_semantics["rule_version"],
                "metric_rule_ref": exposure_semantics["selector_ref"],
                "metric_transform": exposure_semantics["transform"],
                "metric_provenance": list(exposure_semantics["provenance"]),
                "metric_semantics_sha256": exposure_semantics_sha,
                "source_hashes": list(source_hashes), "case_data_sha256": case_data_sha256,
                "window": window, "pattern_sha256": pattern_sha, "log_name": log_name,
                "row_count_denominator": summary.get("row_count")}
        output.append({"key": ("component", component, f"log:{log_name}:{pattern_sha}"),
            "component_id": component, "log_name": log_name, "pattern_sha256": pattern_sha,
            "normalized_tokens": counts["tokens"], "windows": per_window,
            "eligible": False, "computable": valid, "ranking_state": "unknown",
            "support": min(len(counts["blocks"]["reference"]),
                len(counts["blocks"]["alert"])),
            "source_hashes": counts["source_hashes"],
            "source_record": source_record(reference_summary),
            "alert_record": source_record(alert_summary),
            "count_semantics_sha256": source_semantics_sha,
            "count_semantics": dict(count_semantics)})
    if not output:
        return {"rows": [], "status": "abstained", "reason": "no_observed_normalized_patterns_for_candidates"}
    return {"rows": output, "status": "ready", "reason": None,
        "summary_schema": "e4-typed-operator-ir-v2-log-patterns/1",
        "ranking_state": "unknown"}


def _log_select_pattern(counts: Mapping[str, Any], params: Mapping[str, Any]) -> dict[str, Any]:
    pattern_sha, log_name = params.get("pattern_sha256"), params.get("log_name")
    if (not isinstance(pattern_sha, str) or len(pattern_sha) != 64 or
            any(char not in "0123456789abcdef" for char in pattern_sha) or
            not isinstance(log_name, str) or not log_name or len(log_name) > 128):
        _v1._fail("ir_log_selector", "pattern selector must be a bounded exact SHA-256 and log name")
    rows = [row for row in counts.get("rows", [])
        if row.get("pattern_sha256") == pattern_sha and row.get("log_name") == log_name]
    if not rows:
        return {"rows": [], "status": "abstained",
            "reason": "pattern_selector_not_present_in_trusted_full_window_summary"}
    return {"rows": rows, "status": "ready", "reason": None}


def _log_exposure_rate(counts: Mapping[str, Any], statistic: str,
                       minimum_blocks: int) -> dict[str, Any]:
    if statistic not in {"mean_rate", "variance_rate", "max_rate", "q90_rate"}:
        _v1._fail("ir_log_statistic", "unsupported log exposure statistic")
    if type(minimum_blocks) is not int or minimum_blocks <= 0:
        _v1._fail("ir_log_support", "minimum_blocks must be a positive runtime value")
    output = []
    for row in counts.get("rows", []):
        windows = row.get("windows", {})
        before, after = windows.get("reference", {}), windows.get("alert", {})
        window_rows = {}
        computable = row.get("computable") is True
        for window_name, source in (("reference", before), ("alert", after)):
            block_denominators = source.get("block_denominators", {})
            block_counts = source.get("block_pattern_counts", {})
            block_rates = {start: block_counts.get(start, 0) / denominator
                for start, denominator in block_denominators.items() if denominator > 0}
            total_rows = source.get("total_rows", 0)
            if (len(block_rates) < minimum_blocks or type(total_rows) is not int or total_rows <= 0):
                computable = False
                value = None
            elif statistic == "mean_rate":
                value = source["pattern_rows"] / total_rows
            elif statistic == "variance_rate":
                mean = math.fsum(block_rates.values()) / len(block_rates)
                value = math.fsum((rate - mean) ** 2 for rate in block_rates.values()) / len(block_rates)
            elif statistic == "max_rate":
                value = max(block_rates.values())
            else:
                value = _empirical_quantile(list(block_rates.values()), 0.9)
            window_rows[window_name] = {"value": value, "support": len(block_rates),
                "window": source.get("window"), "record": row["source_record" if window_name == "reference"
                    else "alert_record"], "rates_by_block": block_rates}
        before_rate = window_rows["reference"]["value"]
        after_rate = window_rows["alert"]["value"]
        computable = computable and before_rate is not None and after_rate is not None
        output.append({"key": row["key"], "windows": {
                "reference": {**window_rows["reference"], "value": before_rate or 0.0},
                "alert": {**window_rows["alert"], "value": after_rate or 0.0}},
            "eligible": computable, "computable": computable, "ranking_state": "unknown",
            "kind": "normalized_pattern_exposure", "source_record": row["source_record"],
            "alert_record": row["alert_record"], "support": row.get("support", 0),
            "support_counts": {
                "reference_window_independent_time_blocks": len(window_rows["reference"]["rates_by_block"]),
                "alert_window_independent_time_blocks": len(window_rows["alert"]["rates_by_block"])},
            "details": {"statistic": statistic, "minimum_blocks": minimum_blocks,
                "numeric_unit": "dimensionless",
                "source_unit": row["source_record"].get("unit", "undeclared"),
                "pattern_sha256": row["pattern_sha256"],
                "log_name": row["log_name"], "normalized_tokens": row["normalized_tokens"],
                "reference_pattern_rows": before.get("pattern_rows"),
                "reference_total_rows": before.get("total_rows"),
                "reference_exposure_rate": (before.get("pattern_rows", 0) / before["total_rows"]
                    if before.get("total_rows", 0) > 0 else None),
                "reference_statistic_value": before_rate,
                "reference_rates_by_block": window_rows["reference"]["rates_by_block"],
                "alert_pattern_rows": after.get("pattern_rows"),
                "alert_total_rows": after.get("total_rows"),
                "alert_exposure_rate": (after.get("pattern_rows", 0) / after["total_rows"]
                    if after.get("total_rows", 0) > 0 else None),
                "alert_statistic_value": after_rate,
                "alert_rates_by_block": window_rows["alert"]["rates_by_block"],
                "reference_exact_blocks": len(before.get("block_denominators", {})),
                "alert_exact_blocks": len(after.get("block_denominators", {})),
                "block_independence_claim": "distinct configured time blocks; independence is not established by aggregation",
                "ranking_state": "unknown_until_declared_uncertainty_test"}})
    if not output:
        return {"rows": [], "status": "abstained",
            "reason": counts.get("reason", "no_selected_pattern_counts")}
    return {"rows": output, "status": "ready", "reason": None,
        "ranking_state": "unknown", "statistic": statistic,
        "descriptive_exposure_only": True}


def _block_medians(record: Mapping[str, Any], block_ms: int) -> list[tuple[int, float]]:
    window = record.get("window")
    start = window.get("start_ms_utc") if isinstance(window, Mapping) else None
    if type(start) is not int:
        return []
    buckets: dict[int, list[float]] = {}
    for timestamp, value in record.get("samples", []):
        if type(timestamp) is not int or not _v1._finite(value):
            continue
        block_start = start + ((timestamp - start) // block_ms) * block_ms
        buckets.setdefault(block_start, []).append(float(value))
    return [(block_start, statistics.median(values))
        for block_start, values in sorted(buckets.items())]


def _normalize_reference_scale(series: Mapping[str, Any], params: Mapping[str, Any],
        runtime: Mapping[str, Any], trusted: Mapping[str, Any]) -> dict[str, Any]:
    block_minutes = params["block_minutes"]
    minimum_blocks = params["minimum_blocks"]
    relative_floor = float(params["relative_scale_floor"])
    absolute_floor = float(params["absolute_scale_floor"])
    if (type(block_minutes) is not int or block_minutes <= 0 or
            type(minimum_blocks) is not int or minimum_blocks <= 0 or
            not math.isfinite(relative_floor) or relative_floor <= 0 or
            not math.isfinite(absolute_floor) or absolute_floor <= 0):
        _v1._fail("ir_scale_parameters", "reference-scale parameters violate their runtime bounds")
    block_ms = block_minutes * 60_000
    semantics = trusted.get("observed_metric_semantics", {})
    output = []
    for row in series.get("rows", []):
        field = row["reference"].get("source_field")
        metric = (semantics.get(f"bank/metric_container.csv:{field}")
            if isinstance(semantics, Mapping) else None)
        reference = _block_medians(row["reference"], block_ms)
        alert = _block_medians(row["alert"], block_ms)
        reference_values = [value for _, value in reference]
        center = statistics.median(reference_values) if reference_values else None
        mad = (statistics.median([abs(value - center) for value in reference_values])
            if center is not None else None)
        robust_scale = (1.4826 * mad if mad is not None else None)
        scale_floor = max(absolute_floor,
            relative_floor * abs(center) if center is not None else absolute_floor)
        scale = max(robust_scale or 0.0, scale_floor) if reference else None
        valid = (len(reference) >= minimum_blocks and len(alert) >= minimum_blocks and
            center is not None and scale is not None and math.isfinite(scale) and scale > 0)
        normalized = {}
        for name, values in (("reference", reference), ("alert", alert)):
            normalized_values = []
            if valid:
                assert center is not None and scale is not None
                normalized_values = [(timestamp, (value - center) / scale)
                    for timestamp, value in values]
            normalized[name] = {"values": normalized_values,
                "support": len(values), "block_medians": values,
                "window": row[name]["window"]}
        output.append({"key": row["key"], "source_field": field,
            "component_id": row["key"][1], "metric_semantics": metric,
            "windows": normalized, "reference_center": center,
            "reference_mad": mad, "robust_reference_scale": robust_scale,
            "effective_scale": scale, "scale_floor": scale_floor,
            "minimum_blocks": minimum_blocks, "block_minutes": block_minutes,
            "eligible": valid, "computable": valid, "ranking_state": "unknown",
            "support": min(len(reference), len(alert)),
            "support_counts": {
                "reference_window_independent_time_blocks": len(reference),
                "alert_window_independent_time_blocks": len(alert)},
            "source_record": row["reference"], "alert_record": row["alert"],
            "reason": None if valid else "insufficient_reference_scale_window_support"})
    valid_count = sum(bool(row["computable"]) for row in output)
    return {"rows": output, "status": "ready" if valid_count else "abstained",
        "reason": None if valid_count else "no_series_meets_declared_reference_scale_support",
        "ranking_state": "unknown",
        "scale_definition": "max(1.4826*MAD(reference block medians), absolute_scale_floor, relative_scale_floor*abs(reference median))",
        "block_independence_claim": "not established by time bucketing"}


def _empirical_quantile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("quantile requires observations")
    index = min(len(ordered) - 1, max(0, int(math.ceil(quantile * len(ordered)) - 1)))
    return ordered[index]


def _exceedance_window_row(row: Mapping[str, Any], *, kind: str,
        reference_rate: float | None, alert_rate: float | None,
        reference_count: int, alert_count: int, computable: bool,
        details: Mapping[str, Any], reason: str) -> dict[str, Any]:
    ref, alt = row["windows"]["reference"], row["windows"]["alert"]
    source_record = row["source_record"]
    return {"key": row["key"], "component_id": row["component_id"],
        "source_field": row["source_field"], "metric_semantics": row.get("metric_semantics"),
        "windows": {
            "reference": {"value": reference_rate, "support": ref["support"],
                "window": ref["window"], "record": row["source_record"]},
            "alert": {"value": alert_rate, "support": alt["support"],
                "window": alt["window"], "record": row["alert_record"]}},
        "eligible": computable, "computable": computable, "ranking_state": "unknown",
        "kind": kind, "source_record": row["source_record"],
        "alert_record": row["alert_record"], "support": row["support"],
        "support_counts": dict(row.get("support_counts", {})),
        "details": {**dict(details), "reference_exceedance_count": reference_count,
            "alert_exceedance_count": alert_count, "numeric_unit": "dimensionless",
            "source_unit": source_record.get("unit", "undeclared")}, "reason": reason}


def _reference_quantile_exceedance(series: Mapping[str, Any], tail_quantile: float) -> dict[str, Any]:
    if not _v1._finite(tail_quantile) or not 0.75 <= float(tail_quantile) <= 0.99:
        _v1._fail("ir_quantile_parameter", "tail_quantile must be within the declared trusted runtime range")
    output = []
    for row in series.get("rows", []):
        semantics = row.get("metric_semantics") or {}
        direction = semantics.get("direction", "unknown")
        reference = [value for _timestamp, value in row["windows"]["reference"]["values"]]
        alert = [value for _timestamp, value in row["windows"]["alert"]["values"]]
        can_compute = bool(row.get("computable") and reference and alert)
        if not can_compute or direction not in {"higher_is_worse", "lower_is_worse", "two_sided"}:
            output.append(_exceedance_window_row(row,
                kind="reference_quantile_exceedance", reference_rate=None, alert_rate=None,
                reference_count=0, alert_count=0, computable=False,
                details={"direction": direction, "tail_quantile": float(tail_quantile)},
                reason="metric_direction_unknown" if direction == "unknown" else
                    "reference_scale_unavailable"))
            continue
        q = float(tail_quantile)
        if direction == "higher_is_worse":
            low_threshold, high_threshold = None, _empirical_quantile(reference, q)
        elif direction == "lower_is_worse":
            low_threshold, high_threshold = _empirical_quantile(reference, 1.0 - q), None
        else:
            tail = (1.0 - q) / 2.0
            low_threshold, high_threshold = (_empirical_quantile(reference, tail),
                _empirical_quantile(reference, 1.0 - tail))
        def rate(values):
            if direction == "higher_is_worse":
                count = sum(value > high_threshold for value in values)
            elif direction == "lower_is_worse":
                count = sum(value < low_threshold for value in values)
            else:
                count = sum(value < low_threshold or value > high_threshold for value in values)
            return count / len(values), count
        reference_rate, reference_count = rate(reference)
        alert_rate, alert_count = rate(alert)
        output.append(_exceedance_window_row(row,
            kind="reference_quantile_exceedance", reference_rate=reference_rate,
            alert_rate=alert_rate, reference_count=reference_count,
            alert_count=alert_count, computable=True,
            details={"direction": direction, "reference_threshold": low_threshold,
                "alert_threshold": high_threshold, "reference_exceedance_rate": reference_rate,
                "alert_exceedance_rate": alert_rate, "tail_quantile": q,
                "ranking_state": "unknown"},
            reason="exceedance_effect_computed_without_uncertainty_test"))
    ready = any(row.get("computable") is True for row in output)
    return {"rows": output, "status": "ready" if ready else "abstained",
        "reason": None if ready else "no_directional_reference_quantile_results",
        "ranking_state": "unknown",
        "estimand": "alert fraction of time blocks outside the reference empirical tail threshold"}


def _sigma_exceedance(series: Mapping[str, Any], sigma_multiple: float) -> dict[str, Any]:
    if sigma_multiple not in {2.0, 3.0}:
        _v1._fail("ir_sigma_parameter", "sigma_multiple must be explicitly set to 2.0 or 3.0")
    output = []
    for row in series.get("rows", []):
        semantics = row.get("metric_semantics") or {}
        direction = semantics.get("direction", "unknown")
        reference = [value for _timestamp, value in row["windows"]["reference"]["values"]]
        alert = [value for _timestamp, value in row["windows"]["alert"]["values"]]
        if not row.get("computable") or not reference or not alert or direction == "unknown":
            output.append(_exceedance_window_row(row, kind="sigma_exceedance",
                reference_rate=None, alert_rate=None, reference_count=0, alert_count=0,
                computable=False, details={"direction": direction,
                    "sigma_multiple": float(sigma_multiple)},
                reason="metric_direction_unknown" if direction == "unknown" else
                    "reference_scale_unavailable"))
            continue
        mean = math.fsum(reference) / len(reference)
        variance = math.fsum((value - mean) ** 2 for value in reference) / len(reference)
        sd = math.sqrt(variance)
        low_threshold, high_threshold = mean - sigma_multiple * sd, mean + sigma_multiple * sd
        if direction == "higher_is_worse":
            alert_count = sum(value > high_threshold for value in alert)
            reference_count = sum(value > high_threshold for value in reference)
        elif direction == "lower_is_worse":
            alert_count = sum(value < low_threshold for value in alert)
            reference_count = sum(value < low_threshold for value in reference)
        else:
            alert_count = sum(value < low_threshold or value > high_threshold for value in alert)
            reference_count = sum(value < low_threshold or value > high_threshold for value in reference)
        output.append(_exceedance_window_row(row, kind="sigma_exceedance",
            reference_rate=reference_count / len(reference),
            alert_rate=alert_count / len(alert), reference_count=reference_count,
            alert_count=alert_count, computable=True,
            details={"direction": direction, "reference_mean": mean, "reference_sd": sd,
                "thresholds": {"lower": low_threshold, "upper": high_threshold},
                "sigma_multiple": float(sigma_multiple), "ranking_state": "unknown",
                "distributional_assumption": "parametric reference sigma; not distribution-free"},
            reason="parametric_sigma_exceedance_assumes_reference_block_distribution"))
    ready = any(row.get("computable") is True for row in output)
    return {"rows": output, "status": "ready" if ready else "abstained",
        "reason": None if ready else "no_directional_sigma_results",
        "ranking_state": "unknown",
        "distributional_assumption": "sigma threshold is parametric and not distribution-free"}


def _wire_scored_v2(value: Mapping[str, Any]) -> dict[str, Any]:
    result = _BASE_WIRE_SCORED(value)
    fields = result.get("$fields", {})
    status_details = fields.get("status_details", {})
    detail_map = status_details.get("$map", {}) if isinstance(status_details, Mapping) else {}
    detail_map = dict(detail_map)
    detail_map["operator_ir_availability"] = list(_TRACE)
    fields["status_details"] = {"$map": detail_map}
    fields["scorer"] = "typed-operator-ir-v2"
    result["$fields"] = fields
    return result


def execute_ir(program: Mapping[str, Any], raw_inputs: Mapping[str, Any],
        parameters: Mapping[str, Any], runtime: Mapping[str, Any],
        trusted: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(program, Mapping) or program.get("schema_version") != IR_SCHEMA:
        _v1._fail("ir_schema", "unsupported or malformed V2 operator IR program")
    if trusted.get("v2_base_worker_sha256") != BASE_WORKER_SHA256:
        _v1._fail("ir_bundle_pin", "V2 base worker hash differs from the frozen source bundle")
    _TRACE.clear()
    _CURRENT["index"] = 0
    _CURRENT["program"] = program
    original_dispatch = (_v1.PROFILE, _v1.IR_SCHEMA, _v1.ENGINE_ABI,
        _v1.KERNELS, _v1._invoke, _v1._wire_scored)
    _v1.PROFILE = PROFILE
    _v1.IR_SCHEMA = IR_SCHEMA
    _v1.ENGINE_ABI = ENGINE_ABI
    _v1.KERNELS = KERNELS
    _v1._invoke = _invoke_v2
    _v1._wire_scored = _wire_scored_v2
    try:
        return _v1.execute_ir(program, raw_inputs, parameters, runtime, trusted)
    finally:
        (_v1.PROFILE, _v1.IR_SCHEMA, _v1.ENGINE_ABI,
            _v1.KERNELS, _v1._invoke, _v1._wire_scored) = original_dispatch
