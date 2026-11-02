"""Pure standard-library interpreter for the closed typed M4 operator IR.

This module is mounted read-only into sandbox.py's existing bubblewrap worker.
It never imports or evaluates model-provided source code.
"""
from __future__ import annotations

import math
import statistics
import hashlib
import json

PROFILE = "typed-operator-ir-v1"
IR_SCHEMA = "e4-typed-operator-ir/1"
ENGINE_ABI = "e4-typed-operator-ir-kernel-abi/2"
MAX_NODES = 40
MAX_SERIES = 4096
MAX_POINTS = 250_000
MAX_PAIRS = 1_000_000
MAX_OPERATIONS = 5_000_000

# The catalog is intentionally private: only the evolution prompt receives it.
# Adaptation and serving receive registered generated-operator signatures only.
KERNELS = {
    "series.component_metrics@1": {
        "inputs": {"reference": "Aggregate", "alert": "Aggregate", "candidates": "CandidateSet"},
        "output": "ComponentSeries", "params": {}},
    "series.primary_kpi@1": {
        "inputs": {"reference": "Aggregate", "alert": "Aggregate"},
        "output": "KPISeries", "params": {}},
    "series.select_field@1": {
        "inputs": {"series": "ComponentSeries"}, "output": "ComponentSeries",
        "params": {"source_field": {"type": "string", "required": True}}},
    "series.align_kpi@1": {
        "inputs": {"components": "ComponentSeries", "kpi": "KPISeries"},
        "output": "AlignedSeriesPairs", "params": {}},
    "series.align_kpi_both_windows@1": {
        "inputs": {"components": "ComponentSeries", "kpi": "KPISeries"},
        "output": "AlignedSeriesPairs", "params": {}},
    "series.correlate@1": {
        "inputs": {"pairs": "AlignedSeriesPairs"}, "output": "KeyedNumbers",
        "params": {"method": {"type": "string", "required": True, "choices": ["spearman"]},
            "minimum_pairs": {"type": "positive_integer", "required": True,
                "runtime_field": "minimum_support"}}},
    "series.reduce@1": {
        "inputs": {"series": "ComponentSeries"}, "output": "WindowSummaries",
        "params": {"statistic": {"type": "string", "required": True,
                "choices": ["mean", "median", "variance", "q90"]},
            "block_minutes": {"type": "positive_integer", "required": True,
                "runtime_field": "block_minutes"},
            "minimum_blocks": {"type": "positive_integer", "required": True,
                "runtime_field": "minimum_independent_blocks"}}},
    "numbers.window_compare@1": {
        "inputs": {"values": "WindowSummaries"}, "output": "KeyedNumbers",
        "params": {"operation": {"type": "string", "required": True,
                "choices": ["difference", "relative_change", "ratio"]}}},
    "numbers.absolute@1": {"inputs": {"values": "KeyedNumbers"},
        "output": "KeyedNumbers", "params": {}},
    "numbers.positive_part@1": {"inputs": {"values": "KeyedNumbers"},
        "output": "KeyedNumbers", "params": {}},
    "numbers.multiply@1": {"inputs": {"left": "KeyedNumbers", "right": "KeyedNumbers"},
        "output": "KeyedNumbers", "params": {}},
    "numbers.reduce_component@1": {
        "inputs": {"values": "KeyedNumbers"}, "output": "ComponentScores",
        "params": {"method": {"type": "string", "required": True,
                "choices": ["mean", "max"]}}},
    "evidence.from_scores@1": {
        "inputs": {"scores": "ComponentScores"}, "output": "EvidenceSet", "params": {}},
    "scores.assemble@1": {
        "inputs": {"candidates": "CandidateSet", "scores": "ComponentScores",
            "evidence": "EvidenceSet"}, "output": "ScoredCandidates", "params": {}},
}


def _fail(code, message):
    raise ValueError(f"{code}: {message}")


def _decode(value):
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"$map"}:
            return {key: _decode(item) for key, item in value["$map"].items()}
        if set(value) == {"$tuple"}:
            return [_decode(item) for item in value["$tuple"]]
        if set(value) == {"$type", "$fields"}:
            return {"$type": value["$type"], **{key: _decode(item)
                for key, item in value["$fields"].items()}}
        return {key: _decode(item) for key, item in value.items()}
    return value


def _fields(value, type_name):
    if not isinstance(value, dict) or value.get("$type") != type_name:
        _fail("ir_input_type", f"expected typed {type_name} input")
    return value


def _finite(value):
    return type(value) in (int, float) and math.isfinite(float(value))


def _canonical_entity(entity):
    if not isinstance(entity, dict) or entity.get("$type") != "EntityRef":
        _fail("ir_entity", "input entity is not a typed EntityRef")
    fields = entity
    role, identifier = fields.get("role"), fields.get("identifier")
    if not isinstance(role, str) or not isinstance(identifier, str) or not identifier:
        _fail("ir_entity", "input EntityRef fields are malformed")
    return role, identifier


def _metric_key(row):
    role, identifier = _canonical_entity(row.get("entity"))
    return role, identifier, row.get("metric_key")


def _aggregate(value, window_name):
    fields = _fields(value, "Aggregate")
    window = fields.get("window")
    if not isinstance(window, dict) or window.get("name") != window_name:
        _fail("ir_window", f"expected {window_name} Aggregate")
    metadata = fields.get("metadata")
    source_hashes = metadata.get("source_hashes") if isinstance(metadata, dict) else None
    data_sha256 = metadata.get("case_data_sha256") if isinstance(metadata, dict) else None
    if (not isinstance(source_hashes, dict) or not source_hashes or
            not isinstance(data_sha256, str) or len(data_sha256) != 64 or
            any(c not in "0123456789abcdef" for c in data_sha256)):
        _fail("ir_provenance", "aggregate lacks trusted source hash provenance")
    return fields, window, source_hashes, data_sha256


def _samples(details):
    values = details.get("bucket_samples") if isinstance(details, dict) else None
    if not isinstance(values, list):
        _fail("ir_samples_missing", "aggregate lacks full-window bucket_samples")
    result = []
    previous = None
    for item in values:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            _fail("ir_samples_shape", "bucket sample must be [timestamp,value]")
        timestamp, value = item
        if type(timestamp) is not int or not _finite(value):
            continue
        if previous is not None and timestamp <= previous:
            _fail("ir_samples_order", "bucket samples must have distinct increasing timestamps")
        previous = timestamp
        result.append((timestamp, float(value)))
        if len(result) > MAX_POINTS:
            _fail("ir_series_limit", "one series exceeds the point limit")
    return result


def _window_series(reference, alert, *, role, source_table, source_field=None,
                   only_eligible=False, candidates=None):
    ref, ref_window, ref_hashes, ref_data_sha = _aggregate(reference, "reference")
    alt, alt_window, alt_hashes, alt_data_sha = _aggregate(alert, "alert")
    if ref_hashes != alt_hashes or ref_data_sha != alt_data_sha:
        _fail("ir_provenance_pair", "reference and alert aggregates must share exact source and case-data pins")
    if ref_window.get("end_ms_utc") != alt_window.get("start_ms_utc"):
        _fail("ir_window_adjacency", "reference and alert windows are not adjacent")
    candidates_set = None
    if candidates is not None:
        candidate_fields = _fields(candidates, "CandidateSet")
        items = candidate_fields.get("candidates")
        if not isinstance(items, list):
            _fail("ir_candidate_shape", "candidate set is malformed")
        candidates_set = {_canonical_entity(item.get("entity"))[1] for item in items}
    point_total = [0]
    def select(aggregate):
        by_key = {}
        for row in aggregate.get("rows", []):
            if not isinstance(row, dict) or row.get("statistic") != "median":
                continue
            entity_role, identifier, metric_key = _metric_key(row)
            details = row.get("details") or {}
            if entity_role != role or details.get("source_table") != source_table:
                continue
            field = details.get("source_field")
            if source_field is not None and field != source_field:
                continue
            if only_eligible and (details.get("ranking_eligible") is not True or
                                  details.get("metric_kind") not in {"gauge", "rate"}):
                continue
            if candidates_set is not None and identifier not in candidates_set:
                continue
            samples = _samples(details)
            if not samples:
                continue
            point_total[0] += len(samples)
            if point_total[0] > MAX_POINTS:
                _fail("ir_point_limit", "aggregate extraction exceeds the global point limit")
            key = (entity_role, identifier, metric_key)
            by_key[key] = {"role": entity_role, "entity": identifier,
                "metric_key": metric_key, "source_table": source_table,
                "source_field": field, "metric_kind": details.get("metric_kind"),
                "unit": details.get("unit", "undeclared"),
                "ranking_eligible": details.get("ranking_eligible") is True,
                "samples": samples, "support": int(row.get("support", len(samples))),
                "source_hashes": dict(aggregate.get("metadata", {}).get("source_hashes", {})),
                "case_data_sha256": aggregate.get("metadata", {}).get("case_data_sha256"),
                "window": dict(aggregate["window"])}
        return by_key
    refs, alerts = select(ref), select(alt)
    rows = []
    for key in sorted(set(refs) & set(alerts)):
        rows.append({"key": key, "reference": refs[key], "alert": alerts[key]})
    if len(rows) > MAX_SERIES:
        _fail("ir_series_limit", "series set exceeds the fixed series count")
    return rows


def _primary_kpi(inputs, trusted):
    reference = inputs["reference"]
    alert = inputs["alert"]
    ref, _, ref_hashes, ref_data_sha = _aggregate(reference, "reference")
    alt, _, alt_hashes, alt_data_sha = _aggregate(alert, "alert")
    if ref_hashes != alt_hashes or ref_data_sha != alt_data_sha:
        _fail("ir_provenance_pair", "reference and alert aggregates must share exact source and case-data pins")
    allowlist = trusted.get("primary_metric_allowlist")
    minimum = trusted.get("primary_kpi_minimum_support")
    if (not isinstance(allowlist, list) or not allowlist or type(minimum) is not int or minimum <= 0):
        _fail("ir_trusted_policy", "primary KPI selection policy is incomplete")
    point_total = [0]
    def by_window(agg, name):
        found = {}
        for row in agg["rows"]:
            if row.get("statistic") != "median":
                continue
            role, identifier, _metric = _metric_key(row)
            details = row.get("details") or {}
            field = f"metric_app.{details.get('source_field')}"
            if role != "app_kpi" or details.get("source_table") != "bank/metric_app.csv" or field not in allowlist:
                continue
            samples = _samples(details)
            if samples:
                point_total[0] += len(samples)
                if point_total[0] > MAX_POINTS:
                    _fail("ir_point_limit", "KPI extraction exceeds the global point limit")
                found[(field, identifier)] = {"role": role, "entity": identifier,
                    "metric_key": row["metric_key"], "source_table": "bank/metric_app.csv",
                    "source_field": details.get("source_field"), "samples": samples,
                    "support": len(samples), "source_hashes": dict(agg["metadata"]["source_hashes"]),
                    "case_data_sha256": agg["metadata"].get("case_data_sha256"),
                    "window": dict(agg["window"])}
        return found
    refs, alts = by_window(ref, "reference"), by_window(alt, "alert")
    candidates = [(min(len(refs[key]["samples"]), len(alts[key]["samples"])), key)
        for key in set(refs) & set(alts)
        if min(len(refs[key]["samples"]), len(alts[key]["samples"])) >= minimum]
    if not candidates:
        return {"rows": [], "status": "abstained", "reason": "no_primary_kpi_meets_policy_support"}
    candidates.sort(key=lambda item: (-item[0], item[1][1].casefold(), item[1][1], item[1][0]))
    _support, key = candidates[0]
    return {"rows": [{"key": key, "reference": refs[key], "alert": alts[key]}],
        "status": "ready", "selection_policy_sha256": trusted.get("primary_kpi_policy_sha256")}


def _component_series(inputs, trusted):
    rows = _window_series(inputs["reference"], inputs["alert"], role="component",
        source_table="bank/metric_container.csv", only_eligible=True,
        candidates=inputs["candidates"])
    allowed = set(trusted.get("component_metric_allowlist", []))
    rows = [row for row in rows if row["reference"]["source_field"] in allowed]
    return {"rows": rows, "status": "ready" if rows else "abstained",
        "reason": None if rows else "no_allowlisted_component_series"}


def _select_field(series, source_field):
    if not isinstance(source_field, str):
        _fail("ir_parameter", "source_field must be a string")
    rows = [row for row in series["rows"] if row["reference"]["source_field"] == source_field]
    return {"rows": rows, "status": "ready" if rows else "abstained",
        "reason": None if rows else "selected_field_unavailable"}


def _align_kpi(components, kpi):
    if not components.get("rows") or not kpi.get("rows"):
        return {"rows": [], "status": "abstained", "reason": "component_or_primary_kpi_series_missing"}
    selected = kpi["rows"][0]
    kpi_ref = dict(selected["reference"]["samples"])
    kpi_alt = dict(selected["alert"]["samples"])
    baseline = statistics.median(kpi_ref.values())
    output = []
    for row in components["rows"]:
        ref = row["reference"]
        alt = row["alert"]
        comp_baseline = statistics.median(value for _, value in ref["samples"])
        comp_alert = dict(alt["samples"])
        shared = sorted(set(comp_alert) & set(kpi_alt))
        if len(output) + len(shared) > MAX_PAIRS:
            _fail("ir_pair_limit", "timestamp joins exceed the fixed pair limit")
        pairs = [(timestamp, comp_alert[timestamp] - comp_baseline,
                  kpi_alt[timestamp] - baseline) for timestamp in shared]
        output.append({"key": row["key"], "series": row,
            "kpi": selected, "pairs": pairs, "support": len(pairs)})
        if len(output) > MAX_SERIES:
            _fail("ir_series_limit", "aligned series exceeds the fixed row limit")
    return {"rows": output, "status": "ready" if output else "abstained",
        "reason": None if output else "no_timestamp_aligned_pairs"}


def _align_kpi_both_windows(components, kpi):
    """Exact-join both windows and center component/KPI values on reference medians."""
    if not components.get("rows") or not kpi.get("rows"):
        return {"rows": [], "status": "abstained", "reason": "component_or_primary_kpi_series_missing",
                "timestamp_scope": "reference_and_alert_exact_no_interpolation"}
    selected = kpi["rows"][0]
    kpi_reference = dict(selected["reference"]["samples"])
    kpi_alert = dict(selected["alert"]["samples"])
    kpi_baseline = statistics.median(kpi_reference.values())
    output = []
    total_pairs = 0
    for row in components["rows"]:
        component_reference = dict(row["reference"]["samples"])
        component_alert = dict(row["alert"]["samples"])
        component_baseline = statistics.median(component_reference.values())
        pairs = []
        reference_pair_count = len(set(component_reference) & set(kpi_reference))
        alert_pair_count = len(set(component_alert) & set(kpi_alert))
        for component_samples, kpi_samples in ((component_reference, kpi_reference),
                                                (component_alert, kpi_alert)):
            for timestamp in sorted(set(component_samples) & set(kpi_samples)):
                pairs.append((timestamp, component_samples[timestamp] - component_baseline,
                              kpi_samples[timestamp] - kpi_baseline))
        total_pairs += len(pairs)
        if total_pairs > MAX_PAIRS:
            _fail("ir_pair_limit", "joint-window timestamp joins exceed the fixed pair limit")
        output.append({"key": row["key"], "series": row,
            "kpi": selected, "pairs": pairs, "support": len(pairs),
            "reference_pair_count": reference_pair_count,
            "alert_pair_count": alert_pair_count,
            "both_windows_supported": reference_pair_count > 0 and alert_pair_count > 0})
        if len(output) > MAX_SERIES:
            _fail("ir_series_limit", "joint-window aligned series exceeds the fixed row limit")
    return {"rows": output, "status": "ready" if output else "abstained",
        "reason": None if output else "no_timestamp_aligned_pairs",
        "timestamp_scope": "reference_and_alert_exact_no_interpolation"}


def _rank(values):
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    ranks = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and values[order[start]] == values[order[end]]:
            end += 1
        average = ((start + 1) + end) / 2.0
        for index in order[start:end]:
            ranks[index] = average
        start = end
    return ranks


def _pearson(x, y):
    if len(x) < 2:
        return None
    try:
        mx, my = math.fsum(x) / len(x), math.fsum(y) / len(y)
    except OverflowError:
        return None
    dx, dy = [value - mx for value in x], [value - my for value in y]
    try:
        xx, yy = math.fsum(value * value for value in dx), math.fsum(value * value for value in dy)
    except OverflowError:
        return None
    if xx <= 0 or yy <= 0:
        return None
    try:
        value = math.fsum(a * b for a, b in zip(dx, dy)) / math.sqrt(xx * yy)
    except (OverflowError, ValueError, ZeroDivisionError):
        return None
    return max(-1.0, min(1.0, value)) if math.isfinite(value) else None


def _merge_support_counts(*rows):
    """Keep the minimum trusted count for each support unit present in inputs."""
    merged = {}
    for row in rows:
        counts = row.get("support_counts", {}) if isinstance(row, dict) else {}
        if not isinstance(counts, dict):
            _fail("ir_support_metadata", "trusted support_counts must be a mapping")
        for unit, count in counts.items():
            if not isinstance(unit, str) or not unit or type(count) is not int or count < 0:
                _fail("ir_support_metadata", "trusted support counts must have named units and nonnegative integers")
            merged[unit] = min(merged.get(unit, count), count)
    return {unit: merged[unit] for unit in sorted(merged)}


def _support_unit(support_counts):
    if not support_counts:
        return "unavailable"
    if len(support_counts) == 1:
        return next(iter(support_counts))
    return "minimum_across_mixed_support_counts"


def _correlate(aligned, method, minimum_pairs):
    output = []
    for row in aligned["rows"]:
        pairs = row["pairs"]
        x, y = [item[1] for item in pairs], [item[2] for item in pairs]
        value = None
        window_support_complete = row.get("both_windows_supported", True)
        if len(pairs) >= minimum_pairs and window_support_complete:
            value = _pearson(_rank(x), _rank(y)) if method == "spearman" else _pearson(x, y)
        record = row["series"]["reference"]
        aligned_count = len(pairs)
        support_counts = {"exact_timestamp_pairs": aligned_count}
        details = {"correlation": value, "method": method,
            "numeric_unit": "dimensionless", "source_field": record["source_field"],
            "support_counts": support_counts,
            "minimum_pairs": minimum_pairs,
            "timestamp_alignment": aligned.get("timestamp_scope", "exact_no_interpolation"),
            "selected_kpi": row["kpi"]["reference"]["source_field"],
            "selected_kpi_id": row["kpi"]["reference"]["entity"]}
        if "reference_pair_count" in row or "alert_pair_count" in row:
            support_counts.update({"reference_window_timestamp_pairs": int(row.get("reference_pair_count", 0)),
                                   "alert_window_timestamp_pairs": int(row.get("alert_pair_count", 0))})
            details.update({"reference_window_timestamp_pairs": support_counts["reference_window_timestamp_pairs"],
                "alert_window_timestamp_pairs": support_counts["alert_window_timestamp_pairs"],
                "both_windows_supported": bool(window_support_complete)})
        if "timestamp_scope" in aligned:
            details["timestamp_scope"] = aligned["timestamp_scope"]
        output.append({"key": row["key"], "value": value if value is not None else 0.0,
            "support": aligned_count,
            "support_counts": support_counts,
            "eligible": value is not None,
            "kind": "association", "source_record": record,
            "alert_record": row["series"]["alert"],
            "details": details})
    ready = any(row["eligible"] for row in output)
    missing_window_support = any(row.get("both_windows_supported") is False for row in aligned["rows"])
    reason = (None if ready else
        "joint_window_exact_support_missing_from_one_or_more_windows" if missing_window_support else
        "insufficient_or_constant_aligned_series")
    result = {"rows": output, "status": "ready" if ready else "abstained", "reason": reason}
    if "timestamp_scope" in aligned:
        result["timestamp_scope"] = aligned["timestamp_scope"]
    return result


def _reduce_series(series, statistic, block_minutes, minimum_blocks, runtime):
    block_ms = block_minutes * 60_000
    output = []
    for row in series["rows"]:
        valid = True
        blocks_by_window = {}
        for side in ("reference", "alert"):
            record = row[side]
            start = record["window"]["start_ms_utc"]
            blocks = {}
            for timestamp, value in record["samples"]:
                block = (timestamp - start) // block_ms
                blocks.setdefault(block, []).append(value)
            points = [statistics.median(values) for _block, values in sorted(blocks.items())]
            if len(points) < minimum_blocks:
                valid = False
                summary = None
            else:
                try:
                    if statistic == "mean":
                        summary = math.fsum(points) / len(points)
                    elif statistic == "median":
                        summary = statistics.median(points)
                    elif statistic == "variance":
                        mean = math.fsum(points) / len(points)
                        summary = math.fsum((value - mean) ** 2 for value in points) / len(points)
                    else:
                        ordered = sorted(points)
                        summary = ordered[min(len(ordered) - 1, int(0.9 * (len(ordered) - 1)))]
                    if not math.isfinite(summary):
                        valid, summary = False, None
                except (OverflowError, ValueError):
                    valid, summary = False, None
            blocks_by_window[side] = {"value": summary, "support": len(points),
                "window": record["window"], "record": record}
        support_counts = {"independent_time_blocks": min(
            blocks_by_window["reference"]["support"], blocks_by_window["alert"]["support"])}
        output.append({"key": row["key"], "windows": blocks_by_window,
            "eligible": valid, "kind": statistic, "source_record": row["reference"],
            "support": min(blocks_by_window["reference"]["support"],
                           blocks_by_window["alert"]["support"]),
            "support_counts": support_counts,
            "details": {"statistic": statistic, "block_minutes": block_minutes,
                "minimum_blocks": minimum_blocks,
                "source_field": row["reference"]["source_field"],
                "source_unit": row["reference"]["unit"],
                "numeric_unit": (f"variance({row['reference']['unit']})" if statistic == "variance"
                    else row["reference"]["unit"])}})
    return {"rows": output, "status": "ready" if any(row["eligible"] for row in output) else "abstained",
        "reason": None if any(row["eligible"] for row in output) else "insufficient_independent_blocks"}


def _window_compare(values, operation, runtime):
    floor = float(runtime["absolute_scale_floor"])
    output = []
    for row in values["rows"]:
        before, after = row["windows"]["reference"], row["windows"]["alert"]
        eligible = row["eligible"] and before["value"] is not None and after["value"] is not None
        a, b = before["value"], after["value"]
        if not eligible:
            value = 0.0
        else:
            try:
                if operation == "difference":
                    value = b - a
                elif operation == "relative_change":
                    value = (b - a) / max(abs(a), floor)
                else:
                    value = b / max(abs(a), floor)
            except (OverflowError, ZeroDivisionError):
                value = float("inf")
        if not math.isfinite(value):
            eligible, value = False, 0.0
        output.append({"key": row["key"], "value": value, "support": row["support"],
            "support_counts": dict(row.get("support_counts", {})),
            "eligible": eligible, "kind": operation, "source_record": row["source_record"],
            "alert_record": row.get("alert_record", row["source_record"]),
            "details": {**row["details"], "operation": operation,
                "numeric_unit": ("dimensionless" if operation in {"relative_change", "ratio"}
                    else row["details"].get("numeric_unit", "undeclared")),
                "reference_value": a, "alert_value": b,
                "support_by_window": {key: value["support"] for key, value in row["windows"].items()}}})
    return {"rows": output, "status": "ready" if any(row["eligible"] for row in output) else "abstained",
        "reason": None if any(row["eligible"] for row in output) else "no_supported_window_comparisons"}


def _unary(values, operation):
    output = []
    for row in values["rows"]:
        value = abs(row["value"]) if operation == "absolute" else max(0.0, row["value"])
        output.append({**row, "value": value, "kind": operation})
    return {"rows": output, "status": values["status"], "reason": values.get("reason")}


def _multiply(left, right):
    right_map = {tuple(row["key"]): row for row in right["rows"]}
    output = []
    for row in left["rows"]:
        other = right_map.get(tuple(row["key"]))
        if other is None:
            continue
        eligible = row["eligible"] and other["eligible"]
        try:
            value = row["value"] * other["value"]
        except OverflowError:
            value = float("inf")
        if not math.isfinite(value):
            eligible, value = False, 0.0
        support_counts = _merge_support_counts(row, other)
        output.append({"key": row["key"], "value": value,
            "support": min(row["support"], other["support"]), "eligible": eligible,
            "support_counts": support_counts,
            "kind": "multiply", "source_record": row["source_record"],
            "alert_record": row.get("alert_record", row["source_record"]),
            "details": {"left": row["details"], "right": other["details"],
                "numeric_unit": _multiply_unit(row["details"].get("numeric_unit", "undeclared"),
                    other["details"].get("numeric_unit", "undeclared"))}})
    return {"rows": output, "status": "ready" if any(row["eligible"] for row in output) else "abstained",
        "reason": None if any(row["eligible"] for row in output) else "no_joined_numeric_results"}


def _multiply_unit(left, right):
    if left == "dimensionless":
        return right
    if right == "dimensionless":
        return left
    if left == right:
        return f"({left})^2"
    return "mixed"


def _score_lineage(row, record):
    lineage = {"key": row["key"], "value": row["value"],
        "support": row["support"], "eligible": row["eligible"],
        "details": row["details"], "source_hashes": record["source_hashes"],
        "case_data_sha256": record["case_data_sha256"]}
    return hashlib.sha256(json.dumps(lineage, sort_keys=True, separators=(",", ":"),
        allow_nan=False).encode()).hexdigest()


def _reduce_component(values, method):
    grouped = {}
    for row in values["rows"]:
        role, entity, _metric = row["key"]
        if role != "component":
            continue
        grouped.setdefault((role, entity), []).append(row)
    output = []
    joint_scope = values.get("timestamp_scope") == "reference_and_alert_exact_no_interpolation"
    for key, rows in sorted(grouped.items()):
        valid = [row for row in rows if row["eligible"]]
        if not valid:
            support_counts = _merge_support_counts(*rows) if joint_scope else {}
            row_details = {"metrics": []}
            if joint_scope:
                row_details.update({"timestamp_scope": values["timestamp_scope"],
                    "alignment_support_counts": support_counts})
            output.append({"key": [key[0], key[1], None], "value": 0.0,
                "support": min((row["support"] for row in rows), default=0) if joint_scope else 0,
                "support_counts": support_counts, "eligible": False, "kind": "component_reduce",
                "source_record": rows[0]["source_record"],
                "alert_record": rows[0].get("alert_record", rows[0]["source_record"]),
                "details": row_details})
            continue
        units = {row["details"].get("numeric_unit", "undeclared") for row in valid}
        source_fields = {row["source_record"].get("source_field") for row in valid}
        incompatible_native_fields = len(source_fields) > 1 and "dimensionless" not in units
        if len(units) != 1 or "mixed" in units or incompatible_native_fields:
            output.append({"key": [key[0], key[1], None], "value": 0.0,
                "support": 0, "support_counts": _merge_support_counts(*valid),
                "eligible": False, "kind": "component_reduce",
                "source_record": valid[0]["source_record"],
                "alert_record": valid[0].get("alert_record", valid[0]["source_record"]),
                "details": {"method": method, "metrics": [], "score_unit": "incompatible_units"}})
            continue
        scores = [row["value"] for row in valid]
        metric_fields = sorted({row["source_record"].get("source_field") for row in valid
            if isinstance(row["source_record"].get("source_field"), str)})
        support_counts = _merge_support_counts(*valid)
        try:
            score = max(scores) if method == "max" else math.fsum(scores) / len(scores)
        except OverflowError:
            score = float("inf")
        score_finite = math.isfinite(score)
        if not score_finite:
            score = 0.0
        output.append({"key": [key[0], key[1], None], "value": score,
            "support": min(row["support"] for row in valid),
            "support_counts": support_counts, "eligible": score_finite and score > 0,
            "kind": "component_reduce", "source_record": valid[0]["source_record"],
            "alert_record": valid[0].get("alert_record", valid[0]["source_record"]),
            "details": {"method": method, "score_unit": next(iter(units)),
                "support_counts": support_counts,
                "support_unit": _support_unit(support_counts),
                "metric_fields": metric_fields,
                "metrics": [{"metric_key": row["key"][2],
                "value": row["value"], "support": row["support"],
                "eligible": row["eligible"], "details": row["details"]} for row in valid]}})
    ready = any(row["eligible"] for row in output)
    no_score_reason = (values.get("reason") if joint_scope and values.get("reason") else
                       "no_positive_component_scores")
    return {"rows": output, "status": "ready" if ready else "abstained",
        "reason": None if ready else no_score_reason}


def _build_evidence(scores, statistic):
    evidence = []
    for row in scores["rows"]:
        if row.get("eligible") is not True or row.get("value", 0) <= 0:
            continue
        rec = row.get("alert_record", row["source_record"])
        reference = row["source_record"]
        lineage_sha = _score_lineage(row, rec)
        evidence.append({"role": row["key"][0], "entity": row["key"][1],
            "source": rec["source_table"], "window": rec["window"],
            "statistic": statistic,
            "value": row["value"], "support": row["support"],
            "ranking_eligible": row["eligible"] is True,
            "source_hashes": rec["source_hashes"],
            "case_data_sha256": rec["case_data_sha256"],
            "details": {**row["details"], "reference_window": {key: reference["window"][key]
                    for key in ("name", "start_ms_utc", "end_ms_utc")},
                "metric_kind": rec["metric_kind"],
                "metric_field": (row["details"].get("metric_fields", [rec["source_field"]])[0]
                    if len(row["details"].get("metric_fields", [rec["source_field"]])) == 1 else None),
                "metric_fields": row["details"].get("metric_fields", [rec["source_field"]]),
                "unit": row["details"].get("score_unit", "dimensionless"),
                "score_lineage_sha256": lineage_sha,
                "policy_provenance": "existing_bank_metric_kind_policy_allowlist",
                "support_counts": dict(row.get("support_counts", {})),
                "support_unit": _support_unit(row.get("support_counts", {})),
                "ranking_eligible": row["eligible"] is True}})
    return {"rows": evidence, "status": scores["status"], "reason": scores.get("reason")}


def _assemble(inputs, runtime):
    candidate_set = _fields(inputs["candidates"], "CandidateSet")
    candidates = candidate_set.get("candidates", [])
    score_map = {(row["key"][0], row["key"][1]): row for row in inputs["scores"]["rows"]}
    evidence_map = {}
    for row in inputs["evidence"]["rows"]:
        evidence_map.setdefault((row["role"], row["entity"]), []).append(row)
    output = []
    for candidate in candidates:
        role, identifier = _canonical_entity(candidate.get("entity"))
        score = score_map.get((role, identifier))
        evs = evidence_map.get((role, identifier), [])
        lineage_sha = _score_lineage(score, score.get("alert_record", score["source_record"])) if score else None
        evidence_matches = [e for e in evs if score and e.get("ranking_eligible") is True
            and e.get("value") == score.get("value") and e.get("support") == score.get("support")
            and e.get("details", {}).get("score_lineage_sha256") == lineage_sha]
        score_value = float(score["value"]) if score and score["eligible"] and evidence_matches else 0.0
        output.append({"entity": {"role": role, "identifier": identifier},
            "score": score_value, "evidence": evidence_matches,
            "analysis_family": runtime.get("ir_family", "bank_ir_analysis")})
    ready = any(row["score"] > 0 and any(e.get("ranking_eligible") for e in row["evidence"])
        for row in output)
    return {"status": "ready" if ready else "abstained",
        "reason": None if ready else inputs["evidence"].get("reason", "no_eligible_IR_evidence"),
        "candidates": output}


def _invoke(kernel, inputs, params, runtime, trusted):
    if kernel == "series.component_metrics":
        return _component_series(inputs, trusted)
    if kernel == "series.primary_kpi":
        return _primary_kpi(inputs, trusted)
    if kernel == "series.select_field":
        return _select_field(inputs["series"], params["source_field"])
    if kernel == "series.align_kpi":
        return _align_kpi(inputs["components"], inputs["kpi"])
    if kernel == "series.align_kpi_both_windows":
        return _align_kpi_both_windows(inputs["components"], inputs["kpi"])
    if kernel == "series.correlate":
        return _correlate(inputs["pairs"], params["method"], params["minimum_pairs"])
    if kernel == "series.reduce":
        return _reduce_series(inputs["series"], params["statistic"], params["block_minutes"],
            params["minimum_blocks"], runtime)
    if kernel == "numbers.window_compare":
        return _window_compare(inputs["values"], params["operation"], runtime)
    if kernel == "numbers.absolute":
        return _unary(inputs["values"], "absolute")
    if kernel == "numbers.positive_part":
        return _unary(inputs["values"], "positive_part")
    if kernel == "numbers.multiply":
        return _multiply(inputs["left"], inputs["right"])
    if kernel == "numbers.reduce_component":
        return _reduce_component(inputs["values"], params["method"])
    if kernel == "evidence.from_scores":
        return _build_evidence(inputs["scores"], runtime.get("ir_statistic", "typed_ir_metric_change"))
    if kernel == "scores.assemble":
        return _assemble(inputs, runtime)
    _fail("ir_kernel_unknown", f"kernel {kernel!r} is not in the fixed ABI")


def _estimated_work(kernel, inputs):
    if kernel == "series.reduce":
        return sum(len(row[side]["samples"]) for row in inputs["series"]["rows"]
            for side in ("reference", "alert"))
    if kernel == "series.correlate":
        return sum(len(row["pairs"]) for row in inputs["pairs"]["rows"])
    if kernel in {"series.align_kpi", "series.align_kpi_both_windows"}:
        component_points = sum(len(row[side]["samples"])
            for row in inputs["components"]["rows"] for side in ("reference", "alert"))
        kpi = inputs["kpi"].get("rows", [])
        kpi_points = sum(len(row[side]["samples"]) for row in kpi for side in ("reference", "alert"))
        return component_points + kpi_points
    if kernel == "series.select_field":
        return len(inputs["series"]["rows"])
    if kernel == "numbers.reduce_component":
        return len(inputs["values"]["rows"])
    if kernel == "numbers.multiply":
        return len(inputs["left"]["rows"]) + len(inputs["right"]["rows"])
    return max(1, sum(len(value.get("rows", [])) for value in inputs.values()
        if isinstance(value, dict)))


def execute_ir(program, raw_inputs, parameters, runtime, trusted):
    """Validate again and execute a finite DAG; returns bounded JSON only."""
    if not isinstance(program, dict) or program.get("schema_version") != IR_SCHEMA or set(program) != {"schema_version", "nodes", "return"}:
        _fail("ir_schema", "unsupported or malformed IR program")
    nodes = program.get("nodes")
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= MAX_NODES:
        _fail("ir_node_limit", "IR program node count is outside 1..40")
    if not isinstance(raw_inputs, dict):
        _fail("ir_inputs", "IR inputs must be an object")
    inputs = _decode(raw_inputs)
    runtime = runtime if isinstance(runtime, dict) else {}
    values = {}
    types = {}
    depths = {}
    evidence_sources = {}
    operations = 0
    for row in nodes:
        if not isinstance(row, dict) or set(row) != {"id", "kernel", "version", "inputs", "params", "bindings"}:
            _fail("ir_node_shape", "IR nodes must use exact id/kernel/version/inputs/params/bindings fields")
        node_id, kernel, version = row["id"], row["kernel"], row["version"]
        identity = f"{kernel}@{version}"
        signature = KERNELS.get(identity)
        if signature is None:
            _fail("ir_kernel_unknown", f"kernel {identity!r} is not registered")
        if not isinstance(node_id, str) or not node_id or node_id in values:
            _fail("ir_node_id", "IR node IDs must be unique nonempty strings")
        raw_node_inputs = row["inputs"]
        if not isinstance(raw_node_inputs, dict) or set(raw_node_inputs) != set(signature["inputs"]):
            _fail("ir_ports", f"{node_id} inputs differ from the fixed kernel ports")
        resolved_inputs = {}
        for port, ref in raw_node_inputs.items():
            if not isinstance(ref, str):
                _fail("ir_reference", "IR references are scalar node or $input.port strings")
            if ref.startswith("$input."):
                name = ref[7:]
                value = inputs.get(name)
                expected = signature["inputs"][port]
                value = _fields(value, expected) if expected in {"Aggregate", "CandidateSet"} else value
            else:
                if ref not in values:
                    _fail("ir_dag_order", "IR references must point to earlier nodes")
                value = values[ref]
                expected = signature["inputs"][port]
                if types[ref] != expected:
                    _fail("ir_edge_type", f"{node_id}.{port} expects {expected}, received {types[ref]}")
            resolved_inputs[port] = value
        if kernel == "evidence.from_scores":
            evidence_sources[node_id] = raw_node_inputs["scores"]
        elif (kernel == "scores.assemble" and
                evidence_sources.get(raw_node_inputs["evidence"]) != raw_node_inputs["scores"]):
            _fail("ir_evidence_lineage", "assembly evidence must be derived from the same ComponentScores node")
        raw_params, bindings = row["params"], row["bindings"]
        if not isinstance(raw_params, dict) or not isinstance(bindings, dict):
            _fail("ir_params", "IR params and bindings must be objects")
        specs = signature["params"]
        if set(raw_params) | set(bindings) != set(specs) or set(raw_params) & set(bindings):
            _fail("ir_params", f"{node_id} must supply every kernel parameter exactly once")
        resolved_params = {}
        for name, descriptor in specs.items():
            if name in bindings:
                binding = bindings[name]
                expected_field = descriptor.get("runtime_field")
                if binding != f"runtime.{expected_field}" or expected_field not in runtime:
                    _fail("ir_runtime_binding", f"{node_id}.{name} must use its fixed runtime binding")
                value = runtime[expected_field]
            else:
                if descriptor.get("runtime_field"):
                    _fail("ir_runtime_binding", f"{node_id}.{name} must use its bounded runtime policy binding")
                value = raw_params[name]
            if descriptor["type"] == "string":
                valid = isinstance(value, str)
            elif descriptor["type"] == "positive_integer":
                valid = type(value) is int and value > 0
            else:
                valid = _finite(value)
            if not valid or (descriptor.get("choices") and value not in descriptor["choices"]):
                _fail("ir_parameter_value", f"{node_id}.{name} violates the kernel parameter contract")
            resolved_params[name] = value
        estimated = _estimated_work(kernel, resolved_inputs)
        if operations + estimated > MAX_OPERATIONS:
            _fail("ir_operation_budget", "IR operation budget exceeded before kernel execution")
        result = _invoke(kernel, resolved_inputs, resolved_params, runtime, trusted)
        output_type = signature["output"]
        if not isinstance(result, dict) or not isinstance(result.get("rows"), list) and output_type != "ScoredCandidates":
            _fail("ir_kernel_output", f"{node_id} returned malformed {output_type}")
        row_count = len(result.get("rows", result.get("candidates", [])))
        operations += estimated + max(1, row_count)
        if operations > MAX_OPERATIONS:
            _fail("ir_operation_budget", "IR cumulative operation budget exceeded")
        values[node_id], types[node_id] = result, output_type
        parent_depths = [depths[ref] for ref in raw_node_inputs.values()
            if isinstance(ref, str) and not ref.startswith("$input.")]
        depths[node_id] = 1 + max(parent_depths, default=0)
        if depths[node_id] > 12:
            _fail("ir_depth_limit", "IR program exceeds the protected depth limit of 12")
    returned = program.get("return")
    if not isinstance(returned, dict) or set(returned) != {"node"} or returned["node"] not in values:
        _fail("ir_return", "IR return node is invalid")
    final = values[returned["node"]]
    if types[returned["node"]] != "ScoredCandidates":
        _fail("ir_return_type", "IR must return trusted ScoredCandidates")
    if len(final.get("candidates", [])) > 1000:
        _fail("ir_output_budget", "IR output candidate limit exceeded")
    return _wire_scored(final)


def _wire_map(value):
    return {"$map": value}


def _wire_window(value):
    return {"$type": "Window", "$fields": {key: value[key]
        for key in ("name", "start_ms_utc", "end_ms_utc")}}


def _wire_scored(value):
    """Materialize the exact trusted ScoredCandidates cache contract envelope."""
    output = []
    for row in value.get("candidates", []):
        entity = row["entity"]
        wire_entity = {"$type": "EntityRef", "$fields": {
            "role": entity["role"], "identifier": entity["identifier"]}}
        evidence = []
        for item in row.get("evidence", []):
            source_hashes = item.get("source_hashes", {})
            window = item["window"]
            details = dict(item.get("details", {}))
            details.update({"case_data_sha256": item.get("case_data_sha256"),
                "source_hashes": source_hashes,
                "ranking_eligible": item.get("ranking_eligible") is True})
            evidence.append({"$type": "Evidence", "$fields": {
                "source": item["source"], "window": _wire_window(window),
                "statistic": item["statistic"], "value": item["value"],
                "contributing_entities": {"$tuple": [wire_entity]},
                "support": item["support"], "details": _wire_map(details)}})
        output.append({"$type": "ScoredCandidate", "$fields": {
            "entity": wire_entity, "score": row["score"], "category": None,
            "evidence": {"$tuple": evidence}, "analysis_family": row.get("analysis_family")}})
    return {"$type": "ScoredCandidates", "$fields": {
        "candidates": {"$tuple": output}, "scorer": "typed-operator-ir-v1",
        "status": value.get("status", "abstained"),
        "status_details": _wire_map({"reason": value.get("reason")})}}
