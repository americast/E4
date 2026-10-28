"""Reconstructed backend Telecom analysis operators (not the unpublished E4 code)."""

from __future__ import annotations

import math
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

import numpy as np
from scipy import stats
from sklearn.linear_model import Lasso
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler

from e4.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
                          EntityRef, Evidence, RankedAnswer, ScoredCandidate,
                          ScoredCandidates, Span, Telemetry)
from e4.dsl.executor import NodeContext, OperatorResult
from e4.operators.numerics import (block_reduce, contrast_score, median_within_blocks, permutation_contrast,
                                   stable_seed)
from e4.dsl.registry import OperatorRegistry, OperatorSpec, PortSpec
from e4.dsl.runtime import ParameterSpec
from e4.paths import ROOT


_METRIC_POLICY = json.loads((ROOT /
                             "config/telecom/metric_kind_policy.json").read_text())


def telecom_registry() -> OperatorRegistry:
    """Build the supported six-family reconstructed operator registry."""
    registry = OperatorRegistry()
    registry.register(OperatorSpec("case.telemetry", "1", (), "Telemetry", cacheable=False,
        external_input=True,
        description="Executor-injected, hash-verified public per-case telemetry."))
    registry.register(OperatorSpec("metrics.aggregate", "1",
        (PortSpec("data", "Telemetry"),), "Aggregate",
        {"window": ParameterSpec("window", "string", required=True,
                                  choices=("reference", "alert"))}, family="aggregation",
        implementation=aggregate_metrics,
        description="Aggregate all metric series within one explicitly selected window."))
    registry.register(OperatorSpec("traces.aggregate", "1",
        (PortSpec("data", "Telemetry"),), "Aggregate",
        {"window": ParameterSpec("window", "string", required=True,
                                  choices=("reference", "alert"))}, family="aggregation",
        implementation=aggregate_traces,
        description="Aggregate one explicitly selected window into per-trace independent units."))
    registry.register(OperatorSpec("candidates.generate", "1",
        (PortSpec("data", "Telemetry"),
         PortSpec("aggregates", "Aggregate", required=False, variadic=True, minimum_items=0)),
        "CandidateSet", family="candidate", implementation=generate_candidates,
        description="Label-free candidates from deployment catalog and observed backend roles."))
    registry.register(OperatorSpec("metrics.contrast", "1",
        (PortSpec("reference", "Aggregate"), PortSpec("alert", "Aggregate"),
         PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates", family="metric", implementation=score_metrics,
        description="Robust median/tail metric contrasts with block-permutation evidence."))
    registry.register(OperatorSpec("traces.error_contrast", "1",
        (PortSpec("reference", "Aggregate"), PortSpec("alert", "Aggregate"),
         PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates", family="trace_error", implementation=score_trace_errors,
        description="Per-trace failure-fraction contrasts with valid denominators."))
    registry.register(OperatorSpec("traces.latency_contrast", "1",
        (PortSpec("reference", "Aggregate"), PortSpec("alert", "Aggregate"),
         PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates", family="trace_latency", implementation=score_trace_latency,
        description="Within-elapsedTime per-trace median and tail contrasts; no cross-field conversion."))
    registry.register(OperatorSpec("flow.conditional_contribution", "1",
        (PortSpec("data", "Telemetry"), PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates", family="flow", implementation=score_conditional_flow,
        description="L1 conditional attribution from one unambiguous root OSB logical service to same-trace backend span elapsedTime; abstains on mixed root KPI services or weak support/fit."))
    registry.register(OperatorSpec("dependencies.propagation", "1",
        (PortSpec("data", "Telemetry"), PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates", family="dependency", implementation=score_dependency,
        description="Same-trace failure propagation and deployment relationship evidence; unresolved links stay missing."))
    registry.register(OperatorSpec("answers.combine", "1",
        (PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),),
        "RankedAnswer", {"limit": ParameterSpec("limit", "positive_integer", runtime_field="lead_count")},
        family="ranking", implementation=combine_scores,
        description="Maximum-normalized family scores, common weighted denominator, canonical-ID deduplication."))
    return registry


def classify_metric_kind(table_path: str, metric_name: str) -> tuple[str, str]:
    """Classify only explicit supported names; unknown names stay descriptive-only."""
    if table_path.endswith("metric_app.csv"):
        spec = _METRIC_POLICY.get("metric_app", {}).get(metric_name)
        if spec:
            return spec["kind"], spec["provenance"]
        return "unknown", "metric_app field is absent from the explicit policy"
    table = next((name for name in ("container", "middleware", "node", "service")
                  if table_path.endswith(f"metric_{name}.csv")), None)
    if table is None:
        return "unknown", "source table is not in the policy"
    if metric_name in _METRIC_POLICY["counters"].get(table, []):
        if table == "middleware" and metric_name in {
            "total_commands_processed", "total_connections_received", "expired_keys",
            "evicted_keys", "keyspace_hits", "keyspace_misses", "used_cpu_sys", "used_cpu_user",
        }:
            provenance = "Redis INFO documentation: cumulative total/counter field"
        else:
            provenance = "explicit custom Telecom counter assumption in metric_kind_policy.json"
        return "counter", provenance
    if metric_name in _METRIC_POLICY["rates"].get(table, []):
        return "rate", "explicit per-second/rate telemetry field in metric_kind_policy.json"
    if metric_name in _METRIC_POLICY["gauges_and_rates"].get(table, []):
        return "gauge", "explicit gauge assumption in metric_kind_policy.json"
    return "unknown", "metric name is absent from the explicit policy"


def metric_contrast_direction(metric_key: str, metric_kind: str) -> str:
    if metric_kind == "counter_rate":
        return _METRIC_POLICY["directions"].get("counter_rate", {}).get("default", "two_sided")
    table, metric_name = metric_key.split("|", 2)[:2]
    table_name = next((name for name in ("metric_app", "container", "middleware", "node", "service")
                       if table.endswith(f"{name}.csv")), None)
    if table_name == "metric_app":
        return _METRIC_POLICY["directions"]["metric_app"].get(
            metric_name, _METRIC_POLICY["directions"]["metric_app"]["default"])
    return _METRIC_POLICY["directions"].get(table_name or "", {}).get("default", "two_sided")


def aggregate_metrics(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> OperatorResult:
    telemetry: Telemetry = inputs["data"]
    selected_name = params["window"]
    if selected_name not in {"reference", "alert"}:
        raise ValueError("metric aggregate window must be reference or alert")
    selected_window = telemetry.reference_window if selected_name == "reference" else telemetry.alert_window
    tail_quantile = float(context.runtime_values["tail_quantile"])
    # Collapse repeated samples at a timestamp, and transform declared
    # cumulative counters to reset-aware rates before window aggregation.
    series_samples: dict[tuple, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    raw_counts: dict[tuple, int] = defaultdict(int)
    kind_for_series: dict[tuple, tuple[str, str]] = {}
    for point in telemetry.metrics:
        window_name = _window_name(telemetry, point.timestamp_ms_utc)
        if window_name is None:
            continue
        series_key = (point.table, point.entity, point.metric_name,
                      point.bomc_id or "", point.item_id or "")
        identity = (*series_key, point.timestamp_ms_utc)
        raw_counts[identity] += 1
        kind_for_series[series_key] = classify_metric_kind(point.table, point.metric_name)
        series_samples[series_key]  # materialize even when every numeric value is invalid
        if point.value is not None and math.isfinite(point.value):
            series_samples[series_key][point.timestamp_ms_utc].append(point.value)
    series: dict[tuple, list[tuple[str, int, float, int, int]]] = defaultdict(list)
    kind_counts = defaultdict(int)
    for series_key, timestamp_values in series_samples.items():
        table, entity, metric_name, bomc_id, item_id = series_key
        kind, kind_source = kind_for_series[series_key]
        distinct = [(timestamp, float(statistics.median(values)), raw_counts[(*series_key, timestamp)])
                    for timestamp, values in sorted(timestamp_values.items()) if values]
        if kind == "counter":
            transformed = []
            previous = None
            for timestamp, value, raw_count in distinct:
                if previous is not None:
                    previous_time, previous_value = previous
                    elapsed_minutes = (timestamp - previous_time) / 60_000
                    if elapsed_minutes <= 0 or value < 0:
                        previous = (timestamp, value)
                        continue
                    reset = value < previous_value
                    delta = value if reset else value - previous_value
                    transformed.append((timestamp, delta / elapsed_minutes, raw_count, int(reset)))
                previous = (timestamp, value)
            observations = transformed
            output_kind = "counter_rate"
        else:
            observations = [(timestamp, value, raw_count, 0)
                            for timestamp, value, raw_count in distinct]
            output_kind = kind
        metric_key = "|".join((table, metric_name, bomc_id, item_id))
        for timestamp, value, raw_count, was_reset in observations:
            window_name = _window_name(telemetry, timestamp)
            if window_name is not None:
                series[(entity, metric_key)].append((window_name, timestamp, value, raw_count, was_reset))
        kind_counts[output_kind] += 1
    rows: list[AggregateRow] = []
    duplicate_rows_total = 0
    counter_reset_total = 0
    for (entity, metric_key), observations in sorted(series.items(), key=lambda item: (item[0][0], item[0][1])):
        table, metric_name, _, _ = metric_key.split("|", 3)
        kind, kind_source = classify_metric_kind(table, metric_name)
        output_kind = "counter_rate" if kind == "counter" else kind
        for row_window_name in ("reference", "alert"):
            selected = [(timestamp, value, raw_count, was_reset)
                        for window, timestamp, value, raw_count, was_reset in observations
                        if window == row_window_name]
            if not selected:
                continue
            if row_window_name != selected_name:
                continue
            window = selected_window
            values = [value for _, value, _, _ in selected]
            bucket_samples = tuple((timestamp, value) for timestamp, value, _, _ in selected)
            duplicate_total = sum(max(0, raw_count - 1) for _, _, raw_count, _ in selected)
            reset_total = sum(was_reset for _, _, _, was_reset in selected)
            duplicate_rows_total += duplicate_total
            counter_reset_total += reset_total
            common_details = {"bucket_samples": bucket_samples,
                              "raw_rows": sum(raw_count for _, _, raw_count, _ in selected),
                              "duplicate_timestamp_rows": duplicate_total,
                              "counter_resets": reset_total,
                              "metric_kind": output_kind,
                              "kind_provenance": kind_source,
                              "tail_quantile": tail_quantile,
                              "aggregation": ("median per repeated timestamp, then requested window statistic"
                                  if kind != "counter" else _METRIC_POLICY["counter_transform"]["name"]),
                              "rate_unit": ("source count delta per elapsed minute" if kind == "counter" else None)}
            for statistic, value in (("median", float(statistics.median(values))),
                                     ("mean", float(statistics.mean(values))),
                                     ("tail_quantile", float(np.quantile(values, tail_quantile, method="linear")) )):
                rows.append(AggregateRow(entity, metric_key, window, statistic, value,
                                         len(selected), (table,), (entity,), common_details))
    return OperatorResult(Aggregate(selected_window,
                                    tuple(rows), tuple(sorted({point.table for point in telemetry.metrics})),
                                    {"raw_metric_points": len(telemetry.metrics),
                                     "series_count": len(series_samples),
                                     "window_name": selected_name,
                                     "metric_kind_counts": dict(kind_counts),
                                     "duplicate_timestamp_rows": duplicate_rows_total,
                                     "counter_reset_count": counter_reset_total,
                                     "kind_policy_version": _METRIC_POLICY["version"],
                                     "duplicate_policy": "median per key and distinct timestamp"}),
                          rows_processed=len(telemetry.metrics))


def aggregate_traces(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> OperatorResult:
    telemetry: Telemetry = inputs["data"]
    selected_name = params["window"]
    if selected_name not in {"reference", "alert"}:
        raise ValueError("trace aggregate window must be reference or alert")
    selected_window = telemetry.reference_window if selected_name == "reference" else telemetry.alert_window
    tail_quantile = float(context.runtime_values["tail_quantile"])
    # Each trace/entity/callType contributes at most one success fraction,
    # duration median and duration tail, avoiding span-count pseudoreplication.
    per_trace: dict[tuple, dict[str, Any]] = {}
    valid_status = 0
    invalid_status = 0
    elapsed_valid = 0
    elapsed_invalid = 0
    for span in telemetry.spans:
        if not selected_window.contains(span.start_ms_utc):
            continue
        entities = tuple(entity for entity in (span.executing_component, span.logical_service, span.database_target)
                         if entity is not None)
        elapsed = _finite_number(span.elapsed_raw)
        if elapsed is None:
            elapsed_invalid += 1
        else:
            elapsed_valid += 1
        if span.success_status == "valid":
            valid_status += 1
        else:
            invalid_status += 1
        for entity in entities:
            key = (selected_name, entity, span.call_type, span.trace_id)
            row = per_trace.setdefault(key, {"valid": 0, "failures": 0, "elapsed": [], "unresolved": 0})
            if span.success_status == "valid":
                row["valid"] += 1
                row["failures"] += int(span.success is False)
            if elapsed is not None:
                row["elapsed"].append(elapsed)
            row["unresolved"] += int(span.parent_unresolved_in_clip)
    grouped: dict[tuple, dict[str, list[tuple[str, float]]]] = defaultdict(lambda: {"reference": [], "alert": []})
    for (window_name, entity, call_type, trace_id), row in per_trace.items():
        metric_base = f"trace|{call_type}"
        if row["valid"]:
            grouped[(entity, metric_base + "|failure_rate")][window_name].append(
                (trace_id, row["failures"] / row["valid"]))
        if row["elapsed"]:
            grouped[(entity, metric_base + "|elapsed_mean")][window_name].append(
                (trace_id, float(statistics.mean(row["elapsed"]))))
            grouped[(entity, metric_base + "|elapsed_median")][window_name].append(
                (trace_id, float(statistics.median(row["elapsed"]))))
            grouped[(entity, metric_base + "|elapsed_tail")][window_name].append(
                (trace_id, float(np.quantile(row["elapsed"], tail_quantile, method="linear"))))
    rows: list[AggregateRow] = []
    for (entity, metric_key), by_window in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        for window_name, values_by_trace in by_window.items():
            if not values_by_trace:
                continue
            if window_name != selected_name:
                continue
            window = selected_window
            values = [value for _, value in sorted(values_by_trace)]
            trace_samples = tuple(values)
            stat_name = ("per_trace_failure_fraction" if metric_key.endswith("|failure_rate")
                         else "per_trace_elapsed_value")
            base_details = {"independent_unit": "traceId",
                            "per_trace_values": trace_samples,
                            "trace_count": len(values),
                            "tail_quantile": tail_quantile,
                            "invalid_success_rows": invalid_status,
                            "elapsed_numeric_rows": elapsed_valid,
                            "elapsed_invalid_rows": elapsed_invalid,
                            "duration_units": "not declared by source; within elapsedTime only"}
            value = (float(statistics.mean(values)) if metric_key.endswith("|failure_rate")
                     else float(statistics.median(values)))
            rows.append(AggregateRow(entity, metric_key, window, stat_name, value,
                                     len(values), ("trace/trace_span.csv",), (entity,), base_details))
    # Add per-entity propagation-oriented event counts while the original spans
    # remain available for the dedicated dependency scorer.
    return OperatorResult(Aggregate(selected_window,
                                    tuple(rows), ("trace/trace_span.csv",),
                                    {"window_name": selected_name,
                                     "span_rows": len(telemetry.spans),
                                     "trace_entity_calltype_units": len(per_trace),
                                     "valid_success_rows": valid_status,
                                     "missing_or_invalid_success_rows": invalid_status,
                                     "elapsed_numeric_rows": elapsed_valid,
                                     "elapsed_invalid_rows": elapsed_invalid,
                                     "observed_roles": ["component", "service", "database"]}),
                          rows_processed=len(telemetry.spans))


def generate_candidates(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> CandidateSet:
    telemetry: Telemetry = inputs["data"]
    aggregates: tuple[Aggregate, ...] = inputs.get("aggregates", ()) or ()
    candidates: dict[tuple[str, str], Candidate] = {}
    for identifier, entity_info in telemetry.deployment_catalog.items():
        entity = EntityRef("component", identifier)
        candidates[(entity.role, entity.canonical_id)] = Candidate(entity, "deployment catalog entity")
    for point in telemetry.metrics:
        key = (point.entity.role, point.entity.canonical_id)
        candidates.setdefault(key, Candidate(point.entity, f"observed in {point.table}"))
    for span in telemetry.spans:
        for entity in (span.executing_component, span.logical_service, span.database_target):
            if entity is not None:
                candidates.setdefault((entity.role, entity.canonical_id),
                                      Candidate(entity, f"observed as trace {entity.role}"))
    for aggregate in aggregates:
        for row in aggregate.rows:
            candidates.setdefault((row.entity.role, row.entity.canonical_id),
                                  Candidate(row.entity, f"observed in {row.metric_key}"))
    return CandidateSet(tuple(candidates[key] for key in sorted(candidates)),
                        source="public deployment catalog and observed telemetry roles")


def score_metrics(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    reference_aggregate: Aggregate = inputs["reference"]
    alert_aggregate: Aggregate = inputs["alert"]
    candidates: CandidateSet = inputs["candidates"]
    rows_by_key = _pair_rows(reference_aggregate, alert_aggregate)
    values = context.runtime_values
    min_support = int(values["minimum_support"])
    min_blocks = int(values["minimum_independent_blocks"])
    block_minutes = int(values["block_minutes"])
    resamples = int(values["permutation_resamples"])
    seed = int(values["seed"])
    min_requests = int(values["minimum_request_count"])
    rel_floor = float(values["relative_scale_floor"])
    abs_floor = float(values["absolute_scale_floor"])
    p_floor = float(values["permutation_p_score_floor"])
    min_effect = float(values["minimum_normalized_effect"])
    max_p = float(values["maximum_change_p_value"])
    tail_quantile = float(values["tail_quantile"])
    rows_by_entity: dict[tuple[str, str], list[tuple[tuple[str, str, str, str], dict[str, AggregateRow]]]] = defaultdict(list)
    for key, pair in rows_by_key.items():
        rows_by_entity[(key[0], key[1])].append((key, pair))
    scores = []
    for candidate in candidates.candidates:
        evidence: list[Evidence] = []
        metric_scores = []
        entity_rows = rows_by_entity.get((candidate.entity.role, candidate.entity.canonical_id), ())
        for (role, entity_id, metric_key, statistic), pair in entity_rows:
            if statistic not in {"median", "tail_quantile"}:
                continue
            reference = pair.get("reference")
            alert = pair.get("alert")
            if reference is None or alert is None:
                continue
            if reference.support < min_support or alert.support < min_support:
                continue
            metric_name = metric_key.split("|")[1]
            metric_kind = reference.details.get("metric_kind", "unknown")
            if metric_kind == "unknown":
                evidence.append(Evidence(reference.source_tables[0], alert.window,
                    "unknown_metric_kind_excluded", reference.value, (candidate.entity,),
                    min(reference.support, alert.support),
                    {"metric_key": metric_key, "kind_provenance": reference.details.get("kind_provenance")}))
                continue
            if metric_name in {"num", "succee_num"}:
                evidence.append(Evidence(reference.source_tables[0], alert.window,
                    "window_count_context_only", alert.value, (candidate.entity,),
                    min(reference.support, alert.support), {"metric_key": metric_key,
                    "policy": "traffic counts gate/support latency but do not independently rank a component"}))
                continue
            if metric_name == "avg_time" and metric_key.startswith("metric/metric_app.csv|"):
                ref_requests, alert_requests = _metric_app_requests(
                    reference_aggregate, alert_aggregate, candidate.entity)
                if ref_requests is None or alert_requests is None or min(ref_requests, alert_requests) < min_requests:
                    evidence.append(Evidence("metric/metric_app.csv", alert.window,
                        "low_traffic_abstention", min(ref_requests or 0, alert_requests or 0),
                        (candidate.entity,), min(reference.support, alert.support),
                        {"minimum_request_count": min_requests, "metric_key": metric_key}))
                    continue
            is_tail = statistic == "tail_quantile"
            block_quantile = tail_quantile if is_tail else 0.5
            before = block_reduce(reference.details.get("bucket_samples", ()),
                                  reference.window.start_ms_utc, block_minutes, block_quantile)
            after = block_reduce(alert.details.get("bucket_samples", ()),
                                 alert.window.start_ms_utc, block_minutes, block_quantile)
            result = permutation_contrast(before, after, seed=stable_seed(seed, entity_id, metric_key, statistic),
                                          resamples=resamples, statistic="quantile" if is_tail else "median",
                                          minimum_blocks=min_blocks,
                                          relative_scale_floor=rel_floor,
                                          absolute_scale_floor=abs_floor,
                                          quantile=tail_quantile)
            if result is None:
                continue
            direction = metric_contrast_direction(metric_key, metric_kind)
            direction_ok = (direction == "two_sided" or
                            (direction == "higher_is_worse" and result.raw_delta > 0) or
                            (direction == "lower_is_worse" and result.raw_delta < 0))
            eligible = (direction_ok and abs(result.normalized_effect) >= min_effect
                        and result.p_value <= max_p)
            score = contrast_score(result, p_floor=p_floor) if eligible else 0.0
            metric_scores.append(score)
            evidence.append(Evidence(
                source=reference.source_tables[0], window=alert.window,
                statistic=f"{statistic}_block_permutation_contrast",
                value=result.raw_delta, contributing_entities=(candidate.entity,),
                support=min(result.baseline_support, result.alert_support),
                details={"metric_key": metric_key, "baseline_center": result.baseline_center,
                         "alert_center": result.alert_center,
                         "normalized_effect": result.normalized_effect, "p_value": result.p_value,
                         "ranking_eligible": eligible,
                         "contrast_direction": direction,
                         "minimum_normalized_effect": min_effect,
                         "maximum_change_p_value": max_p,
                         "metric_kind": metric_kind,
                         "kind_provenance": reference.details.get("kind_provenance"),
                         "baseline_mad_scale": result.baseline_mad_scale,
                         "relative_scale_floor": rel_floor,
                         "absolute_scale_floor": abs_floor,
                         "permutation_p_score_floor": p_floor,
                         "tail_quantile": tail_quantile if is_tail else None,
                         "independent_unit": f"{block_minutes}-minute nonoverlapping block medians",
                         "source_duplicate_aggregation": "median per distinct timestamp"}))
        scores.append(ScoredCandidate(candidate.entity, max(metric_scores, default=0.0), None,
                                      tuple(evidence), analysis_family="metric"))
    ready = any(any("permutation_contrast" in item.statistic for item in score.evidence)
                for score in scores)
    return ScoredCandidates(tuple(scores), scorer="metrics.contrast",
                            status="ready" if ready else "abstained",
                            status_details={} if ready else {"reason": "no_metric_series_met_support_and_traffic_gates"})


def score_trace_errors(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    return _score_trace_rows(inputs["reference"], inputs["alert"], inputs["candidates"], context,
                             suffix="failure_rate", statistic="per_trace_failure_fraction",
                             family="trace_error", contrast_stat="mean")


def score_trace_latency(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    return _score_trace_rows(inputs["reference"], inputs["alert"], inputs["candidates"], context,
                             suffix="elapsed", statistic=None,
                             family="trace_latency", contrast_stat="median")


def _score_trace_rows(reference_aggregate: Aggregate, alert_aggregate: Aggregate,
                      candidates: CandidateSet, context: NodeContext,
                      *, suffix: str, statistic: str | None, family: str,
                      contrast_stat: str) -> ScoredCandidates:
    pairs = _pair_rows(reference_aggregate, alert_aggregate)
    values = context.runtime_values
    min_support = int(values["minimum_support"])
    resamples = int(values["permutation_resamples"])
    seed = int(values["seed"])
    rel_floor = float(values["relative_scale_floor"])
    abs_floor = float(values["absolute_scale_floor"])
    p_floor = float(values["permutation_p_score_floor"])
    tail_quantile = float(values["tail_quantile"])
    min_effect = float(values["minimum_normalized_effect"])
    max_p = float(values["maximum_change_p_value"])
    rows_by_entity: dict[tuple[str, str], list[tuple[tuple[str, str, str, str], dict[str, AggregateRow]]]] = defaultdict(list)
    for key, pair in pairs.items():
        rows_by_entity[(key[0], key[1])].append((key, pair))
    results = []
    for candidate in candidates.candidates:
        evidence = []
        scores = []
        entity_rows = rows_by_entity.get((candidate.entity.role, candidate.entity.canonical_id), ())
        for (role, entity_id, metric_key, row_stat), pair in entity_rows:
            if suffix not in metric_key:
                continue
            if statistic is not None and row_stat != statistic:
                continue
            if statistic is None and row_stat != "per_trace_elapsed_value":
                continue
            before, after = pair.get("reference"), pair.get("alert")
            if before is None or after is None or min(before.support, after.support) < min_support:
                continue
            before_values = before.details.get("per_trace_values", ())
            after_values = after.details.get("per_trace_values", ())
            contrast_kind = ("quantile" if metric_key.endswith("|elapsed_tail") else
                             "mean" if metric_key.endswith("|elapsed_mean") else contrast_stat)
            test = permutation_contrast(before_values, after_values,
                seed=stable_seed(seed, entity_id, metric_key, row_stat), resamples=resamples,
                statistic=contrast_kind, minimum_blocks=min_support,
                relative_scale_floor=rel_floor, absolute_scale_floor=abs_floor,
                quantile=tail_quantile)
            if test is None:
                continue
            degradation = test.raw_delta > 0
            eligible = (degradation and abs(test.normalized_effect) >= min_effect
                        and test.p_value <= max_p)
            score = contrast_score(test, p_floor=p_floor) if eligible else 0.0
            scores.append(score)
            evidence.append(Evidence("trace/trace_span.csv", after.window,
                f"{row_stat}_permutation_contrast", test.raw_delta, (candidate.entity,),
                min(test.baseline_support, test.alert_support),
                {"trace_metric": metric_key, "normalized_effect": test.normalized_effect,
                 "p_value": test.p_value, "baseline_center": test.baseline_center,
                 "alert_center": test.alert_center, "independent_unit": "traceId",
                 "ranking_eligible": eligible,
                 "direction": "higher_failure_or_latency_is_worse",
                 "minimum_normalized_effect": min_effect,
                 "maximum_change_p_value": max_p,
                 "relative_scale_floor": rel_floor, "absolute_scale_floor": abs_floor,
                 "permutation_p_score_floor": p_floor,
                 "tail_quantile": tail_quantile if contrast_kind == "quantile" else None,
                 "summary_statistic": ("mean across independent trace elapsed values" if contrast_kind == "mean"
                                       else "quantile across independent trace summaries" if contrast_kind == "quantile"
                                       else "median across independent trace summaries"),
                 "normalization_scale": ("reference sample standard deviation for mean contrast"
                                         if contrast_kind == "mean" else "reference MAD with declared floors"),
                 "duration_units": "unresolved absolute units; comparison stays within elapsedTime"}))
        results.append(ScoredCandidate(candidate.entity, max(scores, default=0.0), None,
                                       tuple(evidence), analysis_family=family))
    ready = any(any("permutation_contrast" in item.statistic for item in score.evidence)
                for score in results)
    return ScoredCandidates(tuple(results), scorer=f"traces.{family}",
                            status="ready" if ready else "abstained",
                            status_details={} if ready else {"reason": "no_trace_series_met_support_gates"})


def score_dependency(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    telemetry: Telemetry = inputs["data"]
    candidates: CandidateSet = inputs["candidates"]
    weighted_by_window: dict[tuple[str, EntityRef, str], dict[str, float]] = defaultdict(dict)
    propagation_weight = float(context.runtime_values["trace_propagation_weight"])
    deployment_weight = float(context.runtime_values["deployment_host_weight"])
    details_by_key: dict[tuple[str, EntityRef, str], dict[str, Any]] = {}
    span_map = {(span.trace_id, span.span_id): span for span in telemetry.spans}
    all_units: dict[tuple[str, EntityRef, str], set[str]] = defaultdict(set)
    for span in telemetry.spans:
        window = _window_name(telemetry, span.start_ms_utc)
        if window is None or span.success_status != "valid":
            continue
        for entity in (span.executing_component, span.logical_service, span.database_target):
            if entity is not None:
                all_units[(window, entity, span.call_type)].add(span.trace_id)
        if span.executing_component is not None:
            component_info = telemetry.deployment_catalog.get(span.executing_component.identifier, {})
            host = component_info.get("host")
            if host:
                all_units[(window, EntityRef("component", str(host)), span.call_type)].add(span.trace_id)
    failed = {(span.trace_id, span.span_id) for span in telemetry.spans
              if span.success_status == "valid" and span.success is False}
    propagated: set[tuple[str, str]] = set()
    for trace_id, span_id in failed:
        current = span_map.get((trace_id, span_id))
        seen = set()
        while current and current.parent_id and current.parent_id not in seen:
            seen.add(current.parent_id)
            parent_key = (trace_id, current.parent_id)
            parent = span_map.get(parent_key)
            if parent is None or not _same_window(telemetry, current, parent):
                break
            propagated.add(parent_key)
            current = parent
    for trace_id, span_id in failed:
        span = span_map.get((trace_id, span_id))
        if span is None:
            continue
        window = _window_name(telemetry, span.start_ms_utc)
        if window is None:
            continue
        is_propagated = (trace_id, span_id) in propagated
        weight = propagation_weight if is_propagated else 1.0
        entities = tuple(entity for entity in (span.executing_component, span.logical_service, span.database_target)
                         if entity is not None)
        for entity in entities:
            key = (window, entity, span.call_type)
            existing = weighted_by_window[key].get(trace_id, 0.0)
            weighted_by_window[key][trace_id] = max(existing, weight)
            detail = details_by_key.setdefault(key, {"direct_failures": 0, "propagated_failures": 0,
                                                       "unresolved_parent_rows": 0})
            detail["propagated_failures" if is_propagated else "direct_failures"] += 1
        if span.executing_component is not None:
            component_info = telemetry.deployment_catalog.get(span.executing_component.identifier, {})
            host = component_info.get("host")
            if host:
                host_entity = EntityRef("component", str(host))
                key = (_window_name(telemetry, span.start_ms_utc), host_entity, span.call_type)
                existing = weighted_by_window[key].get(trace_id, 0.0)
                weighted_by_window[key][trace_id] = max(existing, weight * deployment_weight)
                detail = details_by_key.setdefault(key, {"direct_failures": 0, "propagated_failures": 0,
                                                          "unresolved_parent_rows": 0,
                                                          "deployment_propagated_failures": 0})
                detail["deployment_propagated_failures"] += 1
    for span in telemetry.spans:
        if span.parent_unresolved_in_clip:
            window = _window_name(telemetry, span.start_ms_utc)
            if window is None:
                continue
            for entity in (span.executing_component, span.logical_service, span.database_target):
                if entity is not None:
                    details_by_key.setdefault((window, entity, span.call_type),
                        {"direct_failures": 0, "propagated_failures": 0,
                         "unresolved_parent_rows": 0})["unresolved_parent_rows"] += 1
    pairs = {}
    for (window, entity, call_type), trace_ids in all_units.items():
        observed_weights = weighted_by_window.get((window, entity, call_type), {})
        pairs.setdefault((entity.role, entity.canonical_id, call_type), {})[window] = {
            trace_id: observed_weights.get(trace_id, 0.0) for trace_id in sorted(trace_ids)}
    output = []
    min_support = int(context.runtime_values["minimum_support"])
    rel_floor = float(context.runtime_values["relative_scale_floor"])
    abs_floor = float(context.runtime_values["absolute_scale_floor"])
    p_floor = float(context.runtime_values["permutation_p_score_floor"])
    min_effect = float(context.runtime_values["minimum_normalized_effect"])
    max_p = float(context.runtime_values["maximum_change_p_value"])
    for candidate in candidates.candidates:
        scores, evidence = [], []
        for (role, entity_id, call_type), by_window in pairs.items():
            if role != candidate.entity.role or entity_id != candidate.entity.canonical_id:
                continue
            before, after = by_window.get("reference", {}), by_window.get("alert", {})
            if len(before) < min_support or len(after) < min_support:
                continue
            test = permutation_contrast(tuple(before.values()), tuple(after.values()),
                seed=stable_seed(int(context.runtime_values["seed"]), entity_id, call_type, "dependency"),
                resamples=int(context.runtime_values["permutation_resamples"]),
                statistic="mean", minimum_blocks=min_support,
                relative_scale_floor=rel_floor, absolute_scale_floor=abs_floor)
            if test is None:
                continue
            degradation = test.raw_delta > 0
            eligible = (degradation and abs(test.normalized_effect) >= min_effect
                        and test.p_value <= max_p)
            scores.append(contrast_score(test, p_floor=p_floor) if eligible else 0.0)
            alert_details = details_by_key.get(("alert", candidate.entity, call_type), {})
            catalog_info = telemetry.deployment_catalog.get(candidate.entity.identifier, {})
            evidence.append(Evidence("trace/trace_span.csv", telemetry.alert_window,
                "direct_vs_propagated_failure_weight", test.raw_delta, (candidate.entity,),
                min(test.baseline_support, test.alert_support),
                {"call_type": call_type, "baseline_weighted_failures": sum(before.values()),
                 "alert_weighted_failures": sum(after.values()),
                 "direct_failures_alert": alert_details.get("direct_failures", 0),
                 "propagated_failures_alert": alert_details.get("propagated_failures", 0),
                 "deployment_propagated_failures_alert": alert_details.get("deployment_propagated_failures", 0),
                 "unresolved_parent_rows_alert": alert_details.get("unresolved_parent_rows", 0),
                 "deployment_type": catalog_info.get("deployment_type"),
                 "host": catalog_info.get("host"),
                 "container_group_links": catalog_info.get("container_links"),
                 "normalized_effect_scale_floors": {"relative": rel_floor, "absolute": abs_floor},
                 "permutation_p_score_floor": p_floor,
                 "ranking_eligible": eligible,
                 "direction": "higher_weighted_failure_is_worse",
                 "minimum_normalized_effect": min_effect,
                 "maximum_change_p_value": max_p,
                 "propagation_weight": propagation_weight,
                 "deployment_host_weight": deployment_weight,
                 "unresolved_edges_are_not_repaired": True,
                 "independent_unit": "traceId"}))
        output.append(ScoredCandidate(candidate.entity, max(scores, default=0.0), None,
                                      tuple(evidence), analysis_family="dependency"))
    ready = any(any(item.statistic == "direct_vs_propagated_failure_weight" for item in score.evidence)
                for score in output)
    return ScoredCandidates(tuple(output), scorer="dependencies.propagation",
                            status="ready" if ready else "abstained",
                            status_details={} if ready else {"reason": "no_failure_propagation_series_met_support"})


def score_conditional_flow(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    telemetry: Telemetry = inputs["data"]
    candidates: CandidateSet = inputs["candidates"]
    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    # Each response row is one root OSB trace. Predictors come from child spans
    # within that same trace and share the same unresolved elapsedTime field.
    roots: dict[str, dict[str, Any]] = {}
    root_services_by_trace: dict[str, set[str]] = defaultdict(set)
    features_by_trace: dict[str, dict[tuple[str, str], list[float]]] = defaultdict(lambda: defaultdict(list))
    for span in telemetry.spans:
        window = _window_name(telemetry, span.start_ms_utc)
        if window is None:
            continue
        value = _finite_number(span.elapsed_raw)
        if value is None:
            continue
        if span.call_type.casefold() == "osb" and span.parent_id is None and span.logical_service is not None:
            root_services_by_trace[span.trace_id].add(span.logical_service.canonical_id)
            roots.setdefault(span.trace_id, {"target": value, "window": window,
                                            "root_service": span.logical_service})
            continue
        if span.call_type.casefold() == "osb":
            continue
        for entity in (span.executing_component, span.logical_service, span.database_target):
            if entity is not None:
                features_by_trace[span.trace_id][(entity.role, entity.canonical_id)].append(value)
    usable = {trace_id: root for trace_id, root in roots.items()
              if trace_id in features_by_trace and any(features_by_trace[trace_id].values())}
    matched_root_service_ids = sorted({service_id for trace_id in usable
                                       for service_id in root_services_by_trace.get(trace_id, ())})
    mixed_service_traces = sorted(trace_id for trace_id in usable
                                  if len(root_services_by_trace.get(trace_id, ())) > 1)
    if len(matched_root_service_ids) != 1 or mixed_service_traces:
        return _unsupported_flow(candidates, telemetry, "ambiguous_root_kpi_service",
            {"matched_root_traces": len(usable),
             "root_service_ids": matched_root_service_ids,
             "root_service_count": len(matched_root_service_ids),
             "traces_with_multiple_root_services": len(mixed_service_traces),
             "required": "exactly one logical root OSB service across matched traces",
             "root_KPI_alignment": "flow response must match the single label-blind selected root service"})
    min_samples = int(context.runtime_values["flow_min_samples"])
    if len(usable) < min_samples:
        return _unsupported_flow(candidates, telemetry, "insufficient_root_trace_support",
                                 {"matched_root_traces": len(usable), "minimum": min_samples})
    candidate_keys = sorted({(candidate.entity.role, candidate.entity.canonical_id)
                             for candidate in candidates.candidates
                             if candidate.entity.role in {"component", "database", "service"}})
    coverage = {}
    for key in candidate_keys:
        counts = {name: sum(key in features_by_trace.get(trace_id, {})
                            for trace_id, root in usable.items() if root["window"] == name)
                  for name in ("reference", "alert")}
        if min(counts.values()) >= min_samples:
            coverage[key] = counts
    if not coverage:
        return _unsupported_flow(candidates, telemetry, "no_feature_has_reference_and_alert_support",
                                 {"candidate_features": len(candidate_keys), "minimum_per_window": min_samples})
    x = np.full((len(usable), len(coverage)), np.nan, dtype=np.float64)
    y = np.zeros(len(usable), dtype=np.float64)
    window_names = []
    trace_ids = sorted(usable)
    feature_keys = sorted(coverage)
    for row_index, trace_id in enumerate(trace_ids):
        root = usable[trace_id]
        y[row_index] = root["target"]
        window_names.append(root["window"])
        for column, key in enumerate(feature_keys):
            values = features_by_trace.get(trace_id, {}).get(key, ())
            if values:
                x[row_index, column] = float(statistics.median(values))
    # Median imputation is fitted on the combined, explicitly recorded two-window scope.
    fill = np.nanmedian(x, axis=0)
    fill = np.where(np.isfinite(fill), fill, 0.0)
    missing = ~np.isfinite(x)
    x[missing] = np.take(fill, np.nonzero(missing)[1])
    variable = np.std(x, axis=0) > 1e-12
    x = x[:, variable]
    missing = missing[:, variable]
    feature_keys = [key for key, keep in zip(feature_keys, variable) if keep]
    coverage = {key: value for key, value, keep in zip(list(coverage), list(coverage.values()), variable) if keep}
    if not feature_keys:
        return _unsupported_flow(candidates, telemetry, "no_variable_backend_features",
                                 {"matched_root_traces": len(y)})
    if len(feature_keys) > 1:
        correlations = np.corrcoef(x, rowvar=False)
        upper = correlations[np.triu_indices_from(correlations, k=1)]
        max_abs_correlation = float(np.nanmax(np.abs(upper))) if upper.size else 0.0
        if math.isfinite(max_abs_correlation) and max_abs_correlation >= 0.98:
            return _unsupported_flow(candidates, telemetry, "conditional_features_collinear",
                                     {"matched_root_traces": len(y),
                                      "max_abs_feature_correlation": max_abs_correlation,
                                      "fit_scope": "reference_plus_alert"})
    if not np.isfinite(x).all() or not np.isfinite(y).all() or float(np.std(y)) <= 1e-12:
        return _unsupported_flow(candidates, telemetry, "nonfinite_or_constant_response",
                                 {"matched_root_traces": len(y)})
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    alpha = float(context.runtime_values["flow_alpha"])
    model = Lasso(alpha=alpha, fit_intercept=True, max_iter=20_000, tol=1e-6,
                  selection="cyclic")
    model.fit(x_scaled, y)
    prediction = model.predict(x_scaled)
    fit_r2 = float(r2_score(y, prediction))
    required_r2 = float(context.runtime_values["flow_min_r2"])
    if not math.isfinite(fit_r2) or fit_r2 < required_r2:
        return _unsupported_flow(candidates, telemetry, "conditional_model_fit_below_threshold",
                                 {"matched_root_traces": len(y), "r2": fit_r2,
                                  "minimum_r2": required_r2, "alpha": alpha,
                                  "fit_scope": "reference_plus_alert"})
    beta_original = model.coef_ / np.where(scaler.scale_ == 0, 1.0, scaler.scale_)
    response_scale = max(float(stats.median_abs_deviation(y, scale="normal")),
                         abs(float(np.median(y))) * float(context.runtime_values["relative_scale_floor"]),
                         float(context.runtime_values["absolute_scale_floor"]))
    minimum_effect = float(context.runtime_values["minimum_normalized_effect"])
    contribution = {}
    for index, key in enumerate(feature_keys):
        ref_values = [x[i, index] for i, name in enumerate(window_names) if name == "reference"]
        alert_values = [x[i, index] for i, name in enumerate(window_names) if name == "alert"]
        contribution[key] = (float(beta_original[index]) *
                             (float(np.mean(alert_values)) - float(np.mean(ref_values))))
    root_response_delta = float(np.mean(y[np.asarray(window_names) == "alert"]) -
                                np.mean(y[np.asarray(window_names) == "reference"]))
    if root_response_delta <= 0:
        return _unsupported_flow(candidates, telemetry, "root_latency_did_not_degrade",
            {"root_elapsedTime_delta": root_response_delta,
             "required_direction": "alert root elapsedTime mean exceeds reference"})
    min_fraction = float(context.runtime_values["flow_min_contribution_fraction"])
    out = []
    weight = float(context.runtime_values["analysis_weights"].get("flow", 1.0))
    for candidate in candidates.candidates:
        key = (candidate.entity.role, candidate.entity.canonical_id)
        if key not in contribution:
            out.append(ScoredCandidate(candidate.entity, 0.0, None, (), analysis_family="flow"))
            continue
        value = contribution[key]
        normalized_effect = value / response_scale
        contribution_fraction = value / root_response_delta
        ranking_eligible = (value > 0 and abs(normalized_effect) >= minimum_effect and
                            contribution_fraction >= min_fraction)
        evidence = Evidence("trace/trace_span.csv", telemetry.alert_window,
            "conditional_elapsedTime_contribution", value, (candidate.entity,),
            min(coverage[key].values()),
            {"fit_r2": fit_r2, "alpha": alpha, "coefficient": float(beta_original[feature_keys.index(key)]),
             "normalized_effect": normalized_effect,
             "root_elapsedTime_delta": root_response_delta,
             "minimum_normalized_effect": minimum_effect,
             "contribution_fraction": contribution_fraction,
             "minimum_contribution_fraction": min_fraction,
             "ranking_eligible": ranking_eligible,
             "reference_feature_mean": float(np.mean([x[i, feature_keys.index(key)] for i, n in enumerate(window_names) if n == "reference"])),
             "alert_feature_mean": float(np.mean([x[i, feature_keys.index(key)] for i, n in enumerate(window_names) if n == "alert"])),
             "matched_root_traces": len(y), "feature_coverage_by_window": coverage[key],
             "median_imputed_values": int(missing[:, feature_keys.index(key)].sum()),
             "fit_scope": "reference_plus_alert", "target_excluded_from_predictors": True,
             "feature_and_target": "elapsedTime within trace roles; absolute unit remains unresolved",
             "not_client_session_data": True})
        out.append(ScoredCandidate(candidate.entity,
                                   abs(normalized_effect) * weight if ranking_eligible else 0.0,
                                   None, (evidence,),
                                   analysis_family="flow"))
    return ScoredCandidates(tuple(out), scorer="flow.conditional_contribution")


def _unsupported_flow(candidates: CandidateSet, telemetry: Telemetry, status: str,
                      details: Mapping[str, Any]) -> ScoredCandidates:
    evidence = Evidence("trace/trace_span.csv", telemetry.alert_window,
        "conditional_flow_abstention", status, (), 0,
        {**dict(details), "prediction": "unsupported", "alert_or_gold_not_used": True})
    return ScoredCandidates(tuple(ScoredCandidate(candidate.entity, 0.0, None, (evidence,),
        analysis_family="flow") for candidate in candidates.candidates),
        scorer="flow.conditional_contribution", status="abstained",
        status_details={"reason": status, **dict(details)})


def combine_scores(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> RankedAnswer:
    branches: tuple[ScoredCandidates, ...] = inputs["scores"]
    weights = context.runtime_values["analysis_weights"]
    family_scores: dict[str, dict[str, float]] = defaultdict(dict)
    family_weights: dict[str, float] = {}
    candidates_by_entity: dict[str, list[ScoredCandidate]] = defaultdict(list)
    for branch in branches:
        if branch.status == "abstained":
            continue
        family = branch.candidates[0].analysis_family if branch.candidates else branch.scorer
        family = family or branch.scorer
        family_weight = float(weights.get(family, 1.0))
        if family_weight <= 0:
            continue
        family_weights[family] = family_weight
        # Canonical-ID duplicates across roles are merged before family ranking,
        # so one component cannot consume multiple rank positions or votes.
        local: dict[str, ScoredCandidate] = {}
        for candidate in branch.candidates:
            key = candidate.entity.canonical_id
            prior = local.get(key)
            if prior is None:
                local[key] = candidate
            else:
                evidence_by_hash = {_evidence_key(item): item for item in (*prior.evidence, *candidate.evidence)}
                best = max((prior, candidate), key=lambda item: (item.score, item.entity.role))
                categories = {item.category for item in (prior, candidate) if item.category is not None}
                category = next(iter(categories)) if len(categories) == 1 else None
                local[key] = ScoredCandidate(best.entity, max(prior.score, candidate.score), category,
                    tuple(evidence_by_hash[k] for k in sorted(evidence_by_hash)),
                    analysis_family=family)
        for key, candidate in local.items():
            candidates_by_entity[key].append(candidate)
            family_scores[family][key] = max(family_scores[family].get(key, 0.0), candidate.score)

    # Normalize once per family, irrespective of how many variants that family
    # contributed. Zero/no-evidence candidates stay at zero and are not leads.
    # Divide by the strongest positive family score, preserving within-family
    # strength ratios while removing that family's raw units.
    normalized_by_entity: dict[str, dict[str, float]] = defaultdict(dict)
    for family, raw_scores in family_scores.items():
        maximum = max(raw_scores.values(), default=0.0)
        if maximum <= 0:
            continue
        for key, score in raw_scores.items():
            if score > 0:
                normalized_by_entity[key][family] = score / maximum
    common_weight = sum(family_weights.values())
    merged = []
    for key, candidates in candidates_by_entity.items():
        family_contributions = normalized_by_entity.get(key, {})
        normalized_score = (sum(value * family_weights[family] for family, value in family_contributions.items()) /
                            common_weight) if common_weight else 0.0
        if normalized_score <= 0:
            continue
        best = max(candidates, key=lambda item: (item.score, item.entity.role))
        evidence_by_hash = {}
        categories = {item.category for item in candidates if item.category is not None}
        for item in candidates:
            for evidence in item.evidence:
                evidence_by_hash[_evidence_key(evidence)] = evidence
        category = next(iter(categories)) if len(categories) == 1 else None
        merged.append(ScoredCandidate(best.entity, float(normalized_score), category,
            tuple(evidence_by_hash[key] for key in sorted(evidence_by_hash)), analysis_family="combined"))
    merged.sort(key=lambda item: (-item.score, item.entity.canonical_id, item.entity.role))
    limit = int(params["limit"])
    return RankedAnswer(tuple(merged[:limit]))


def metric_seed_dag() -> dict[str, Any]:
    return _seed_document(["metrics"])


def trace_error_seed_dag() -> dict[str, Any]:
    return _seed_document(["traces"], scorer="error")


def trace_latency_seed_dag() -> dict[str, Any]:
    return _seed_document(["traces"], scorer="latency")


def flow_seed_dag() -> dict[str, Any]:
    return _seed_document([], scorer="flow")


def dependency_seed_dag() -> dict[str, Any]:
    return _seed_document([], scorer="dependency")


def combined_seed_dag() -> dict[str, Any]:
    return _seed_document(["metrics", "traces"], scorer="combined")


def _seed_document(aggregates: list[str], scorer: str = "metric") -> dict[str, Any]:
    nodes = []
    def node(identifier, operator, inputs=None, params=None, bindings=None):
        nodes.append({"id": identifier, "operator": operator, "version": "1",
                      "inputs": inputs or {}, "params": params or {}, "bindings": bindings or {}})
    node("data", "case.telemetry")
    aggregate_nodes = {}
    aggregate_refs = []
    for name in aggregates:
        operator = "metrics.aggregate" if name == "metrics" else "traces.aggregate"
        aggregate_nodes[name] = {}
        for window in ("reference", "alert"):
            node_id = f"{name}_{window}"
            node(node_id, operator, {"data": "data"}, {"window": window})
            aggregate_nodes[name][window] = node_id
            aggregate_refs.append(node_id)
    node("candidates", "candidates.generate",
         {"data": "data", "aggregates": aggregate_refs})
    score_ids = []
    if scorer in {"metric", "combined"}:
        node("metric_score", "metrics.contrast", {
            "reference": aggregate_nodes["metrics"]["reference"],
            "alert": aggregate_nodes["metrics"]["alert"],
            "candidates": "candidates"})
        score_ids.append("metric_score")
    if scorer in {"error", "combined"}:
        node("error_score", "traces.error_contrast", {
            "reference": aggregate_nodes["traces"]["reference"],
            "alert": aggregate_nodes["traces"]["alert"],
            "candidates": "candidates"})
        score_ids.append("error_score")
    if scorer in {"latency", "combined"}:
        node("latency_score", "traces.latency_contrast", {
            "reference": aggregate_nodes["traces"]["reference"],
            "alert": aggregate_nodes["traces"]["alert"],
            "candidates": "candidates"})
        score_ids.append("latency_score")
    if scorer in {"flow", "combined"}:
        node("flow_score", "flow.conditional_contribution", {"data": "data", "candidates": "candidates"})
        score_ids.append("flow_score")
    if scorer in {"dependency", "combined"}:
        node("dependency_score", "dependencies.propagation", {"data": "data", "candidates": "candidates"})
        score_ids.append("dependency_score")
    node("rank", "answers.combine", {"scores": score_ids})
    return {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "rank"}}


def _pair_rows(reference: Aggregate, alert: Aggregate) -> dict[tuple[str, str, str, str], dict[str, AggregateRow]]:
    if reference.window.name != "reference" or alert.window.name != "alert":
        raise ValueError("comparison scorers require one reference Aggregate and one alert Aggregate")
    result: dict[tuple[str, str, str, str], dict[str, AggregateRow]] = defaultdict(dict)
    for row in reference.rows:
        result[(row.entity.role, row.entity.canonical_id, row.metric_key, row.statistic)]["reference"] = row
    for row in alert.rows:
        result[(row.entity.role, row.entity.canonical_id, row.metric_key, row.statistic)]["alert"] = row
    return result


def _metric_app_requests(reference: Aggregate, alert: Aggregate,
                         entity: EntityRef) -> tuple[float | None, float | None]:
    values = {"reference": None, "alert": None}
    for row in (*reference.rows, *alert.rows):
        if (row.entity.canonical_id == entity.canonical_id and
                row.metric_key.startswith("metric/metric_app.csv|num|")
                and row.statistic == "median"):
            values[row.window.name] = row.value
    return values["reference"], values["alert"]


def _window_name(telemetry: Telemetry, timestamp: int) -> str | None:
    if telemetry.reference_window.contains(timestamp):
        return "reference"
    if telemetry.alert_window.contains(timestamp):
        return "alert"
    return None


def _same_window(telemetry: Telemetry, first: Span, second: Span) -> bool:
    return _window_name(telemetry, first.start_ms_utc) == _window_name(telemetry, second.start_ms_utc)


def _finite_number(value: str) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _evidence_key(evidence: Evidence) -> str:
    from e4.dsl.cache import digest_value
    return digest_value(evidence)
