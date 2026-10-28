"""Bank-only operators that preserve Bank source units and identities."""

from __future__ import annotations

import json
import hashlib
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping

from e4.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet, EntityRef,
                          Evidence, RankedAnswer, ScoredCandidate, ScoredCandidates,
                          Telemetry)
from e4.dsl.executor import NodeContext, OperatorResult
from e4.operators.numerics import block_reduce, contrast_score, percentile, permutation_contrast, stable_seed
from e4.dsl.registry import OperatorRegistry, OperatorSpec, PortSpec
from e4.dsl.runtime import ParameterSpec
from e4.paths import ROOT


METRIC_POLICY_PATH = ROOT / "config/bank/metric_kind_policy.json"
KPI_POLICY_PATH = ROOT / "config/bank/kpi_policy.json"
_POLICY_VERSION = "e4-bank-metric-kind-policy/1"
_SUPPORTED_RANKING_KINDS = {"gauge", "rate", "event_count"}


def _policy_version(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return f"1+{digest}"


def bank_registry(*, root: Path = ROOT) -> OperatorRegistry:
    metric_policy_path = root / "config/bank/metric_kind_policy.json"
    kpi_policy_path = root / "config/bank/kpi_policy.json"
    metric_version = _policy_version(metric_policy_path)
    kpi_version = _policy_version(kpi_policy_path)
    registry = OperatorRegistry()
    registry.register(OperatorSpec("case.telemetry", "1", (), "Telemetry",
        cacheable=False, external_input=True,
        description="Executor-injected hash-pinned public Bank telemetry."))
    registry.register(OperatorSpec("bank.metrics.aggregate", metric_version,
        (PortSpec("data", "Telemetry"),), "Aggregate",
        {"window": ParameterSpec("window", "string", required=True,
                                  choices=("reference", "alert"))},
        family="bank_aggregation",
        implementation=lambda i, p, c: aggregate_bank_metrics(i, p, c, policy_path=metric_policy_path),
        description="Native-field Bank metric and derived log-count aggregation; unreviewed KPI kinds remain descriptive only."))
    registry.register(OperatorSpec("bank.traces.aggregate", metric_version,
        (PortSpec("data", "Telemetry"),), "Aggregate",
        {"window": ParameterSpec("window", "string", required=True,
                                  choices=("reference", "alert"))},
        family="bank_aggregation", implementation=aggregate_bank_trace_durations,
        description="Per-trace native duration summaries with undeclared units; no Telecom root/call-type assumptions."))
    registry.register(OperatorSpec("bank.candidates.generate", "1",
        (PortSpec("data", "Telemetry"), PortSpec("aggregates", "Aggregate", required=False,
                                                   variadic=True, minimum_items=0)),
        "CandidateSet", family="bank_candidate", implementation=generate_bank_candidates,
        description="Exact observed Bank component IDs only; no service aliases or topology edges are inferred."))
    registry.register(OperatorSpec("bank.metrics.contrast", metric_version,
        (PortSpec("reference", "Aggregate"), PortSpec("alert", "Aggregate"),
         PortSpec("candidates", "CandidateSet")), "ScoredCandidates",
        family="bank_metric", implementation=score_bank_metrics,
        description="Paired two-sided native-field contrasts only for explicitly allowlisted metric kinds."))
    registry.register(OperatorSpec("bank.traces.latency_contrast", metric_version,
        (PortSpec("reference", "Aggregate"), PortSpec("alert", "Aggregate"),
         PortSpec("candidates", "CandidateSet")), "ScoredCandidates",
        family="bank_trace_latency", implementation=score_bank_trace_duration,
        description="Paired per-trace duration change on its native field; duration units remain undeclared."))
    registry.register(OperatorSpec("bank.logs.volume_contrast", metric_version,
        (PortSpec("reference", "Aggregate"), PortSpec("alert", "Aggregate"),
         PortSpec("candidates", "CandidateSet")), "ScoredCandidates",
        family="bank_log_volume", implementation=score_bank_log_volume,
        description="Per-minute source-tagged log row-volume change; not an error rate or GC duration."))
    registry.register(OperatorSpec("bank.answers.combine", kpi_version,
        (PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),),
        "RankedAnswer", {"limit": ParameterSpec("limit", "positive_integer", runtime_field="lead_count")},
        family="bank_ranking",
        implementation=lambda i, p, c: combine_bank_scores(i, p, c, policy_path=kpi_policy_path),
        description="Equal configured family weights after within-family maximum normalization; no category prediction."))
    return registry


def _policy(path: Path = METRIC_POLICY_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if value.get("schema_version") != _POLICY_VERSION:
        raise ValueError("unsupported Bank metric kind policy")
    return value


def _dev_policy(path: Path = KPI_POLICY_PATH) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if value.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise ValueError("unsupported Bank development KPI policy")
    return value


def _policy_entry(point: Any, policy: Mapping[str, Any]) -> dict[str, Any]:
    semantics = policy.get("source_semantics", {})
    if point.table == "bank/metric_app.csv":
        return dict(semantics.get("metric_app", {}).get("fields", {}).get(point.metric_name, {}))
    if point.table == "bank/metric_container.csv":
        return dict(semantics.get("metric_container", {}).get("ranking_allowlist", {}).get(
            point.metric_name, {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False}))
    if point.table == "bank/log/log_service.csv" and point.metric_name == "log_volume_count":
        value = semantics.get("log_service", {})
        return {"kind": value.get("derived_kind", "unknown"),
                "unit": value.get("derived_unit", "undeclared"),
                "ranking_eligible": value.get("ranking_eligible", False),
                "provenance": "derived by counting all hash-pinned log rows per component/log_name/minute"}
    return {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False}


def aggregate_bank_metrics(inputs: Mapping[str, Any], params: Mapping[str, Any],
                           context: NodeContext, *,
                           policy_path: Path = METRIC_POLICY_PATH) -> OperatorResult:
    telemetry: Telemetry = inputs["data"]
    window_name = params["window"]
    window = telemetry.reference_window if window_name == "reference" else telemetry.alert_window
    policy = _policy(policy_path)
    grouped: dict[tuple[EntityRef, str, str, str, str], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    source_by_series: dict[tuple[EntityRef, str, str, str, str], set[str]] = defaultdict(set)
    policy_by_series: dict[tuple[EntityRef, str, str, str, str], dict[str, Any]] = {}
    point_by_series: dict[tuple[EntityRef, str, str, str, str], Any] = {}
    raw_count: dict[tuple[EntityRef, str, str, str, str, int], int] = defaultdict(int)
    for point in telemetry.metrics:
        if not window.contains(point.timestamp_ms_utc):
            continue
        key = (point.entity, point.table, point.metric_name,
               point.bomc_id or "", point.item_id or "")
        source_by_series[key].add(point.table)
        policy_by_series[key] = _policy_entry(point, policy)
        point_by_series[key] = point
        identity = (*key, point.timestamp_ms_utc)
        raw_count[identity] += 1
        if point.value is not None and math.isfinite(point.value):
            grouped[key][point.timestamp_ms_utc].append(float(point.value))
    rows = []
    for (entity, table, metric_name, bomc_id, item_id), buckets in sorted(grouped.items(),
            key=lambda pair: (pair[0][0].role, pair[0][0].canonical_id,
                              pair[0][1], pair[0][2], pair[0][3], pair[0][4])):
        series_key = (entity, table, metric_name, bomc_id, item_id)
        buckets_median = [(timestamp, float(median(values)))
                          for timestamp, values in sorted(buckets.items()) if values]
        if not buckets_median:
            continue
        point = point_by_series[series_key]
        classification = policy_by_series[series_key]
        metric_kind = classification.get("kind", "unknown")
        details = {
            "metric_kind": metric_kind,
            "kind_provenance": classification.get("provenance", "not present in explicit Bank metric allowlist"),
            "unit": classification.get("unit", "undeclared"),
            "ranking_eligible": bool(classification.get("ranking_eligible", False)) and
                                metric_kind in _SUPPORTED_RANKING_KINDS,
            "source_field": metric_name,
            "source_table": point.table,
            "item_id": item_id,
            "bucket_samples": tuple(buckets_median),
            "distinct_time_buckets": len(buckets_median),
            "raw_rows_by_timestamp": tuple((timestamp, raw_count[(*series_key, timestamp)])
                                            for timestamp, _ in buckets_median),
            "comparison_policy": "same source field only; no conversion across Bank metrics",
        }
        metric_key = "|".join((table, metric_name, bomc_id, item_id))
        source_tables = tuple(sorted(source_by_series[series_key]))
        for statistic, quantile in (("median", 0.5), ("tail_quantile", float(
                context.runtime_values.get("tail_quantile", 0.9)))):
            selected = [value for _, value in buckets_median]
            value = (float(median(selected)) if statistic == "median"
                     else percentile(selected, quantile))
            if value is None:
                continue
            rows.append(AggregateRow(entity, metric_key, window, statistic, float(value),
                                     len(selected), source_tables, (), details))
    return OperatorResult(Aggregate(window, tuple(rows), tuple(sorted({
        table for point in telemetry.metrics if window.contains(point.timestamp_ms_utc)
        for table in (point.table,)})),
        {"source": "Bank telemetry", "policy_schema": policy["schema_version"],
         "window": window_name, "metric_semantics": "explicit allowlist; unreviewed fields excluded"}))


def aggregate_bank_trace_durations(inputs: Mapping[str, Any], params: Mapping[str, Any],
                                  context: NodeContext) -> OperatorResult:
    telemetry: Telemetry = inputs["data"]
    window_name = params["window"]
    window = telemetry.reference_window if window_name == "reference" else telemetry.alert_window
    per_trace: dict[tuple[EntityRef, str], list[float]] = defaultdict(list)
    source = "bank/trace_span.csv"
    for span in telemetry.spans:
        if (not window.contains(span.start_ms_utc) or span.executing_component is None or
                not span.trace_id or not span.span_id):
            continue
        try:
            value = float(span.elapsed_raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value >= 0:
            per_trace[(span.executing_component, span.trace_id)].append(value)
    by_entity: dict[EntityRef, dict[str, tuple[float, float, float]]] = defaultdict(dict)
    for (entity, trace_id), samples in per_trace.items():
        if trace_id:
            by_entity[entity][trace_id] = (
                float(median(samples)), float(mean(samples)),
                float(percentile(samples, float(context.runtime_values.get("tail_quantile", 0.9)))))
    rows = []
    for entity, trace_values in sorted(by_entity.items(), key=lambda item: (item[0].role, item[0].canonical_id)):
        for summary_index, statistic in enumerate(("per_trace_median", "per_trace_mean",
                                                   "per_trace_tail_quantile")):
            trace_samples = tuple(sorted((trace_id, values[summary_index])
                                         for trace_id, values in trace_values.items()))
            independent_values = [value for _, value in trace_samples]
            if not independent_values:
                continue
            rows.append(AggregateRow(
                entity=entity, metric_key=f"{source}|duration||duration", window=window,
                statistic=statistic, value=float(median(independent_values)),
                support=len(independent_values), source_tables=(source,),
                details={"trace_values": trace_samples, "independent_unit": "trace_id",
                         "duration_unit": "undeclared", "comparison_policy": "within duration field only",
                         "trace_summary": statistic,
                         "tail_quantile": float(context.runtime_values.get("tail_quantile", 0.9)),
                         "success_status": "not available in Bank trace schema"}))
    return OperatorResult(Aggregate(window, tuple(rows), (source,),
        {"window": window_name, "duration_unit": "undeclared", "success_field": None,
         "call_type_field": None, "aggregation": "median span duration within trace, then paired trace values"}))


def generate_bank_candidates(inputs: Mapping[str, Any], params: Mapping[str, Any],
                              context: NodeContext) -> CandidateSet:
    telemetry: Telemetry = inputs["data"]
    observed = set()
    for point in telemetry.metrics:
        if point.entity.role == "component":
            observed.add(point.entity.identifier)
    for span in telemetry.spans:
        if span.executing_component is not None:
            observed.add(span.executing_component.identifier)
    candidates = tuple(Candidate(EntityRef("component", identifier),
                                 "exact Bank cmdb_id observed in public telemetry; no alias inference")
                       for identifier in sorted(observed, key=lambda value: (value.casefold(), value)))
    return CandidateSet(candidates, source="exact observed Bank component identifiers")


def _runtime_values(context: NodeContext) -> dict[str, Any]:
    values = context.runtime_values
    return {"minimum_support": int(values.get("minimum_support", 4)),
            "minimum_independent_blocks": int(values.get("minimum_independent_blocks", 4)),
            "block_minutes": int(values.get("block_minutes", 5)),
            "permutation_resamples": int(values.get("permutation_resamples", 999)),
            "relative_scale_floor": float(values.get("relative_scale_floor", 0.05)),
            "absolute_scale_floor": float(values.get("absolute_scale_floor", 1e-9)),
            "permutation_p_score_floor": float(values.get("permutation_p_score_floor", 1e-3)),
            "minimum_normalized_effect": float(values.get("minimum_normalized_effect", 0.5)),
            "maximum_change_p_value": float(values.get("maximum_change_p_value", 0.1)),
            "tail_quantile": float(values.get("tail_quantile", 0.9)),
            "seed": int(values.get("seed", 42))}


def _pair_rows(reference: Aggregate, alert: Aggregate, statistic: str) -> dict[tuple, dict[str, AggregateRow]]:
    if (reference.window.name != "reference" or alert.window.name != "alert" or
            reference.window.end_ms_utc != alert.window.start_ms_utc):
        raise ValueError("Bank scorer requires adjacent reference and alert aggregates")
    result: dict[tuple, dict[str, AggregateRow]] = defaultdict(dict)
    for envelope in (reference, alert):
        for row in envelope.rows:
            if row.statistic != statistic:
                continue
            key = (row.entity.role, row.entity.canonical_id, row.metric_key, statistic)
            result[key][envelope.window.name] = row
    return result


def score_bank_metrics(inputs: Mapping[str, Any], params: Mapping[str, Any],
                       context: NodeContext) -> ScoredCandidates:
    before: Aggregate = inputs["reference"]
    after: Aggregate = inputs["alert"]
    candidates: CandidateSet = inputs["candidates"]
    runtime = _runtime_values(context)
    pairs_by_statistic = {statistic: _pair_rows(before, after, statistic)
                          for statistic in ("median", "tail_quantile")}
    by_entity: dict[tuple[str, str], list[tuple[dict[str, AggregateRow], str]]] = defaultdict(list)
    for statistic, pairs in pairs_by_statistic.items():
        for key, pair in pairs.items():
            by_entity[(key[0], key[1])].append((pair, statistic))
    output = []
    for candidate in candidates.candidates:
        scores, evidence = [], []
        for pair, statistic in by_entity.get((candidate.entity.role, candidate.entity.canonical_id), []):
            reference, alert = pair.get("reference"), pair.get("alert")
            if reference is None or alert is None:
                continue
            details = reference.details
            metric_kind = details.get("metric_kind", "unknown")
            eligible_kind = (metric_kind in {"gauge", "rate"} and
                             details.get("ranking_eligible") is True)
            if not eligible_kind:
                evidence.append(Evidence(reference.source_tables[0], alert.window,
                    "unclassified_metric_excluded", alert.value, (candidate.entity,),
                    min(reference.support, alert.support),
                    {"metric_key": reference.metric_key, "metric_kind": metric_kind,
                     "unit": details.get("unit", "undeclared"),
                     "policy_provenance": details.get("kind_provenance"),
                     "ranking_eligible": False}))
                continue
            if min(reference.support, alert.support) < runtime["minimum_support"]:
                evidence.append(Evidence(reference.source_tables[0], alert.window,
                    "low_timestamp_support_excluded", alert.value, (candidate.entity,),
                    min(reference.support, alert.support),
                    {"metric_key": reference.metric_key, "minimum_support": runtime["minimum_support"],
                     "ranking_eligible": False}))
                continue
            block_quantile = 0.5 if statistic == "median" else runtime["tail_quantile"]
            samples_before = block_reduce(reference.details.get("bucket_samples", ()),
                reference.window.start_ms_utc, runtime["block_minutes"], block_quantile)
            samples_after = block_reduce(alert.details.get("bucket_samples", ()),
                alert.window.start_ms_utc, runtime["block_minutes"], block_quantile)
            test = permutation_contrast(samples_before, samples_after,
                seed=stable_seed(runtime["seed"], candidate.entity.identifier, reference.metric_key),
                resamples=runtime["permutation_resamples"],
                statistic="median" if statistic == "median" else "quantile",
                minimum_blocks=runtime["minimum_independent_blocks"],
                relative_scale_floor=runtime["relative_scale_floor"],
                absolute_scale_floor=runtime["absolute_scale_floor"],
                quantile=runtime["tail_quantile"])
            if test is None:
                continue
            direction = details.get("direction", "two_sided")
            direction_ok = (direction == "two_sided" or
                            (direction == "higher_is_worse" and test.raw_delta > 0) or
                            (direction == "lower_is_worse" and test.raw_delta < 0))
            eligible = (direction_ok and
                        abs(test.normalized_effect) >= runtime["minimum_normalized_effect"] and
                        test.p_value <= runtime["maximum_change_p_value"])
            score = contrast_score(test, p_floor=runtime["permutation_p_score_floor"]) if eligible else 0.0
            scores.append(score)
            evidence.append(Evidence(reference.source_tables[0], alert.window,
                f"bank_native_metric_{statistic}_contrast", test.raw_delta, (candidate.entity,),
                min(test.baseline_support, test.alert_support),
                {"metric_key": reference.metric_key, "metric_kind": metric_kind,
                 "unit": details.get("unit", "undeclared"),
                 "normalized_effect": test.normalized_effect, "p_value": test.p_value,
                 "direction": direction, "comparison_scope": "same field across windows only",
                 "summary_statistic": statistic, "block_quantile": block_quantile,
                 "minimum_timestamp_support": runtime["minimum_support"],
                 "minimum_independent_blocks": runtime["minimum_independent_blocks"],
                 "minimum_normalized_effect": runtime["minimum_normalized_effect"],
                 "maximum_change_p_value": runtime["maximum_change_p_value"],
                 "ranking_eligible": eligible}))
        output.append(ScoredCandidate(candidate.entity, max(scores, default=0.0), None,
                                      tuple(evidence), analysis_family="bank_metric"))
    ready = any(any(item.details.get("ranking_eligible") is True for item in row.evidence)
                for row in output)
    return ScoredCandidates(tuple(output), scorer="bank.metrics.contrast",
                            status="ready" if ready else "abstained",
                            status_details={} if ready else {"reason": "no explicitly allowlisted metric series met support gates"})


def score_bank_trace_duration(inputs: Mapping[str, Any], params: Mapping[str, Any],
                              context: NodeContext) -> ScoredCandidates:
    reference: Aggregate = inputs["reference"]
    alert: Aggregate = inputs["alert"]
    if (reference.window.name != "reference" or alert.window.name != "alert" or
            reference.window.end_ms_utc != alert.window.start_ms_utc):
        raise ValueError("Bank trace scorer requires adjacent reference and alert aggregates")
    candidates: CandidateSet = inputs["candidates"]
    runtime = _runtime_values(context)
    paired = {statistic: (
        {row.entity.canonical_id: row for row in reference.rows if row.statistic == statistic},
        {row.entity.canonical_id: row for row in alert.rows if row.statistic == statistic})
        for statistic in ("per_trace_median", "per_trace_mean", "per_trace_tail_quantile")}
    output = []
    for candidate in candidates.candidates:
        scores, evidences = [], []
        for summary_stat, (before, after) in paired.items():
            ref = before.get(candidate.entity.canonical_id)
            alt = after.get(candidate.entity.canonical_id)
            if ref is None or alt is None:
                continue
            if min(ref.support, alt.support) < runtime["minimum_support"]:
                continue
            ref_values = [value for _, value in ref.details.get("trace_values", ())]
            alert_values = [value for _, value in alt.details.get("trace_values", ())]
            contrast_stat = ("mean" if summary_stat == "per_trace_mean" else
                             "quantile" if summary_stat == "per_trace_tail_quantile" else "median")
            test = permutation_contrast(ref_values, alert_values,
                seed=stable_seed(runtime["seed"], candidate.entity.identifier, summary_stat),
                resamples=runtime["permutation_resamples"], statistic=contrast_stat,
                minimum_blocks=runtime["minimum_support"],
                relative_scale_floor=runtime["relative_scale_floor"],
                absolute_scale_floor=runtime["absolute_scale_floor"],
                quantile=runtime["tail_quantile"])
            if test is None:
                continue
            # Higher native duration is treated as degradation; an improvement
            # remains evidence but cannot rank a component as a fault.
            eligible = (test.raw_delta > 0 and
                        test.normalized_effect >= runtime["minimum_normalized_effect"] and
                        test.p_value <= runtime["maximum_change_p_value"])
            scores.append(contrast_score(test, p_floor=runtime["permutation_p_score_floor"])
                          if eligible else 0.0)
            evidences.append(Evidence("bank/trace_span.csv", alert.window,
                f"native_duration_{summary_stat}_contrast", test.raw_delta,
                (candidate.entity,), min(test.baseline_support, test.alert_support),
                {"normalized_effect": test.normalized_effect, "p_value": test.p_value,
                 "direction": "higher duration only for degradation ranking",
                 "duration_unit": "undeclared", "independent_unit": "trace_id",
                 "success_semantics": "not available",
                 "comparison_scope": "Bank duration field only; no cross-field conversion",
                 "minimum_trace_support": runtime["minimum_support"],
                 "minimum_normalized_effect": runtime["minimum_normalized_effect"],
                 "maximum_change_p_value": runtime["maximum_change_p_value"],
                 "ranking_eligible": eligible}))
        output.append(ScoredCandidate(candidate.entity, max(scores, default=0.0), None,
                                      tuple(evidences), analysis_family="bank_trace_latency"))
    ready = any(any(evidence.details.get("ranking_eligible") is True for evidence in row.evidence)
                for row in output)
    return ScoredCandidates(tuple(output), scorer="bank.traces.latency_contrast",
                            status="ready" if ready else "abstained",
                            status_details={} if ready else {"reason": "no paired trace-duration support"})


def score_bank_log_volume(inputs: Mapping[str, Any], params: Mapping[str, Any],
                          context: NodeContext) -> ScoredCandidates:
    reference: Aggregate = inputs["reference"]
    alert: Aggregate = inputs["alert"]
    candidates: CandidateSet = inputs["candidates"]
    runtime = _runtime_values(context)
    pairs_by_stat = {statistic: _pair_rows(reference, alert, statistic)
                     for statistic in ("median", "tail_quantile")}
    by_entity: dict[str, list[tuple[AggregateRow, AggregateRow, str]]] = defaultdict(list)
    for statistic, pairs in pairs_by_stat.items():
        for pair in pairs.values():
            ref, alt = pair.get("reference"), pair.get("alert")
            if ref is not None and alt is not None and ref.source_tables == ("bank/log/log_service.csv",):
                if ref.metric_key.split("|")[1] == "log_volume_count":
                    by_entity[ref.entity.canonical_id].append((ref, alt, statistic))
    output = []
    for candidate in candidates.candidates:
        scores, evidences = [], []
        for ref, alt, statistic in by_entity.get(candidate.entity.canonical_id, []):
            if min(ref.support, alt.support) < runtime["minimum_support"]:
                continue
            block_quantile = 0.5 if statistic == "median" else runtime["tail_quantile"]
            before = block_reduce(ref.details.get("bucket_samples", ()), ref.window.start_ms_utc,
                                  runtime["block_minutes"], block_quantile)
            after = block_reduce(alt.details.get("bucket_samples", ()), alt.window.start_ms_utc,
                                 runtime["block_minutes"], block_quantile)
            test = permutation_contrast(before, after,
                seed=stable_seed(runtime["seed"], candidate.entity.identifier, ref.metric_key, "log-volume"),
                resamples=runtime["permutation_resamples"],
                statistic="median" if statistic == "median" else "quantile",
                minimum_blocks=runtime["minimum_independent_blocks"],
                relative_scale_floor=runtime["relative_scale_floor"],
                absolute_scale_floor=runtime["absolute_scale_floor"],
                quantile=runtime["tail_quantile"])
            if test is None:
                continue
            eligible = (abs(test.normalized_effect) >= runtime["minimum_normalized_effect"] and
                        test.p_value <= runtime["maximum_change_p_value"])
            scores.append(contrast_score(test, p_floor=runtime["permutation_p_score_floor"])
                          if eligible else 0.0)
            evidences.append(Evidence("bank/log/log_service.csv", alt.window,
                f"log_volume_{statistic}_change_per_minute", test.raw_delta, (candidate.entity,),
                min(test.baseline_support, test.alert_support),
                {"log_name": ref.details.get("item_id"),
                 "normalized_effect": test.normalized_effect, "p_value": test.p_value,
                 "unit": "raw rows per fixed one-minute bucket",
                 "zero_semantics": "no rows recorded in the supplied clip; not evidence of healthy logging",
                 "summary_statistic": statistic,
                 "minimum_timestamp_support": runtime["minimum_support"],
                 "minimum_independent_blocks": runtime["minimum_independent_blocks"],
                 "minimum_normalized_effect": runtime["minimum_normalized_effect"],
                 "maximum_change_p_value": runtime["maximum_change_p_value"],
                 "ranking_eligible": eligible,
                 "interpretation": "log-volume change only; not an error rate or event severity"}))
        output.append(ScoredCandidate(candidate.entity, max(scores, default=0.0), None,
                                      tuple(evidences), analysis_family="bank_log_volume"))
    ready = any(any(evidence.details.get("ranking_eligible") is True for evidence in row.evidence)
                for row in output)
    return ScoredCandidates(tuple(output), scorer="bank.logs.volume_contrast",
                            status="ready" if ready else "abstained",
                            status_details={} if ready else {"reason": "no paired log-volume support"})


def combine_bank_scores(inputs: Mapping[str, Any], params: Mapping[str, Any],
                        context: NodeContext, *,
                        policy_path: Path = KPI_POLICY_PATH,
                        weights_override: Mapping[str, float] | None = None) -> RankedAnswer:
    score_families: tuple[ScoredCandidates, ...] = inputs["scores"]
    if weights_override is None:
        # Preserve the frozen M3 Bank behavior exactly unless a versioned M4
        # wrapper explicitly supplies model-selected runtime weights.
        weights = _dev_policy(policy_path).get("family_weights", {})
    else:
        if (not isinstance(weights_override, Mapping) or any(
                not isinstance(name, str) or type(value) not in (int, float) or
                not math.isfinite(float(value)) or float(value) < 0
                for name, value in weights_override.items())):
            raise ValueError("Bank M4 family-weight override must be a finite nonnegative JSON mapping")
        weights = dict(weights_override)
    family_scores: dict[str, dict[str, float]] = defaultdict(dict)
    family_evidence: dict[str, dict[str, list[Evidence]]] = defaultdict(lambda: defaultdict(list))
    family_status: dict[str, bool] = defaultdict(bool)
    family_entities: dict[str, dict[str, set[EntityRef]]] = defaultdict(lambda: defaultdict(set))
    for family in score_families:
        for row in family.candidates:
            name = row.analysis_family or family.scorer
            canonical = row.entity.canonical_id
            family_scores[name][canonical] = max(family_scores[name].get(canonical, 0.0), row.score)
            family_entities[name][canonical].add(row.entity)
            family_evidence[name][canonical].extend(row.evidence)
            if family.status == "ready" and row.score > 0:
                family_status[name] = True
    eligible_families = [name for name, is_ready in family_status.items() if is_ready]
    family_max = {name: max(scores.values(), default=0.0)
                  for name, scores in family_scores.items()}
    eligible_families = sorted(name for name in eligible_families
                               if family_max.get(name, 0.0) > 0 and
                               float(weights.get(name, 1.0)) > 0)
    denominator = sum(float(weights.get(name, 1.0)) for name in eligible_families)
    if denominator <= 0:
        return RankedAnswer(())
    all_ids = sorted(set().union(*(set(scores) for scores in family_scores.values()))
                     if family_scores else set())
    combined = []
    for canonical in all_ids:
        score = 0.0
        evidence = []
        representatives = []
        for name in eligible_families:
            candidates = family_entities[name].get(canonical, set())
            evidence.extend(family_evidence[name].get(canonical, ()))
            if candidates:
                representatives.extend(candidates)
                score += (float(weights.get(name, 1.0)) *
                          family_scores[name].get(canonical, 0.0) / family_max[name])
        if not representatives:
            continue
        entity = min(representatives, key=lambda item: (item.identifier.casefold(),
                                                        item.identifier, item.role))
        eligible_evidence = any(item.details.get("ranking_eligible") is True for item in evidence)
        total_score = score / denominator
        if total_score > 0 and eligible_evidence:
            evidence.sort(key=_stable_evidence_key)
            combined.append(ScoredCandidate(entity, total_score, None, tuple(evidence),
                                            analysis_family="bank_combined"))
    combined.sort(key=lambda item: (-item.score, item.entity.canonical_id, item.entity.role))
    limit = int(params.get("limit", context.runtime_values.get("lead_count", 3)))
    return RankedAnswer(tuple(combined[:limit]))


def _stable_evidence_key(evidence: Evidence) -> str:
    """Return a canonical key without changing or deduplicating evidence."""
    def normalize(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): normalize(child) for key, child in
                    sorted(value.items(), key=lambda pair: str(pair[0]))}
        if isinstance(value, (list, tuple)):
            return [normalize(child) for child in value]
        if isinstance(value, (set, frozenset)):
            normalized = [normalize(child) for child in value]
            return sorted(normalized, key=lambda child: json.dumps(
                child, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        if isinstance(value, EntityRef):
            return {"role": value.role, "identifier": value.identifier}
        return value

    content = {
        "source": evidence.source,
        "window": {"name": evidence.window.name,
                   "start_ms_utc": evidence.window.start_ms_utc,
                   "end_ms_utc": evidence.window.end_ms_utc},
        "statistic": evidence.statistic,
        "value": evidence.value,
        "contributing_entities": [normalize(entity) for entity in evidence.contributing_entities],
        "support": evidence.support,
        "details": normalize(evidence.details),
    }
    return json.dumps(content, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)
