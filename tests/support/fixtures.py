"""Tiny deterministic fixture operators for core conformance and cache tests."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any, Mapping

from e4.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet, EntityRef, Evidence,
                          RankedAnswer, ScoredCandidate, ScoredCandidates, Telemetry)
from e4.dsl.cache import digest_value
from e4.dsl.executor import NodeContext, OperatorResult
from e4.dsl.registry import OperatorRegistry, OperatorSpec, PortSpec
from e4.dsl.runtime import ParameterSpec


def fixture_registry() -> OperatorRegistry:
    registry = OperatorRegistry()
    registry.register(OperatorSpec("fixture.telemetry", "1", (), "Telemetry",
        cacheable=False, implementation=None, external_input=True,
        description="Reads the executor-injected public synthetic Telemetry value."))
    registry.register(OperatorSpec("fixture.aggregate_input", "1", (), "Aggregate",
        cacheable=False, implementation=None, external_input=True,
        description="Test-only injection point for standalone aggregate cache invalidation."))
    registry.register(OperatorSpec("fixture.aggregate", "1",
        (PortSpec("data", "Telemetry"),), "Aggregate",
        {"table": ParameterSpec("table", "string", required=True),
         "metric_name": ParameterSpec("metric_name", "string", required=True),
         "window": ParameterSpec("window", "string", required=True)},
        family="aggregation", implementation=_aggregate,
        description="Mean finite values in one selected metric table/window."))
    registry.register(OperatorSpec("fixture.candidates", "1",
        (PortSpec("aggregate", "Aggregate"),), "CandidateSet",
        implementation=_candidates, family="candidate",
        description="One-candidate synthetic generator."))
    registry.register(OperatorSpec("fixture.score", "1",
        (PortSpec("aggregate", "Aggregate"), PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates", {"scale": ParameterSpec("scale", "number", default=1.0, has_default=True)},
        family="scoring", implementation=_score,
        description="Hand-checkable absolute aggregate score."))
    registry.register(OperatorSpec("fixture.adjust", "1",
        (PortSpec("scores", "ScoredCandidates"),), "ScoredCandidates",
        {"factor": ParameterSpec("factor", "number", default=1.0, has_default=True)},
        family="scoring", implementation=_adjust,
        description="Synthetic score-adjustment node for intermediate score-sink verification."))
    registry.register(OperatorSpec("fixture.rank", "1",
        (PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),), "RankedAnswer",
        {"limit": ParameterSpec("limit", "positive_integer", runtime_field="lead_count")},
        family="ranking", implementation=_rank,
        description="Merge branches, dedupe by canonical component ID, stable score/id tie-break."))
    return registry


def fixture_dag() -> dict[str, Any]:
    def node(identifier: str, operator: str, inputs: dict | None = None,
             params: dict | None = None, bindings: dict | None = None) -> dict:
        return {"id": identifier, "operator": operator, "version": "1",
                "inputs": inputs or {}, "params": params or {}, "bindings": bindings or {}}
    return {"schema_version": "e4-dag/1", "nodes": [
        node("data", "fixture.telemetry"),
        node("metrics", "fixture.aggregate", {"data": "data"},
             {"table": "metric/metric_app.csv", "metric_name": "avg_time", "window": "alert"}),
        node("resources", "fixture.aggregate", {"data": "data"},
             {"table": "metric/metric_node.csv", "metric_name": "cpu", "window": "alert"}),
        node("metric_candidates", "fixture.candidates", {"aggregate": "metrics"}),
        node("resource_candidates", "fixture.candidates", {"aggregate": "resources"}),
        node("metric_scores", "fixture.score", {"aggregate": "metrics", "candidates": "metric_candidates"}),
        node("resource_scores", "fixture.score", {"aggregate": "resources", "candidates": "resource_candidates"}),
        node("adjusted_metric_scores", "fixture.adjust", {"scores": "metric_scores"}, {"factor": 0.5}),
        node("rank", "fixture.rank", {"scores": ["adjusted_metric_scores", "resource_scores"]}),
    ], "return": {"node": "rank"}}


def _aggregate(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> OperatorResult:
    telemetry: Telemetry = inputs["data"]
    table = params["table"]
    metric_name = params["metric_name"]
    window_name = params["window"]
    window = telemetry.alert_window if window_name == "alert" else telemetry.reference_window
    grouped: dict[tuple[EntityRef, str], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    rows_seen = 0
    for point in telemetry.metrics:
        if (point.table != table or point.metric_name != metric_name
                or not window.contains(point.timestamp_ms_utc)):
            continue
        rows_seen += 1
        if point.value is not None and math.isfinite(point.value):
            grouped[(point.entity, point.metric_name)][point.timestamp_ms_utc].append(point.value)
    if not grouped:
        # The fixture assumes its selected table contains a single supported series.
        raise ValueError(f"no finite metric values for fixture table {table}")
    rows = []
    for (entity, metric_key), buckets in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        per_minute = [statistics.median(values) for _, values in sorted(buckets.items())]
        rows.append(AggregateRow(entity, metric_key, window, "mean_bucket_median",
                                 sum(per_minute) / len(per_minute), len(per_minute), (table,), (entity,),
                                 {"raw_rows": sum(map(len, buckets.values())),
                                  "distinct_timestamps": len(buckets),
                                  "duplicate_timestamp_rows": sum(max(0, len(values) - 1) for values in buckets.values())}))
    return OperatorResult(Aggregate(window, tuple(rows), (table,),
                                    {"raw_points_scanned": rows_seen, "series_count": len(rows)}),
                          rows_processed=rows_seen)


def _candidates(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> CandidateSet:
    aggregate: Aggregate = inputs["aggregate"]
    entities: dict[tuple[str, str], Candidate] = {}
    for row in aggregate.rows:
        key = (row.entity.role, row.entity.canonical_id)
        prior = entities.get(key)
        rationale = f"fixture source {row.metric_key}"
        entities[key] = Candidate(row.entity, rationale if prior is None else prior.rationale + "; " + rationale)
    return CandidateSet(tuple(entities[key] for key in sorted(entities)),
                        source="fixture.aggregate")


def _score(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    aggregate: Aggregate = inputs["aggregate"]
    candidates: CandidateSet = inputs["candidates"]
    scored = []
    for candidate in candidates.candidates:
        rows = [row for row in aggregate.rows
                if row.entity.canonical_id == candidate.entity.canonical_id]
        if not rows:
            continue
        score = max(abs(float(row.value or 0.0)) for row in rows) * float(params["scale"])
        evidence = tuple(Evidence(table, row.window, row.statistic, row.value,
                                  row.contributing_entities, row.support,
                                  {"metric_key": row.metric_key, **dict(row.details)})
                         for row in rows for table in row.source_tables)
        scored.append(ScoredCandidate(candidate.entity, score, None, evidence,
                                      analysis_family="fixture"))
    return ScoredCandidates(tuple(scored), scorer="fixture.score")


def _adjust(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> ScoredCandidates:
    scores: ScoredCandidates = inputs["scores"]
    factor = float(params["factor"])
    return ScoredCandidates(tuple(ScoredCandidate(item.entity, item.score * factor,
        item.category, item.evidence, analysis_family="fixture.adjust") for item in scores.candidates),
        scorer="fixture.adjust")


def _rank(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> RankedAnswer:
    scores: tuple[ScoredCandidates, ...] = inputs["scores"]
    merged: dict[str, ScoredCandidate] = {}
    for branch in scores:
        for candidate in branch.candidates:
            key = candidate.entity.canonical_id
            prior = merged.get(key)
            if prior is None:
                merged[key] = candidate
                continue
            evidence_by_hash = {digest_value(item): item for item in (*prior.evidence, *candidate.evidence)}
            evidence = tuple(evidence_by_hash[key] for key in sorted(evidence_by_hash))
            category = prior.category if prior.category == candidate.category else None
            best = prior if prior.score >= candidate.score else candidate
            merged[key] = ScoredCandidate(best.entity, max(prior.score, candidate.score), category,
                                           evidence, analysis_family="fixture.rank")
    ordered = sorted(merged.values(), key=lambda item: (-item.score, item.entity.canonical_id))
    return RankedAnswer(tuple(ordered[:params["limit"]]))
