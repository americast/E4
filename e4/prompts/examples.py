"""Typed API references for inclusion in authoring prompts."""

from __future__ import annotations

from dataclasses import fields
from inspect import signature
from typing import get_type_hints

from e4.sandbox.views import (
    AggregateRowView,
    AggregateView,
    AggregateGroupView,
    CandidateSetView,
    CandidateView,
    EntityRefView,
    EvidenceView,
    LogRecordView,
    MetricPointView,
    ScoredCandidateView,
    ScoredCandidatesView,
    SummaryResult,
    CoverageView,
    SpanView,
    WindowView,
    aggregate_view,
    aggregate_records,
    candidate_views,
    entity_id,
    group_by,
    log_records,
    make_aggregate,
    make_aggregate_row,
    make_candidate,
    make_candidate_set,
    make_entity_ref,
    make_evidence,
    make_scored_candidate,
    make_scored_candidates,
    make_window,
    metric_points,
    scored_views,
    span_records,
    summarize,
    top_k,
    telemetry_view,
)


def typed_api_reference() -> str:
    """Render actual runtime helper signatures and typed-view fields."""
    helpers = (
        metric_points,
        span_records,
        log_records,
        telemetry_view,
        aggregate_records,
        group_by,
        summarize,
        top_k,
        make_evidence,
        make_window,
        make_entity_ref,
        make_candidate,
        make_candidate_set,
        make_aggregate_row,
        make_aggregate,
        make_scored_candidate,
        make_scored_candidates,
        entity_id,
        aggregate_view,
        candidate_views,
        scored_views,
    )
    lines = ["Typed data helper signatures (rendered from the runtime API):"]
    lines.extend(f"- {fn.__name__}{signature(fn)}" for fn in helpers)
    for label, row_type in (
        ("CoverageView", CoverageView),
        ("MetricPointView", MetricPointView),
        ("SpanView", SpanView),
        ("LogRecordView", LogRecordView),
        ("WindowView", WindowView),
        ("EntityRefView", EntityRefView),
        ("EvidenceView", EvidenceView),
        ("CandidateView", CandidateView),
        ("CandidateSetView", CandidateSetView),
        ("AggregateRowView", AggregateRowView),
        ("AggregateView", AggregateView),
        ("AggregateGroupView", AggregateGroupView),
        ("SummaryResult", SummaryResult),
        ("ScoredCandidateView", ScoredCandidateView),
        ("ScoredCandidatesView", ScoredCandidatesView),
    ):
        hints = get_type_hints(row_type)
        field_types = ", ".join(
            f"{field.name}: {hints.get(field.name, field.type)}"
            for field in fields(row_type)
        )
        lines.append(f"{label} fields: {field_types}")
    lines.append(
        "TelemetryView fields: case_id: str, reference_window: WindowView, "
        "alert_window: WindowView, source_tables: Sequence[str], point_count: int"
    )
    lines.append(
        "QueryResult[T] fields: rows: Sequence[T], window: WindowView, "
        "source_tables: Sequence[str], coverage: CoverageView"
    )
    return "\n".join(lines)


# This is a data access/output example, not a prescribed diagnostic algorithm.
METRIC_ANOMALY_EXAMPLE = '''
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    """Compare full-window per-entity metric means; this is an API example."""
    tel = telemetry_view(inputs["telemetry"])
    ref_w = tel.reference_window
    alt_w = tel.alert_window
    source_table = params["source_table"]

    # Aggregation scans the requested window. If its grouped output exceeds the
    # helper bound, it reports incomplete coverage and this example abstains.
    ref_res = aggregate_records(
        tel, source_table, ref_w, "metric", "value", "mean",
        group_by=["entity_role", "entity_identifier", "metric_key"],
    )
    alt_res = aggregate_records(
        tel, source_table, alt_w, "metric", "value", "mean",
        group_by=["entity_role", "entity_identifier", "metric_key"],
    )
    if not ref_res.coverage.complete or not alt_res.coverage.complete:
        return make_aggregate(
            alt_w,
            [],
            source_tables=[source_table],
            metadata={"abstained": "incomplete full-window aggregation coverage"},
        )

    reference_by_key: dict[tuple[Scalar, ...], AggregateGroupView] = {
        group.key: group for group in ref_res.rows
    }

    rows: list[AggregateRowView] = []
    for alert_group in alt_res.rows:
        key = alert_group.key
        if len(key) != 3:
            continue
        role, identifier, metric_key = key
        if not isinstance(role, str) or not isinstance(identifier, str):
            continue
        if not isinstance(metric_key, str) or alert_group.value is None:
            continue
        reference_group = reference_by_key.get(key)
        # A missing reference group is unknown, not a zero-valued baseline.
        if reference_group is None or reference_group.value is None:
            continue
        entity = make_entity_ref(role, identifier)
        rows.append(
            make_aggregate_row(
                entity=entity,
                metric_key=metric_key,
                window=alt_w,
                statistic="alert_mean_minus_reference_mean",
                value=float(alert_group.value) - float(reference_group.value),
                support=min(alert_group.support, reference_group.support),
                source_tables=[source_table],
                contributing_entities=[entity],
            )
        )

    metadata: dict[str, str] = {}
    if not rows:
        metadata["abstained"] = "no complete matched entity/metric groups"
    return make_aggregate(alt_w, rows, source_tables=[source_table], metadata=metadata)
'''.strip()

# This is a complete log-analysis example. It illustrates full-window grouped
# aggregation and coverage handling, not a prescribed diagnostic algorithm.
LOG_ANALYSIS_EXAMPLE = '''
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    """Summarize alert-window log event counts for identified entities."""
    tel = telemetry_view(inputs["telemetry"])
    window = tel.alert_window
    source_table = params["source_table"]
    result = aggregate_records(
        tel,
        source_table,
        window,
        "log",
        "count",
        "count",
        group_by=["entity_role", "entity_identifier"],
    )
    if not result.coverage.complete:
        return make_aggregate(
            window,
            [],
            source_tables=[source_table],
            metadata={"abstained": "incomplete log query coverage"},
        )

    rows: list[AggregateRowView] = []
    for group in result.rows:
        if len(group.key) != 2:
            continue
        role, identifier = group.key
        if not isinstance(role, str) or not isinstance(identifier, str):
            continue
        if not isinstance(group.value, (int, float)):
            continue
        entity = make_entity_ref(role, identifier)
        rows.append(
            make_aggregate_row(
                entity=entity,
                metric_key="log.event_count",
                window=window,
                statistic="count",
                value=float(group.value),
                support=group.support,
                source_tables=[source_table],
                contributing_entities=[entity],
            )
        )
    metadata: dict[str, str] = {}
    if not rows:
        metadata["abstained"] = "no complete identified log records"
    return make_aggregate(window, rows, source_tables=[source_table], metadata=metadata)
'''.strip()

# Example 2: Scored Candidates Operator
SCORED_CANDIDATE_EXAMPLE = '''
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    """Convert supported aggregate rows into an illustrative scored output."""
    agg = aggregate_view(inputs["aggregate"])
    scores: list[ScoredCandidateView] = []

    if (
        agg.metadata.get("truncated") is True
        or agg.metadata.get("coverage_complete") is False
        or "abstained" in agg.metadata
    ):
        return make_scored_candidates(
            scores,
            scorer="anomaly_scorer",
            status="abstained",
            status_details={"reason": "input aggregate is incomplete or abstained"},
        )

    for r in agg.rows:
        if r.value is None or r.support <= 0:
            continue
        contributors = r.contributing_entities
        if not contributors:
            continue
        # Example mapping only; choose a justified mapping for the domain.
        score = abs(float(r.value))
        ev = make_evidence(
            source="aggregate_input",
            window=r.window,
            statistic=r.statistic,
            value=r.value,
            contributing_entities=contributors,
            support=r.support,
            ranking_eligible=True,
        )
        scores.append(
            make_scored_candidate(
                entity=r.entity,
                score=score,
                evidence=[ev],
                analysis_family="metric_anomaly",
            )
        )

    if not scores:
        return make_scored_candidates(
            scores,
            scorer="anomaly_scorer",
            status="abstained",
            status_details={"reason": "no rows with supported non-null values"},
        )
    return make_scored_candidates(scores, scorer="anomaly_scorer")
'''.strip()
