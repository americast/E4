"""Typed API references for inclusion in authoring prompts."""

from __future__ import annotations

from dataclasses import fields
from inspect import signature

from e4.sandbox.views import (
    AggregateRowView,
    AggregateView,
    CandidateSetView,
    CandidateView,
    EntityRefView,
    EvidenceView,
    LogRecordView,
    MetricPointView,
    ScoredCandidateView,
    ScoredCandidatesView,
    SpanView,
    WindowView,
    aggregate_view,
    aggregate_records,
    candidate_views,
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
    """Render signatures and row fields from the runtime implementation."""
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
        aggregate_view,
        candidate_views,
        scored_views,
    )
    lines = ["Typed data helper signatures (from the runtime API):"]
    lines.extend(f"- {fn.__name__}{signature(fn)}" for fn in helpers)
    for label, row_type in (
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
        ("ScoredCandidateView", ScoredCandidateView),
        ("ScoredCandidatesView", ScoredCandidatesView),
    ):
        lines.append(f"{label} fields: {', '.join(field.name for field in fields(row_type))}")
    lines.append(
        "QueryResult exposes rows, window, source_tables, and coverage; "
        "CoverageView exposes complete, truncated, offset, next_offset, total_matches, and support."
    )
    return "\n".join(lines)


# This is a data access/output example, not a prescribed diagnostic algorithm.
METRIC_ANOMALY_EXAMPLE = '''
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    """Read one schema-selected metric table and return a typed aggregate."""
    tel = telemetry_view(inputs["telemetry"])
    alt_w = tel.alert_window
    source_table = params["source_table"]

    # A capped result is incomplete; do not build downstream evidence from it.
    alt_res = metric_points(tel, source_table, alt_w)
    if not alt_res.coverage.complete:
        return make_aggregate(
            alt_w,
            [],
            source_tables=[source_table],
            metadata={"abstained": "incomplete query coverage"},
        )

    by_entity = group_by(alt_res.rows, ["entity_identifier"])

    rows: list[AggregateRowView] = []
    for (ent_id,), points in by_entity.items():
        if not ent_id or not points:
            continue
        vals = [p.value for p in points]
        summary = summarize(vals, "mean")
        if summary.value is not None:
            ent = make_entity_ref(points[0].entity_role, str(ent_id))
            rows.append(
                make_aggregate_row(
                    entity=ent,
                    metric_key=points[0].metric_key,
                    window=alt_w,
                    statistic="mean",
                    value=float(summary.value),
                    support=summary.support,
                    source_tables=[source_table],
                )
            )

    return make_aggregate(alt_w, rows, source_tables=[source_table])
'''.strip()

# Example 2: Scored Candidates Operator
SCORED_CANDIDATE_EXAMPLE = '''
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    """Rank entities based on aggregate anomaly rows."""
    agg = aggregate_view(inputs["aggregate"])
    scores: list[ScoredCandidateView] = []

    for r in agg.rows:
        val = r.value if r.value is not None else 0.0
        score = min(1.0, max(0.0, float(val) / 100.0))
        ev = make_evidence(
            source="anomaly_detector",
            window=r.window,
            statistic=r.statistic,
            value=val,
            contributing_entities=[r.entity],
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

    return make_scored_candidates(scores, scorer="anomaly_scorer")
'''.strip()
