"""Statically checked typed API examples for inclusion in authoring prompts."""

from __future__ import annotations

# Example 1: Metric Anomaly Operator
METRIC_ANOMALY_EXAMPLE = '''
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    """Compare alert window metric mean against reference baseline."""
    tel = telemetry_view(inputs["telemetry"])
    ref_w = tel.reference_window
    alt_w = tel.alert_window
    threshold = float(params.get("threshold", 2.0))

    # 1. Query metric points for alert window using .rows
    alt_res = metric_points(tel, "metrics.csv", alt_w)
    
    # 2. Group alert points by entity
    by_entity = group_by(alt_res.rows, ["entity_identifier"])

    rows: list[AggregateRowView] = []
    for (ent_id,), points in by_entity.items():
        if not ent_id or not points:
            continue
        vals = [p.value for p in points]
        summary = summarize(vals, "mean")
        if summary.value is not None and summary.value > threshold:
            ent = make_entity_ref("service", str(ent_id))
            rows.append(
                make_aggregate_row(
                    entity=ent,
                    metric_key=points[0].metric_key,
                    window=alt_w,
                    statistic="mean",
                    value=summary.value,
                    support=summary.support,
                    source_tables=["metrics.csv"],
                )
            )

    return make_aggregate(alt_w, rows, source_tables=["metrics.csv"])
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
