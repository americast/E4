"""Bank baseline starter operator catalog and seed playbooks."""

from __future__ import annotations

from typing import Any, Mapping

from e4.dag import VerifiedDAG
from e4.registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec
from e4.sandbox.views import (
    Aggregate,
    AggregateRow,
    Candidate,
    CandidateSet,
    EntityRef,
    Evidence,
    MetricPointView,
    QueryResult,
    ScoredCandidate,
    ScoredCandidates,
    SummaryResult,
    TelemetryView,
    Window,
    aggregate_view,
    candidate_views,
    group_by,
    make_aggregate,
    make_aggregate_row,
    make_candidate,
    make_candidate_set,
    make_evidence,
    make_scored_candidate,
    make_scored_candidates,
    metric_points,
    scored_views,
    summarize,
    telemetry_view,
    top_k,
)
from e4.types import RankedAnswer


def bank_metric_aggregate_impl(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> Aggregate:
    """Baseline operator: compute mean metric value for each component in alert window."""
    tel = telemetry_view(inputs["telemetry"])
    alt_w = tel.alert_window
    tbl = str(params.get("table", "bank/metric_container.csv"))

    points_res = metric_points(tel, tbl, alt_w)
    grouped = group_by(points_res.rows, ["entity_identifier", "metric_key"])

    rows: list[AggregateRow] = []
    for (ent_id, m_key), pts in grouped.items():
        if not ent_id or not pts:
            continue
        vals = [p.value for p in pts]
        sm = summarize(vals, "mean")
        ent = EntityRef("service", str(ent_id))
        rows.append(
            AggregateRow(
                entity=ent,
                metric_key=str(m_key),
                window=alt_w,
                statistic="mean",
                value=sm.value,
                support=sm.support,
                source_tables=(tbl,),
            )
        )
    return Aggregate(window=alt_w, rows=tuple(rows), source_tables=(tbl,))


def bank_anomaly_scorer_impl(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> ScoredCandidates:
    """Baseline operator: score components based on highest aggregate anomaly."""
    agg = aggregate_view(inputs["aggregate"])
    scores_by_entity: dict[str, tuple[EntityRef, float, list[Evidence]]] = {}

    for r in agg.rows:
        val = r.value if r.value is not None else 0.0
        cid = r.entity.identifier
        if cid not in scores_by_entity or val > scores_by_entity[cid][1]:
            ev = Evidence(
                source="metric_anomaly_scorer",
                window=Window(r.window.name, r.window.start_ms_utc, r.window.end_ms_utc),
                statistic=r.statistic,
                value=val,
                contributing_entities=(r.entity,),
                support=r.support,
                ranking_eligible=True,
            )
            scores_by_entity[cid] = (r.entity, float(val), [ev])

    scored: list[ScoredCandidate] = []
    for ent, sc, ev_list in scores_by_entity.values():
        scored.append(
            ScoredCandidate(
                entity=ent,
                score=sc,
                evidence=tuple(ev_list),
                analysis_family="metric_anomaly",
            )
        )
    scored.sort(key=lambda x: x.score, reverse=True)
    return ScoredCandidates(candidates=tuple(scored), scorer="bank_metric_anomaly_scorer")


def weighted_consensus_impl(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> RankedAnswer:
    """Baseline operator: combine candidate scores across multiple upstream scorers."""
    in_scores = inputs.get("scores")
    scorers = in_scores if isinstance(in_scores, (list, tuple)) else [in_scores]

    weights = params.get("family_weights", {})
    totals: dict[str, tuple[EntityRef, float]] = {}

    for item in scorers:
        if not item:
            continue
        sc_view = item if hasattr(item, "candidates") else ScoredCandidates(candidates=(), scorer="")
        for c in sc_view.candidates:
            if hasattr(c, "entity"):
                ent = c.entity
                score_val = float(c.score)
                fam = getattr(c, "analysis_family", "default")
            elif isinstance(c, Mapping):
                ent = c["entity"]
                score_val = float(c.get("score", 0.0))
                fam = c.get("analysis_family", "default")
            else:
                continue
            cid = ent.identifier if hasattr(ent, "identifier") else ent["identifier"]
            e_role = ent.role if hasattr(ent, "role") else ent.get("role", "service")
            e_obj = EntityRef(e_role, cid)
            w = float(weights.get(fam, 1.0))
            weighted_score = score_val * w
            if cid not in totals:
                totals[cid] = (e_obj, 0.0)
            totals[cid] = (e_obj, totals[cid][1] + weighted_score)

    sorted_entries = sorted(totals.values(), key=lambda t: t[1], reverse=True)
    cands = tuple(item[0] for item in sorted_entries)
    scores = tuple(item[1] for item in sorted_entries)
    return RankedAnswer(candidates=cands, scores=scores, stage="consensus")


def build_bank_registry() -> OperatorRegistry:
    """Build and return an OperatorRegistry pre-loaded with Bank baseline operators."""
    reg = OperatorRegistry()

    reg.register(
        OperatorSpec(
            name="bank.metrics.aggregate",
            version="1",
            inputs=(PortSpec(name="telemetry", value_type="Telemetry"),),
            output_type="Aggregate",
            parameters={
                "table": ParameterSpec(
                    name="table",
                    value_type="string",
                    has_default=True,
                    default="bank/metric_container.csv",
                )
            },
            family="metric",
            implementation=bank_metric_aggregate_impl,
            description="Aggregate Bank metric container observations by component and metric",
        )
    )

    reg.register(
        OperatorSpec(
            name="bank.anomaly.scorer",
            version="1",
            inputs=(PortSpec(name="aggregate", value_type="Aggregate"),),
            output_type="ScoredCandidates",
            family="scorer",
            implementation=bank_anomaly_scorer_impl,
            description="Score components by highest aggregated metric anomaly",
        )
    )

    reg.register(
        OperatorSpec(
            name="paper.answers.weighted_consensus",
            version="1",
            inputs=(PortSpec(name="scores", value_type="ScoredCandidates", variadic=True),),
            output_type="RankedAnswer",
            parameters={
                "family_weights": ParameterSpec(
                    name="family_weights",
                    value_type="object",
                    has_default=True,
                    default={},
                )
            },
            family="consensus",
            implementation=weighted_consensus_impl,
            description="Synthesize weighted consensus root-cause ranking across analysis families",
        )
    )

    return reg


def get_bank_seed_playbook() -> dict[str, Any]:
    """Default retrieve playbook for OpenRCA Bank."""
    return {
        "name": "bank.starter.metric_consensus",
        "description": "Baseline 3-node diagnosis DAG for Bank container metrics",
        "dag": {
            "nodes": [
                {
                    "id": "node_metric_agg",
                    "operator": "bank.metrics.aggregate",
                    "version": "1",
                    "inputs": {"telemetry": "telemetry"},
                    "params": {"table": "bank/metric_container.csv"},
                },
                {
                    "id": "node_anomaly_scorer",
                    "operator": "bank.anomaly.scorer",
                    "version": "1",
                    "inputs": {"aggregate": "node_metric_agg"},
                },
                {
                    "id": "node_consensus",
                    "operator": "paper.answers.weighted_consensus",
                    "version": "1",
                    "inputs": {"scores": ["node_anomaly_scorer"]},
                },
            ],
            "return_node": "node_consensus",
        },
    }
