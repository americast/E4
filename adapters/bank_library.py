"""Bank baseline starter operator catalog and seed playbooks."""

from __future__ import annotations

import math
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
    SpanView,
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
    span_records,
    summarize,
    telemetry_view,
    top_k,
)
from e4.types import RankedAnswer


# Canonical consensus group aliases
CANONICAL_FAMILY_ALIASES: Mapping[str, str] = {
    "metric": "backend_metric",
    "bank_metric": "backend_metric",
    "metric_anomaly": "backend_metric",
    "bank.metrics.contrast": "backend_metric",
    "trace_latency": "trace_latency",
    "bank_trace_latency": "trace_latency",
    "bank.traces.latency_contrast": "trace_latency",
    "log_volume": "log_volume",
    "bank_log_volume": "log_volume",
    "bank.logs.volume_contrast": "log_volume",
}


def bank_metric_aggregate_impl(
    inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any
) -> Aggregate:
    """Aggregate metric observations by component and metric key for a declared window."""
    tel = telemetry_view(inputs["telemetry"])
    win_param = str(params.get("window", "alert")).lower()
    target_w = tel.reference_window if win_param == "reference" else tel.alert_window
    tbl = str(params.get("table", "bank/metric_container.csv"))

    points_res = metric_points(tel, tbl, target_w)
    grouped = group_by(points_res.rows, ["entity_identifier", "metric_key"])

    rows: list[AggregateRow] = []
    for (ent_id, m_key), pts in grouped.items():
        if not ent_id or not pts:
            continue
        vals = [p.value for p in pts]
        sm_mean = summarize(vals, "mean")
        sm_med = summarize(vals, "quantile", quantile=0.5)
        ent = EntityRef("service", str(ent_id))
        rows.append(
            AggregateRow(
                entity=ent,
                metric_key=str(m_key),
                window=target_w,
                statistic="mean",
                value=sm_mean.value,
                support=sm_mean.support,
                source_tables=(tbl,),
                details={"mean": sm_mean.value, "median": sm_med.value},
            )
        )
    return Aggregate(window=target_w, rows=tuple(rows), source_tables=(tbl,))


def bank_metrics_contrast_impl(
    inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any
) -> ScoredCandidates:
    """Paired reference-to-alert window metric contrast scoring."""
    ref_agg = aggregate_view(inputs["reference"])
    alt_agg = aggregate_view(inputs["alert"])

    # Index reference metrics by (entity_identifier, metric_key)
    ref_map: dict[tuple[str, str], AggregateRow] = {}
    for r in ref_agg.rows:
        ref_map[(r.entity.identifier, r.metric_key)] = r

    # Group alert metrics by entity
    alt_by_entity: dict[str, list[AggregateRow]] = {}
    for r in alt_agg.rows:
        cid = r.entity.identifier
        if cid not in alt_by_entity:
            alt_by_entity[cid] = []
        alt_by_entity[cid].append(r)

    scored: list[ScoredCandidate] = []
    for cid, alt_rows in alt_by_entity.items():
        entity = alt_rows[0].entity
        metric_shifts: list[tuple[float, Evidence]] = []

        for alt_r in alt_rows:
            ref_r = ref_map.get((cid, alt_r.metric_key))
            if ref_r is None:
                continue

            ref_val = ref_r.value if ref_r.value is not None else 0.0
            alt_val = alt_r.value if alt_r.value is not None else 0.0
            denom = max(abs(ref_val), 1e-4)
            shift = abs(alt_val - ref_val) / denom

            ev = Evidence(
                source="bank.metrics.contrast",
                window=Window(alt_r.window.name, alt_r.window.start_ms_utc, alt_r.window.end_ms_utc),
                statistic="relative_shift",
                value=shift,
                contributing_entities=(entity,),
                support=min(ref_r.support, alt_r.support),
                ranking_eligible=True,
                details={
                    "metric_key": alt_r.metric_key,
                    "reference_value": ref_val,
                    "alert_value": alt_val,
                },
            )
            metric_shifts.append((shift, ev))

        if not metric_shifts:
            continue

        # Sort by largest shift
        metric_shifts.sort(key=lambda item: item[0], reverse=True)
        max_shift = metric_shifts[0][0]
        top_evidences = tuple(item[1] for item in metric_shifts[:3])

        scored.append(
            ScoredCandidate(
                entity=entity,
                score=max_shift,
                evidence=top_evidences,
                analysis_family="backend_metric",
            )
        )

    scored.sort(key=lambda sc: sc.score, reverse=True)
    return ScoredCandidates(candidates=tuple(scored), scorer="bank.metrics.contrast")


def bank_traces_aggregate_impl(
    inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any
) -> Aggregate:
    """Aggregate trace durations per component for a declared window."""
    tel = telemetry_view(inputs["telemetry"])
    win_param = str(params.get("window", "alert")).lower()
    target_w = tel.reference_window if win_param == "reference" else tel.alert_window
    tbl = str(params.get("table", "bank/trace_span.csv"))

    spans_res = span_records(tel, tbl, target_w)
    grouped = group_by(spans_res.rows, ["executing_component_identifier"])

    rows: list[AggregateRow] = []
    for (cid,), spans in grouped.items():
        if not cid or not spans:
            continue
        durations = [s.duration_ms for s in spans]
        sm = summarize(durations, "mean")
        ent = EntityRef("service", str(cid))
        rows.append(
            AggregateRow(
                entity=ent,
                metric_key="trace_duration_ms",
                window=target_w,
                statistic="mean",
                value=sm.value,
                support=sm.support,
                source_tables=(tbl,),
            )
        )
    return Aggregate(window=target_w, rows=tuple(rows), source_tables=(tbl,))


def bank_traces_latency_contrast_impl(
    inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any
) -> ScoredCandidates:
    """Contrast trace durations between reference and alert windows."""
    ref_agg = aggregate_view(inputs["reference"])
    alt_agg = aggregate_view(inputs["alert"])

    ref_map = {r.entity.identifier: r for r in ref_agg.rows}
    scored: list[ScoredCandidate] = []

    for alt_r in alt_agg.rows:
        cid = alt_r.entity.identifier
        ref_r = ref_map.get(cid)
        if ref_r is None:
            continue

        ref_dur = ref_r.value if ref_r.value is not None else 0.0
        alt_dur = alt_r.value if alt_r.value is not None else 0.0
        denom = max(ref_dur, 1.0)
        shift = (alt_dur - ref_dur) / denom if alt_dur > ref_dur else 0.0

        ev = Evidence(
            source="bank.traces.latency_contrast",
            window=Window(alt_r.window.name, alt_r.window.start_ms_utc, alt_r.window.end_ms_utc),
            statistic="relative_latency_shift",
            value=shift,
            contributing_entities=(alt_r.entity,),
            support=min(ref_r.support, alt_r.support),
            ranking_eligible=True,
            details={"reference_duration_ms": ref_dur, "alert_duration_ms": alt_dur},
        )
        scored.append(
            ScoredCandidate(
                entity=alt_r.entity,
                score=shift,
                evidence=(ev,),
                analysis_family="trace_latency",
            )
        )

    scored.sort(key=lambda x: x.score, reverse=True)
    return ScoredCandidates(candidates=tuple(scored), scorer="bank.traces.latency_contrast")


def bank_anomaly_scorer_impl(
    inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any
) -> ScoredCandidates:
    """Baseline operator: score components based on highest aggregate alert anomaly."""
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
                analysis_family="backend_metric",
            )
        )
    scored.sort(key=lambda x: x.score, reverse=True)
    return ScoredCandidates(candidates=tuple(scored), scorer="bank.anomaly.scorer")


def weighted_consensus_impl(
    inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any
) -> RankedAnswer:
    """Synthesize weighted consensus root-cause ranking across analysis families, preserving evidence."""
    in_scores = inputs.get("scores")
    scorers = in_scores if isinstance(in_scores, (list, tuple)) else [in_scores]

    raw_weights = params.get("family_weights", {})
    weights: dict[str, float] = {}
    if isinstance(raw_weights, Mapping):
        for k, v in raw_weights.items():
            try:
                weights[str(k)] = float(v)
            except (ValueError, TypeError):
                pass

    totals: dict[str, float] = {}
    entities: dict[str, EntityRef] = {}
    all_evidence: dict[str, list[Evidence]] = {}

    for item in scorers:
        if not item:
            continue
        sc_view = item if hasattr(item, "candidates") else ScoredCandidates(candidates=(), scorer="")
        if not sc_view.candidates:
            continue

        raw_scores: list[float] = [float(c.score) for c in sc_view.candidates if hasattr(c, "score")]
        max_scorer_val = max(raw_scores) if raw_scores and max(raw_scores) > 0 else 1.0

        for c in sc_view.candidates:
            if hasattr(c, "entity"):
                ent = c.entity
                score_val = float(c.score)
                raw_fam = getattr(c, "analysis_family", None) or sc_view.scorer
                ev_seq = getattr(c, "evidence", ())
            elif isinstance(c, Mapping):
                ent = c["entity"]
                score_val = float(c.get("score", 0.0))
                raw_fam = c.get("analysis_family") or sc_view.scorer
                ev_seq = c.get("evidence", ())
            else:
                continue

            cid = ent.identifier if hasattr(ent, "identifier") else ent["identifier"]
            e_role = ent.role if hasattr(ent, "role") else ent.get("role", "service")
            canonical_fam = CANONICAL_FAMILY_ALIASES.get(raw_fam, raw_fam)
            fam_weight = weights.get(canonical_fam, weights.get(raw_fam, 1.0))

            norm_score = (score_val / max_scorer_val) * fam_weight
            entities[cid] = EntityRef(e_role, cid)
            totals[cid] = totals.get(cid, 0.0) + norm_score

            if cid not in all_evidence:
                all_evidence[cid] = []
            for ev in ev_seq:
                if isinstance(ev, Evidence):
                    all_evidence[cid].append(ev)
                elif hasattr(ev, "as_dict"):
                    d = ev.as_dict()
                    w = Window(d["window"]["name"], d["window"]["start_ms_utc"], d["window"]["end_ms_utc"])
                    all_evidence[cid].append(
                        Evidence(
                            source=d["source"],
                            window=w,
                            statistic=d["statistic"],
                            value=d.get("value"),
                            contributing_entities=(entities[cid],),
                            support=d.get("support", 0),
                            ranking_eligible=d.get("ranking_eligible", True),
                            details=d.get("details", {}),
                        )
                    )

    sorted_cids = sorted(totals.keys(), key=lambda k: totals[k], reverse=True)
    limit = int(params.get("limit", 10))
    top_cids = sorted_cids[:limit]

    cands = tuple(entities[k] for k in top_cids)
    scores = tuple(totals[k] for k in top_cids)
    ev_list: list[Evidence] = []
    for k in top_cids:
        ev_list.extend(all_evidence.get(k, []))

    return RankedAnswer(candidates=cands, scores=scores, stage="consensus", evidence=tuple(ev_list))


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
                ),
                "window": ParameterSpec(
                    name="window",
                    value_type="string",
                    has_default=True,
                    default="alert",
                    choices=("reference", "alert"),
                ),
            },
            family="backend_metric",
            implementation=bank_metric_aggregate_impl,
            description="Aggregate Bank metric container observations by component and metric",
        )
    )

    reg.register(
        OperatorSpec(
            name="bank.metrics.contrast",
            version="1",
            inputs=(
                PortSpec(name="reference", value_type="Aggregate"),
                PortSpec(name="alert", value_type="Aggregate"),
            ),
            output_type="ScoredCandidates",
            family="backend_metric",
            implementation=bank_metrics_contrast_impl,
            description="Score components by paired reference-to-alert window metric contrast",
        )
    )

    reg.register(
        OperatorSpec(
            name="bank.traces.aggregate",
            version="1",
            inputs=(PortSpec(name="telemetry", value_type="Telemetry"),),
            output_type="Aggregate",
            parameters={
                "table": ParameterSpec(
                    name="table",
                    value_type="string",
                    has_default=True,
                    default="bank/trace_span.csv",
                ),
                "window": ParameterSpec(
                    name="window",
                    value_type="string",
                    has_default=True,
                    default="alert",
                    choices=("reference", "alert"),
                ),
            },
            family="trace_latency",
            implementation=bank_traces_aggregate_impl,
            description="Aggregate Bank trace span durations by component",
        )
    )

    reg.register(
        OperatorSpec(
            name="bank.traces.latency_contrast",
            version="1",
            inputs=(
                PortSpec(name="reference", value_type="Aggregate"),
                PortSpec(name="alert", value_type="Aggregate"),
            ),
            output_type="ScoredCandidates",
            family="trace_latency",
            implementation=bank_traces_latency_contrast_impl,
            description="Score components by reference-to-alert trace latency shift",
        )
    )

    reg.register(
        OperatorSpec(
            name="bank.anomaly.scorer",
            version="1",
            inputs=(PortSpec(name="aggregate", value_type="Aggregate"),),
            output_type="ScoredCandidates",
            family="backend_metric",
            implementation=bank_anomaly_scorer_impl,
            description="Score components by highest aggregated alert anomaly",
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
                ),
                "limit": ParameterSpec(
                    name="limit",
                    value_type="integer",
                    has_default=True,
                    default=10,
                ),
            },
            family="consensus",
            implementation=weighted_consensus_impl,
            description="Synthesize weighted consensus root-cause ranking across analysis families",
        )
    )

    return reg


def get_bank_contrast_playbook() -> dict[str, Any]:
    """Canonical reference-alert metric contrast diagnosis DAG."""
    return {
        "name": "bank.starter.metric_contrast",
        "description": "Baseline 4-node reference-alert contrast diagnosis DAG for Bank metrics",
        "dag": {
            "nodes": [
                {
                    "id": "node_metric_ref",
                    "operator": "bank.metrics.aggregate",
                    "version": "1",
                    "inputs": {"telemetry": "telemetry"},
                    "params": {"table": "bank/metric_container.csv", "window": "reference"},
                },
                {
                    "id": "node_metric_alt",
                    "operator": "bank.metrics.aggregate",
                    "version": "1",
                    "inputs": {"telemetry": "telemetry"},
                    "params": {"table": "bank/metric_container.csv", "window": "alert"},
                },
                {
                    "id": "node_contrast",
                    "operator": "bank.metrics.contrast",
                    "version": "1",
                    "inputs": {
                        "reference": "node_metric_ref",
                        "alert": "node_metric_alt",
                    },
                },
                {
                    "id": "node_consensus",
                    "operator": "paper.answers.weighted_consensus",
                    "version": "1",
                    "inputs": {"scores": ["node_contrast"]},
                },
            ],
            "return_node": "node_consensus",
        },
    }


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
                    "params": {"table": "bank/metric_container.csv", "window": "alert"},
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
