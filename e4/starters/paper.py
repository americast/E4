"""Small native reconstruction of the paper's backend metric operator family.

The cited paper reports operator families and a workflow, not the exact source,
equations, or playbook DAGs reproduced here. All formulas and defaults below are
package implementation choices and are labelled as such in the shipped catalog.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from importlib.resources import files
from typing import Any, Callable, Mapping, Sequence

from ..controller.library import Playbook
from ..registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec
from ..sandbox.views import (
    make_candidate,
    make_candidate_set,
    make_evidence,
    make_scored_candidate,
    make_scored_candidates,
    telemetry_view,
)
from ..types import CandidateSet, DatasetSchema, EntityRef, MetricRecord, RankedAnswer, ScoredCandidates, SpanRecord, TableSchema
from .statistics import permutation_test, welch_mean_test


PAPER_REFERENCE = "Table 2, Agentic RCA for Internet-Scale Services Using Constrained Creativity (arXiv:2610.08622v1)"
_BACKEND_ROLE_NAMES = frozenset({
    "backend", "component", "container", "database", "db", "host", "machine",
    "microservice", "node", "pod", "service", "worker",
})
_NON_COMPONENT_ROLE_NAMES = frozenset({
    "app", "application", "client", "endpoint", "route", "session", "transaction",
    "url", "user",
})
_IMPLEMENTED = frozenset({
    "paper.backend.metric_conditions",
    "paper.backend.mean_welch",
    "paper.backend.location_permutation",
    "paper.backend.tail_permutation",
    "paper.scores.dependency_adjust",
    "paper.answers.weighted_consensus",
})


def paper_catalog() -> dict[str, Any]:
    """Return the complete shipped 19-operator/11-book reconstruction catalog."""
    path = files(__package__).joinpath("resources/paper_catalog.json")
    catalog = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(catalog, dict) or len(catalog.get("operators", ())) != 19 or len(catalog.get("playbooks", ())) != 11:
        raise RuntimeError("shipped paper catalog does not match its declared 19/11 inventory")
    return catalog


def _normal_role(role: str) -> str:
    return role.strip().casefold().replace("-", "_").replace(" ", "_")


def _rankable_role(role: str) -> bool:
    normalized = _normal_role(role)
    if normalized in _NON_COMPONENT_ROLE_NAMES:
        return False
    return normalized in _BACKEND_ROLE_NAMES


def _schema_tables(schema: DatasetSchema | None, record_type: str) -> list[Any]:
    if schema is None:
        return []
    return [table for table in schema.tables if table.record_type == record_type]


def _metric_table_rows(schema: DatasetSchema | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in _schema_tables(schema, "metric"):
        roles = sorted(role for role in table.entity_roles if _rankable_role(role))
        reasons: list[str] = []
        if not roles:
            reasons.append("schema has no explicit recognized backend/component entity role")
        rows.append({
            "name": table.name,
            "roles": roles,
            "available": not reasons,
            "reason": "; ".join(reasons) if reasons else "adapter declares canonical metric records and explicit rankable roles",
        })
    return rows


def _metric_tables(schema: DatasetSchema | None) -> list[dict[str, Any]]:
    return [row for row in _metric_table_rows(schema) if row["available"]]


def _trace_table_rows(schema: DatasetSchema | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in _schema_tables(schema, "span"):
        roles = sorted(role for role in table.entity_roles if _rankable_role(role))
        reasons: list[str] = []
        if not roles:
            reasons.append("schema has no explicit recognized backend/component entity role")
        rows.append({
            "name": table.name,
            "roles": roles,
            "available": not reasons,
            "reason": "; ".join(reasons) if reasons else (
                "adapter declares canonical span records and rankable roles; adjustment is conditional "
                "on exact trace-local parent edges being present at runtime"
            ),
        })
    return rows


def _parameter(
    name: str,
    value_type: str,
    default: Any = None,
    *,
    required: bool = False,
    has_default: bool = True,
    choices: Sequence[Any] = (),
    constraints: Mapping[str, Any] | None = None,
    description: str = "",
) -> ParameterSpec:
    return ParameterSpec(
        name=name,
        value_type=value_type,
        required=required,
        default=default,
        has_default=has_default,
        choices=tuple(choices),
        constraints=dict(constraints or {}),
        description=description,
    )


def _metric_parameters(*, statistical: bool = False, method: str = "welch") -> dict[str, ParameterSpec]:
    result = {
        "source_table": _parameter(
            "source_table", "string", required=True, has_default=False,
            description="Exact metric table selected from the validated dataset schema and bound by its playbook.",
        ),
        "metric_names": _parameter(
            "metric_names", "string_list", [],
            description="Optional exact metric-name filter; an empty list keeps each metric stream separate.",
        ),
        "eligible_roles": _parameter(
            "eligible_roles", "string_list", [],
            description="Optional component/backend roles; empty applies the documented conservative role policy.",
        ),
        "minimum_support": _parameter(
            "minimum_support", "integer", 2,
            constraints={"minimum": 1},
            description="Minimum finite raw metric observations in each window.",
        ),
        "bucket_ms": _parameter(
            "bucket_ms", "positive_integer", 60000,
            constraints={"minimum": 1},
            description="Fixed UTC time-block width; independent-block interpretation is a reconstruction assumption.",
        ),
    }
    if not statistical:
        result["minimum_absolute_change"] = _parameter(
            "minimum_absolute_change", "number", 0.0,
            constraints={"minimum": 0.0},
            description="Minimum alert-minus-reference mean magnitude in the metric's own declared unit.",
        )
    else:
        result.update({
            "minimum_blocks": _parameter(
                "minimum_blocks", "integer", 3,
                constraints={"minimum": 2},
                description="Minimum independent time blocks in each window.",
            ),
            "minimum_effect": _parameter(
                "minimum_effect", "number", 0.0,
                constraints={"minimum": 0.0},
                description="Minimum absolute effect in the metric's own declared unit.",
            ),
            "alpha": _parameter(
                "alpha", "number", 0.05,
                constraints={"minimum": 0.0, "maximum": 1.0},
                description="Two-sided significance cutoff; the test is an implementation reconstruction.",
            ),
        })
        if method != "welch":
            result["resamples"] = _parameter(
                "resamples", "positive_integer", 499,
                constraints={"minimum": 1, "maximum": 9999},
                description="Number of deterministic independent-block permutation draws.",
            )
            result["seed"] = _parameter(
                "seed", "integer", 0,
                description="Deterministic base seed for permutation draws.",
            )
        if method == "tail":
            result["quantile"] = _parameter(
                "quantile", "number", 0.95,
                constraints={"minimum": 0.0, "maximum": 1.0},
                description="Tail probability used for the separate quantile contrast.",
            )
    return result


def _selected_tables(telemetry: Any, params: Mapping[str, Any]) -> list[str]:
    source_table = params.get("source_table")
    if not isinstance(source_table, str) or not source_table:
        raise ValueError("source_table must be explicitly selected from the dataset schema")
    if source_table not in telemetry.source_tables:
        return []
    return [source_table]


def _allowed_roles(params: Mapping[str, Any]) -> set[str] | None:
    configured = params.get("eligible_roles", ())
    if isinstance(configured, (list, tuple)) and configured:
        return {_normal_role(role) for role in configured if isinstance(role, str)}
    return None


def _matches_role(role: str, params: Mapping[str, Any]) -> bool:
    allowed = _allowed_roles(params)
    if allowed is not None:
        return _normal_role(role) in allowed and _rankable_role(role)
    return _rankable_role(role)


def _table_complete(telemetry: Any, source_table: str) -> bool:
    truncated_tables = getattr(telemetry, "truncated_tables", set())
    if truncated_tables:
        return source_table not in truncated_tables
    return not bool(getattr(telemetry, "truncated", False))


def _metric_blocks(
    telemetry: Any,
    source_table: str,
    window: Any,
    *,
    bucket_ms: int,
    metric_names: set[str] | None = None,
) -> tuple[dict[tuple[str, str, str, str | None], dict[int, tuple[float, int]]], bool]:
    """Scan a typed table once into (entity, metric, unit, time-block) means."""
    totals: dict[tuple[str, str, str, str | None, int], list[float | int]] = {}
    for record in telemetry.iter_records(source_table, window.name):
        if not isinstance(record, MetricRecord) or record.source_table != source_table:
            continue
        if not window.start_ms_utc <= record.timestamp_ms_utc < window.end_ms_utc:
            continue
        if metric_names is not None and record.metric_name not in metric_names:
            continue
        if record.value is None or not math.isfinite(record.value):
            continue
        bucket = window.start_ms_utc + ((record.timestamp_ms_utc - window.start_ms_utc) // bucket_ms) * bucket_ms
        key = (record.entity.role, record.entity.identifier, record.metric_name, record.unit, bucket)
        aggregate = totals.setdefault(key, [0.0, 0])
        aggregate[0] = float(aggregate[0]) + record.value
        aggregate[1] = int(aggregate[1]) + 1

    grouped: dict[tuple[str, str, str, str | None], dict[int, tuple[float, int]]] = defaultdict(dict)
    for (role, identifier, metric, unit, bucket), (total, count) in totals.items():
        if count > 0:
            grouped[(role, identifier, metric, unit)][bucket] = (float(total) / int(count), int(count))
    return dict(grouped), _table_complete(telemetry, source_table)


def _window_means(blocks: Mapping[int, tuple[float, int]]) -> tuple[float | None, int]:
    total = 0.0
    support = 0
    for block_mean, count in blocks.values():
        total += block_mean * count
        support += count
    return (total / support if support else None), support


def _metric_candidate_operator(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> CandidateSet:
    telemetry = telemetry_view(inputs["telemetry"])
    source_tables = _selected_tables(telemetry, params)
    if not source_tables:
        return make_candidate_set([], source="paper.backend.metric_conditions")
    bucket_ms = int(params.get("bucket_ms", 60000))
    minimum_support = int(params.get("minimum_support", 2))
    minimum_change = float(params.get("minimum_absolute_change", 0.0))
    requested = params.get("metric_names", ())
    metric_names = {name for name in requested if isinstance(name, str)} if requested else None
    candidates: dict[tuple[str, str], list[str]] = defaultdict(list)

    for table in source_tables:
        reference, ref_complete = _metric_blocks(
            telemetry, table, telemetry.reference_window, bucket_ms=bucket_ms,
            metric_names=metric_names,
        )
        alert, alert_complete = _metric_blocks(
            telemetry, table, telemetry.alert_window, bucket_ms=bucket_ms,
            metric_names=metric_names,
        )
        if not ref_complete or not alert_complete:
            return make_candidate_set([], source="paper.backend.metric_conditions (abstained: incomplete coverage)")
        for key in sorted(set(reference) & set(alert), key=lambda row: (row[0], row[1], row[2], row[3] or "")):
            role, identifier, metric, unit = key
            if not _matches_role(role, params):
                continue
            ref_mean, ref_support = _window_means(reference[key])
            alt_mean, alt_support = _window_means(alert[key])
            if ref_mean is None or alt_mean is None or min(ref_support, alt_support) < minimum_support:
                continue
            delta = alt_mean - ref_mean
            if abs(delta) < minimum_change:
                continue
            rationale = f"{metric} [{unit or 'unit unspecified'}] alert-reference mean change={delta:.6g}"
            candidates[(role, identifier)].append(rationale)

    values = [
        make_candidate(EntityRef(role, identifier), "; ".join(sorted(reasons)))
        for (role, identifier), reasons in sorted(candidates.items())
    ]
    return make_candidate_set(values, source="paper.backend.metric_conditions")


def _stable_group_seed(seed: int, key: tuple[str, str, str, str | None]) -> int:
    token = "\0".join((str(seed), *key[:3], key[3] or ""))
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _scorer_operator(method: str, source_name: str) -> Callable[[Mapping[str, Any], Mapping[str, Any], Any], Any]:
    def score(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> Any:
        telemetry = telemetry_view(inputs["telemetry"])
        candidate_set = inputs["candidates"]
        candidate_keys = {
            (candidate.entity.role, candidate.entity.identifier)
            for candidate in candidate_set.candidates
        }
        if not candidate_keys:
            return make_scored_candidates([], scorer=source_name, status="abstained", status_details={"reason": "empty candidate set"})
        table_names = _selected_tables(telemetry, params)
        if not table_names:
            return make_scored_candidates([], scorer=source_name, status="abstained", status_details={"reason": "selected metric table is unavailable"})
        bucket_ms = int(params.get("bucket_ms", 60000))
        requested = params.get("metric_names", ())
        metric_names = {name for name in requested if isinstance(name, str)} if requested else None
        references: dict[tuple[str, str, str, str | None], dict[int, tuple[float, int]]] = {}
        alerts: dict[tuple[str, str, str, str | None], dict[int, tuple[float, int]]] = {}
        for table in table_names:
            ref_rows, ref_complete = _metric_blocks(
                telemetry, table, telemetry.reference_window, bucket_ms=bucket_ms,
                metric_names=metric_names,
            )
            alert_rows, alert_complete = _metric_blocks(
                telemetry, table, telemetry.alert_window, bucket_ms=bucket_ms,
                metric_names=metric_names,
            )
            if not ref_complete or not alert_complete:
                return make_scored_candidates([], scorer=source_name, status="abstained", status_details={"reason": "incomplete query coverage"})
            # A table is chosen explicitly. Keep (role, identifier, metric, unit)
            # streams separate; distinct metric units are never pooled.
            for key, blocks in ref_rows.items():
                references[key] = blocks
            for key, blocks in alert_rows.items():
                alerts[key] = blocks

        minimum_blocks = int(params.get("minimum_blocks", 3))
        minimum_support = int(params.get("minimum_support", 2))
        minimum_effect = float(params.get("minimum_effect", 0.0))
        alpha = float(params.get("alpha", 0.05))
        resamples = int(params.get("resamples", 499))
        seed = int(params.get("seed", 0))
        quantile_probability = float(params.get("quantile", 0.95))
        supported: dict[tuple[str, str], list[Any]] = defaultdict(list)
        scores: dict[tuple[str, str], float] = defaultdict(float)
        eligible_seen: set[tuple[str, str]] = set()
        observed = False

        for key in sorted(set(references) & set(alerts), key=lambda row: (row[0], row[1], row[2], row[3] or "")):
            role, identifier, metric, unit = key
            identity = (role, identifier)
            if identity not in candidate_keys or not _matches_role(role, params):
                continue
            ref_blocks = [value for _, (value, _) in sorted(references[key].items())]
            alt_blocks = [value for _, (value, _) in sorted(alerts[key].items())]
            if len(ref_blocks) < minimum_blocks or len(alt_blocks) < minimum_blocks:
                continue
            ref_support = sum(count for _, count in references[key].values())
            alert_support = sum(count for _, count in alerts[key].values())
            if min(ref_support, alert_support) < minimum_support:
                continue
            observed = True
            try:
                if method == "welch":
                    effect, p_value = welch_mean_test(ref_blocks, alt_blocks)
                    statistic = "welch_mean_difference"
                elif method == "location":
                    effect, p_value = permutation_test(
                        ref_blocks, alt_blocks, statistic="mean", quantile_probability=0.5,
                        resamples=resamples, seed=_stable_group_seed(seed, key),
                    )
                    statistic = "permutation_mean_difference"
                else:
                    effect, p_value = permutation_test(
                        ref_blocks, alt_blocks, statistic="quantile",
                        quantile_probability=quantile_probability,
                        resamples=resamples, seed=_stable_group_seed(seed, key),
                    )
                    statistic = f"permutation_quantile_{quantile_probability:g}_difference"
            except (ValueError, ArithmeticError):
                continue
            if not math.isfinite(effect) or not math.isfinite(p_value):
                continue
            entity = EntityRef(role, identifier)
            eligible = p_value <= alpha and abs(effect) >= minimum_effect
            evidence = make_evidence(
                source=source_name,
                window=telemetry.alert_window,
                statistic=f"{statistic}:{metric}",
                value=float(effect),
                contributing_entities=[entity],
                support=min(ref_support, alert_support),
                ranking_eligible=eligible,
                details={
                    "metric_name": metric,
                    "unit": unit,
                    "p_value": p_value,
                    "reference_blocks": len(ref_blocks),
                    "alert_blocks": len(alt_blocks),
                    "reference_support": ref_support,
                    "alert_support": alert_support,
                    "minimum_effect": minimum_effect,
                    "alpha": alpha,
                    "block_independence": "reconstruction assumption; not established by the adapter",
                },
            )
            supported[identity].append(evidence)
            if eligible:
                eligible_seen.add(identity)
                p_floor = max(p_value, 1e-300)
                scores[identity] = max(scores[identity], min(300.0, -math.log10(p_floor)))

        if not observed:
            return make_scored_candidates([], scorer=source_name, status="abstained", status_details={"reason": "insufficient supported independent blocks"})
        outputs = [
            make_scored_candidate(
                EntityRef(role, identifier),
                score,
                evidence=supported[(role, identifier)],
                analysis_family="paper.backend_metric",
            )
            for (role, identifier), score in sorted(scores.items())
            if (role, identifier) in eligible_seen and supported[(role, identifier)]
        ]
        return make_scored_candidates(outputs, scorer=source_name, status="ready" if outputs else "abstained")

    return score


def _eligible_evidence(candidate: Any) -> tuple[Any, ...]:
    return tuple(
        evidence for evidence in candidate.evidence
        if evidence.ranking_eligible and evidence.support > 0 and any(
            contributor.role == candidate.entity.role
            and contributor.identifier == candidate.entity.identifier
            for contributor in evidence.contributing_entities
        )
    )


def _dependency_adjust(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> Any:
    telemetry = telemetry_view(inputs["telemetry"])
    score_sets = tuple(inputs["scores"])
    flattened = [candidate for score_set in score_sets for candidate in score_set.candidates]
    if not flattened:
        return make_scored_candidates([], scorer="paper.scores.dependency_adjust", status="abstained", status_details={"reason": "no source candidates"})
    trace_table = params.get("trace_table")
    if not isinstance(trace_table, str) or trace_table not in telemetry.source_tables:
        return make_scored_candidates(flattened, scorer="paper.scores.dependency_adjust", status="ready")

    if not _table_complete(telemetry, trace_table):
        return make_scored_candidates(flattened, scorer="paper.scores.dependency_adjust", status="ready", status_details={"dependency_adjustment": "skipped due to incomplete span coverage"})

    spans: list[SpanRecord] = [
        record
        for record in telemetry.iter_records(trace_table, telemetry.alert_window.name)
        if isinstance(record, SpanRecord)
        and record.source_table == trace_table
        and telemetry.alert_window.start_ms_utc <= record.timestamp_ms_utc < telemetry.alert_window.end_ms_utc
    ]
    span_entities: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
    for span in spans:
        component = span.executing_component
        if component is not None and _rankable_role(component.role):
            span_entities[(span.trace_id, span.span_id)].add((component.role, component.identifier))
    edges: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    for span in spans:
        child_component = span.executing_component
        if not span.trace_id or not span.span_id or not span.parent_id or child_component is None:
            continue
        if not _rankable_role(child_component.role):
            continue
        child = (child_component.role, child_component.identifier)
        parents = span_entities.get((span.trace_id, span.parent_id), set())
        if len(parents) != 1:
            continue
        parent = next(iter(parents))
        if parent != child:
            edges.add((parent, child))

    if not edges:
        return make_scored_candidates(flattened, scorer="paper.scores.dependency_adjust", status="ready", status_details={"dependency_adjustment": "no unambiguous observed parent edges"})

    penalty = min(1.0, max(0.0, float(params.get("downstream_penalty", 0.5))))
    eligible_by_entity: dict[tuple[str, str], float] = defaultdict(float)
    for candidate in flattened:
        if _eligible_evidence(candidate):
            key = (candidate.entity.role, candidate.entity.identifier)
            eligible_by_entity[key] = max(eligible_by_entity[key], max(0.0, float(candidate.score)))
    max_score = max(eligible_by_entity.values(), default=0.0)
    normalized = {
        key: value / max_score if max_score > 0.0 else 0.0
        for key, value in eligible_by_entity.items()
    }
    children_by_parent: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for parent, child in edges:
        if child in normalized:
            children_by_parent[parent].append(child)
    adjusted = []
    for candidate in flattened:
        key = (candidate.entity.role, candidate.entity.identifier)
        child_score = max((normalized[child] for child in children_by_parent.get(key, ())), default=0.0)
        score = float(candidate.score)
        if key in normalized and child_score > 0.0:
            score = max(0.0, score * (1.0 - penalty * child_score))
        adjusted.append(make_scored_candidate(
            candidate.entity,
            score,
            category=candidate.category,
            evidence=candidate.evidence,
            analysis_family=candidate.analysis_family,
        ))
    return make_scored_candidates(
        adjusted,
        scorer="paper.scores.dependency_adjust",
        status="ready",
        status_details={
            "dependency_adjustment": "caller score reduced only for exact same-trace parent-to-callee edges with eligible child anomaly evidence",
            "reconstruction_formula": "parent score * (1 - downstream_penalty * normalized eligible child score)",
            "observed_edge_count": len(edges),
        },
    )


def _weighted_consensus(inputs: Mapping[str, Any], params: Mapping[str, Any], runtime: Any) -> Any:
    score_sets = tuple(inputs.get("scores", ()))
    if not score_sets:
        return RankedAnswer((), stage="paper_consensus")
    family_weights = params.get("family_weights", {})
    if not isinstance(family_weights, Mapping):
        raise ValueError("family_weights must map family names to finite nonnegative numbers")
    weights: dict[str, float] = {}
    for family, weight in family_weights.items():
        if not isinstance(family, str) or type(weight) not in (int, float) or not math.isfinite(float(weight)) or float(weight) < 0:
            raise ValueError("family_weights must map family names to finite nonnegative numbers")
        weights[family] = float(weight)

    # First normalize each scorer's scale using only its eligible evidence. Multiple methods in the same family
    # are averaged within that family, so they do not silently receive multiple
    # family-level votes. Evidence objects pass through without rewriting.
    by_identity_family_scorer: dict[tuple[str, str], dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    evidence_by_identity: dict[tuple[str, str], list[Any]] = defaultdict(list)
    entity_by_identity: dict[tuple[str, str], EntityRef] = {}
    for score_set in score_sets:
        if score_set.status != "ready":
            continue
        eligible_candidates = [
            (candidate, _eligible_evidence(candidate))
            for candidate in score_set.candidates
            if math.isfinite(float(candidate.score)) and float(candidate.score) > 0.0
        ]
        eligible_candidates = [(candidate, evidence) for candidate, evidence in eligible_candidates if evidence]
        finite_scores = [float(candidate.score) for candidate, _ in eligible_candidates]
        max_score = max(finite_scores, default=0.0)
        if max_score <= 0.0:
            continue
        for candidate, eligible_evidence in eligible_candidates:
            raw_score = float(candidate.score)
            identity = (candidate.entity.role, candidate.entity.identifier)
            family = candidate.analysis_family or score_set.scorer
            normalized_score = max(0.0, raw_score) / max_score
            prior = by_identity_family_scorer[identity][family].get(score_set.scorer, 0.0)
            by_identity_family_scorer[identity][family][score_set.scorer] = max(prior, normalized_score)
            entity_by_identity[identity] = candidate.entity
            evidence_by_identity[identity].extend(eligible_evidence)

    family_votes: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for identity, families in by_identity_family_scorer.items():
        for family, votes in families.items():
            family_votes[identity][family] = sum(votes.values()) / len(votes)

    ranked: list[tuple[EntityRef, float, tuple[Any, ...]]] = []
    all_families = {family for families in family_votes.values() for family in families}
    fixed_denominator = sum(weights.get(family, 1.0) for family in all_families)
    if fixed_denominator <= 0.0:
        return RankedAnswer((), stage="paper_consensus")
    for identity, families in family_votes.items():
        evidence = tuple(evidence_by_identity[identity])
        if not evidence:
            continue
        score = sum(value * weights.get(family, 1.0) for family, value in families.items()) / fixed_denominator
        if not math.isfinite(score) or score <= 0.0:
            continue
        # Preserve eligible evidence exactly, deduplicating only equal records.
        unique_evidence_list: list[Any] = []
        for item in evidence:
            if item not in unique_evidence_list:
                unique_evidence_list.append(item)
        unique_evidence = tuple(unique_evidence_list)
        ranked.append((entity_by_identity[identity], score, unique_evidence))
    ranked.sort(key=lambda row: (-row[1], row[0].role, row[0].identifier))
    if not ranked:
        return RankedAnswer((), stage="paper_consensus")

    all_evidence_list: list[Any] = []
    for _, _, rows in ranked:
        for evidence in rows:
            if evidence not in all_evidence_list:
                all_evidence_list.append(evidence)
    all_evidence = tuple(all_evidence_list)
    return RankedAnswer(
        candidates=tuple(entity for entity, _, _ in ranked),
        scores=tuple(score for _, score, _ in ranked),
        stage="paper_consensus",
        evidence=all_evidence,
    )


def _operator_specs() -> dict[str, OperatorSpec]:
    specs: dict[str, OperatorSpec] = {}
    metric_port = (PortSpec("telemetry", "Telemetry"),)
    candidate_port = (PortSpec("candidates", "CandidateSet"), PortSpec("telemetry", "Telemetry"))
    for name, description, implementation, output_type, inputs, parameters in (
        (
            "paper.backend.metric_conditions",
            "Paper-family reconstruction: generate observed backend entities from full-window metric contrasts. Metric names and native units remain separate; the formula is not attributed as the paper's exact equation.",
            _metric_candidate_operator,
            "CandidateSet",
            metric_port,
            _metric_parameters(),
        ),
        (
            "paper.backend.mean_welch",
            "Paper-family reconstruction: Welch test over fixed-width independent time-block metric means.",
            _scorer_operator("welch", "paper.backend.mean_welch"),
            "ScoredCandidates",
            candidate_port,
            _metric_parameters(statistical=True),
        ),
        (
            "paper.backend.location_permutation",
            "Paper-family reconstruction: two-sided independent-block permutation test for mean-location change.",
            _scorer_operator("location", "paper.backend.location_permutation"),
            "ScoredCandidates",
            candidate_port,
            _metric_parameters(statistical=True, method="location"),
        ),
        (
            "paper.backend.tail_permutation",
            "Paper-family reconstruction: two-sided independent-block permutation test for a selected metric quantile change.",
            _scorer_operator("tail", "paper.backend.tail_permutation"),
            "ScoredCandidates",
            candidate_port,
            _metric_parameters(statistical=True, method="tail"),
        ),
    ):
        specs[name] = OperatorSpec(
            name=name,
            version="1",
            inputs=inputs,
            output_type=output_type,
            parameters=parameters,
            family="paper_backend_metric",
            implementation=implementation,
            description=f"{description} Source family reference: {PAPER_REFERENCE}. Exact formula/code is unpublished.",
        )

    dependency_parameters = {
        "trace_table": _parameter(
            "trace_table", "string", required=True, has_default=False,
            description="Exact span table with declared trace_id/span_id/parent_id and component roles.",
        ),
        "downstream_penalty": _parameter(
            "downstream_penalty", "number", 0.5,
            constraints={"minimum": 0.0, "maximum": 1.0},
            description="Reconstruction weight for supported downstream anomaly evidence.",
        ),
    }
    specs["paper.scores.dependency_adjust"] = OperatorSpec(
        name="paper.scores.dependency_adjust",
        version="1",
        inputs=(
            PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),
            PortSpec("telemetry", "Telemetry"),
        ),
        output_type="ScoredCandidates",
        parameters=dependency_parameters,
        family="paper_dependency_adjustment",
        implementation=_dependency_adjust,
        description=(
            "Conditional paper-family reconstruction over exact same-trace parent_span_id links. "
            "Uses observed caller→callee direction and eligible candidate evidence; no inferred topology. "
            f"Source family reference: {PAPER_REFERENCE}."
        ),
    )
    specs["paper.answers.weighted_consensus"] = OperatorSpec(
        name="paper.answers.weighted_consensus",
        version="1",
        inputs=(PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),),
        output_type="RankedAnswer",
        parameters={
            "family_weights": _parameter(
                "family_weights", "object", {"paper.backend_metric": 1.0},
                description="JSON mapping of analysis_family to finite nonnegative family weight.",
            ),
        },
        family="paper_weighted_consensus",
        implementation=_weighted_consensus,
        description=(
            "Reconstructed family-weight consensus: normalize each scorer, average votes within each "
            "analysis family, apply explicit family weights, and retain eligible evidence unchanged. "
            f"Source family reference: {PAPER_REFERENCE}; exact original formula is unpublished."
        ),
    )
    return specs


def build_paper_registry(schema: DatasetSchema | None = None) -> OperatorRegistry:
    """Build schema-compatible native paper-family operators; no legacy imports."""
    all_specs = _operator_specs()
    registry = OperatorRegistry()
    if schema is None:
        names = _IMPLEMENTED
    else:
        metric_available = bool(_metric_tables(schema))
        trace_available = bool([row for row in _trace_table_rows(schema) if row["available"]])
        names = {"paper.answers.weighted_consensus"}
        if metric_available:
            names.update({
                "paper.backend.metric_conditions",
                "paper.backend.mean_welch",
                "paper.backend.location_permutation",
                "paper.backend.tail_permutation",
            })
        if metric_available and trace_available:
            names.add("paper.scores.dependency_adjust")
    for name in sorted(names):
        registry.register(all_specs[name])
    return registry


def _source_ref(book_id: str) -> dict[str, str]:
    return {"paper": PAPER_REFERENCE, "table": "Table 2", "reconstruction_id": book_id}


def _playbook_doc(
    *,
    book_id: str,
    name: str,
    description: str,
    source_table: str,
    roles: Sequence[str],
    scorer_name: str,
    additional_scorers: Sequence[str] = (),
    trace_table: str | None = None,
    include_dependency: bool = False,
) -> Playbook:
    nodes: list[dict[str, Any]] = [
        {
            "id": "candidate",
            "operator": "paper.backend.metric_conditions",
            "version": "1",
            "inputs": {"telemetry": "telemetry"},
            "params": {"source_table": source_table, "eligible_roles": list(roles)},
        },
    ]
    score_node_ids: list[str] = []
    for index, operator_name in enumerate((scorer_name, *additional_scorers)):
        node_id = f"scores_{index}"
        score_node_ids.append(node_id)
        nodes.append({
            "id": node_id,
            "operator": operator_name,
            "version": "1",
            "inputs": {"candidates": "candidate", "telemetry": "telemetry"},
            "params": {"source_table": source_table, "eligible_roles": list(roles)},
        })
    answer_inputs = score_node_ids
    if include_dependency and trace_table:
        nodes.append({
            "id": "dependency_adjusted",
            "operator": "paper.scores.dependency_adjust",
            "version": "1",
            "inputs": {"scores": score_node_ids, "telemetry": "telemetry"},
            "params": {"trace_table": trace_table},
        })
        answer_inputs = ["dependency_adjusted"]
    dag = {
        "schema_version": "e4-dag/1",
        "nodes": nodes + [{
            "id": "answer",
            "operator": "paper.answers.weighted_consensus",
            "version": "1",
            "inputs": {"scores": answer_inputs},
            "params": {},
        }],
        "return_node": "answer",
    }
    return Playbook(
        name=name,
        description=description,
        dag=dag,
        metadata={
            "source": "reconstructed-paper-starter-v1",
            "reconstruction_id": book_id,
            "paper_reference": _source_ref(book_id),
            "fidelity": "Package implementation reconstruction; exact paper DAG/equation is not published.",
            "bound_source_table": source_table,
            "bound_entity_roles": list(roles),
        },
    )


def get_paper_playbooks(schema: DatasetSchema | None = None) -> list[Playbook]:
    """Return only table-bound books whose typed requirements the schema supports."""
    if schema is None:
        return []
    metric_tables = _metric_tables(schema)
    trace_tables = [row for row in _trace_table_rows(schema) if row["available"]]
    playbooks: list[Playbook] = []
    for metric_table in metric_tables:
        for book_id, base_name, description, scorer_name in (
            ("B4", "backend_mean_shift", "Welch comparison of independent time-block metric means.", "paper.backend.mean_welch"),
            ("B5", "backend_location_shift", "Independent-block permutation test for a metric mean-location change.", "paper.backend.location_permutation"),
            ("B6", "backend_tail_degradation", "Independent-block permutation test for a separately declared metric quantile change.", "paper.backend.tail_permutation"),
        ):
            table_suffix = "" if len(metric_tables) == 1 else "__" + _safe_node_suffix(metric_table["name"])
            playbooks.append(_playbook_doc(
                book_id=book_id,
                name=base_name + table_suffix,
                description=f"{description} Reconstructed package semantics; bound to the compatible table {metric_table['name']!r}.",
                source_table=metric_table["name"],
                roles=metric_table["roles"],
                scorer_name=scorer_name,
            ))
    if metric_tables and trace_tables:
        metric_table = metric_tables[0]
        trace_table = trace_tables[0]
        playbooks.append(_playbook_doc(
            book_id="B11",
            name="dependency_adjusted_backend_consensus",
            description=(
                "Reconstructed three-scorer backend composition with conditional exact trace-local "
                f"dependency adjustment using {trace_table['name']!r}; metric table {metric_table['name']!r}."
            ),
            source_table=metric_table["name"],
            roles=metric_table["roles"],
            scorer_name="paper.backend.mean_welch",
            trace_table=trace_table["name"],
            additional_scorers=(
                "paper.backend.location_permutation",
                "paper.backend.tail_permutation",
            ),
            include_dependency=True,
        ))
    return playbooks


def _safe_node_suffix(value: str) -> str:
    normalized = "".join(character.lower() if character.isalnum() else "_" for character in value)
    return "_".join(token for token in normalized.split("_") if token) or "table"


def paper_availability(schema: DatasetSchema | None) -> dict[str, Any]:
    """Return the full inventory plus implementation and schema-availability reasons."""
    catalog = paper_catalog()
    metric_rows = _metric_table_rows(schema)
    metric_tables = [row for row in metric_rows if row["available"]]
    trace_rows = _trace_table_rows(schema)
    trace_tables = [row for row in trace_rows if row["available"]]
    operator_rows: list[dict[str, Any]] = []
    for raw in catalog["operators"]:
        row = dict(raw)
        name = row["name"]
        implementation = row.get("implementation", {})
        status = implementation.get("status", "not_implemented")
        active = status == "implemented_generic"
        reason = "implemented generic native operator; schema filtering has not been requested"
        if name.startswith("paper.backend."):
            if schema is None:
                reason = "generic implementation is available; bind an adapter-validated metric table and rankable roles at runtime"
            else:
                active = active and bool(metric_tables)
                reason = "compatible metric table available" if active else (
                    "no metric table in schema" if not metric_rows else "; ".join(sorted({item["reason"] for item in metric_rows}))
                )
        elif name == "paper.scores.dependency_adjust":
            if schema is None:
                reason = "generic implementation is available; adjustment requires observed exact trace-local parent edges"
            else:
                active = active and bool(trace_tables)
                reason = "conditional: typed span table available; adjustment occurs only for exact observed trace-local parent edges" if active else (
                    "no span table in schema" if not trace_rows else "; ".join(sorted({item["reason"] for item in trace_rows}))
                )
        elif name == "paper.answers.weighted_consensus":
            reason = "registered generic native consensus; output is the supported RankedAnswer type"
        else:
            reason = implementation.get("reason", "operator family is not implemented in this package")
        row["availability"] = "available" if active else ("dataset_unavailable" if status == "implemented_generic" else "not_implemented")
        row["active"] = active
        row["availability_reason"] = reason
        operator_rows.append(row)

    compatible_books = get_paper_playbooks(schema)
    compatible_names = {playbook.name for playbook in compatible_books}
    book_rows: list[dict[str, Any]] = []
    for raw in catalog["playbooks"]:
        row = dict(raw)
        status = row.get("implementation_status", "not_implemented")
        active = status == "implemented_reconstruction" and (
            schema is None or row["name"] in compatible_names
        )
        if status != "implemented_reconstruction":
            reason = row.get("gap_reason", "required operator family has no current canonical typed data contract")
            availability = "not_implemented"
        elif schema is None:
            reason = "schema required to bind a source table and explicit rankable entity roles"
            availability = "dataset_unavailable"
        elif row["name"] == "dependency_adjusted_backend_consensus" and active:
            reason = "available conditionally: adjustment occurs only when runtime data contains exact observed trace-local parent edges"
            availability = "available"
        elif active:
            reason = "all bound source requirements are present in the supplied schema"
            availability = "available"
        elif row["name"] == "dependency_adjusted_backend_consensus":
            reason = "requires compatible typed span records; runtime dependency adjustment is conditional on observed exact trace-local parent edges"
            availability = "dataset_unavailable"
        else:
            reason = "requires a metric table with a supported metric field and explicit recognized backend/component roles"
            availability = "dataset_unavailable"
        row["availability"] = availability
        row["active"] = active
        row["availability_reason"] = reason
        book_rows.append(row)
    return {
        "operators": operator_rows,
        "playbooks": book_rows,
        "counts": dict(catalog.get("counts", {})),
        "schema": schema.as_dict() if schema is not None else None,
        "active_operator_names": sorted(row["name"] for row in operator_rows if row["active"]),
        "active_playbook_names": sorted(playbook.name for playbook in compatible_books),
        "metric_tables": metric_rows,
        "trace_tables": trace_rows,
        "formula_disclosure": catalog.get("formula_disclosure", "Exact formulas and DAGs are not published."),
    }


def paper_reference_examples() -> list[dict[str, Any]]:
    """Return compact operator and book examples; none are callable from this result."""
    catalog = paper_catalog()
    specs = _operator_specs()
    names = {
        "paper.backend.metric_conditions",
        "paper.backend.mean_welch",
        "paper.backend.location_permutation",
        "paper.backend.tail_permutation",
        "paper.scores.dependency_adjust",
        "paper.answers.weighted_consensus",
    }
    operator_examples = [
        {
            "kind": "operator",
            "name": row["name"],
            "category": row["category"],
            "description": row["purpose"],
            "semantics": row["implementation"].get(
                "semantics",
                row["implementation"].get("reason", "The family is catalogued but not implemented."),
            ),
            "implementation_status": row["implementation"].get("status", "not_implemented"),
            "inputs": [port.as_dict() for port in specs[row["name"]].inputs],
            "output_type": specs[row["name"]].output_type,
            "parameters": {
                name: parameter.as_dict()
                for name, parameter in specs[row["name"]].parameters.items()
            },
            "reference_only": True,
            "active_registration": False,
            "paper_reference": {**catalog["source"], "location": row.get("reference_location", "Table 2")},
            "fidelity_note": "Illustrative reconstructed family; examples do not add a callable registry entry.",
        }
        for row in catalog["operators"]
        if row["name"] in names
    ]
    example_schema = DatasetSchema((
        TableSchema(
            "<validated metric table>",
            "metric",
            (),
            entity_roles=("service",),
            description="Illustrative placeholder; bind only to an adapter-validated metric table.",
        ),
    ))
    books = get_paper_playbooks(example_schema)
    selected_book_ids = {"B4", "B6"}
    book_examples = [
        {
            "kind": "playbook",
            "name": book.name,
            "description": book.description,
            "reconstruction_id": book.metadata["reconstruction_id"],
            "dag": dict(book.dag),
            "reference_only": True,
            "active_registration": False,
            "schema_table_names_illustrative": True,
            "rebind_to_validated_schema": True,
            "paper_reference": {**catalog["source"], "location": "Table 2"},
            "fidelity_note": "Illustrative reconstructed composition; this is not the paper's unpublished original DAG.",
        }
        for book in books
        if book.metadata["reconstruction_id"] in selected_book_ids
    ]
    return operator_examples + book_examples

