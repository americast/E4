"""Bounded, label-free diagnostics for Stage 3 generated operators.

This module observes existing execution records and typed cache entries only. Its
counterfactual is restricted to direct ScoredCandidates inputs of an explicitly
trusted profile terminal; it never executes generated operators or queries telemetry.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from .cache import CacheStore, canonical_json
from .contracts import (Aggregate, CandidateSet, EntityRef, Evidence, RankedAnswer,
                        ScoredCandidate, ScoredCandidates)
from .dag import VerifiedDAG, VerifiedNode
from .executor import ExecutionFailure, NodeContext, OperatorResult
from .registry import OperatorRegistry, OperatorSpec
from .runtime import RuntimeContext, _jsonable

SCHEMA_VERSION = "e4-evolution-contribution-diagnostics/1"
TRUSTED_TERMINAL_NAMES = frozenset({
    "bank.answers.combine",
    "paper.answers.weighted_consensus",
})
MAX_NEW_OPERATORS = 3
MAX_DAG_NODES = 40
MAX_QUERY_AUDIT_ROWS = 32
MAX_OUTPUT_CANDIDATES = 10_000
MAX_OUTPUT_EVIDENCE = 20_000
MAX_OUTPUT_ROWS = 10_000
MAX_CACHE_ENTRY_BYTES = 16 * 1024 * 1024
MAX_TOTAL_CACHE_READ_BYTES = 64 * 1024 * 1024
MAX_COUNTERFACTUAL_CANDIDATES = 20_000
MAX_COUNTERFACTUAL_EVIDENCE = 100_000
_ALLOWED_EVIDENCE_SOURCES = frozenset({
    "bank/metric_app.csv", "bank/metric_container.csv", "bank/trace_span.csv",
    "bank/log/log_service.csv", "bank/metric/metric_app.csv",
    "bank/metric/metric_container.csv", "bank/trace/trace_span.csv",
})


def _trusted_terminal(node: VerifiedNode | None) -> bool:
    return (node is not None and node.operator.name in TRUSTED_TERMINAL_NAMES and
            node.operator.output_type == "RankedAnswer")


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _qualified(name: str, version: str) -> str:
    return f"{name}@{version}"


def _capabilities(gap_record: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(gap_record, Mapping):
        return {}
    records = gap_record.get("capabilities", gap_record.get("operators", []))
    result = {}
    if isinstance(records, list):
        for row in records[:MAX_NEW_OPERATORS]:
            if not isinstance(row, Mapping):
                continue
            name = row.get("operator_name", row.get("name"))
            version = row.get("operator_version", row.get("version"))
            if isinstance(name, str) and isinstance(version, str):
                result[_qualified(name, version)] = _plain(row)
    return result


def _node_spec_by_id(nodes: Sequence[VerifiedNode]) -> dict[str, VerifiedNode]:
    return {node.node_id: node for node in nodes}


def _successors(nodes: Sequence[VerifiedNode]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {node.node_id: [] for node in nodes}
    for node in nodes:
        for refs in node.inputs.values():
            for ref in refs:
                result.setdefault(ref, []).append(node.node_id)
    return result


def _path(start: str, end: str, successors: Mapping[str, Sequence[str]]) -> list[str] | None:
    stack: list[tuple[str, list[str]]] = [(start, [start])]
    seen: set[str] = set()
    while stack:
        current, path = stack.pop()
        if current == end:
            return path
        if current in seen:
            continue
        seen.add(current)
        for child in reversed(successors.get(current, ())):
            if child not in seen:
                stack.append((child, [*path, child]))
    return None


def _record_map(execution: Any, failure: ExecutionFailure | None) -> dict[str, Any]:
    records = getattr(execution, "records", ()) if execution is not None else ()
    if failure is not None:
        records = failure.records
    return {record.node_id: record for record in records}


def _failure_summary(failure: ExecutionFailure | None) -> dict[str, Any] | None:
    if failure is None:
        return None
    rows = []
    for record in failure.records[:MAX_DAG_NODES]:
        rows.append({"node_id": record.node_id, "operator": record.operator,
            "version": record.version, "status": record.status,
            "elapsed_ms": record.elapsed_ms, "cache_status": record.cache_status,
            "output_digest": record.output_digest,
            "error_code": record.error.get("code") if record.error else None,
            "error_type": record.error.get("exception_type") if record.error else None})
    return {"code": failure.error.get("code"), "node_id": failure.error.get("node_id"),
        "message": str(failure)[:500], "records": rows,
        "records_omitted": max(0, len(failure.records) - len(rows))}


def _read_output(record: Any, output_type: str, cache: CacheStore,
                 remaining_bytes: int) -> tuple[Any | None, str, int]:
    if record is None:
        return None, "node_record_unavailable", 0
    if record.status != "success":
        return None, "node_execution_failed", 0
    if not record.cache_key:
        return None, "cache_key_unavailable", 0
    cache_path = cache.root / f"{record.cache_key}.json"
    try:
        cache_size = cache_path.stat().st_size
    except OSError:
        return None, "cache_miss", 0
    if cache_size > MAX_CACHE_ENTRY_BYTES:
        return None, "cache_entry_over_read_cap", 0
    if cache_size > remaining_bytes:
        return None, "cache_total_read_cap_reached", 0
    try:
        value, digest, status = cache.get(record.cache_key, output_type)
    except Exception as exc:
        return None, f"cache_read_error:{type(exc).__name__}", cache_size
    if value is None:
        return None, f"cache_{status}", cache_size
    if not record.output_digest or digest != record.output_digest:
        return None, "cache_record_digest_mismatch", cache_size
    return value, "available_from_typed_cache", cache_size


def _eligible_evidence(evidence: Evidence, entity: EntityRef) -> bool:
    return (evidence.details.get("ranking_eligible") is True and evidence.support > 0 and
            evidence.source in _ALLOWED_EVIDENCE_SOURCES and entity in evidence.contributing_entities)


def _scored_metrics(value: ScoredCandidates) -> dict[str, Any]:
    candidates = value.candidates[:MAX_OUTPUT_CANDIDATES]
    eligible_ids = set()
    positive_eligible_ids = set()
    eligible_evidence_count = 0
    sourced_attributed_evidence_count = 0
    support_by_unit: dict[str, dict[str, int]] = {}
    evidence_summarized = 0
    evidence_total = sum(len(candidate.evidence) for candidate in value.candidates)
    for candidate in candidates:
        remaining = max(0, MAX_OUTPUT_EVIDENCE - evidence_summarized)
        evidence = candidate.evidence[:remaining]
        evidence_summarized += len(evidence)
        eligible = [_eligible_evidence(item, candidate.entity) for item in evidence]
        eligible_evidence_count += sum(item.details.get("ranking_eligible") is True for item in evidence)
        sourced_attributed_evidence_count += sum(eligible)
        if any(eligible):
            eligible_ids.add(candidate.entity.canonical_id)
            if candidate.score > 0:
                positive_eligible_ids.add(candidate.entity.canonical_id)
        for item in evidence:
            if _eligible_evidence(item, candidate.entity):
                unit = item.details.get("support_unit")
                unit_key = unit if isinstance(unit, str) and unit else "unspecified"
                bucket = support_by_unit.setdefault(unit_key, {"evidence_count": 0, "support_sum": 0})
                bucket["evidence_count"] += 1
                bucket["support_sum"] += item.support
    return {"status": value.status, "candidate_count": len(value.candidates),
        "candidate_count_summarized": len(candidates),
        "candidate_counts_truncated": len(value.candidates) > MAX_OUTPUT_CANDIDATES,
        "evidence_item_count": evidence_total,
        "evidence_items_summarized": evidence_summarized,
        "evidence_summary_truncated": evidence_total > evidence_summarized,
        "positive_score_count": sum(candidate.score > 0 for candidate in candidates),
        "ranking_eligible_evidence_count": eligible_evidence_count,
        "supported_sourced_attributed_evidence_count": sourced_attributed_evidence_count,
        "eligible_component_count": len(eligible_ids),
        "positive_eligible_component_count": len(positive_eligible_ids),
        "eligible_support_by_unit": support_by_unit,
        "eligible_component_ids_internal": eligible_ids,
        "positive_eligible_component_ids_internal": positive_eligible_ids}


def _output_summary(value: Any) -> dict[str, Any]:
    if isinstance(value, Aggregate):
        metadata = value.metadata
        query = metadata.get("public_query") if isinstance(metadata, Mapping) else None
        coverage = query.get("coverage", {}) if isinstance(query, Mapping) else {}
        safe_coverage = {key: coverage[key] for key in (
            "total_matching_rows", "returned_rows", "source_files", "source_rows_scanned",
            "source_bytes_scanned", "source_bytes_hashed", "returned_bytes",
            "omitted_before_rows", "remaining_after_rows", "truncated", "page_complete", "complete",
            "next_offset")
            if key in coverage}
        projection = metadata.get("projection") if isinstance(metadata, Mapping) else None
        projection_summary = None
        if isinstance(projection, Mapping):
            projection_summary = {key: projection[key] for key in (
                "source_row_count", "total_matching_row_count", "selected_row_count",
                "omitted_before", "remaining_after", "complete", "offset", "limit", "next_offset")
                if key in projection}
        rows = value.rows[:MAX_OUTPUT_ROWS]
        return {"output_type": "Aggregate", "aggregate_row_count": len(value.rows),
            "aggregate_rows_summarized": len(rows),
            "aggregate_rows_truncated_in_summary": len(value.rows) > len(rows),
            "source_table_count": len(value.source_tables),
            "supported_row_count_summarized": sum(row.support > 0 for row in rows),
            "row_source_attribution_count_summarized": sum(bool(row.source_tables) and bool(row.contributing_entities)
                for row in rows),
            "public_query_coverage": safe_coverage or None,
            "projection_coverage": projection_summary,
            "counterfactual_not_evaluated": True,
            "counterfactual_reason": "generated Aggregate requires downstream execution to measure ranking effect"}
    if isinstance(value, CandidateSet):
        return {"output_type": "CandidateSet", "candidate_count": len(value.candidates),
            "counterfactual_not_evaluated": True,
            "counterfactual_reason": "generated CandidateSet requires downstream execution to measure ranking effect"}
    if isinstance(value, ScoredCandidates):
        return {"output_type": "ScoredCandidates", **_scored_metrics(value)}
    return {"output_type": type(value).__name__, "summary_unavailable": True}


def _answer_summary(answer: RankedAnswer | None) -> dict[str, Any] | None:
    if answer is None:
        return None
    leads = answer.leads[:MAX_OUTPUT_CANDIDATES]
    eligible = {candidate.entity.canonical_id for candidate in leads
                if any(_eligible_evidence(item, candidate.entity) for item in candidate.evidence)}
    top3 = {candidate.entity.canonical_id for candidate in leads[:3]}
    top5 = {candidate.entity.canonical_id for candidate in leads[:5]}
    return {"lead_count": len(answer.leads), "leads_summarized": len(leads),
        "lead_counts_truncated": len(answer.leads) > MAX_OUTPUT_CANDIDATES,
        "eligible_evidence_lead_count": len(eligible),
        "top3_eligible_evidence_lead_count": len(top3 & eligible),
        "top5_eligible_evidence_lead_count": len(top5 & eligible),
        "top3_score_sum": sum(candidate.score for candidate in leads[:3]),
        "top5_score_sum": sum(candidate.score for candidate in leads[:5]),
        "answer_sha256": _hash([{"entity": candidate.entity.identifier,
            "role": candidate.entity.role, "score": candidate.score,
            "evidence_count": len(candidate.evidence)} for candidate in leads])}


def _evidence_fingerprint(evidence: Evidence) -> str:
    return _hash({"source": evidence.source, "window": evidence.window.name,
        "statistic": evidence.statistic, "value": evidence.value,
        "support": evidence.support, "details": _plain(evidence.details)})


def _answer_delta(before: RankedAnswer, after: RankedAnswer) -> dict[str, Any]:
    before_ids = [candidate.entity.canonical_id for candidate in before.leads]
    after_ids = [candidate.entity.canonical_id for candidate in after.leads]
    b3, a3, b5, a5 = set(before_ids[:3]), set(after_ids[:3]), set(before_ids[:5]), set(after_ids[:5])
    b_scores = {candidate.entity.canonical_id: candidate.score for candidate in before.leads}
    a_scores = {candidate.entity.canonical_id: candidate.score for candidate in after.leads}
    common = set(b_scores) & set(a_scores)
    before_evidence_items = [item for lead in before.leads for item in lead.evidence]
    after_evidence_items = [item for lead in after.leads for item in lead.evidence]
    before_evidence = Counter(_evidence_fingerprint(item) for item in before_evidence_items[:MAX_OUTPUT_CANDIDATES])
    after_evidence = Counter(_evidence_fingerprint(item) for item in after_evidence_items[:MAX_OUTPUT_CANDIDATES])
    removed_evidence = sum((before_evidence - after_evidence).values())
    added_evidence = sum((after_evidence - before_evidence).values())
    before_eligible = sum(_eligible_evidence(item, lead.entity)
        for lead in before.leads for item in lead.evidence[:MAX_OUTPUT_CANDIDATES])
    after_eligible = sum(_eligible_evidence(item, lead.entity)
        for lead in after.leads for item in lead.evidence[:MAX_OUTPUT_CANDIDATES])
    return {"answer_changed": (before_ids != after_ids or
            any(b_scores[key] != a_scores[key] for key in common) or
            removed_evidence > 0 or added_evidence > 0),
        "top3_order_changed": before_ids[:3] != after_ids[:3],
        "top5_order_changed": before_ids[:5] != after_ids[:5],
        "top3_added_count": len(a3 - b3), "top3_removed_count": len(b3 - a3),
        "top3_retained_count": len(a3 & b3), "top5_added_count": len(a5 - b5),
        "top5_removed_count": len(b5 - a5), "top5_retained_count": len(a5 & b5),
        "shared_lead_score_changed_count": sum(b_scores[key] != a_scores[key] for key in common),
        "shared_lead_max_absolute_score_delta": max((abs(b_scores[key] - a_scores[key]) for key in common), default=0.0),
        "eligible_evidence_count_before_summarized": before_eligible,
        "eligible_evidence_count_after_summarized": after_eligible,
        "evidence_items_removed_summarized": removed_evidence,
        "evidence_items_added_summarized": added_evidence,
        "evidence_comparison_truncated": (len(before_evidence_items) > MAX_OUTPUT_CANDIDATES or
                                           len(after_evidence_items) > MAX_OUTPUT_CANDIDATES)}


def _query_coverage(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "audit/public_query_calls.jsonl"
    if not path.is_file():
        return {"status": "unavailable", "reason": "public_query_calls.jsonl not present"}
    rows = []
    lines = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            if not line.strip():
                continue
            row = json.loads(line)
            rows.append({key: row.get(key) for key in ("node_id", "table", "window", "offset", "limit",
                "status", "rows", "returned_bytes", "truncated", "next_offset")})
            if len(rows) >= MAX_QUERY_AUDIT_ROWS:
                break
    except Exception as exc:
        return {"status": "unavailable", "reason": f"audit_read_error:{type(exc).__name__}"}
    return {"status": "available", "scope": "case_cumulative",
        "attribution": "calls may include prior stages and are not attributed to a generated operator by node_id",
        "calls_summarized": len(rows),
        "calls_omitted": max(0, sum(bool(line.strip()) for line in lines) - len(rows)),
        "calls": rows, "incomplete_call_count": sum(row.get("truncated") is True for row in rows)}


def _structure(dag: VerifiedDAG | None, dag_document: Mapping[str, Any] | None,
               new_keys: set[str], records: Mapping[str, Any], return_node: str | None):
    if dag is None:
        if not isinstance(dag_document, Mapping):
            return {}, {}, "dag_unavailable"
        raw_nodes = dag_document.get("nodes", [])
        nodes = {row.get("id"): row for row in raw_nodes if isinstance(row, Mapping) and isinstance(row.get("id"), str)}
        successors: dict[str, list[str]] = {node_id: [] for node_id in nodes}
        for row in nodes.values():
            for refs in row.get("inputs", {}).values() if isinstance(row.get("inputs"), Mapping) else ():
                refs = refs if isinstance(refs, list) else [refs]
                for ref in refs:
                    if isinstance(ref, str):
                        successors.setdefault(ref, []).append(row["id"])
        return nodes, successors, "preverification_failed"
    if len(dag.nodes) > MAX_DAG_NODES:
        return {}, {}, "verified_dag_over_limit"
    return _node_spec_by_id(dag.nodes), _successors(dag.nodes), None


def _combine_counterfactuals(*, verified: VerifiedDAG, execution: Any,
                              records: Mapping[str, Any], cached_outputs: Mapping[str, Any],
                              new_nodes: Sequence[VerifiedNode], runtime: RuntimeContext,
                              registry: OperatorRegistry, case_id: str | None,
                              composite_attribution: Mapping[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
    combiner = next((node for node in verified.nodes
                     if node.node_id == verified.return_node), None)
    if not _trusted_terminal(combiner):
        return {"status": "counterfactual_not_evaluated", "reason": "return_is_not_a_trusted_profile_terminal"}
    direct_refs = tuple(combiner.inputs.get("scores", ()))
    composite_attribution = composite_attribution or {}
    direct_generated = [node for node in new_nodes
        if node.operator.output_type == "ScoredCandidates" and node.node_id in direct_refs]
    successors = _successors(verified.nodes)
    indirect_or_mixed = [node.node_id for node in new_nodes
        if node.node_id not in direct_refs and _path(node.node_id, combiner.node_id, successors)]
    mixed_direct = [node.node_id for node in direct_generated
        if any(consumer != combiner.node_id and _path(consumer, combiner.node_id, successors)
               for consumer in successors.get(node.node_id, ()))]
    if indirect_or_mixed or mixed_direct:
        return {"status": "counterfactual_not_evaluated",
            "reason": "generated_operator_is_intermediate_or_mixed_into_downstream_scoring_path",
            "direct_generated_scored_node_count": len(direct_generated),
            "intermediate_or_mixed_node_ids_count": len(indirect_or_mixed) + len(mixed_direct)}
    if not direct_generated:
        return {"status": "counterfactual_not_evaluated", "reason": "no_direct_generated_scored_input"}
    all_scores = []
    counterfactual_candidate_count = 0
    counterfactual_evidence_count = 0
    for node_id in direct_refs:
        value = cached_outputs.get(node_id)
        if not isinstance(value, ScoredCandidates):
            return {"status": "counterfactual_not_evaluated", "reason": "combiner_score_input_cache_unavailable",
                "unavailable_score_input_count": sum(not isinstance(cached_outputs.get(ref), ScoredCandidates)
                                                       for ref in direct_refs)}
        counterfactual_candidate_count += len(value.candidates)
        if counterfactual_candidate_count > MAX_COUNTERFACTUAL_CANDIDATES:
            return {"status": "counterfactual_not_evaluated", "reason": "combiner_input_candidate_count_over_cap",
                "candidate_count": counterfactual_candidate_count,
                "candidate_count_cap": MAX_COUNTERFACTUAL_CANDIDATES}
        counterfactual_evidence_count += sum(len(candidate.evidence) for candidate in value.candidates)
        if counterfactual_evidence_count > MAX_COUNTERFACTUAL_EVIDENCE:
            return {"status": "counterfactual_not_evaluated", "reason": "combiner_input_evidence_count_over_cap",
                "evidence_count": counterfactual_evidence_count,
                "evidence_count_cap": MAX_COUNTERFACTUAL_EVIDENCE}
        all_scores.append((node_id, value))
    if execution is None or not isinstance(execution.answer, RankedAnswer):
        return {"status": "counterfactual_not_evaluated", "reason": "successful_final_answer_unavailable"}
    combiner_record = records.get(combiner.node_id)
    if combiner_record is None or combiner_record.status != "success":
        return {"status": "counterfactual_not_evaluated", "reason": "trusted_combiner_execution_record_unavailable"}
    trusted_spec = registry.get(combiner.operator.name, combiner.operator.version)
    if trusted_spec is None or trusted_spec.output_type != "RankedAnswer":
        return {"status": "counterfactual_not_evaluated", "reason": "trusted_terminal_registry_entry_unavailable"}
    implementation = trusted_spec.implementation
    if not callable(implementation):
        return {"status": "counterfactual_not_evaluated", "reason": "trusted_terminal_implementation_unavailable"}
    runtime_values = runtime.snapshot.values
    node_context = NodeContext(case_id or execution.case_id, combiner.node_id,
        trusted_spec.qualified_name, runtime_values, runtime.snapshot.provenance,
        runtime_values.get("reference_window"), runtime_values.get("alert_window"),
        _data_digest(records))
    params = combiner_record.resolved_params
    base_answer = execution.answer
    generated_ids = {node.node_id for node in direct_generated}
    grouped: dict[str, list[VerifiedNode]] = {}
    for node in direct_generated:
        attribution = composite_attribution.get(node.node_id)
        label = (str(attribution.get("composite")) if attribution and
            isinstance(attribution.get("composite"), str) else
            _qualified(node.operator.name, node.operator.version))
        grouped.setdefault(label, []).append(node)

    def recompute(removed: set[str]) -> tuple[RankedAnswer | None, str | None]:
        scores = tuple(value for node_id, value in all_scores if node_id not in removed)
        try:
            answer = implementation({"scores": scores}, params, node_context)
            if isinstance(answer, OperatorResult):
                answer = answer.value
            if not isinstance(answer, RankedAnswer):
                return None, "trusted_combiner_return_type_invalid"
            return answer, None
        except Exception as exc:
            return None, f"trusted_combiner_error:{type(exc).__name__}:{str(exc)[:200]}"

    individual = []
    node_individual = []
    for node_id in sorted(generated_ids):
        answer, error = recompute({node_id})
        node_row = {"node_id": node_id, "status": "unavailable" if error else "evaluated",
            "reason": error, "effect": _answer_delta(base_answer, answer) if answer else None}
        if node_id in composite_attribution:
            node_row["composite"] = composite_attribution[node_id].get("composite")
            node_row["logical_call_id"] = composite_attribution[node_id].get("logical_call_id")
        node_individual.append(node_row)
    for key, nodes in sorted(grouped.items()):
        answer, error = recompute({node.node_id for node in nodes})
        individual.append({"operator": key, "node_count_removed": len(nodes),
            "status": "unavailable" if error else "evaluated", "reason": error,
            "effect": _answer_delta(base_answer, answer) if answer else None})
    joint, joint_error = recompute(generated_ids)

    overlaps = []
    for index, left in enumerate(direct_generated):
        left_output = cached_outputs[left.node_id]
        left_ids = _scored_metrics(left_output)["positive_eligible_component_ids_internal"]
        for right in direct_generated[index + 1:]:
            right_output = cached_outputs[right.node_id]
            right_ids = _scored_metrics(right_output)["positive_eligible_component_ids_internal"]
            overlaps.append({"left_node_id": left.node_id, "right_node_id": right.node_id,
                "positive_eligible_component_overlap_count": len(left_ids & right_ids)})
    return {"status": "evaluated", "scope": f"direct generated ScoredCandidates inputs to trusted {combiner.operator.name} only",
        "direct_generated_scored_node_count": len(direct_generated),
        "counterfactual_input_candidate_count": counterfactual_candidate_count,
        "counterfactual_input_evidence_count": counterfactual_evidence_count,
        "individual_operator_removals": individual,
        "individual_node_removals": node_individual,
        "joint_direct_generated_removal": {"status": "unavailable" if joint_error else "evaluated",
            "reason": joint_error, "effect": _answer_delta(base_answer, joint) if joint else None},
        "overlapping_direct_inputs": overlaps}


def _data_digest(records: Mapping[str, Any]) -> str:
    for record in records.values():
        if "$external" in record.input_digests:
            return str(record.input_digests["$external"])
    return "diagnostic-data-digest-unavailable"


def build_evolution_contribution_diagnostics(*, gap_record: Mapping[str, Any] | None,
        new_specs: Sequence[Mapping[str, Any]], dag_document: Mapping[str, Any] | None,
        verified: VerifiedDAG | None, execution: Any | None,
        failure: ExecutionFailure | None, cache: CacheStore,
        runtime: RuntimeContext, registry: OperatorRegistry, run_dir: Path,
        preverification_error: Mapping[str, Any] | None = None,
        case_id: str | None = None,
        composite_expansion: Mapping[str, Any] | Any | None = None) -> dict[str, Any]:
    """Build required bounded per-operator diagnostics for every entered Stage 3.

    The only recomputation permitted is leave-one-out recombination of cached direct
    ScoredCandidates using the exact trusted registered combiner implementation.
    """
    started = time.perf_counter()
    keys = {_qualified(str(spec.get("name")), str(spec.get("version")))
            for spec in list(new_specs)[:MAX_NEW_OPERATORS]}
    if composite_expansion is not None and hasattr(composite_expansion, "as_dict"):
        composite_expansion = composite_expansion.as_dict()
    call_outputs = (composite_expansion.get("call_outputs", {})
        if isinstance(composite_expansion, Mapping) else {})
    call_rows = [row for row in call_outputs.values()
        if isinstance(row, Mapping) and row.get("composite") in keys]
    gaps = _capabilities(gap_record)
    records = _record_map(execution, failure)
    dag_nodes, successors, structure_error = _structure(verified, dag_document, keys,
        records, verified.return_node if verified else None)
    by_id = _node_spec_by_id(verified.nodes if verified else ())
    by_key: dict[str, list[Any]] = {key: [] for key in keys}
    for node in (verified.nodes if verified else ()):
        key = _qualified(node.operator.name, node.operator.version)
        if key in by_key:
            by_key[key].append(node)

    cached_outputs: dict[str, Any] = {}
    cache_states: dict[str, str] = {}
    cache_read_bytes = 0
    if verified is not None:
        wanted = {node.node_id for node in verified.nodes
                  if _qualified(node.operator.name, node.operator.version) in keys}
        wanted.update(str(row["output_node_id"]) for row in call_rows
            if isinstance(row.get("output_node_id"), str))
        trusted_return = next((node for node in verified.nodes
            if node.node_id == verified.return_node), None)
        if _trusted_terminal(trusted_return):
            wanted.update(trusted_return.inputs.get("scores", ()))
        for node in verified.nodes:
            if node.node_id not in wanted:
                continue
            record = records.get(node.node_id)
            if record is None or record.status != "success":
                cache_states[node.node_id] = ("node_record_unavailable" if record is None
                                              else "node_execution_failed")
                continue
            value, state, consumed = _read_output(record, node.operator.output_type,
                cache, MAX_TOTAL_CACHE_READ_BYTES - cache_read_bytes)
            cache_states[node.node_id] = state
            cache_read_bytes += consumed
            if value is not None:
                cached_outputs[node.node_id] = value

    outputs = []
    raw_nodes = (dag_document.get("nodes", []) if verified is None and
                 isinstance(dag_document, Mapping) else [])
    for spec in list(new_specs)[:MAX_NEW_OPERATORS]:
        name, version = str(spec.get("name", "")), str(spec.get("version", ""))
        key = _qualified(name, version)
        nodes = by_key.get(key, [])
        declared_version = spec.get("declared_version")
        matching_versions = {version}
        if declared_version is not None:
            matching_versions.add(str(declared_version))
        raw_matches = [row for row in raw_nodes if isinstance(row, Mapping) and
            row.get("operator") == name and str(row.get("version")) in matching_versions]
        gap = (gaps.get(_qualified(name, str(declared_version)))
               if declared_version is not None else None) or gaps.get(key)
        if gap is None:
            by_name = [row for gap_key, row in gaps.items() if gap_key.split("@", 1)[0] == name]
            gap = by_name[0] if len(by_name) == 1 else None
        if spec.get("authoring_profile") == "operator-dag-v1":
            macro_calls = [row for row in call_rows if row.get("composite") == key]
            call_node_rows = []
            macro_leaf_nodes: list[Any] = []
            for call_row in macro_calls[:MAX_DAG_NODES]:
                output_id = call_row.get("output_node_id")
                leaf = by_id.get(output_id) if isinstance(output_id, str) else None
                record = records.get(output_id) if isinstance(output_id, str) else None
                if leaf is not None:
                    macro_leaf_nodes.append(leaf)
                execution_state = (record.status if record else
                    "not_reached_due_upstream_failure" if failure is not None else
                    "not_executed_preverification_failed" if verified is None else "record_unavailable")
                output = cached_outputs.get(output_id) if record is not None and record.status == "success" else None
                downstream = (_path(output_id, verified.return_node, successors)
                    if verified is not None and isinstance(output_id, str) else None)
                upstream = []
                for source in (candidate.node_id for candidate in (verified.nodes if verified else ())
                               if candidate.operator.external_input):
                    path = _path(source, output_id, successors) if isinstance(output_id, str) else None
                    if path:
                        upstream = path
                        break
                output_info = _output_summary(output) if output is not None else None
                if output_info is not None:
                    output_info.pop("eligible_component_ids_internal", None)
                    output_info.pop("positive_eligible_component_ids_internal", None)
                call_node_rows.append({"node_id": call_row.get("logical_node_id"),
                    "kind": "composite", "output_type": spec.get("output_type"),
                    "structurally_reachable_to_return": downstream is not None,
                    "path_from_external_input": upstream, "path_to_return": downstream,
                    "execution_status": execution_state,
                    "expanded_output_node_id": output_id,
                    "primitive_operator": leaf.operator.name if leaf else None,
                    "primitive_version": leaf.operator.version if leaf else None,
                    "expanded_node_ids": _plain(call_row.get("expanded_node_ids", [])),
                    "macro_path": _plain(call_row.get("call_path", [])),
                    "primitive_operator_counts": _plain(call_row.get("primitive_operator_counts", {})),
                    "cache_status": record.cache_status if record else "unavailable",
                    "cache_output_status": cache_states.get(output_id, "not_read"),
                    "input_digests": _plain(record.input_digests) if record else None,
                    "output_digest": record.output_digest if record else None,
                    "elapsed_ms": record.elapsed_ms if record else None,
                    "rows_processed": record.rows_processed if record else None,
                    "bytes_processed": record.bytes_processed if record else None,
                    "output": output_info})
            direct_refs = set()
            if verified is not None:
                trusted_return = next((node for node in verified.nodes
                    if node.node_id == verified.return_node), None)
                if not _trusted_terminal(trusted_return):
                    trusted_return = None
                direct_refs = set(trusted_return.inputs.get("scores", ())) if trusted_return else set()
            directly_scored = any(row.get("expanded_output_node_id") in direct_refs and
                row.get("output_type") == "ScoredCandidates" for row in call_node_rows)
            outputs.append({"operator": key, "kind": "composite", "status":
                "expanded" if macro_calls else "not_expanded",
                "declared_version": declared_version,
                "capability_gap": _plain(gap) if gap else None,
                "structural_node_count": len(macro_calls),
                "structurally_reachable_node_count": sum(
                    row.get("structurally_reachable_to_return") is True for row in call_node_rows),
                "nodes_omitted": max(0, len(macro_calls) - len(call_node_rows)),
                "nodes": call_node_rows,
                "counterfactual_not_evaluated": not directly_scored,
                "counterfactual_reason": (None if directly_scored else
                    "composite_output_not_direct_scored_candidates_to_trusted_combiner")})
            by_key[key] = macro_leaf_nodes
            continue
        node_rows = []
        for node in nodes[:MAX_DAG_NODES]:
            record = records.get(node.node_id)
            execution_state = (record.status if record else
                "not_reached_due_upstream_failure" if failure is not None else
                "not_executed_preverification_failed" if verified is None else "record_unavailable")
            if record is not None and record.status == "success":
                output = cached_outputs.get(node.node_id)
                cache_state = cache_states.get(node.node_id, "cache_not_read")
            else:
                output, cache_state = None, "not_read_without_successful_node_record"
            upstream = []
            for source in (candidate.node_id for candidate in (verified.nodes if verified else ())
                           if candidate.operator.external_input):
                path = _path(source, node.node_id, successors)
                if path:
                    upstream = path
                    break
            downstream = (_path(node.node_id, verified.return_node, successors)
                          if verified is not None else None)
            output_info = _output_summary(output) if output is not None else None
            if output_info is not None:
                output_info.pop("eligible_component_ids_internal", None)
                output_info.pop("positive_eligible_component_ids_internal", None)
            node_rows.append({"node_id": node.node_id,
                "output_type": node.operator.output_type,
                "structurally_reachable_to_return": downstream is not None,
                "path_from_external_input": upstream,
                "path_to_return": downstream,
                "execution_status": execution_state,
                "cache_status": record.cache_status if record else "unavailable",
                "cache_output_status": cache_state,
                "input_digests": _plain(record.input_digests) if record else None,
                "output_digest": record.output_digest if record else None,
                "elapsed_ms": record.elapsed_ms if record else None,
                "rows_processed": record.rows_processed if record else None,
                "bytes_processed": record.bytes_processed if record else None,
                "output": output_info})
        if verified is None:
            for raw in raw_matches[:MAX_DAG_NODES - len(node_rows)]:
                node_rows.append({"node_id": raw.get("id"),
                    "output_type": next((spec.get("output_type") for spec in new_specs
                        if spec.get("name") == name and str(spec.get("version")) == version), None),
                    "structurally_reachable_to_return": None,
                    "path_from_external_input": None, "path_to_return": None,
                    "execution_status": "not_executed_preverification_failed",
                    "cache_status": "not_available_before_verification",
                    "cache_output_status": "unavailable",
                    "input_digests": None, "output_digest": None, "elapsed_ms": None,
                    "rows_processed": None, "bytes_processed": None, "output": None})
        entry: dict[str, Any] = {"operator": key,
            "declared_version": declared_version,
            "capability_gap": _plain(gap) if gap else None,
            "structural_node_count": len(nodes),
            "structurally_reachable_node_count": sum(
                _path(node.node_id, verified.return_node, successors) is not None
                for node in nodes) if verified else 0,
            "nodes_omitted": max(0, len(nodes) - len(node_rows)),
            "preverification_nodes_omitted": max(0, len(raw_matches) - len(node_rows)) if verified is None else 0,
            "nodes": node_rows,
            "counterfactual_not_evaluated": verified is None or not node_rows or any(
                row.get("output_type") != "ScoredCandidates" for row in node_rows),
            "counterfactual_reason": ("preverification_failed" if verified is None else
                "operator node unavailable in verified DAG" if not node_rows else
                "generated intermediate requires downstream execution" if any(
                    row.get("output_type") != "ScoredCandidates" for row in node_rows) else None)}
        if spec.get("authoring_profile") in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
            entry.update({"kind": "typed_operator_ir", "authoring_profile": spec["authoring_profile"],
                "program_sha256": spec.get("program_sha256"),
                "kernel_abi_sha256": spec.get("kernel_abi_sha256"),
                "interpreter_sha256": spec.get("interpreter_sha256"),
                "policy_pins": _plain(spec.get("policy_pins", {}))})
        elif spec.get("authoring_profile") == "typed-operator-code-v1":
            entry.update({"kind": "typed_operator_code", "authoring_profile": spec["authoring_profile"],
                "source_sha256": spec.get("source_sha256"),
                "computation_contract_hash": spec.get("contract_hash")})
        outputs.append(entry)

    query_coverage = _query_coverage(run_dir)
    answer = execution.answer if execution is not None else None
    answer_stats = _answer_summary(answer)
    if verified is None:
        counterfactual = {"status": "counterfactual_not_evaluated",
            "reason": "preverification_failed" if preverification_error else "verified_dag_unavailable"}
    else:
        composite_attribution = {}
        composite_output_nodes = []
        for call_row in call_rows:
            output_id = call_row.get("output_node_id")
            node = by_id.get(output_id) if isinstance(output_id, str) else None
            if node is not None:
                composite_output_nodes.append(node)
                composite_attribution[node.node_id] = {"composite": call_row.get("composite"),
                    "logical_call_id": call_row.get("logical_node_id")}
        counterfactual = _combine_counterfactuals(verified=verified, execution=execution,
            records=records, cached_outputs=cached_outputs,
            new_nodes=[node for node in verified.nodes
                if _qualified(node.operator.name, node.operator.version) in keys] + composite_output_nodes,
            runtime=runtime, registry=registry, case_id=case_id,
            composite_attribution=composite_attribution)
    if counterfactual.get("status") == "counterfactual_not_evaluated":
        for row in outputs:
            row["counterfactual_not_evaluated"] = True
            row["counterfactual_reason"] = counterfactual.get("reason")
    elif verified is not None:
        trusted_return = next((node for node in verified.nodes
            if node.node_id == verified.return_node), None)
        if not _trusted_terminal(trusted_return):
            trusted_return = None
        direct_refs = set(trusted_return.inputs.get("scores", ())) if trusted_return else set()
        for row in outputs:
            direct = any(node.get("expanded_output_node_id", node.get("node_id")) in direct_refs and
                         node.get("output_type") == "ScoredCandidates"
                         for node in row.get("nodes", []))
            row["counterfactual_not_evaluated"] = not direct
            row["counterfactual_reason"] = (None if direct else
                "composite_output_not_direct_scored_candidates_to_trusted_combiner"
                if row.get("kind") == "composite" else
                "operator is not a direct trusted-combiner ScoredCandidates input")
    result: dict[str, Any] = {"schema_version": SCHEMA_VERSION,
        "status": "completed" if structure_error is None else "degraded",
        "diagnostic_scope": "observed execution/cache summaries and trusted-combiner direct-input ablation; no generated code rerun",
        "gap_record": _plain(gap_record) if gap_record is not None else None,
        "preverification_error": _plain(preverification_error) if preverification_error else None,
        "execution_failure": _failure_summary(failure),
        "structure_status": structure_error or "verified",
        "new_operator_count": min(len(new_specs), MAX_NEW_OPERATORS),
        "operators_omitted": max(0, len(new_specs) - MAX_NEW_OPERATORS),
        "operators": outputs,
        "composite_expansion": _plain(composite_expansion) if composite_expansion is not None else None,
        "final_answer": answer_stats,
        "final_answer_eligible_evidence_overlap": _answer_overlap(outputs, answer, cached_outputs),
        "query_coverage": query_coverage,
        "counterfactual": counterfactual,
        "cache_read_bytes": cache_read_bytes,
        "cache_read_bytes_cap": MAX_TOTAL_CACHE_READ_BYTES}
    result["elapsed_ms"] = (time.perf_counter() - started) * 1000
    return result


def _answer_overlap(operator_rows: Sequence[Mapping[str, Any]], answer: RankedAnswer | None,
                    cached_outputs: Mapping[str, Any]) -> dict[str, Any] | None:
    if answer is None:
        return None
    top3 = {candidate.entity.canonical_id for candidate in answer.leads[:3]
        if any(_eligible_evidence(item, candidate.entity) for item in candidate.evidence)}
    top5 = {candidate.entity.canonical_id for candidate in answer.leads[:5]
        if any(_eligible_evidence(item, candidate.entity) for item in candidate.evidence)}
    all_eligible = {candidate.entity.canonical_id for candidate in answer.leads
        if any(_eligible_evidence(item, candidate.entity) for item in candidate.evidence)}
    final_evidence_by_limit = {}
    for label, limit in (("top3", 3), ("top5", 5), ("all", len(answer.leads))):
        final_evidence_by_limit[label] = {}
        for lead in answer.leads[:limit]:
            signatures = {_evidence_fingerprint(item) for item in lead.evidence
                          if _eligible_evidence(item, lead.entity)}
            if signatures:
                final_evidence_by_limit[label][lead.entity.canonical_id] = signatures
    per_operator = []
    for row in operator_rows:
        op_evidence: dict[str, set[str]] = {}
        scored_cache_available = 0
        scored_cache_unavailable = 0
        candidate_summary_truncated = False
        for node in row.get("nodes", []):
            output_id = node.get("expanded_output_node_id", node.get("node_id"))
            value = cached_outputs.get(output_id)
            if not isinstance(value, ScoredCandidates):
                if node.get("output_type") == "ScoredCandidates":
                    scored_cache_unavailable += 1
                continue
            scored_cache_available += 1
            candidate_summary_truncated = candidate_summary_truncated or len(value.candidates) > MAX_OUTPUT_CANDIDATES
            for candidate in value.candidates[:MAX_OUTPUT_CANDIDATES]:
                eligible = [item for item in candidate.evidence if _eligible_evidence(item, candidate.entity)]
                if candidate.score > 0 and eligible:
                    op_evidence.setdefault(candidate.entity.canonical_id, set()).update(
                        _evidence_fingerprint(item) for item in eligible)
        op_ids = set(op_evidence)
        overlap = {label: sum(len(op_evidence.get(entity, set()) & signatures)
                              for entity, signatures in final_map.items())
                   for label, final_map in final_evidence_by_limit.items()}
        per_operator.append({"operator": row.get("operator"),
            "positive_eligible_component_count": len(op_ids),
            "eligible_component_overlap_top3_count": len(op_ids & top3),
            "eligible_component_overlap_top5_count": len(op_ids & top5),
            "eligible_component_overlap_all_count": len(op_ids & all_eligible),
            "eligible_evidence_signature_overlap_top3_count": overlap["top3"],
            "eligible_evidence_signature_overlap_top5_count": overlap["top5"],
            "eligible_evidence_signature_overlap_all_count": overlap["all"],
            "scored_candidate_nodes_cached": scored_cache_available,
            "scored_candidate_nodes_unavailable": scored_cache_unavailable,
            "candidate_summary_truncated": candidate_summary_truncated,
            "comparison_status": ("unavailable_missing_typed_cache" if scored_cache_available == 0 and
                scored_cache_unavailable else "partial_typed_cache_or_candidate_cap" if
                scored_cache_unavailable or candidate_summary_truncated else
                "observed_evidence_overlap_not_causal_contribution")})
    return {"final_top3_eligible_lead_count": len(top3),
        "final_top5_eligible_lead_count": len(top5),
        "final_eligible_lead_count": len(all_eligible),
        "per_operator": per_operator,
        "status": "computed" if per_operator else "no_cached_direct_scored_candidates"}
