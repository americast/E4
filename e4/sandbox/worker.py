"""Standalone worker script for executing an authored Python operator in a child process."""

from __future__ import annotations

import json
import math
import resource
import sys
import time
import builtins
from typing import Any, Mapping

from e4.sandbox.ast_validator import ALLOWED_MODULES, SAFE_BUILTINS, validate_source
from e4.types import RankedAnswer
from e4.sandbox.views import (
    Aggregate,
    AggregateGroupView,
    AggregateRow,
    AggregateRowView,
    AggregateView,
    Candidate,
    CandidateSet,
    CandidateSetView,
    CandidateView,
    CoverageView,
    EntityRef,
    EntityRefView,
    Evidence,
    EvidenceView,
    LogRecord,
    LogRecordView,
    MetricPointView,
    MetricRecord,
    QueryResult,
    ScoredCandidate,
    ScoredCandidateView,
    ScoredCandidates,
    ScoredCandidatesView,
    SpanRecord,
    SpanView,
    SummaryResult,
    TelemetryView,
    Window,
    WindowView,
    aggregate_records,
    aggregate_view,
    candidate_views,
    entity_id,
    entity_identifier,
    group_by,
    log_records,
    make_aggregate,
    make_aggregate_row,
    make_candidate,
    make_candidate_set,
    make_evidence,
    make_scored_candidate,
    make_scored_candidates,
    make_window,
    make_entity_ref,
    metric_points,
    scored_views,
    span_records,
    summarize,
    telemetry_view,
    top_k,
)


def _restricted_import(
    name: str,
    globals: Mapping[str, Any] | None = None,
    locals: Mapping[str, Any] | None = None,
    fromlist: tuple[str, ...] = (),
    level: int = 0,
) -> Any:
    """Import only exact modules already allowed by authored-source validation."""
    if level != 0 or name not in ALLOWED_MODULES:
        raise ImportError(f"Importing unauthorized module: {name}")
    return builtins.__import__(name, globals, locals, fromlist, level)


def _safe_builtins() -> dict[str, Any]:
    """Build authored builtins, with import syntax limited to the AST allowlist."""
    names = {name: getattr(builtins, name) for name in SAFE_BUILTINS if hasattr(builtins, name)}
    names["__import__"] = _restricted_import
    return names


def _deserialize_input(val: Any) -> Any:
    if isinstance(val, Mapping):
        type_tag = val.get("$type")
        fields = val.get("$fields", val)
        if type_tag == "Window" or ("name" in fields and "start_ms_utc" in fields and "end_ms_utc" in fields):
            return Window(fields["name"], int(fields["start_ms_utc"]), int(fields["end_ms_utc"]))
        if type_tag == "EntityRef" or ("role" in fields and "identifier" in fields and "name" not in fields):
            return EntityRef(fields["role"], fields["identifier"])
        if type_tag == "Evidence" or ("source" in fields and "statistic" in fields and "contributing_entities" in fields):
            w = _deserialize_input(fields["window"])
            ents = tuple(_deserialize_input(e) for e in fields.get("contributing_entities", ()))
            return Evidence(
                source=fields["source"],
                window=w,
                statistic=fields["statistic"],
                value=fields.get("value"),
                contributing_entities=ents,
                support=fields.get("support", 0),
                ranking_eligible=fields.get("ranking_eligible", True),
                details=fields.get("details", {}),
            )
        if type_tag == "Candidate" or ("entity" in fields and "rationale" in fields and "score" not in fields):
            return Candidate(_deserialize_input(fields["entity"]), fields.get("rationale", ""))
        if type_tag == "CandidateSet" or ("candidates" in fields and "source" in fields and "scorer" not in fields):
            cands = tuple(_deserialize_input(c) for c in fields.get("candidates", ()))
            return CandidateSet(candidates=cands, source=fields.get("source", ""))
        if type_tag == "AggregateRow" or ("metric_key" in fields and "statistic" in fields and "entity" in fields):
            e = _deserialize_input(fields["entity"])
            w = _deserialize_input(fields["window"])
            ents = tuple(_deserialize_input(item) for item in fields.get("contributing_entities", ()))
            return AggregateRow(
                entity=e,
                metric_key=fields["metric_key"],
                window=w,
                statistic=fields["statistic"],
                value=fields.get("value"),
                support=fields.get("support", 0),
                source_tables=tuple(fields.get("source_tables", ())),
                contributing_entities=ents,
                details=fields.get("details", {}),
            )
        if type_tag == "Aggregate" or ("rows" in fields and "source_tables" in fields and "window" in fields):
            w = _deserialize_input(fields["window"])
            rows = tuple(_deserialize_input(r) for r in fields.get("rows", ()))
            return Aggregate(
                window=w,
                rows=rows,
                source_tables=tuple(fields.get("source_tables", ())),
                metadata=fields.get("metadata", {}),
            )
        if type_tag == "ScoredCandidate" or ("score" in fields and "entity" in fields):
            e = _deserialize_input(fields["entity"])
            ev = tuple(_deserialize_input(x) for x in fields.get("evidence", ()))
            return ScoredCandidate(
                entity=e,
                score=fields["score"],
                category=fields.get("category"),
                evidence=ev,
                analysis_family=fields.get("analysis_family"),
            )
        if type_tag == "ScoredCandidates" or ("candidates" in fields and "scorer" in fields):
            cands = tuple(_deserialize_input(c) for c in fields.get("candidates", ()))
            return ScoredCandidates(
                candidates=cands,
                scorer=fields.get("scorer", ""),
                status=fields.get("status", "ready"),
                status_details=fields.get("status_details", {}),
            )
        if type_tag in {"Telemetry", "TelemetryView", "TelemetryHandle"}:
            ref = _deserialize_input(fields["reference_window"])
            alt = _deserialize_input(fields["alert_window"])
            record_store = None
            if fields.get("record_store") is not None:
                from ..data.record_store import TelemetryRecordStore

                record_store = TelemetryRecordStore.from_descriptor(fields["record_store"])
            records = []
            for r in fields.get("records", ()):
                rtype = r.get("$type")
                rfields = r.get("$fields", r)
                if rtype == "MetricRecord":
                    ent = _deserialize_input(rfields["entity"])
                    records.append(
                        MetricRecord(
                            source_table=rfields["source_table"],
                            timestamp_ms_utc=rfields["timestamp_ms_utc"],
                            entity=ent,
                            metric_name=rfields["metric_name"],
                            value=rfields.get("value"),
                            unit=rfields.get("unit"),
                            details=rfields.get("details", {}),
                        )
                    )
                elif rtype == "SpanRecord":
                    ec = _deserialize_input(rfields["executing_component"]) if rfields.get("executing_component") else None
                    ls = _deserialize_input(rfields["logical_service"]) if rfields.get("logical_service") else None
                    dt = _deserialize_input(rfields["database_target"]) if rfields.get("database_target") else None
                    records.append(
                        SpanRecord(
                            source_table=rfields["source_table"],
                            timestamp_ms_utc=rfields["timestamp_ms_utc"],
                            trace_id=rfields["trace_id"],
                            span_id=rfields["span_id"],
                            parent_id=rfields.get("parent_id"),
                            duration_ms=rfields.get("duration_ms"),
                            success=rfields.get("success"),
                            call_type=rfields.get("call_type"),
                            executing_component=ec,
                            logical_service=ls,
                            database_target=dt,
                            details=rfields.get("details", {}),
                        )
                    )
                elif rtype == "LogRecord":
                    ent = _deserialize_input(rfields["entity"]) if rfields.get("entity") else None
                    records.append(
                        LogRecord(
                            source_table=rfields["source_table"],
                            timestamp_ms_utc=rfields["timestamp_ms_utc"],
                            entity=ent,
                            log_name=rfields.get("log_name"),
                            value=rfields.get("value"),
                            severity=rfields.get("severity"),
                            details=rfields.get("details", {}),
                        )
                    )
            return TelemetryView(
                case_id=fields.get("case_id", "case"),
                reference_window=ref,
                alert_window=alt,
                records=records,
                source_tables=fields.get("source_tables"),
                metadata=fields.get("metadata", {}),
                record_store=record_store,
            )
        return {k: _deserialize_input(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return tuple(_deserialize_input(item) for item in val)
    return val


def _validate_runtime_output_type(result: Any, expected_type: str) -> None:
    if not expected_type or expected_type == "Any":
        return
    if expected_type == "Aggregate":
        if not (isinstance(result, (Aggregate, AggregateView)) or (hasattr(result, "rows") and hasattr(result, "window"))):
            raise TypeError(f"Operator returned {type(result).__name__}, expected declared output_type 'Aggregate'")
    elif expected_type == "CandidateSet":
        if not (isinstance(result, (CandidateSet, CandidateSetView)) or (hasattr(result, "candidates") and not hasattr(result, "scores"))):
            raise TypeError(f"Operator returned {type(result).__name__}, expected declared output_type 'CandidateSet'")
    elif expected_type == "ScoredCandidates":
        if not (isinstance(result, (ScoredCandidates, ScoredCandidatesView)) or (hasattr(result, "candidates") and hasattr(result, "scorer"))):
            raise TypeError(f"Operator returned {type(result).__name__}, expected declared output_type 'ScoredCandidates'")
    elif expected_type == "RankedAnswer":
        if not (isinstance(result, RankedAnswer) or (hasattr(result, "candidates") and hasattr(result, "scores"))):
            raise TypeError(f"Operator returned {type(result).__name__}, expected declared output_type 'RankedAnswer'")


def _serialize_output(val: Any) -> Any:
    if hasattr(val, "as_dict"):
        return {"$type": type(val).__name__, "$fields": val.as_dict()}
    if isinstance(val, Mapping):
        return {str(k): _serialize_output(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_serialize_output(x) for x in val]
    return val


def main() -> None:
    try:
        raw_input = sys.stdin.read()
        payload = json.loads(raw_input)
    except Exception as exc:
        json.dump({"ok": False, "error": f"Failed to parse stdin payload: {exc}"}, sys.stdout)
        sys.exit(1)

    source = payload.get("source", "")
    raw_inputs = payload.get("inputs", {})
    params = payload.get("params", {})
    runtime_dict = payload.get("runtime", {})
    output_type = payload.get("output_type")

    # Install memory limit if configured
    memory_limit_mb = payload.get("memory_limit_mb")
    if memory_limit_mb:
        try:
            bytes_limit = int(memory_limit_mb) * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (bytes_limit, bytes_limit))
        except Exception:
            pass

    # Validate AST security in child process
    try:
        validate_source(source)
    except Exception as exc:
        json.dump({"ok": False, "error": f"Security validation failed: {exc}"}, sys.stdout)
        sys.exit(1)

    # Deserialize inputs
    inputs = {k: _deserialize_input(v) for k, v in raw_inputs.items()}

    # Construct safe execution globals
    exec_globals: dict[str, Any] = {
        "__builtins__": _safe_builtins(),
        # Math & collections
        "math": __import__("math"),
        "statistics": __import__("statistics"),
        "itertools": __import__("itertools"),
        "collections": __import__("collections"),
        # Typed views and queries
        "Window": Window,
        "EntityRef": EntityRef,
        "Evidence": Evidence,
        "Candidate": Candidate,
        "CandidateSet": CandidateSet,
        "AggregateRow": AggregateRow,
        "Aggregate": Aggregate,
        "ScoredCandidate": ScoredCandidate,
        "ScoredCandidates": ScoredCandidates,
        "WindowView": WindowView,
        "EntityRefView": EntityRefView,
        "CoverageView": CoverageView,
        "MetricPointView": MetricPointView,
        "SpanView": SpanView,
        "LogRecordView": LogRecordView,
        "QueryResult": QueryResult,
        "SummaryResult": SummaryResult,
        "AggregateGroupView": AggregateGroupView,
        "EvidenceView": EvidenceView,
        "CandidateView": CandidateView,
        "CandidateSetView": CandidateSetView,
        "AggregateRowView": AggregateRowView,
        "AggregateView": AggregateView,
        "ScoredCandidateView": ScoredCandidateView,
        "ScoredCandidatesView": ScoredCandidatesView,
        "TelemetryView": TelemetryView,
        # Helpers
        "telemetry_view": telemetry_view,
        "metric_points": metric_points,
        "span_records": span_records,
        "log_records": log_records,
        "aggregate_records": aggregate_records,
        "group_by": group_by,
        "summarize": summarize,
        "top_k": top_k,
        "make_window": make_window,
        "make_entity_ref": make_entity_ref,
        "make_evidence": make_evidence,
        "make_candidate": make_candidate,
        "make_candidate_set": make_candidate_set,
        "make_aggregate_row": make_aggregate_row,
        "make_aggregate": make_aggregate,
        "make_scored_candidate": make_scored_candidate,
        "make_scored_candidates": make_scored_candidates,
        "entity_identifier": entity_identifier,
        "entity_id": entity_id,
        "aggregate_view": aggregate_view,
        "candidate_views": candidate_views,
        "scored_views": scored_views,
    }

    class RuntimeShim:
        def __init__(self, d: Mapping[str, Any]):
            self._d = dict(d)
            for k, v in d.items():
                setattr(self, k, v)

    runtime_obj = RuntimeShim(runtime_dict)

    local_namespace = exec_globals
    try:
        compiled = compile(source, "<authored-operator>", "exec")
        exec(compiled, exec_globals, local_namespace)
        op_fn = local_namespace.get("operator")
        if not callable(op_fn):
            raise ValueError("Authored code did not define a callable 'operator'")
        t0 = time.monotonic()
        result = op_fn(inputs, params, runtime_obj)
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        if output_type:
            _validate_runtime_output_type(result, output_type)
    except Exception as exc:
        json.dump({"ok": False, "error": f"Operator execution failed: {type(exc).__name__}: {exc}"}, sys.stdout)
        sys.exit(0)

    try:
        serialized = _serialize_output(result)
        json.dump({"ok": True, "result": serialized, "elapsed_ms": elapsed_ms}, sys.stdout)
    except Exception as exc:
        json.dump({"ok": False, "error": f"Failed to serialize operator output: {exc}"}, sys.stdout)


if __name__ == "__main__":
    main()
