"""Cyber operators: they read a Case's log records and score its components.

    cyber.logs.match         TelemetryWindows, CandidateSet -> ScoredCandidates
    cyber.answers.combine    ScoredCandidates...            -> RankedAnswer (component_time_top3)
"""
from __future__ import annotations

import re
from typing import Any, Mapping

from .case_ops import register_case_operators
from .contracts import (CandidateSet, LogRecord, RankedAnswer, ScoredCandidate, ScoredCandidates,
                        TelemetryWindows)
from .cyber_adapter import epoch_ms, naive_utc
from .cyber_data import register_cyber_data_operators
from .m4_generated import register_generated_specs
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec


def record_interval(record: LogRecord, run_start_ms: int) -> tuple[int, int]:
    """When a record ran, in ms since the run's start. An action runs until its ts_finish;
    any other record is a single moment, one millisecond long."""
    start = record.timestamp_ms_utc
    end = start + 1
    finish = record.fields.get("ts_finish")
    if finish:
        finish_ms = epoch_ms(naive_utc(finish)) - run_start_ms
        if finish_ms > start:
            end = finish_ms
    return start, end


def field_matches(record: LogRecord, field: str, pattern: re.Pattern) -> bool:
    value = record.fields.get(field)
    if value is None:
        return False
    return pattern.search(str(value)) is not None


def match_logs(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any) -> ScoredCandidates:
    """For each candidate: the window's records of that kind whose `field` matches `pattern`.
    The score is how many matched; the intervals are when each one ran."""
    windows: TelemetryWindows = inputs["windows"]
    candidates: CandidateSet = inputs["candidates"]
    if len(windows.windows) != 1:
        raise ValueError("cyber.logs.match reads exactly one window")
    window = windows.windows[0]
    run_start_ms = epoch_ms(naive_utc(window.telemetry.params["wall_start"]))
    pattern = re.compile(params["pattern"])

    scored = []
    for candidate in candidates.candidates:
        intervals = []
        for record in window.logs:
            if record.kind != candidate.entity.identifier:
                continue
            if not field_matches(record, params["field"], pattern):
                continue
            intervals.append(record_interval(record, run_start_ms))
        if intervals:
            scored.append(ScoredCandidate(candidate.entity, float(len(intervals)), None, (),
                                          analysis_family="cyber.logs.match",
                                          intervals=tuple(intervals)))

    status = "ready" if scored else "abstained"
    return ScoredCandidates(tuple(scored), scorer="cyber.logs.match", status=status)


def combine_answers(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any) -> RankedAnswer:
    """Every candidate with a positive score, one lead per component (its best), best first."""
    best: dict[tuple[str, str], ScoredCandidate] = {}
    for scored in inputs["scores"]:
        for candidate in scored.candidates:
            if candidate.score <= 0:
                continue
            key = (candidate.entity.role, candidate.entity.canonical_id)
            if key not in best or candidate.score > best[key].score:
                best[key] = candidate

    def rank(candidate: ScoredCandidate):
        return (-candidate.score, candidate.entity.canonical_id, candidate.entity.role)

    leads = sorted(best.values(), key=rank)
    limit = int(params["limit"])
    return RankedAnswer(tuple(leads[:limit]), acceptance_contract="component_time_top3")


def register_cyber_operators(registry: OperatorRegistry) -> OperatorRegistry:
    registry.register(OperatorSpec(
        "cyber.logs.match", "1",
        (PortSpec("windows", "TelemetryWindows"), PortSpec("candidates", "CandidateSet")),
        "ScoredCandidates",
        {"field": ParameterSpec("field", "string", required=True,
                                description="log record field to test, e.g. action_params.command"),
         "pattern": ParameterSpec("pattern", "string", required=True,
                                  description="regular expression the field must contain")},
        family="cyber_match", implementation=match_logs,
        description="Per candidate: its records in the one window whose field matches the pattern; "
                    "score is the count, intervals are when each ran."))
    registry.register(OperatorSpec(
        "cyber.answers.combine", "1",
        (PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),), "RankedAnswer",
        {"limit": ParameterSpec("limit", "positive_integer", runtime_field="lead_count")},
        family="cyber_ranking", implementation=combine_answers,
        description="Positive-score candidates, one per component, best first, with their intervals."))
    return registry


def cyber_registry(generated_specs: list[Mapping[str, Any]] = ()) -> OperatorRegistry:
    """The Case operators, the cyber data and scoring operators, and any generated operators."""
    registry = OperatorRegistry()
    register_case_operators(registry)
    register_cyber_data_operators(registry)
    register_cyber_operators(registry)
    register_generated_specs(registry, list(generated_specs))
    return registry
