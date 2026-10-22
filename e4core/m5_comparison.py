"""Paired Accuracy@3,i summaries and transparent resource accounting for M5/M6."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

COST_FIELDS = ("model_calls", "prompt_tokens", "completion_tokens", "model_seconds",
              "cpu_seconds", "load_seconds", "startup_seconds", "wall_seconds",
              "logical_bytes_scanned", "physical_bytes_read", "returned_bytes")


def _attempt_slots(system: str, case_id: str, item: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    slots = item.get("attempts")
    if not isinstance(slots, list) or len(slots) != 3:
        raise ValueError(f"{system}/{case_id} must preserve exactly three ordered diagnosis slots")
    for index, slot in enumerate(slots, 1):
        if not isinstance(slot, Mapping) or slot.get("attempt") != index:
            raise ValueError(f"{system}/{case_id} attempt slots must be ordered and indexed 1..3")
        status = slot.get("status")
        if status not in {"submitted", "failed", "not_run"}:
            raise ValueError(f"{system}/{case_id} has invalid attempt slot status")
        if type(slot.get("accepted")) is not bool:
            raise ValueError(f"{system}/{case_id} attempt slot needs a trusted accepted boolean")
        rank = slot.get("correct_rank")
        if rank is not None and (type(rank) is not int or rank not in {1, 2, 3}):
            raise ValueError(f"{system}/{case_id} has invalid trusted correct_rank")
        if slot["accepted"] and (status != "submitted" or slot.get("valid") is False):
            raise ValueError(f"{system}/{case_id} accepted status conflicts with slot validity")
        if slot.get("correct_rank") is not None and not slot["accepted"]:
            raise ValueError(f"{system}/{case_id} accepted flag conflicts with exact-match rank")
        if status == "not_run" and slot["accepted"]:
            raise ValueError(f"{system}/{case_id} unrun slot cannot be accepted")
    return slots


def _finite_nonnegative(value: Any, label: str) -> float | None:
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(float(value)) or value < 0:
        raise ValueError(f"{label} must be a finite nonnegative number or null")
    return float(value)


def summarize_system(system: str, cases: Mapping[str, Mapping[str, Any]], *,
                     expected_case_ids: Sequence[str],
                     bootstrap_cost: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Compute top-three acceptance across the first i diagnosis slots, i=1..3."""
    expected = tuple(expected_case_ids)
    if len(set(expected)) != len(expected) or set(cases) != set(expected):
        raise ValueError("system result case IDs must exactly equal the fixed comparison allowlist")
    outcomes: dict[str, list[bool]] = {}
    ranks: dict[str, int | None] = {}
    totals = {field: 0.0 for field in COST_FIELDS}
    known = {field: 0 for field in COST_FIELDS}
    submissions = 0
    for case_id in expected:
        item = cases[case_id]
        slots = _attempt_slots(system, case_id, item)
        outcomes[case_id] = [slot["accepted"] for slot in slots]
        ranks[case_id] = next((slot["correct_rank"] for slot in slots
                               if slot.get("correct_rank") is not None), None)
        submissions += sum(slot["status"] != "not_run" for slot in slots)
        cost = item.get("cost", {})
        if not isinstance(cost, Mapping):
            raise ValueError(f"{system}/{case_id} cost record must be an object")
        for field in COST_FIELDS:
            value = _finite_nonnegative(cost.get(field), f"{system}/{case_id} {field}")
            if value is not None:
                totals[field] += value
                known[field] += 1
    n = len(expected)
    accuracy = {f"accuracy_at_3_{i}": sum(any(outcomes[case_id][:i]) for case_id in expected) / n
                for i in (1, 2, 3)}
    aggregate_cost = {field: (totals[field] if known[field] == n else None) for field in COST_FIELDS}
    known_subtotals = {field: (totals[field] if known[field] else None) for field in COST_FIELDS}
    coverage = {field: {"measured_cases": known[field], "total_cases": n}
                for field in COST_FIELDS}
    boot = {field: _finite_nonnegative((bootstrap_cost or {}).get(field), f"bootstrap {field}")
            for field in COST_FIELDS}
    aggregate_with_bootstrap = {field: (aggregate_cost[field] + boot[field]
                                        if aggregate_cost[field] is not None and boot[field] is not None else None)
                                for field in COST_FIELDS}
    amortized = {field: (aggregate_with_bootstrap[field] / n
                         if aggregate_cost[field] is not None and boot[field] is not None else None)
                 for field in COST_FIELDS}
    return {"system": system, "case_count": n, **accuracy,
            "submissions": submissions, "per_case_correct_rank": ranks,
            "per_case_accepted_by_slot": outcomes,
            "aggregate_cost": aggregate_cost, "cost_measurement_coverage": coverage,
            "known_cost_subtotals_not_complete": known_subtotals,
            "bootstrap_cost_separate": boot,
            "aggregate_cost_with_bootstrap": aggregate_with_bootstrap,
            "aggregate_cost_with_bootstrap_amortized_per_case": amortized}


def paired_comparison(results: Mapping[str, Mapping[str, Mapping[str, Any]]], *,
                      expected_case_ids: Sequence[str],
                      bootstrap_costs: Mapping[str, Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Compare systems on the same frozen case list, preserving each paired outcome."""
    if len(results) < 2:
        raise ValueError("at least two systems are required for comparison")
    bootstrap_costs = bootstrap_costs or {}
    summaries = {name: summarize_system(name, cases, expected_case_ids=expected_case_ids,
                                        bootstrap_cost=bootstrap_costs.get(name))
                 for name, cases in results.items()}
    paired = {case_id: {name: summary["per_case_accepted_by_slot"][case_id]
                        for name, summary in summaries.items()}
              for case_id in expected_case_ids}
    return {"schema_version": "e4-m5-comparison/2", "case_ids": list(expected_case_ids),
            "systems": summaries, "paired_accepted_by_slot": paired,
            "metric_definition": "Accuracy@3,i is exact component acceptance in top three within first i diagnosis slots; failed, invalid, and unrun slots are false"}
