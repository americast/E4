"""Judge a playbook's per-run answers against a labeled behavior: component, then time.

A lead hits when its component matches and its intervals reach both
    recall    = |P ∩ T| / |T|   how much of the true behavior it points to
    precision = |P ∩ T| / |P|   how much of what it points to is the true behavior
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from .contracts import EntityRef, RankedAnswer

Intervals = tuple[tuple[int, int], ...]


@dataclass(frozen=True, slots=True)
class GroundTruth:
    """One behavior in a scope; runs maps each run in scope to its true intervals (empty: absent)."""

    scope: Mapping[str, str]          # model, harness, environment
    component: EntityRef
    reason: str
    runs: Mapping[str, Intervals] = field(default_factory=dict)


def merge(intervals: Intervals) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def total_length(intervals: list[tuple[int, int]]) -> int:
    total = 0
    for start, end in intervals:
        total += end - start
    return total


def overlap_length(first: list[tuple[int, int]], second: list[tuple[int, int]]) -> int:
    """Both lists merged and sorted; walk them together, advancing whichever ends first."""
    total = 0
    i = 0
    j = 0
    while i < len(first) and j < len(second):
        start = max(first[i][0], second[j][0])
        end = min(first[i][1], second[j][1])
        if end > start:
            total += end - start
        if first[i][1] < second[j][1]:
            i += 1
        else:
            j += 1
    return total


def recall_precision(predicted: Intervals, truth: Intervals) -> tuple[float, float]:
    """truth must be nonempty; predicting no time gives precision 0."""
    predicted_merged = merge(predicted)
    truth_merged = merge(truth)
    overlap = overlap_length(predicted_merged, truth_merged)
    recall = overlap / total_length(truth_merged)
    predicted_length = total_length(predicted_merged)
    if predicted_length == 0:
        return recall, 0.0
    return recall, overlap / predicted_length


def same_component(entity: EntityRef, component: EntityRef) -> bool:
    return entity.role == component.role and entity.canonical_id == component.canonical_id


def run_passes(truth: GroundTruth, run_id: str, answer: RankedAnswer, n: int = 3,
               min_recall: float = 0.5, min_precision: float = 0.5) -> bool:
    true_intervals = truth.runs[run_id]
    matching = []
    for lead in answer.leads[:n]:
        if same_component(lead.entity, truth.component):
            matching.append(lead)

    # Behavior absent from this run: pass unless a matching lead points at any time.
    if not true_intervals:
        for lead in matching:
            if lead.intervals:
                return False
        return True

    for lead in matching:
        recall, precision = recall_precision(lead.intervals or (), true_intervals)
        if recall >= min_recall and precision >= min_precision:
            return True
    return False


def run_report(truth: GroundTruth, run_id: str, answer: RankedAnswer, n: int = 3,
               min_recall: float = 0.5, min_precision: float = 0.5) -> dict:
    """For people, never for the model: each top-n lead on the true component with its recall and
    precision in percent, and whether the run passed."""
    true_intervals = truth.runs[run_id]
    leads = []
    for rank, lead in enumerate(answer.leads[:n], start=1):
        if not same_component(lead.entity, truth.component):
            continue
        report = {"rank": rank, "component": f"{lead.entity.role}:{lead.entity.identifier}"}
        if true_intervals:
            recall, precision = recall_precision(lead.intervals or (), true_intervals)
            report["recall_percent"] = round(100 * recall, 1)
            report["precision_percent"] = round(100 * precision, 1)
        else:
            report["flags_time_in_clean_run"] = bool(lead.intervals)
        leads.append(report)
    return {"behavior_present": bool(true_intervals), "true_interval_count": len(true_intervals),
            "true_component_leads": leads,
            "passed": run_passes(truth, run_id, answer, n, min_recall, min_precision)}


def judge(truth: GroundTruth, answers: Mapping[str, RankedAnswer], n: int = 3,
          min_recall: float = 0.5, min_precision: float = 0.5) -> bool:
    """answers maps every run in scope to the playbook's answer on that run's Case."""
    for run_id in truth.runs:
        if run_id not in answers:
            raise ValueError(f"no answer for run in scope: {run_id}")
        if not run_passes(truth, run_id, answers[run_id], n, min_recall, min_precision):
            return False
    return True


if __name__ == "__main__":
    from .contracts import ScoredCandidate

    MINUTE = 60_000
    context = EntityRef("model", "context")
    scan = EntityRef("harness", "Scan")

    def lead(entity: EntityRef, score: float, intervals: Intervals | None) -> ScoredCandidate:
        return ScoredCandidate(entity, score, None, (), intervals=intervals)

    def answer(*leads: ScoredCandidate) -> RankedAnswer:
        return RankedAnswer(leads, acceptance_contract="component_time_top3")

    # Synthetic label: context loss twice in t0, once in t2, never in t1.
    truth = GroundTruth({"model": "qwen38", "harness": "incalmo", "environment": "enterprise_b"},
                        context, "context loss",
                        {"t0": ((10 * MINUTE, 15 * MINUTE), (40 * MINUTE, 45 * MINUTE)),
                         "t1": (),
                         "t2": ((5 * MINUTE, 20 * MINUTE),)})

    # Interval math: 5 of 10 true minutes found, 5 of 15 flagged minutes true.
    recall, precision = recall_precision(((5 * MINUTE, 15 * MINUTE), (50 * MINUTE, 55 * MINUTE)),
                                         truth.runs["t0"])
    assert (recall, round(precision, 4)) == (0.5, 0.3333)
    assert merge(((3, 5), (1, 4), (7, 8))) == [(1, 5), (7, 8)]

    good = {"t0": answer(lead(scan, 0.9, None),
                         lead(context, 0.8, ((9 * MINUTE, 16 * MINUTE), (40 * MINUTE, 44 * MINUTE)))),
            "t1": answer(lead(scan, 0.9, None)),
            "t2": answer(lead(context, 0.7, ((6 * MINUTE, 22 * MINUTE),)))}
    assert judge(truth, good)

    # Each failure on its own: wrong time, flagging a clean run, component outside top n.
    wrong_time = dict(good, t2=answer(lead(context, 0.7, ((30 * MINUTE, 40 * MINUTE),))))
    flags_clean = dict(good, t1=answer(lead(context, 0.5, ((1 * MINUTE, 2 * MINUTE),))))
    fourth = dict(good, t2=answer(lead(EntityRef("environment.host", "attacker"), 0.9, None),
                                  lead(EntityRef("environment.host", "webserver0"), 0.8, None),
                                  lead(scan, 0.7, None),
                                  lead(context, 0.6, ((6 * MINUTE, 22 * MINUTE),))))
    assert not judge(truth, wrong_time)
    assert not judge(truth, flags_clean)
    assert not judge(truth, fourth)
    print("cyber judge checks pass")
