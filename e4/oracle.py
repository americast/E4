"""Pluggable acceptance and stopping policies decoupling RCA from benchmark labels."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Mapping, Protocol, Sequence

from .types import EntityRef, RankedAnswer, canonical_component_id


@dataclass(frozen=True, slots=True)
class Decision:
    """Actionable diagnostic decision returned by an AcceptanceOracle."""

    action: Literal["confirm", "revise", "stop_unverified"]
    reason: str
    accepted: bool = False
    score: float | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action == "confirm" and not self.accepted:
            object.__setattr__(self, "accepted", True)

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "accepted": self.accepted,
            "reason": self.reason,
            "score": self.score,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class DiagnosticContext:
    """Run metadata provided to an oracle for decision-making."""

    case_id: str
    stage: str
    attempt: int
    max_attempts: int
    history: tuple[Mapping[str, Any], ...] = ()


class AcceptanceOracle(Protocol):
    """Abstract interface for deciding whether a diagnosis is confirmed, revised, or stopped."""

    def evaluate(self, answer: RankedAnswer, context: DiagnosticContext) -> Decision:
        """Evaluate ranked answer and return confirm, revise, or stop_unverified."""
        ...


class BudgetStopPolicy:
    """Unlabelled production stopping policy: advances stages until budget completes."""

    def __init__(self, max_attempts: int = 3):
        self.max_attempts = max_attempts

    def evaluate(self, answer: RankedAnswer, context: DiagnosticContext) -> Decision:
        if context.attempt >= self.max_attempts:
            return Decision(
                action="stop_unverified",
                reason=f"Diagnostic budget of {self.max_attempts} attempts reached. Returning best unverified candidates.",
                accepted=False,
            )
        return Decision(
            action="revise",
            reason=f"Stage '{context.stage}' completed; advancing to next stage.",
            accepted=False,
        )


class GroundTruthOracle:
    """Benchmark scoring oracle: evaluates candidate ranking against hidden labels."""

    def __init__(
        self,
        labels_by_case: Mapping[str, Sequence[str]] | None = None,
        top_k: int = 3,
        max_attempts: int = 3,
    ):
        self._labels_by_case: dict[str, list[str]] = {}
        if labels_by_case:
            for cid, labels in labels_by_case.items():
                self._labels_by_case[cid] = [canonical_component_id(lbl) for lbl in labels]
        self.top_k = top_k
        self.max_attempts = max_attempts

    def set_labels(self, case_id: str, labels: Sequence[str]) -> None:
        self._labels_by_case[case_id] = [canonical_component_id(lbl) for lbl in labels]

    def evaluate(self, answer: RankedAnswer, context: DiagnosticContext) -> Decision:
        targets = self._labels_by_case.get(context.case_id, [])
        top_candidates = [c.canonical_id for c in answer.candidates[: self.top_k]]

        matched = [tgt for tgt in targets if tgt in top_candidates]
        if matched:
            return Decision(
                action="confirm",
                reason=f"Ground truth target(s) {matched} found in top {self.top_k} candidates.",
                accepted=True,
                details={"matched_targets": matched, "top_k": self.top_k},
            )

        if context.attempt >= self.max_attempts:
            return Decision(
                action="stop_unverified",
                reason=f"Attempt limit ({self.max_attempts}) reached without top-{self.top_k} ground truth match.",
                accepted=False,
                details={"top_candidates": top_candidates, "expected_targets": targets},
            )

        return Decision(
            action="revise",
            reason=f"Top-{self.top_k} candidate did not match ground truth target; escalating to next stage.",
            accepted=False,
            details={"top_candidates": top_candidates},
        )


class HumanInTheLoopOracle:
    """Interactive oracle delegating acceptance to an analyst callback or input."""

    def __init__(self, callback: Callable[[RankedAnswer, DiagnosticContext], Decision]):
        self._callback = callback

    def evaluate(self, answer: RankedAnswer, context: DiagnosticContext) -> Decision:
        return self._callback(answer, context)
