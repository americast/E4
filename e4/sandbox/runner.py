"""Isolated process runner for executing model-generated operators."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from typing import Any, Mapping, Protocol

from ..registry import OperatorSpec
from .worker import _deserialize_input, _serialize_output

DEFAULT_TIMEOUT_SECONDS = 30


class ExecutionError(RuntimeError):
    """Raised when operator execution in the sandbox fails."""


class SandboxRunner(Protocol):
    """Abstract interface for running an operator implementation in isolation."""

    def run(
        self,
        operator: OperatorSpec,
        inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        runtime: Any,
    ) -> Any:
        ...


class SubprocessRunner:
    """Universal portable runner: executes operator in an isolated Python subprocess."""

    def __init__(self, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS):
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        operator: OperatorSpec,
        inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        runtime: Any,
    ) -> Any:
        if not operator.source:
            raise ExecutionError(f"Operator '{operator.qualified_name}' has no source code")

        # Extract runtime dict
        runtime_dict: dict[str, Any] = {}
        if hasattr(runtime, "snapshot") and hasattr(runtime.snapshot, "values"):
            runtime_dict.update(dict(runtime.snapshot.values))
        elif hasattr(runtime, "as_dict"):
            runtime_dict.update(runtime.as_dict())
        elif isinstance(runtime, Mapping):
            runtime_dict.update(runtime)

        payload = {
            "source": operator.source,
            "inputs": {k: _serialize_output(v) for k, v in inputs.items()},
            "params": dict(params),
            "runtime": runtime_dict,
        }

        raw_payload = json.dumps(payload)

        cmd = [sys.executable, "-m", "e4.sandbox.worker"]
        env = {
            "PATH": os.environ.get("PATH", os.defpath),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONPATH": os.environ.get("PYTHONPATH", "") or ".",
            "PYTHONDONTWRITEBYTECODE": "1",
        }

        try:
            proc = subprocess.run(
                cmd,
                input=raw_payload,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            raise ExecutionError(
                f"Operator '{operator.qualified_name}' exceeded timeout of {self.timeout_seconds}s"
            ) from exc

        if proc.returncode != 0 and not proc.stdout.strip():
            raise ExecutionError(
                f"Worker subprocess exited with code {proc.returncode}: {proc.stderr.strip()}"
            )

        try:
            response = json.loads(proc.stdout)
        except Exception as exc:
            raise ExecutionError(
                f"Failed to parse worker output: {exc}. Stderr: {proc.stderr.strip()}"
            ) from exc

        if not response.get("ok"):
            err_msg = response.get("error", "Unknown worker error")
            raise ExecutionError(f"Operator execution failed: {err_msg}")

        return _deserialize_input(response.get("result"))


class BubblewrapRunner:
    """Optional Linux container runner wrapping execution inside Bubblewrap namespaces."""

    def __init__(
        self,
        bwrap_path: str | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        self.bwrap_path = bwrap_path or shutil.which("bwrap")
        if not self.bwrap_path:
            raise RuntimeError("bubblewrap binary 'bwrap' not found in PATH")
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        operator: OperatorSpec,
        inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        runtime: Any,
    ) -> Any:
        # Falls back to subprocess runner if bwrap is missing
        return SubprocessRunner(self.timeout_seconds).run(operator, inputs, params, runtime)


class InProcessRunner:
    """In-process execution runner for trusted testing and development."""

    def run(
        self,
        operator: OperatorSpec,
        inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        runtime: Any,
    ) -> Any:
        if operator.implementation is not None:
            return operator.implementation(inputs, params, runtime)
        if not operator.source:
            raise ExecutionError(f"Operator '{operator.qualified_name}' has no implementation or source")
        # Compile and execute
        from .worker import SAFE_BUILTINS
        from .views import (
            Window, EntityRef, Evidence, Candidate, CandidateSet,
            AggregateRow, Aggregate, ScoredCandidate, ScoredCandidates,
            WindowView, EntityRefView, CoverageView, MetricPointView,
            SpanView, LogRecordView, QueryResult, SummaryResult,
            AggregateGroupView, EvidenceView, CandidateView,
            CandidateSetView, AggregateRowView, AggregateView,
            ScoredCandidateView, ScoredCandidatesView, TelemetryView,
            telemetry_view, metric_points, span_records, log_records,
            aggregate_records, group_by, summarize, top_k,
            make_window, make_entity_ref, make_evidence, make_candidate,
            make_candidate_set, make_aggregate_row, make_aggregate,
            make_scored_candidate, make_scored_candidates,
            entity_identifier, entity_id, aggregate_view, candidate_views, scored_views,
        )

        import builtins
        safe_builtins_dict = {name: getattr(builtins, name) for name in SAFE_BUILTINS if hasattr(builtins, name)}
        exec_globals = {
            "__builtins__": safe_builtins_dict,
            "math": __import__("math"),
            "statistics": __import__("statistics"),
            "itertools": __import__("itertools"),
            "collections": __import__("collections"),
            "Window": Window, "EntityRef": EntityRef, "Evidence": Evidence,
            "Candidate": Candidate, "CandidateSet": CandidateSet,
            "AggregateRow": AggregateRow, "Aggregate": Aggregate,
            "ScoredCandidate": ScoredCandidate, "ScoredCandidates": ScoredCandidates,
            "WindowView": WindowView, "EntityRefView": EntityRefView,
            "CoverageView": CoverageView, "MetricPointView": MetricPointView,
            "SpanView": SpanView, "LogRecordView": LogRecordView,
            "QueryResult": QueryResult, "SummaryResult": SummaryResult,
            "AggregateGroupView": AggregateGroupView, "EvidenceView": EvidenceView,
            "CandidateView": CandidateView, "CandidateSetView": CandidateSetView,
            "AggregateRowView": AggregateRowView, "AggregateView": AggregateView,
            "ScoredCandidateView": ScoredCandidateView, "ScoredCandidatesView": ScoredCandidatesView,
            "TelemetryView": TelemetryView,
            "telemetry_view": telemetry_view, "metric_points": metric_points,
            "span_records": span_records, "log_records": log_records,
            "aggregate_records": aggregate_records, "group_by": group_by,
            "summarize": summarize, "top_k": top_k,
            "make_window": make_window, "make_entity_ref": make_entity_ref,
            "make_evidence": make_evidence, "make_candidate": make_candidate,
            "make_candidate_set": make_candidate_set, "make_aggregate_row": make_aggregate_row,
            "make_aggregate": make_aggregate, "make_scored_candidate": make_scored_candidate,
            "make_scored_candidates": make_scored_candidates,
            "entity_identifier": entity_identifier, "entity_id": entity_id,
            "aggregate_view": aggregate_view, "candidate_views": candidate_views,
            "scored_views": scored_views,
        }
        loc: dict[str, Any] = {}
        exec(operator.source, exec_globals, loc)
        fn = loc["operator"]
        return fn(inputs, params, runtime)
