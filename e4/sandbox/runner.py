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
from .worker import (
    _deserialize_input,
    _safe_builtins,
    _serialize_output,
    _validate_runtime_output_type,
)

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

    def __init__(
        self,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        memory_limit_mb: int = 512,
    ):
        self.timeout_seconds = timeout_seconds
        self.memory_limit_mb = memory_limit_mb

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
            "output_type": operator.output_type,
            "memory_limit_mb": self.memory_limit_mb,
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
        memory_limit_mb: int = 512,
    ):
        self.bwrap_path = bwrap_path or shutil.which("bwrap")
        if not self.bwrap_path:
            raise RuntimeError("bubblewrap binary 'bwrap' not found in PATH")
        self.timeout_seconds = timeout_seconds
        self.memory_limit_mb = memory_limit_mb

    def run(
        self,
        operator: OperatorSpec,
        inputs: Mapping[str, Any],
        params: Mapping[str, Any],
        runtime: Any,
    ) -> Any:
        if not operator.source:
            raise ExecutionError(f"Operator '{operator.qualified_name}' has no source code")
        if any(_contains_file_backed_telemetry(value) for value in inputs.values()):
            raise ExecutionError(
                "BubblewrapRunner does not yet mount file-backed telemetry stores; "
                "select SubprocessRunner for this case or provide in-memory telemetry"
            )

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
            "output_type": operator.output_type,
            "memory_limit_mb": self.memory_limit_mb,
        }

        raw_payload = json.dumps(payload)

        # Bubblewrap execution
        cmd = [
            self.bwrap_path,
            "--ro-bind", "/", "/",
            "--tmpfs", "/tmp",
            "--proc", "/proc",
            "--dev", "/dev",
            "--unshare-all",
            "--die-with-parent",
            sys.executable,
            "-m",
            "e4.sandbox.worker",
        ]
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
                f"Bubblewrap operator '{operator.qualified_name}' exceeded timeout of {self.timeout_seconds}s"
            ) from exc

        if proc.returncode != 0 and not proc.stdout.strip():
            raise ExecutionError(
                f"Bubblewrap worker exited with code {proc.returncode}: {proc.stderr.strip()}"
            )

        try:
            response = json.loads(proc.stdout)
        except Exception as exc:
            raise ExecutionError(
                f"Failed to parse bubblewrap worker output: {exc}. Stderr: {proc.stderr.strip()}"
            ) from exc

        if not response.get("ok"):
            err_msg = response.get("error", "Unknown bubblewrap worker error")
            raise ExecutionError(f"Bubblewrap operator execution failed: {err_msg}")

        return _deserialize_input(response.get("result"))


def _contains_file_backed_telemetry(value: Any) -> bool:
    """Detect a trusted store before serialization so container isolation cannot drop it."""
    if getattr(value, "_record_store", None) is not None:
        return True
    if isinstance(value, Mapping):
        return any(_contains_file_backed_telemetry(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_file_backed_telemetry(item) for item in value)
    return False


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
            res = operator.implementation(inputs, params, runtime)
            _validate_runtime_output_type(res, operator.output_type)
            return res
        if not operator.source:
            raise ExecutionError(f"Operator '{operator.qualified_name}' has no implementation or source")
        # Compile and execute
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

        exec_globals = {
            "__builtins__": _safe_builtins(),
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
        exec(operator.source, exec_globals, exec_globals)
        fn = exec_globals["operator"]
        res = fn(inputs, params, runtime)
        _validate_runtime_output_type(res, operator.output_type)
        return res
