"""E4: Portable, Extensible, and Reusable Root-Cause Analysis Framework."""

from .data.adapter import (
    AdapterConformanceError,
    BaseDatasetAdapter,
    TelemetrySource,
    check_adapter_conformance,
)
from .data.agent_prompt import get_adapter_generation_prompt
from .data.folder import FolderTelemetrySource
from .engine import DiagnosisEngine, setup
from .oracle import (
    AcceptanceOracle,
    BudgetStopPolicy,
    Decision,
    DiagnosticContext,
    GroundTruthOracle,
    HumanInTheLoopOracle,
)
from .types import (
    Aggregate,
    AggregateRow,
    Candidate,
    CandidateSet,
    ColumnSchema,
    DatasetSchema,
    DiagnosticResult,
    EntityRef,
    Evidence,
    ExecutionResult,
    LogRecord,
    MetricRecord,
    RankedAnswer,
    ScoredCandidate,
    ScoredCandidates,
    SpanRecord,
    TableSchema,
    TelemetryRecord,
    Window,
    canonical_component_id,
)

__version__ = "0.2.0"

__all__ = [
    "DiagnosisEngine",
    "setup",
    "TelemetrySource",
    "BaseDatasetAdapter",
    "FolderTelemetrySource",
    "AdapterConformanceError",
    "check_adapter_conformance",
    "get_adapter_generation_prompt",
    "AcceptanceOracle",
    "BudgetStopPolicy",
    "GroundTruthOracle",
    "HumanInTheLoopOracle",
    "Decision",
    "DiagnosticContext",
    "Window",
    "EntityRef",
    "MetricRecord",
    "SpanRecord",
    "LogRecord",
    "TelemetryRecord",
    "ColumnSchema",
    "TableSchema",
    "DatasetSchema",
    "Evidence",
    "Candidate",
    "CandidateSet",
    "AggregateRow",
    "Aggregate",
    "ScoredCandidate",
    "ScoredCandidates",
    "RankedAnswer",
    "ExecutionResult",
    "DiagnosticResult",
    "canonical_component_id",
]
