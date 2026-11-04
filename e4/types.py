"""Canonical typed records, schemas, and runtime contracts for E4."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Iterator, Literal, Mapping, Sequence, Union


def _freeze_mapping(mapping: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if mapping is None:
        return MappingProxyType({})
    result: dict[str, Any] = {}
    for key, value in mapping.items():
        if isinstance(value, Mapping):
            result[str(key)] = _freeze_mapping(value)
        elif isinstance(value, (list, tuple)):
            result[str(key)] = tuple(value)
        else:
            result[str(key)] = value
    return MappingProxyType(result)


def canonical_component_id(identifier: str) -> str:
    """Normalize formatting only; does not infer aliases or equivalence."""
    return identifier.strip().casefold()


@dataclass(frozen=True, slots=True, order=True)
class Window:
    """A half-open time interval [start_ms_utc, end_ms_utc) with an assigned role."""

    name: Literal["reference", "alert"]
    start_ms_utc: int
    end_ms_utc: int

    def __post_init__(self) -> None:
        if type(self.start_ms_utc) is not int or type(self.end_ms_utc) is not int:
            raise ValueError("Window epoch boundaries must be integer UTC milliseconds")
        if self.name not in {"reference", "alert"}:
            raise ValueError("Window name must be 'reference' or 'alert'")
        if self.end_ms_utc <= self.start_ms_utc:
            raise ValueError("Window must have positive half-open duration (end > start)")

    def contains(self, timestamp_ms_utc: int) -> bool:
        return self.start_ms_utc <= timestamp_ms_utc < self.end_ms_utc

    @property
    def duration_ms(self) -> int:
        return self.end_ms_utc - self.start_ms_utc

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "start_ms_utc": self.start_ms_utc,
            "end_ms_utc": self.end_ms_utc,
        }


@dataclass(frozen=True, slots=True, order=True)
class EntityRef:
    """Canonical typed entity identity; role avoids identifier collisions across tiers."""

    role: str
    identifier: str

    def __post_init__(self) -> None:
        if not self.role or not self.identifier:
            raise ValueError("Entity role and identifier must be non-empty strings")

    @property
    def canonical_id(self) -> str:
        return canonical_component_id(self.identifier)

    def as_dict(self) -> dict[str, Any]:
        return {"role": self.role, "identifier": self.identifier}


# -----------------------------------------------------------------------------
# Dataset & Telemetry Schemas
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ColumnSchema:
    name: str
    data_type: str  # "string", "number", "integer", "boolean", "timestamp"
    nullable: bool = False
    unit: str | None = None
    description: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "unit": self.unit,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class TableSchema:
    name: str
    record_type: Literal["metric", "span", "log"]
    columns: tuple[ColumnSchema, ...]
    entity_roles: tuple[str, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "entity_roles", tuple(self.entity_roles))

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "record_type": self.record_type,
            "columns": [col.as_dict() for col in self.columns],
            "entity_roles": list(self.entity_roles),
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    """Declares available tables, columns, entity roles, and time formatting."""

    tables: tuple[TableSchema, ...]
    time_unit: str = "ms_utc"
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "tables", tuple(self.tables))

    def get_table(self, name: str) -> TableSchema | None:
        for t in self.tables:
            if t.name == name:
                return t
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "tables": [t.as_dict() for t in self.tables],
            "time_unit": self.time_unit,
            "description": self.description,
        }


# -----------------------------------------------------------------------------
# Canonical Observability Records (Streamed via TelemetrySource)
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class MetricRecord:
    source_table: str
    timestamp_ms_utc: int
    entity: EntityRef
    metric_name: str
    value: float | None
    unit: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "timestamp_ms_utc": self.timestamp_ms_utc,
            "entity": self.entity.as_dict(),
            "metric_name": self.metric_name,
            "value": self.value,
            "unit": self.unit,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class SpanRecord:
    source_table: str
    timestamp_ms_utc: int
    trace_id: str
    span_id: str
    parent_id: str | None
    duration_ms: float | None
    success: bool | None
    call_type: str | None = None
    executing_component: EntityRef | None = None
    logical_service: EntityRef | None = None
    database_target: EntityRef | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.timestamp_ms_utc) is not int:
            raise ValueError("Span timestamp must be UTC milliseconds")
        object.__setattr__(self, "details", _freeze_mapping(self.details))

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "timestamp_ms_utc": self.timestamp_ms_utc,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "duration_ms": self.duration_ms,
            "success": self.success,
            "call_type": self.call_type,
            "executing_component": self.executing_component.as_dict() if self.executing_component else None,
            "logical_service": self.logical_service.as_dict() if self.logical_service else None,
            "database_target": self.database_target.as_dict() if self.database_target else None,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class LogRecord:
    source_table: str
    timestamp_ms_utc: int
    entity: EntityRef | None = None
    log_name: str | None = None
    value: str | None = None
    severity: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.timestamp_ms_utc) is not int:
            raise ValueError("Log timestamp must be UTC milliseconds")
        object.__setattr__(self, "details", _freeze_mapping(self.details))

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "timestamp_ms_utc": self.timestamp_ms_utc,
            "entity": self.entity.as_dict() if self.entity else None,
            "log_name": self.log_name,
            "value": self.value,
            "severity": self.severity,
            "details": dict(self.details),
        }


TelemetryRecord = Union[MetricRecord, SpanRecord, LogRecord]


# -----------------------------------------------------------------------------
# Analysis & Diagnostic Output Contracts
# -----------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Evidence:
    source: str
    window: Window
    statistic: str
    value: float | str | bool | None
    contributing_entities: tuple[EntityRef, ...]
    support: int
    ranking_eligible: bool = True
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "contributing_entities", tuple(self.contributing_entities))
        object.__setattr__(self, "details", _freeze_mapping(self.details))

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "window": self.window.as_dict(),
            "statistic": self.statistic,
            "value": self.value,
            "contributing_entities": [e.as_dict() for e in self.contributing_entities],
            "support": self.support,
            "ranking_eligible": self.ranking_eligible,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class Candidate:
    entity: EntityRef
    rationale: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {"entity": self.entity.as_dict(), "rationale": self.rationale}


@dataclass(frozen=True, slots=True)
class CandidateSet:
    candidates: tuple[Candidate, ...]
    source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidates": [c.as_dict() for c in self.candidates],
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class AggregateRow:
    entity: EntityRef
    metric_key: str
    window: Window
    statistic: str
    value: float | None
    support: int
    source_tables: tuple[str, ...] = ()
    contributing_entities: tuple[EntityRef, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_tables", tuple(self.source_tables))
        object.__setattr__(self, "contributing_entities", tuple(self.contributing_entities))
        object.__setattr__(self, "details", _freeze_mapping(self.details))

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity.as_dict(),
            "metric_key": self.metric_key,
            "window": self.window.as_dict(),
            "statistic": self.statistic,
            "value": self.value,
            "support": self.support,
            "source_tables": list(self.source_tables),
            "contributing_entities": [e.as_dict() for e in self.contributing_entities],
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class Aggregate:
    window: Window
    rows: tuple[AggregateRow, ...]
    source_tables: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "rows", tuple(self.rows))
        object.__setattr__(self, "source_tables", tuple(self.source_tables))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))

    def as_dict(self) -> dict[str, Any]:
        return {
            "window": self.window.as_dict(),
            "rows": [r.as_dict() for r in self.rows],
            "source_tables": list(self.source_tables),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class ScoredCandidate:
    entity: EntityRef
    score: float
    category: str | None = None
    evidence: tuple[Evidence, ...] = ()
    analysis_family: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity.as_dict(),
            "score": self.score,
            "category": self.category,
            "evidence": [e.as_dict() for e in self.evidence],
            "analysis_family": self.analysis_family,
        }


@dataclass(frozen=True, slots=True)
class ScoredCandidates:
    candidates: tuple[ScoredCandidate, ...]
    scorer: str
    status: Literal["ready", "abstained"] = "ready"
    status_details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        object.__setattr__(self, "status_details", _freeze_mapping(self.status_details))

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidates": [c.as_dict() for c in self.candidates],
            "scorer": self.scorer,
            "status": self.status,
            "status_details": dict(self.status_details),
        }


@dataclass(frozen=True, slots=True)
class RankedAnswer:
    candidates: tuple[EntityRef, ...]
    scores: tuple[float, ...] = ()
    stage: str = ""
    evidence: tuple[Evidence, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        object.__setattr__(self, "scores", tuple(self.scores))
        object.__setattr__(self, "evidence", tuple(self.evidence))

    @property
    def top_candidate(self) -> EntityRef | None:
        return self.candidates[0] if self.candidates else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidates": [c.as_dict() for c in self.candidates],
            "scores": list(self.scores),
            "stage": self.stage,
            "evidence": [e.as_dict() for e in self.evidence],
        }


@dataclass(frozen=True, slots=True)
class NodeRecord:
    node_id: str
    operator: str
    inputs: Mapping[str, Any]
    params: Mapping[str, Any]
    elapsed_ms: int = 0
    output_type: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "operator": self.operator,
            "inputs": dict(self.inputs),
            "params": dict(self.params),
            "elapsed_ms": self.elapsed_ms,
            "output_type": self.output_type,
        }


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    return_node: str
    return_value: Any
    records: tuple[NodeRecord, ...] = ()
    elapsed_ms: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", tuple(self.records))

    def as_dict(self) -> dict[str, Any]:
        val = self.return_value
        if hasattr(val, "as_dict"):
            val = val.as_dict()
        return {
            "return_node": self.return_node,
            "return_value": val,
            "records": [r.as_dict() for r in self.records],
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass(frozen=True, slots=True)
class DiagnosticResult:
    case_id: str
    status: Literal["confirmed", "unverified", "exhausted", "error"]
    stage: str
    top_candidates: tuple[EntityRef, ...]
    answer: RankedAnswer | None = None
    admitted_operators: tuple[str, ...] = ()
    admitted_playbook: str | None = None
    total_calls: int = 0
    history: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "top_candidates", tuple(self.top_candidates))
        object.__setattr__(self, "admitted_operators", tuple(self.admitted_operators))
        object.__setattr__(self, "history", tuple(self.history))

    def as_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "status": self.status,
            "stage": self.stage,
            "top_candidates": [c.as_dict() for c in self.top_candidates],
            "answer": self.answer.as_dict() if self.answer else None,
            "admitted_operators": list(self.admitted_operators),
            "admitted_playbook": self.admitted_playbook,
            "total_calls": self.total_calls,
            "history": [dict(h) for h in self.history],
        }
