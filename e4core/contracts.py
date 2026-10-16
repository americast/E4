"""Immutable values crossing adapter, operator, executor, and answer boundaries."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True, order=True)
class Window:
    name: str
    start_ms_utc: int
    end_ms_utc: int

    def __post_init__(self) -> None:
        if type(self.start_ms_utc) is not int or type(self.end_ms_utc) is not int:
            raise ValueError("window epoch boundaries must be integer UTC milliseconds")
        if self.name not in {"reference", "alert"}:
            raise ValueError("window name must be 'reference' or 'alert'")
        if self.end_ms_utc <= self.start_ms_utc:
            raise ValueError("window must have positive half-open duration")

    def contains(self, timestamp_ms_utc: int) -> bool:
        return self.start_ms_utc <= timestamp_ms_utc < self.end_ms_utc


@dataclass(frozen=True, slots=True, order=True)
class EntityRef:
    """Canonical identity; role remains part of the identity to avoid ID collisions."""

    role: str
    identifier: str

    def __post_init__(self) -> None:
        if not self.role or not self.identifier:
            raise ValueError("entity role and identifier must be nonempty")

    @property
    def canonical_id(self) -> str:
        return canonical_component_id(self.identifier)


def canonical_component_id(identifier: str) -> str:
    """Normalize formatting only; does not infer aliases or deployment equivalence."""
    return identifier.strip().casefold()


@dataclass(frozen=True, slots=True)
class MetricPoint:
    table: str
    timestamp_ms_utc: int
    entity: EntityRef
    metric_name: str
    bomc_id: str | None
    item_id: str | None
    value: float | None
    raw_value: str
    row_key: str

    def __post_init__(self) -> None:
        if type(self.timestamp_ms_utc) is not int:
            raise ValueError("metric point timestamp must be UTC milliseconds")
        if self.value is not None and not math.isfinite(self.value):
            raise ValueError("metric point value must be finite or missing")


@dataclass(frozen=True, slots=True)
class Span:
    trace_id: str
    span_id: str
    parent_id: str | None
    start_ms_utc: int
    elapsed_raw: str
    success: bool | None
    success_status: str
    success_raw: str
    call_type: str
    executing_component: EntityRef | None
    logical_service: EntityRef | None
    database_target: EntityRef | None
    parent_resolved_in_clip: bool
    parent_unresolved_in_clip: bool

    def __post_init__(self) -> None:
        if type(self.start_ms_utc) is not int:
            raise ValueError("span start timestamp must be UTC milliseconds")
        if self.success_status not in {"valid", "missing", "invalid"}:
            raise ValueError("invalid success status")
        if (self.success_status == "valid") != (self.success is not None):
            raise ValueError("success value and parse status disagree")


@dataclass(frozen=True, slots=True)
class Telemetry:
    case_id: str
    reference_window: Window
    alert_window: Window
    metrics: tuple[MetricPoint, ...]
    spans: tuple[Span, ...]
    deployment_catalog: Mapping[str, Mapping[str, Any]]
    data_sha256: str
    file_sha256: Mapping[str, str]
    duplicate_counts: Mapping[str, int]
    quality_counts: Mapping[str, int]
    adapter_version: str
    file_bytes: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", tuple(self.metrics))
        object.__setattr__(self, "spans", tuple(self.spans))
        object.__setattr__(self, "deployment_catalog", _freeze_mapping(self.deployment_catalog))
        object.__setattr__(self, "file_sha256", _freeze_mapping(self.file_sha256))
        object.__setattr__(self, "file_bytes", _freeze_mapping(self.file_bytes))
        object.__setattr__(self, "duplicate_counts", _freeze_mapping(self.duplicate_counts))
        object.__setattr__(self, "quality_counts", _freeze_mapping(self.quality_counts))
        if self.reference_window.name != "reference" or self.alert_window.name != "alert":
            raise ValueError("telemetry windows have incorrect roles")
        if self.reference_window.end_ms_utc != self.alert_window.start_ms_utc:
            raise ValueError("reference and alert windows must be adjacent half-open intervals")


@dataclass(frozen=True, slots=True)
class AggregateRow:
    entity: EntityRef
    metric_key: str
    window: Window
    statistic: str
    value: float | None
    support: int
    source_tables: tuple[str, ...]
    contributing_entities: tuple[EntityRef, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_tables", tuple(self.source_tables))
        object.__setattr__(self, "contributing_entities", tuple(self.contributing_entities))
        object.__setattr__(self, "details", _freeze_mapping(self.details))
        if self.support < 0:
            raise ValueError("aggregate support cannot be negative")
        if self.value is not None and not math.isfinite(self.value):
            raise ValueError("aggregate value must be finite or missing")


@dataclass(frozen=True, slots=True)
class Aggregate:
    """One cached envelope for all entity/metric rows in a window."""
    window: Window
    rows: tuple[AggregateRow, ...]
    source_tables: tuple[str, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "rows", tuple(self.rows))
        object.__setattr__(self, "source_tables", tuple(self.source_tables))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))
        if any(row.window != self.window for row in self.rows):
            raise ValueError("aggregate rows must match the envelope's single window")
        identities = [(row.entity.role, row.entity.canonical_id, row.metric_key,
                       row.window.name, row.statistic)
                      for row in self.rows]
        if len(set(identities)) != len(identities):
            raise ValueError("aggregate envelope rows must have unique entity/metric/statistic keys")


@dataclass(frozen=True, slots=True)
class Evidence:
    source: str
    window: Window
    statistic: str
    value: float | str | bool | None
    contributing_entities: tuple[EntityRef, ...]
    support: int
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "contributing_entities", tuple(self.contributing_entities))
        object.__setattr__(self, "details", _freeze_mapping(self.details))
        if self.support < 0:
            raise ValueError("evidence support cannot be negative")
        if isinstance(self.value, float) and not math.isfinite(self.value):
            raise ValueError("evidence value must be finite")


@dataclass(frozen=True, slots=True)
class Candidate:
    entity: EntityRef
    rationale: str = ""


@dataclass(frozen=True, slots=True)
class CandidateSet:
    candidates: tuple[Candidate, ...]
    source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        keys = [(item.entity.role, item.entity.identifier) for item in self.candidates]
        if len(set(keys)) != len(keys):
            raise ValueError("candidate entity identities must be unique within a set")


@dataclass(frozen=True, slots=True)
class ScoredCandidate:
    entity: EntityRef
    score: float
    category: str | None
    evidence: tuple[Evidence, ...]
    analysis_family: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))
        if not math.isfinite(self.score):
            raise ValueError("candidate score must be finite")
        if self.category not in {None, "CPU fault", "db close", "db connection limit",
                                 "network delay", "network loss"}:
            raise ValueError("category must be a supported source fault category or unknown/null")


@dataclass(frozen=True, slots=True)
class ScoredCandidates:
    candidates: tuple[ScoredCandidate, ...]
    scorer: str
    status: str = "ready"
    status_details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        object.__setattr__(self, "status_details", _freeze_mapping(self.status_details))
        if self.status not in {"ready", "abstained"}:
            raise ValueError("scored candidates status must be ready or abstained")
        keys = [(item.entity.role, item.entity.identifier) for item in self.candidates]
        if len(set(keys)) != len(keys):
            raise ValueError("scored entity identities must be unique within a scorer output")


@dataclass(frozen=True, slots=True)
class RankedAnswer:
    leads: tuple[ScoredCandidate, ...]
    acceptance_contract: str = "component_identity_top3"
    secondary_contract: str = "component_and_category_top3"

    def __post_init__(self) -> None:
        object.__setattr__(self, "leads", tuple(self.leads))
        if self.acceptance_contract != "component_identity_top3":
            raise ValueError("primary acceptance contract is frozen to component identity in top 3")
        identities = [item.entity.canonical_id for item in self.leads]
        if len(set(identities)) != len(identities):
            raise ValueError("ranked answer has duplicate entity leads")
        ordering = [(-item.score, item.entity.canonical_id, item.entity.role) for item in self.leads]
        if ordering != sorted(ordering):
            raise ValueError("ranked leads must use descending score then canonical ID/role tie order")


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    values: Mapping[str, Any]
    provenance: Mapping[str, str]
    schema_version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", _freeze_mapping(self.values))
        object.__setattr__(self, "provenance", _freeze_mapping(self.provenance))


@dataclass(frozen=True, slots=True)
class NodeRecord:
    node_id: str
    operator: str
    version: str
    status: str
    input_digests: Mapping[str, str | tuple[str, ...]]
    resolved_params: Mapping[str, Any]
    parameter_provenance: Mapping[str, str]
    runtime_snapshot: Mapping[str, Any]
    started_at: str
    elapsed_ms: float
    rows_processed: int
    bytes_processed: int
    cache_key: str
    cache_status: str
    output_digest: str | None
    implementation_fingerprint: str
    error: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        for name in ("input_digests", "resolved_params", "parameter_provenance", "runtime_snapshot"):
            object.__setattr__(self, name, _freeze_mapping(getattr(self, name)))
        if self.error is not None:
            object.__setattr__(self, "error", _freeze_mapping(self.error))


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    answer: RankedAnswer
    records: tuple[NodeRecord, ...]
    cache_hits: int
    case_id: str
    implementation_fingerprint: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", tuple(self.records))


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    from types import MappingProxyType
    def freeze(item: Any) -> Any:
        if isinstance(item, Mapping):
            return MappingProxyType({key: freeze(child) for key, child in item.items()})
        if isinstance(item, (list, tuple)):
            return tuple(freeze(child) for child in item)
        if isinstance(item, set):
            return frozenset(freeze(child) for child in item)
        return item
    return MappingProxyType({key: freeze(child) for key, child in value.items()})
