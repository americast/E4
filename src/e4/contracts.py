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
class LogRecord:
    """One log record kept whole: its parsed fields (any JSON value) and its raw text."""

    table: str
    timestamp_ms_utc: int
    entity: EntityRef
    kind: str
    fields: Mapping[str, Any]
    text: str
    row_key: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", _freeze_mapping(self.fields))
        if type(self.timestamp_ms_utc) is not int:
            raise ValueError("log record timestamp must be UTC milliseconds")


@dataclass(frozen=True, slots=True)
class Telemetry:
    """One experiment's data and its time span. The reference/alert windows are the older,
    Bank-style way of windowing; telemetry built for a Case leaves them None."""
    case_id: str
    reference_window: Window | None
    alert_window: Window | None
    metrics: tuple[MetricPoint, ...]
    spans: tuple[Span, ...]
    deployment_catalog: Mapping[str, Mapping[str, Any]]
    data_sha256: str
    file_sha256: Mapping[str, str]
    duplicate_counts: Mapping[str, int]
    quality_counts: Mapping[str, int]
    adapter_version: str
    file_bytes: Mapping[str, int] = field(default_factory=dict)
    logs: tuple[LogRecord, ...] = ()
    telemetry_id: str = ""
    start_ms: int | None = None       # the experiment's span, half-open: [start_ms, end_ms)
    end_ms: int | None = None
    params: Mapping[str, Any] = field(default_factory=dict)   # e.g. name, model, harness, environment

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", tuple(self.metrics))
        object.__setattr__(self, "spans", tuple(self.spans))
        object.__setattr__(self, "logs", tuple(self.logs))
        object.__setattr__(self, "deployment_catalog", _freeze_mapping(self.deployment_catalog))
        object.__setattr__(self, "file_sha256", _freeze_mapping(self.file_sha256))
        object.__setattr__(self, "file_bytes", _freeze_mapping(self.file_bytes))
        object.__setattr__(self, "duplicate_counts", _freeze_mapping(self.duplicate_counts))
        object.__setattr__(self, "quality_counts", _freeze_mapping(self.quality_counts))
        object.__setattr__(self, "params", _freeze_mapping(self.params))
        if self.reference_window is not None and self.reference_window.name != "reference":
            raise ValueError("telemetry windows have incorrect roles")
        if self.alert_window is not None and self.alert_window.name != "alert":
            raise ValueError("telemetry windows have incorrect roles")
        if (self.start_ms is None) != (self.end_ms is None):
            raise ValueError("telemetry start_ms and end_ms must be given together")
        if self.start_ms is not None and self.end_ms <= self.start_ms:
            raise ValueError("telemetry must have positive half-open duration")


@dataclass(frozen=True, slots=True)
class TelemetryWindow:
    """A time range [start_ms, end_ms) laid over one Telemetry, inside that Telemetry's span."""
    window_id: str
    telemetry: Telemetry
    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if not self.window_id:
            raise ValueError("telemetry window needs an id")
        if self.telemetry.start_ms is None:
            raise ValueError("telemetry window needs telemetry with start_ms and end_ms")
        if self.end_ms <= self.start_ms:
            raise ValueError("telemetry window must have positive half-open duration")
        if self.start_ms < self.telemetry.start_ms or self.end_ms > self.telemetry.end_ms:
            raise ValueError("telemetry window must lie inside its telemetry's span")

    def contains(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms < self.end_ms

    @property
    def metrics(self) -> tuple[MetricPoint, ...]:
        return tuple(p for p in self.telemetry.metrics if self.contains(p.timestamp_ms_utc))

    @property
    def spans(self) -> tuple[Span, ...]:
        return tuple(s for s in self.telemetry.spans if self.contains(s.start_ms_utc))

    @property
    def logs(self) -> tuple[LogRecord, ...]:
        return tuple(r for r in self.telemetry.logs if self.contains(r.timestamp_ms_utc))

    @property
    def ref(self) -> "TelemetryWindowRef":
        return TelemetryWindowRef(self.window_id, self.telemetry.telemetry_id, self.start_ms, self.end_ms)


@dataclass(frozen=True, slots=True, order=True)
class TelemetryWindowRef:
    """Names a TelemetryWindow without carrying its telemetry; it resolves through the Case."""
    window_id: str
    telemetry_id: str
    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if not self.window_id:
            raise ValueError("window reference needs a window id")
        if type(self.start_ms) is not int or type(self.end_ms) is not int:
            raise ValueError("window reference bounds must be integer milliseconds")
        if self.end_ms <= self.start_ms:
            raise ValueError("window reference must have positive half-open duration")


@dataclass(frozen=True, slots=True)
class Case:
    """What to diagnose: some telemetry windows, one of them anomalous, all others baselines,
    and the components a diagnosis may name."""
    case_id: str
    windows: tuple[TelemetryWindow, ...]
    anomaly: str                      # window_id of the anomalous window
    components: tuple[EntityRef, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "windows", tuple(self.windows))
        object.__setattr__(self, "components", tuple(self.components))
        ids = [window.window_id for window in self.windows]
        if len(ids) != len(set(ids)):
            raise ValueError("case window ids must be unique")
        if self.anomaly not in ids:
            raise ValueError("case anomaly must name one of its windows")
        if len(ids) < 2:
            raise ValueError("case needs an anomalous window and at least one baseline")

    def anomalous(self) -> TelemetryWindow:
        for window in self.windows:
            if window.window_id == self.anomaly:
                return window
        raise AssertionError("unreachable: anomaly is checked on construction")

    def baselines(self, ids: tuple[str, ...] | None = None) -> tuple[TelemetryWindow, ...]:
        """Every baseline window, or only those named in `ids`."""
        chosen = []
        for window in self.windows:
            if window.window_id == self.anomaly:
                continue
            if ids is None or window.window_id in ids:
                chosen.append(window)
        if ids is not None and len(chosen) != len(set(ids)):
            raise ValueError("ids must name baseline windows of this case")
        return tuple(chosen)


@dataclass(frozen=True, slots=True)
class TelemetryWindows:
    """Some of a Case's windows, as selected by a DAG node."""
    windows: tuple[TelemetryWindow, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "windows", tuple(self.windows))
        if not self.windows:
            raise ValueError("a window selection must hold at least one window")


@dataclass(frozen=True, slots=True)
class AggregateRow:
    entity: EntityRef
    metric_key: str
    window: Window | TelemetryWindowRef
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
    window: Window | TelemetryWindowRef
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
                       row.window, row.statistic)
                      for row in self.rows]
        if len(set(identities)) != len(identities):
            raise ValueError("aggregate envelope rows must have unique entity/metric/statistic keys")


@dataclass(frozen=True, slots=True)
class Evidence:
    source: str
    window: Window | TelemetryWindowRef
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
    # When to look: (start_ms, end_ms) spans in the anomalous run; None when time isn't answered.
    intervals: tuple[tuple[int, int], ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))
        if self.intervals is not None:
            object.__setattr__(self, "intervals", tuple(tuple(span) for span in self.intervals))
            for start, end in self.intervals:
                if type(start) is not int or type(end) is not int or end <= start:
                    raise ValueError("candidate intervals must be integer (start_ms, end_ms) with end > start")
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
        if self.acceptance_contract not in {"component_identity_top3", "component_time_top3"}:
            raise ValueError("acceptance contract must be component_identity_top3 or component_time_top3")
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
