"""Typed views, query results, and data helper functions for authored operators."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Generic, Iterator, Literal, TypeVar

from ..types import (
    Aggregate,
    AggregateRow,
    Candidate,
    CandidateSet,
    EntityRef,
    Evidence,
    LogRecord,
    MetricRecord,
    ScoredCandidate,
    ScoredCandidates,
    SpanRecord,
    TelemetryRecord,
    Window,
    canonical_component_id,
)

T = TypeVar("T", covariant=True)


@dataclass(frozen=True, slots=True)
class WindowView:
    name: Literal["reference", "alert"]
    start_ms_utc: int
    end_ms_utc: int

    @property
    def duration_ms(self) -> int:
        return self.end_ms_utc - self.start_ms_utc

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "start_ms_utc": self.start_ms_utc,
            "end_ms_utc": self.end_ms_utc,
        }


@dataclass(frozen=True, slots=True)
class EntityRefView:
    role: str
    identifier: str

    @property
    def canonical_id(self) -> str:
        return canonical_component_id(self.identifier)

    def as_dict(self) -> dict[str, Any]:
        return {"role": self.role, "identifier": self.identifier}


@dataclass(frozen=True, slots=True)
class CoverageView:
    complete: bool
    truncated: bool
    offset: int
    next_offset: int | None
    total_matches: int
    support: int


@dataclass(frozen=True, slots=True)
class MetricPointView:
    source_table: str
    timestamp_ms_utc: int
    entity_role: str
    entity_identifier: str
    origin_row_id: str
    metric_name: str
    metric_key: str
    value: float | None
    unit: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SpanView:
    source_table: str
    timestamp_ms_utc: int
    trace_id: str
    span_id: str
    parent_id: str | None
    duration_ms: float | None
    duration_value: float | None
    success: bool | None
    call_type: str | None = None
    executing_component_role: str | None = None
    executing_component_identifier: str | None = None
    logical_service_role: str | None = None
    logical_service_identifier: str | None = None
    database_target_role: str | None = None
    database_target_identifier: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LogRecordView:
    source_table: str
    timestamp_ms_utc: int
    log_name: str | None = None
    entity_role: str | None = None
    entity_identifier: str | None = None
    value: str | None = None
    severity: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueryResult(Generic[T]):
    rows: Sequence[T]
    window: WindowView
    source_tables: Sequence[str]
    coverage: CoverageView

    def __iter__(self) -> Iterator[T]:
        # Emits rows on direct iteration for user convenience
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)


@dataclass(frozen=True, slots=True)
class SummaryResult:
    value: float | int | None
    support: int
    missing_count: int
    nonfinite_count: int
    invalid_count: int


@dataclass(frozen=True, slots=True)
class AggregateGroupView:
    key: tuple[str | int | float | bool | None, ...]
    group_fields: Sequence[str]
    value: float | int | str | None
    support: int
    missing_count: int
    nonfinite_count: int
    invalid_count: int


@dataclass(frozen=True, slots=True)
class EvidenceView:
    source: str
    window: WindowView
    statistic: str
    value: float | str | bool | None
    contributing_entities: Sequence[EntityRefView]
    support: int
    ranking_eligible: bool
    details: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CandidateView:
    entity: EntityRefView
    rationale: str


@dataclass(frozen=True, slots=True)
class CandidateSetView:
    candidates: Sequence[CandidateView]
    source: str


@dataclass(frozen=True, slots=True)
class AggregateRowView:
    entity: EntityRefView
    metric_key: str
    window: WindowView
    statistic: str
    value: float | None
    support: int
    source_tables: Sequence[str]
    contributing_entities: Sequence[EntityRefView]
    details: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class AggregateView:
    window: WindowView
    rows: Sequence[AggregateRowView]
    source_tables: Sequence[str]
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ScoredCandidateView:
    entity: EntityRefView
    score: float
    category: str | None
    evidence: Sequence[EvidenceView]
    analysis_family: str | None


@dataclass(frozen=True, slots=True)
class ScoredCandidatesView:
    candidates: Sequence[ScoredCandidateView]
    scorer: str
    status: Literal["ready", "abstained"]
    status_details: Mapping[str, Any]


# -----------------------------------------------------------------------------
# Telemetry Container & In-Sandbox Helpers
# -----------------------------------------------------------------------------

class TelemetryView:
    """Read-only handle to incident telemetry available inside the sandbox."""

    def __init__(
        self,
        case_id: str,
        reference_window: Window | WindowView | Mapping[str, Any],
        alert_window: Window | WindowView | Mapping[str, Any],
        records: Sequence[TelemetryRecord],
        source_tables: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        self.case_id = case_id
        if hasattr(reference_window, "start_ms_utc"):
            ref_name = reference_window.name
            ref_start = reference_window.start_ms_utc
            ref_end = reference_window.end_ms_utc
        else:
            ref_name = reference_window["name"]
            ref_start = reference_window["start_ms_utc"]
            ref_end = reference_window["end_ms_utc"]
        self.reference_window = WindowView(ref_name, int(ref_start), int(ref_end))

        if hasattr(alert_window, "start_ms_utc"):
            alt_name = alert_window.name
            alt_start = alert_window.start_ms_utc
            alt_end = alert_window.end_ms_utc
        else:
            alt_name = alert_window["name"]
            alt_start = alert_window["start_ms_utc"]
            alt_end = alert_window["end_ms_utc"]
        self.alert_window = WindowView(alt_name, int(alt_start), int(alt_end))

        self._records = tuple(records)
        tables = set(source_tables or [])
        for r in self._records:
            tables.add(r.source_table)
        self.source_tables = tuple(sorted(tables))
        self.metadata = dict(metadata or {})
        self.truncated = bool(self.metadata.get("truncated", False))
        self.truncated_tables = set(self.metadata.get("truncated_tables", ()))

    @property
    def point_count(self) -> int:
        return len(self._records)

    def as_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "reference_window": {
                "name": self.reference_window.name,
                "start_ms_utc": self.reference_window.start_ms_utc,
                "end_ms_utc": self.reference_window.end_ms_utc,
            },
            "alert_window": {
                "name": self.alert_window.name,
                "start_ms_utc": self.alert_window.start_ms_utc,
                "end_ms_utc": self.alert_window.end_ms_utc,
            },
            "records": [
                {"$type": type(r).__name__, "$fields": r.as_dict()}
                for r in self._records
            ],
            "source_tables": list(self.source_tables),
            "metadata": dict(self.metadata),
        }


def telemetry_view(telemetry: Any) -> TelemetryView:
    if isinstance(telemetry, TelemetryView):
        return telemetry
    if hasattr(telemetry, "as_telemetry_view"):
        return telemetry.as_telemetry_view()
    if isinstance(telemetry, Mapping) and "reference_window" in telemetry and "alert_window" in telemetry:
        ref = telemetry["reference_window"]
        alt = telemetry["alert_window"]
        ref_w = ref if isinstance(ref, Window) else Window(ref["name"], ref["start_ms_utc"], ref["end_ms_utc"])
        alt_w = alt if isinstance(alt, Window) else Window(alt["name"], alt["start_ms_utc"], alt["end_ms_utc"])
        return TelemetryView(
            case_id=telemetry.get("case_id", "case"),
            reference_window=ref_w,
            alert_window=alt_w,
            records=telemetry.get("records", ()),
            source_tables=telemetry.get("source_tables"),
            metadata=telemetry.get("metadata", {}),
        )
    raise TypeError(f"Cannot convert {type(telemetry).__name__} to TelemetryView")


def metric_points(
    telemetry: TelemetryView,
    source_table: str,
    window: WindowView,
    entity_role: str | None = None,
    entity_identifier: str | None = None,
    metric_name: str | None = None,
    metric_key: str | None = None,
    limit: int = 10000,
    offset: int = 0,
) -> QueryResult[MetricPointView]:
    matches: list[MetricPointView] = []
    total = 0
    row_idx = 0

    target_name = metric_name or metric_key

    for r in telemetry._records:
        if not isinstance(r, MetricRecord):
            continue
        if r.source_table != source_table:
            continue
        if not (window.start_ms_utc <= r.timestamp_ms_utc < window.end_ms_utc):
            continue
        if entity_role is not None and r.entity.role != entity_role:
            continue
        if entity_identifier is not None and r.entity.identifier != entity_identifier:
            continue
        if target_name is not None and r.metric_name != target_name:
            continue

        total += 1
        if total <= offset:
            continue
        if len(matches) < limit:
            matches.append(
                MetricPointView(
                    source_table=r.source_table,
                    timestamp_ms_utc=r.timestamp_ms_utc,
                    entity_role=r.entity.role,
                    entity_identifier=r.entity.identifier,
                    origin_row_id=f"{r.source_table}:{row_idx}",
                    metric_name=r.metric_name,
                    metric_key=r.metric_name,
                    value=r.value,
                    unit=r.unit,
                    details=r.details,
                )
            )
        row_idx += 1

    trunc_tables = getattr(telemetry, "truncated_tables", None)
    if trunc_tables is not None and len(trunc_tables) > 0:
        table_truncated = source_table in trunc_tables
    else:
        table_truncated = bool(getattr(telemetry, "truncated", False))
    truncated = (total > (offset + limit)) or table_truncated
    next_offset = (offset + limit) if (total > (offset + limit)) else None
    coverage = CoverageView(
        complete=(not truncated) and (not table_truncated),
        truncated=truncated,
        offset=offset,
        next_offset=next_offset,
        total_matches=total,
        support=len(matches),
    )
    return QueryResult(
        rows=tuple(matches),
        window=window,
        source_tables=(source_table,),
        coverage=coverage,
    )


def span_records(
    telemetry: TelemetryView,
    source_table: str,
    window: WindowView,
    component_role: str | None = None,
    component_identifier: str | None = None,
    limit: int = 10000,
    offset: int = 0,
) -> QueryResult[SpanView]:
    matches: list[SpanView] = []
    total = 0

    for r in telemetry._records:
        if not isinstance(r, SpanRecord):
            continue
        if r.source_table != source_table:
            continue
        if not (window.start_ms_utc <= r.timestamp_ms_utc < window.end_ms_utc):
            continue
        if component_role is not None:
            if not r.executing_component or r.executing_component.role != component_role:
                continue
        if component_identifier is not None:
            if not r.executing_component or r.executing_component.identifier != component_identifier:
                continue

        total += 1
        if total <= offset:
            continue
        if len(matches) < limit:
            matches.append(
                SpanView(
                    source_table=r.source_table,
                    timestamp_ms_utc=r.timestamp_ms_utc,
                    trace_id=r.trace_id,
                    span_id=r.span_id,
                    parent_id=r.parent_id,
                    duration_ms=r.duration_ms,
                    duration_value=r.duration_ms,
                    success=r.success,
                    call_type=r.call_type,
                    executing_component_role=r.executing_component.role if r.executing_component else None,
                    executing_component_identifier=r.executing_component.identifier if r.executing_component else None,
                    logical_service_role=r.logical_service.role if r.logical_service else None,
                    logical_service_identifier=r.logical_service.identifier if r.logical_service else None,
                    database_target_role=r.database_target.role if r.database_target else None,
                    database_target_identifier=r.database_target.identifier if r.database_target else None,
                    details=r.details,
                )
            )

    trunc_tables = getattr(telemetry, "truncated_tables", None)
    if trunc_tables is not None and len(trunc_tables) > 0:
        table_truncated = source_table in trunc_tables
    else:
        table_truncated = bool(getattr(telemetry, "truncated", False))
    truncated = (total > (offset + limit)) or table_truncated
    next_offset = (offset + limit) if (total > (offset + limit)) else None
    coverage = CoverageView(
        complete=(not truncated) and (not table_truncated),
        truncated=truncated,
        offset=offset,
        next_offset=next_offset,
        total_matches=total,
        support=len(matches),
    )
    return QueryResult(
        rows=tuple(matches),
        window=window,
        source_tables=(source_table,),
        coverage=coverage,
    )


def log_records(
    telemetry: TelemetryView,
    source_table: str,
    window: WindowView,
    entity_identifier: str | None = None,
    log_name: str | None = None,
    limit: int = 10000,
    offset: int = 0,
) -> QueryResult[LogRecordView]:
    matches: list[LogRecordView] = []
    total = 0

    for r in telemetry._records:
        if not isinstance(r, LogRecord):
            continue
        if r.source_table != source_table:
            continue
        if not (window.start_ms_utc <= r.timestamp_ms_utc < window.end_ms_utc):
            continue
        if entity_identifier is not None:
            if not r.entity or r.entity.identifier != entity_identifier:
                continue
        if log_name is not None and r.log_name != log_name:
            continue

        total += 1
        if total <= offset:
            continue
        if len(matches) < limit:
            matches.append(
                LogRecordView(
                    source_table=r.source_table,
                    timestamp_ms_utc=r.timestamp_ms_utc,
                    log_name=r.log_name,
                    entity_role=r.entity.role if r.entity else None,
                    entity_identifier=r.entity.identifier if r.entity else None,
                    value=r.value,
                    severity=r.severity,
                    details=r.details,
                )
            )

    trunc_tables = getattr(telemetry, "truncated_tables", None)
    if trunc_tables is not None and len(trunc_tables) > 0:
        table_truncated = source_table in trunc_tables
    else:
        table_truncated = bool(getattr(telemetry, "truncated", False))
    truncated = (total > (offset + limit)) or table_truncated
    next_offset = (offset + limit) if (total > (offset + limit)) else None
    coverage = CoverageView(
        complete=(not truncated) and (not table_truncated),
        truncated=truncated,
        offset=offset,
        next_offset=next_offset,
        total_matches=total,
        support=len(matches),
    )
    return QueryResult(
        rows=tuple(matches),
        window=window,
        source_tables=(source_table,),
        coverage=coverage,
    )


def aggregate_records(
    telemetry: TelemetryView,
    source_table: str,
    window: WindowView,
    record_type: Literal["metric", "span", "log"],
    field: str,
    statistic: Literal["count", "sum", "mean", "min", "max", "quantile"],
    group_by: Sequence[str] = (),
    time_bucket_ms: int | None = None,
    quantile: float | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> QueryResult[AggregateGroupView]:
    groups: dict[tuple[Any, ...], list[float | int | None]] = {}

    for r in telemetry._records:
        if r.source_table != source_table:
            continue
        if not (window.start_ms_utc <= r.timestamp_ms_utc < window.end_ms_utc):
            continue

        val: float | int | None = None
        key_parts: list[Any] = []

        if record_type == "metric" and isinstance(r, MetricRecord):
            if field == "value":
                val = r.value
            elif field in ("timestamp", "timestamp_ms_utc"):
                val = float(r.timestamp_ms_utc)
            elif field in r.details:
                try:
                    val = float(r.details[field])
                except (ValueError, TypeError):
                    val = None
            else:
                raise ValueError(f"Field '{field}' does not exist on MetricRecord")

            for gfield in group_by:
                if gfield in ("entity", "entity_identifier"):
                    key_parts.append(r.entity.identifier)
                elif gfield == "entity_role":
                    key_parts.append(r.entity.role)
                elif gfield in ("metric_name", "metric_key"):
                    key_parts.append(r.metric_name)
                elif gfield in ("timestamp", "timestamp_ms_utc"):
                    key_parts.append(r.timestamp_ms_utc)
                elif gfield in r.details:
                    key_parts.append(r.details[gfield])
                else:
                    raise ValueError(f"Grouping field '{gfield}' does not exist on MetricRecord")
        elif record_type == "span" and isinstance(r, SpanRecord):
            if field in ("duration", "duration_ms"):
                val = r.duration_ms
            elif field in ("timestamp", "timestamp_ms_utc"):
                val = float(r.timestamp_ms_utc)
            elif field == "success":
                val = 1.0 if r.success is True else (0.0 if r.success is False else None)
            elif field in r.details:
                try:
                    val = float(r.details[field])
                except (ValueError, TypeError):
                    val = None
            else:
                raise ValueError(f"Field '{field}' does not exist on SpanRecord")

            for gfield in group_by:
                if gfield in ("component", "executing_component", "entity", "entity_identifier"):
                    key_parts.append(r.executing_component.identifier if r.executing_component else None)
                elif gfield == "entity_role":
                    key_parts.append(r.executing_component.role if r.executing_component else None)
                elif gfield == "logical_service":
                    key_parts.append(r.logical_service.identifier if r.logical_service else None)
                elif gfield == "database_target":
                    key_parts.append(r.database_target.identifier if r.database_target else None)
                elif gfield in ("trace_id", "span_id", "parent_id", "call_type"):
                    key_parts.append(getattr(r, gfield, None))
                elif gfield in ("timestamp", "timestamp_ms_utc"):
                    key_parts.append(r.timestamp_ms_utc)
                elif gfield == "success":
                    key_parts.append(r.success)
                elif gfield in r.details:
                    key_parts.append(r.details[gfield])
                else:
                    raise ValueError(f"Grouping field '{gfield}' does not exist on SpanRecord")
        elif record_type == "log" and isinstance(r, LogRecord):
            if field == "count":
                val = 1.0
            elif field in ("timestamp", "timestamp_ms_utc"):
                val = float(r.timestamp_ms_utc)
            elif field == "value":
                try:
                    val = float(r.value) if r.value is not None else None
                except (ValueError, TypeError):
                    val = None
            elif field in r.details:
                try:
                    val = float(r.details[field])
                except (ValueError, TypeError):
                    val = None
            else:
                raise ValueError(f"Field '{field}' does not exist on LogRecord")

            for gfield in group_by:
                if gfield in ("entity", "entity_identifier"):
                    key_parts.append(r.entity.identifier if r.entity else None)
                elif gfield == "entity_role":
                    key_parts.append(r.entity.role if r.entity else None)
                elif gfield == "log_name":
                    key_parts.append(r.log_name)
                elif gfield == "severity":
                    key_parts.append(r.severity)
                elif gfield in ("timestamp", "timestamp_ms_utc"):
                    key_parts.append(r.timestamp_ms_utc)
                elif gfield in r.details:
                    key_parts.append(r.details[gfield])
                else:
                    raise ValueError(f"Grouping field '{gfield}' does not exist on LogRecord")

        if time_bucket_ms and time_bucket_ms > 0:
            bucket = (r.timestamp_ms_utc // time_bucket_ms) * time_bucket_ms
            key_parts.append(bucket)

        gkey = tuple(key_parts)
        if gkey not in groups:
            groups[gkey] = []
        groups[gkey].append(val)

    result_rows: list[AggregateGroupView] = []
    total = len(groups)
    sorted_keys = sorted(groups.keys(), key=lambda k: str(k))

    for k in sorted_keys[offset : offset + limit]:
        vals = groups[k]
        summary = summarize(vals, statistic, quantile=quantile)
        result_rows.append(
            AggregateGroupView(
                key=k,
                group_fields=tuple(group_by),
                value=summary.value,
                support=summary.support,
                missing_count=summary.missing_count,
                nonfinite_count=summary.nonfinite_count,
                invalid_count=summary.invalid_count,
            )
        )

    trunc_tables = getattr(telemetry, "truncated_tables", None)
    if trunc_tables is not None and len(trunc_tables) > 0:
        table_truncated = source_table in trunc_tables
    else:
        table_truncated = bool(getattr(telemetry, "truncated", False))
    truncated = (total > (offset + limit)) or table_truncated
    next_offset = (offset + limit) if (total > (offset + limit)) else None
    coverage = CoverageView(
        complete=(not truncated) and (not table_truncated),
        truncated=truncated,
        offset=offset,
        next_offset=next_offset,
        total_matches=total,
        support=len(result_rows),
    )
    return QueryResult(
        rows=tuple(result_rows),
        window=window,
        source_tables=(source_table,),
        coverage=coverage,
    )


def group_by(rows: Sequence[Any], fields: Sequence[str]) -> Mapping[tuple[Any, ...], Sequence[Any]]:
    grouped: dict[tuple[Any, ...], list[Any]] = {}
    for r in rows:
        key_parts = []
        for f in fields:
            if hasattr(r, f):
                key_parts.append(getattr(r, f))
            elif isinstance(r, Mapping) and f in r:
                key_parts.append(r[f])
            else:
                raise ValueError(f"Grouping field '{f}' not found on record {type(r).__name__}")
        k = tuple(key_parts)
        if k not in grouped:
            grouped[k] = []
        grouped[k].append(r)
    return {k: tuple(v) for k, v in grouped.items()}


def summarize(
    values: Sequence[float | int | None],
    statistic: Literal["count", "distinct", "sum", "mean", "min", "max", "quantile"],
    quantile: float | None = None,
) -> SummaryResult:
    valid_nums: list[float] = []
    missing_count = 0
    nonfinite_count = 0
    invalid_count = 0

    for v in values:
        if v is None:
            missing_count += 1
        elif isinstance(v, (int, float)):
            fv = float(v)
            if math.isfinite(fv):
                valid_nums.append(fv)
            else:
                nonfinite_count += 1
        else:
            invalid_count += 1

    support = len(valid_nums)
    if statistic == "count":
        return SummaryResult(
            value=len(values),
            support=len(values),
            missing_count=missing_count,
            nonfinite_count=nonfinite_count,
            invalid_count=invalid_count,
        )

    if not valid_nums:
        return SummaryResult(
            value=None,
            support=0,
            missing_count=missing_count,
            nonfinite_count=nonfinite_count,
            invalid_count=invalid_count,
        )

    val: float | None = None
    if statistic == "sum":
        val = sum(valid_nums)
    elif statistic == "mean":
        val = sum(valid_nums) / len(valid_nums)
    elif statistic == "min":
        val = min(valid_nums)
    elif statistic == "max":
        val = max(valid_nums)
    elif statistic == "distinct":
        val = float(len(set(valid_nums)))
    elif statistic == "quantile":
        q = 0.5 if quantile is None else max(0.0, min(1.0, float(quantile)))
        s = sorted(valid_nums)
        idx = int(round(q * (len(s) - 1)))
        val = s[idx]

    return SummaryResult(
        value=val,
        support=support,
        missing_count=missing_count,
        nonfinite_count=nonfinite_count,
        invalid_count=invalid_count,
    )


def top_k(rows: Sequence[Any], field: str, k: int, descending: bool = True) -> Sequence[Any]:
    def get_sort_key(item: Any) -> float:
        val = getattr(item, field, None) if hasattr(item, field) else item.get(field)
        if val is None or not isinstance(val, (int, float)) or not math.isfinite(float(val)):
            return -math.inf if descending else math.inf
        return float(val)

    sorted_rows = sorted(rows, key=get_sort_key, reverse=descending)
    return tuple(sorted_rows[:k])


# -----------------------------------------------------------------------------
# Record Constructors & View Wrappers
# -----------------------------------------------------------------------------

def make_window(name: Literal["reference", "alert"], start_ms_utc: int, end_ms_utc: int) -> Window:
    return Window(name=name, start_ms_utc=start_ms_utc, end_ms_utc=end_ms_utc)


def make_entity_ref(role: str, identifier: str) -> EntityRef:
    return EntityRef(role=role, identifier=identifier)


def make_evidence(
    source: str,
    window: Window | WindowView,
    statistic: str,
    value: float | str | bool | None,
    contributing_entities: Sequence[EntityRef | EntityRefView],
    support: int,
    ranking_eligible: bool = True,
    details: Mapping[str, Any] | None = None,
    coverage: CoverageView | None = None,
) -> Evidence:
    if coverage is not None and (coverage.truncated or not coverage.complete):
        ranking_eligible = False
    w = (
        window
        if isinstance(window, Window)
        else Window(window.name, window.start_ms_utc, window.end_ms_utc)
    )
    entities = tuple(
        e if isinstance(e, EntityRef) else EntityRef(e.role, e.identifier)
        for e in contributing_entities
    )
    return Evidence(
        source=source,
        window=w,
        statistic=statistic,
        value=value,
        contributing_entities=entities,
        support=support,
        ranking_eligible=ranking_eligible,
        details=dict(details or {}),
    )


def make_candidate(entity: EntityRef | EntityRefView, rationale: str = "") -> Candidate:
    e = entity if isinstance(entity, EntityRef) else EntityRef(entity.role, entity.identifier)
    return Candidate(entity=e, rationale=rationale)


def make_candidate_set(candidates: Sequence[Candidate], source: str) -> CandidateSet:
    return CandidateSet(candidates=tuple(candidates), source=source)


def make_aggregate_row(
    entity: EntityRef | EntityRefView,
    metric_key: str,
    window: Window | WindowView,
    statistic: str,
    value: float | None,
    support: int,
    source_tables: Sequence[str] = (),
    contributing_entities: Sequence[EntityRef | EntityRefView] = (),
    details: Mapping[str, Any] | None = None,
) -> AggregateRow:
    e = entity if isinstance(entity, EntityRef) else EntityRef(entity.role, entity.identifier)
    w = (
        window
        if isinstance(window, Window)
        else Window(window.name, window.start_ms_utc, window.end_ms_utc)
    )
    entities = tuple(
        item if isinstance(item, EntityRef) else EntityRef(item.role, item.identifier)
        for item in contributing_entities
    )
    return AggregateRow(
        entity=e,
        metric_key=metric_key,
        window=w,
        statistic=statistic,
        value=value,
        support=support,
        source_tables=tuple(source_tables),
        contributing_entities=entities,
        details=dict(details or {}),
    )


def make_aggregate(
    window: Window | WindowView,
    rows: Sequence[AggregateRow],
    source_tables: Sequence[str] = (),
    metadata: Mapping[str, Any] | None = None,
) -> Aggregate:
    w = (
        window
        if isinstance(window, Window)
        else Window(window.name, window.start_ms_utc, window.end_ms_utc)
    )
    return Aggregate(
        window=w,
        rows=tuple(rows),
        source_tables=tuple(source_tables),
        metadata=dict(metadata or {}),
    )


def make_scored_candidate(
    entity: EntityRef | EntityRefView,
    score: float,
    category: str | None = None,
    evidence: Sequence[Evidence] = (),
    analysis_family: str | None = None,
) -> ScoredCandidate:
    e = entity if isinstance(entity, EntityRef) else EntityRef(entity.role, entity.identifier)
    return ScoredCandidate(
        entity=e,
        score=score,
        category=category,
        evidence=tuple(evidence),
        analysis_family=analysis_family,
    )


def make_scored_candidates(
    candidates: Sequence[ScoredCandidate],
    scorer: str,
    status: Literal["ready", "abstained"] = "ready",
    status_details: Mapping[str, Any] | None = None,
) -> ScoredCandidates:
    return ScoredCandidates(
        candidates=tuple(candidates),
        scorer=scorer,
        status=status,
        status_details=dict(status_details or {}),
    )


def entity_identifier(entity_ref: Any) -> str:
    if hasattr(entity_ref, "identifier"):
        return str(entity_ref.identifier)
    if isinstance(entity_ref, Mapping) and "identifier" in entity_ref:
        return str(entity_ref["identifier"])
    raise TypeError(f"Cannot extract identifier from {type(entity_ref).__name__}")


entity_id = entity_identifier


def aggregate_view(agg: Aggregate) -> AggregateView:
    return AggregateView(
        window=WindowView(agg.window.name, agg.window.start_ms_utc, agg.window.end_ms_utc),
        rows=tuple(
            AggregateRowView(
                entity=EntityRefView(r.entity.role, r.entity.identifier),
                metric_key=r.metric_key,
                window=WindowView(r.window.name, r.window.start_ms_utc, r.window.end_ms_utc),
                statistic=r.statistic,
                value=r.value,
                support=r.support,
                source_tables=r.source_tables,
                contributing_entities=tuple(
                    EntityRefView(e.role, e.identifier) for e in r.contributing_entities
                ),
                details=r.details,
            )
            for r in agg.rows
        ),
        source_tables=agg.source_tables,
        metadata=agg.metadata,
    )


def candidate_views(cset: CandidateSet) -> tuple[CandidateView, ...]:
    return tuple(
        CandidateView(
            entity=EntityRefView(c.entity.role, c.entity.identifier),
            rationale=c.rationale,
        )
        for c in cset.candidates
    )


def scored_views(sc: ScoredCandidates) -> tuple[ScoredCandidateView, ...]:
    return tuple(
        ScoredCandidateView(
            entity=EntityRefView(c.entity.role, c.entity.identifier),
            score=c.score,
            category=c.category,
            evidence=tuple(
                EvidenceView(
                    source=e.source,
                    window=WindowView(e.window.name, e.window.start_ms_utc, e.window.end_ms_utc),
                    statistic=e.statistic,
                    value=e.value,
                    contributing_entities=tuple(
                        EntityRefView(ent.role, ent.identifier) for ent in e.contributing_entities
                    ),
                    support=e.support,
                    ranking_eligible=e.ranking_eligible,
                    details=e.details,
                )
                for e in c.evidence
            ),
            analysis_family=c.analysis_family,
        )
        for c in sc.candidates
    )
