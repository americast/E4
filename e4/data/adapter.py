"""Dataset adaptation protocol, base adapter, and conformance test harness."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterator, Protocol, Sequence

from ..types import (
    DatasetSchema,
    EntityRef,
    LogRecord,
    MetricRecord,
    SpanRecord,
    TelemetryRecord,
    Window,
)


class TelemetrySource(Protocol):
    """Abstract interface for querying incident observability data."""

    def get_schema(self) -> DatasetSchema:
        """Declare tables, typed fields, units, nullability, and entity roles."""
        ...

    def get_windows(self) -> tuple[Window, Window]:
        """Return (reference_window, alert_window) as UTC epoch millisecond intervals."""
        ...

    def extract_suspect_entities(self) -> list[EntityRef]:
        """Return distinct typed entity candidates to diagnose and rank."""
        ...

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        """Stream canonical MetricRecord, SpanRecord, or LogRecord values."""
        ...


class BaseDatasetAdapter(ABC):
    """Base class for dataset adapters adapting incident data to E4."""

    @abstractmethod
    def get_schema(self) -> DatasetSchema:
        """Declare tables, typed fields, units, nullability, and entity roles."""
        raise NotImplementedError

    @abstractmethod
    def get_windows(self) -> tuple[Window, Window]:
        """Return (reference_window, alert_window) as UTC epoch millisecond intervals."""
        raise NotImplementedError

    @abstractmethod
    def extract_suspect_entities(self) -> list[EntityRef]:
        """Return distinct typed entity candidates to diagnose and rank."""
        raise NotImplementedError

    @abstractmethod
    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        """Stream canonical MetricRecord, SpanRecord, or LogRecord values."""
        raise NotImplementedError

    def as_telemetry_view(self, max_records_per_table: int | None = 10000) -> Any:
        """Construct an in-memory view, optionally limiting records per table/window.

        The default is a safety bound. Pass ``None`` only when the caller has a
        full-window transport path and enough memory for every matching record;
        a capped view reports its true counts and must not be treated as complete.
        """
        from ..sandbox.views import TelemetryView

        if max_records_per_table is not None and max_records_per_table < 0:
            raise ValueError("max_records_per_table must be non-negative or None")
        ref_w, alt_w = self.get_windows()
        schema = self.get_schema()
        records: list[TelemetryRecord] = []
        truncated_tables: set[str] = set()
        record_counts: dict[str, dict[str, int]] = {}
        is_truncated = False
        for table in schema.tables:
            record_counts[table.name] = {}
            for w in (ref_w, alt_w):
                count = 0
                for r in self.stream_records(table.name, w):
                    count += 1
                    if max_records_per_table is None or count <= max_records_per_table:
                        records.append(r)
                    else:
                        is_truncated = True
                        truncated_tables.add(table.name)
                record_counts[table.name][w.name] = count
        return TelemetryView(
            case_id="adapter_case",
            reference_window=ref_w,
            alert_window=alt_w,
            records=records,
            source_tables=[t.name for t in schema.tables],
            metadata={
                "truncated": is_truncated,
                "truncated_tables": list(truncated_tables),
                "record_counts": record_counts,
                "record_limit_per_table_window": max_records_per_table,
            },
        )

    def as_full_telemetry_view(self, case_id: str = "adapter_case") -> Any:
        """Build a disk-backed, uncapped view for trusted full-window analyses.

        The view retains the temporary store, so callers must keep the view alive
        for the full worker lifetime. Records remain on disk and are read by
        table/window instead of being copied into a large in-memory payload.
        """
        from .record_store import TelemetryRecordStore
        from ..sandbox.views import TelemetryView

        ref_w, alt_w = self.get_windows()
        schema = self.get_schema()
        store = TelemetryRecordStore.from_adapter(self)
        counts = store.descriptor["counts_by_table_window"]
        return TelemetryView(
            case_id=case_id,
            reference_window=ref_w,
            alert_window=alt_w,
            records=(),
            source_tables=[table.name for table in schema.tables],
            metadata={
                "truncated": False,
                "truncated_tables": [],
                "record_counts": counts,
                "record_limit_per_table_window": None,
            },
            record_store=store,
        )


class AdapterConformanceError(ValueError):
    """Raised when an adapter fails specification conformance checks."""


def check_adapter_conformance(adapter: BaseDatasetAdapter) -> dict[str, Any]:
    """Test and verify that an adapter strictly satisfies the 4-method contract."""
    # 1. Check get_schema
    try:
        schema = adapter.get_schema()
    except Exception as exc:
        raise AdapterConformanceError(f"get_schema() failed: {exc}") from exc

    if not isinstance(schema, DatasetSchema) or not schema.tables:
        raise AdapterConformanceError("get_schema() must return a DatasetSchema with at least one table")

    # 2. Check get_windows
    try:
        windows = adapter.get_windows()
    except Exception as exc:
        raise AdapterConformanceError(f"get_windows() failed: {exc}") from exc

    if not isinstance(windows, tuple) or len(windows) != 2:
        raise AdapterConformanceError("get_windows() must return a 2-tuple (reference_window, alert_window)")
    ref_w, alt_w = windows[0], windows[1]
    if not isinstance(ref_w, Window) or not isinstance(alt_w, Window):
        raise AdapterConformanceError("Both windows returned by get_windows() must be Window instances")
    if ref_w.name != "reference" or alt_w.name != "alert":
        raise AdapterConformanceError("Window roles must be 'reference' and 'alert'")
    if ref_w.end_ms_utc <= ref_w.start_ms_utc or alt_w.end_ms_utc <= alt_w.start_ms_utc:
        raise AdapterConformanceError("Windows must have positive durations")

    # 3. Check extract_suspect_entities
    try:
        entities = adapter.extract_suspect_entities()
    except Exception as exc:
        raise AdapterConformanceError(f"extract_suspect_entities() failed: {exc}") from exc

    if not isinstance(entities, list) or not entities:
        raise AdapterConformanceError("extract_suspect_entities() must return a non-empty list of EntityRef")
    for ent in entities:
        if not isinstance(ent, EntityRef) or not ent.role or not ent.identifier:
            raise AdapterConformanceError(f"Invalid EntityRef in suspect entities: {ent!r}")

    # 4. Check stream_records against the declared canonical table type and
    # both windows. Empty iterables are valid for an available empty table.
    record_types = {
        "metric": MetricRecord,
        "span": SpanRecord,
        "log": LogRecord,
    }
    total_records = 0
    total_records_checked = 0
    table_record_counts: dict[str, int] = {}
    table_window_counts: dict[str, dict[str, int]] = {}
    for table in schema.tables:
        expected_type = record_types.get(table.record_type)
        if expected_type is None:
            raise AdapterConformanceError(
                f"Table '{table.name}' has unsupported canonical record_type {table.record_type!r}"
            )
        table_window_counts[table.name] = {}
        for window in (ref_w, alt_w):
            count = 0
            try:
                records = adapter.stream_records(table.name, window)
                iterator = iter(records)
                if iterator is not records:
                    raise AdapterConformanceError(
                        f"stream_records('{table.name}', {window.name}_window) must return an iterator or generator"
                    )
                for record in iterator:
                    count += 1
                    if not isinstance(record, expected_type):
                        raise AdapterConformanceError(
                            f"Table '{table.name}' declares record_type '{table.record_type}' "
                            f"but stream_records returned {type(record).__name__}"
                        )
                    timestamp = record.timestamp_ms_utc
                    if type(timestamp) is not int:
                        raise AdapterConformanceError(
                            f"Record timestamp in table '{table.name}' must be an integer UTC millisecond value"
                        )
                    if not (window.start_ms_utc <= timestamp < window.end_ms_utc):
                        raise AdapterConformanceError(
                            f"Record timestamp {timestamp} in table '{table.name}' outside "
                            f"{window.name} window [{window.start_ms_utc}, {window.end_ms_utc})"
                        )
                    if record.source_table != table.name:
                        raise AdapterConformanceError(
                            f"Record source_table '{record.source_table}' does not match "
                            f"requested table '{table.name}'"
                        )
            except Exception as exc:
                raise AdapterConformanceError(
                    f"stream_records('{table.name}', {window.name}_window) failed: {exc}"
                ) from exc

            table_window_counts[table.name][window.name] = count
            total_records_checked += count
            if window.name == "alert":
                table_record_counts[table.name] = count
                total_records += count

    return {
        "status": "conforming",
        "table_count": len(schema.tables),
        "suspect_entities_count": len(entities),
        "total_records_sampled": total_records,
        "table_counts": table_record_counts,
        "total_records_checked": total_records_checked,
        "table_window_counts": table_window_counts,
    }
