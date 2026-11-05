"""Dataset adaptation protocol, base adapter, and conformance test harness."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterator, Protocol, Sequence

from ..types import DatasetSchema, EntityRef, TelemetryRecord, Window


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

    def as_telemetry_view(self, max_records_per_table: int = 10000) -> Any:
        """Convenience method to construct an in-memory TelemetryView for this adapter."""
        from ..sandbox.views import TelemetryView
        ref_w, alt_w = self.get_windows()
        schema = self.get_schema()
        records: list[TelemetryRecord] = []
        truncated_tables: set[str] = set()
        is_truncated = False
        for table in schema.tables:
            for w in (ref_w, alt_w):
                count = 0
                for r in self.stream_records(table.name, w):
                    count += 1
                    if count <= max_records_per_table:
                        records.append(r)
                    else:
                        is_truncated = True
                        truncated_tables.add(table.name)
        return TelemetryView(
            case_id="adapter_case",
            reference_window=ref_w,
            alert_window=alt_w,
            records=records,
            source_tables=[t.name for t in schema.tables],
            metadata={
                "truncated": is_truncated,
                "truncated_tables": list(truncated_tables),
            },
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

    if not isinstance(windows, (tuple, list)) or len(windows) != 2:
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

    # 4. Check stream_records for each table
    total_records = 0
    table_record_counts: dict[str, int] = {}
    for table in schema.tables:
        count = 0
        try:
            it = adapter.stream_records(table.name, alt_w)
            for r in it:
                count += 1
                if not (alt_w.start_ms_utc <= r.timestamp_ms_utc < alt_w.end_ms_utc):
                    raise AdapterConformanceError(
                        f"Record timestamp {r.timestamp_ms_utc} in table '{table.name}' outside alert window [{alt_w.start_ms_utc}, {alt_w.end_ms_utc})"
                    )
                if r.source_table != table.name:
                    raise AdapterConformanceError(
                        f"Record source_table '{r.source_table}' does not match requested table '{table.name}'"
                    )
        except Exception as exc:
            raise AdapterConformanceError(f"stream_records('{table.name}', alert_window) failed: {exc}") from exc

        table_record_counts[table.name] = count
        total_records += count

    return {
        "status": "conforming",
        "table_count": len(schema.tables),
        "suspect_entities_count": len(entities),
        "total_records_sampled": total_records,
        "table_counts": table_record_counts,
    }
