"""Temporary, table/window-partitioned JSONL storage for normalized telemetry."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from ..types import EntityRef, LogRecord, MetricRecord, SpanRecord, TelemetryRecord


def _plain(value: Any) -> Any:
    """Convert frozen record details to JSON-compatible values."""
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _entity(value: Mapping[str, Any] | None) -> EntityRef | None:
    if value is None:
        return None
    return EntityRef(role=str(value["role"]), identifier=str(value["identifier"]))


def _decode_record(doc: Mapping[str, Any]) -> TelemetryRecord:
    kind = doc["$type"]
    fields = doc["$fields"]
    if kind == "MetricRecord":
        return MetricRecord(
            source_table=fields["source_table"],
            timestamp_ms_utc=fields["timestamp_ms_utc"],
            entity=_entity(fields["entity"]),  # type: ignore[arg-type]
            metric_name=fields["metric_name"],
            value=fields.get("value"),
            unit=fields.get("unit"),
            details=fields.get("details", {}),
        )
    if kind == "SpanRecord":
        return SpanRecord(
            source_table=fields["source_table"],
            timestamp_ms_utc=fields["timestamp_ms_utc"],
            trace_id=fields["trace_id"],
            span_id=fields["span_id"],
            parent_id=fields.get("parent_id"),
            duration_ms=fields.get("duration_ms"),
            success=fields.get("success"),
            call_type=fields.get("call_type"),
            executing_component=_entity(fields.get("executing_component")),
            logical_service=_entity(fields.get("logical_service")),
            database_target=_entity(fields.get("database_target")),
            details=fields.get("details", {}),
        )
    if kind == "LogRecord":
        return LogRecord(
            source_table=fields["source_table"],
            timestamp_ms_utc=fields["timestamp_ms_utc"],
            entity=_entity(fields.get("entity")),
            log_name=fields.get("log_name"),
            value=fields.get("value"),
            severity=fields.get("severity"),
            details=fields.get("details", {}),
        )
    raise ValueError(f"Unknown telemetry record type: {kind!r}")


class TelemetryRecordStore:
    """A trusted disk-backed snapshot partitioned by source table and window.

    It streams the adapter once and stores only normalized canonical rows. The
    JSON descriptor contains public data paths/counts and no incident labels.
    The caller owns the store for as long as subprocess workers need its files.
    """

    def __init__(self, root: Path, *, owner: tempfile.TemporaryDirectory[str] | None = None):
        self.root = Path(root)
        self._owner = owner
        self._files: dict[str, dict[str, Path]] = {}
        self._counts: dict[str, dict[str, int]] = {}

    @classmethod
    def from_descriptor(cls, descriptor: Mapping[str, Any]) -> "TelemetryRecordStore":
        """Open an existing descriptor without taking ownership of its files."""
        if descriptor.get("kind") != "jsonl":
            raise ValueError("Unsupported telemetry store descriptor kind")
        raw_files = descriptor.get("files")
        raw_counts = descriptor.get("counts_by_table_window")
        if not isinstance(raw_files, Mapping) or not isinstance(raw_counts, Mapping):
            raise ValueError("Malformed telemetry store descriptor")
        store = cls(Path("."))
        for table, windows in raw_files.items():
            if not isinstance(windows, Mapping):
                raise ValueError(f"Malformed files mapping for table {table!r}")
            counts = raw_counts.get(table)
            if not isinstance(counts, Mapping):
                raise ValueError(f"Missing record counts for table {table!r}")
            store._files[str(table)] = {str(window): Path(str(path)) for window, path in windows.items()}
            store._counts[str(table)] = {str(window): int(counts[window]) for window in windows}
            for window, path in store._files[str(table)].items():
                if window not in store._counts[str(table)] or not path.is_file():
                    raise ValueError(f"Missing telemetry partition for {table!r}/{window!r}")
        return store

    @classmethod
    def from_adapter(
        cls,
        adapter: Any,
        directory: str | Path | None = None,
    ) -> "TelemetryRecordStore":
        owner: tempfile.TemporaryDirectory[str] | None = None
        if directory is None:
            owner = tempfile.TemporaryDirectory(prefix="e4-telemetry-")
            root = Path(owner.name)
        else:
            root = Path(directory)
            root.mkdir(parents=True, exist_ok=True)

        store = cls(root, owner=owner)
        try:
            ref_window, alert_window = adapter.get_windows()
            for table_index, table in enumerate(adapter.get_schema().tables):
                store._files[table.name] = {}
                store._counts[table.name] = {}
                for window_index, window in enumerate((ref_window, alert_window)):
                    path = root / f"records-{table_index:04d}-{window_index:02d}.jsonl"
                    count = 0
                    with path.open("w", encoding="utf-8") as output:
                        for record in adapter.stream_records(table.name, window):
                            if record.source_table != table.name:
                                raise ValueError(
                                    f"Adapter yielded {record.source_table!r} while streaming {table.name!r}"
                                )
                            if not (window.start_ms_utc <= record.timestamp_ms_utc < window.end_ms_utc):
                                raise ValueError(
                                    f"Adapter yielded a record outside {window.name!r} window for {table.name!r}"
                                )
                            doc = {"$type": type(record).__name__, "$fields": _plain(record.as_dict())}
                            output.write(json.dumps(doc, allow_nan=True, separators=(",", ":")) + "\n")
                            count += 1
                    store._files[table.name][window.name] = path
                    store._counts[table.name][window.name] = count
            return store
        except Exception:
            store.close()
            raise

    @property
    def descriptor(self) -> dict[str, Any]:
        """Return the JSON-ready worker handoff descriptor."""
        return {
            "kind": "jsonl",
            "files": {
                table: {window: str(path) for window, path in windows.items()}
                for table, windows in self._files.items()
            },
            "counts_by_table_window": _plain(self._counts),
        }

    def iter_records(
        self,
        table_name: str | None = None,
        window_name: str | None = None,
    ) -> Iterator[TelemetryRecord]:
        """Read only matching table/window partitions from disk."""
        tables = [table_name] if table_name is not None else list(self._files)
        for table in tables:
            if table not in self._files:
                raise KeyError(f"Unknown telemetry table {table!r}")
            windows = [window_name] if window_name is not None else list(self._files[table])
            for window in windows:
                if window not in self._files[table]:
                    raise KeyError(f"Unknown window {window!r} for table {table!r}")
                with self._files[table][window].open("r", encoding="utf-8") as source:
                    for line in source:
                        if line.strip():
                            yield _decode_record(json.loads(line))

    def close(self) -> None:
        if self._owner is not None:
            self._owner.cleanup()
            self._owner = None

    def __enter__(self) -> "TelemetryRecordStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
