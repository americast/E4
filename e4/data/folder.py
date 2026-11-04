"""Generic folder and CSV/TSV telemetry source loader with declarative mappings."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from ..types import (
    ColumnSchema,
    DatasetSchema,
    EntityRef,
    LogRecord,
    MetricRecord,
    SpanRecord,
    TableSchema,
    TelemetryRecord,
    Window,
)
from .adapter import BaseDatasetAdapter


class FolderTelemetrySource(BaseDatasetAdapter):
    """Loads incident telemetry from loose CSV/TSV files in a folder using a declarative mapping."""

    def __init__(
        self,
        incident_dir: Path | str,
        mapping: Mapping[str, Any] | Path | str | None = None,
    ):
        self.incident_dir = Path(incident_dir)
        if mapping is None:
            mapping_file = self.incident_dir / "mapping.json"
            if mapping_file.is_file():
                self.mapping = json.loads(mapping_file.read_text(encoding="utf-8"))
            else:
                raise ValueError(
                    f"No mapping provided and no mapping.json found in {self.incident_dir}"
                )
        elif isinstance(mapping, (Path, str)):
            self.mapping = json.loads(Path(mapping).read_text(encoding="utf-8"))
        elif isinstance(mapping, Mapping):
            self.mapping = dict(mapping)
        else:
            raise TypeError("mapping must be a Mapping, Path, or None")

        self._init_windows()
        self._init_schema()

    def _init_windows(self) -> None:
        windows_config = self.mapping.get("windows", {})
        ref = windows_config.get("reference", [0, 1])
        alt = windows_config.get("alert", [1, 2])
        self._reference_window = Window("reference", int(ref[0]), int(ref[1]))
        self._alert_window = Window("alert", int(alt[0]), int(alt[1]))

    def _init_schema(self) -> None:
        table_configs = self.mapping.get("tables", [])
        tables: list[TableSchema] = []
        for tc in table_configs:
            cols = [
                ColumnSchema(name=tc.get("timestamp_col", "timestamp"), data_type="timestamp"),
                ColumnSchema(name=tc.get("entity_col", "entity"), data_type="string"),
            ]
            rtype = tc.get("record_type", "metric")
            if rtype == "metric":
                cols.append(ColumnSchema(name=tc.get("metric_name_col", "metric_name"), data_type="string"))
                cols.append(ColumnSchema(name=tc.get("value_col", "value"), data_type="number"))
            elif rtype == "span":
                cols.append(ColumnSchema(name=tc.get("trace_id_col", "trace_id"), data_type="string"))
                cols.append(ColumnSchema(name=tc.get("duration_col", "duration_ms"), data_type="number"))
            elif rtype == "log":
                cols.append(ColumnSchema(name=tc.get("message_col", "message"), data_type="string"))

            tables.append(
                TableSchema(
                    name=tc.get("name", tc.get("file", "table")),
                    record_type=rtype,
                    columns=tuple(cols),
                    entity_roles=(tc.get("entity_role", "service"),),
                )
            )
        self._schema = DatasetSchema(tables=tuple(tables))

    def get_schema(self) -> DatasetSchema:
        return self._schema

    def get_windows(self) -> tuple[Window, Window]:
        return (self._reference_window, self._alert_window)

    def extract_suspect_entities(self) -> list[EntityRef]:
        declared = self.mapping.get("suspect_entities")
        if declared:
            return [
                EntityRef(role=item.get("role", "service"), identifier=item["identifier"])
                for item in declared
            ]

        # Dynamically discover entities from the first table
        discovered: set[tuple[str, str]] = set()
        table_configs = self.mapping.get("tables", [])
        if not table_configs:
            return [EntityRef("service", "unknown_service")]

        tc = table_configs[0]
        file_path = self.incident_dir / tc.get("file", tc.get("name", ""))
        entity_col = tc.get("entity_col", "entity")
        entity_role = tc.get("entity_role", "service")

        if file_path.is_file():
            delim = "\t" if file_path.suffix == ".tsv" else ","
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=delim)
                for row in reader:
                    val = row.get(entity_col)
                    if val and val.strip():
                        discovered.add((entity_role, val.strip()))

        if not discovered:
            discovered.add((entity_role, "default_service"))

        return [EntityRef(role=r, identifier=i) for r, i in sorted(discovered)]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        table_config = None
        for tc in self.mapping.get("tables", []):
            if tc.get("name") == table_name or tc.get("file") == table_name:
                table_config = tc
                break

        if not table_config:
            return

        file_path = self.incident_dir / table_config.get("file", table_name)
        if not file_path.is_file():
            return

        delim = "\t" if file_path.suffix == ".tsv" else ","
        rtype = table_config.get("record_type", "metric")
        ts_col = table_config.get("timestamp_col", "timestamp")
        ts_unit = table_config.get("timestamp_unit", "ms")
        entity_col = table_config.get("entity_col", "entity")
        entity_role = table_config.get("entity_role", "service")

        with file_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delim)
            for row in reader:
                raw_ts = row.get(ts_col)
                if not raw_ts:
                    continue
                try:
                    ts = int(float(raw_ts))
                    if ts_unit == "s":
                        ts *= 1000
                except (ValueError, TypeError):
                    continue

                if not (window.start_ms_utc <= ts < window.end_ms_utc):
                    continue

                ent_id = row.get(entity_col, "")
                ent = EntityRef(entity_role, ent_id) if ent_id else EntityRef(entity_role, "unknown")

                if rtype == "metric":
                    m_name = row.get(table_config.get("metric_name_col", "metric_name"), "metric")
                    val_str = row.get(table_config.get("value_col", "value"))
                    try:
                        val = float(val_str) if val_str is not None and val_str != "" else None
                    except ValueError:
                        val = None
                    yield MetricRecord(
                        source_table=table_name,
                        timestamp_ms_utc=ts,
                        entity=ent,
                        metric_name=m_name,
                        value=val,
                    )
                elif rtype == "span":
                    trace_id = row.get(table_config.get("trace_id_col", "trace_id"), "")
                    span_id = row.get(table_config.get("span_id_col", "span_id"), "")
                    parent_id = row.get(table_config.get("parent_id_col", "parent_id"))
                    dur_str = row.get(table_config.get("duration_col", "duration_ms"))
                    try:
                        dur = float(dur_str) if dur_str is not None else None
                    except ValueError:
                        dur = None
                    yield SpanRecord(
                        source_table=table_name,
                        timestamp_ms_utc=ts,
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_id=parent_id,
                        duration_ms=dur,
                        success=True,
                        executing_component=ent,
                    )
                elif rtype == "log":
                    msg = row.get(table_config.get("message_col", "message"))
                    log_name = row.get(table_config.get("log_name_col", "log_name"))
                    yield LogRecord(
                        source_table=table_name,
                        timestamp_ms_utc=ts,
                        entity=ent,
                        log_name=log_name,
                        value=msg,
                    )
