"""OpenRCA Bank benchmark dataset adapter implementing BaseDatasetAdapter."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterator, Sequence

from e4.data.adapter import BaseDatasetAdapter
from e4.types import (
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


class BankAdapter(BaseDatasetAdapter):
    """Adapts OpenRCA Bank incident cases to the E4 TelemetrySource contract."""

    def __init__(self, case_dir: Path | str):
        self.case_dir = Path(case_dir)
        alert_file = self.case_dir / "alert.json"
        if not alert_file.is_file():
            raise FileNotFoundError(f"Missing alert.json in {self.case_dir}")

        self.alert_data = json.loads(alert_file.read_text(encoding="utf-8"))
        self._init_windows()
        self._init_schema()
        self._locate_data_dir()

    def _init_windows(self) -> None:
        ref_start = int(self.alert_data["analysis_window_start_epoch_ms_utc"])
        alt_start = int(self.alert_data["alert_window_start_epoch_ms_utc"])
        alt_end = int(self.alert_data["alert_window_end_epoch_ms_utc"])
        self._ref_window = Window("reference", ref_start, alt_start)
        self._alt_window = Window("alert", alt_start, alt_end)

    def _locate_data_dir(self) -> None:
        dates = self.alert_data.get("telemetry_dates", [self.alert_data.get("telemetry_date", "")])
        for d in dates:
            candidate = self.case_dir / "source_dates" / d
            if candidate.is_dir():
                self.data_dir = candidate
                return
        self.data_dir = self.case_dir

    def _init_schema(self) -> None:
        self._schema = DatasetSchema(
            tables=(
                TableSchema(
                    name="bank/metric_container.csv",
                    record_type="metric",
                    columns=(
                        ColumnSchema("timestamp", "timestamp", unit="s"),
                        ColumnSchema("cmdb_id", "string", description="Component/Service ID"),
                        ColumnSchema("kpi_name", "string", description="Metric KPI name"),
                        ColumnSchema("value", "number", description="Measurement value"),
                    ),
                    entity_roles=("service", "container"),
                ),
                TableSchema(
                    name="bank/trace_span.csv",
                    record_type="span",
                    columns=(
                        ColumnSchema("timestamp", "timestamp", unit="ms"),
                        ColumnSchema("cmdb_id", "string"),
                        ColumnSchema("trace_id", "string"),
                        ColumnSchema("span_id", "string"),
                        ColumnSchema("parent_id", "string", nullable=True),
                        ColumnSchema("duration", "number", unit="ms"),
                    ),
                    entity_roles=("service",),
                ),
                TableSchema(
                    name="bank/log/log_service.csv",
                    record_type="log",
                    columns=(
                        ColumnSchema("timestamp", "timestamp", unit="s"),
                        ColumnSchema("log_id", "string"),
                        ColumnSchema("cmdb_id", "string"),
                        ColumnSchema("log_name", "string"),
                        ColumnSchema("value", "string"),
                    ),
                    entity_roles=("service",),
                ),
            ),
            time_unit="ms_utc",
            description="OpenRCA Bank microservice benchmark telemetry",
        )

    def get_schema(self) -> DatasetSchema:
        return self._schema

    def get_windows(self) -> tuple[Window, Window]:
        return (self._ref_window, self._alt_window)

    def extract_suspect_entities(self) -> list[EntityRef]:
        entities: set[str] = set()
        metric_file = self.data_dir / "metric" / "metric_container.csv"
        if metric_file.is_file():
            with metric_file.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row.get("cmdb_id")
                    if cid and cid.strip():
                        entities.add(cid.strip())

        if not entities:
            # Fallback if metric file is empty or unavailable
            entities.update(["db-001", "service-order", "service-payment", "redis-001"])

        return [EntityRef(role="service", identifier=cid) for cid in sorted(entities)]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        if "metric_container" in table_name:
            file_path = self.data_dir / "metric" / "metric_container.csv"
            if not file_path.is_file():
                return
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw_ts = row.get("timestamp")
                    if not raw_ts:
                        continue
                    try:
                        ts_ms = int(float(raw_ts) * 1000)
                    except ValueError:
                        continue

                    if not (window.start_ms_utc <= ts_ms < window.end_ms_utc):
                        continue

                    cid = row.get("cmdb_id", "unknown")
                    kpi = row.get("kpi_name", "unknown_metric")
                    val_str = row.get("value")
                    try:
                        val = float(val_str) if val_str is not None else None
                    except ValueError:
                        val = None

                    yield MetricRecord(
                        source_table=table_name,
                        timestamp_ms_utc=ts_ms,
                        entity=EntityRef("service", cid),
                        metric_name=kpi,
                        value=val,
                    )

        elif "trace_span" in table_name:
            file_path = self.data_dir / "trace" / "trace_span.csv"
            if not file_path.is_file():
                return
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw_ts = row.get("timestamp")
                    if not raw_ts:
                        continue
                    try:
                        ts_ms = int(float(raw_ts))
                    except ValueError:
                        continue

                    if not (window.start_ms_utc <= ts_ms < window.end_ms_utc):
                        continue

                    cid = row.get("cmdb_id", "unknown")
                    dur_str = row.get("duration")
                    try:
                        dur = float(dur_str) if dur_str is not None else None
                    except ValueError:
                        dur = None

                    yield SpanRecord(
                        source_table=table_name,
                        timestamp_ms_utc=ts_ms,
                        trace_id=row.get("trace_id", ""),
                        span_id=row.get("span_id", ""),
                        parent_id=row.get("parent_id"),
                        duration_ms=dur,
                        success=True,
                        executing_component=EntityRef("service", cid),
                    )

        elif "log_service" in table_name:
            file_path = self.data_dir / "log" / "log_service.csv"
            if not file_path.is_file():
                return
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw_ts = row.get("timestamp")
                    if not raw_ts:
                        continue
                    try:
                        ts_ms = int(float(raw_ts) * 1000)
                    except ValueError:
                        continue

                    if not (window.start_ms_utc <= ts_ms < window.end_ms_utc):
                        continue

                    cid = row.get("cmdb_id", "unknown")
                    yield LogRecord(
                        source_table=table_name,
                        timestamp_ms_utc=ts_ms,
                        entity=EntityRef("service", cid),
                        log_name=row.get("log_name"),
                        value=row.get("value"),
                    )
