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


class BankAdapterSchemaError(ValueError):
    """Raised when source unit metadata cannot safely normalize a CSV timestamp."""


class BankAdapter(BaseDatasetAdapter):
    """Adapts OpenRCA Bank incident cases to the E4 TelemetrySource contract."""

    def __init__(self, case_dir: Path | str):
        self.case_dir = Path(case_dir)
        alert_file = self.case_dir / "alert.json"
        if not alert_file.is_file():
            raise FileNotFoundError(f"Missing alert.json in {self.case_dir}")

        self.alert_data = json.loads(alert_file.read_text(encoding="utf-8"))
        self._init_windows()
        self._locate_data_dirs()
        self._init_schema()

    def _init_windows(self) -> None:
        ref_start = int(self.alert_data["analysis_window_start_epoch_ms_utc"])
        alt_start = int(self.alert_data["alert_window_start_epoch_ms_utc"])
        alt_end = int(self.alert_data["alert_window_end_epoch_ms_utc"])
        self._ref_window = Window("reference", ref_start, alt_start)
        self._alt_window = Window("alert", alt_start, alt_end)

    def _locate_data_dirs(self) -> None:
        self.data_dirs: list[Path] = []
        source_dates_dir = self.case_dir / "source_dates"
        if source_dates_dir.is_dir():
            dates = self.alert_data.get("telemetry_dates", [self.alert_data.get("telemetry_date", "")])
            for d in dates:
                if d:
                    candidate = source_dates_dir / d
                    if candidate.is_dir():
                        self.data_dirs.append(candidate)
            if not self.data_dirs:
                self.data_dirs = sorted([d for d in source_dates_dir.iterdir() if d.is_dir()])
        if not self.data_dirs:
            self.data_dirs = [self.case_dir]
        self.data_dir = self.data_dirs[0]

    def _init_schema(self) -> None:
        declared_schema = DatasetSchema(
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
                    name="bank/metric_service.csv",
                    record_type="metric",
                    columns=(
                        ColumnSchema("timestamp", "timestamp", unit="s"),
                        ColumnSchema("cmdb_id", "string", description="Component/Service ID"),
                        ColumnSchema("kpi_name", "string", description="Metric KPI name"),
                        ColumnSchema("value", "number", description="Measurement value"),
                    ),
                    entity_roles=("service",),
                ),
                TableSchema(
                    name="bank/metric_app.csv",
                    record_type="metric",
                    columns=(
                        ColumnSchema("timestamp", "timestamp", unit="s"),
                        ColumnSchema("tc", "string", description="Application transaction code / test case"),
                        ColumnSchema("rr", "number", nullable=True, description="Source metric rr; unit/meaning undeclared"),
                        ColumnSchema("sr", "number", nullable=True, description="Source metric sr; unit/meaning undeclared"),
                        ColumnSchema("cnt", "number", nullable=True, description="Source metric cnt; unit/meaning undeclared"),
                        ColumnSchema("mrt", "number", nullable=True, description="Source metric mrt; unit undeclared"),
                    ),
                    entity_roles=("app_kpi",),
                    description="Application KPI context. Units and metric meanings are not declared by the source.",
                ),
                TableSchema(
                    name="bank/trace_span.csv",
                    record_type="span",
                    columns=(
                        ColumnSchema("timestamp", "timestamp", unit=None, description="Raw unit is declared per source date; normalized to UTC milliseconds"),
                        ColumnSchema("cmdb_id", "string"),
                        ColumnSchema("trace_id", "string"),
                        ColumnSchema("span_id", "string"),
                        ColumnSchema("parent_id", "string", nullable=True),
                        ColumnSchema("duration", "number", unit=None),
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
        relative_paths = {
            "bank/metric_container.csv": "metric/metric_container.csv",
            "bank/metric_service.csv": "metric/metric_service.csv",
            "bank/metric_app.csv": "metric/metric_app.csv",
            "bank/trace_span.csv": "trace/trace_span.csv",
            "bank/log/log_service.csv": "log/log_service.csv",
        }
        # Availability is inferred from public case files only. A present but
        # empty CSV remains declared; a table absent from all source dates does not.
        available_tables = tuple(
            table
            for table in declared_schema.tables
            if any((data_dir / relative_paths[table.name]).is_file() for data_dir in self.data_dirs)
        )
        self._schema = DatasetSchema(
            tables=available_tables,
            time_unit=declared_schema.time_unit,
            description=declared_schema.description,
        )

    def get_schema(self) -> DatasetSchema:
        return self._schema

    def get_windows(self) -> tuple[Window, Window]:
        return (self._ref_window, self._alt_window)

    def _timestamp_ms(self, raw_value: str, source_dir: Path, relative_path: str, default_unit: str) -> int:
        """Normalize a source timestamp using its audited per-date unit metadata."""
        unit = self.alert_data.get("source_date_timestamp_units", {}).get(source_dir.name, {}).get(relative_path)
        if unit is None:
            unit = self.alert_data.get("timestamp_unit_in_source_files", {}).get(relative_path)
        if unit is None:
            unit = default_unit
        normalized_unit = str(unit).strip().lower().replace("epoch ", "")
        try:
            value = float(raw_value)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Invalid timestamp {raw_value!r} in {relative_path}") from exc
        if normalized_unit in {"s", "sec", "second", "seconds"}:
            return int(round(value * 1000))
        if normalized_unit in {"ms", "millisecond", "milliseconds"}:
            return int(round(value))
        raise BankAdapterSchemaError(
            f"Unsupported timestamp unit {unit!r} for {source_dir.name}/{relative_path}"
        )

    def extract_suspect_entities(self) -> list[EntityRef]:
        entities: set[str] = set()
        for ddir in self.data_dirs:
            for sub, file_name in [("metric", "metric_container.csv"), ("metric", "metric_service.csv"), ("trace", "trace_span.csv")]:
                metric_file = ddir / sub / file_name
                if metric_file.is_file():
                    with metric_file.open("r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            cid = row.get("cmdb_id")
                            if cid and cid.strip():
                                entities.add(cid.strip())

        return [EntityRef(role="service", identifier=cid) for cid in sorted(entities)]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        if "metric_app" in table_name:
            for ddir in self.data_dirs:
                file_path = ddir / "metric" / "metric_app.csv"
                if not file_path.is_file():
                    continue
                with file_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    metric_fields = ("rr", "sr", "cnt", "mrt")
                    for row_idx, row in enumerate(reader):
                        raw_ts = row.get("timestamp")
                        if not raw_ts:
                            continue
                        try:
                            ts_ms = self._timestamp_ms(raw_ts, ddir, "metric/metric_app.csv", "seconds")
                        except BankAdapterSchemaError:
                            raise
                        except ValueError:
                            continue

                        if not (window.start_ms_utc <= ts_ms < window.end_ms_utc):
                            continue

                        tc = (row.get("tc") or "").strip()
                        if not tc:
                            # `tc` is the only declared identity for application
                            # context. Do not invent a shared placeholder entity.
                            continue
                        ent = EntityRef(role="app_kpi", identifier=tc)
                        for col_name in metric_fields:
                            val_str = row.get(col_name)
                            if val_str is None or val_str == "":
                                continue
                            try:
                                val = float(val_str)
                            except ValueError:
                                val = None
                            yield MetricRecord(
                                source_table=table_name,
                                timestamp_ms_utc=ts_ms,
                                entity=ent,
                                metric_name=col_name,
                                value=val,
                                unit=None,
                                details={
                                    "tc": tc,
                                    "kpi_column": col_name,
                                    "origin_row_id": f"source_dates/{ddir.name}/metric/metric_app.csv:{row_idx}",
                                    "source_unit": None,
                                },
                            )
            return

        if "metric" in table_name:
            sub_file = "metric_container.csv" if "metric_container" in table_name else "metric_service.csv"
            for ddir in self.data_dirs:
                file_path = ddir / "metric" / sub_file
                if not file_path.is_file():
                    continue
                with file_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row_idx, row in enumerate(reader):
                        raw_ts = row.get("timestamp")
                        if not raw_ts:
                            continue
                        try:
                            ts_ms = self._timestamp_ms(raw_ts, ddir, f"metric/{sub_file}", "seconds")
                        except BankAdapterSchemaError:
                            raise
                        except ValueError:
                            continue

                        if not (window.start_ms_utc <= ts_ms < window.end_ms_utc):
                            continue

                        cid = (row.get("cmdb_id") or "").strip()
                        kpi = (row.get("kpi_name") or "").strip()
                        if not cid or not kpi:
                            continue
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
                            unit=None,
                            details={
                                "origin_row_id": f"source_dates/{ddir.name}/metric/{sub_file}:{row_idx}",
                                "kpi_name": kpi,
                            },
                        )

        elif "trace_span" in table_name:
            for ddir in self.data_dirs:
                file_path = ddir / "trace" / "trace_span.csv"
                if not file_path.is_file():
                    continue
                with file_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        raw_ts = row.get("timestamp")
                        if not raw_ts:
                            continue
                        try:
                            ts_ms = self._timestamp_ms(raw_ts, ddir, "trace/trace_span.csv", "milliseconds")
                        except BankAdapterSchemaError:
                            raise
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

                        raw_succ = row.get("success")
                        succ = str(raw_succ).lower() in ("true", "1", "yes", "t") if raw_succ is not None and raw_succ != "" else None

                        yield SpanRecord(
                            source_table=table_name,
                            timestamp_ms_utc=ts_ms,
                            trace_id=row.get("trace_id", ""),
                            span_id=row.get("span_id", ""),
                            parent_id=row.get("parent_id"),
                            duration_ms=dur,
                            success=succ,
                            executing_component=EntityRef("service", cid),
                        )

        elif "log_service" in table_name:
            for ddir in self.data_dirs:
                file_path = ddir / "log" / "log_service.csv"
                if not file_path.is_file():
                    continue
                with file_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        raw_ts = row.get("timestamp")
                        if not raw_ts:
                            continue
                        try:
                            ts_ms = self._timestamp_ms(raw_ts, ddir, "log/log_service.csv", "seconds")
                        except BankAdapterSchemaError:
                            raise
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
