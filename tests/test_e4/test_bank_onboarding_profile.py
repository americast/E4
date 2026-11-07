"""Offline prompt and parity gates for the generated Bank adapter profile."""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.bank_onboarding import (
    bank_adapter_config,
    build_bank_adapter_prompt,
    validate_bank_adapter_submission,
)


DEV_IDS = ("bank-20210323T0630-utc8", "bank-20210325T1530-utc8")


def _write_csv(path: Path, header: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)


def _make_case(root: Path, case_id: str, date: str, default_unit: str) -> None:
    case = root / case_id
    metric_raw = (5, 15) if default_unit == "seconds" else (5000, 15000)
    trace_unit = "milliseconds" if default_unit == "seconds" else "seconds"
    trace_raw = (5000, 15000) if trace_unit == "milliseconds" else (5, 15)
    file_units = {
        "metric/metric_container.csv": default_unit,
        "metric/metric_app.csv": default_unit,
        "trace/trace_span.csv": trace_unit,
        "log/log_service.csv": default_unit,
    }
    alert = {
        "alert_text": "A single failure occurred in the declared interval. Cause and component are unknown.",
        "analysis_window_start_epoch_ms_utc": 0,
        "alert_window_start_epoch_ms_utc": 10000,
        "alert_window_end_epoch_ms_utc": 20000,
        "telemetry_dates": [date],
        "telemetry_date": date,
        "timezone": "Asia/Shanghai",
        "source_date_timestamp_units": {date: file_units},
        "timestamp_unit_in_source_files": {
            path: ("epoch " + unit) for path, unit in file_units.items()
        },
    }
    case.mkdir(parents=True)
    (case / "alert.json").write_text(json.dumps(alert), encoding="utf-8")

    _write_csv(
        case / "source_dates" / date / "metric" / "metric_container.csv",
        ("timestamp", "cmdb_id", "kpi_name", "value"),
        [(metric_raw[0], "svc-a", "cpu", 1.0), (metric_raw[1], "svc-a", "cpu", 2.0)],
    )
    _write_csv(
        case / "source_dates" / date / "metric" / "metric_app.csv",
        ("timestamp", "rr", "sr", "cnt", "mrt", "tc"),
        [(metric_raw[0], 100, 98, 4, 12.5, "opaque-tc-1"), (metric_raw[1], 95, 93, 5, 20.0, "opaque-tc-1")],
    )
    _write_csv(
        case / "source_dates" / date / "trace" / "trace_span.csv",
        ("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"),
        [
            (trace_raw[0], "svc-a", "root", "span-1", "trace-1", 1.25),
            (trace_raw[1], "svc-a", "root", "span-2", "trace-2", 2.5),
        ],
    )
    _write_csv(
        case / "source_dates" / date / "log" / "log_service.csv",
        ("log_id", "timestamp", "cmdb_id", "log_name", "value"),
        [("row-1", metric_raw[0], "svc-a", "service", "ref log"), ("row-2", metric_raw[1], "svc-a", "service", "alert log")],
    )


_BANK_ADAPTER_SOURCE = '''from __future__ import annotations

import csv
import json
from collections.abc import Iterator, Mapping
from pathlib import Path

from e4.data.adapter import BaseDatasetAdapter
from e4.types import ColumnSchema, DatasetSchema, EntityRef, LogRecord, MetricRecord, SpanRecord, TableSchema, TelemetryRecord, Window

_TABLES = {
    "bank/metric_container.csv": ("metric/metric_container.csv", "metric"),
    "bank/metric_service.csv": ("metric/metric_service.csv", "metric"),
    "bank/metric_app.csv": ("metric/metric_app.csv", "app"),
    "bank/trace_span.csv": ("trace/trace_span.csv", "span"),
    "bank/log/log_service.csv": ("log/log_service.csv", "log"),
}

class DatasetAdapter(BaseDatasetAdapter):
    def __init__(self, config: Mapping[str, object]) -> None:
        self._case_dir = Path(str(config["case_dir"]))
        self._alert = json.loads((self._case_dir / "alert.json").read_text(encoding="utf-8"))

    def get_schema(self) -> DatasetSchema:
        tables = []
        for table_name, (relative_path, kind) in _TABLES.items():
            present = any((self._case_dir / "source_dates" / date / relative_path).is_file() for date in self._alert.get("telemetry_dates", []))
            if not present:
                continue
            if kind == "metric":
                columns = (
                    ColumnSchema("timestamp", "timestamp", unit="s"),
                    ColumnSchema("cmdb_id", "string"),
                    ColumnSchema("kpi_name", "string"),
                    ColumnSchema("value", "number"),
                )
                roles = ("service", "container") if "container" in table_name else ("service",)
            elif kind == "app":
                columns = (
                    ColumnSchema("timestamp", "timestamp", unit="s"),
                    ColumnSchema("tc", "string"),
                    ColumnSchema("rr", "number", nullable=True),
                    ColumnSchema("sr", "number", nullable=True),
                    ColumnSchema("cnt", "number", nullable=True),
                    ColumnSchema("mrt", "number", nullable=True),
                )
                roles = ("app_kpi",)
            elif kind == "span":
                columns = (
                    ColumnSchema("timestamp", "timestamp"),
                    ColumnSchema("cmdb_id", "string"),
                    ColumnSchema("trace_id", "string"),
                    ColumnSchema("span_id", "string"),
                    ColumnSchema("parent_id", "string", nullable=True),
                    ColumnSchema("duration", "number"),
                )
                roles = ("service",)
            else:
                columns = (
                    ColumnSchema("timestamp", "timestamp", unit="s"),
                    ColumnSchema("log_id", "string"),
                    ColumnSchema("cmdb_id", "string"),
                    ColumnSchema("log_name", "string"),
                    ColumnSchema("value", "string"),
                )
                roles = ("service",)
            tables.append(TableSchema(table_name, kind if kind != "app" else "metric", columns, entity_roles=roles))
        return DatasetSchema(tuple(tables), time_unit="ms_utc")

    def get_windows(self) -> tuple[Window, Window]:
        return (
            Window("reference", int(self._alert["analysis_window_start_epoch_ms_utc"]), int(self._alert["alert_window_start_epoch_ms_utc"])),
            Window("alert", int(self._alert["alert_window_start_epoch_ms_utc"]), int(self._alert["alert_window_end_epoch_ms_utc"])),
        )

    def extract_suspect_entities(self) -> list[EntityRef]:
        identifiers = set()
        for table_name in ("bank/metric_container.csv", "bank/metric_service.csv", "bank/trace_span.csv"):
            relative_path, _ = _TABLES[table_name]
            for date in self._alert.get("telemetry_dates", []):
                path = self._case_dir / "source_dates" / date / relative_path
                if path.is_file():
                    with path.open("r", encoding="utf-8-sig", newline="") as stream:
                        for row in csv.DictReader(stream):
                            identifier = row.get("cmdb_id")
                            if identifier:
                                identifiers.add(identifier.strip())
        return [EntityRef("service", identifier) for identifier in sorted(identifiers)]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        if table_name not in _TABLES:
            return
        relative_path, kind = _TABLES[table_name]
        for date in self._alert.get("telemetry_dates", []):
            path = self._case_dir / "source_dates" / date / relative_path
            if not path.is_file():
                continue
            unit = self._alert.get("source_date_timestamp_units", {}).get(date, {}).get(relative_path)
            if unit is None:
                unit = self._alert.get("timestamp_unit_in_source_files", {}).get(relative_path)
            if unit is None:
                unit = "seconds" if kind in {"metric", "app", "log"} else "milliseconds"
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                for row_index, row in enumerate(csv.DictReader(stream)):
                    raw_timestamp = float(row["timestamp"])
                    normalized_unit = str(unit).strip().lower().replace("epoch ", "")
                    timestamp = int(round(raw_timestamp * 1000)) if normalized_unit in {"seconds", "second", "s", "sec"} else int(round(raw_timestamp))
                    if not window.start_ms_utc <= timestamp < window.end_ms_utc:
                        continue
                    if kind == "metric":
                        yield MetricRecord(table_name, timestamp, EntityRef("service", row["cmdb_id"].strip()), row["kpi_name"].strip(), float(row["value"]), None, {"origin_row_id": f"source_dates/{date}/{relative_path}:{row_index}", "kpi_name": row["kpi_name"].strip()})
                    elif kind == "app":
                        origin = f"source_dates/{date}/{relative_path}:{row_index}"
                        tc = row["tc"].strip()
                        for metric_name in ("rr", "sr", "cnt", "mrt"):
                            raw_value = row.get(metric_name)
                            if raw_value is not None and raw_value != "":
                                yield MetricRecord(table_name, timestamp, EntityRef("app_kpi", tc), metric_name, float(raw_value), None, {"tc": tc, "kpi_column": metric_name, "origin_row_id": origin})
                    elif kind == "span":
                        duration = row.get("duration")
                        yield SpanRecord(table_name, timestamp, row.get("trace_id", ""), row.get("span_id", ""), row.get("parent_id"), None if duration in (None, "") else float(duration), None, executing_component=EntityRef("service", row["cmdb_id"].strip()))
                    else:
                        yield LogRecord(table_name, timestamp, EntityRef("service", row["cmdb_id"].strip()), row.get("log_name"), row.get("value"))

def create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter:
    return DatasetAdapter(config)
'''


class BankOnboardingProfileTests(unittest.TestCase):
    def test_prompt_uses_only_configured_public_development_cases_without_ids_or_labels(self) -> None:
        cases_dir = Path("data/prepared/bank/public/cases")
        prompt = build_bank_adapter_prompt(cases_dir, DEV_IDS)
        with open("config/bank_sample.json", encoding="utf-8") as stream:
            scored_ids = json.load(stream)["selection"]["scored_ids"]

        self.assertIn("development_case_1", prompt)
        self.assertIn("source_date_timestamp_units", prompt)
        self.assertIn("analysis_window_start_epoch_ms_utc", prompt)
        self.assertIn("alert_window_start_epoch_ms_utc", prompt)
        self.assertIn("alert_window_end_epoch_ms_utc", prompt)
        self.assertIn("Required Python module template", prompt)
        self.assertIn("Draft 2020-12", prompt)
        self.assertIn("duration", prompt)
        self.assertIn("app_kpi", prompt)
        self.assertIn("Missing trace success remains `None`", prompt)
        for case_id in (*DEV_IDS, *scored_ids):
            self.assertNotIn(case_id, prompt)
        self.assertNotIn('"windows":', prompt)
        self.assertNotIn("ground_truth", prompt.casefold())
        self.assertNotIn("BankAdapter", prompt)
        self.assertEqual(bank_adapter_config("/tmp/case"), {"case_dir": "/tmp/case"})

    def test_generated_adapter_matches_public_development_fixtures_across_timestamp_units(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cases_root = Path(directory)
            _make_case(cases_root, DEV_IDS[0], "2021_03_23", "seconds")
            _make_case(cases_root, DEV_IDS[1], "2021_03_25", "milliseconds")
            submission = {
                "status": "ready",
                "source": _BANK_ADAPTER_SOURCE,
                "config_schema": {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "properties": {"case_dir": {"type": "string", "minLength": 1}},
                    "required": ["case_dir"],
                    "additionalProperties": False,
                },
            }
            # Draft 2020-12 is the validator default when the optional identifier is omitted.
            submission["config_schema"].pop("$schema")
            report = validate_bank_adapter_submission(submission, cases_root, DEV_IDS)

        self.assertEqual(report["status"], "conforming", report.get("errors"))
        self.assertEqual(report["source_manifest"]["development_ids"], list(DEV_IDS))
        self.assertEqual(len(report["cases"]), 2)
        for case_report in report["cases"]:
            self.assertTrue(case_report["windows_match"])
            self.assertTrue(case_report["schema_match"])
            self.assertTrue(case_report["suspect_entities_match"])
            self.assertEqual(case_report["tables"]["bank/metric_container.csv"]["reference"]["reference_count"], 1)
            self.assertEqual(case_report["tables"]["bank/metric_container.csv"]["alert"]["candidate_count"], 1)
            self.assertEqual(case_report["tables"]["bank/trace_span.csv"]["reference"]["sample_records_compared"], 1)

    def test_gate_rejects_static_seconds_and_existing_adapter_delegation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cases_root = Path(directory)
            _make_case(cases_root, DEV_IDS[0], "2021_03_23", "seconds")
            _make_case(cases_root, DEV_IDS[1], "2021_03_25", "milliseconds")
            schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"case_dir": {"type": "string"}},
                "required": ["case_dir"],
                "additionalProperties": False,
            }
            hardcoded_seconds = _BANK_ADAPTER_SOURCE.replace(
                'unit = self._alert.get("source_date_timestamp_units", {}).get(date, {}).get(relative_path)',
                'unit = "seconds"',
            )
            bad_unit = validate_bank_adapter_submission(
                {"status": "ready", "source": hardcoded_seconds, "config_schema": schema},
                cases_root,
                DEV_IDS,
            )
            self.assertEqual(bad_unit["status"], "invalid")
            self.assertEqual(bad_unit["stage"], "milliseconds_normalization")

            substring_bug = _BANK_ADAPTER_SOURCE.replace(
                'normalized_unit in {"seconds", "second", "s", "sec"}',
                '"second" in normalized_unit',
            )
            bad_milliseconds = validate_bank_adapter_submission(
                {"status": "ready", "source": substring_bug, "config_schema": schema},
                cases_root,
                DEV_IDS,
            )
            self.assertEqual(bad_milliseconds["status"], "invalid")
            self.assertEqual(bad_milliseconds["stage"], "milliseconds_normalization")
            self.assertTrue(any("milliseconds" in error["message"] for error in bad_milliseconds["errors"]))

            delegated = _BANK_ADAPTER_SOURCE.replace(
                "from e4.data.adapter import BaseDatasetAdapter",
                "from e4.data.adapter import BaseDatasetAdapter\nfrom adapters.bank_adapter import BankAdapter",
            )
            forbidden = validate_bank_adapter_submission(
                {"status": "ready", "source": delegated, "config_schema": schema},
                cases_root,
                DEV_IDS,
            )
            self.assertEqual(forbidden["stage"], "source")

            extra_config = validate_bank_adapter_submission(
                {
                    "status": "ready",
                    "source": _BANK_ADAPTER_SOURCE,
                    "config_schema": {
                        **schema,
                        "properties": {
                            **schema["properties"],
                            "timezone": {"type": "string"},
                        },
                    },
                },
                cases_root,
                DEV_IDS,
            )
            self.assertEqual(extra_config["status"], "invalid")
            self.assertEqual(extra_config["stage"], "config_schema")

            wrong_draft = validate_bank_adapter_submission(
                {
                    "status": "ready",
                    "source": _BANK_ADAPTER_SOURCE,
                    "config_schema": {**schema, "$schema": "https://json-schema.org/draft/2019-09/schema"},
                },
                cases_root,
                DEV_IDS,
            )
            self.assertEqual(wrong_draft["stage"], "config_schema")

    def test_gate_rejects_nondevelopment_case_selection_and_needs_input_is_reported(self) -> None:
        with self.assertRaises(ValueError):
            build_bank_adapter_prompt("data/prepared/bank/public/cases", ("bank-20210325T2000-utc8",))

        with tempfile.TemporaryDirectory() as directory:
            case_paths = Path(directory)
            for case_id, date in zip(DEV_IDS, ("2021_03_23", "2021_03_25")):
                _make_case(case_paths, case_id, date, "seconds")
            report = validate_bank_adapter_submission(
                {"status": "needs_input", "required_fields": [{"name": "timezone", "type": "string", "reason": "Timestamp metadata needs user clarification."}]},
                case_paths,
                DEV_IDS,
            )
        self.assertEqual(report["status"], "needs_input")
        self.assertEqual(report["required_fields"][0]["name"], "timezone")


if __name__ == "__main__":
    unittest.main()
