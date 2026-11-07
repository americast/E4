"""Trusted adapter submission, loader, and conformance integration tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from e4.data.adapter import AdapterConformanceError, BaseDatasetAdapter, check_adapter_conformance
from e4.data.folder import FolderTelemetrySource
from e4.data.submissions import (
    AdapterInputRequired,
    AdapterSubmissionError,
    load_adapter_submission,
    parse_adapter_submission,
    validate_adapter_config,
)
from e4.types import ColumnSchema, DatasetSchema, EntityRef, LogRecord, MetricRecord, TableSchema, Window


class _WindowContractAdapter(BaseDatasetAdapter):
    def __init__(self, records_by_window):
        self.records_by_window = records_by_window

    def get_schema(self):
        return DatasetSchema((
            TableSchema(
                "metrics.csv",
                "metric",
                (ColumnSchema("timestamp", "integer"), ColumnSchema("value", "number")),
                entity_roles=("service",),
            ),
        ))

    def get_windows(self):
        return Window("reference", 0, 10), Window("alert", 10, 20)

    def extract_suspect_entities(self):
        return [EntityRef("service", "svc-a")]

    def stream_records(self, table_name, window):
        yield from self.records_by_window.get(window.name, ())


class _ListStreamAdapter(_WindowContractAdapter):
    def stream_records(self, table_name, window):
        return list(self.records_by_window.get(window.name, ()))


class _ListWindowsAdapter(_WindowContractAdapter):
    def get_windows(self):
        return [Window("reference", 0, 10), Window("alert", 10, 20)]


class AdapterConformanceIntegrationTests(unittest.TestCase):
    def test_folder_telemetry_source_remains_conforming(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "metrics.csv").write_text(
                "timestamp,service,metric_name,value\n"
                "5,svc-a,cpu,1.0\n"
                "15,svc-a,cpu,2.0\n",
                encoding="utf-8",
            )
            adapter = FolderTelemetrySource(
                root,
                mapping={
                    "windows": {"reference": [0, 10], "alert": [10, 20]},
                    "tables": [{
                        "name": "metrics.csv",
                        "file": "metrics.csv",
                        "record_type": "metric",
                        "timestamp_col": "timestamp",
                        "entity_col": "service",
                        "metric_name_col": "metric_name",
                        "value_col": "value",
                        "entity_role": "service",
                    }],
                },
            )
            result = check_adapter_conformance(adapter)
            self.assertEqual(result["status"], "conforming")
            self.assertEqual(result["table_window_counts"]["metrics.csv"], {"reference": 1, "alert": 1})

    def test_typed_records_are_checked_in_both_windows_and_empty_tables_are_valid(self) -> None:
        adapter = _WindowContractAdapter({
            "reference": [MetricRecord("metrics.csv", 5, EntityRef("service", "svc-a"), "cpu", 1.0)],
            "alert": [MetricRecord("metrics.csv", 15, EntityRef("service", "svc-a"), "cpu", 2.0)],
        })
        result = check_adapter_conformance(adapter)
        self.assertEqual(result["status"], "conforming")
        self.assertEqual(result["table_counts"], {"metrics.csv": 1})
        self.assertEqual(result["table_window_counts"], {"metrics.csv": {"reference": 1, "alert": 1}})
        self.assertEqual(result["total_records_sampled"], 1)
        self.assertEqual(result["total_records_checked"], 2)

        empty_result = check_adapter_conformance(_WindowContractAdapter({}))
        self.assertEqual(empty_result["table_window_counts"], {"metrics.csv": {"reference": 0, "alert": 0}})

    def test_declared_record_kind_and_reference_window_boundary_are_enforced(self) -> None:
        wrong_kind = _WindowContractAdapter({
            "reference": [LogRecord("metrics.csv", 5, EntityRef("service", "svc-a"), "app", "message")],
        })
        with self.assertRaisesRegex(AdapterConformanceError, "declares record_type 'metric'"):
            check_adapter_conformance(wrong_kind)

        outside_reference = _WindowContractAdapter({
            "reference": [MetricRecord("metrics.csv", 10, EntityRef("service", "svc-a"), "cpu", 1.0)],
        })
        with self.assertRaisesRegex(AdapterConformanceError, "outside reference window"):
            check_adapter_conformance(outside_reference)

        outside_alert = _WindowContractAdapter({
            "alert": [MetricRecord("metrics.csv", 20, EntityRef("service", "svc-a"), "cpu", 1.0)],
        })
        with self.assertRaisesRegex(AdapterConformanceError, "outside alert window"):
            check_adapter_conformance(outside_alert)

        wrong_source = _WindowContractAdapter({
            "alert": [MetricRecord("other.csv", 15, EntityRef("service", "svc-a"), "cpu", 1.0)],
        })
        with self.assertRaisesRegex(AdapterConformanceError, "does not match requested table"):
            check_adapter_conformance(wrong_source)

    def test_fixed_return_shapes_require_window_tuple_and_record_iterator(self) -> None:
        with self.assertRaisesRegex(AdapterConformanceError, "must return a 2-tuple"):
            check_adapter_conformance(_ListWindowsAdapter({}))

        with self.assertRaisesRegex(AdapterConformanceError, "must return an iterator or generator"):
            check_adapter_conformance(_ListStreamAdapter({}))


def _ready_submission(marker_path: Path, *, mode: str = "valid") -> dict[str, object]:
    source = f'''from __future__ import annotations

import csv
from collections.abc import Iterator, Mapping
from pathlib import Path

from e4.data.adapter import BaseDatasetAdapter
from e4.types import ColumnSchema, DatasetSchema, EntityRef, MetricRecord, TableSchema, TelemetryRecord, Window

Path({str(marker_path)!r}).write_text("executed", encoding="utf-8")

class DatasetAdapter(BaseDatasetAdapter):
    def __init__(self, config: Mapping[str, object]) -> None:
        self.config = dict(config)

    def get_schema(self) -> DatasetSchema:
        if {mode!r} == "bad_schema":
            return "not a schema"
        if {mode!r} == "missing_metadata":
            return DatasetSchema(tables=())
        return DatasetSchema(tables=(TableSchema(
            name="metrics.csv",
            record_type="metric",
            columns=(ColumnSchema("timestamp", "integer"), ColumnSchema("value", "number")),
            entity_roles=("service",),
        ),))

    def get_windows(self) -> tuple[Window, Window]:
        if {mode!r} == "bad_windows":
            return [Window("reference", 0, 10), Window("alert", 10, 20)]
        return Window("reference", 0, 10), Window("alert", 10, 20)

    def extract_suspect_entities(self) -> list[EntityRef]:
        return [EntityRef("service", "svc-a")]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        with open(self.config["csv_path"], "r", encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                timestamp = int(row["timestamp"])
                if window.start_ms_utc <= timestamp < window.end_ms_utc:
                    yield MetricRecord(
                        "metrics.csv", timestamp, EntityRef("service", row["service"]),
                        row["metric_name"], float(row["value"]),
                    )

    def __new__(cls, config: Mapping[str, object]):
        if {mode!r} == "bad_factory":
            return object()
        return super().__new__(cls)

def create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter:
    return DatasetAdapter(config)
'''
    return {
        "status": "ready",
        "source": source,
        "config_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"csv_path": {"type": "string", "minLength": 1}},
            "required": ["csv_path"],
            "additionalProperties": False,
        },
    }


class AdapterSubmissionIntegrationTests(unittest.TestCase):
    def test_json_submission_config_and_csv_factory_run_without_model_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            csv_path = root / "metrics.csv"
            csv_path.write_text(
                "timestamp,service,metric_name,value\n"
                "5,svc-a,cpu,1.0\n"
                "15,svc-a,cpu,2.0\n",
                encoding="utf-8",
            )
            marker = root / "factory-ran.txt"
            response = _ready_submission(marker)
            parsed = parse_adapter_submission(json.dumps(response))
            self.assertEqual(parsed["status"], "ready")
            config = validate_adapter_config(response["config_schema"], {"csv_path": str(csv_path)})
            self.assertEqual(config, {"csv_path": str(csv_path)})

            adapter = load_adapter_submission(response, config)
            self.assertIsInstance(adapter, BaseDatasetAdapter)
            self.assertTrue(marker.is_file())
            ref_w, alt_w = adapter.get_windows()
            self.assertEqual(len(list(adapter.stream_records("metrics.csv", ref_w))), 1)
            self.assertEqual(len(list(adapter.stream_records("metrics.csv", alt_w))), 1)
            result = check_adapter_conformance(adapter)
            self.assertEqual(result["table_window_counts"], {"metrics.csv": {"reference": 1, "alert": 1}})

    def test_needs_input_and_invalid_config_do_not_execute_submitted_module(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "factory-ran.txt"
            response = _ready_submission(marker)
            with self.assertRaises(AdapterSubmissionError) as invalid_config:
                load_adapter_submission(response, {})
            self.assertEqual(invalid_config.exception.stage, "config")
            self.assertFalse(marker.exists())

            needs_input = {
                "status": "needs_input",
                "required_fields": [{"name": "timezone", "type": "string", "reason": "Timestamp origin is unknown."}],
            }
            with self.assertRaises(AdapterInputRequired) as required:
                load_adapter_submission(needs_input, {})
            self.assertEqual(required.exception.stage, "needs_input")
            self.assertEqual(required.exception.required_fields[0]["name"], "timezone")
            self.assertFalse(marker.exists())

    def test_factory_and_conformance_failures_are_structured(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "factory-ran.txt"
            response = _ready_submission(marker, mode="bad_factory")
            with self.assertRaises(AdapterSubmissionError) as factory_error:
                load_adapter_submission(response, {"csv_path": str(Path(directory) / "unused.csv")})
            self.assertEqual(factory_error.exception.stage, "factory")

            bad_schema = _ready_submission(marker, mode="bad_schema")
            with self.assertRaises(AdapterSubmissionError) as conformance_error:
                load_adapter_submission(bad_schema, {"csv_path": str(Path(directory) / "unused.csv")})
            self.assertEqual(conformance_error.exception.stage, "conformance")

            bad_windows = _ready_submission(marker, mode="bad_windows")
            with self.assertRaises(AdapterSubmissionError) as windows_error:
                load_adapter_submission(bad_windows, {"csv_path": str(Path(directory) / "unused.csv")})
            self.assertEqual(windows_error.exception.stage, "conformance")

            missing_metadata = _ready_submission(marker, mode="missing_metadata")
            with self.assertRaises(AdapterSubmissionError) as metadata_error:
                load_adapter_submission(missing_metadata, {"csv_path": str(Path(directory) / "unused.csv")})
            self.assertEqual(metadata_error.exception.stage, "conformance")


if __name__ == "__main__":
    unittest.main()
