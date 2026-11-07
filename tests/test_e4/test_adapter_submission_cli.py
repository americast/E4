from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import e4
from e4.cli import main
from e4.data.agent_prompt import get_adapter_generation_prompt
from e4.data.submissions import adapter_code_template, adapter_submission_schema


def _submission_source() -> str:
    return '''from collections.abc import Iterator, Mapping
from e4.data.adapter import BaseDatasetAdapter
from e4.types import DatasetSchema, EntityRef, TelemetryRecord, Window

class DatasetAdapter(BaseDatasetAdapter):
    def __init__(self, config: Mapping[str, object]) -> None:
        self.config = dict(config)

    def get_schema(self) -> DatasetSchema:
        print("ADAPTER_SOURCE_EXECUTED")
        return DatasetSchema(tables=())

    def get_windows(self) -> tuple[Window, Window]:
        return Window("reference", 0, 1), Window("alert", 1, 2)

    def extract_suspect_entities(self) -> list[EntityRef]:
        return [EntityRef("service", "example")]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        return iter(())

def create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter:
    return DatasetAdapter(config)
'''


class AdapterSubmissionPromptTests(unittest.TestCase):
    def test_submission_api_is_exported_from_package_root(self):
        for name in (
            "adapter_submission_schema",
            "adapter_code_template",
            "parse_adapter_submission",
            "validate_adapter_config",
            "load_adapter_submission",
        ):
            self.assertTrue(callable(getattr(e4, name)))
        self.assertTrue(issubclass(e4.AdapterSubmissionError, Exception))
        self.assertTrue(issubclass(e4.AdapterInputRequired, e4.AdapterSubmissionError))

    def test_adapter_prompt_embeds_authoritative_schema_and_template(self):
        response_schema = adapter_submission_schema()
        template = adapter_code_template()
        prompt = get_adapter_generation_prompt(
            "timestamp,value\n...",
            domain_brief="Rank documented hosts.",
            public_metadata_text="timestamp_unit=seconds",
        )
        self.assertIn(json.dumps(response_schema, separators=(",", ":")), prompt)
        self.assertIn(template, prompt)
        self.assertIn("status", response_schema["oneOf"][0]["properties"])
        self.assertIn("needs_input", prompt)
        self.assertIn("Rank documented hosts.", prompt)
        self.assertIn("timestamp_unit=seconds", prompt)
        self.assertIn("Return exactly one JSON object", prompt)
        for canonical_type in (
            "DatasetSchema", "TableSchema", "ColumnSchema", "Window", "EntityRef",
            "MetricRecord", "SpanRecord", "LogRecord",
        ):
            self.assertIn(canonical_type + "(", prompt)
        self.assertIn("MetricRecord(source_table: 'str', timestamp_ms_utc: 'int'", prompt)
        self.assertIn("SpanRecord(source_table: 'str', timestamp_ms_utc: 'int'", prompt)
        self.assertIn("LogRecord(source_table: 'str', timestamp_ms_utc: 'int'", prompt)
        self.assertIn("Window(name:", prompt)
        self.assertIn("start_ms_utc: 'int'", prompt)
        self.assertNotIn("```json", prompt)

    def test_adapter_template_cli_prints_python_scaffold(self):
        with patch.object(sys, "argv", ["e4", "adapter-template"]):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                main()
        self.assertEqual(output.getvalue().strip(), adapter_code_template().strip())
        self.assertIn("class DatasetAdapter(BaseDatasetAdapter)", output.getvalue())

    def test_adapter_validate_accepts_ready_json_without_executing_source(self):
        with tempfile.TemporaryDirectory() as directory:
            response_path = Path(directory) / "adapter.json"
            response_path.write_text(json.dumps({
                "status": "ready",
                "source": _submission_source(),
                "config_schema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False,
                },
            }), encoding="utf-8")
            with patch.object(sys, "argv", ["e4", "adapter-validate", str(response_path)]), patch(
                "e4.data.submissions.load_adapter_submission"
            ) as load_submission:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    main()
            load_submission.assert_not_called()
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["summary"]["source_validated"])
        self.assertNotIn("ADAPTER_SOURCE_EXECUTED", output.getvalue())

    def test_adapter_validate_requests_missing_required_config(self):
        with tempfile.TemporaryDirectory() as directory:
            response_path = Path(directory) / "adapter.json"
            response_path.write_text(json.dumps({
                "status": "ready",
                "source": _submission_source(),
                "config_schema": {
                    "type": "object",
                    "properties": {"dataset_root": {"type": "string", "description": "Path to the dataset."}},
                    "required": ["dataset_root"],
                    "additionalProperties": False,
                },
            }), encoding="utf-8")
            with patch.object(sys, "argv", ["e4", "adapter-validate", str(response_path)]):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    main()
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "needs_input")
        self.assertEqual(result["required_fields"][0]["name"], "dataset_root")

    def test_adapter_validate_returns_needs_input_without_treating_it_as_error(self):
        with tempfile.TemporaryDirectory() as directory:
            response_path = Path(directory) / "adapter.json"
            response_path.write_text(json.dumps({
                "status": "needs_input",
                "required_fields": [{
                    "name": "timestamp_unit",
                    "type": "string",
                    "reason": "Source timestamps cannot be normalized without their unit.",
                }],
            }), encoding="utf-8")
            with patch.object(sys, "argv", ["e4", "adapter-validate", str(response_path)]):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    main()
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "needs_input")
        self.assertEqual(result["required_fields"][0]["name"], "timestamp_unit")

    def test_adapter_validate_reports_malformed_response_as_json_and_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            response_path = Path(directory) / "adapter.json"
            response_path.write_text("not json", encoding="utf-8")
            with patch.object(sys, "argv", ["e4", "adapter-validate", str(response_path)]):
                output = io.StringIO()
                with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as raised:
                    main()
        self.assertEqual(raised.exception.code, 2)
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "error")
        self.assertIn("stage", result)


if __name__ == "__main__":
    unittest.main()
