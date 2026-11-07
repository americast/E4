from __future__ import annotations

import json
import sys
import unittest

from e4.data import (
    AdapterInputRequired,
    AdapterSubmissionError,
    adapter_code_template,
    adapter_submission_schema,
    load_adapter_submission,
    parse_adapter_submission,
    validate_adapter_config,
)
from e4.data.adapter import BaseDatasetAdapter, check_adapter_conformance


CONFIG_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "dataset": {
            "type": "object",
            "properties": {"service_name": {"type": "string", "minLength": 1}},
            "required": ["service_name"],
            "additionalProperties": False,
        }
    },
    "required": ["dataset"],
    "additionalProperties": False,
}
CONFIG = {"dataset": {"service_name": "checkout"}}

VALID_SOURCE = '''from __future__ import annotations

from collections.abc import Iterator, Mapping

from e4.data.adapter import BaseDatasetAdapter
from e4.types import ColumnSchema, DatasetSchema, EntityRef, MetricRecord, TableSchema, TelemetryRecord, Window


class DatasetAdapter(BaseDatasetAdapter):
    def __init__(self, config: Mapping[str, object]) -> None:
        self._config = dict(config)

    def get_schema(self) -> DatasetSchema:
        return DatasetSchema((TableSchema(
            "metrics.csv", "metric",
            (ColumnSchema("timestamp", "timestamp"), ColumnSchema("service", "string"),
             ColumnSchema("metric", "string"), ColumnSchema("value", "number", unit="ms")),
            entity_roles=("service",),
        ),))

    def get_windows(self) -> tuple[Window, Window]:
        return Window("reference", 0, 1000), Window("alert", 1000, 2000)

    def extract_suspect_entities(self) -> list[EntityRef]:
        dataset_config = self._config["dataset"]
        service_name = dataset_config["service_name"]
        if not isinstance(service_name, str):
            return []
        return [EntityRef("service", service_name)]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        if table_name != "metrics.csv":
            return
        dataset_config = self._config["dataset"]
        service_name = dataset_config["service_name"]
        if isinstance(service_name, str):
            yield MetricRecord(table_name, window.start_ms_utc + 1,
                               EntityRef("service", service_name), "latency", 2.0, unit="ms")


def create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter:
    return DatasetAdapter(config)
'''


def _ready(source: str = VALID_SOURCE, config_schema: dict | None = None) -> dict:
    return {"status": "ready", "source": source, "config_schema": config_schema or CONFIG_SCHEMA}


class AdapterSubmissionTests(unittest.TestCase):
    def test_resources_and_public_contract_are_reviewable(self) -> None:
        schema = adapter_submission_schema()
        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertEqual(len(schema["oneOf"]), 2)
        template = adapter_code_template()
        for required in (
            "class DatasetAdapter(BaseDatasetAdapter)",
            "from pathlib import Path",
            "    ColumnSchema,",
            "    MetricRecord,",
            "    SpanRecord,",
            "    LogRecord,",
            "def get_schema(self) -> DatasetSchema",
            "def get_windows(self) -> tuple[Window, Window]",
            "def extract_suspect_entities(self) -> list[EntityRef]",
            "def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]",
            "def create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter",
        ):
            self.assertIn(required, template)
        json.dumps(schema)

    def test_exact_ready_and_needs_input_envelopes_parse(self) -> None:
        ready = _ready()
        self.assertEqual(parse_adapter_submission(json.dumps(ready)), ready)
        self.assertEqual(parse_adapter_submission(ready), ready)
        waiting = {
            "status": "needs_input",
            "required_fields": [{"name": "timezone", "type": "string", "reason": "timestamp offset is unspecified"}],
        }
        self.assertEqual(parse_adapter_submission(json.dumps(waiting)), waiting)
        with self.assertRaises(AdapterInputRequired) as caught:
            load_adapter_submission(waiting, CONFIG)
        self.assertEqual(caught.exception.stage, "needs_input")
        self.assertEqual(caught.exception.required_fields, waiting["required_fields"])

    def test_strict_json_envelope_rejects_prose_duplicates_nonfinite_and_extras(self) -> None:
        invalid = (
            "Here is the result: {}",
            json.dumps(_ready()) + " trailing",
            '{"status":"ready","status":"needs_input","source":"x","config_schema":{}}',
            '{"status":"ready","source":"x","config_schema":{"n":NaN}}',
        )
        for response in invalid:
            with self.subTest(response=response[:35]), self.assertRaises(AdapterSubmissionError) as caught:
                parse_adapter_submission(response)
            self.assertEqual(caught.exception.stage, "envelope")
        with self.assertRaises(AdapterSubmissionError):
            parse_adapter_submission({**_ready(), "commentary": "extra field"})
        with self.assertRaises(AdapterSubmissionError):
            parse_adapter_submission({"status": "needs_input", "required_fields": [
                {"name": "path", "type": "string"}, {"name": "path", "type": "string"},
            ]})
        with self.assertRaises(AdapterSubmissionError):
            parse_adapter_submission({"status": "needs_input", "required_fields": [{"name": "path", "type": "Path"}]})

    def test_nested_config_validation_and_no_ref_schema_policy(self) -> None:
        self.assertEqual(validate_adapter_config(CONFIG_SCHEMA, CONFIG), CONFIG)
        with self.assertRaises(AdapterSubmissionError) as caught:
            validate_adapter_config(CONFIG_SCHEMA, {"dataset": {}})
        self.assertEqual(caught.exception.stage, "config")
        self.assertEqual(caught.exception.details["path"], ["dataset"])
        for schema in (
            {"type": "string", "properties": {}, "required": [], "additionalProperties": False},
            {"type": "object", "properties": {}, "required": []},
            {"type": "object", "properties": {"x": {"$ref": "https://example.invalid/schema"}}, "required": [], "additionalProperties": False},
            {"type": "object", "properties": {"x": {"type": "nonsense"}}, "required": [], "additionalProperties": False},
        ):
            with self.subTest(schema=schema), self.assertRaises(AdapterSubmissionError) as caught:
                validate_adapter_config(schema, {})
            self.assertEqual(caught.exception.stage, "config_schema")

    def test_invalid_config_and_needs_input_never_execute_source(self) -> None:
        source = "raise RuntimeError('source executed before config validation')\n" + VALID_SOURCE
        with self.assertRaises(AdapterSubmissionError) as caught:
            load_adapter_submission(_ready(source), {"dataset": {}})
        self.assertEqual(caught.exception.stage, "config")
        with self.assertRaises(AdapterInputRequired):
            load_adapter_submission(
                {"status": "needs_input", "required_fields": [{"name": "timezone", "type": "string"}]},
                CONFIG,
            )

    def test_static_source_contract_rejects_placeholders_bad_methods_and_factory(self) -> None:
        method_start = VALID_SOURCE.index("    def extract_suspect_entities")
        method_end = VALID_SOURCE.index("    def stream_records", method_start)
        direct_stub = (
            VALID_SOURCE[:method_start]
            + "    def extract_suspect_entities(self) -> list[EntityRef]:\n"
            + "        pass\n\n"
            + VALID_SOURCE[method_end:]
        )
        cases = (
            adapter_code_template(),
            VALID_SOURCE.replace("def get_schema(self) -> DatasetSchema:", "def get_schema(self):"),
            VALID_SOURCE.replace("return DatasetAdapter(config)", "return config"),
            direct_stub,
            VALID_SOURCE.replace("def get_windows(self) -> tuple[Window, Window]:", "async def get_windows(self) -> tuple[Window, Window]:"),
        )
        for source in cases:
            with self.subTest(source=source[:45]), self.assertRaises(AdapterSubmissionError) as caught:
                load_adapter_submission(_ready(source), CONFIG)
            self.assertEqual(caught.exception.stage, "source")

    def test_valid_source_loads_and_passes_adapter_conformance(self) -> None:
        adapter = load_adapter_submission(json.dumps(_ready()), CONFIG)
        self.assertIsInstance(adapter, BaseDatasetAdapter)
        report = check_adapter_conformance(adapter)
        self.assertEqual(report["status"], "conforming")
        self.assertEqual(report["table_count"], 1)
        self.assertEqual(report["suspect_entities_count"], 1)
        self.assertEqual(adapter.get_schema().tables[0].name, "metrics.csv")
        self.assertEqual(adapter.get_windows()[1].name, "alert")

    def test_factory_import_and_conformance_failures_have_stages_and_clean_modules(self) -> None:
        before = {name for name in sys.modules if name.startswith("_e4_submitted_adapter_")}
        missing_import = VALID_SOURCE.replace(
            "from e4.data.adapter import BaseDatasetAdapter",
            "from e4.data.adapter import BaseDatasetAdapter\nimport e4_missing_submission_dependency",
        )
        with self.assertRaises(AdapterSubmissionError) as caught:
            load_adapter_submission(_ready(missing_import), CONFIG)
        self.assertEqual(caught.exception.stage, "source")

        broken_factory = VALID_SOURCE.replace(
            "self._config = dict(config)",
            "raise ValueError('factory construction failed')",
        )
        with self.assertRaises(AdapterSubmissionError) as caught:
            load_adapter_submission(_ready(broken_factory), CONFIG)
        self.assertEqual(caught.exception.stage, "factory")

        invalid_schema = VALID_SOURCE.replace(
            'return DatasetSchema((TableSchema(',
            'return DatasetSchema(())  # invalid adapter output\n        # removed table details\n        return DatasetSchema((TableSchema(',
        )
        with self.assertRaises(AdapterSubmissionError) as caught:
            load_adapter_submission(_ready(invalid_schema), CONFIG)
        self.assertEqual(caught.exception.stage, "conformance")
        after = {name for name in sys.modules if name.startswith("_e4_submitted_adapter_")}
        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
