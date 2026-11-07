"""Persistence and engine-selection contracts for paper and clean-slate modes."""

from __future__ import annotations

import json
import tempfile
import unittest
from types import MappingProxyType
from pathlib import Path
from unittest.mock import patch

from e4.controller.library import LibraryManager, Playbook
from e4.engine import DiagnosisEngine
from e4.onboarding import (
    normalize_dataset_schema,
    paper_catalog,
    paper_reference_examples,
    prepare_library,
)
from e4.registry import OperatorSpec
from e4.types import ColumnSchema, DatasetSchema, DiagnosticResult, TableSchema


def _schema() -> DatasetSchema:
    return DatasetSchema(
        tables=(
            TableSchema(
                "metrics.csv",
                "metric",
                (ColumnSchema("timestamp", "integer"), ColumnSchema("value", "number", True, "ms")),
                entity_roles=("service",),
                description="Metric observations",
            ),
        ),
        time_unit="ms_utc",
        description="Test schema",
    )


def _paper_schema(table_name: str = "metrics.csv") -> DatasetSchema:
    return DatasetSchema(
        tables=(
            TableSchema(
                table_name,
                "metric",
                (
                    ColumnSchema("timestamp", "integer"),
                    ColumnSchema("value", "number"),
                    ColumnSchema("metric_name", "string"),
                ),
                entity_roles=("service",),
            ),
        )
    )


class LibraryModeTests(unittest.TestCase):
    def test_schema_mapping_roundtrip_preserves_declared_contract(self) -> None:
        original = _schema()
        normalized = normalize_dataset_schema(original.as_dict())
        self.assertEqual(normalized, original)
        with self.assertRaisesRegex(ValueError, "record_type"):
            normalize_dataset_schema({"tables": [{"name": "metrics.csv", "columns": []}]})
        with self.assertRaisesRegex(TypeError, "entity_roles"):
            normalize_dataset_schema({
                "tables": [{
                    "name": "metrics.csv",
                    "record_type": "metric",
                    "columns": [],
                    "entity_roles": "service",
                }]
            })

    def test_frozen_playbook_dag_and_metadata_are_json_serializable(self) -> None:
        book = Playbook(
            name="frozen.book",
            description="nested immutable payload",
            dag=MappingProxyType({
                "nodes": (
                    MappingProxyType({
                        "id": "node",
                        "operator": "test.op",
                        "params": MappingProxyType({"nested": MappingProxyType({"limit": 3})}),
                    }),
                ),
                "return_node": "node",
            }),
            metadata=MappingProxyType({"origin": MappingProxyType({"kind": "fixture"})}),
        )
        library = LibraryManager(playbooks=(book,))

        payload = book.as_dict()
        json.dumps(payload)
        self.assertEqual(payload["dag"]["nodes"][0]["params"]["nested"]["limit"], 3)
        self.assertEqual(payload["metadata"]["origin"]["kind"], "fixture")
        json.dumps(library.as_dict())

    def test_clean_slate_persists_without_a_paper_base_or_implicit_merge(self) -> None:
        library = prepare_library(_schema(), mode="clean_slate")
        examples = paper_reference_examples()
        self.assertEqual(library.mode, "clean_slate")
        self.assertEqual(library.registry.all_operators(), [])
        self.assertEqual(library.list_playbooks(), [])
        self.assertTrue(examples)
        self.assertTrue(all(example["reference_only"] for example in examples))
        self.assertTrue(all(example["active_registration"] is False for example in examples))
        self.assertEqual(library.registry.all_operators(), [])

        with tempfile.TemporaryDirectory() as directory:
            library.save_to_dir(directory)
            restored = LibraryManager.load_from_dir(directory)
            self.assertEqual(restored.mode, "clean_slate")
            self.assertEqual(restored.origin, "user-clean-slate")
            self.assertEqual(restored.registry.all_operators(), [])
            self.assertEqual(restored.list_playbooks(), [])
            self.assertEqual(restored.manifest["mode"], "clean_slate")
            self.assertEqual(restored.as_dict()["operators"], [])
            self.assertEqual(restored.as_dict()["playbooks"], [])

    def test_clean_slate_diagnosis_stops_before_any_model_call_until_prepared(self) -> None:
        calls = []
        engine = DiagnosisEngine(
            model=lambda prompt: calls.append(prompt) or "{}",
            library_mode="clean_slate",
        )

        class EmptyTelemetry:
            def get_schema(self):
                return _schema()

        with self.assertRaisesRegex(ValueError, "generate and validate initial operators and playbooks"):
            engine.diagnose(EmptyTelemetry())
        self.assertEqual(calls, [])

    def test_default_paper_selection_is_delayed_and_library_is_retained(self) -> None:
        selected = LibraryManager(mode="paper", origin="shipped-paper")
        engine = DiagnosisEngine(model=lambda prompt: "{}")
        self.assertTrue(engine._pending_default_library)
        self.assertEqual(engine.library.mode, "paper")
        result = DiagnosticResult("case", "unverified", "select", ())

        class EmptyTelemetry:
            def get_schema(self):
                return _schema()

        with patch("e4.engine.prepare_library", return_value=selected) as prepare, patch.object(
            engine.controller, "run_diagnostic", return_value=result
        ):
            self.assertIs(engine.diagnose(EmptyTelemetry()), result)
            self.assertIs(engine.library, selected)
            self.assertIs(engine.controller.library, selected)
            self.assertFalse(engine._pending_default_library)
            self.assertIs(engine.diagnose(EmptyTelemetry()), result)
        prepare.assert_called_once_with(_schema(), mode="paper")

    def test_explicit_custom_library_is_preserved_without_default_replacement(self) -> None:
        custom = LibraryManager()
        engine = DiagnosisEngine(model=lambda prompt: "{}", library=custom)
        self.assertIs(engine.library, custom)
        self.assertEqual(engine.library.mode, "custom")
        self.assertFalse(engine._pending_default_library)

    def test_conflicting_mode_and_explicit_library_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "conflicts with supplied library mode"):
            prepare_library(_schema(), mode="clean_slate", library=LibraryManager())

        with self.assertRaisesRegex(ValueError, "mode must be 'paper' or 'clean_slate'"):
            prepare_library(_schema(), mode="custom")

    def test_paper_defaults_filter_and_rebind_per_schema_without_dropping_operators(self) -> None:
        first_schema = _paper_schema("metrics_a.csv")
        library = prepare_library(first_schema, mode="paper")
        learned = OperatorSpec(
            name="evolved.learned_metric",
            version="1",
            inputs=(),
            output_type="Aggregate",
        )
        library.registry.register(learned)
        with tempfile.TemporaryDirectory() as directory:
            library.save_to_dir(directory)
            library = LibraryManager.load_from_dir(directory)
        initial_registry_names = {op.name for op in library.registry.all_operators()}
        self.assertIn("paper.backend.metric_conditions", initial_registry_names)
        self.assertIn("evolved.learned_metric", initial_registry_names)
        implemented_paper_names = {
            item["name"] for item in paper_catalog()["operators"]
            if item.get("implementation", {}).get("status") == "implemented_generic"
        }
        self.assertTrue(all(
            op.implementation is not None
            for op in library.registry.all_operators()
            if op.name in implemented_paper_names
        ))

        trace_schema = DatasetSchema((
            TableSchema(
                "spans_b.csv",
                "span",
                (
                    ColumnSchema("trace_id", "string"),
                    ColumnSchema("span_id", "string"),
                    ColumnSchema("parent_id", "string"),
                ),
                entity_roles=("service",),
            ),
        ))
        second_metric_table = TableSchema(
            "metrics_b.csv",
            "metric",
            (
                ColumnSchema("ts_utc", "integer"),
                ColumnSchema("reading", "number"),
                ColumnSchema("signal_kind", "string"),
            ),
            entity_roles=("service",),
        )
        second_case_schema = DatasetSchema(
            tables=(second_metric_table,) + trace_schema.tables
        )
        rebound_books = library.list_playbooks_for_schema(second_case_schema)
        self.assertEqual(len(rebound_books), 4)
        self.assertTrue(all(
            node.get("params", {}).get("source_table") == "metrics_b.csv"
            for book in rebound_books
            for node in book.dag["nodes"]
            if node.get("operator") == "paper.backend.metric_conditions"
        ))
        dependency_book = next(book for book in rebound_books if book.name == "dependency_adjusted_backend_consensus")
        self.assertTrue(any(
            node.get("params", {}).get("trace_table") == "spans_b.csv"
            for node in dependency_book.dag["nodes"]
            if node.get("operator") == "paper.scores.dependency_adjust"
        ))

        third_case_schema = DatasetSchema(tables=(
            TableSchema(
                "metrics_c.csv",
                "metric",
                (ColumnSchema("time_ms", "integer"), ColumnSchema("amount", "number")),
                entity_roles=("service",),
            ),
        ))
        third_case_books = library.list_playbooks_for_schema(third_case_schema)
        self.assertEqual(len(third_case_books), 3)
        self.assertNotIn("dependency_adjusted_backend_consensus", {book.name for book in third_case_books})
        self.assertIn("paper.scores.dependency_adjust", {op.name for op in library.registry.all_operators()})
        self.assertIn("evolved.learned_metric", {op.name for op in library.registry.all_operators()})

        incompatible_schema = DatasetSchema((
            TableSchema(
                "logs_only.csv",
                "metric",
                (ColumnSchema("timestamp", "integer"), ColumnSchema("value", "number")),
                entity_roles=("application",),
            ),
        ))
        self.assertEqual(library.list_playbooks_for_schema(incompatible_schema), [])
        self.assertTrue(initial_registry_names.issubset({op.name for op in library.registry.all_operators()}))
        self.assertIn("evolved.learned_metric", {op.name for op in library.registry.all_operators()})
        active_view = library.as_dict(incompatible_schema)
        json.dumps(active_view)
        self.assertEqual(active_view["playbooks"], [])
        self.assertNotIn("source", active_view["operators"][0])

    def test_paper_library_reload_reattaches_native_implementations(self) -> None:
        schema = _paper_schema()
        library = prepare_library(schema, mode="paper")
        with tempfile.TemporaryDirectory() as directory:
            library.save_to_dir(directory)
            restored = LibraryManager.load_from_dir(directory)

        self.assertEqual(restored.mode, "paper")
        self.assertTrue(restored.registry.all_operators())
        self.assertTrue(all(op.implementation is not None for op in restored.registry.all_operators()))
        signature_names = {item["name"] for item in restored.signatures_for_schema(schema)}
        self.assertIn("paper.backend.metric_conditions", signature_names)
        self.assertNotIn("paper.scores.dependency_adjust", signature_names)

        candidate_registry = restored.registry.clone()
        candidate_registry.register(OperatorSpec(
            name="evolved.candidate_probe",
            version="1",
            inputs=(),
            output_type="Aggregate",
            source="def operator(inputs, params, runtime):\n    return make_aggregate(runtime.alert_window, [])\n",
        ))
        candidate_names = {
            item["name"]
            for item in restored.signatures_for_schema(schema, registry=candidate_registry)
        }
        self.assertIn("evolved.candidate_probe", candidate_names)
        self.assertNotIn("paper.scores.dependency_adjust", candidate_names)
        self.assertEqual(
            {book.name for book in restored.list_playbooks_for_schema(schema)},
            {book.name for book in library.list_playbooks()},
        )

    def test_clean_slate_rejects_canonical_paper_contamination(self) -> None:
        paper = prepare_library(_paper_schema(), mode="paper")
        contaminated = LibraryManager(
            registry=paper.registry.clone(),
            playbooks=paper.list_playbooks(),
            mode="clean_slate",
            origin="user-clean-slate",
        )
        with self.assertRaisesRegex(ValueError, "cannot load a paper library"):
            prepare_library(_paper_schema(), mode="clean_slate", library=contaminated)

        engine_library = LibraryManager(mode="clean_slate", origin="user-clean-slate")
        with self.assertRaisesRegex(ValueError, "cannot load a paper library"):
            DiagnosisEngine(model=lambda prompt: "{}", library=engine_library, base_registry=paper.registry)
        self.assertEqual(engine_library.registry.all_operators(), [])

        with tempfile.TemporaryDirectory() as clean_directory:
            LibraryManager(mode="clean_slate", origin="user-clean-slate").save_to_dir(clean_directory)
            with self.assertRaisesRegex(ValueError, "cannot merge shipped paper operators"):
                LibraryManager.load_from_dir(clean_directory, base_registry=paper.registry)

        with tempfile.TemporaryDirectory() as directory:
            contaminated.save_to_dir(directory)
            with self.assertRaisesRegex(ValueError, "contains shipped paper operators"):
                LibraryManager.load_from_dir(directory)


if __name__ == "__main__":
    unittest.main()
