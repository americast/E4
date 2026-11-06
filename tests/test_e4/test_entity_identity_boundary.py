"""Actual starter execution must survive the view-to-contract boundary."""

from __future__ import annotations

import copy
import json
import unittest

from adapters.bank_library import build_bank_registry, get_bank_contrast_playbook, get_bank_seed_playbook
from e4.controller.controller import DiagnosisController, _to_ranked_answer
from e4.controller.library import LibraryManager, Playbook
from e4.dag import SerialExecutor, verify_dag
from e4.data.adapter import BaseDatasetAdapter
from e4.oracle import BudgetStopPolicy
from e4.sandbox.runner import InProcessRunner
from e4.sandbox.views import EntityRefView, TelemetryView
from e4.types import (
    ColumnSchema, DatasetSchema, EntityRef, Evidence, MetricRecord,
    RankedAnswer, SpanRecord, TableSchema, Window,
)


class PublicFixture(BaseDatasetAdapter):
    def get_schema(self):
        return DatasetSchema((
            TableSchema("bank/metric_container.csv", "metric", (
                ColumnSchema("timestamp", "timestamp"), ColumnSchema("cmdb_id", "string"),
                ColumnSchema("kpi_name", "string"), ColumnSchema("value", "number"),
            ), entity_roles=("service",)),
            TableSchema("bank/trace_span.csv", "span", (
                ColumnSchema("timestamp", "timestamp"), ColumnSchema("duration", "number"),
            ), entity_roles=("service",)),
        ))

    def get_windows(self):
        return Window("reference", 0, 10), Window("alert", 10, 20)

    def extract_suspect_entities(self):
        return [EntityRef("service", "a"), EntityRef("service", "b")]

    def stream_records(self, table_name, window):
        for entity in self.extract_suspect_entities():
            value = 20.0 if window.name == "alert" and entity.identifier == "a" else 1.0
            if table_name == "bank/metric_container.csv":
                yield MetricRecord(table_name, window.start_ms_utc + 1, entity, "cpu", value)
            elif table_name == "bank/trace_span.csv":
                yield SpanRecord(table_name, window.start_ms_utc + 1,
                                 "trace-" + entity.identifier, "span-" + entity.identifier,
                                 parent_id=None, duration_ms=value, success=None,
                                 executing_component=entity)


class EntityIdentityBoundaryTests(unittest.TestCase):
    def test_view_contributors_match_canonical_identity_and_serialize(self):
        observed = EntityRef("service", "CaseSensitiveDisplay")
        ev = Evidence("public-fixture", Window("alert", 10, 20), "mean", 2.0,
                      (EntityRefView("service", "casesensitivedisplay"),), 1, True)
        raw = RankedAnswer((observed,), (1.0,), evidence=(ev,))
        answer = _to_ranked_answer(raw, "select", [observed])
        self.assertIsNotNone(answer)
        self.assertEqual(answer.candidates, (observed,))
        self.assertIsInstance(answer.evidence[0].contributing_entities[0], EntityRef)
        json.dumps(answer.as_dict())

    def test_normalization_does_not_bypass_eligibility_support_or_identity(self):
        observed = EntityRef("service", "a")
        for contributor, support, eligible in (
            (EntityRefView("service", "a"), 0, True),
            (EntityRefView("service", "a"), 1, False),
            (EntityRefView("app_kpi", "a"), 1, True),
            (EntityRefView("service", "unobserved"), 1, True),
        ):
            with self.subTest(contributor=contributor, support=support, eligible=eligible):
                ev = Evidence("public-fixture", Window("alert", 10, 20), "mean", 2.0,
                              (contributor,), support, eligible)
                raw = RankedAnswer((observed,), (1.0,), evidence=(ev,))
                self.assertIsNone(_to_ranked_answer(raw, "select", [observed]))

    def test_actual_metric_and_trace_starters_produce_canonical_supported_answers(self):
        adapter = PublicFixture()
        registry = build_bank_registry()
        trace = copy.deepcopy(get_bank_contrast_playbook().as_dict()["dag"])
        for node in trace["nodes"]:
            node["operator"] = {
                "bank.metrics.aggregate": "bank.traces.aggregate",
                "bank.metrics.contrast": "bank.traces.latency_contrast",
            }.get(node["operator"], node["operator"])
            if node.get("params", {}).get("table"):
                node["params"]["table"] = "bank/trace_span.csv"
        books = (get_bank_seed_playbook(), get_bank_contrast_playbook(),
                 Playbook("trace-fixture", "Trace boundary fixture", trace))
        view = adapter.as_full_telemetry_view(case_id="public-fixture")
        try:
            for book in books:
                with self.subTest(playbook=book.name):
                    dag = verify_dag(book.dag, registry, dataset_schema=adapter.get_schema())
                    executed = SerialExecutor(registry).execute_verified(
                        dag, {"dag_depth": dag.max_depth},
                        external_values={"data": view, "telemetry": view},
                    )
                    raw = executed.return_value
                    self.assertTrue(raw.candidates)
                    self.assertTrue(raw.evidence)
                    self.assertTrue(all(isinstance(entity, EntityRef)
                                        for ev in raw.evidence for entity in ev.contributing_entities))
                    answer = _to_ranked_answer(raw, "select", adapter.extract_suspect_entities())
                    self.assertIsNotNone(answer)
                    self.assertEqual(answer.candidates[0].identifier, "a")
                    json.dumps(answer.as_dict())
        finally:
            view.close()

    def test_public_controller_retains_a_valid_real_starter_answer_without_labels(self):
        for book in (get_bank_seed_playbook(), get_bank_contrast_playbook()):
            with self.subTest(playbook=book.name):
                library = LibraryManager(build_bank_registry(), [book])
                model_calls = []

                def select(prompt):
                    model_calls.append(prompt)
                    return json.dumps({"selected_playbook": book.name, "parameter_bindings": {}})

                controller = DiagnosisController(library, BudgetStopPolicy(1), InProcessRunner())
                result = controller.run_diagnostic(PublicFixture(), select, "public-fixture")
                self.assertEqual(len(model_calls), 1)
                self.assertEqual(result.status, "unverified")
                self.assertEqual(result.stage, "select")
                self.assertIsNotNone(result.answer)
                self.assertEqual(result.top_candidates[0].identifier, "a")
                self.assertFalse(any(row.get("status") == "empty_answer" for row in result.history))


if __name__ == "__main__":
    unittest.main()
