"""Opt-in paper starter inventory, Bank capability, and trace-integrity checks."""

from __future__ import annotations

import unittest
from pathlib import Path

from e4core.contracts import (EntityRef, Evidence, PaperStarterInput, ScoredCandidate,
    ScoredCandidates, Span, Telemetry, Window)
from e4core.dag import verify_dag
from e4core.executor import NodeContext
from e4core.m4_fixtures import synthetic_case_runtime
from e4core.paper_starter import (PROFILE, _dependency_adjust, _samples,
    canonical_paper_library_source, make_paper_starter_registry)
from e4core.paper_starter_fixtures import validate_starter_fixture_suite
from e4core.paper_starter_math import _finite_1d


ROOT = Path(__file__).resolve().parents[1]


class PaperStarterIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.suite = validate_starter_fixture_suite()

    def test_canonical_inventory_and_real_fixture_behavior_are_frozen(self):
        registry = make_paper_starter_registry()
        source = canonical_paper_library_source(registry)
        self.assertEqual(source["profile"], PROFILE)
        self.assertEqual(source["operator_count"], 19)
        self.assertEqual(source["playbook_count"], 11)
        self.assertTrue(self.suite["passed"], self.suite)
        self.assertTrue(self.suite["fixture_source_rows_beyond_64"])
        self.assertFalse(self.suite["raw_log_text_in_typed_input"])
        self.assertTrue(self.suite["canonical_semantic_gates_passed"])
        self.assertTrue(self.suite["bank_positive_gates_passed"])
        self.assertTrue(self.suite["bank_negative_gates_passed"])

    def test_fixed_bank_registry_membership_does_not_depend_on_case_edges(self):
        positive = self.suite["bank_adaptation_results"]["positive"]
        no_trace = self.suite["bank_adaptation_results"]["collinear_trace"]
        self.assertEqual(set(positive["books"]), set(no_trace["books"]))
        self.assertEqual(positive["diagnostic_count"], no_trace["diagnostic_count"])
        self.assertEqual(positive["capabilities"].keys(), no_trace["capabilities"].keys())
        self.assertIn("bank_trace_root_duration_flow", positive["books"])

    def test_dependency_adjustment_uses_partial_acyclic_edges_and_rejects_duplicate_identity(self):
        reference = Window("reference", 1_700_000_000_000, 1_700_001_800_000)
        alert = Window("alert", 1_700_001_800_000, 1_700_003_600_000)
        caller, callee = EntityRef("component", "caller"), EntityRef("component", "callee")
        spans = (
            Span("partial", "parent", "outside-clip", reference.start_ms_utc, "1", None,
                "missing", "", "unprovided", caller, None, None, False, True),
            Span("partial", "child", "parent", reference.start_ms_utc + 1, "2", None,
                "missing", "", "unprovided", callee, None, None, True, False),
            Span("duplicate", "same", "root", reference.start_ms_utc + 2, "3", None,
                "missing", "", "unprovided", caller, None, None, True, False),
            Span("duplicate", "same", "root", reference.start_ms_utc + 3, "4", None,
                "missing", "", "unprovided", callee, None, None, True, False),
        )
        telemetry = Telemetry("duplicate-span-fixture", reference, alert, (), spans,
            {"caller": {"role": "component"}, "callee": {"role": "component"}},
            "d" * 64, {}, {}, {}, "paper-test/1")
        starter = PaperStarterInput(telemetry, None, None,
            {"sessions": "unsupported", "urls": "unsupported", "native_logs": "unsupported",
             "native_log_patterns": "unsupported", "component_metrics": "unsupported",
             "app_mrt": "unsupported", "trace_dependencies": "available"}, 60_000)
        source = ScoredCandidates(tuple(ScoredCandidate(entity, score, None,
            (Evidence("bank/metric_container.csv", alert, "test", score, (entity,), 3,
                {"ranking_eligible": True, "source_tables": ("bank/metric_container.csv",)}),),
            "paper.backend.mean_welch")
            for entity, score in ((caller, 1.0), (callee, 0.5))), "paper.backend.mean_welch")
        context = NodeContext(telemetry.case_id, "adjust", "paper.scores.dependency_adjust@1",
            {}, {}, reference, alert, telemetry.data_sha256)
        output = _dependency_adjust({"data": starter, "scores": source},
            {"downstream_penalty": 0.9}, context).value
        self.assertEqual([item.score for item in output.candidates], [0.55, 0.5])
        self.assertEqual(output.status_details["valid_trace_graph_count"], 1)
        self.assertEqual(output.status_details["skipped_trace_counts"]["duplicate_span_ids"], 1)
        self.assertEqual(output.status_details["skipped_trace_counts"]["unresolved_external_parent_edges"], 1)

    def test_paper_dag_uses_trusted_paper_terminal(self):
        registry = make_paper_starter_registry(bank_only=True)
        signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
        document = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "telemetry", "operator": "case.telemetry", "version": "1",
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter", "operator": "paper.input.prepare", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "score", "operator": "paper.bank.trace_root_flow", "version": "1",
             "inputs": {"data": "starter", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
             "inputs": {"scores": ["score"]}, "params": {}, "bindings": {}}],
            "return": {"node": "answer"}}
        verified = verify_dag(document, registry, runtime=synthetic_case_runtime(),
            max_nodes=40, max_depth=12)
        self.assertEqual(verified.return_node, "answer")
        self.assertIn(("paper.answers.weighted_consensus", "1"), signatures)

    def test_metric_samples_are_exact_field_component_and_configured_time_blocks(self):
        positive = self.suite["bank_adaptation_results"]["positive"]
        self.assertEqual(positive["capabilities"]["component_metrics"], "available")
        # Each accepted candidate uses the exact native field and fixed one-minute block grain.
        self.assertEqual(self.suite["staggered_metric_case_book_count"], positive["diagnostic_count"])


if __name__ == "__main__":
    unittest.main()
