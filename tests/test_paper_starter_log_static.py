"""Static multiplicity/resolution checks for paper Bank log-pattern tests."""

from __future__ import annotations

import copy
import unittest

from e4core.dag import DAGValidationError, verify_dag
from e4core.paper_starter import make_paper_starter_registry
from e4core.runtime import RuntimeContext, RuntimeField, RuntimeSchema


class PaperStarterLogStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = make_paper_starter_registry(bank_only=True)
        cls.document = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "telemetry", "operator": "case.telemetry", "version": "1",
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter", "operator": "paper.input.prepare", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "score", "operator": "paper.bank.log_pattern_contrast", "version": "1",
             "inputs": {"data": "starter", "candidates": "candidates"},
             "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
             "inputs": {"scores": ["score"]}, "params": {}, "bindings": {}}],
            "return": {"node": "answer"}}

    def test_invalid_monte_carlo_floor_is_rejected_before_execution(self):
        bad = copy.deepcopy(self.document)
        bad["nodes"][3]["params"] = {"alpha": 0.05,
            "maximum_patterns": 64, "resamples": 999}
        with self.assertRaises(DAGValidationError) as raised:
            verify_dag(bad, self.registry, max_nodes=40, max_depth=12)
        self.assertIn("resamples must be at least", str(raised.exception))

    def test_valid_resolution_and_all_three_runtime_bound_values_are_checked(self):
        valid = copy.deepcopy(self.document)
        valid["nodes"][3]["params"] = {"alpha": 0.05,
            "maximum_patterns": 64, "resamples": 1999}
        verify_dag(valid, self.registry, max_nodes=40, max_depth=12)

        bound = copy.deepcopy(self.document)
        bound["nodes"][3]["bindings"] = {
            "alpha": "runtime.pattern_alpha",
            "maximum_patterns": "runtime.pattern_count",
            "resamples": "runtime.permutation_count"}
        schema = RuntimeSchema({
            "pattern_alpha": RuntimeField("pattern_alpha", "number", "test", "run"),
            "pattern_count": RuntimeField("pattern_count", "positive_integer", "test", "run"),
            "permutation_count": RuntimeField("permutation_count", "positive_integer", "test", "run"),
        }, "paper-log-test/1")
        invalid_runtime = RuntimeContext.build(schema, {"pattern_alpha": 0.05,
            "pattern_count": 64, "permutation_count": 999})
        with self.assertRaises(DAGValidationError):
            verify_dag(bound, self.registry, runtime=invalid_runtime, max_nodes=40, max_depth=12)
        valid_runtime = RuntimeContext.build(schema, {"pattern_alpha": 0.05,
            "pattern_count": 64, "permutation_count": 1999})
        verify_dag(bound, self.registry, runtime=valid_runtime, max_nodes=40, max_depth=12)


if __name__ == "__main__":
    unittest.main()
