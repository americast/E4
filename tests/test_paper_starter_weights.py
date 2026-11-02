"""Static and runtime contract tests for paper consensus weight groups."""

from __future__ import annotations

import unittest

from e4core.dag import DAGValidationError, verify_dag
from e4core.m4_fixtures import synthetic_case_runtime
from e4core.paper_starter import (consensus_weight_catalog, make_paper_starter_registry,
    normalize_consensus_weights)
from e4core.runtime import RuntimeContext, RuntimeField, RuntimeSchema


class PaperStarterWeightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = make_paper_starter_registry(bank_only=True)
        cls.base = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "telemetry", "operator": "case.telemetry", "version": "1",
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter", "operator": "paper.input.prepare", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "score", "operator": "paper.backend.mean_welch", "version": "1",
             "inputs": {"data": "starter", "candidates": "candidates"},
             "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
             "inputs": {"scores": ["score"]}, "params": {}, "bindings": {}}],
            "return": {"node": "answer"}}

    def _document(self, *, weights=None, binding=None):
        import copy
        value = copy.deepcopy(self.base)
        terminal = value["nodes"][-1]
        if weights is not None:
            terminal["params"]["family_weights"] = weights
        if binding is not None:
            terminal["bindings"]["family_weights"] = binding
        return value

    def test_run02_legacy_bank_weights_are_accepted_as_one_canonical_budget(self):
        supplied = {"bank_metric": 1.0, "bank_trace_latency": 1.0, "bank_log_volume": 1.0}
        verified = verify_dag(self._document(weights=supplied), self.registry,
            runtime=synthetic_case_runtime(), max_nodes=40, max_depth=12)
        self.assertEqual(verified.return_node, "answer")
        self.assertEqual(normalize_consensus_weights(supplied), {
            "backend_metric": 1.0, "trace_latency": 1.0, "log_volume": 1.0})
        catalog = consensus_weight_catalog()
        self.assertEqual(catalog["legacy_aliases"]["bank_metric"], "backend_metric")
        self.assertEqual(catalog["legacy_aliases"]["metric"], "backend_metric")

    def test_unknown_duplicate_nonfinite_negative_and_overcap_maps_fail_static(self):
        invalid_maps = (
            {"made_up_family": 1.0},
            {"bank_metric": 1.0, "backend_metric": 1.0},
            {"metric": 1.0, "backend_metric": 1.0},
            {"backend_metric": -0.1},
            {"backend_metric": float("nan")},
            {"backend_metric": 5.01},
        )
        for weights in invalid_maps:
            with self.subTest(weights=weights), self.assertRaises(DAGValidationError):
                verify_dag(self._document(weights=weights), self.registry,
                    runtime=synthetic_case_runtime(), max_nodes=40, max_depth=12)

    def test_runtime_bound_map_is_validated_before_external_execution(self):
        runtime = RuntimeContext.build(RuntimeSchema({"weights": RuntimeField(
            "weights", "number_map", "test consensus map", "run")}, "weights-test/1"),
            {"weights": {"legacy_unknown": 1.0}})
        with self.assertRaises(DAGValidationError):
            verify_dag(self._document(binding="runtime.weights"), self.registry,
                runtime=runtime, max_nodes=40, max_depth=12)
        valid = RuntimeContext.build(RuntimeSchema({"weights": RuntimeField(
            "weights", "number_map", "test consensus map", "run")}, "weights-test/1"),
            {"weights": {"bank_metric": 1.0}})
        verified = verify_dag(self._document(binding="runtime.weights"), self.registry,
            runtime=valid, max_nodes=40, max_depth=12)
        self.assertEqual(verified.return_node, "answer")

    def test_runtime_analysis_weights_alias_map_compiles_and_runs(self):
        from pathlib import Path
        from e4core.cache import CacheStore
        from e4core.executor import SerialExecutor
        from e4core.m4_fixtures import synthetic_case_runtime
        from e4core.paper_starter_fixtures import _rich_fixture

        telemetry, starter = _rich_fixture()
        registry = make_paper_starter_registry(bank_only=True, starter_input=starter)
        # Match the existing protected runtime field shape used by Bank M4.
        from e4core.runtime import RuntimeSchema
        fields = {"analysis_weights": RuntimeField("analysis_weights", "number_map",
            "existing Bank scorer family weights", "run")}
        runtime = RuntimeContext.build(RuntimeSchema(fields, "paper-weight-runtime/1"),
            {"analysis_weights": {"metric": 1.0, "trace_latency": 1.0, "log_volume": 1.0}})
        document = self._document(binding="runtime.analysis_weights")
        # The typed runtime used for this test must supply the DAG's standard windows.
        class _WindowedRuntime:
            def __init__(self, values):
                base = synthetic_case_runtime(telemetry)
                merged = {**dict(base.snapshot.values), **dict(values.snapshot.values)}
                self.snapshot = type(base.snapshot)(merged,
                    {**dict(base.snapshot.provenance), **dict(values.snapshot.provenance)},
                    base.snapshot.schema_version)
                self.schema = type(base.schema)({**dict(base.schema.fields), **dict(values.schema.fields)},
                    base.schema.version)
        windowed = _WindowedRuntime(runtime)
        verified = verify_dag(document, registry, runtime=windowed, max_nodes=40, max_depth=12)
        import tempfile
        with tempfile.TemporaryDirectory(prefix="paper-weight-binding-") as temp:
            execution = SerialExecutor(registry, CacheStore(Path(temp) / "cache", enabled=False)).execute_verified(
                verified, windowed, {"telemetry": telemetry}, cache_enabled=False)
        self.assertTrue(execution.answer.leads)
        self.assertTrue(all(record.status == "success" for record in execution.records))


if __name__ == "__main__":
    unittest.main()
