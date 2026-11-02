"""Paper-profile trusted-terminal contribution ablation tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from e4core.cache import CacheStore, canonical_json
from e4core.contracts import EntityRef, PaperStarterInput, Span, Telemetry
from e4core.dag import verify_dag
from e4core.executor import NodeContext, SerialExecutor
from e4core.m4_fixtures import synthetic_case_runtime
from e4core.m4_evolution_diagnostics import (_combine_counterfactuals, _record_map)
from e4core.m4_generated import validate_generated_spec
from e4core.m4_operator_ir import trusted_runtime_context
from e4core.m4_operator_ir_worker import execute_ir
from e4core.paper_starter import make_paper_starter_registry
from e4core.paper_starter_fixtures import _rich_fixture
from tests.test_m4_operator_ir import ALT, DATA_SHA, REF, RUNTIME, _spec, _trusted_inputs
from unittest.mock import patch


class PaperEvolutionCounterfactualTests(unittest.TestCase):
    def test_registered_paper_scorer_is_ablated_through_actual_paper_terminal(self):
        telemetry, starter = _rich_fixture()
        registry = make_paper_starter_registry(bank_only=True, starter_input=starter)
        runtime = synthetic_case_runtime(telemetry)
        document = {"schema_version": "e4-dag/1", "nodes": [
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
        verified = verify_dag(document, registry, runtime=runtime, max_nodes=40, max_depth=12)
        source_id = next(node.node_id for node in verified.nodes if node.operator.external_input)
        with tempfile.TemporaryDirectory(prefix="paper-counterfactual-") as temp:
            execution = SerialExecutor(registry, CacheStore(Path(temp) / "cache", enabled=False)).execute_verified(
                verified, runtime, {source_id: telemetry}, cache_enabled=False)
        score_node = next(node for node in verified.nodes if node.node_id == "score")
        score_spec = score_node.operator
        context = NodeContext(telemetry.case_id, score_node.node_id, score_spec.qualified_name,
            runtime.snapshot.values, runtime.snapshot.provenance,
            telemetry.reference_window, telemetry.alert_window, telemetry.data_sha256)
        candidate_spec = registry.require("bank.candidates.generate", "1")
        candidates = candidate_spec.implementation({"data": telemetry}, {}, context)
        starter_context = NodeContext(telemetry.case_id, "starter", "paper.input.prepare@1",
            runtime.snapshot.values, runtime.snapshot.provenance,
            telemetry.reference_window, telemetry.alert_window, telemetry.data_sha256)
        starter_result = registry.require("paper.input.prepare", "1").implementation(
            {"data": telemetry}, {"analysis_block_minutes": 1}, starter_context)
        params = {name: spec.default for name, spec in score_spec.parameters.items() if spec.has_default}
        score_result = score_spec.implementation({"data": starter_result, "candidates": candidates}, params, context)
        score_value = getattr(score_result, "value", score_result)
        records = _record_map(execution, None)
        result = _combine_counterfactuals(verified=verified, execution=execution,
            records=records, cached_outputs={"score": score_value},
            new_nodes=[score_node], runtime=runtime, registry=registry,
            case_id=telemetry.case_id)
        self.assertEqual(result["status"], "evaluated", result)
        self.assertEqual(result["scope"],
            "direct generated ScoredCandidates inputs to trusted paper.answers.weighted_consensus only")
        removal = result["individual_operator_removals"][0]
        self.assertEqual(removal["status"], "evaluated", result)
        self.assertTrue(removal["effect"]["answer_changed"])
        self.assertGreater(removal["effect"]["top3_removed_count"], 0)

    def test_generated_ir_then_dependency_adjust_uses_same_trusted_group_at_terminal(self):
        normalized = validate_generated_spec(_spec("variance_change"))
        registry = make_paper_starter_registry(bank_only=True, generated_specs=[normalized])
        inputs = _trusted_inputs()
        wire_inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
                       for key, value in inputs.items()}
        ir_output = execute_ir(normalized["program"], wire_inputs, {}, RUNTIME,
                               trusted_runtime_context())
        generated = registry.require(normalized["name"], normalized["version"])
        with patch("e4core.m4_generated.run_generated_operator", return_value={
                "ok": True, "result": ir_output, "stdout": "", "stderr": ""}):
            scored = generated.implementation(inputs, {}, NodeContext("analytic-fixture",
                "generated", generated.name, RUNTIME, {}, REF, ALT, DATA_SHA))
        self.assertEqual(scored.scorer, normalized["name"])

        identifiers = ("component-coral", "component-ember", "component-jade")
        telemetry = Telemetry("analytic-fixture", REF, ALT, (), (
            Span("partial", "parent", "outside", REF.start_ms_utc, "1", None,
                "missing", "", "unprovided", EntityRef("component", "component-coral"),
                None, None, False, True),
            Span("partial", "child", "parent", REF.start_ms_utc + 1, "2", None,
                "missing", "", "unprovided", EntityRef("component", "component-ember"),
                None, None, True, False)),
            {identifier: {"role": "component"} for identifier in identifiers},
            DATA_SHA, {}, {}, {}, "paper-evolution-test/1")
        starter = PaperStarterInput(telemetry, None, None, {
            "sessions": "unsupported", "urls": "unsupported", "native_logs": "unsupported",
            "native_log_patterns": "unsupported", "component_metrics": "unsupported",
            "app_mrt": "unsupported", "trace_dependencies": "available",
            "trace_root_duration": "unsupported"}, 60_000)
        context = NodeContext(telemetry.case_id, "adjust", "paper.scores.dependency_adjust@1",
            {}, {}, REF, ALT, DATA_SHA)
        adjusted = registry.require("paper.scores.dependency_adjust", "1").implementation(
            {"data": starter, "scores": scored}, {"downstream_penalty": 0.5}, context).value
        answer = registry.require("paper.answers.weighted_consensus", "1").implementation(
            {"scores": (adjusted,)}, {"family_weights": {"generated_metric_ir": 1.0},
                "tie_tolerance": 1e-9, "limit": 5}, context).value
        self.assertTrue(answer.leads)
        self.assertEqual(adjusted.status_details["trusted_source_scorer"], normalized["name"])
        self.assertEqual(adjusted.status_details["valid_trace_graph_count"], 1)


if __name__ == "__main__":
    unittest.main()
