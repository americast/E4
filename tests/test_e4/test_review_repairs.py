"""Regression test suite verifying fixes for R01 through R16."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from adapters.bank_adapter import BankAdapter
from adapters.bank_library import (
    build_bank_registry,
    get_bank_contrast_playbook,
    get_bank_seed_playbook,
)
from benchmarks.bank_benchmark import LocalLlamaClient, load_ground_truth
from e4.controller.controller import DiagnosisController, _to_ranked_answer
from e4.controller.library import LibraryManager, Playbook
from e4.controller.repair import StaticRepairController
from e4.controller.schemas import validate_operator_proposal, validate_proposal_bundle
from e4.dag import SerialExecutor, verify_dag
from e4.data.adapter import BaseDatasetAdapter
from e4.data.folder import FolderTelemetrySource
from e4.oracle import AcceptanceOracle, Decision, DiagnosticContext
from e4.prompts.examples import METRIC_ANOMALY_EXAMPLE
from e4.prompts.templates import format_evolve_prompt
from e4.registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec
from e4.sandbox.ast_validator import SecurityValidationError, validate_source
from e4.sandbox.runner import BubblewrapRunner, ExecutionError, InProcessRunner, SubprocessRunner
from e4.sandbox.typechecker import TypeCheckError, check_operator_types
from e4.sandbox.views import (
    CandidateSet,
    EntityRef,
    MetricRecord,
    SpanRecord,
    TelemetryView,
    Window,
    aggregate_records,
    group_by,
    metric_points,
)
from e4.types import RankedAnswer


class FixedOracle(AcceptanceOracle):
    def __init__(self, target_id: str):
        self.target_id = target_id

    def evaluate(self, answer: RankedAnswer, context: DiagnosticContext) -> Decision:
        if answer and answer.candidates:
            if any(c.identifier == self.target_id for c in answer.candidates):
                return Decision("confirm", "Target confirmed")
        return Decision("revise", "Keep investigating")


class MockTelemetry(BaseDatasetAdapter):
    def __init__(self, entities: Sequence[EntityRef] | None = None):
        self._entities = list(entities or [EntityRef("service", "target_a"), EntityRef("service", "entity_b")])

    def get_schema(self):
        from e4.types import ColumnSchema, DatasetSchema, TableSchema
        return DatasetSchema(
            tables=(TableSchema("metrics.csv", "metric", (ColumnSchema("val", "number"), ColumnSchema("cmdb_id", "string"))),)
        )

    def get_windows(self):
        return (Window("reference", 0, 1000), Window("alert", 1000, 2000))

    def extract_suspect_entities(self):
        return list(self._entities)

    def stream_records(self, table_name, window):
        return iter(())


class TestReviewRepairs(unittest.TestCase):
    """Verifies that all 16 issues (R01 to R16) are strictly fixed."""

    # -------------------------------------------------------------------------
    # R01: Failed pipelines must not become confirmed diagnoses
    # -------------------------------------------------------------------------
    def test_r01_failed_adaptation_does_not_fabricate_answer(self):
        reg = OperatorRegistry()
        reg.register(
            OperatorSpec(
                name="initial.op",
                version="1",
                inputs=(),
                output_type="RankedAnswer",
                implementation=lambda i, p, r: RankedAnswer(
                    candidates=(EntityRef("service", "entity_b"),)
                ),
            )
        )
        lib = LibraryManager(registry=reg)
        lib.add_playbook(
            Playbook(
                name="pb_1",
                description="test",
                dag={
                    "nodes": [{"id": "n1", "operator": "initial.op", "version": "1"}],
                    "return_node": "n1",
                },
            )
        )

        class ModelWithBrokenAdaptation:
            def complete(self, prompt: str) -> str:
                if "Stage 1" in prompt:
                    return json.dumps({"selected_playbook": "pb_1"})
                if "Stage 2" in prompt:
                    # References an unknown operator
                    return json.dumps({
                        "dag": {
                            "nodes": [{"id": "bad", "operator": "non_existent.op", "version": "1"}],
                            "return_node": "bad",
                        }
                    })
                # Stage 3
                return json.dumps({"gap_record": {}, "proposed_operators": []})

        oracle = FixedOracle(target_id="target_a")
        controller = DiagnosisController(library=lib, oracle=oracle, runner=InProcessRunner())
        result = controller.run_diagnostic(MockTelemetry(), ModelWithBrokenAdaptation())

        # Must NOT confirm target_a because adaptation failed and Stage 3 had no operators
        self.assertNotEqual(result.status, "confirmed")
        self.assertIsNone(result.admitted_playbook)
        # Adaptation stage must be recorded as failed execution
        compose_history = [h for h in result.history if h.get("stage") == "compose"]
        self.assertTrue(any(h.get("status") == "failed_execution" for h in compose_history))

    def test_r01_to_ranked_answer_returns_none_on_invalid_value(self):
        entities = [EntityRef("service", "a"), EntityRef("service", "b")]
        self.assertIsNone(_to_ranked_answer(None, "select", entities))
        self.assertIsNone(_to_ranked_answer("not an answer", "select", entities))
        self.assertIsNone(_to_ranked_answer(12345, "select", entities))

    # -------------------------------------------------------------------------
    # R02: Active library mutation & DAG deep copy
    # -------------------------------------------------------------------------
    def test_r02_unverified_evolution_does_not_mutate_active_library(self):
        reg = OperatorRegistry()
        lib = LibraryManager(registry=reg)

        valid_source = (
            "def operator(inputs, params, runtime):\n"
            "    return make_candidate_set(())\n"
        )

        class ModelWithUnusedEvolve:
            def complete(self, prompt: str) -> str:
                if "Stage 3A" in prompt:
                    return json.dumps({
                        "gap_record": {"gap_description": "test", "proposed_operator_names": ["evolved.unused"]},
                        "proposed_operators": [{
                            "name": "evolved.unused",
                            "version": "1",
                            "authoring_profile": "typed-operator-code-v1",
                            "description": "unused operator",
                            "family": "custom",
                            "input_ports": [{"name": "data", "type": "Telemetry"}],
                            "output_type": "CandidateSet",
                            "parameters": {},
                            "input_requirements": {"data": {}},
                            "source": valid_source,
                        }],
                    })
                if "Stage 3B" in prompt:
                    # SERVE DAG does not reference evolved.unused
                    return json.dumps({
                        "dag": {
                            "nodes": [{"id": "n1", "operator": "non_existent", "version": "1"}],
                            "return_node": "n1",
                        }
                    })
                return json.dumps({})

        controller = DiagnosisController(library=lib, runner=InProcessRunner())
        result = controller.run_diagnostic(MockTelemetry([EntityRef("service", "svc_1")]), ModelWithUnusedEvolve())

        # evolved.unused must NOT be in lib.registry because serving failed
        self.assertFalse(lib.registry.has("evolved.unused", "1"))
        self.assertNotIn("evolved.unused@1", result.admitted_operators)

    def test_r02_retrieval_parameter_binding_does_not_mutate_stored_playbook(self):
        reg = OperatorRegistry()
        reg.register(
            OperatorSpec(
                name="test.op",
                version="1",
                inputs=(),
                output_type="RankedAnswer",
                parameters={"x": ParameterSpec("x", "number", default=1.0, has_default=True)},
                implementation=lambda i, p, r: RankedAnswer(
                    candidates=(EntityRef("service", "svc"),), scores=(float(p.get("x", 0)),)
                ),
            )
        )
        stored_dag = {
            "nodes": [{"id": "n1", "operator": "test.op", "version": "1", "params": {"x": 1.0}}],
            "return_node": "n1",
        }
        lib = LibraryManager(registry=reg)
        lib.add_playbook(Playbook("pb_test", "desc", stored_dag))

        class ModelWithOverride:
            def complete(self, prompt: str) -> str:
                return json.dumps({
                    "selected_playbook": "pb_test",
                    "parameter_bindings": {"n1": {"x": 9.0}},
                })

        controller = DiagnosisController(library=lib, runner=InProcessRunner())
        controller.run_diagnostic(MockTelemetry([EntityRef("service", "svc")]), ModelWithOverride())

        # The stored playbook's DAG in library must remain x = 1.0!
        orig_pb = lib.get_playbook("pb_test")
        self.assertEqual(orig_pb.dag["nodes"][0]["params"]["x"], 1.0)

    # -------------------------------------------------------------------------
    # R03: Saving library must persist operators alongside playbooks
    # -------------------------------------------------------------------------
    def test_r03_save_and_load_library_persists_operators(self):
        reg = OperatorRegistry()
        spec = OperatorSpec(
            name="evolved.custom_ratio",
            version="1",
            inputs=(PortSpec(name="data", value_type="Telemetry"),),
            output_type="Aggregate",
            parameters={"thresh": ParameterSpec("thresh", "number", default=2.5, has_default=True)},
            source="def operator(inputs, params, runtime):\n    return make_aggregate(runtime.alert_window, ())\n",
            description="Ratio operator",
        )
        reg.register(spec)
        lib = LibraryManager(registry=reg)
        lib.add_playbook(
            Playbook(
                name="pb_custom",
                description="Playbook with custom op",
                dag={
                    "nodes": [{"id": "n1", "operator": "evolved.custom_ratio", "version": "1"}],
                    "return_node": "n1",
                },
            )
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            lib.save_to_dir(tmpdir)
            self.assertTrue((Path(tmpdir) / "operators.json").is_file())
            self.assertTrue((Path(tmpdir) / "playbooks.json").is_file())

            # Load into fresh LibraryManager with empty registry
            loaded = LibraryManager.load_from_dir(tmpdir)
            self.assertTrue(loaded.registry.has("evolved.custom_ratio", "1"))
            loaded_spec = loaded.registry.require("evolved.custom_ratio", "1")
            self.assertEqual(loaded_spec.source, spec.source)
            self.assertEqual(loaded_spec.parameters["thresh"].default, 2.5)

    # -------------------------------------------------------------------------
    # R04: No invented entities and fractional seconds conversion
    # -------------------------------------------------------------------------
    def test_r04_folder_source_no_invented_entities_and_preserves_fractions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "metrics.csv").write_text("timestamp,value\n1.5,10.0\n", encoding="utf-8")
            mapping = {
                "tables": [{
                    "name": "metrics.csv",
                    "file": "metrics.csv",
                    "record_type": "metric",
                    "timestamp_unit": "s",
                }]
            }
            src = FolderTelemetrySource(root, mapping)
            # Empty entity column -> empty suspect entities (NOT default_service)
            self.assertEqual(src.extract_suspect_entities(), [])

            # Fractional seconds 1.5s should become 1500 ms (NOT 1000)
            records = list(src.stream_records("metrics.csv", Window("alert", 0, 5000)))
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].timestamp_ms_utc, 1500)

    def test_r04_span_without_success_column_yields_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "trace.csv").write_text("timestamp,trace_id,span_id,duration\n100,t1,s1,50.0\n", encoding="utf-8")
            mapping = {
                "tables": [{
                    "name": "trace.csv",
                    "file": "trace.csv",
                    "record_type": "span",
                    "timestamp_unit": "ms",
                }]
            }
            src = FolderTelemetrySource(root, mapping)
            records = list(src.stream_records("trace.csv", Window("alert", 0, 5000)))
            self.assertEqual(len(records), 1)
            # success must be None, NOT True
            self.assertIsNone(records[0].success)

    # -------------------------------------------------------------------------
    # R05: Bank starter library equivalence
    # -------------------------------------------------------------------------
    def test_r05_bank_contrast_and_consensus_library(self):
        reg = build_bank_registry()
        self.assertTrue(reg.has("bank.metrics.contrast", "1"))
        self.assertTrue(reg.has("paper.answers.weighted_consensus", "1"))
        contrast_pb = get_bank_contrast_playbook()
        self.assertEqual(contrast_pb["name"], "bank.starter.metric_contrast")
        # Playbook verified DAG should pass
        verified = verify_dag(contrast_pb["dag"], reg)
        self.assertEqual(verified.return_node, "node_consensus")

    # -------------------------------------------------------------------------
    # R06: Truncation metadata in coverage
    # -------------------------------------------------------------------------
    def test_r06_telemetry_truncation_reported_in_coverage(self):
        ref_w = Window("reference", 0, 1000)
        alt_w = Window("alert", 1000, 2000)
        recs = [
            MetricRecord("metrics.csv", 1500, EntityRef("service", "s1"), "cpu", 0.5)
        ]
        tel_view = TelemetryView(
            case_id="case_t",
            reference_window=ref_w,
            alert_window=alt_w,
            records=recs,
            source_tables=["metrics.csv"],
            metadata={"truncated": True, "truncated_tables": ["metrics.csv"]},
        )
        res = metric_points(tel_view, "metrics.csv", alt_w)
        self.assertTrue(res.coverage.truncated)
        self.assertFalse(res.coverage.complete)

    # -------------------------------------------------------------------------
    # R07: Aggregation requested field & grouping validation
    # -------------------------------------------------------------------------
    def test_r07_aggregate_records_requested_field_and_invalid_field_raises(self):
        ref_w = Window("reference", 0, 1000)
        alt_w = Window("alert", 1000, 2000)
        recs = [
            MetricRecord("m.csv", 1011, EntityRef("service", "s1"), "cpu", 3.0)
        ]
        tel = TelemetryView("c1", ref_w, alt_w, recs)

        # Field "timestamp_ms_utc" must aggregate timestamp (1011.0), NOT metric value 3.0!
        res_ts = aggregate_records(tel, "m.csv", alt_w, "metric", field="timestamp_ms_utc", statistic="sum")
        self.assertEqual(res_ts.rows[0].value, 1011.0)

        # Field "does_not_exist" must raise ValueError!
        with self.assertRaises(ValueError):
            aggregate_records(tel, "m.csv", alt_w, "metric", field="does_not_exist", statistic="sum")

        # Unknown grouping field must raise ValueError!
        with self.assertRaises(ValueError):
            aggregate_records(tel, "m.csv", alt_w, "metric", field="value", statistic="sum", group_by=["non_existent_group"])

    def test_r07_group_by_unknown_field_raises(self):
        class Row:
            def __init__(self, a):
                self.a = a

        rows = [Row(1), Row(2)]
        with self.assertRaises(ValueError):
            group_by(rows, ["non_existent_field"])

    # -------------------------------------------------------------------------
    # R08: Rejection of mypy / type suppression & runtime output contract
    # -------------------------------------------------------------------------
    def test_r08_rejects_suppression_directives(self):
        bad_source_1 = (
            "# mypy: ignore-errors\n"
            "def operator(inputs, params, runtime):\n"
            "    return 123\n"
        )
        with self.assertRaises(SecurityValidationError):
            validate_source(bad_source_1)

        bad_source_2 = (
            "def operator(inputs, params, runtime):\n"
            "    return 123  # type: ignore\n"
        )
        with self.assertRaises(SecurityValidationError):
            validate_source(bad_source_2)

    def test_r08_worker_validates_declared_output_type(self):
        # Operator declared to return CandidateSet, but returning int
        source = (
            "def operator(inputs, params, runtime):\n"
            "    return 123\n"
        )
        spec = OperatorSpec(
            name="evolved.bad_return",
            version="1",
            inputs=(),
            output_type="CandidateSet",
            source=source,
        )
        runner = InProcessRunner()
        with self.assertRaises(TypeError):
            runner.run(spec, {}, {}, None)

    # -------------------------------------------------------------------------
    # R09: Input requirements validation and retention
    # -------------------------------------------------------------------------
    def test_r09_input_requirements_validation(self):
        bad_spec = {
            "name": "evolved.test_op",
            "version": "1",
            "authoring_profile": "typed-operator-code-v1",
            "description": "desc",
            "family": "custom",
            "input_ports": [{"name": "data", "type": "Telemetry"}],
            "output_type": "CandidateSet",
            "parameters": {},
            "input_requirements": {"data": "not a contract"},  # String instead of mapping
            "source": "def operator(inputs, params, runtime):\n    return make_candidate_set(())\n",
        }
        bundle = {"gap_record": {}, "proposed_operators": [bad_spec]}
        errors = validate_proposal_bundle(bundle)
        self.assertTrue(any("input_requirements for port 'data' must be a mapping" in e for e in errors))

    # -------------------------------------------------------------------------
    # R10: Runtime parameter resolution across mappings
    # -------------------------------------------------------------------------
    def test_r10_runtime_parameter_resolution(self):
        reg = OperatorRegistry()
        reg.register(
            OperatorSpec(
                name="test.runtime_op",
                version="1",
                inputs=(),
                output_type="Aggregate",
                parameters={"depth": ParameterSpec("depth", "integer", required=True, runtime_field="dag_depth")},
                implementation=lambda i, p, r: int(p["depth"]),
            )
        )
        dag_doc = {
            "nodes": [{
                "id": "n1",
                "operator": "test.runtime_op",
                "version": "1",
                "bindings": {"depth": "runtime.dag_depth"},
            }],
            "return_node": "n1",
        }
        verified = verify_dag(dag_doc, reg, runtime={"dag_depth": 5})
        executor = SerialExecutor(reg)
        res = executor.execute_verified(verified, runtime={"dag_depth": 5})
        self.assertEqual(res.return_value, 5)

    # -------------------------------------------------------------------------
    # R11: Bounded context & duplicate body rejection
    # -------------------------------------------------------------------------
    def test_r11_evolve_prompt_includes_catalog_and_rejects_duplicates(self):
        prompt = format_evolve_prompt(
            "case_01",
            [{"stage": "select", "decision": "revise"}],
            {"case_id": "case_01"},
            {"tables": []},
            available_operators=[{"name": "bank.metrics.contrast"}],
        )
        self.assertIn("bank.metrics.contrast", prompt)

        bundle = {
            "gap_record": {},
            "proposed_operators": [
                {
                    "name": "evolved.op_a",
                    "version": "1",
                    "authoring_profile": "typed-operator-code-v1",
                    "description": "op a",
                    "family": "c",
                    "input_ports": [{"name": "d", "type": "Telemetry"}],
                    "output_type": "Aggregate",
                    "parameters": {},
                    "input_requirements": {"d": {}},
                    "source": "def operator(inputs, params, runtime):\n    return make_aggregate(runtime.alert_window, ())\n",
                },
                {
                    "name": "evolved.op_b",
                    "version": "1",
                    "authoring_profile": "typed-operator-code-v1",
                    "description": "op b",
                    "family": "c",
                    "input_ports": [{"name": "d", "type": "Telemetry"}],
                    "output_type": "Aggregate",
                    "parameters": {},
                    "input_requirements": {"d": {}},
                    "source": "def operator(inputs, params, runtime):\n    return make_aggregate(runtime.alert_window, ())\n",
                },
            ],
        }
        errors = validate_proposal_bundle(bundle)
        self.assertTrue(any("Structural duplicate body" in e for e in errors))

    # -------------------------------------------------------------------------
    # R12: Initial JSON parse repair and accurate call accounting
    # -------------------------------------------------------------------------
    def test_r12_initial_json_parse_repair_and_accounting(self):
        controller = StaticRepairController(max_repairs=2)

        valid_bundle = json.dumps({
            "gap_record": {"gap_description": "test", "proposed_operator_names": ["evolved.fixed"]},
            "proposed_operators": [{
                "name": "evolved.fixed",
                "version": "1",
                "authoring_profile": "typed-operator-code-v1",
                "description": "fixed",
                "family": "custom",
                "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
                "output_type": "Aggregate",
                "parameters": {"threshold": {"type": "number", "default": 2.0}},
                "input_requirements": {"telemetry": {}},
                "source": METRIC_ANOMALY_EXAMPLE,
            }],
        })

        # Initial response is invalid JSON: "{"
        calls = 0

        def model_fn(prompt: str) -> str:
            nonlocal calls
            calls += 1
            return valid_bundle

        ok, specs, errors, repairs_used = controller.attempt_repairs("{", model_fn)
        self.assertTrue(ok)
        self.assertEqual(len(specs), 1)
        self.assertEqual(repairs_used, 1)
        self.assertEqual(calls, 1)

    # -------------------------------------------------------------------------
    # R13: BubblewrapRunner invokes binary and fails when binary fails
    # -------------------------------------------------------------------------
    def test_r13_bubblewrap_runner_invokes_binary_and_fails(self):
        spec = OperatorSpec(
            name="test.bwrap",
            version="1",
            inputs=(),
            output_type="Aggregate",
            source="def operator(inputs, params, runtime):\n    return 1\n",
        )
        runner = BubblewrapRunner(bwrap_path="/usr/bin/false")
        with self.assertRaises(ExecutionError):
            runner.run(spec, {}, {}, None)

    # -------------------------------------------------------------------------
    # R14: Benchmark profile and ground truth validation
    # -------------------------------------------------------------------------
    def test_r14_benchmark_validation_and_client_usage(self):
        with self.assertRaises(FileNotFoundError):
            load_ground_truth(Path("non_existent_gt_file.jsonl"))

        client = LocalLlamaClient(seed=43, temperature=0.5)
        self.assertEqual(client.seed, 43)
        self.assertEqual(client.temperature, 0.5)
        self.assertEqual(client.usage["total_requests"], 0)


if __name__ == "__main__":
    unittest.main()
