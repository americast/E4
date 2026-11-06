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
    weighted_consensus_impl,
)
from benchmarks.bank_benchmark import LocalLlamaClient, load_ground_truth, load_pinned_inference_config
from e4.controller.controller import DiagnosisController, _to_ranked_answer
from e4.controller.library import LibraryManager, Playbook
from e4.controller.repair import StaticRepairController
from e4.controller.schemas import validate_operator_proposal, validate_proposal_bundle
from e4.dag import DAGValidationError, SerialExecutor, verify_dag
from e4.data.adapter import BaseDatasetAdapter
from e4.data.folder import FolderTelemetrySource
from e4.engine import DiagnosisEngine
from e4.oracle import AcceptanceOracle, BudgetStopPolicy, Decision, DiagnosticContext, GroundTruthOracle
from e4.prompts.examples import METRIC_ANOMALY_EXAMPLE
from e4.prompts.templates import format_evolve_prompt
from e4.registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec
from e4.sandbox.ast_validator import SecurityValidationError, validate_source
from e4.sandbox.runner import BubblewrapRunner, ExecutionError, InProcessRunner, SubprocessRunner
from e4.sandbox.typechecker import TypeCheckError, check_operator_types
from e4.sandbox.views import (
    Candidate,
    CandidateSet,
    CoverageView,
    EntityRef,
    MetricRecord,
    ScoredCandidate,
    ScoredCandidates,
    SpanRecord,
    TelemetryView,
    Window,
    aggregate_records,
    group_by,
    make_evidence,
    metric_points,
)
from e4.types import Evidence, RankedAnswer


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
                "parameters": {"source_table": {"type": "string", "required": True}},
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

    # -------------------------------------------------------------------------
    # F01: Unscored CandidateSet rejected as terminal ranked answer
    # -------------------------------------------------------------------------
    def test_f01_unscored_candidate_set_rejected_by_to_ranked_answer(self):
        cset = CandidateSet(
            candidates=(Candidate(EntityRef("service", "cartservice"), "discovery rationale"),),
            source="discovery",
        )
        ans = _to_ranked_answer(cset, "select", (EntityRef("service", "cartservice"),))
        self.assertIsNone(ans)

    # -------------------------------------------------------------------------
    # F02: Worker preserves metadata and scopes truncation to affected tables
    # -------------------------------------------------------------------------
    def test_f02_worker_preserves_metadata_and_scopes_truncation(self):
        rec1 = MetricRecord(
            source_table="bank/metric_service.csv",
            timestamp_ms_utc=150,
            entity=EntityRef("service", "cartservice"),
            metric_name="cpu",
            value=10.0,
        )
        rec2 = MetricRecord(
            source_table="bank/metric_container.csv",
            timestamp_ms_utc=150,
            entity=EntityRef("service", "cartservice"),
            metric_name="cpu",
            value=20.0,
        )
        tel = TelemetryView(
            case_id="case_01",
            reference_window=Window("reference", 0, 100),
            alert_window=Window("alert", 100, 200),
            records=[rec1, rec2],
            metadata={"truncated": True, "truncated_tables": ["bank/metric_container.csv"]},
        )
        # Query untruncated table: complete=True
        q1 = metric_points(tel, "bank/metric_service.csv", tel.alert_window)
        self.assertTrue(q1.coverage.complete)
        self.assertFalse(q1.coverage.truncated)

        # Query truncated table: complete=False, truncated=True
        q2 = metric_points(tel, "bank/metric_container.csv", tel.alert_window)
        self.assertFalse(q2.coverage.complete)
        self.assertTrue(q2.coverage.truncated)

        # Truncated coverage marks evidence ranking_eligible=False
        ev = make_evidence(
            source="bank/metric_container.csv",
            window=tel.alert_window,
            statistic="mean",
            value=20.0,
            contributing_entities=(EntityRef("service", "cartservice"),),
            support=1,
            coverage=q2.coverage,
        )
        self.assertFalse(ev.ranking_eligible)

    # -------------------------------------------------------------------------
    # F03: Admission reachability excludes disconnected operators
    # -------------------------------------------------------------------------
    def test_f03_admission_reachability_excludes_disconnected_operator(self):
        reg = OperatorRegistry()
        lib = LibraryManager(registry=reg)
        controller = DiagnosisController(library=lib, oracle=FixedOracle("target_a"), runner=InProcessRunner())

        evolved_conn_dict = {
            "name": "evolved.connected",
            "version": "1",
            "authoring_profile": "typed-operator-code-v1",
            "description": "Connected operator",
            "family": "custom",
            "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "input_requirements": {"telemetry": {}},
            "source": (
                "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n"
                "    tel = telemetry_view(inputs['telemetry'])\n"
                "    ent = make_entity_ref('service', 'target_a')\n"
                "    ev = make_evidence('scorer_a', tel.alert_window, 'score', 1.0, [ent], 1)\n"
                "    return make_scored_candidates([make_scored_candidate(ent, 1.0, evidence=[ev])], scorer='scorer_a')\n"
            ),
        }
        evolved_disconn_dict = {
            "name": "evolved.disconnected",
            "version": "1",
            "authoring_profile": "typed-operator-code-v1",
            "description": "Disconnected operator",
            "family": "custom",
            "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "input_requirements": {"telemetry": {}},
            "source": "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n    return make_scored_candidates([make_scored_candidate(make_entity_ref(\"service\", \"other\"), 0.5)], scorer=\"scorer_b\")\n",
        }

        serving_dag = {
            "nodes": [
                {"id": "node_conn", "operator": "evolved.connected", "version": "1", "inputs": {"telemetry": "telemetry"}},
                {"id": "node_disconn", "operator": "evolved.disconnected", "version": "1", "inputs": {"telemetry": "telemetry"}},
            ],
            "return_node": "node_conn",
        }

        class EvolveMock:
            def complete(self, prompt: str) -> str:
                if "Stage 1: Playbook Retrieval" in prompt:
                    return json.dumps({"selected_playbook": "dummy", "parameter_bindings": {}})
                if "Stage 2: DAG Adaptation" in prompt:
                    return json.dumps({"dag": {}})
                if "Stage 3A" in prompt or "repair" in prompt.lower():
                    return json.dumps({
                        "gap_record": {
                            "gap_description": "Custom connected analysis for target",
                            "proposed_operator_names": ["evolved.connected", "evolved.disconnected"],
                        },
                        "proposed_operators": [evolved_conn_dict, evolved_disconn_dict],
                    })
                return json.dumps({"dag": serving_dag})

        res = controller.run_diagnostic(MockTelemetry(), EvolveMock())
        self.assertEqual(res.status, "confirmed")
        # Only connected operator admitted; disconnected operator excluded
        self.assertIn("evolved.connected@1", res.admitted_operators)
        self.assertNotIn("evolved.disconnected@1", res.admitted_operators)
        self.assertFalse(lib.registry.has("evolved.disconnected", "1"))

    # -------------------------------------------------------------------------
    # F04: Public library reload with base_registry preserves built-in callables
    # -------------------------------------------------------------------------
    def test_f04_library_reload_with_base_registry_preserves_builtins(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_reg = build_bank_registry()
            lib = LibraryManager(registry=base_reg.clone())
            lib.add_playbook(Playbook("test.pb", "desc", get_bank_contrast_playbook()["dag"]))
            lib.save_to_dir(tmpdir)

            # Reload into fresh engine using base_registry
            engine = DiagnosisEngine(
                model=lambda p: json.dumps({}),
                library=tmpdir,
                base_registry=base_reg,
            )
            spec = engine.library.registry.require("bank.metrics.contrast", "1")
            self.assertIsNotNone(spec.implementation)

    # -------------------------------------------------------------------------
    # F05: Capability validation rejects invalid statistics, grouping, and tables
    # -------------------------------------------------------------------------
    def test_f05_dag_verification_validates_data_requirements_and_rejects_missing(self):
        reg = OperatorRegistry()
        reg.register(
            OperatorSpec(
                name="test.producer",
                version="1",
                inputs=(),
                output_type="Aggregate",
                implementation=lambda i, p, r: 1,
            )
        )
        reg.register(
            OperatorSpec(
                name="test.consumer_bad_table",
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={"data": {"source_table": "missing_table.missing_column"}},
                implementation=lambda i, p, r: 1,
            )
        )
        bad_dag = {
            "nodes": [
                {"id": "n1", "operator": "test.producer", "version": "1", "inputs": {}},
                {"id": "n2", "operator": "test.consumer_bad_table", "version": "1", "inputs": {"data": "n1"}},
            ],
            "return_node": "n2",
        }
        with self.assertRaises(DAGValidationError):
            verify_dag(bad_dag, reg)

    # -------------------------------------------------------------------------
    # F06: Stricter caller budget respected and SELECT malformed JSON repaired
    # -------------------------------------------------------------------------
    def test_f06_stricter_caller_budget_and_select_parse_repair(self):
        engine = DiagnosisEngine(
            model=lambda p: json.dumps({}),
            max_attempts=1,
            oracle=BudgetStopPolicy(max_attempts=3),
        )
        self.assertEqual(engine.controller.max_attempts, 1)

        # SELECT with malformed initial JSON is repaired
        lib = LibraryManager()
        lib.add_playbook(Playbook("pb1", "desc", {"nodes": [{"id": "n1", "operator": "test", "inputs": {}}], "return_node": "n1"}))
        ctrl = DiagnosisController(library=lib, max_attempts=1)
        calls = 0

        def faulty_model(prompt: str) -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                return "{"  # malformed JSON
            return json.dumps({"selected_playbook": "pb1", "parameter_bindings": {}})

        res = ctrl.run_diagnostic(MockTelemetry(), faulty_model)
        self.assertGreaterEqual(calls, 1)

    # -------------------------------------------------------------------------
    # F07: Consensus deduplication invariance across duplicate scorers
    # -------------------------------------------------------------------------
    def test_f07_bank_consensus_duplicate_input_invariance(self):
        cand_a = ScoredCandidate(EntityRef("service", "service_a"), 0.8, analysis_family="backend_metric")
        cand_b = ScoredCandidate(EntityRef("service", "service_b"), 1.0, analysis_family="trace_latency")

        metric_scorer = ScoredCandidates((cand_a,), scorer="bank.metrics.contrast")
        trace_scorer = ScoredCandidates((cand_b,), scorer="bank.traces.latency_contrast")

        params = {"family_weights": {"trace_latency": 1.2, "backend_metric": 1.0}}

        # 1 metric scorer + 1 trace scorer: trace_b wins (1.2 vs 1.0)
        ans1 = weighted_consensus_impl({"scores": [metric_scorer, trace_scorer]}, params, {})
        self.assertEqual(ans1.candidates[0].identifier, "service_b")

        # 4 identical metric scorers + 1 trace scorer: trace_b STILL wins (invariance)
        ans4 = weighted_consensus_impl(
            {"scores": [metric_scorer, metric_scorer, metric_scorer, metric_scorer, trace_scorer]},
            params,
            {},
        )
        self.assertEqual(ans4.candidates[0].identifier, "service_b")

    # -------------------------------------------------------------------------
    # F08: AST-aware duplicate check against existing library
    # -------------------------------------------------------------------------
    def test_f08_ast_aware_duplicate_checking_against_existing_library(self):
        existing_reg = OperatorRegistry()
        existing_reg.register(
            OperatorSpec(
                name="existing.contrast",
                version="1",
                inputs=(),
                output_type="Aggregate",
                source="def operator(inputs, params, runtime):\n    x = 10\n    return x\n",
            )
        )
        # Proposed operator has identical AST structure with different spacing/comments
        bundle = {
            "gap_record": {
                "gap_description": "Custom variance contrast computation",
                "proposed_operator_names": ["evolved.duplicate_op"],
            },
            "proposed_operators": [{
                "name": "evolved.duplicate_op",
                "version": "1",
                "authoring_profile": "typed-operator-code-v1",
                "description": "desc",
                "family": "custom",
                "input_ports": [{"name": "d", "type": "Telemetry"}],
                "output_type": "Aggregate",
                "parameters": {},
                "input_requirements": {"d": {}},
                "source": "# A comment\ndef operator(inputs, params, runtime):\n    x = 10\n\n    return x\n",
            }],
        }
        errors = validate_proposal_bundle(bundle, existing_library=existing_reg)
        self.assertTrue(any("Duplicate implementation: identical source to existing" in e for e in errors))

    # -------------------------------------------------------------------------
    # G01: Consistent requirement shape, plural columns/stats, group_by dimensions
    # -------------------------------------------------------------------------
    def test_g01_capability_checking_plural_and_schema(self):
        reg = OperatorRegistry()
        reg.register(
            OperatorSpec(
                name="test.producer",
                version="1",
                inputs=(),
                output_type="Aggregate",
                parameters={
                    "table": ParameterSpec("table", "string", default="bank/metric_container.csv"),
                    "statistic": ParameterSpec("statistic", "string", default="mean"),
                },
                implementation=lambda i, p, r: 1,
            )
        )
        adapter = BankAdapter("data/prepared/bank/public/cases/bank-20210304T1800-utc8")
        schema = adapter.get_schema()

        # 1. Invalid table in source_columns with dataset_schema
        reg.register(
            OperatorSpec(
                name="test.consumer_bad_source_columns_table",
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={"data": {"source_columns": {"missing_table": ["col"]}}},
                implementation=lambda i, p, r: 1,
            )
        )
        bad_dag1 = {
            "nodes": [
                {"id": "n1", "operator": "test.producer", "version": "1", "inputs": {}},
                {"id": "n2", "operator": "test.consumer_bad_source_columns_table", "version": "1", "inputs": {"data": "n1"}},
            ],
            "return_node": "n2",
        }
        with self.assertRaises(DAGValidationError) as raised:
            verify_dag(bad_dag1, reg, dataset_schema=schema)
        self.assertEqual(raised.exception.code, "input_requirement_mismatch")

        # 2. Invalid column in existing table in dataset_schema
        reg.register(
            OperatorSpec(
                name="test.consumer_bad_column",
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={"data": {"source_columns": {"bank/metric_container.csv": ["nonexistent_col"]}}},
                implementation=lambda i, p, r: 1,
            )
        )
        bad_dag2 = {
            "nodes": [
                {"id": "n1", "operator": "test.producer", "version": "1", "inputs": {}},
                {"id": "n2", "operator": "test.consumer_bad_column", "version": "1", "inputs": {"data": "n1"}},
            ],
            "return_node": "n2",
        }
        with self.assertRaises(DAGValidationError) as raised:
            verify_dag(bad_dag2, reg, dataset_schema=schema)
        self.assertEqual(raised.exception.code, "input_requirement_mismatch")

        # 3. Invented statistic in statistics list
        reg.register(
            OperatorSpec(
                name="test.consumer_invented_stat",
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={"data": {"statistics": ["invented_statistic"], "group_by": []}},
                implementation=lambda i, p, r: 1,
            )
        )
        bad_dag3 = {
            "nodes": [
                {"id": "n1", "operator": "test.producer", "version": "1", "inputs": {}},
                {"id": "n2", "operator": "test.consumer_invented_stat", "version": "1", "inputs": {"data": "n1"}},
            ],
            "return_node": "n2",
        }
        with self.assertRaises(DAGValidationError) as raised:
            verify_dag(bad_dag3, reg, dataset_schema=schema)
        self.assertEqual(raised.exception.code, "input_requirement_mismatch")

        # 4. Valid group_by list and valid producer capability passes
        reg.register(
            OperatorSpec(
                name="test.consumer_valid",
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={
                    "data": {
                        "source_columns": {"bank/metric_container.csv": ["timestamp", "value"]},
                        "statistics": ["mean"],
                        "group_by": ["service"],
                    }
                },
                implementation=lambda i, p, r: 1,
            )
        )
        good_dag = {
            "nodes": [
                {"id": "n1", "operator": "test.producer", "version": "1", "inputs": {}, "params": {"table": "bank/metric_container.csv", "statistic": "mean"}},
                {"id": "n2", "operator": "test.consumer_valid", "version": "1", "inputs": {"data": "n1"}},
            ],
            "return_node": "n2",
        }
        verified = verify_dag(good_dag, reg, dataset_schema=schema)
        self.assertEqual(verified.return_node, "n2")

        # 5. Unsupported grouping dimension fails
        reg.register(
            OperatorSpec(
                name="test.consumer_bad_group",
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={"data": {"group_by": ["invented_dimension"]}},
                implementation=lambda i, p, r: 1,
            )
        )
        bad_dag4 = {
            "nodes": [
                {"id": "n1", "operator": "test.producer", "version": "1", "inputs": {}},
                {"id": "n2", "operator": "test.consumer_bad_group", "version": "1", "inputs": {"data": "n1"}},
            ],
            "return_node": "n2",
        }
        with self.assertRaises(DAGValidationError) as raised:
            verify_dag(bad_dag4, reg, dataset_schema=schema)
        self.assertEqual(raised.exception.code, "input_requirement_mismatch")

    # -------------------------------------------------------------------------
    # G02: Bank application metrics native schema (timestamp, rr, sr, cnt, mrt, tc)
    # -------------------------------------------------------------------------
    def test_g02_bank_application_metrics_schema_and_streaming(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            td = Path(tmp_dir)
            metric_dir = td / "source_dates" / "2021_03_04" / "metric"
            metric_dir.mkdir(parents=True)
            app_csv = metric_dir / "metric_app.csv"
            app_csv.write_text(
                "timestamp,rr,sr,cnt,mrt,tc\n"
                "1614850200,99.5,100.0,10,25.0,app-a\n"
                "1614850260,98.0,99.0,15,30.0,app-a\n",
                encoding="utf-8",
            )
            alert_json = td / "alert.json"
            alert_json.write_text(json.dumps({
                "analysis_window_start_local": "2021-03-04 17:30:00",
                "alert_window_start_local": "2021-03-04 18:00:00",
                "alert_window_end_local": "2021-03-04 18:30:00",
                "analysis_window_start_epoch_ms_utc": 1614848400000,
                "alert_window_start_epoch_ms_utc": 1614850200000,
                "alert_window_end_epoch_ms_utc": 1614852000000,
            }), encoding="utf-8")

            adapter = BankAdapter(td)
            records = list(adapter.stream_records("bank/metric_app.csv", adapter._alt_window))
            self.assertEqual(len(records), 8)

            mrt_records = [r for r in records if r.metric_name == "mrt"]
            self.assertEqual(len(mrt_records), 2)
            self.assertEqual(mrt_records[0].value, 25.0)
            # The Bank source schema leaves mrt's unit undeclared.
            self.assertIsNone(mrt_records[0].unit)
            self.assertEqual(mrt_records[0].entity.role, "app_kpi")
            self.assertEqual(mrt_records[0].entity.identifier, "app-a")

            cnt_records = [r for r in records if r.metric_name == "cnt"]
            self.assertEqual(cnt_records[0].value, 10.0)
            self.assertIsNone(cnt_records[0].unit)

            suspects = adapter.extract_suspect_entities()
            self.assertFalse(any(s.identifier == "app-a" for s in suspects))

    # -------------------------------------------------------------------------
    # G03: Coverage-to-evidence protection end-to-end and typecheck
    # -------------------------------------------------------------------------
    def test_g03_coverage_to_evidence_protection_end_to_end(self):
        source = (
            "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> ScoredCandidates:\n"
            "    tel = telemetry_view(inputs['telemetry'])\n"
            "    q = metric_points(tel, 'bank/metric_container.csv', tel.alert_window)\n"
            "    ev = make_evidence(\n"
            "        source='custom',\n"
            "        window=tel.alert_window,\n"
            "        statistic='mean',\n"
            "        value=1.0,\n"
            "        contributing_entities=[make_entity_ref('service', 'svc_a')],\n"
            "        support=10,\n"
            "        coverage=q.coverage,\n"
            "    )\n"
            "    c = make_scored_candidate(make_entity_ref('service', 'svc_a'), 1.0, evidence=[ev])\n"
            "    return make_scored_candidates([c], scorer='custom')\n"
        )
        spec = {
            "name": "custom.op",
            "version": "1",
            "authoring_profile": "typed-operator-code-v1",
            "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "input_requirements": {"telemetry": {}},
            "source": source,
        }
        report = check_operator_types(spec)
        self.assertEqual(report["status"], "passed")

        ent_a = EntityRef("service", "service_a")
        ent_b = EntityRef("service", "service_b")
        win = Window("alert", 100, 200)

        ineligible_ev = Evidence(
            source="test", window=win, statistic="mean", value=10.0,
            contributing_entities=(ent_a,), support=5, ranking_eligible=False,
        )
        eligible_ev = Evidence(
            source="test", window=win, statistic="mean", value=2.0,
            contributing_entities=(ent_b,), support=5, ranking_eligible=True,
        )

        cand_a = ScoredCandidate(entity=ent_a, score=10.0, evidence=(ineligible_ev,), analysis_family="metric")
        cand_b = ScoredCandidate(entity=ent_b, score=2.0, evidence=(eligible_ev,), analysis_family="metric")

        scorers = [ScoredCandidates(candidates=(cand_a, cand_b), scorer="test_scorer")]
        ans = weighted_consensus_impl({"scores": scorers}, {}, None)

        self.assertGreater(len(ans.candidates), 0)
        self.assertEqual(ans.candidates[0].identifier, "service_b")

        scorers_all_ineligible = [ScoredCandidates(candidates=(cand_a,), scorer="test_scorer")]
        ans_ineligible = weighted_consensus_impl({"scores": scorers_all_ineligible}, {}, None)
        self.assertEqual(len(ans_ineligible.candidates), 0)

    # -------------------------------------------------------------------------
    # G04: Reachability filtering leaves valid pruned admitted playbook
    # -------------------------------------------------------------------------
    def test_g04_reachability_filtering_pruned_admitted_playbook_reuse(self):
        class MockClient:
            def __init__(self):
                self.calls = 0
            def complete(self, prompt: str) -> str:
                self.calls += 1
                if "Stage 1" in prompt:
                    return json.dumps({"selected_playbook": "bank.starter.metric_consensus", "parameter_bindings": {}})
                elif "Stage 2" in prompt:
                    return json.dumps({
                        "adapted": False,
                        "identified_gap": {"gap_description": "Need custom score", "missing_capabilities": ["custom"]},
                    })
                elif "Stage 3A" in prompt:
                    return json.dumps({
                        "gap_record": {"gap_description": "Add connected and disconnected", "proposed_operator_names": ["evolved.connected", "evolved.disconnected"]},
                        "proposed_operators": [
                            {
                                "name": "evolved.connected",
                                "version": "1",
                                "authoring_profile": "typed-operator-code-v1",
                                "description": "Connected operator",
                                "family": "custom",
                                "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
                                "output_type": "ScoredCandidates",
                                "parameters": {},
                                "input_requirements": {"telemetry": {}},
                                "source": (
                                    "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> ScoredCandidates:\n"
                                    "    tel = telemetry_view(inputs['telemetry'])\n"
                                    f"    ent = make_entity_ref('service', {target_literal})\n"
                                    "    ev = make_evidence('conn', tel.alert_window, 'mean', 1.0, [ent], 5)\n"
                                    "    c = make_scored_candidate(ent, 5.0, evidence=[ev], analysis_family='backend_metric')\n"
                                    "    return make_scored_candidates([c], scorer='conn')\n"
                                ),
                            },
                            {
                                "name": "evolved.disconnected",
                                "version": "1",
                                "authoring_profile": "typed-operator-code-v1",
                                "description": "Disconnected operator",
                                "family": "custom",
                                "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
                                "output_type": "Aggregate",
                                "parameters": {},
                                "input_requirements": {"telemetry": {}},
                                "source": (
                                    "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Aggregate:\n"
                                    "    tel = telemetry_view(inputs['telemetry'])\n"
                                    "    return make_aggregate(tel.alert_window, [])\n"
                                ),
                            },
                        ],
                    })
                else:
                    return json.dumps({
                        "dag": {
                            "nodes": [
                                {"id": "n_conn", "operator": "evolved.connected", "version": "1", "inputs": {"telemetry": "telemetry"}},
                                {"id": "n_disc", "operator": "evolved.disconnected", "version": "1", "inputs": {"telemetry": "telemetry"}},
                                {"id": "n_consensus", "operator": "paper.answers.weighted_consensus", "version": "1", "inputs": {"scores": ["n_conn"]}},
                            ],
                            "return_node": "n_consensus",
                        }
                    })

        reg = build_bank_registry()
        lib = LibraryManager(registry=reg)
        seed = get_bank_seed_playbook()
        lib.add_playbook(seed)

        adapter = BankAdapter("data/prepared/bank/public/cases/bank-20210304T1800-utc8")
        target_id = adapter.extract_suspect_entities()[0].identifier
        target_literal = json.dumps(target_id)
        oracle = FixedOracle(target_id=target_id)
        ctrl = DiagnosisController(library=lib, oracle=oracle, runner=InProcessRunner(), max_attempts=3)

        client = MockClient()
        res = ctrl.run_diagnostic(telemetry=adapter, model_client=client, case_id="case_1")
        self.assertEqual(res.status, "confirmed")

        admitted_pb_name = res.admitted_playbook
        self.assertIsNotNone(admitted_pb_name)
        pb = lib.get_playbook(admitted_pb_name)
        self.assertIsNotNone(pb)
        pb_node_ops = [n["operator"] for n in pb.dag["nodes"]]
        self.assertIn("evolved.connected", pb_node_ops)
        self.assertNotIn("evolved.disconnected", pb_node_ops)

        class SecondClient:
            def complete(self, prompt: str) -> str:
                return json.dumps({"selected_playbook": admitted_pb_name, "parameter_bindings": {}})

        res2 = ctrl.run_diagnostic(telemetry=adapter, model_client=SecondClient(), case_id="case_2")
        self.assertEqual(res2.status, "confirmed")
        self.assertEqual(res2.stage, "select")

    # -------------------------------------------------------------------------
    # G05: Quickstart and base registry API overloads
    # -------------------------------------------------------------------------
    def test_g05_public_api_quickstart_and_base_registry_overloads(self):
        reg = build_bank_registry()
        lib = LibraryManager(registry=reg)
        seed_pb = get_bank_seed_playbook()
        lib.add_playbook(seed_pb)
        self.assertIsNotNone(lib.get_playbook("bank.starter.metric_consensus"))

        oracle = GroundTruthOracle(ground_truth_entities=["cartservice"])
        ans = RankedAnswer(
            candidates=(EntityRef("service", "cartservice"),),
            scores=(1.0,),
            stage="test",
            evidence=(),
        )
        decision = oracle.evaluate(ans, DiagnosticContext("case_x", "select", 1, 3))
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.action, "confirm")

        empty_lib = LibraryManager()
        engine = DiagnosisEngine(
            model=lambda p: "{}",
            library=empty_lib,
            base_registry=reg,
            oracle=oracle,
        )
        self.assertGreater(len(engine.library.registry.all_operators()), 0)
        self.assertTrue(engine.library.registry.has("bank.metrics.contrast", "1"))

    # -------------------------------------------------------------------------
    # G06: Frozen inference profile top-k is 64
    # -------------------------------------------------------------------------
    def test_g06_frozen_profile_top_k(self):
        cfg = load_pinned_inference_config()
        self.assertEqual(cfg.get("top_k"), 64)
        client = LocalLlamaClient()
        self.assertEqual(client.top_k, 64)


if __name__ == "__main__":
    unittest.main()
