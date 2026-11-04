"""Unit tests for e4 DAG execution, AST validator, typechecker, and subprocess runner."""

import unittest

from e4.dag import DAGValidationError, SerialExecutor, verify_dag
from e4.registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec
from e4.sandbox.ast_validator import SecurityValidationError, validate_source
from e4.sandbox.runner import InProcessRunner, SubprocessRunner
from e4.sandbox.typechecker import TypeCheckError, check_operator_types
from e4.sandbox.views import (
    Aggregate,
    EntityRef,
    MetricRecord,
    TelemetryView,
    Window,
    make_aggregate,
    make_aggregate_row,
    make_candidate,
    make_candidate_set,
    make_scored_candidate,
    make_scored_candidates,
)
from e4.types import RankedAnswer


class TestDAGAndSandbox(unittest.TestCase):
    def test_ast_validator_accepts_clean_python(self):
        code = (
            "def operator(inputs, params, runtime):\n"
            "    import math\n"
            "    alpha = params.get('alpha', 0.5)\n"
            "    return make_candidate_set([], source='clean_op')\n"
        )
        validate_source(code)

    def test_ast_validator_rejects_forbidden_imports(self):
        code = (
            "def operator(inputs, params, runtime):\n"
            "    import os\n"
            "    return make_candidate_set([], source='bad')\n"
        )
        with self.assertRaises(SecurityValidationError):
            validate_source(code)

    def test_ast_validator_rejects_eval_and_exec(self):
        code = (
            "def operator(inputs, params, runtime):\n"
            "    eval('1 + 1')\n"
            "    return make_candidate_set([], source='bad')\n"
        )
        with self.assertRaises(SecurityValidationError):
            validate_source(code)

    def test_dag_verification_and_serial_executor(self):
        registry = OperatorRegistry()

        def op_a_impl(inputs, params, runtime):
            return {"count": 42}

        def op_b_impl(inputs, params, runtime):
            return RankedAnswer(
                candidates=(EntityRef("service", "billing-service"),),
                scores=(0.95,),
                stage="test",
            )

        registry.register(
            OperatorSpec(
                name="test.op_a",
                version="1",
                inputs=(),
                output_type="Mapping",
                implementation=op_a_impl,
            )
        )
        registry.register(
            OperatorSpec(
                name="test.op_b",
                version="1",
                inputs=(PortSpec(name="data", value_type="Mapping"),),
                output_type="RankedAnswer",
                implementation=op_b_impl,
            )
        )

        doc = {
            "nodes": [
                {"id": "node_1", "operator": "test.op_a", "version": "1"},
                {"id": "node_2", "operator": "test.op_b", "version": "1", "inputs": {"data": "node_1"}},
            ],
            "return_node": "node_2",
        }

        verified = verify_dag(doc, registry)
        self.assertEqual(len(verified.nodes), 2)
        self.assertEqual(verified.return_node, "node_2")

        executor = SerialExecutor(registry)
        result = executor.execute_verified(verified, runtime=None)
        self.assertIsInstance(result.return_value, RankedAnswer)
        self.assertEqual(result.return_value.top_candidate.identifier, "billing-service")

    def test_typechecker_and_subprocess_runner(self):
        valid_spec = {
            "name": "evolved.metric_anomaly",
            "version": "1",
            "authoring_profile": "typed-operator-code-v1",
            "description": "Calculates metric anomaly contrast",
            "family": "metric",
            "input_ports": [
                {"name": "data", "type": "Telemetry", "required": True, "variadic": False}
            ],
            "output_type": "Aggregate",
            "parameters": {
                "threshold": {"type": "number", "default": 2.5}
            },
            "source": (
                "def operator(inputs, params, runtime):\n"
                "    tel = telemetry_view(inputs['data'])\n"
                "    thresh = params.get('threshold', 2.5)\n"
                "    w = tel.alert_window\n"
                "    return make_aggregate(w, [])\n"
            ),
        }

        # Check types with Mypy
        check_result = check_operator_types(valid_spec)
        self.assertEqual(check_result["status"], "passed")

        # Execute in SubprocessRunner
        runner = SubprocessRunner(timeout_seconds=5)
        spec = OperatorSpec(
            name=valid_spec["name"],
            version=valid_spec["version"],
            inputs=(PortSpec(name="data", value_type="Telemetry"),),
            output_type="Aggregate",
            authoring_profile="typed-operator-code-v1",
            source=valid_spec["source"],
        )

        ref_w = Window("reference", 1000, 2000)
        alt_w = Window("alert", 2000, 3000)
        tel_view = TelemetryView(
            case_id="case_01",
            reference_window=ref_w,
            alert_window=alt_w,
            records=[
                MetricRecord("metrics.csv", 2500, EntityRef("service", "auth"), "cpu", 88.0)
            ],
        )

        res = runner.run(spec, inputs={"data": tel_view}, params={"threshold": 2.0}, runtime=None)
        self.assertIsInstance(res, Aggregate)
        self.assertEqual(res.window.name, "alert")


if __name__ == "__main__":
    unittest.main()
