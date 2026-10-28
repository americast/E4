import json
import tempfile
import unittest
from pathlib import Path

from e4.contracts import (Candidate, CandidateSet, EntityRef, Evidence,
                          RankedAnswer, ScoredCandidate, ScoredCandidates,
                          Window)
from e4.dsl.verifier import DAGValidationError, verify_dag
from e4.dsl.registry import OperatorRegistry, OperatorSpec, PortSpec
from e4.dsl.runtime import (ParameterSpec, RuntimeContext, RuntimeField,
                            RuntimeResolutionError, RuntimeSchema,
                            build_case_runtime, resolve_parameters)


def fixture_registry():
    registry = OperatorRegistry()
    def add(name, inputs, output, family="utility", parameters=None):
        registry.register(OperatorSpec(name, "1", tuple(inputs), output,
                                       parameters or {}, family=family,
                                       implementation=lambda i, p, r: None))
    add("input.telemetry", [], "Telemetry")
    add("aggregate.metric", [PortSpec("data", "Telemetry")], "Aggregate")
    add("aggregate.trace", [PortSpec("data", "Telemetry")], "Aggregate")
    add("candidate.from_aggregate", [PortSpec("data", "Aggregate")], "CandidateSet")
    add("candidate.pass", [PortSpec("data", "CandidateSet")], "CandidateSet")
    add("score.metric", [PortSpec("data", "Telemetry"), PortSpec("candidates", "CandidateSet")], "ScoredCandidates", family="anything",
        parameters={"threshold": ParameterSpec("threshold", "number")})
    add("score.adjust", [PortSpec("scores", "ScoredCandidates")], "ScoredCandidates")
    add("rank.merge", [PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1)], "RankedAnswer")
    return registry


def node(identifier, operator, inputs=None, params=None, bindings=None):
    return {"id": identifier, "operator": operator, "version": "1",
            "inputs": inputs or {}, "params": params or {}, "bindings": bindings or {}}


def valid_dag():
    return {"schema_version": "e4-dag/1", "nodes": [
        node("data", "input.telemetry"),
        node("am", "aggregate.metric", {"data": "data"}),
        node("at", "aggregate.trace", {"data": "data"}),
        node("cm", "candidate.from_aggregate", {"data": "am"}),
        node("ct", "candidate.from_aggregate", {"data": "at"}),
        node("sm", "score.metric", {"data": "data", "candidates": "cm"}),
        node("st", "score.metric", {"data": "data", "candidates": "ct"}),
        node("adjusted", "score.adjust", {"scores": "sm"}),
        node("rank", "rank.merge", {"scores": ["adjusted", "st"]}),
    ], "return": {"node": "rank"}}


class RuntimeContractTests(unittest.TestCase):
    def test_runtime_fields_are_typed_and_immutable(self):
        schema = RuntimeSchema({
            "dag_depth": RuntimeField("dag_depth", "positive_integer", "depth limit", "run", required=True),
            "minimum_support": RuntimeField("minimum_support", "positive_integer", "support", "operator", default=8, has_default=True),
        }, "test/1")
        context = RuntimeContext.build(schema, {"dag_depth": 9})
        self.assertEqual(context.snapshot.values["dag_depth"], 9)
        with self.assertRaises(TypeError):
            context.snapshot.values["dag_depth"] = 5
        with self.assertRaises(RuntimeResolutionError) as error:
            RuntimeContext.build(schema, {})
        self.assertEqual(error.exception.code, "missing_required_runtime_field")
        with self.assertRaises(RuntimeResolutionError):
            RuntimeContext.build(schema, {"dag_depth": True})

    def test_tail_quantile_one_is_rejected_before_operator_execution(self):
        with self.assertRaisesRegex(RuntimeResolutionError, "outside"):
            build_case_runtime({"analysis_window_start_epoch_ms_utc": 0,
                                "alert_window_start_epoch_ms_utc": 1,
                                "alert_window_end_epoch_ms_utc": 2},
                               overrides={"tail_quantile": 1.0})

    def test_parameter_precedence_provenance_auto_and_required_missing(self):
        schema = RuntimeSchema({
            "dag_depth": RuntimeField("dag_depth", "positive_integer", "limit", "run", default=9, has_default=True),
            "support": RuntimeField("support", "positive_integer", "support", "operator", default=12, has_default=True),
            "unavailable": RuntimeField("unavailable", "number", "not provided", "operator"),
        }, "test/1")
        context = RuntimeContext.build(schema, {})
        specs = {"limit": ParameterSpec("limit", "positive_integer", runtime_field="dag_depth",
                                         default=3, has_default=True),
                 "support": ParameterSpec("support", "positive_integer", estimator="median_reference",
                                          description="reference only")}
        resolved = resolve_parameters(specs, {"limit": 4}, {"limit": "runtime.dag_depth"}, context)
        self.assertEqual(resolved.values["limit"], 4)
        self.assertEqual(resolved.provenance["limit"], "explicit_argument")
        bound = resolve_parameters(specs, {}, {"limit": "runtime.dag_depth"}, context)
        self.assertEqual(bound.values["limit"], 9)
        auto = resolve_parameters({"support": specs["support"]}, {"support": "auto"}, {}, context,
                                  {"median_reference": lambda reference: sorted(reference)[len(reference)//2]},
                                  reference_data=[1, 7, 3])
        self.assertEqual(auto.values["support"], 3)
        required = {"x": ParameterSpec("x", "number", required=True, runtime_field="unavailable")}
        with self.assertRaises(RuntimeResolutionError) as error:
            resolve_parameters(required, {}, {}, context)
        self.assertEqual(error.exception.code, "missing_required_runtime_field")
        with self.assertRaises(RuntimeResolutionError):
            resolve_parameters({"x": ParameterSpec("x", "number", required=True)}, {"x": None}, {}, context)

    def test_frozen_contracts_reject_duplicate_canonical_answer_ids(self):
        ev = Evidence("metric", Window("alert", 10, 20), "median_delta", 1.0, (), 3,
                      details={"rows": [1, 2]})
        with self.assertRaises(TypeError):
            ev.details["rows"][0] = 9
        leads = (ScoredCandidate(EntityRef("component", "OSB_001"), 1.0, None, ()),
                 ScoredCandidate(EntityRef("service", "osb_001 "), 0.5, None, ()))
        with self.assertRaisesRegex(ValueError, "duplicate entity leads"):
            RankedAnswer(leads)
        tied_out_of_order = (ScoredCandidate(EntityRef("component", "z"), 1.0, None, ()),
                             ScoredCandidate(EntityRef("component", "a"), 1.0, None, ()))
        with self.assertRaisesRegex(ValueError, "tie order"):
            RankedAnswer(tied_out_of_order)
        with self.assertRaisesRegex(ValueError, "integer UTC milliseconds"):
            Window("alert", True, 20)


class DAGVerifierTests(unittest.TestCase):
    def setUp(self):
        schema = RuntimeSchema({"dag_depth": RuntimeField("dag_depth", "positive_integer", "limit", "run", default=12, has_default=True),
                                "threshold": RuntimeField("threshold", "number", "threshold", "operator")}, "test/1")
        self.runtime = RuntimeContext.build(schema, {})
        self.registry = fixture_registry()

    def test_valid_branching_shared_data_and_intermediate_score_sink(self):
        verified = verify_dag(valid_dag(), self.registry, self.runtime, max_nodes=40, max_depth=12)
        self.assertEqual(verified.return_node, "rank")
        self.assertEqual(verified.measured_node_count, 9)
        self.assertEqual(verified.max_depth, 6)
        self.assertEqual(verified.nodes[5].inputs["candidates"], ("cm",))

    def test_rejects_invalid_bindings_edges_and_candidate_use(self):
        dag = valid_dag()
        dag["nodes"][5]["bindings"] = {"nonexistent": "runtime.dag_depth"}
        with self.assertRaisesRegex(DAGValidationError, "unsupported parameter bindings"):
            verify_dag(dag, self.registry, self.runtime)
        dag = valid_dag()
        dag["nodes"][5]["params"] = {"threshold": 0.5}
        dag["nodes"][5]["bindings"] = {"threshold": "runtime.threshold"}
        # Explicit wins, so missing-but-declared runtime.threshold does not block this graph.
        verified = verify_dag(dag, self.registry, self.runtime)
        self.assertEqual(verified.nodes[5].params["threshold"], 0.5)
        dag["nodes"][5]["bindings"] = {"threshold": "runtime.not_declared"}
        with self.assertRaisesRegex(DAGValidationError, "undeclared"):
            verify_dag(dag, self.registry, self.runtime)
        dag = valid_dag()
        dag["nodes"][7]["inputs"] = {"scores": "st"}
        # This leaves sm as an unused score output.
        with self.assertRaises(DAGValidationError) as error:
            verify_dag(dag, self.registry, self.runtime)
        self.assertIn(error.exception.code, {"unused_binding", "scored_sink_not_returned"})

    def test_rejects_forward_refs_wrong_types_unused_nodes_and_limits(self):
        dag = valid_dag()
        dag["nodes"][1]["inputs"]["data"] = "cm"
        with self.assertRaisesRegex(DAGValidationError, "forward"):
            verify_dag(dag, self.registry, self.runtime)
        dag = valid_dag()
        dag["nodes"][5]["inputs"]["candidates"] = "am"
        with self.assertRaisesRegex(DAGValidationError, "expects CandidateSet"):
            verify_dag(dag, self.registry, self.runtime)
        dag = valid_dag()
        dag["nodes"].insert(1, node("unused", "aggregate.metric", {"data": "data"}))
        with self.assertRaisesRegex(DAGValidationError, "unused"):
            verify_dag(dag, self.registry, self.runtime)
        with self.assertRaisesRegex(DAGValidationError, "depth"):
            verify_dag(valid_dag(), self.registry, self.runtime, max_depth=4)
        with self.assertRaisesRegex(DAGValidationError, "maximum"):
            verify_dag(valid_dag(), self.registry, self.runtime, max_nodes=8)

    def test_each_candidate_set_must_be_directly_scored_and_return_is_terminal(self):
        dag = valid_dag()
        # Candidate generation can consume an earlier candidate set, but each
        # candidate binding also needs a direct scoring consumer.
        dag["nodes"].insert(5, node("cm2", "candidate.pass", {"data": "cm"}))
        dag["nodes"][6]["inputs"]["candidates"] = "cm2"
        with self.assertRaisesRegex(DAGValidationError, "not directly consumed"):
            verify_dag(dag, self.registry, self.runtime)
        dag = valid_dag()
        dag["return"]["extra"] = "anything"
        with self.assertRaisesRegex(DAGValidationError, "exactly"):
            verify_dag(dag, self.registry, self.runtime)


if __name__ == "__main__":
    unittest.main()
