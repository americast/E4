"""Capability checks for trusted paper C4 CandidateSet provenance."""

from __future__ import annotations

import unittest
import json

from e4core.dag import verify_dag
from e4core.m4_capabilities import (
    InputCapabilityError, check_typed_input_capabilities,
    input_producer_capability_schema,
)
from e4core.m4_generated import register_generated_specs, validate_generated_spec
from e4core.m4_operator_ir import normalize_operator_ir_spec
from e4core.paper_starter import make_paper_starter_registry
from tests.test_m4_operator_ir import _spec as typed_ir_fixture_spec


def _capability_dag(*, include_untrusted_candidate_passthrough: bool):
    registry = make_paper_starter_registry(bank_only=True)
    raw_typed_spec = typed_ir_fixture_spec("variance_change")
    typed_spec = normalize_operator_ir_spec(raw_typed_spec)
    generated_specs = [typed_spec]
    if include_untrusted_candidate_passthrough:
        generated_specs.insert(0, validate_generated_spec({
            "name": "evolved.untrusted_candidate_passthrough", "version": "1",
            "description": "Opaque generated CandidateSet pass-through fixture.",
            "family": "fixture", "input_ports": [
                {"name": "data", "type": "CandidateSet"}],
            "output_type": "CandidateSet", "parameters": {},
            "source": "def operator(inputs, params, runtime):\n    return inputs['data']\n",
        }))
    normalized_specs = register_generated_specs(registry, generated_specs)
    typed = next(row for row in normalized_specs
        if row.get("authoring_profile") == "typed-operator-ir-v1")
    aggregate_version = next(row["version"] for row in registry.signatures()
        if row["name"] == "bank.metrics.aggregate")
    nodes = [
        {"id": "telemetry", "operator": "case.telemetry", "version": "1",
         "inputs": {}, "params": {}, "bindings": {}},
        {"id": "starter", "operator": "paper.input.prepare", "version": "1",
         "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
        {"id": "c4_candidates", "operator": "paper.backend.metric_conditions", "version": "1",
         "inputs": {"data": "starter"}, "params": {}, "bindings": {}},
        {"id": "reference", "operator": "bank.metrics.aggregate", "version": aggregate_version,
         "inputs": {"data": "telemetry"}, "params": {"window": "reference"}, "bindings": {}},
        {"id": "alert", "operator": "bank.metrics.aggregate", "version": aggregate_version,
         "inputs": {"data": "telemetry"}, "params": {"window": "alert"}, "bindings": {}},
    ]
    if include_untrusted_candidate_passthrough:
        passthrough = next(row for row in normalized_specs
            if row["name"] == "evolved.untrusted_candidate_passthrough")
        nodes.append({"id": "untrusted_candidates", "operator": passthrough["name"],
            "version": passthrough["version"], "inputs": {"data": "c4_candidates"},
            "params": {}, "bindings": {}})
        nodes.extend([
            {"id": "trusted_ir", "operator": typed["name"], "version": typed["version"],
             "inputs": {"reference": "reference", "alert": "alert",
                        "candidates": "c4_candidates"}, "params": {}, "bindings": {}},
            {"id": "untrusted_ir", "operator": typed["name"], "version": typed["version"],
             "inputs": {"reference": "reference", "alert": "alert",
                        "candidates": "untrusted_candidates"}, "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
             "inputs": {"scores": ["trusted_ir", "untrusted_ir"]},
             "params": {}, "bindings": {}},
        ])
    else:
        nodes.extend([
            {"id": "closed_ir", "operator": typed["name"], "version": typed["version"],
             "inputs": {"reference": "reference", "alert": "alert",
                        "candidates": "c4_candidates"}, "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
             "inputs": {"scores": ["closed_ir"]}, "params": {}, "bindings": {}},
        ])
    document = {"schema_version": "e4-dag/1", "nodes": nodes,
        "return": {"node": "answer"}}
    verified = verify_dag(document, registry, runtime=None, max_nodes=40, max_depth=12)
    return registry, verified, normalized_specs, typed


def _v2_grouping_dag():
    from tests.test_m4_operator_ir_v2 import _program, _spec

    registry = make_paper_starter_registry(bank_only=True)
    authored = normalize_operator_ir_spec(_spec(_program(with_kpi=True)))
    typed = register_generated_specs(registry, [authored])[0]
    aggregate_version = next(row["version"] for row in registry.signatures()
        if row["name"] == "bank.metrics.aggregate")
    nodes = [
        {"id": "telemetry", "operator": "case.telemetry", "version": "1",
            "inputs": {}, "params": {}, "bindings": {}},
        {"id": "starter", "operator": "paper.input.prepare", "version": "1",
            "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
        {"id": "candidates", "operator": "paper.backend.metric_conditions", "version": "1",
            "inputs": {"data": "starter"}, "params": {}, "bindings": {}},
        {"id": "reference", "operator": "bank.metrics.aggregate", "version": aggregate_version,
            "inputs": {"data": "telemetry"}, "params": {"window": "reference"}, "bindings": {}},
        {"id": "alert", "operator": "bank.metrics.aggregate", "version": aggregate_version,
            "inputs": {"data": "telemetry"}, "params": {"window": "alert"}, "bindings": {}},
        {"id": "typed", "operator": typed["name"], "version": typed["version"],
            "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
            "params": {}, "bindings": {}},
        {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
            "inputs": {"scores": ["typed"]}, "params": {}, "bindings": {}},
    ]
    verified = verify_dag({"schema_version": "e4-dag/1", "nodes": nodes,
        "return": {"node": "answer"}}, registry, runtime=None, max_nodes=40, max_depth=12)
    return verified, typed


class PaperC4CandidateCapabilityTests(unittest.TestCase):
    def test_v2_component_plus_kpi_grouping_checks_each_real_aggregate_table(self):
        from e4core.m4_operator_ir import PROFILE_V2
        from e4core.dag import verify_dag
        from tests.test_m4_operator_ir_v2 import _program, _spec

        registry = make_paper_starter_registry(bank_only=True)
        typed_spec = normalize_operator_ir_spec(_spec(_program(with_kpi=True)))
        registered = register_generated_specs(registry, [typed_spec])[0]
        aggregate_version = next(row["version"] for row in registry.signatures()
            if row["name"] == "bank.metrics.aggregate")
        nodes = [
            {"id": "telemetry", "operator": "case.telemetry", "version": "1",
                "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter", "operator": "paper.input.prepare", "version": "1",
                "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "paper.backend.metric_conditions", "version": "1",
                "inputs": {"data": "starter"}, "params": {}, "bindings": {}},
            {"id": "reference", "operator": "bank.metrics.aggregate", "version": aggregate_version,
                "inputs": {"data": "telemetry"}, "params": {"window": "reference"}, "bindings": {}},
            {"id": "alert", "operator": "bank.metrics.aggregate", "version": aggregate_version,
                "inputs": {"data": "telemetry"}, "params": {"window": "alert"}, "bindings": {}},
            {"id": "typed", "operator": registered["name"], "version": registered["version"],
                "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
                "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
                "inputs": {"scores": ["typed"]}, "params": {}, "bindings": {}},
        ]
        dag = verify_dag({"schema_version": "e4-dag/1", "nodes": nodes,
            "return": {"node": "answer"}}, registry, runtime=None, max_nodes=40, max_depth=12)
        report = check_typed_input_capabilities(dag, [registered])
        by_port = {row["port"]: row for row in report if row["node_id"] == "typed"}
        self.assertEqual(by_port["reference"]["status"], "verified")
        self.assertEqual(by_port["alert"]["status"], "verified")
        self.assertEqual(by_port["reference"]["available_group_by_by_table"], {
            "metric/metric_container.csv": ["bomc_id", "component", "item_id", "source_field"],
            "metric/metric_app.csv": ["app_kpi", "bomc_id", "item_id", "source_field"]})
        self.assertEqual(registered["authoring_profile"], PROFILE_V2)

        wrong = {**registered, "input_requirements": json.loads(
            json.dumps(registered["input_requirements"]))}
        wrong["input_requirements"]["reference"]["group_by_by_table"][
            "metric/metric_app.csv"] = ["component"]
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(dag, [wrong])
        self.assertEqual(raised.exception.code, "input_grouping_unavailable")
        self.assertEqual(raised.exception.details["table"], "metric/metric_app.csv")

    def test_capability_schema_describes_c4_as_exact_observed_components(self):
        registry, _dag, _specs, _typed = _capability_dag(
            include_untrusted_candidate_passthrough=False)
        schema = input_producer_capability_schema(registry)
        producer = next(row for row in schema["producers"]
            if row["name"] == "paper.backend.metric_conditions")
        self.assertEqual(producer["output_type"], "CandidateSet")
        self.assertEqual(producer["source_columns"], {})
        self.assertEqual(producer["group_by"], ["component"])
        self.assertIsNone(producer["support_unit"])
        self.assertEqual(producer["coverage"], "full_window")
        self.assertIn("exact observed component IDs", producer["provenance"])

    def test_closed_typed_ir_accepts_exact_trusted_c4_candidate_producer(self):
        _registry, dag, specs, typed = _capability_dag(
            include_untrusted_candidate_passthrough=False)
        reports = check_typed_input_capabilities(dag, [typed])
        candidates = next(row for row in reports
            if row["node_id"] == "closed_ir" and row["port"] == "candidates")
        self.assertEqual(candidates["status"], "verified")
        self.assertEqual(candidates["producer"], "paper.backend.metric_conditions@1")
        self.assertEqual(candidates["coverage"], "full_window")
        self.assertEqual(candidates["declared_requirements"]["group_by"], ["component"])

    def test_generated_candidate_set_passthrough_does_not_inherit_c4_trust(self):
        _registry, dag, _specs, typed = _capability_dag(
            include_untrusted_candidate_passthrough=True)
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(dag, [typed])
        self.assertEqual(raised.exception.code, "input_capability_unverified")
        self.assertEqual(raised.exception.details["producer_nodes"], ["untrusted_candidates"])

    def test_v2_component_and_kpi_requirements_check_each_real_aggregate_table(self):
        verified, typed = _v2_grouping_dag()
        report = check_typed_input_capabilities(verified, [typed])
        by_port = {row["port"]: row for row in report if row["node_id"] == "typed"}
        expected = {"metric/metric_container.csv": ["component"],
            "metric/metric_app.csv": ["app_kpi"]}
        for port in ("reference", "alert"):
            self.assertEqual(by_port[port]["status"], "verified")
            self.assertEqual(by_port[port]["declared_requirements"]["group_by_by_table"], expected)
            self.assertEqual(by_port[port]["available_group_by_by_table"], {
                "metric/metric_container.csv": ["bomc_id", "component", "item_id", "source_field"],
                "metric/metric_app.csv": ["app_kpi", "bomc_id", "item_id", "source_field"]})

    def test_v2_wrong_per_table_group_is_rejected_even_with_other_table_support(self):
        verified, typed = _v2_grouping_dag()
        wrong = {**typed, "input_requirements": json.loads(json.dumps(typed["input_requirements"]))}
        wrong["input_requirements"]["reference"]["group_by_by_table"][
            "metric/metric_app.csv"] = ["component"]
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(verified, [wrong])
        self.assertEqual(raised.exception.code, "input_grouping_unavailable")
        self.assertEqual(raised.exception.details["table"], "metric/metric_app.csv")


if __name__ == "__main__":
    unittest.main()
