"""Closed operator-DAG macro normalization and expansion tests."""

import copy
import unittest

from e4core.cache import CacheStore
from e4core.dag import verify_dag
from e4core.executor import SerialExecutor
from e4core.fixtures import fixture_registry
from e4core.m4_composite import (PROFILE, CompositeSpecError, composite_spec_schema,
    expand_composite_dag, normalize_composite_spec, trusted_aggregate_semantics)
from e4core.registry import OperatorSpec, PortSpec
from e4core.runtime import ParameterSpec
from tests.test_e4core_executor import runtime as fixture_runtime, synthetic_telemetry


def _macro(registry, *, name="evolved.metric_macro", version="1", metric="avg_time"):
    return {"name": name, "version": version, "description": "One trusted aggregate call.",
        "family": "metric", "authoring_profile": PROFILE,
        "input_ports": [{"name": "data", "type": "Telemetry"}],
        "output_type": "Aggregate",
        "parameters": {
            "table": {"type": "string", "required": True},
            "metric_name": {"type": "string", "required": True},
            "window": {"type": "string", "required": True, "choices": ["reference", "alert"]}},
        "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
            "id": "metric_call", "operator": "fixture.aggregate", "version": "1",
            "inputs": {"data": "$input.data"},
            "params": {"table": {"parameter": "table"},
                "metric_name": {"parameter": "metric_name"},
                "window": {"parameter": "window"}}, "bindings": {}}],
            "return": {"node": "metric_call"}}}


def _register_composite(registry, spec):
    ports = tuple(PortSpec(row["name"], row["type"], row["required"], row["variadic"],
        row["minimum_items"], row["maximum_items"]) for row in spec["input_ports"])
    parameters = {name: ParameterSpec(name, row["type"], row["required"], row["nullable"],
        row["default"], row["has_default"], row["runtime_field"], None,
        row["description"], tuple(row["choices"])) for name, row in spec["parameters"].items()}
    registry.register(OperatorSpec(spec["name"], spec["version"], ports, spec["output_type"],
        parameters, family=spec["family"], implementation=None, description=spec["description"]))


class CompositeSpecTests(unittest.TestCase):
    def test_closed_schema_and_program_hash_are_stable_and_source_free(self):
        registry = fixture_registry()
        raw = _macro(registry)
        original = copy.deepcopy(raw)
        one = normalize_composite_spec(raw, registry)
        two = normalize_composite_spec(raw, registry)
        self.assertEqual(raw, original)
        self.assertEqual(one["version"], two["version"])
        self.assertEqual(one["program_sha256"], two["program_sha256"])
        self.assertEqual(one["signature_sha256"], two["signature_sha256"])
        self.assertNotIn("source", one)
        self.assertEqual(one["dependencies"], [])
        changed = copy.deepcopy(raw)
        changed["program"]["nodes"][0]["params"]["table"] = "metric/metric_node.csv"
        self.assertNotEqual(one["version"], normalize_composite_spec(changed, registry)["version"])
        schema = composite_spec_schema()
        self.assertFalse(schema["additional_fields"])
        self.assertEqual(schema["fields"]["authoring_profile"]["const"], PROFILE)

    def test_published_aggregate_semantics_filters_to_registry_signatures(self):
        empty_bank = trusted_aggregate_semantics(fixture_registry())
        self.assertEqual(empty_bank["producers"], {})
        self.assertEqual(empty_bank["consumers"], {})
        from e4core.m4_data import make_m4_registry
        registry = make_m4_registry(None, [])
        semantics = trusted_aggregate_semantics(registry)
        self.assertIn("bank.public.query", semantics["producers"])
        self.assertIn("bank.logs.volume_contrast", semantics["consumers"])
        self.assertTrue(semantics["registered_signatures"]["bank.public.query"])

    def test_compiles_two_macro_calls_to_distinct_primitive_ids_and_real_pipeline(self):
        registry = fixture_registry()
        first = normalize_composite_spec(_macro(registry), registry)
        _register_composite(registry, first)
        rows = [{"id": "data", "operator": "fixture.telemetry", "version": "1",
                 "inputs": {}, "params": {}, "bindings": {}}]
        for call_id, table, metric in (("metrics", "metric/metric_app.csv", "avg_time"),
                                      ("resources", "metric/metric_node.csv", "cpu")):
            rows.append({"id": call_id, "operator": first["name"], "version": first["version"],
                "inputs": {"data": "data"}, "params": {"table": table,
                    "metric_name": metric, "window": "alert"}, "bindings": {}})
        rows.extend([
            {"id": "metric_candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "metrics"}, "params": {}, "bindings": {}},
            {"id": "resource_candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "resources"}, "params": {}, "bindings": {}},
            {"id": "metric_scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "metrics", "candidates": "metric_candidates"}, "params": {}, "bindings": {}},
            {"id": "resource_scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "resources", "candidates": "resource_candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["metric_scores", "resource_scores"]}, "params": {},
             "bindings": {"limit": "runtime.lead_count"}},
        ])
        logical = {"schema_version": "e4-dag/1", "nodes": rows, "return": {"node": "rank"}}
        expanded = expand_composite_dag(logical, [first], registry, fixture_runtime())
        self.assertEqual(expanded.report["logical_node_count"], len(rows))
        self.assertEqual(expanded.report["expanded_node_count"], len(rows))
        metric_inner = expanded.logical_to_expanded["metrics"]["output_node_id"]
        resource_inner = expanded.logical_to_expanded["resources"]["output_node_id"]
        self.assertEqual(metric_inner, "metrics")
        self.assertEqual(resource_inner, "resources")
        self.assertNotEqual(metric_inner, resource_inner)
        self.assertEqual(set(expanded.call_outputs), {"metrics", "resources"})
        self.assertEqual(expanded.node_origins[metric_inner]["primitive_key"], "fixture.aggregate@1")
        verified = verify_dag(expanded.dag, registry, fixture_runtime())
        self.assertEqual(verified.measured_node_count, len(rows))
        result = SerialExecutor(registry, CacheStore("/tmp/e4-composite-test-cache", enabled=False)).execute_document(
            expanded.dag, fixture_runtime(), lambda: {"data": synthetic_telemetry()})
        self.assertEqual([lead.entity.identifier for lead in result.answer.leads], ["os_017", "osb_001"])

    def test_nested_macro_parameter_substitution_and_scoped_paths(self):
        registry = fixture_registry()
        inner = normalize_composite_spec(_macro(registry, name="evolved.inner"), registry)
        _register_composite(registry, inner)
        middle_raw = copy.deepcopy(_macro(registry, name="evolved.middle"))
        middle_raw["program"]["nodes"] = [{"id": "inner_call", "operator": inner["name"],
            "version": inner["version"], "inputs": {"data": "$input.data"},
            "params": {"table": {"parameter": "table"},
                "metric_name": {"parameter": "metric_name"}, "window": {"parameter": "window"}},
            "bindings": {}}]
        middle_raw["program"]["return"] = {"node": "inner_call"}
        middle = normalize_composite_spec(middle_raw, registry, composite_specs=[inner])
        _register_composite(registry, middle)
        outer_raw = copy.deepcopy(_macro(registry, name="evolved.outer"))
        outer_raw["program"]["nodes"] = [{"id": "middle_call", "operator": middle["name"],
            "version": middle["version"], "inputs": {"data": "$input.data"},
            "params": {"table": {"parameter": "table"},
                "metric_name": {"parameter": "metric_name"}, "window": {"parameter": "window"}},
            "bindings": {}}]
        outer_raw["program"]["return"] = {"node": "middle_call"}
        outer = normalize_composite_spec(outer_raw, registry, composite_specs=[inner, middle])
        _register_composite(registry, outer)
        logical = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "fixture.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            {"id": "call", "operator": outer["name"], "version": outer["version"],
             "inputs": {"data": "data"}, "params": {"table": "metric/metric_app.csv",
                "metric_name": "avg_time", "window": "alert"}, "bindings": {}},
            {"id": "candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "call"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "call", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
            "return": {"node": "rank"}}
        expanded = expand_composite_dag(logical, [inner, middle, outer], registry, fixture_runtime())
        self.assertEqual(expanded.call_outputs["call"]["composite"], f"{outer['name']}@{outer['version']}")
        nested_calls = [value for key, value in expanded.call_outputs.items() if key != "call"]
        self.assertEqual({value["composite"] for value in nested_calls},
                         {f"{inner['name']}@{inner['version']}", f"{middle['name']}@{middle['version']}"})
        self.assertTrue(all(len(value["call_path"]) >= 2 for value in nested_calls))
        final_ids = {row["id"] for row in expanded.dag["nodes"]}
        for value in expanded.call_outputs.values():
            self.assertIn(value["output_node_id"], final_ids)
            self.assertTrue(set(value["expanded_node_ids"]) <= final_ids)
        self.assertEqual(expanded.report["primitive_node_count"], len(expanded.dag["nodes"]))

    def test_runtime_bindings_keep_existing_verifier_spelling(self):
        registry = fixture_registry()
        macro = {"name": "evolved.bound_score", "version": "1", "description": "bound trusted scorer",
            "family": "scoring", "authoring_profile": PROFILE,
            "input_ports": [{"name": "aggregate", "type": "Aggregate"},
                {"name": "candidates", "type": "CandidateSet"}],
            "output_type": "ScoredCandidates", "parameters": {},
            "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
                "id": "score", "operator": "fixture.score", "version": "1",
                "inputs": {"aggregate": "$input.aggregate", "candidates": "$input.candidates"},
                "params": {}, "bindings": {"scale": "runtime.relative_scale_floor"}}],
                "return": {"node": "score"}}}
        normalized = normalize_composite_spec(macro, registry, fixture_runtime())
        _register_composite(registry, normalized)
        logical = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "fixture.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            {"id": "aggregate", "operator": "fixture.aggregate", "version": "1",
             "inputs": {"data": "data"}, "params": {"table": "metric/metric_app.csv",
                "metric_name": "avg_time", "window": "alert"}, "bindings": {}},
            {"id": "candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "aggregate"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": normalized["name"], "version": normalized["version"],
             "inputs": {"aggregate": "aggregate", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
            "return": {"node": "rank"}}
        expanded = expand_composite_dag(logical, [normalized], registry, fixture_runtime())
        score_id = expanded.logical_to_expanded["scores"]["output_node_id"]
        score_row = next(row for row in expanded.dag["nodes"] if row["id"] == score_id)
        self.assertEqual(score_row["bindings"]["scale"], "runtime.relative_scale_floor")
        verify_dag(expanded.dag, registry, fixture_runtime())

    def test_variadic_macro_inputs_expand_to_variadic_operator_refs(self):
        registry = fixture_registry()
        registry.register(OperatorSpec("fixture.merge_aggregates", "1",
            (PortSpec("aggregates", "Aggregate", variadic=True, minimum_items=2, maximum_items=4),),
            "Aggregate", implementation=lambda inputs, params, context: inputs["aggregates"][0]))
        macro = {"name": "evolved.aggregate_merge", "version": "1", "description": "merge call",
            "family": "aggregation", "authoring_profile": PROFILE,
            "input_ports": [{"name": "aggregates", "type": "Aggregate", "variadic": True,
                "minimum_items": 2, "maximum_items": 3}], "output_type": "Aggregate", "parameters": {},
            "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
                "id": "merge", "operator": "fixture.merge_aggregates", "version": "1",
                "inputs": {"aggregates": ["$input.aggregates"]}, "params": {}, "bindings": {}}],
                "return": {"node": "merge"}}}
        normalized = normalize_composite_spec(macro, registry)
        _register_composite(registry, normalized)
        def aggregate_node(node_id, table, metric):
            return {"id": node_id, "operator": "fixture.aggregate", "version": "1",
                "inputs": {"data": "data"}, "params": {"table": table,
                    "metric_name": metric, "window": "alert"}, "bindings": {}}
        logical = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "fixture.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            aggregate_node("a", "metric/metric_app.csv", "avg_time"),
            aggregate_node("b", "metric/metric_node.csv", "cpu"),
            {"id": "merged", "operator": normalized["name"], "version": normalized["version"],
             "inputs": {"aggregates": ["a", "b"]}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "merged"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "merged", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
            "return": {"node": "rank"}}
        expanded = expand_composite_dag(logical, [normalized], registry, fixture_runtime())
        merge_id = expanded.logical_to_expanded["merged"]["output_node_id"]
        merge_row = next(row for row in expanded.dag["nodes"] if row["id"] == merge_id)
        self.assertEqual(merge_row["inputs"]["aggregates"], [
            expanded.logical_to_expanded["a"]["output_node_id"],
            expanded.logical_to_expanded["b"]["output_node_id"]])

    def test_rejects_cycles_forward_refs_unknown_calls_and_unreachable_nodes(self):
        registry = fixture_registry()
        raw = _macro(registry)
        raw["program"]["nodes"][0]["inputs"]["data"] = "later"
        raw["program"]["nodes"].append({"id": "later", "operator": "fixture.aggregate",
            "version": "1", "inputs": {"data": "$input.data"}, "params": {
                "table": {"parameter": "table"}, "metric_name": {"parameter": "metric_name"},
                "window": {"parameter": "window"}}, "bindings": {}})
        with self.assertRaises(CompositeSpecError):
            normalize_composite_spec(raw, registry)
        raw = _macro(registry)
        raw["program"]["nodes"].append({"id": "unused", "operator": "fixture.aggregate",
            "version": "1", "inputs": {"data": "$input.data"}, "params": {
                "table": {"parameter": "table"}, "metric_name": {"parameter": "metric_name"},
                "window": {"parameter": "window"}}, "bindings": {}})
        with self.assertRaisesRegex(CompositeSpecError, "do not contribute"):
            normalize_composite_spec(raw, registry)
        raw = _macro(registry)
        raw["program"]["nodes"][0]["operator"] = "unregistered.operator"
        with self.assertRaisesRegex(CompositeSpecError, "not registered"):
            normalize_composite_spec(raw, registry)

    def test_expansion_enforces_primitive_node_and_depth_limits(self):
        registry = fixture_registry()
        registry.register(OperatorSpec("fixture.copy_aggregate", "1",
            (PortSpec("data", "Aggregate"),), "Aggregate",
            implementation=lambda inputs, params, context: inputs["data"]))
        raw = _macro(registry, name="evolved.two_steps")
        raw["program"]["nodes"] = [
            {"id": "first", "operator": "fixture.aggregate", "version": "1",
             "inputs": {"data": "$input.data"}, "params": {
                "table": {"parameter": "table"}, "metric_name": {"parameter": "metric_name"},
                "window": {"parameter": "window"}}, "bindings": {}},
            {"id": "second", "operator": "fixture.copy_aggregate", "version": "1",
             "inputs": {"data": "first"}, "params": {}, "bindings": {}}]
        raw["program"]["return"] = {"node": "second"}
        normalized = normalize_composite_spec(raw, registry)
        _register_composite(registry, normalized)
        logical = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "fixture.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            {"id": "aggregate", "operator": normalized["name"], "version": normalized["version"],
             "inputs": {"data": "data"}, "params": {"table": "metric/metric_app.csv",
                "metric_name": "avg_time", "window": "alert"}, "bindings": {}},
            {"id": "candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "aggregate"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "aggregate", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
            "return": {"node": "rank"}}
        with self.assertRaises(CompositeSpecError) as caught:
            expand_composite_dag(logical, [normalized], registry, fixture_runtime(), max_nodes=5)
        self.assertEqual(caught.exception.code, "node_limit")
        with self.assertRaises(CompositeSpecError) as caught:
            expand_composite_dag(logical, [normalized], registry, fixture_runtime(), max_depth=5)
        self.assertEqual(caught.exception.code, "depth_limit")

    def test_dependency_cycles_in_active_composite_metadata_fail_closed(self):
        registry = fixture_registry()
        inner = normalize_composite_spec(_macro(registry, name="evolved.cycle_a"), registry)
        _register_composite(registry, inner)
        outer_raw = copy.deepcopy(_macro(registry, name="evolved.cycle_b"))
        outer_raw["program"]["nodes"] = [{"id": "nested", "operator": inner["name"],
            "version": inner["version"], "inputs": {"data": "$input.data"},
            "params": {"table": {"parameter": "table"},
                "metric_name": {"parameter": "metric_name"}, "window": {"parameter": "window"}},
            "bindings": {}}]
        outer_raw["program"]["return"] = {"node": "nested"}
        outer = normalize_composite_spec(outer_raw, registry, composite_specs=[inner])
        cyclic_inner, cyclic_outer = copy.deepcopy(inner), copy.deepcopy(outer)
        cyclic_inner["dependencies"] = [f"{outer['name']}@{outer['version']}"]
        with self.assertRaisesRegex(CompositeSpecError, "dependency cycle"):
            expand_composite_dag({"schema_version": "e4-dag/1", "nodes": [],
                "return": {"node": "none"}}, [cyclic_inner, cyclic_outer], registry)

    def test_public_query_fields_are_checked_before_execution_and_project_cannot_reproject(self):
        from e4core.m4_data import make_m4_registry
        registry = make_m4_registry(None, [])
        query_raw = {"name": "evolved.public_query", "version": "1", "description": "query",
            "family": "query", "authoring_profile": PROFILE,
            "input_ports": [{"name": "data", "type": "Telemetry"}], "output_type": "Aggregate",
            "parameters": {}, "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
                "id": "query", "operator": "bank.public.query", "version": "1",
                "inputs": {"data": "$input.data"}, "params": {"table": "metric/metric_container.csv",
                    "window": "alert", "fields": ["missing_column"]}, "bindings": {}}],
                "return": {"node": "query"}}}
        with self.assertRaisesRegex(CompositeSpecError, "absent from"):
            normalize_composite_spec(query_raw, registry)

        query_raw["program"]["nodes"][0]["params"]["fields"] = ["cmdb_id", "timestamp_ms_utc"]
        query = normalize_composite_spec(query_raw, registry)
        project = registry.require("bank.public.project", next(row["version"] for row in registry.signatures()
            if row["name"] == "bank.public.project"))
        project_raw = {"name": "evolved.double_project", "version": "1", "description": "double project",
            "family": "projection", "authoring_profile": PROFILE,
            "input_ports": [{"name": "data", "type": "Telemetry"}], "output_type": "Aggregate",
            "parameters": {}, "program": {"schema_version": "e4-operator-dag/1", "nodes": [
                {"id": "query", "operator": "bank.public.query", "version": "1",
                 "inputs": {"data": "$input.data"}, "params": {"table": "metric/metric_container.csv",
                    "window": "alert", "fields": ["cmdb_id"]}, "bindings": {}},
                {"id": "project1", "operator": project.name, "version": project.version,
                 "inputs": {"data": "query"}, "params": {}, "bindings": {}},
                {"id": "project2", "operator": project.name, "version": project.version,
                 "inputs": {"data": "project1"}, "params": {}, "bindings": {}}],
                "return": {"node": "project2"}}}
        with self.assertRaisesRegex(CompositeSpecError, "cannot project"):
            normalize_composite_spec(project_raw, registry)

        # Projection's source_field values are data labels (including Bank KPI
        # names), not CSV column names; only their shape and frozen size bounds apply.
        project_raw["program"]["nodes"] = [{"id": "project", "operator": project.name,
            "version": project.version, "inputs": {"data": "$input.data"},
            "params": {"source_fields": ["OSLinux-CPU_CPU_0_UTIL"]}, "bindings": {}}]
        project_raw["program"]["return"] = {"node": "project"}
        project_raw["input_ports"] = [{"name": "data", "type": "Aggregate"}]
        project_raw["name"] = "evolved.project_label"
        project = normalize_composite_spec(project_raw, registry)
        self.assertIn("OSLinux-CPU_CPU_0_UTIL", project["program"]["nodes"][0]["params"]["source_fields"])

    def test_call_time_macro_fields_and_top_level_query_fields_are_checked_preexecution(self):
        from e4core.m4_data import make_m4_registry
        m4_registry = make_m4_registry(None, [])
        registry = fixture_registry()
        registry.register(m4_registry.require("bank.public.query", "1"))

        # Direct stage-2 query nodes receive the same catalog validation as macro-expanded calls.
        direct = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "fixture.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            {"id": "query", "operator": "bank.public.query", "version": "1",
             "inputs": {"data": "data"}, "params": {"table": "trace/trace_span.csv",
                "window": "alert", "fields": ["success"]}, "bindings": {}},
            {"id": "candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "query"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "query", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
            "return": {"node": "rank"}}
        with self.assertRaisesRegex(CompositeSpecError, "absent from trace/trace_span.csv"):
            expand_composite_dag(direct, [], registry, fixture_runtime())

        macro = {"name": "evolved.query_fields", "version": "1", "description": "query chosen fields",
            "family": "query", "authoring_profile": PROFILE,
            "input_ports": [{"name": "data", "type": "Telemetry"}], "output_type": "Aggregate",
            "parameters": {"fields": {"type": "string_list", "required": True}},
            "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
                "id": "query", "operator": "bank.public.query", "version": "1",
                "inputs": {"data": "$input.data"}, "params": {"table": "trace/trace_span.csv",
                    "window": "alert", "fields": {"parameter": "fields"}}, "bindings": {}}],
                "return": {"node": "query"}}}
        normalized = normalize_composite_spec(macro, registry)
        _register_composite(registry, normalized)
        expanded_logical = copy.deepcopy(direct)
        expanded_logical["nodes"][1] = {"id": "query", "operator": normalized["name"],
            "version": normalized["version"], "inputs": {"data": "data"},
            "params": {"fields": ["success"]}, "bindings": {}}
        with self.assertRaisesRegex(CompositeSpecError, "absent from trace/trace_span.csv"):
            expand_composite_dag(expanded_logical, [normalized], registry, fixture_runtime())

    def test_bank_contrast_known_wrong_window_role_fails_before_execution(self):
        from e4core.m4_data import make_m4_registry
        registry = make_m4_registry(None, [])
        versions = {row["name"]: row["version"] for row in registry.signatures()}
        def logical(reference_window):
            return {"schema_version": "e4-dag/1", "nodes": [
                {"id": "data", "operator": "case.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
                {"id": "reference", "operator": "bank.metrics.aggregate", "version": versions["bank.metrics.aggregate"],
                 "inputs": {"data": "data"}, "params": {"window": reference_window}, "bindings": {}},
                {"id": "alert", "operator": "bank.metrics.aggregate", "version": versions["bank.metrics.aggregate"],
                 "inputs": {"data": "data"}, "params": {"window": "alert"}, "bindings": {}},
                {"id": "candidates", "operator": "bank.candidates.generate", "version": versions["bank.candidates.generate"],
                 "inputs": {"data": "data"}, "params": {}, "bindings": {}},
                {"id": "scores", "operator": "bank.metrics.contrast", "version": versions["bank.metrics.contrast"],
                 "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
                 "params": {}, "bindings": {}},
                {"id": "answer", "operator": "bank.answers.combine", "version": versions["bank.answers.combine"],
                 "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
                "return": {"node": "answer"}}
        with self.assertRaisesRegex(CompositeSpecError, "requires reference Aggregate"):
            expand_composite_dag(logical("alert"), [], registry, fixture_runtime())
        self.assertTrue(expand_composite_dag(logical("reference"), [], registry,
            fixture_runtime()).report["expanded_node_count"])

    def test_raw_query_pages_and_projected_summaries_cannot_feed_bank_contrasts(self):
        from e4core.m4_data import make_m4_registry
        registry = make_m4_registry(None, [])
        versions = {row["name"]: row["version"] for row in registry.signatures()}

        def doc(producer, scorer, *, project=False):
            nodes = [{"id": "data", "operator": "case.telemetry", "version": "1",
                "inputs": {}, "params": {}, "bindings": {}}]
            aggregate_refs = []
            for side, window in (("reference", "reference"), ("alert", "alert")):
                if producer == "bank.public.query":
                    node = {"id": side, "operator": producer, "version": versions[producer],
                        "inputs": {"data": "data"}, "params": {"table": "log/log_service.csv",
                            "window": window}, "bindings": {}}
                    nodes.append(node)
                    aggregate_refs.append(side)
                else:
                    source_id = side + "_aggregate"
                    nodes.append({"id": source_id, "operator": "bank.metrics.aggregate",
                        "version": versions["bank.metrics.aggregate"], "inputs": {"data": "data"},
                        "params": {"window": window}, "bindings": {}})
                    if project:
                        nodes.append({"id": side, "operator": "bank.public.project",
                            "version": versions["bank.public.project"],
                            "inputs": {"data": source_id}, "params": {}, "bindings": {}})
                        aggregate_refs.append(side)
                    else:
                        aggregate_refs.append(source_id)
            nodes.extend([
                {"id": "candidates", "operator": "bank.candidates.generate",
                 "version": versions["bank.candidates.generate"], "inputs": {"data": "data"},
                 "params": {}, "bindings": {}},
                {"id": "scores", "operator": scorer, "version": versions[scorer],
                 "inputs": {"reference": aggregate_refs[0], "alert": aggregate_refs[1],
                    "candidates": "candidates"}, "params": {}, "bindings": {}},
                {"id": "answer", "operator": "bank.answers.combine",
                 "version": versions["bank.answers.combine"], "inputs": {"scores": ["scores"]},
                 "params": {}, "bindings": {"limit": "runtime.lead_count"}}])
            return {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "answer"}}

        cases = [("bank.public.query", "bank.logs.volume_contrast", False,
                  "bank_public_raw_page"),
                 ("bank.public.project", "bank.logs.volume_contrast", True,
                  "bank_projected_scalar_summaries")]
        for producer, consumer, projected, actual_capability in cases:
            with self.subTest(producer=producer):
                with self.assertRaises(CompositeSpecError) as caught:
                    expand_composite_dag(doc(producer, consumer, project=projected), [],
                        registry, fixture_runtime())
                error = caught.exception
                self.assertEqual(error.code, "aggregate_capability_mismatch")
                self.assertEqual(error.details["consumer"].split("@")[0], consumer)
                self.assertEqual(error.details["actual_capability"], actual_capability)
                self.assertIn("expected_capability", error.details)

        # The same incompatibility is visible while normalizing a proposed
        # composite with direct query->contrast wiring, before a serving call.
        query_score = {"name": "evolved.raw_log_score", "version": "1",
            "description": "Raw log page scorer", "family": "bank_log_volume",
            "authoring_profile": PROFILE,
            "input_ports": [{"name": "data", "type": "Telemetry"}],
            "output_type": "ScoredCandidates", "parameters": {},
            "program": {"schema_version": "e4-operator-dag/1", "nodes": [
                {"id": "reference_page", "operator": "bank.public.query", "version": "1",
                 "inputs": {"data": "$input.data"}, "params": {"table": "log/log_service.csv",
                    "window": "reference"}, "bindings": {}},
                {"id": "alert_page", "operator": "bank.public.query", "version": "1",
                 "inputs": {"data": "$input.data"}, "params": {"table": "log/log_service.csv",
                    "window": "alert"}, "bindings": {}},
                {"id": "candidates", "operator": "bank.candidates.generate",
                 "version": "1", "inputs": {"data": "$input.data"}, "params": {}, "bindings": {}},
                {"id": "scores", "operator": "bank.logs.volume_contrast",
                 "version": versions["bank.logs.volume_contrast"], "inputs": {
                    "reference": "reference_page", "alert": "alert_page", "candidates": "candidates"},
                 "params": {}, "bindings": {}}], "return": {"node": "scores"}}}
        with self.assertRaises(CompositeSpecError) as caught:
            normalize_composite_spec(query_score, registry)
        self.assertEqual(caught.exception.code, "aggregate_capability_mismatch")
        self.assertEqual(caught.exception.details["actual_capability"], "bank_public_raw_page")

    def test_full_window_summary_producers_are_valid_directly_and_inside_macros(self):
        from e4core.m4_data import make_m4_registry
        registry = make_m4_registry(None, [])
        versions = {row["name"]: row["version"] for row in registry.signatures()}
        runtime = fixture_runtime()

        def direct(producer, consumer):
            nodes = [{"id": "data", "operator": "case.telemetry", "version": "1",
                "inputs": {}, "params": {}, "bindings": {}}]
            for side, window in (("reference", "reference"), ("alert", "alert")):
                nodes.append({"id": side, "operator": producer, "version": versions[producer],
                    "inputs": {"data": "data"}, "params": {"window": window}, "bindings": {}})
            nodes.extend([
                {"id": "candidates", "operator": "bank.candidates.generate",
                 "version": versions["bank.candidates.generate"], "inputs": {"data": "data"},
                 "params": {}, "bindings": {}},
                {"id": "scores", "operator": consumer, "version": versions[consumer],
                 "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
                 "params": {}, "bindings": {}},
                {"id": "answer", "operator": "bank.answers.combine",
                 "version": versions["bank.answers.combine"], "inputs": {"scores": ["scores"]},
                 "params": {}, "bindings": {"limit": "runtime.lead_count"}}])
            return {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "answer"}}

        metric_expanded = expand_composite_dag(direct("bank.metrics.aggregate",
            "bank.logs.volume_contrast"), [], registry, runtime)
        trace_expanded = expand_composite_dag(direct("bank.traces.aggregate",
            "bank.traces.latency_contrast"), [], registry, runtime)
        self.assertGreater(metric_expanded.report["expanded_node_count"], 0)
        self.assertGreater(trace_expanded.report["expanded_node_count"], 0)

        macro = {"name": "evolved.log_summaries", "version": "1",
            "description": "Use the registered full-window Bank metric summary producer.",
            "family": "bank_log_volume", "authoring_profile": PROFILE,
            "input_ports": [{"name": "data", "type": "Telemetry"}],
            "output_type": "ScoredCandidates", "parameters": {},
            "program": {"schema_version": "e4-operator-dag/1", "nodes": [
                {"id": "reference", "operator": "bank.metrics.aggregate",
                 "version": versions["bank.metrics.aggregate"], "inputs": {"data": "$input.data"},
                 "params": {"window": "reference"}, "bindings": {}},
                {"id": "alert", "operator": "bank.metrics.aggregate",
                 "version": versions["bank.metrics.aggregate"], "inputs": {"data": "$input.data"},
                 "params": {"window": "alert"}, "bindings": {}},
                {"id": "candidates", "operator": "bank.candidates.generate",
                 "version": versions["bank.candidates.generate"], "inputs": {"data": "$input.data"},
                 "params": {}, "bindings": {}},
                {"id": "scores", "operator": "bank.logs.volume_contrast",
                 "version": versions["bank.logs.volume_contrast"], "inputs": {
                     "reference": "reference", "alert": "alert", "candidates": "candidates"},
                 "params": {}, "bindings": {}}], "return": {"node": "scores"}}}
        normalized = normalize_composite_spec(macro, registry, runtime)
        _register_composite(registry, normalized)
        logical = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "case.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            {"id": "macro_scores", "operator": normalized["name"], "version": normalized["version"],
             "inputs": {"data": "data"}, "params": {}, "bindings": {}},
            {"id": "answer", "operator": "bank.answers.combine",
             "version": versions["bank.answers.combine"], "inputs": {"scores": ["macro_scores"]},
             "params": {}, "bindings": {"limit": "runtime.lead_count"}}],
            "return": {"node": "answer"}}
        macro_expanded = expand_composite_dag(logical, [normalized], registry, runtime)
        self.assertEqual(macro_expanded.report["macro_call_count"], 1)


if __name__ == "__main__":
    unittest.main()
