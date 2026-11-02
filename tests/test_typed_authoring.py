from __future__ import annotations

import json
import shutil
import unittest
import os
import json
from pathlib import Path

from e4core.m4_typecheck import (PROFILE, TypedAuthoringError,
                                 check_generated_spec_types)
from e4core.cache import deserialize_contract_json, _encode
from e4core.contracts import Aggregate, AggregateRow, EntityRef, Window
from e4core.sandbox import run_generated_operator
from e4core.sandbox_worker import (aggregate_view, make_aggregate, make_aggregate_row,
    make_candidate, make_candidate_set, make_entity_ref, make_window, validate_source)
from e4core.sandbox_worker import typed_record_helper_schema


def _spec(source: str, output_type: str = "ScoredCandidates"):
    return {"name": "evolved.typed_test", "version": "1", "declared_version": "1",
        "description": "typed test", "family": "test", "authoring_profile": PROFILE,
        "input_ports": [{"name": "data", "type": "Aggregate", "required": True,
                         "variadic": False, "minimum_items": 1, "maximum_items": None}],
        "output_type": output_type, "parameters": {}, "source": source}


class TypedAuthoringTests(unittest.TestCase):
    def test_typed_operator_and_annotated_nested_helper_pass(self):
        source = '''def operator(inputs, params, runtime):
    data = aggregate_view(inputs['data'])
    def row_count(rows: Sequence[AggregateRowView]) -> int:
        return len(rows)
    n = row_count(data.rows)
    return make_scored_candidates([], 'typed', status_details={'rows': n})
'''
        report = check_generated_spec_types(_spec(source))
        self.assertEqual(report["status"], "passed")
        self.assertFalse(report["cache_hit"])
        second = check_generated_spec_types(_spec(source))
        self.assertTrue(second["cache_hit"])

    def test_checker_overrides_model_top_level_annotations_without_moving_body_lines(self):
        source = '''# café annotation line
def operator(
    inputs: str,
    params: str,
    runtime: str,
) -> int:
    return make_scored_candidates([], 'typed')
'''
        report = check_generated_spec_types(_spec(source))
        self.assertEqual(report["status"], "passed")

    def test_published_record_aliases_work_only_in_annotations(self):
        annotated = '''def operator(inputs, params, runtime):
    def wrap(entity: EntityRefView) -> EntityRef:
        return make_entity_ref(entity.role, entity.identifier)
    def build(window: WindowView, entity: EntityRefView) -> Evidence:
        return make_evidence('synthetic.csv', window, 'check', None, [entity], 1,
            False, {'ranking_eligible': False})
    def show(value: JSONValue) -> str:
        return str(value)
    def count_context(value: Inputs, configured: Params, context: Runtime) -> int:
        return len(value) + len(configured) + context.dag_depth
    def pass_output(value: Output) -> Output:
        return value
    return make_scored_candidates([], 'typed')
'''
        self.assertEqual(check_generated_spec_types(_spec(annotated))["status"], "passed")
        mapping_narrowing = '''def operator(inputs, params, runtime):
    if isinstance(inputs['data'], Mapping):
        return make_scored_candidates([], 'typed')
    return make_scored_candidates([], 'typed')
'''
        self.assertEqual(check_generated_spec_types(_spec(mapping_narrowing))["status"], "passed")
        runtime_alias = '''def operator(inputs, params, runtime):
    return Output(**inputs['data'])
'''
        with self.assertRaisesRegex(TypedAuthoringError, "only as a type annotation"):
            check_generated_spec_types(_spec(runtime_alias))

    def test_bad_record_field_is_rejected(self):
        source = '''def operator(inputs, params, runtime):
    data = aggregate_view(inputs['data'])
    return data.not_a_field
'''
        with self.assertRaises(TypedAuthoringError) as caught:
            check_generated_spec_types(_spec(source, "json"))
        self.assertEqual(caught.exception.code, "typed_static_type_error")
        self.assertIn("not_a_field", str(caught.exception))
        self.assertEqual(caught.exception.line, 3)
        self.assertIsInstance(caught.exception.column, int)
        self.assertIsInstance(caught.exception.mypy_code, str)

    def test_untyped_nested_function_and_any_escape_are_rejected(self):
        for source in (
            "def operator(inputs, params, runtime):\n    def f(x): return x\n    return 1\n",
            "from typing import Any\ndef operator(inputs, params, runtime):\n    x: Any = 1\n    return x\n",
            "def operator(inputs, params, runtime):  # type: ignore\n    return 1\n",
        ):
            with self.subTest(source=source), self.assertRaises(TypedAuthoringError):
                check_generated_spec_types(_spec(source, "json"))

    def test_wrong_helper_argument_is_rejected(self):
        source = '''def operator(inputs, params, runtime):
    data = aggregate_view(inputs['data'])
    return make_aggregate(data.window, [data], data.source_tables)
'''
        with self.assertRaises(TypedAuthoringError):
            check_generated_spec_types(_spec(source, "Aggregate"))

    def test_spec_derived_input_and_parameter_names_are_checked(self):
        source = '''def operator(inputs, params, runtime):
    aggregate_view(inputs['other'])
    return make_scored_candidates([], params['missing'])
'''
        with self.assertRaises(TypedAuthoringError) as caught:
            check_generated_spec_types(_spec(source))
        self.assertIn("other", str(caught.exception))
        self.assertIn("missing", str(caught.exception))

    def test_legacy_envelope_row_reader_is_not_admitted_in_typed_profile(self):
        source = '''def operator(inputs, params, runtime):
    rows = aggregate_rows(aggregate_view(inputs['data']))
    count = len(rows[0])
    return make_scored_candidates([], 'probe', status_details={'count': count})
'''
        with self.assertRaises(TypedAuthoringError) as caught:
            check_generated_spec_types(_spec(source))
        self.assertIn("aggregate_rows", str(caught.exception))

    def test_record_and_runtime_views_are_read_only(self):
        source = '''def operator(inputs, params, runtime):
    data = aggregate_view(inputs['data'])
    data.window = data.window
    runtime.dag_depth = 4
    runtime.alert_window['start_ms_utc'] = 99
    return make_scored_candidates([], 'typed')
'''
        with self.assertRaises(TypedAuthoringError):
            check_generated_spec_types(_spec(source))

    def test_typed_aggregate_view_is_immutable_snapshot_of_nested_input(self):
        window = make_window("alert", 100, 200)
        entity = make_entity_ref("component", "synthetic")
        row = make_aggregate_row(entity, "table|field", window, "summary", None, 1,
            ["table"], [entity], {"samples": ["before"], "flags": {"usable": True}})
        wire = make_aggregate(window, [row], ["table"], {"nested": {"count": 1}})
        view = aggregate_view(wire)
        with self.assertRaises(TypeError):
            view.metadata["nested"]["count"] = 2
        original_metadata = wire["$fields"]["metadata"]["$map"]["nested"]["$map"]
        original_metadata["count"] = 9
        self.assertEqual(view.metadata["nested"]["count"], 1)
        with self.assertRaises(TypeError):
            view.rows[0].details["flags"]["usable"] = False

    def test_typed_profile_blocks_manual_wire_access_and_telemetry_input(self):
        source = '''def operator(inputs, params, runtime):
    return {'$type': 'ScoredCandidates', '$fields': {}}
'''
        with self.assertRaisesRegex(ValueError, "wire-format keys"):
            validate_source(source, PROFILE)
        # Legacy source validation remains available for existing wire programs.
        validate_source(source)
        telemetry = _spec("def operator(inputs, params, runtime):\n    return make_scored_candidates([], 'x')\n")
        telemetry["input_ports"][0]["type"] = "Telemetry"
        with self.assertRaises(TypedAuthoringError) as caught:
            check_generated_spec_types(telemetry)
        self.assertEqual(caught.exception.code, "typed_input_not_supported")

    def test_typechecker_requires_profile_and_normalized_schema(self):
        with self.assertRaises(TypedAuthoringError):
            check_generated_spec_types({"source": "x"})
        malformed = {"authoring_profile": PROFILE, "source": "def operator(inputs, params, runtime): return 1"}
        with self.assertRaises(TypedAuthoringError):
            check_generated_spec_types(malformed)

    def test_float_parameter_choices_remain_valid_number_type(self):
        source = '''def operator(inputs, params, runtime):
    value = params['threshold']
    return make_scored_candidates([], 'typed', status_details={'value': value})
'''
        spec = _spec(source)
        spec["parameters"] = {"threshold": {"type": "number", "required": True,
            "nullable": False, "default": None, "has_default": False,
            "choices": [0.1, 0.2], "runtime_field": None, "description": ""}}
        self.assertEqual(check_generated_spec_types(spec)["status"], "passed")

    def test_typed_scored_candidate_category_is_none_only(self):
        source = '''def operator(inputs, params, runtime):
    entity = make_entity_ref('component', 'synthetic')
    scored = make_scored_candidate(entity, 0.5, 'CPU fault', [])
    return make_scored_candidates([scored], 'typed')
'''
        with self.assertRaises(TypedAuthoringError):
            check_generated_spec_types(_spec(source))

    def test_sandbox_rejects_source_mismatch_before_worker_start(self):
        source = "def operator(inputs, params, runtime):\n    return make_scored_candidates([], 'x')\n"
        spec = _spec(source)
        changed = source.replace("'x'", "'y'")
        result = run_generated_operator(changed, {"data": {}}, {}, {}, "ScoredCandidates",
            authoring_profile=PROFILE, typecheck_spec=spec)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "typed_spec_mismatch")

    def test_published_typed_api_has_constructor_types_without_wire_details(self):
        schema = typed_record_helper_schema()
        by_name = {row["name"]: row for row in schema["helpers"]}
        expected = {"make_window", "make_entity_ref", "make_evidence", "make_candidate",
            "make_candidate_set", "make_aggregate_row", "make_aggregate",
            "make_scored_candidate", "make_scored_candidates"}
        self.assertTrue(expected <= set(by_name))
        self.assertNotIn("aggregate_rows", by_name)
        self.assertNotIn("candidate_items", by_name)
        self.assertIn("ranking_eligible", by_name["make_evidence"]["required_parameters"])
        self.assertEqual(by_name["make_evidence"]["parameters"]["ranking_eligible"], "bool")
        rendered = json.dumps(schema, sort_keys=True)
        for wire_detail in ("$type", "$fields", "$map", "$tuple", "cache envelope"):
            self.assertNotIn(wire_detail, rendered)

    @unittest.skipUnless(shutil.which("bwrap"), "bubblewrap is unavailable")
    def test_typed_profile_executes_and_passes_strict_output_deserializer(self):
        source = '''def operator(inputs, params, runtime):
    candidate = candidate_views(inputs['candidates'])[0]
    entity = make_entity_ref(candidate.entity.role, candidate.entity.identifier)
    alert = runtime.alert_window
    window = make_window(alert['name'], alert['start_ms_utc'], alert['end_ms_utc'])
    evidence = make_evidence('synthetic/source.csv', window, 'typed_check',
        params['value'], [entity], params['support'], True,
        {'ranking_eligible': True, 'support_unit': 'synthetic_records'})
    score = make_scored_candidate(entity, params['score'], None, [evidence], 'typed_test')
    return make_scored_candidates([score], 'evolved.typed_test')
'''
        spec = {"authoring_profile": PROFILE, "name": "evolved.typed_test", "version": "1",
            "declared_version": "1", "description": "typed test", "family": "test",
            "input_ports": [{"name": "candidates", "type": "CandidateSet", "required": True,
                "variadic": False, "minimum_items": 1, "maximum_items": None}],
            "parameters": {
                "value": {"type": "number", "required": True, "nullable": False,
                    "default": None, "has_default": False, "choices": [], "runtime_field": None,
                    "description": ""},
                "score": {"type": "number", "required": True, "nullable": False,
                    "default": None, "has_default": False, "choices": [], "runtime_field": None,
                    "description": ""},
                "support": {"type": "positive_integer", "required": True, "nullable": False,
                    "default": None, "has_default": False, "choices": [], "runtime_field": None,
                    "description": ""}}, "output_type": "ScoredCandidates", "source": source}
        candidates = make_candidate_set([make_candidate(make_entity_ref("component", "synthetic"))],
                                        "synthetic")
        result = run_generated_operator(source, {"candidates": candidates},
            {"value": 1.5, "score": 0.7, "support": 2},
            {"alert_window": {"name": "alert", "start_ms_utc": 0, "end_ms_utc": 1}},
            "ScoredCandidates", authoring_profile=PROFILE, typecheck_spec=spec)
        self.assertTrue(result["ok"], result)
        decoded = deserialize_contract_json(result["result"], "ScoredCandidates")
        self.assertEqual(decoded.candidates[0].entity.identifier, "synthetic")

    @unittest.skipUnless(shutil.which("bwrap"), "bubblewrap is unavailable")
    def test_typed_nonempty_aggregate_and_native_query_views_execute(self):
        source = '''def operator(inputs, params, runtime):
    aggregate = aggregate_view(inputs['data'])
    row_count = len(aggregate.rows)
    detail_count = len(aggregate.rows[0].details)
    query_count = len(native_query_rows(aggregate))
    return make_scored_candidates([], 'neutral_probe', status_details={
        'rows': row_count, 'detail_keys': detail_count, 'query_rows': query_count})
'''
        spec = _spec(source)
        window = make_window("alert", 10, 20)
        entity = make_entity_ref("component", "synthetic")
        row = make_aggregate_row(entity, "synthetic|metric", window, "summary", 1.0, 2,
            ["synthetic.csv"], [entity], {"sample_count": 2, "labels": ["a", "b"]})
        aggregate = make_aggregate(window, [row], ["synthetic.csv"],
            {"public_query": {"rows_data": [{"metric": "cpu", "value": 1.0}]}})
        result = run_generated_operator(source, {"data": aggregate}, {}, {}, "ScoredCandidates",
            authoring_profile=PROFILE, typecheck_spec=spec)
        self.assertTrue(result["ok"], result)
        decoded = deserialize_contract_json(result["result"], "ScoredCandidates")
        self.assertEqual(decoded.status_details, {"rows": 1, "detail_keys": 2, "query_rows": 1})

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
                         "large typed Aggregate view stress is opt-in")
    @unittest.skipUnless(shutil.which("bwrap"), "bubblewrap is unavailable")
    def test_large_typed_aggregate_view_fits_frozen_m4_limits(self):
        source = '''def operator(inputs, params, runtime):
    aggregate = aggregate_view(inputs['data'])
    row = aggregate.rows[0]
    entity = make_entity_ref(row.entity.role, row.entity.identifier)
    evidence = make_evidence('synthetic/aggregate.csv', row.window, 'bounded_read',
        None, [entity], row.support, False, {'ranking_eligible': False})
    scored = make_scored_candidate(entity, 0.0, None, [evidence], 'stress')
    return make_scored_candidates([scored], 'evolved.typed_stress', 'abstained')
'''
        window = Window("alert", 1_700_001_800_000, 1_700_003_600_000)
        entity = EntityRef("component", "Tomcat01")
        sample = tuple("x" * 90 for _ in range(24))
        rows = tuple(AggregateRow(entity, f"metric|field-{index}", window,
            "nested_sample", None, 24, ("synthetic/aggregate.csv",), (entity,),
            {"bucket_samples": sample, "group": {"members": tuple(range(4))}})
            for index in range(5000))
        aggregate = Aggregate(window, rows, ("synthetic/aggregate.csv",), {})
        payload = {"data": _encode(aggregate)}
        request_size = len(json.dumps({"source": source, "inputs": payload,
            "params": {}, "runtime": {}, "output_type": "ScoredCandidates",
            "authoring_profile": PROFILE}, separators=(",", ":")).encode())
        self.assertGreater(request_size, 12 * 1024 * 1024)
        self.assertLess(request_size, 16 * 1024 * 1024)
        spec = {"authoring_profile": PROFILE, "name": "evolved.typed_stress", "version": "1",
            "declared_version": "1", "description": "typed stress", "family": "stress",
            "input_ports": [{"name": "data", "type": "Aggregate", "required": True,
                "variadic": False, "minimum_items": 1, "maximum_items": None}],
            "parameters": {}, "output_type": "ScoredCandidates", "source": source}
        result = run_generated_operator(source, payload, {}, {}, "ScoredCandidates",
            limits={"input_bytes": 16 * 1024 * 1024, "cpu_seconds": 10,
                    "wall_seconds": 15, "memory_bytes": 512 * 1024 * 1024},
            authoring_profile=PROFILE, typecheck_spec=spec)
        self.assertTrue(result["ok"], result)
        self.assertEqual(deserialize_contract_json(result["result"], "ScoredCandidates").status,
                         "abstained")


if __name__ == "__main__":
    unittest.main()
