import math
import json
import os
import tempfile
import unittest
from pathlib import Path

from e4core.sandbox import (DEFAULT_LIMITS, _validate_worker_response, probe_os_isolation,
                           run_generated_operator)
from e4core.sandbox_worker import contract_fields


class GeneratedOperatorSandboxTests(unittest.TestCase):
    def test_contract_fields_preserves_nested_typed_envelopes(self):
        window = {"$type": "Window", "$fields": {"name": "alert",
            "start_ms_utc": 1, "end_ms_utc": 2}}
        entity = {"$type": "EntityRef", "$fields": {
            "role": "component", "identifier": "synthetic-service"}}
        row = {"$type": "AggregateRow", "$fields": {
            "entity": entity, "metric_key": "synthetic", "window": window,
            "statistic": "median", "value": 1.0, "support": 1,
            "source_tables": {"$tuple": ["synthetic.csv"]},
            "contributing_entities": {"$tuple": [entity]},
            "details": {"$map": {"ranking_eligible": True}}}}
        aggregate_wire = {"$type": "Aggregate", "$fields": {
            "window": window, "rows": {"$tuple": [row]},
            "source_tables": {"$tuple": ["synthetic.csv"]},
            "metadata": {"$map": {}}}}

        aggregate = contract_fields(aggregate_wire, "Aggregate")
        nested_row = aggregate["rows"][0]
        self.assertEqual(nested_row["$type"], "AggregateRow")
        with self.assertRaises(KeyError):
            _ = nested_row["entity"]

        row_fields = contract_fields(nested_row, "AggregateRow")
        entity_fields = contract_fields(row_fields["entity"], "EntityRef")
        self.assertEqual(entity_fields["identifier"], "synthetic-service")

    def test_worker_exposes_published_isinstance_and_explicit_import_capabilities(self):
        source = """import math
def operator(inputs, params, runtime):
    return math.sqrt(16) + int(isinstance(inputs, dict))
"""
        result = run_generated_operator(source, {"value": 1}, {}, {}, "number")
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["result"], 5)

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
                         "opt-in 15 MiB bubblewrap resource stress test")
    def test_near_limit_nested_aggregates_reduce_to_small_typed_result(self):
        def aggregate(name):
            rows = [{"$map": {"entity_id": f"service-{index:04d}",
                              "metric_value": index % 17,
                              "note": "x" * 3350}}
                    for index in range(2250)]
            return {"$type": "Aggregate", "$fields": {
                "window": {"$type": "Window", "$fields": {
                    "name": name, "start_ms_utc": 1000, "end_ms_utc": 2000}},
                "rows": {"$tuple": []}, "source_tables": {"$tuple": ["synthetic/metric.csv"]},
                "metadata": {"$map": {"public_query": {"$map": {"rows_data": rows}}}}}}

        source = '''def operator(inputs, params, runtime):
    alert = contract_fields(inputs["alert"], "Aggregate")
    reference = contract_fields(inputs["reference"], "Aggregate")
    alert_rows = alert["metadata"]["public_query"]["rows_data"]
    reference_rows = reference["metadata"]["public_query"]["rows_data"]
    hits = 0
    total = 0
    for row in alert_rows:
        value = row["metric_value"]
        total += value
        if value >= params["threshold"]:
            hits += 1
    baseline = sum(row["metric_value"] for row in reference_rows)
    return {"$type": "Aggregate", "$fields": {
        "window": alert["window"], "rows": {"$tuple": []},
        "source_tables": {"$tuple": ["synthetic/metric.csv"]},
        "metadata": {"$map": {"reduction": {"$map": {
            "alert_rows": len(alert_rows), "reference_rows": len(reference_rows),
            "threshold_hits": hits, "alert_total": total, "reference_total": baseline}}}}}}
'''
        inputs = {"alert": aggregate("alert"), "reference": aggregate("reference"),
                  "candidates": {"$type": "CandidateSet", "$fields": {
                      "candidates": {"$tuple": []}, "source": "synthetic"}}}
        limits = {"input_bytes": 16 * 1024 * 1024}
        request_bytes = len(json.dumps(
            {"source": source, "inputs": inputs, "params": {"threshold": 8},
             "runtime": {}, "output_type": "Aggregate",
             "limits": {**DEFAULT_LIMITS, **limits}}, separators=(",", ":"),
            allow_nan=False).encode())
        self.assertGreater(request_bytes, 15_000_000)
        self.assertLess(request_bytes, 16 * 1024 * 1024)

        result = run_generated_operator(source, inputs, {"threshold": 8}, {},
                                        "Aggregate", limits=limits)
        self.assertTrue(result["ok"], result)
        summary = result["result"]["$fields"]["metadata"]["$map"]["reduction"]["$map"]
        self.assertEqual(summary, {"alert_rows": 2250, "reference_rows": 2250,
                                   "threshold_hits": 1188, "alert_total": 17967,
                                   "reference_total": 17967})

        generic_default = run_generated_operator(source, inputs, {"threshold": 8}, {},
            "Aggregate")
        self.assertFalse(generic_default["ok"])
        self.assertEqual(generic_default["error"]["code"], "input_limit")

        oversized = run_generated_operator(source, inputs,
            {"threshold": 8, "padding": "x" * (2 * 1024 * 1024)}, {},
            "Aggregate", limits=limits)
        self.assertFalse(oversized["ok"])
        self.assertEqual(oversized["error"]["code"], "input_limit")

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
                         "opt-in nested AggregateRow CPU/RSS characterization")
    def test_nested_aggregate_row_stress_characterizes_cpu_budget(self):
        row_count = 2950
        sample_count = 28

        def window(name):
            start = 1_700_000_000_000 if name == "reference" else 1_700_001_800_000
            return {"$type": "Window", "$fields": {"name": name,
                "start_ms_utc": start, "end_ms_utc": start + 1_800_000}}

        def aggregate(name):
            win = window(name)
            rows = []
            for index in range(row_count):
                entity = {"$type": "EntityRef", "$fields": {
                    "role": "component", "identifier": f"service-{index:05d}"}}
                start = win["$fields"]["start_ms_utc"]
                buckets = [{"$tuple": [start + minute * 60_000,
                                       float((index + minute) % 97)]}
                           for minute in range(sample_count)]
                raw_counts = [{"$tuple": [start + minute * 60_000,
                                           (index + minute) % 5 + 1]}
                              for minute in range(sample_count)]
                rows.append({"$type": "AggregateRow", "$fields": {
                    "entity": entity, "metric_key": "metric/metric_app.csv|mrt||",
                    "window": win, "statistic": "median", "value": float(index % 97),
                    "support": sample_count,
                    "source_tables": {"$tuple": ["metric/metric_app.csv"]},
                    "contributing_entities": {"$tuple": []},
                    "details": {"$map": {
                        "metric_kind": "latency", "unit": "ms", "source_field": "mrt",
                        "source_table": "metric/metric_app.csv", "item_id": None,
                        "bucket_samples": {"$tuple": buckets},
                        "distinct_time_buckets": sample_count,
                        "raw_rows_by_timestamp": {"$tuple": raw_counts},
                        "comparison_policy": "same source field only; no conversion across Bank metrics"}}}})
            return {"$type": "Aggregate", "$fields": {"window": win,
                "rows": {"$tuple": rows},
                "source_tables": {"$tuple": ["metric/metric_app.csv"]},
                "metadata": {"$map": {}}}}

        source = '''def operator(inputs, params, runtime):
    alert = contract_fields(inputs["alert"], "Aggregate")
    reference = contract_fields(inputs["reference"], "Aggregate")
    alert_total = 0.0
    reference_total = 0.0
    sample_count = 0
    for row_wire in alert["rows"]:
        row = contract_fields(row_wire, "AggregateRow")
        details = row["details"]
        samples = details["bucket_samples"]
        sample_count += len(samples) + len(details["raw_rows_by_timestamp"])
        alert_total += sum(item[1] for item in samples)
    for row_wire in reference["rows"]:
        row = contract_fields(row_wire, "AggregateRow")
        reference_total += sum(item[1] for item in row["details"]["bucket_samples"])
    return {"$type":"Aggregate","$fields":{
        "window":alert["window"],"rows":{"$tuple":[]},
        "source_tables":{"$tuple":["metric/metric_app.csv"]},
        "metadata":{"$map":{"reduction":{"$map":{
            "alert_rows":len(alert["rows"]),"reference_rows":len(reference["rows"]),
            "sample_count":sample_count,"alert_total":alert_total,
            "reference_total":reference_total}}}}}}
'''
        inputs = {"alert": aggregate("alert"), "reference": aggregate("reference"),
                  "candidates": {"$type": "CandidateSet", "$fields": {
                      "candidates": {"$tuple": []}, "source": "synthetic"}}}
        from e4core.m4_data import generated_sandbox_limits
        m4_limits = generated_sandbox_limits()
        limits = {"input_bytes": m4_limits["input_bytes"]}
        request = {"source": source, "inputs": inputs, "params": {}, "runtime": {},
                   "output_type": "Aggregate", "limits": {**DEFAULT_LIMITS, **m4_limits}}
        request_bytes = len(json.dumps(request, separators=(",", ":"),
                                    allow_nan=False).encode())
        nested_tuple_objects = 2 * row_count * sample_count * 2
        self.assertGreater(request_bytes, 14_000_000)
        self.assertLess(request_bytes, 15_000_000)
        self.assertGreater(nested_tuple_objects, 200_000)

        bounded = run_generated_operator(source, inputs, {}, {}, "Aggregate",
            limits={**m4_limits, "cpu_seconds": DEFAULT_LIMITS["cpu_seconds"],
                    "wall_seconds": DEFAULT_LIMITS["wall_seconds"]})
        self.assertFalse(bounded["ok"], bounded)
        self.assertEqual(bounded["error"]["code"], "resource_limit")

        production = run_generated_operator(source, inputs, {}, {}, "Aggregate",
                                             limits=m4_limits)
        self.assertTrue(production["ok"], production)
        summary = production["result"]["$fields"]["metadata"]["$map"]["reduction"]["$map"]
        self.assertEqual(summary, {"alert_rows": row_count, "reference_rows": row_count,
                                   "sample_count": row_count * sample_count * 2,
                                   "alert_total": 3_948_000.0,
                                   "reference_total": 3_948_000.0})

        generic_default = run_generated_operator(source, inputs, {}, {}, "Aggregate")
        self.assertFalse(generic_default["ok"])
        self.assertEqual(generic_default["error"]["code"], "input_limit")
        over_cap = run_generated_operator(source, inputs,
            {"padding": "x" * (3 * 1024 * 1024)}, {}, "Aggregate", limits=m4_limits)
        self.assertFalse(over_cap["ok"])
        self.assertEqual(over_cap["error"]["code"], "input_limit")

    def test_useful_operator_gets_independent_json_inputs(self):
        source = """def operator(inputs, params, runtime):
    scale = params['scale']
    return {'sum': sum(inputs['values']) * scale,
            'seed': runtime['seed']}
"""
        outputs = []
        for values, expected in (([2, 3], 25), ([-4, 7], 15), ([], 0), ([11], 55)):
            result = run_generated_operator(source, {"values": values}, {"scale": 5},
                                            {"seed": 17}, "object")
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["result"], {"sum": expected, "seed": 17})
            outputs.append(result["result"]["sum"])
        self.assertEqual(outputs, [25, 15, 0, 55])

    def test_source_signature_and_malformed_results_are_rejected(self):
        bad_signature = "def operator(data):\n    return data\n"
        result = run_generated_operator(bad_signature, {}, {}, {}, "json")
        self.assertFalse(result["ok"])
        self.assertIn(result["error"]["code"], {"worker_error", "sandbox_unavailable"})

        for expression, expected in (("float('nan')", "non-finite"),
                                     ("{1, 2}", "JSON-compatible")):
            source = f"def operator(inputs, params, runtime):\n    return {expression}\n"
            result = run_generated_operator(source, {}, {}, {}, "json")
            self.assertFalse(result["ok"], result)
            if result["error"]["code"] != "sandbox_unavailable":
                self.assertIn(expected, result["error"]["message"])

    def test_output_type_and_nonfinite_input_contracts(self):
        result = run_generated_operator(
            "def operator(inputs, params, runtime):\n    return [1, 2]\n",
            {}, {}, {}, "object")
        self.assertFalse(result["ok"])
        if result["error"]["code"] != "sandbox_unavailable":
            self.assertEqual(result["error"]["code"], "output_type_mismatch")
        with self.assertRaisesRegex(ValueError, "finite JSON"):
            run_generated_operator("def operator(inputs, params, runtime):\n    return 1\n",
                                   {"x": math.inf}, {}, {}, "number")

        unsupported = run_generated_operator(
            "def operator(inputs, params, runtime):\n    return {}\n", {}, {}, {}, "InventedType")
        self.assertFalse(unsupported["ok"])
        self.assertEqual(unsupported["error"]["code"], "unsupported_output_type")

        malformed = run_generated_operator(
            "def operator(inputs, params, runtime):\n    return {'candidates': []}\n",
            {}, {}, {}, "CandidateSet")
        self.assertFalse(malformed["ok"], malformed)
        self.assertEqual(malformed["error"]["code"], "output_type_mismatch")

    def test_runtime_supports_read_only_attribute_and_mapping_access(self):
        source = """def operator(inputs, params, runtime):
    return {'depth': runtime.dag_depth, 'seed': runtime['seed']}
"""
        result = run_generated_operator(source, {}, {}, {"dag_depth": 12, "seed": 42}, "object")
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["result"], {"depth": 12, "seed": 42})

        mutation = """def operator(inputs, params, runtime):
    runtime.dag_depth = 99
    return {'depth': runtime.dag_depth}
"""
        rejected = run_generated_operator(mutation, {}, {}, {"dag_depth": 12}, "object")
        self.assertFalse(rejected["ok"])
        self.assertEqual(rejected["error"]["code"], "operator_error")

    def test_parent_rejects_non_object_worker_protocol(self):
        with self.assertRaisesRegex(ValueError, "JSON object"):
            _validate_worker_response(["ok", {"result": 1}])
        with self.assertRaisesRegex(ValueError, "boolean ok"):
            _validate_worker_response({"ok": "yes", "result": 1})

    def test_infinite_loop_allocation_and_output_are_bounded(self):
        cases = [
            ("def operator(inputs, params, runtime):\n    while True: pass\n", "timeout"),
            ("def operator(inputs, params, runtime):\n    x=[]\n    while True: x.append('x'*1000000)\n", None),
            ("def operator(inputs, params, runtime):\n    print('x'*100000)\n    return 1\n", "output_limit"),
        ]
        for source, wanted in cases:
            result = run_generated_operator(source, {}, {}, {}, "number",
                                            {"wall_seconds": 1.5, "cpu_seconds": 1,
                                             "memory_bytes": 128 * 1024 * 1024,
                                             "stdout_bytes": 4096,
                                             "protocol_bytes": 8192})
            self.assertFalse(result["ok"], result)
            if result["error"]["code"] != "sandbox_unavailable" and wanted:
                allowed = {"timeout", "resource_limit"} if wanted == "timeout" else {wanted}
                self.assertIn(result["error"]["code"], allowed)

    def test_os_isolation_without_ast_guard(self):
        # This trusted probe directly uses filesystem and socket APIs in the same
        # namespace builder; generated-source AST rejection cannot satisfy it.
        with tempfile.NamedTemporaryFile() as marker:
            marker.write(b"host-only sentinel")
            marker.flush()
            proof = probe_os_isolation(Path(marker.name))
        self.assertTrue(proof.get("ok"), proof)
        self.assertTrue(proof["observed"]["host_marker_hidden"])
        self.assertTrue(proof["observed"]["etc_passwd_hidden"])
        self.assertTrue(proof["observed"]["network_isolated"])
        self.assertTrue(proof["observed"]["host_listener_hidden"])

    def test_source_and_stdout_are_preserved(self):
        source = "def operator(inputs, params, runtime):\n    print('diagnostic')\n    return 2\n"
        result = run_generated_operator(source, {}, {}, {}, "number")
        if result["ok"]:
            self.assertEqual(result["source"], source)
            self.assertEqual(result["stdout"], "diagnostic\n")
        else:
            self.assertNotEqual(result["error"]["code"], "sandbox_unavailable")

    def test_optional_catalog_mount_exposes_only_bounded_json_reader(self):
        with tempfile.TemporaryDirectory() as tmp:
            catalog = Path(tmp) / "catalog.json"
            catalog.write_text('{"services":["api","worker"]}')
            source = """def operator(inputs, params, runtime):
    catalog = read_catalog_json()
    return {'count': len(catalog['services'])}
"""
            result = run_generated_operator(source, {}, {}, {}, "object",
                                            catalog_path=catalog)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["result"], {"count": 2})

    def test_case_reader_rejects_parent_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            case_dir = Path(tmp) / "case"
            case_dir.mkdir()
            (case_dir / "excerpt.json").write_text('{"value":7}')
            ok_source = "def operator(inputs, params, runtime):\n    return read_case_json('excerpt.json')\n"
            good = run_generated_operator(ok_source, {}, {}, {}, "object", case_dir=case_dir)
            self.assertTrue(good["ok"], good)
            self.assertEqual(good["result"], {"value": 7})
            escape_source = "def operator(inputs, params, runtime):\n    return read_case_json('../outside.json')\n"
            bad = run_generated_operator(escape_source, {}, {}, {}, "object", case_dir=case_dir)
            self.assertFalse(bad["ok"], bad)
            self.assertEqual(bad["error"]["code"], "operator_error")

    def test_generated_core_contract_wire_output_is_strictly_decoded(self):
        valid = """def operator(inputs, params, runtime):
    return {'$type': 'CandidateSet', '$fields': {'candidates': {'$tuple': []}, 'source': 'synthetic'}}
"""
        result = run_generated_operator(valid, {}, {}, {}, "CandidateSet")
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["output_validation"], "checked_typed_contract")
        self.assertEqual(result["validated_contract_type"], "CandidateSet")
        self.assertEqual(result["result"]["$type"], "CandidateSet")

        malformed = """def operator(inputs, params, runtime):
    return {'$type': 'CandidateSet', '$fields': {'candidates': {'$tuple': []}}}
"""
        rejected = run_generated_operator(malformed, {}, {}, {}, "CandidateSet")
        self.assertFalse(rejected["ok"])
        self.assertEqual(rejected["error"]["code"], "output_type_mismatch")


if __name__ == "__main__":
    unittest.main()
