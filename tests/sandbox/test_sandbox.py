import math
import tempfile
import unittest
from pathlib import Path

from e4.sandbox.runner import (_validate_worker_response, probe_os_isolation,
                               run_generated_operator)


class GeneratedOperatorSandboxTests(unittest.TestCase):
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
