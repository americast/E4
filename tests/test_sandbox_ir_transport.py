"""Bounded compressed transport tests for trusted typed-IR results."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import random
import string
import unittest
import zlib

from e4core.cache import _encode, deserialize_contract_json
from e4core.contracts import Aggregate, AggregateRow, Candidate, CandidateSet, EntityRef, Window
from e4core.m4_generated import register_generated_specs
from e4core.m4_operator_ir import trusted_runtime_context
from e4core.m4_operator_ir_worker import execute_ir
from e4core.registry import OperatorRegistry
from e4core.sandbox import (_IR_RESULT_MAX_BYTES, _decode_ir_result_transport,
                            _ir_statistic, _reject_duplicate_json_keys,
                            run_generated_operator)
from e4core.sandbox_worker import (_encode_ir_success_response,
                                   IR_RESULT_TRANSPORT_CODEC,
                                   IR_RESULT_TRANSPORT_VERSION)
from tests.test_m4_operator_ir import ALT, REF, RUNTIME, _spec


PROTOCOL_BYTES = 64 * 1024
SOURCE_HASHES = {"bank/metric_container.csv": "b" * 64}
CASE_SHA = "c" * 64
SOURCE_FIELD = "OSLinux-CPU_CPU_CPUCpuUtil"


def _envelope(raw: bytes) -> dict:
    return {"$transport": IR_RESULT_TRANSPORT_VERSION,
        "codec": IR_RESULT_TRANSPORT_CODEC, "decoded_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "payload": base64.b64encode(zlib.compress(raw)).decode("ascii")}


def _high_cardinality_inputs(count: int = 100):
    identifiers = [f"fixture-component-{index:04d}-" + "x" * 72 for index in range(count)]
    candidates = CandidateSet(tuple(Candidate(EntityRef("component", identifier), "fixture")
        for identifier in identifiers), source="high-cardinality transport fixture")

    def aggregate(window: Window, *, alert: bool):
        rows = []
        for identifier in identifiers:
            samples = []
            for index in range(30):
                timestamp = window.start_ms_utc + index * 60_000
                value = (1.0 + (index % 2) * 2.0) if alert else 1.0
                samples.append((timestamp, value))
            details = {"source_table": "bank/metric_container.csv",
                "source_field": SOURCE_FIELD, "metric_kind": "gauge",
                "ranking_eligible": True, "unit": "fixture native value",
                "bucket_samples": tuple(samples)}
            rows.append(AggregateRow(EntityRef("component", identifier),
                f"bank/metric_container.csv|{SOURCE_FIELD}|value", window, "median",
                2.0 if alert else 1.0, 30, ("bank/metric_container.csv",), (), details))
        return Aggregate(window, tuple(rows), ("bank/metric_container.csv",),
            {"source_hashes": SOURCE_HASHES, "case_data_sha256": CASE_SHA})

    return {"reference": aggregate(REF, alert=False),
        "alert": aggregate(ALT, alert=True), "candidates": candidates}


class TypedIRTransportTests(unittest.TestCase):
    def test_small_response_keeps_plain_wire_and_large_response_uses_exact_envelope(self):
        small = {"$type": "ScoredCandidates", "$fields": {"candidates": {"$tuple": []}}}
        plain = _encode_ir_success_response(small, protocol_bytes=PROTOCOL_BYTES,
            decoded_limit_bytes=_IR_RESULT_MAX_BYTES)
        self.assertEqual(json.loads(plain)["result"], small)
        self.assertNotIn(b"$transport", plain)

        large = {"$type": "ScoredCandidates", "$fields": {
            "candidates": {"$tuple": [{"id": index, "metadata": "same-value" * 20}
                for index in range(500)]}}}
        encoded = _encode_ir_success_response(large, protocol_bytes=PROTOCOL_BYTES,
            decoded_limit_bytes=_IR_RESULT_MAX_BYTES)
        wire = json.loads(encoded)
        self.assertLessEqual(len(encoded), PROTOCOL_BYTES)
        self.assertEqual(set(wire), {"ok", "result", "stdout", "stderr"})
        self.assertEqual(set(wire["result"]), {
            "$transport", "codec", "decoded_bytes", "sha256", "payload"})
        decoded = _decode_ir_result_transport(wire,
            authoring_profile="typed-operator-ir-v1")
        self.assertEqual(decoded["result"], large)

    def test_decoded_and_wire_limits_return_small_structured_error_not_prefix(self):
        decoded_over = {"data": "x" * _IR_RESULT_MAX_BYTES}
        response = json.loads(_encode_ir_success_response(decoded_over,
            protocol_bytes=PROTOCOL_BYTES, decoded_limit_bytes=_IR_RESULT_MAX_BYTES))
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "ir_output_limit")
        self.assertEqual(response["error"]["allowed_bytes"], _IR_RESULT_MAX_BYTES)
        self.assertFalse(response["error"]["exact"])
        self.assertGreater(response["error"]["required_bytes"], _IR_RESULT_MAX_BYTES)
        self.assertIsNone(response["result"])

        incompressible = {"data": "".join(random.Random(17).choices(
            string.ascii_letters + string.digits, k=100_000))}
        response = json.loads(_encode_ir_success_response(incompressible,
            protocol_bytes=PROTOCOL_BYTES, decoded_limit_bytes=_IR_RESULT_MAX_BYTES))
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "ir_output_limit")
        self.assertEqual(response["error"]["allowed_bytes"], PROTOCOL_BYTES)
        self.assertTrue(response["error"]["exact"])
        self.assertGreater(response["error"]["required_bytes"], PROTOCOL_BYTES)
        self.assertLess(len(json.dumps(response, separators=(",", ":")).encode()), 1024)

    def test_exact_plain_wire_and_decoded_boundaries(self):
        prefix = b'{"ok":true,"result":'
        suffix = b',"stdout":"","stderr":""}'
        empty_value = json.dumps({"data": ""}, separators=(",", ":")).encode()
        text_size = PROTOCOL_BYTES - len(prefix) - len(suffix) - len(empty_value)
        exact_wire_result = {"data": "w" * text_size}
        exact_wire = _encode_ir_success_response(exact_wire_result,
            protocol_bytes=PROTOCOL_BYTES, decoded_limit_bytes=_IR_RESULT_MAX_BYTES)
        self.assertEqual(len(exact_wire), PROTOCOL_BYTES)
        self.assertEqual(json.loads(exact_wire)["result"], exact_wire_result)

        empty_decoded_size = len(empty_value)
        exact_decoded_result = {"data": "d" * (_IR_RESULT_MAX_BYTES - empty_decoded_size)}
        exact_decoded_wire = _encode_ir_success_response(exact_decoded_result,
            protocol_bytes=PROTOCOL_BYTES, decoded_limit_bytes=_IR_RESULT_MAX_BYTES)
        transport = json.loads(exact_decoded_wire)
        self.assertTrue(transport["ok"])
        self.assertEqual(transport["result"]["decoded_bytes"], _IR_RESULT_MAX_BYTES)
        decoded = _decode_ir_result_transport(transport,
            authoring_profile="typed-operator-ir-v1")
        self.assertEqual(decoded["result"], exact_decoded_result)

    def test_host_enforces_decoded_limit_for_plain_typed_result(self):
        plain_over = {"ok": True, "result": {"data": "x" * 101},
            "stdout": "", "stderr": ""}
        with self.assertRaisesRegex(ValueError, "plain result exceeds"):
            _decode_ir_result_transport(plain_over,
                authoring_profile="typed-operator-ir-v1", decoded_limit_bytes=100)
        nonfinite = {"ok": True, "result": {"score": float("nan")},
            "stdout": "", "stderr": ""}
        with self.assertRaisesRegex(ValueError, "not finite JSON"):
            _decode_ir_result_transport(nonfinite,
                authoring_profile="typed-operator-ir-v1", decoded_limit_bytes=100)

    def test_envelope_shape_profile_gate_and_corruption_rejection(self):
        raw = json.dumps({"safe": [1, 2, 3]}, separators=(",", ":")).encode()
        response = {"ok": True, "result": _envelope(raw), "stdout": "", "stderr": ""}
        self.assertEqual(_decode_ir_result_transport(response,
            authoring_profile=None), response)
        self.assertEqual(_decode_ir_result_transport(response,
            authoring_profile="typed-operator-ir-v1")["result"], {"safe": [1, 2, 3]})

        corruptions = []
        extra = copy.deepcopy(response); extra["result"]["surprise"] = True; corruptions.append(extra)
        wrong_version = copy.deepcopy(response); wrong_version["result"]["$transport"] = "v0"; corruptions.append(wrong_version)
        wrong_codec = copy.deepcopy(response); wrong_codec["result"]["codec"] = "gzip"; corruptions.append(wrong_codec)
        bad_b64 = copy.deepcopy(response); bad_b64["result"]["payload"] = "%%%"; corruptions.append(bad_b64)
        bad_len = copy.deepcopy(response); bad_len["result"]["decoded_bytes"] += 1; corruptions.append(bad_len)
        bad_hash = copy.deepcopy(response); bad_hash["result"]["sha256"] = "0" * 64; corruptions.append(bad_hash)
        compressed = zlib.compress(raw)
        trailing = copy.deepcopy(response); trailing["result"]["payload"] = base64.b64encode(compressed + b"junk").decode(); corruptions.append(trailing)
        concatenated = copy.deepcopy(response); concatenated["result"]["payload"] = base64.b64encode(compressed + zlib.compress(b"{}" )).decode(); corruptions.append(concatenated)
        truncated = copy.deepcopy(response); truncated["result"]["payload"] = base64.b64encode(compressed[:-1]).decode(); corruptions.append(truncated)
        wrong_outer = {**response, "unexpected": 1}; corruptions.append(wrong_outer)
        for item in corruptions:
            with self.subTest(result=item.get("result")):
                with self.assertRaises(ValueError):
                    _decode_ir_result_transport(item,
                        authoring_profile="typed-operator-ir-v1")

        over_bound_raw = b"x" * 101
        over_bound_envelope = _envelope(over_bound_raw)
        over_bound_envelope["decoded_bytes"] = 100
        over_bound = {"ok": True, "result": over_bound_envelope, "stdout": "", "stderr": ""}
        with self.assertRaisesRegex(ValueError, "decoded byte limit"):
            _decode_ir_result_transport(over_bound,
                authoring_profile="typed-operator-ir-v1", decoded_limit_bytes=100)

        nested_json = b"[" * 2000 + b"0" + b"]" * 2000
        nested = {"ok": True, "result": _envelope(nested_json), "stdout": "", "stderr": ""}
        with self.assertRaisesRegex(ValueError, "valid UTF-8 JSON"):
            _decode_ir_result_transport(nested, authoring_profile="typed-operator-ir-v1")
        nonfinite = {"ok": True, "result": _envelope(b'{"score":NaN}'),
            "stdout": "", "stderr": ""}
        with self.assertRaisesRegex(ValueError, "non-finite JSON"):
            _decode_ir_result_transport(nonfinite, authoring_profile="typed-operator-ir-v1")

        duplicate_outer = b'{"ok":true,"ok":true,"result":{},"stdout":"","stderr":""}'
        with self.assertRaisesRegex(ValueError, "duplicate key"):
            json.loads(duplicate_outer, object_pairs_hook=_reject_duplicate_json_keys)

    def test_legacy_generated_response_with_transport_like_object_is_not_decoded(self):
        source = (
            "def operator(inputs, params, runtime):\n"
            "    return {'$transport': 'e4-typed-ir-result/1', 'codec': 'zlib+base64', "
            "'decoded_bytes': 0, 'sha256': '" + "0" * 64 + "', 'payload': ''}\n")
        result = run_generated_operator(source, {}, {}, {}, "object")
        self.assertTrue(result["ok"])
        self.assertEqual(result["output_validation"], "checked")
        self.assertEqual(result["result"]["$transport"], IR_RESULT_TRANSPORT_VERSION)

    def test_actual_bwrap_roundtrip_preserves_large_typed_scored_candidates(self):
        spec = _spec("variance_change")
        inputs = _high_cardinality_inputs()
        registry = OperatorRegistry()
        normalized = register_generated_specs(registry, [spec])[0]
        runtime = {**RUNTIME, "ir_family": normalized["family"],
            "ir_statistic": _ir_statistic(normalized["program"])}
        wire_inputs = {name: _encode(value) for name, value in inputs.items()}
        direct_wire = execute_ir(normalized["program"], wire_inputs, {}, runtime,
            trusted_runtime_context())
        direct_bytes = json.dumps(direct_wire, separators=(",", ":"), allow_nan=False).encode()
        self.assertGreater(len(direct_bytes), PROTOCOL_BYTES)
        self.assertLess(len(direct_bytes), _IR_RESULT_MAX_BYTES)
        measured = _encode_ir_success_response(direct_wire, protocol_bytes=PROTOCOL_BYTES,
            decoded_limit_bytes=_IR_RESULT_MAX_BYTES)
        self.assertLessEqual(len(measured), PROTOCOL_BYTES)
        self.assertEqual(json.loads(measured)["result"]["$transport"],
            IR_RESULT_TRANSPORT_VERSION)
        expected = deserialize_contract_json(direct_wire, "ScoredCandidates")

        operator = registry.require(normalized["name"], normalized["version"])
        from e4core.executor import NodeContext
        actual = operator.implementation(inputs, {}, NodeContext("fixture", "ir-transport",
            normalized["name"], RUNTIME, {}, REF, ALT, CASE_SHA))
        self.assertEqual(actual, expected)
        self.assertEqual(len(actual.candidates), 100)
        positive = [row for row in actual.candidates if row.score > 0]
        self.assertEqual(len(positive), 100)
        self.assertEqual(len([row for row in actual.candidates if row.evidence]), 100)
        self.assertTrue(all(row.evidence[0].support == 6 for row in positive))
        for row in positive:
            self.assertEqual(row.evidence[0].details["case_data_sha256"], CASE_SHA)
            self.assertEqual(dict(row.evidence[0].details["source_hashes"]), SOURCE_HASHES)


if __name__ == "__main__":
    unittest.main()
