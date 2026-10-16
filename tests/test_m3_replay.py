import copy
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from e4core.benchmark import ROOT, validate_suite
from e4core.model_client import LocalModelClient, ModelCall
from e4core.replay import (ReplayError, TranscriptClient, audit_operator_oracles)


class M3OfflineReplayTests(unittest.TestCase):
    def test_corrected_suite_diff_is_exact_and_all_oracles_are_audited(self):
        v2 = json.loads((ROOT / "config/m3_synthetic_suite_v2.json").read_text())
        v3 = json.loads((ROOT / "config/m3_synthetic_suite_v3.json").read_text())
        validate_suite(v2)
        validate_suite(v3)
        projected = copy.deepcopy(v2)
        projected["schema_version"] = "e4-m3-synthetic-suite/3"
        projected["suite_id"] = "m3-synthetic-v3"
        operator = next(task for task in projected["tasks"]
                        if task["id"] == "operator_04_median_window_contrast")
        operator["checks"][1]["expected"]["delta"] = 2.0
        self.assertEqual(
            json.dumps(projected, sort_keys=True, separators=(",", ":"), allow_nan=False),
            json.dumps(v3, sort_keys=True, separators=(",", ":"), allow_nan=False))

        old_audit = audit_operator_oracles(v2)
        self.assertEqual(old_audit["operator_check_count"], 19)
        self.assertEqual(old_audit["mismatch_count"], 1)
        mismatch = old_audit["mismatches"][0]
        self.assertEqual((mismatch["task_id"], mismatch["case_index"]),
                         ("operator_04_median_window_contrast", 1))
        self.assertEqual(mismatch["independent_result"]["delta"], 2)
        self.assertEqual(mismatch["stored_expected"]["delta"], 1.0)
        new_audit = audit_operator_oracles(v3)
        self.assertEqual(new_audit["operator_check_count"], 19)
        self.assertEqual(new_audit["mismatch_count"], 0)

    def _profile(self):
        return {"model_alias": "fixture", "temperature": 0.2, "top_p": 0.9,
                "top_k": 16, "seed": 42, "max_generation_tokens": 128,
                "context_tokens": 4096, "reasoning_mode": "off"}

    def _call_row(self, prompt):
        profile = self._profile()
        client = LocalModelClient(profile, base_url="http://127.0.0.1:8099")
        payload = client.make_payload(prompt)
        call = ModelCall(
            call_id="fixture-a1", task_id="schema_01", attempt=1,
            started_utc="2026-01-01T00:00:00+00:00", finished_utc="2026-01-01T00:00:01+00:00",
            elapsed_seconds=1.0, generation_elapsed_seconds=0.8, prompt_token_count=50,
            context_tokens=4096, preflight_calls=(), request_payload=payload,
            prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest(), http_status=200,
            raw_response_body='{"choices":[]}', raw_response={"choices": []},
            content='{"status":"ok"}', reasoning="reasoning", usage={"prompt_tokens": 50,
            "completion_tokens": 4}, response_metadata={"finish_reason": "stop"}, error=None)
        return {"record_type": "model_call", **call.as_dict()}

    def test_transcript_replay_verifies_prompt_and_payload_without_http(self):
        prompt = "Return strict JSON {\"status\":\"ok\"}."
        row = self._call_row(prompt)
        client = TranscriptClient([row], self._profile(), "http://127.0.0.1:8099", 4096, 128)
        with patch("urllib.request.urlopen", side_effect=AssertionError("HTTP must remain disabled")):
            call = client.complete(prompt, task_id="schema_01", attempt=1)
        client.require_consumed()
        self.assertEqual(call.call_id, row["call_id"])
        self.assertEqual(client.verified_prompts, 1)
        self.assertEqual(client.verified_payloads, 1)

    def test_transcript_rejects_prompt_and_payload_mismatch(self):
        prompt = "exact captured prompt"
        row = self._call_row(prompt)
        client = TranscriptClient([row], self._profile(), "http://127.0.0.1:8099", 4096, 128)
        with self.assertRaisesRegex(ReplayError, "prompt mismatch"):
            client.complete("altered prompt", task_id="schema_01", attempt=1)

        row = self._call_row(prompt)
        row["request_payload"]["temperature"] = 0.8
        client = TranscriptClient([row], self._profile(), "http://127.0.0.1:8099", 4096, 128)
        with self.assertRaisesRegex(ReplayError, "payload mismatch"):
            client.complete(prompt, task_id="schema_01", attempt=1)

        row = self._call_row(prompt)
        row["request_payload"]["top_k"] = 16.0
        client = TranscriptClient([row], self._profile(), "http://127.0.0.1:8099", 4096, 128)
        self.assertEqual(client.model_client.make_payload(prompt)["top_k"], 16)
        with self.assertRaisesRegex(ReplayError, "payload mismatch"):
            client.complete(prompt, task_id="schema_01", attempt=1)

    def test_transcript_rejects_call_order_and_unused_rows(self):
        row = self._call_row("prompt")
        client = TranscriptClient([row], self._profile(), "http://127.0.0.1:8099", 4096, 128)
        with self.assertRaisesRegex(ReplayError, "call order mismatch"):
            client.complete("prompt", task_id="other", attempt=1)
        with self.assertRaisesRegex(ReplayError, "unused model transcript"):
            TranscriptClient([row], self._profile(), "http://127.0.0.1:8099", 4096, 128).require_consumed()


if __name__ == "__main__":
    unittest.main()
