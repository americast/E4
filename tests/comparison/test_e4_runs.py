import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from e4.comparison.e4_runs import M5TrustedFeedback, validate_staged_task_view
from scripts.comparison import e4_case
from scripts.bank.diagnose import (_bwrap_command, BinaryFeedbackProxy,
                                   serve_feedback_request)
from scripts.comparison.e4_case import _write_case_result


ROOT = Path(__file__).resolve().parents[2]
DEV_CASE = "bank-20210323T0630-utc8"


def _ranked_public_answer():
    from e4.contracts import EntityRef, RankedAnswer, ScoredCandidate
    return RankedAnswer((ScoredCandidate(EntityRef("component", "public-id"), 1.0, None, ()),))


def _read_exact(stream, length):
    data = bytearray()
    while len(data) < length:
        block = stream.recv(length - len(data))
        if not block:
            raise EOFError("test server got an incomplete frame")
        data.extend(block)
    return bytes(data)


class M5E4RunnerTests(unittest.TestCase):
    def test_runner_help_imports_from_a_clean_environment(self):
        with tempfile.TemporaryDirectory() as temp:
            env = {"PATH": os.environ.get("PATH", "")}
            result = subprocess.run([str(ROOT / ".venv/bin/python"),
                str(ROOT / "scripts/comparison/e4_case.py"), "--help"], cwd=temp,
                env=env, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("--experiment-manifest", result.stdout)

    def test_bwrap_forwards_scored_split_only_through_m5_mode(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            for mode, split, case_id in (("m5-diagnostic", "scored", "bank-20210304T0100-utc8"),
                                         ("diagnostic", "dev", DEV_CASE)):
                command = _bwrap_command(view_root=base, run_dir=base,
                    library_root=None, model_name="gemma", mode=mode,
                    case_id=case_id, split=split,
                    feedback_socket="e4m4-test", implementation_fingerprint_value="a" * 64)
                inner = command[command.index("--") + 1:]
                if mode == "m5-diagnostic":
                    self.assertIn("--split", inner)
                    self.assertEqual(inner[inner.index("--split") + 1], "scored")
                else:
                    self.assertNotIn("--split", inner)

    def test_feedback_ipc_accepts_frozen_ids_for_both_splits_without_loading_truth(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            for case_id in (DEV_CASE, "bank-20210304T0100-utc8"):
                received = []
                parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
                def evaluate_ids(selected, ids):
                    received.append((selected, ids))
                    return False
                def trusted_server():
                    serve_feedback_request(child, case_id=case_id,
                        evaluate_ids=evaluate_ids,
                        audit_path=base / (case_id + ".jsonl"))
                thread = threading.Thread(target=trusted_server)
                thread.start()
                proxy = BinaryFeedbackProxy(parent)
                answer = proxy.evaluate(case_id, _ranked_public_answer())
                proxy.close()
                thread.join(timeout=5)
                child.close()
                self.assertFalse(thread.is_alive())
                self.assertEqual(answer, {"accepted": False})
                self.assertEqual(received, [(case_id, ["public-id"])])

    def test_remote_shared_evaluator_receives_raw_ids_and_returns_only_boolean(self):
        case_id = "bank-20210304T0100-utc8"
        response = json.dumps({"accepted": True}, separators=(",", ":")).encode()
        class FakeSocket:
            def __init__(self):
                self.sent = b""
                self.remaining = struct.pack(">I", len(response)) + response
                self.connected_to = None
            def __enter__(self): return self
            def __exit__(self, *_args): return None
            def connect(self, path): self.connected_to = path
            def sendall(self, payload): self.sent += payload
            def recv(self, count):
                chunk, self.remaining = self.remaining[:count], self.remaining[count:]
                return chunk
        fake_socket = FakeSocket()
        with patch("e4.comparison.e4_runs.socket.socket", return_value=fake_socket), \
             patch("e4.comparison.e4_runs.load_bank_case_view", return_value=SimpleNamespace(
                    case_id=case_id, component_ids=("public-id",))):
            evaluator = M5TrustedFeedback(root=ROOT, case_id=case_id, split="scored",
                trusted_evaluator_socket=Path("/tmp/trusted.sock"))
            result = evaluator.evaluate(case_id, _ranked_public_answer())
        size = struct.unpack(">I", fake_socket.sent[:4])[0]
        self.assertEqual(fake_socket.connected_to, "/tmp/trusted.sock")
        self.assertEqual(json.loads(fake_socket.sent[4:4 + size]), {"case_id": case_id,
            "raw_answer": {"ranked_components": ["public-id"]}})
        self.assertEqual(result, {"accepted": True})

    def test_staged_public_view_allows_only_hash_pinned_alert_sidecar(self):
        case_id = "bank-20210304T0100-utc8"
        csv_bytes = b"timestamp,cmdb_id\n1,public-id\n"
        alert_bytes = b'{"incident_id":"bank-20210304T0100-utc8"}\n'
        def sha(data):
            import hashlib
            return hashlib.sha256(data).hexdigest()
        public_view = SimpleNamespace(telemetry_file_sha256={"metric/metric_app.csv": sha(csv_bytes)})
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            staged = root / "public_case"
            (staged / "metric").mkdir(parents=True)
            (staged / "metric/metric_app.csv").write_bytes(csv_bytes)
            (staged / "alert.json").write_bytes(alert_bytes)
            (root / "config/bank").mkdir(parents=True)
            (root / "config/bank/input_manifest.json").write_text(json.dumps({
                "cases": {case_id: {"alert_sha256": sha(alert_bytes)}}}), encoding="utf-8")
            with patch("e4.comparison.e4_runs.load_bank_case_view", return_value=public_view):
                digest = validate_staged_task_view(staged, root=root, case_id=case_id, split="scored")
            self.assertRegex(digest, r"^[0-9a-f]{64}$")
            (staged / "unexpected.txt").write_text("extra", encoding="utf-8")
            with patch("e4.comparison.e4_runs.load_bank_case_view", return_value=public_view), \
                 self.assertRaisesRegex(ValueError, "differs from the selected pinned"):
                validate_staged_task_view(staged, root=root, case_id=case_id, split="scored")

    def test_scored_cli_requires_a_frozen_experiment_manifest(self):
        with self.assertRaisesRegex(SystemExit, "experiment-manifest"):
            e4_case.main(["--case-id", "bank-20210304T0100-utc8",
                "--split", "scored", "--run-dir", "case", "--library-root", "library"])

    def test_dev_feedback_and_three_stage_slots_keep_original_stage_indices(self):
        with patch("e4.comparison.e4_runs.load_bank_case_view", return_value=SimpleNamespace(
                case_id=DEV_CASE, component_ids=("synthetic-A", "synthetic-B"))), \
             patch("e4.comparison.e4_runs.load_development_truth_ids", return_value=("synthetic-A",)):
            feedback = M5TrustedFeedback(root=ROOT, case_id=DEV_CASE, split="dev")
            self.assertTrue(feedback.evaluate_ids(DEV_CASE, ["synthetic-A"]))
            with tempfile.TemporaryDirectory() as temp:
                run_dir = Path(temp)
                (run_dir / "diagnostic_result.json").write_text(json.dumps({"stages": [
                    {"status": "static_failed"}, {"status": "accepted"}]}), encoding="utf-8")
                slots = feedback.write_attempt_slots(run_dir)
                self.assertEqual([row["attempt"] for row in slots], [1, 2, 3])
                self.assertEqual([row["status"] for row in slots], ["failed", "submitted", "not_run"])
                self.assertEqual([row["accepted"] for row in slots], [False, True, False])
                self.assertIsNone(slots[2]["valid"])
                self.assertTrue((run_dir / "m5_attempt_slots.json").is_file())

    def test_timeout_keeps_partial_trusted_feedback_and_original_stage_positions(self):
        with patch("e4.comparison.e4_runs.load_bank_case_view", return_value=SimpleNamespace(
                case_id=DEV_CASE, component_ids=("synthetic-A", "synthetic-B"))), \
             patch("e4.comparison.e4_runs.load_development_truth_ids", return_value=("synthetic-A",)):
            feedback = M5TrustedFeedback(root=ROOT, case_id=DEV_CASE, split="dev")
            self.assertFalse(feedback.evaluate_ids(DEV_CASE, ["synthetic-B"]))
            self.assertTrue(feedback.evaluate_ids(DEV_CASE, ["synthetic-A"]))
            with tempfile.TemporaryDirectory() as temp:
                run_dir = Path(temp)
                for name in ("stage1_select", "stage2_compose"):
                    (run_dir / "attempts" / name).mkdir(parents=True)
                slots = feedback.write_attempt_slots(run_dir, case_timeout=True)
                self.assertEqual([slot["status"] for slot in slots],
                                 ["submitted", "submitted", "not_run"])
                self.assertEqual([slot["accepted"] for slot in slots], [False, True, False])

    def test_stable_case_result_distinguishes_terminal_diagnostic_from_infrastructure_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_dir = root / "case"
            run_dir.mkdir()
            previous_run_root = os.environ.get("E4_RUN_DIR")
            os.environ["E4_RUN_DIR"] = str(root)
            try:
                (run_dir / "diagnostic").mkdir()
                (run_dir / "diagnostic/diagnostic_result.json").write_text(json.dumps({
                    "status": "exhausted_or_failed", "accepted": False,
                    "model_calls": {"model_call_count": 2, "prompt_tokens": 100,
                                    "completion_tokens": 50, "model_elapsed_seconds": 12.5},
                    "query_budget": {"requests": 0}}), encoding="utf-8")
                (run_dir / "supervisor_manifest.json").write_text(json.dumps({
                    "exit_code": 0, "supervisor_wall_seconds": 30.0,
                    "attempt_slots": [{"attempt": 1, "status": "failed", "accepted": False,
                                       "valid": False, "correct_rank": None},
                                      {"attempt": 2, "status": "submitted", "accepted": False,
                                       "valid": True, "correct_rank": None},
                                      {"attempt": 3, "status": "not_run", "accepted": False,
                                       "valid": None, "correct_rank": None}]}), encoding="utf-8")
                args = SimpleNamespace(run_dir=run_dir, case_id=DEV_CASE, split="dev")
                result = _write_case_result(args, 0)
                self.assertEqual(result["terminal_classification"], "diagnostic_complete")
                self.assertEqual(result["attempts"][1]["status"], "submitted")
                self.assertEqual(result["cost"]["model_seconds"], 12.5)
                self.assertEqual(result["cost"]["cpu_seconds"], None)
                self.assertTrue((run_dir / "m5_case_result.json").is_file())

                infra_dir = root / "infra"
                infra_dir.mkdir()
                (infra_dir / "supervisor_manifest.json").write_text(json.dumps({
                    "exit_code": 1, "supervisor_wall_seconds": 3.0,
                    "status": "failed"}), encoding="utf-8")
                infra = _write_case_result(SimpleNamespace(run_dir=infra_dir,
                    case_id=DEV_CASE, split="dev"), 1)
                self.assertEqual(infra["terminal_classification"], "infrastructure_incomplete")
                self.assertTrue(infra["infrastructure_error"])
                self.assertEqual([slot["status"] for slot in infra["attempts"]],
                                 ["not_run", "not_run", "not_run"])

                timeout_dir = root / "timeout"
                timeout_dir.mkdir()
                (timeout_dir / "supervisor_manifest.json").write_text(json.dumps({
                    "exit_code": 1, "case_wall_timeout": True,
                    "supervisor_wall_seconds": 7200.0,
                    "attempt_slots": [{"attempt": 1, "status": "submitted", "accepted": False,
                                       "valid": None, "correct_rank": None},
                                      {"attempt": 2, "status": "failed", "accepted": False,
                                       "valid": False, "correct_rank": None},
                                      {"attempt": 3, "status": "not_run", "accepted": False,
                                       "valid": None, "correct_rank": None}]}), encoding="utf-8")
                timeout = _write_case_result(SimpleNamespace(run_dir=timeout_dir,
                    case_id=DEV_CASE, split="dev"), 0)
                self.assertEqual(timeout["terminal_classification"], "diagnostic_complete")
                self.assertEqual(timeout["status"], "case_wall_timeout")
                self.assertEqual(timeout["attempts"][0]["status"], "submitted")
                self.assertIsNone(timeout["cost"]["model_calls"])
                self.assertEqual(timeout["cost"]["wall_seconds"], 7200.0)
            finally:
                if previous_run_root is None:
                    os.environ.pop("E4_RUN_DIR", None)
                else:
                    os.environ["E4_RUN_DIR"] = previous_run_root


if __name__ == "__main__":
    unittest.main()
