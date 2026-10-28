import unittest
import json
import socket
import struct
import errno
import tempfile
from pathlib import Path

from e4.comparison.evaluator import (EvaluationSession, EvaluationSetupError,
                                     TrustedFeedbackServer)


def session():
    return EvaluationSession(case_id="bank-20210323T0630-utc8",
                             allowed_component_ids=["node_a", "node_b", "node_c"],
                             truth_ids=["node_b"])


class EvaluationTests(unittest.TestCase):
    def test_binary_feedback_validation_and_early_stop(self):
        evaluator = session()
        self.assertEqual(evaluator.submit({"ranked_components": ["node_a", "node_b"]}),
                         {"accepted": True})
        self.assertEqual(evaluator.attempts, 1)
        self.assertTrue(evaluator.terminal)
        self.assertEqual(evaluator.trusted_ledger()[0]["correct_rank"], 2)
        with self.assertRaises(RuntimeError):
            evaluator.submit(["node_b"])

    def test_malformed_or_unknown_answers_consume_failed_slots(self):
        answers = [{"ranked_components": ["node_a", "NODE_A"]},
                   {"ranked_components": ["not_in_catalog"]},
                   {"ranked_components": ["node_a", "node_b", "node_c", "node_a"]},
                   "not json shaped"]
        for answer in answers:
            with self.subTest(answer=answer):
                evaluator = session()
                self.assertEqual(evaluator.submit(answer), {"accepted": False})
                self.assertEqual(evaluator.attempts, 1)
                self.assertFalse(evaluator.trusted_ledger()[0]["valid"])

    def test_rejected_answers_consume_three_submission_budget(self):
        evaluator = session()
        for _ in range(3):
            self.assertEqual(evaluator.submit(["node_a"]), {"accepted": False})
        self.assertTrue(evaluator.terminal)
        with self.assertRaises(RuntimeError):
            evaluator.submit(["node_b"])

    def test_missing_truth_aborts_but_unobserved_target_is_valid_unsolved_case(self):
        with self.assertRaises(EvaluationSetupError):
            EvaluationSession(case_id="bank-20210323T0630-utc8",
                              allowed_component_ids=["node_a"], truth_ids=[])
        evaluator = EvaluationSession(case_id="bank-20210323T0630-utc8",
                                      allowed_component_ids=["node_a"], truth_ids=["node_b"])
        self.assertEqual(evaluator.submit(["node_a"]), {"accepted": False})
        self.assertIsNone(evaluator.trusted_ledger()[0]["correct_rank"])

    def test_trusted_socket_returns_only_one_binary_field(self):
        evaluator = session()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trusted.sock"
            try:
                server_context = TrustedFeedbackServer(path, evaluator)
                server_context.__enter__()
            except PermissionError as exc:
                if exc.errno == errno.EPERM:
                    self.skipTest("sandbox does not permit AF_UNIX socket creation")
                raise
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.connect(str(path))
                    request = json.dumps({"case_id": evaluator.case_id,
                                          "raw_answer": ["node_b"]}, separators=(",", ":")).encode()
                    client.sendall(struct.pack(">I", len(request)) + request)
                    size = struct.unpack(">I", client.recv(4))[0]
                    response = json.loads(client.recv(size))
            finally:
                server_context.__exit__(None, None, None)
            self.assertEqual(response, {"accepted": True})
            self.assertEqual(evaluator.trusted_ledger()[0]["correct_rank"], 1)


if __name__ == "__main__":
    unittest.main()
