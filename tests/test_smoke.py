import unittest
from unittest.mock import patch

from e4infra.smoke import (chat, extract_python, parse_json_answer,
                           validate_and_run_operator)


class SmokeHarnessTests(unittest.TestCase):
    def test_extracts_python_fence(self):
        self.assertEqual(extract_python("```python\ndef operator(data):\n return 1\n```"),
                         "def operator(data):\n return 1\n")

    def test_rejects_empty_and_non_operator_responses(self):
        with self.assertRaises(ValueError):
            extract_python("```python\n``` ")
        with self.assertRaises(ValueError):
            validate_and_run_operator("def other(data):\n    return 1\n", {"values": []})

    def test_generated_code_runs_in_subprocess_and_checks_output(self):
        source = "def operator(data):\n    return sum(data['values'])\n"
        self.assertEqual(validate_and_run_operator(source, {"values": [2, 3, 5]}), 10)

    def test_rejects_filesystem_escape_primitives(self):
        source = "def operator(data):\n    return open('/etc/passwd').read()\n"
        with self.assertRaisesRegex(ValueError, "may call only"):
            validate_and_run_operator(source, {"values": [1]})

    def test_json_response_rejects_malformed_and_truncated_answers(self):
        with self.assertRaises(ValueError):
            parse_json_answer("{broken", "stop")
        with self.assertRaisesRegex(ValueError, "truncated"):
            parse_json_answer('{"status":"ok"', "length")

    def test_chat_keeps_reasoning_separate_from_final_content(self):
        cfg = {"model_alias": "test", "temperature": 1.0, "top_p": 0.95,
               "top_k": 64, "seed": 42, "base_url": "http://127.0.0.1:8080"}
        response = {"choices": [{"message": {"reasoning_content": "thinking",
                                                "content": '{"status":"ok"}',
                                                }, "finish_reason": "stop"}],
                    "usage": {"completion_tokens": 7}, "timings": {"predicted_per_second": 33}}
        with patch("e4infra.smoke.request_json", return_value=response) as request:
            content, reasoning, usage, _, metadata = chat(cfg, "prompt", 64, 10)
        self.assertEqual(content, '{"status":"ok"}')
        self.assertEqual(reasoning, "thinking")
        self.assertEqual(usage["completion_tokens"], 7)
        self.assertEqual(metadata["finish_reason"], "stop")
        self.assertEqual(request.call_args.args[1]["top_k"], 64)

    def test_multiple_operator_inputs_reject_constant_answer(self):
        source = "def operator(data):\n    return 10\n"
        cases = [({"values": [2, 3, 5]}, 10), ({"values": [-4, 7]}, 3),
                 ({"values": []}, 0), ({"values": [11]}, 11)]
        results = [validate_and_run_operator(source, data) == expected
                   for data, expected in cases]
        self.assertEqual(results, [True, False, False, False])


if __name__ == "__main__":
    unittest.main()
