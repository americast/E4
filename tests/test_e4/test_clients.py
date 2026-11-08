"""HTTP contract tests for the ready OpenAI Responses and Ollama clients."""

from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from e4.clients import ModelClientError, OllamaClient, OpenAIClient


def _openai_response(text: str, *, status: str = "completed", output: list[dict] | None = None) -> dict:
    return {
        "id": "resp_mock",
        "status": status,
        "output": output if output is not None else [{
            "type": "message",
            "content": [{"type": "output_text", "text": text}],
        }],
    }


def _ollama_response(content: str, *, done: bool = True, done_reason: str = "stop", thinking: str = "private reasoning") -> dict:
    return {
        "model": "mock-model",
        "message": {"role": "assistant", "content": content, "thinking": thinking},
        "done": done,
        "done_reason": done_reason,
    }


class _MockServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        self.requests: list[dict] = []
        self.responses: list[dict] = []
        super().__init__(("127.0.0.1", 0), _MockHandler)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"


class _MockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            body = None
        self.server.requests.append({
            "path": self.path,
            "headers": {key.lower(): value for key, value in self.headers.items()},
            "body": body,
            "raw": raw,
        })
        spec = self.server.responses.pop(0) if self.server.responses else {
            "status": 200,
            "payload": _openai_response('{"ok":true}'),
        }
        payload = spec.get("payload")
        encoded = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        self.send_response(spec.get("status", 200))
        self.send_header("Content-Type", spec.get("content_type", "application/json"))
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt, *args):
        pass


class ProviderClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _MockServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _openai(self, **kwargs) -> OpenAIClient:
        return OpenAIClient(
            model="mock-openai-model",
            base_url=self.server.base_url + "/v1",
            **kwargs,
        )

    def _ollama(self, **kwargs) -> OllamaClient:
        return OllamaClient(
            model="mock-ollama-model",
            base_url=self.server.base_url,
            **kwargs,
        )

    def test_openai_responses_body_auth_and_output_text_only(self) -> None:
        self.server.responses.append({"status": 200, "payload": _openai_response(
            "ignored",
            output=[
                {"type": "reasoning", "summary": "do not return this"},
                {"type": "message", "content": [
                    {"type": "output_text", "text": '{"answer":'},
                    {"type": "output_text", "text": "true}"},
                ]},
            ],
        )})
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-openai-secret"}):
            client = self._openai(api_key=None, max_output_tokens=321, timeout=3, json_mode=True)
            answer = client.complete("diagnostic prompt")

        self.assertEqual(answer, '{"answer":true}')
        request = self.server.requests[0]
        self.assertEqual(request["path"], "/v1/responses")
        self.assertEqual(request["headers"]["authorization"], "Bearer test-openai-secret")
        self.assertEqual(request["body"]["model"], "mock-openai-model")
        self.assertEqual(request["body"]["input"], "diagnostic prompt")
        self.assertTrue(request["body"].get("instructions"))
        self.assertEqual(request["body"]["max_output_tokens"], 321)
        self.assertEqual(request["body"]["text"]["format"]["type"], "json_object")
        self.assertFalse(request["body"]["store"])
        self.assertFalse(request["body"]["stream"])
        self.assertNotIn("do not return this", answer)

    def test_ollama_uses_host_fallback_json_budget_and_does_not_send_openai_secret(self) -> None:
        self.server.responses.append({"status": 200, "payload": _ollama_response('{"ok":true}')})
        with patch.dict(os.environ, {"OLLAMA_HOST": self.server.base_url, "OPENAI_API_KEY": "must-not-leak"}):
            client = OllamaClient(model="mock-ollama-model", max_output_tokens=777, context_tokens=4096, timeout=3)
            answer = client.complete("ollama prompt")

        self.assertEqual(answer, '{"ok":true}')
        request = self.server.requests[0]
        self.assertEqual(request["path"], "/api/chat")
        self.assertNotIn("authorization", request["headers"])
        self.assertEqual(request["body"]["model"], "mock-ollama-model")
        self.assertEqual(request["body"]["messages"], [{"role": "user", "content": "ollama prompt"}])
        self.assertFalse(request["body"]["stream"])
        self.assertEqual(request["body"]["format"], "json")
        self.assertEqual(request["body"]["options"]["num_ctx"], 4096)
        self.assertEqual(request["body"]["options"]["num_predict"], 777)
        self.assertNotIn("private reasoning", answer)

    def test_identical_prompts_make_fresh_calls_without_response_caching(self) -> None:
        self.server.responses.extend([
            {"status": 200, "payload": _openai_response('{"n":1}')},
            {"status": 200, "payload": _openai_response('{"n":2}')},
            {"status": 200, "payload": _ollama_response('{"n":3}')},
            {"status": 200, "payload": _ollama_response('{"n":4}')},
        ])
        openai = self._openai(api_key="explicit-key")
        self.assertEqual(openai.complete("same"), '{"n":1}')
        self.assertEqual(openai.complete("same"), '{"n":2}')
        ollama = self._ollama()
        self.assertEqual(ollama.complete("same"), '{"n":3}')
        self.assertEqual(ollama.complete("same"), '{"n":4}')
        self.assertEqual(len(self.server.requests), 4)

    def test_openai_explicit_key_overrides_environment_key(self) -> None:
        self.server.responses.append({"status": 200, "payload": _openai_response('{"ok":true}')})
        with patch.dict(os.environ, {"OPENAI_API_KEY": "environment-key"}):
            self._openai(api_key="explicit-key").complete("prompt")
        self.assertEqual(self.server.requests[0]["headers"]["authorization"], "Bearer explicit-key")

    def test_json_mode_requires_object_output_and_text_mode_returns_plain_text(self) -> None:
        self.server.responses.extend([
            {"status": 200, "payload": _openai_response("[1,2]")},
            {"status": 200, "payload": _ollama_response("[1,2]")},
            {"status": 200, "payload": _openai_response("plain OpenAI text")},
            {"status": 200, "payload": _ollama_response("plain Ollama text")},
        ])
        with self.assertRaises(ModelClientError) as openai_error:
            self._openai(api_key="key").complete("prompt")
        with self.assertRaises(ModelClientError) as ollama_error:
            self._ollama().complete("prompt")
        self.assertEqual(openai_error.exception.stage, "output_json")
        self.assertEqual(ollama_error.exception.stage, "output_json")

        self.assertEqual(self._openai(api_key="key", json_mode=False).complete("prompt"), "plain OpenAI text")
        self.assertNotIn("text", self.server.requests[2]["body"])
        self.assertEqual(self._ollama(json_mode=False).complete("prompt"), "plain Ollama text")
        self.assertNotIn("format", self.server.requests[3]["body"])

    def test_http_auth_and_not_found_errors_have_redacted_structured_details(self) -> None:
        secret = "sk-test-secret-value"
        self.server.responses.extend([
            {"status": 401, "payload": {"error": {"message": f"invalid Bearer {secret}"}}},
            {"status": 404, "payload": {"error": "ollama route not found"}},
        ])
        with self.assertRaises(ModelClientError) as auth_error:
            self._openai(api_key=secret).complete("prompt")
        self.assertEqual(auth_error.exception.status_code, 401)
        self.assertEqual(auth_error.exception.stage, "http")
        self.assertNotIn(secret, str(auth_error.exception))
        self.assertNotIn(secret, json.dumps(auth_error.exception.as_dict()))

        with self.assertRaises(ModelClientError) as not_found:
            self._ollama().complete("prompt")
        self.assertEqual(not_found.exception.status_code, 404)
        self.assertEqual(not_found.exception.stage, "http")

    def test_transport_timeout_and_network_errors_are_structured_without_requests(self) -> None:
        with patch("e4.clients.urllib.request.urlopen", side_effect=TimeoutError("socket timed out")):
            with self.assertRaises(ModelClientError) as timeout_error:
                self._openai(api_key="key").complete("prompt")
        self.assertEqual(timeout_error.exception.stage, "transport")
        self.assertEqual(timeout_error.exception.provider, "OpenAI")

        with patch("e4.clients.urllib.request.urlopen", side_effect=OSError("connection refused")):
            with self.assertRaises(ModelClientError) as network_error:
                self._ollama().complete("prompt")
        self.assertEqual(network_error.exception.stage, "transport")
        self.assertEqual(network_error.exception.provider, "Ollama")
        self.assertEqual(self.server.requests, [])

    def test_openai_error_incomplete_refusal_empty_and_truncated_json_fail(self) -> None:
        failing_payloads = [
            ({"status": 500, "payload": {"error": {"message": "Bearer explicit-key leaked"}}}, "http", 500),
            ({"status": 200, "payload": {"error": {"message": "provider failure"}}}, "provider", None),
            ({"status": 200, "payload": _openai_response('{"partial":', status="incomplete")}, "incomplete", None),
            ({"status": 200, "payload": _openai_response("", output=[{"type": "message", "content": [{"type": "refusal", "refusal": "blocked"}]}])}, "refusal", None),
            ({"status": 200, "payload": _openai_response('{"partial":')}, "output_json", None),
            ({"status": 200, "payload": b"not-json", "content_type": "application/json"}, "response_json", None),
        ]
        for response, stage, status_code in failing_payloads:
            with self.subTest(response=response), self.assertRaises(ModelClientError) as caught:
                self.server.responses.append(response)
                self._openai(api_key="explicit-key").complete("request")
            self.assertEqual(caught.exception.provider, "OpenAI")
            self.assertEqual(caught.exception.stage, stage)
            self.assertEqual(caught.exception.status_code, status_code)
            self.assertNotIn("explicit-key", json.dumps(caught.exception.details))

    def test_ollama_error_incomplete_length_empty_and_truncated_json_fail(self) -> None:
        failing_payloads = [
            ({"status": 500, "payload": {"error": "provider failure"}}, "http"),
            ({"status": 200, "payload": {"error": "model unavailable"}}, "provider"),
            ({"status": 200, "payload": _ollama_response('{"partial":', done=False)}, "incomplete"),
            ({"status": 200, "payload": _ollama_response('{"partial":', done_reason="length")}, "incomplete"),
            ({"status": 200, "payload": _ollama_response("")}, "empty"),
            ({"status": 200, "payload": _ollama_response('{"partial":')}, "output_json"),
            ({"status": 200, "payload": b"not-json", "content_type": "application/json"}, "response_json"),
        ]
        for response, stage in failing_payloads:
            with self.subTest(response=response), self.assertRaises(ModelClientError) as caught:
                self.server.responses.append(response)
                self._ollama().complete("request")
            self.assertEqual(caught.exception.provider, "Ollama")
            self.assertEqual(caught.exception.stage, stage)

    def test_bad_settings_and_missing_key_fail_before_external_call(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                self._openai(api_key=None)
        for kwargs in (
            {"api_key": "key", "max_output_tokens": 0},
            {"api_key": "key", "timeout": 0},
            {"api_key": "", "max_output_tokens": 10},
            {"api_key": "key", "model": ""},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                params = dict(kwargs)
                model = params.pop("model", "mock-openai-model")
                OpenAIClient(model=model, base_url=self.server.base_url + "/v1", **params)
        with self.assertRaises(ValueError):
            self._ollama(max_output_tokens=0)
        with self.assertRaises(ValueError):
            self._ollama(context_tokens=8192, max_output_tokens=8192)
        self.assertEqual(self.server.requests, [])


if __name__ == "__main__":
    unittest.main()
