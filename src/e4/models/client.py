"""OpenAI-compatible chat client for one AnalysisModel, with complete call provenance."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from e4.models.analysis_model import AnalysisModel, api_key


SYSTEM_PROMPT = (
    "Complete the requested E4 synthetic benchmark task. Return only the requested final artifact. "
    "Use no external tools, files, services, or unstated data. Keep private reasoning separate from the final output."
)


@dataclass(frozen=True)
class ModelCall:
    call_id: str
    task_id: str
    attempt: int
    started_utc: str
    finished_utc: str
    elapsed_seconds: float
    generation_elapsed_seconds: float
    prompt_token_count: int | None
    context_tokens: int
    preflight_calls: tuple[Mapping[str, Any], ...]
    request_payload: Mapping[str, Any]
    prompt_sha256: str
    http_status: int | None
    raw_response_body: str | None
    raw_response: Any
    content: str | None
    reasoning: str | None
    usage: Mapping[str, Any]
    response_metadata: Mapping[str, Any]
    error: Mapping[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        generated = self.usage.get("completion_tokens", self.usage.get("output_tokens"))
        value["output_tokens_per_second"] = (
            float(generated) / self.generation_elapsed_seconds
            if type(generated) in (int, float) and self.generation_elapsed_seconds > 0 else None)
        return value


class ModelClient:
    """Serial chat client for one model; each call retains exact request and response data."""

    def __init__(self, model: AnalysisModel, *, request_timeout: float | None = None):
        self.model = model
        self.base_url = model.base_url
        self.request_timeout = float(request_timeout or model.request_timeout_seconds)
        self.context_tokens = model.context_tokens
        self.max_generation_tokens = model.max_generation_tokens
        if self.request_timeout <= 0:
            raise ValueError("request timeout must be positive")

    def make_payload(self, prompt: str, *, max_tokens: int | None = None) -> dict[str, Any]:
        generation_limit = int(max_tokens if max_tokens is not None else self.max_generation_tokens)
        if generation_limit <= 0 or generation_limit > self.max_generation_tokens:
            raise ValueError("per-call generation limit must be positive and within the frozen profile budget")
        payload: dict[str, Any] = {
            "model": self.model.model_alias,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                         {"role": "user", "content": prompt}],
        }
        # sampling fields the model file leaves null are left out of the request
        for name, value in (("temperature", self.model.temperature), ("top_p", self.model.top_p),
                            ("top_k", self.model.top_k), ("max_tokens", generation_limit),
                            ("seed", self.model.seed)):
            if value is not None:
                payload[name] = value
        # Preserve each model's frozen reasoning mode. `auto` leaves its native template alone.
        if self.model.reasoning_mode in {"on", "off"}:
            payload["chat_template_kwargs"] = {"enable_thinking": self.model.reasoning_mode == "on"}
        return payload

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.model.api_key_env is not None:
            headers["Authorization"] = f"Bearer {api_key(self.model)}"
        return headers

    def complete(self, prompt: str, *, task_id: str, attempt: int,
                 max_tokens: int | None = None) -> ModelCall:
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt must be non-empty text")
        import hashlib
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        payload = self.make_payload(prompt, max_tokens=max_tokens)
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + "/v1/chat/completions", data=body, headers=self._headers(),
            method="POST")
        call_id = f"{task_id}-a{attempt}-{uuid.uuid4().hex[:12]}"
        started_utc = datetime.now(timezone.utc).isoformat()
        started = time.perf_counter()
        preflight_calls: list[Mapping[str, Any]] = []
        prompt_token_count: int | None = None
        generation_elapsed = 0.0
        status: int | None = None
        raw_body: str | None = None
        raw_response: Any = None
        content: str | None = None
        reasoning: str | None = None
        usage: Mapping[str, Any] = {}
        metadata: Mapping[str, Any] = {}
        error: Mapping[str, Any] | None = None
        try:
            preflight_started = time.perf_counter()
            if self.model.token_counting == "llama_server":
                prompt_token_count, error = self._count_prompt_tokens(payload, preflight_calls)
            preflight_elapsed = time.perf_counter() - preflight_started
            if error is not None:
                elapsed = time.perf_counter() - started
                return ModelCall(
                    call_id=call_id, task_id=task_id, attempt=attempt,
                    started_utc=started_utc,
                    finished_utc=datetime.now(timezone.utc).isoformat(),
                    elapsed_seconds=elapsed, generation_elapsed_seconds=0.0,
                    prompt_token_count=prompt_token_count, context_tokens=self.context_tokens,
                    preflight_calls=tuple(preflight_calls), request_payload=payload,
                    prompt_sha256=prompt_hash, http_status=None, raw_response_body=None,
                    raw_response=None, content=None, reasoning=None, usage={},
                    response_metadata={"preflight_elapsed_seconds": preflight_elapsed},
                    error=error)

            generation_started = time.perf_counter()
            try:
                response = urllib.request.urlopen(request, timeout=self.request_timeout)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                status = int(response.status)
                raw_bytes = response.read()
            raw_body = raw_bytes.decode("utf-8", "replace")
            generation_elapsed = time.perf_counter() - generation_started
            try:
                raw_response = json.loads(raw_body)
            except json.JSONDecodeError as exc:
                error = {"code": "invalid_http_json", "message": str(exc), "http_status": status}
            else:
                if status is None or not 200 <= status < 300:
                    error = {"code": "http_status", "message": f"HTTP {status}", "http_status": status}
                elif type(raw_response) is not dict:
                    error = {"code": "invalid_chat_response", "message": "response must be a JSON object"}
                else:
                    choices = raw_response.get("choices")
                    if not isinstance(choices, list) or not choices or type(choices[0]) is not dict:
                        error = {"code": "invalid_chat_response", "message": "response has no first choice"}
                    else:
                        choice = choices[0]
                        message = choice.get("message")
                        if type(message) is not dict:
                            error = {"code": "invalid_chat_response", "message": "first choice has no message object"}
                        else:
                            content = _content_text(message.get("content"))
                            reasoning_value = message.get("reasoning_content", message.get("reasoning"))
                            reasoning = _content_text(reasoning_value) if reasoning_value is not None else None
                            usage_value = raw_response.get("usage", {})
                            usage = usage_value if isinstance(usage_value, dict) else {}
                            metadata = {"finish_reason": choice.get("finish_reason"),
                                        "timings": raw_response.get("timings"),
                                        "response_id": raw_response.get("id"),
                                        "model": raw_response.get("model")}
                            if content is None:
                                error = {"code": "invalid_chat_response", "message": "message content is not text"}
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            error = {"code": "transport_error", "message": f"{type(exc).__name__}: {exc}"}
        except Exception as exc:
            error = {"code": "client_error", "message": f"{type(exc).__name__}: {exc}"}
        elapsed = time.perf_counter() - started
        return ModelCall(call_id, task_id, attempt, started_utc,
                         datetime.now(timezone.utc).isoformat(), elapsed, generation_elapsed,
                         prompt_token_count, self.context_tokens, tuple(preflight_calls),
                         payload, prompt_hash, status, raw_body, raw_response,
                         content, reasoning, usage, metadata, error)

    def _count_prompt_tokens(self, payload: Mapping[str, Any],
                             preflight_calls: list[Mapping[str, Any]]) -> tuple[int | None, Mapping[str, Any] | None]:
        """llama-server renders the chat template and tokenizes it, so the exact prompt size is
        checked against the context budget before generating."""
        prompt_token_count: int | None = None
        error: Mapping[str, Any] | None = None
        apply_request = dict(payload)
        apply_status, apply_body, apply_response, apply_error = self._post_json(
            "/apply-template", apply_request)
        preflight_calls.append({"endpoint": "/apply-template", "request": apply_request,
                                "http_status": apply_status, "raw_body": apply_body,
                                "raw_response": apply_response, "error": apply_error})
        if apply_error:
            error = {"code": "context_preflight_error", "stage": "apply_template",
                     "details": apply_error}
        elif not isinstance(apply_response, dict) or not isinstance(apply_response.get("prompt"), str):
            error = {"code": "context_preflight_error", "stage": "apply_template",
                     "message": "server returned no rendered chat prompt"}
        if error is None:
            tokenize_request = {"content": apply_response["prompt"],
                                "add_special": True, "parse_special": True}
            token_status, token_body, token_response, token_error = self._post_json(
                "/tokenize", tokenize_request)
            preflight_calls.append({"endpoint": "/tokenize", "request": tokenize_request,
                                    "http_status": token_status, "raw_body": token_body,
                                    "raw_response": token_response, "error": token_error})
            tokens = token_response.get("tokens") if isinstance(token_response, dict) else None
            if token_error or not isinstance(tokens, list) or any(type(item) is not int for item in tokens):
                error = {"code": "context_preflight_error", "stage": "tokenize",
                         "details": token_error or {"message": "server returned invalid token list"}}
            else:
                prompt_token_count = len(tokens)
                requested_generation = int(payload["max_tokens"])
                if prompt_token_count + requested_generation > self.context_tokens:
                    error = {"code": "context_budget_exceeded",
                             "prompt_tokens": prompt_token_count,
                             "max_generation_tokens": requested_generation,
                             "context_tokens": self.context_tokens,
                             "message": "rendered prompt plus generation budget exceeds model context"}
        return prompt_token_count, error

    def _post_json(self, path: str, payload: Mapping[str, Any]):
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST")
        try:
            try:
                response = urllib.request.urlopen(request, timeout=self.request_timeout)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                status = int(response.status)
                raw_body = response.read().decode("utf-8", "replace")
            try:
                parsed = json.loads(raw_body)
            except json.JSONDecodeError as exc:
                return status, raw_body, None, {"code": "invalid_http_json", "message": str(exc)}
            if not 200 <= status < 300:
                return status, raw_body, parsed, {"code": "http_status", "status": status}
            return status, raw_body, parsed, None
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            return None, None, None, {"code": "transport_error", "message": f"{type(exc).__name__}: {exc}"}


def _content_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            else:
                return None
        return "".join(parts)
    return None
