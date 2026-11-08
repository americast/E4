"""Small standard-library clients for OpenAI Responses and local Ollama chat."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Mapping
from urllib.parse import urlsplit


class ModelClientError(RuntimeError):
    """Redacted provider/response error with a stable stage identifier."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        stage: str,
        status_code: int | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.stage = stage
        self.status_code = status_code
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "stage": self.stage,
            "status_code": self.status_code,
            "message": str(self),
            "details": dict(self.details),
        }


def _positive_int(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_timeout(value: Any) -> float:
    if type(value) not in {int, float} or value <= 0:
        raise ValueError("timeout must be a positive number of seconds")
    return float(value)


def _model_name(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("model must be a nonempty string")
    return value.strip()


def _http_base(value: str, *, provider: str) -> str:
    candidate = value.strip()
    if "://" not in candidate:
        candidate = "http://" + candidate
    parts = urlsplit(candidate)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError(f"{provider} base_url must be an HTTP(S) URL or host:port")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError(f"{provider} base_url must not contain credentials, query, or fragment")
    return candidate.rstrip("/")


def _redact(text: str, secrets: tuple[str, ...] = ()) -> str:
    safe = str(text)
    for secret in secrets:
        if secret:
            safe = safe.replace(secret, "[REDACTED]")
    safe = re.sub(r"(?i)\bBearer\s+[^\s,;]+", "Bearer [REDACTED]", safe)
    safe = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "[REDACTED]", safe)
    return safe[:800]


def _post_json(
    url: str,
    payload: Mapping[str, Any],
    *,
    provider: str,
    timeout: float,
    headers: Mapping[str, str],
    secrets: tuple[str, ...] = (),
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json", **dict(headers)},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        detail = _redact(body, secrets)
        raise ModelClientError(
            f"{provider} HTTP request failed with status {exc.code}",
            provider=provider,
            stage="http",
            status_code=exc.code,
            details={"provider_message": detail} if detail else {},
        ) from None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        raise ModelClientError(
            f"{provider} request failed: {_redact(str(reason), secrets)}",
            provider=provider,
            stage="transport",
        ) from None
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ModelClientError(
            f"{provider} returned malformed JSON ({type(exc).__name__})",
            provider=provider,
            stage="response_json",
        ) from None
    if not isinstance(data, dict):
        raise ModelClientError(
            f"{provider} response root must be a JSON object",
            provider=provider,
            stage="response_json",
        )
    return data


def _require_json_object(text: str, *, provider: str) -> str:
    if not text.strip():
        raise ModelClientError(f"{provider} returned empty text", provider=provider, stage="empty")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ModelClientError(
            f"{provider} returned malformed JSON text (line {exc.lineno}, column {exc.colno})",
            provider=provider,
            stage="output_json",
        ) from None
    if not isinstance(value, dict):
        raise ModelClientError(
            f"{provider} JSON mode requires a JSON object",
            provider=provider,
            stage="output_json",
        )
    return text


class OpenAIClient:
    """OpenAI Responses API text client with optional JSON-object mode."""

    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.openai.com/v1",
        max_output_tokens: int = 8192,
        timeout: float = 600,
        json_mode: bool = True,
    ) -> None:
        self.model = _model_name(model)
        chosen_key = api_key if isinstance(api_key, str) and api_key.strip() else os.environ.get("OPENAI_API_KEY")
        if not isinstance(chosen_key, str) or not chosen_key.strip():
            raise ValueError("OpenAI API key is required; pass api_key or set OPENAI_API_KEY")
        self._api_key = chosen_key.strip()
        self.base_url = _http_base(base_url, provider="OpenAI")
        self.max_output_tokens = _positive_int(max_output_tokens, "max_output_tokens")
        self.timeout = _positive_timeout(timeout)
        if type(json_mode) is not bool:
            raise ValueError("json_mode must be a bool")
        self.json_mode = json_mode

    def complete(self, prompt: str) -> str:
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt must be a nonempty string")
        payload: dict[str, Any] = {
            "model": self.model,
            "input": prompt,
            "max_output_tokens": self.max_output_tokens,
            "stream": False,
            "store": False,
        }
        if self.json_mode:
            payload["instructions"] = "Return exactly one JSON object and no surrounding prose."
            payload["text"] = {"format": {"type": "json_object"}}
        data = _post_json(
            self.base_url + "/responses",
            payload,
            provider="OpenAI",
            timeout=self.timeout,
            headers={"Authorization": f"Bearer {self._api_key}"},
            secrets=(self._api_key,),
        )
        if data.get("error"):
            error = data["error"]
            message = error.get("message", "provider returned an error") if isinstance(error, Mapping) else str(error)
            raise ModelClientError(
                f"OpenAI response error: {_redact(message, (self._api_key,))}",
                provider="OpenAI",
                stage="provider",
                details={"type": _redact(str(error.get("type")), (self._api_key,))}
                if isinstance(error, Mapping) and error.get("type") else {},
            )
        status = data.get("status")
        if status != "completed" or data.get("incomplete_details"):
            incomplete_detail = data.get("incomplete_details")
            safe_incomplete = _redact(json.dumps(incomplete_detail, ensure_ascii=False), (self._api_key,)) if incomplete_detail else None
            raise ModelClientError(
                f"OpenAI response did not complete successfully (status={_redact(str(status), (self._api_key,))})",
                provider="OpenAI",
                stage="incomplete",
                details={"status": _redact(str(status), (self._api_key,)),
                         **({"incomplete_details": safe_incomplete} if safe_incomplete else {})},
            )
        output = data.get("output")
        if not isinstance(output, list):
            raise ModelClientError("OpenAI response has no output message array", provider="OpenAI", stage="output")
        fragments: list[str] = []
        for item in output:
            if not isinstance(item, Mapping) or item.get("type") != "message":
                continue
            if item.get("status") not in {None, "completed"}:
                raise ModelClientError("OpenAI output message is incomplete", provider="OpenAI", stage="incomplete")
            content = item.get("content", ())
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, Mapping):
                    continue
                if block.get("type") == "refusal":
                    raise ModelClientError("OpenAI refused the request", provider="OpenAI", stage="refusal")
                if block.get("type") == "output_text":
                    text = block.get("text")
                    if isinstance(text, str):
                        fragments.append(text)
        result = "".join(fragments)
        if self.json_mode:
            return _require_json_object(result, provider="OpenAI")
        if not result.strip():
            raise ModelClientError("OpenAI returned empty text", provider="OpenAI", stage="empty")
        return result


class OllamaClient:
    """Local Ollama /api/chat client with optional JSON-object mode."""

    def __init__(
        self,
        model: str,
        *,
        base_url: str | None = None,
        context_tokens: int = 32768,
        max_output_tokens: int = 8192,
        timeout: float = 600,
        json_mode: bool = True,
    ) -> None:
        self.model = _model_name(model)
        configured_host = base_url if base_url is not None else (os.environ.get("OLLAMA_HOST") or "http://localhost:11434")
        self.base_url = _http_base(configured_host, provider="Ollama")
        self.context_tokens = _positive_int(context_tokens, "context_tokens")
        self.max_output_tokens = _positive_int(max_output_tokens, "max_output_tokens")
        if self.context_tokens <= self.max_output_tokens:
            raise ValueError("context_tokens must be greater than max_output_tokens")
        self.timeout = _positive_timeout(timeout)
        if type(json_mode) is not bool:
            raise ValueError("json_mode must be a bool")
        self.json_mode = json_mode

    def complete(self, prompt: str) -> str:
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt must be a nonempty string")
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"num_ctx": self.context_tokens, "num_predict": self.max_output_tokens},
        }
        if self.json_mode:
            payload["format"] = "json"
        data = _post_json(
            self.base_url + "/api/chat",
            payload,
            provider="Ollama",
            timeout=self.timeout,
            headers={},
        )
        if data.get("error"):
            raise ModelClientError(
                f"Ollama response error: {_redact(str(data['error']))}",
                provider="Ollama",
                stage="provider",
            )
        if data.get("done") is not True:
            raise ModelClientError("Ollama response did not finish", provider="Ollama", stage="incomplete")
        if data.get("done_reason") == "length":
            raise ModelClientError("Ollama response stopped at the output limit", provider="Ollama", stage="incomplete")
        message = data.get("message")
        if not isinstance(message, Mapping) or not isinstance(message.get("content"), str):
            raise ModelClientError("Ollama response has no message.content text", provider="Ollama", stage="output")
        content = message["content"]
        if self.json_mode:
            return _require_json_object(content, provider="Ollama")
        if not content.strip():
            raise ModelClientError("Ollama returned empty text", provider="Ollama", stage="empty")
        return content
