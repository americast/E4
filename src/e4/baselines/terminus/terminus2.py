"""Load and configure Harbor's unmodified Terminus-2 agent."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from e4.paths import ROOT


HARBOR_REPOSITORY = "https://github.com/harbor-framework/harbor.git"
E4_ROOT = ROOT
HARBOR_REVISION = "26106c6d9c66848a1440faf5724d336affac52aa"
HARBOR_AGENT_RELATIVE = Path("src/harbor/agents/terminus_2/terminus_2.py")
PROFILE_RELATIVE = Path("config/models/gemma.json")
MAX_DIAGNOSIS_SUBMISSIONS = 3
DEFAULT_MAX_TURNS = 32
DEFAULT_GENERATED_TOKEN_BUDGET = 73728
MAX_GENERATION_TOKENS = 8192


class TerminusSetupError(RuntimeError):
    """The pinned official Harbor agent is not available or mismatched."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_harbor_source(repository: Path) -> dict[str, str]:
    """Verify the exact upstream Git revision and official Terminus-2 source file."""
    repository = Path(repository).resolve(strict=True)
    try:
        revision = subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True,
            stderr=subprocess.PIPE, timeout=10,
        ).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise TerminusSetupError(f"cannot read Harbor Git revision: {exc}") from exc
    if revision != HARBOR_REVISION:
        raise TerminusSetupError(
            f"Harbor source revision {revision!r} differs from pinned {HARBOR_REVISION}"
        )
    status = subprocess.check_output(
        ["git", "-C", str(repository), "status", "--porcelain", "--untracked-files=all"],
        text=True, stderr=subprocess.PIPE, timeout=30,
    )
    if status.strip():
        raise TerminusSetupError("Harbor source checkout contains modified or untracked files")
    git_tree = subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
        text=True, stderr=subprocess.PIPE, timeout=30,
    ).strip()
    files: dict[str, str] = {}
    for path in sorted(repository.rglob("*")):
        if not path.is_file() or ".git" in path.relative_to(repository).parts:
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        files[path.relative_to(repository).as_posix()] = sha256_file(path)
    canonical = json.dumps(files, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":")).encode("utf-8")
    source_tree_sha256 = hashlib.sha256(canonical).hexdigest()
    source = repository / HARBOR_AGENT_RELATIVE
    if not source.is_file():
        raise TerminusSetupError(f"pinned Harbor agent source is missing: {source}")
    try:
        source_path = repository.relative_to(E4_ROOT).as_posix()
    except ValueError:
        source_path = str(repository)
    return {
        "repository": HARBOR_REPOSITORY,
        "harbor_revision": revision,
        "harbor_git_tree": git_tree,
        "harbor_source_tree_sha256": source_tree_sha256,
        "harbor_source_path": source_path,
        "harbor_agent_path": HARBOR_AGENT_RELATIVE.as_posix(),
        "harbor_agent_sha256": sha256_file(source),
    }


def _load_profile(root: Path) -> dict[str, Any]:
    path = Path(root) / PROFILE_RELATIVE
    try:
        profile = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminusSetupError(f"cannot read frozen Gemma profile: {exc}") from exc
    expected = {
        "context_tokens": 32768,
        "max_generation_tokens": 8192,
        "temperature": 1.0,
        "top_p": 0.95,
        "top_k": 64,
        "seed": 42,
        "reasoning_mode": "on",
        "reasoning_budget": 2048,
    }
    for key, value in expected.items():
        if profile.get(key) != value:
            raise TerminusSetupError(f"Gemma profile {key} differs from the frozen setting")
    return profile


def load_terminus2(repository: Path, *, root: Path, logs_dir: Path,
                   max_turns: int, api_base: str | None = None,
                   enable_summarize: bool = True,
                   proactive_summarization_threshold: int = 8000):
    """Return Harbor's official Terminus2 class and its configured instance.

    Importing the upstream module is delayed so local E4 development and tests can
    use this package without installing Harbor into E4's Python 3.10 environment.
    """
    verify_harbor_source(repository)
    profile = _load_profile(root)
    source_root = str(Path(repository).resolve(strict=True) / "src")
    if source_root not in sys.path:
        sys.path.insert(0, source_root)
    try:
        Terminus2 = importlib.import_module(
            "harbor.agents.terminus_2.terminus_2"
        ).Terminus2
    except (ImportError, AttributeError) as exc:
        raise TerminusSetupError(f"cannot import pinned Harbor Terminus-2: {exc}") from exc

    imported_agent_path = Path(inspect.getfile(Terminus2)).resolve(strict=True)
    pinned_agent_path = (Path(repository).resolve(strict=True) / HARBOR_AGENT_RELATIVE).resolve(strict=True)
    if imported_agent_path != pinned_agent_path:
        raise TerminusSetupError(
            f"imported Terminus-2 source {imported_agent_path} is outside the verified checkout"
        )
    endpoint = api_base or os.environ.get("E4_BASE_URL")
    parsed_endpoint = urlsplit(endpoint or "")
    if (parsed_endpoint.scheme != "http" or
            parsed_endpoint.hostname not in {"127.0.0.1", "localhost"} or
            parsed_endpoint.username is not None or parsed_endpoint.password is not None or
            parsed_endpoint.port is None or not 1 <= parsed_endpoint.port <= 65535):
        raise TerminusSetupError(
            "E4_BASE_URL must be a local HTTP endpoint on 127.0.0.1 or localhost"
        )
    model_name = f"openai/{profile['model_alias']}"
    agent = Terminus2(
        logs_dir=Path(logs_dir),
        model_name=model_name,
        max_turns=max_turns,
        parser_name="json",
        api_base=endpoint,
        temperature=profile["temperature"],
        model_info={
            "max_input_tokens": profile["context_tokens"] - profile["max_generation_tokens"],
            "max_output_tokens": profile["max_generation_tokens"],
        },
        enable_summarize=enable_summarize,
        proactive_summarization_threshold=proactive_summarization_threshold,
        llm_kwargs={"api_key": "e4-local-no-auth"},
        llm_call_kwargs={
            "max_tokens": profile["max_generation_tokens"],
            "top_p": profile["top_p"],
            "seed": profile["seed"],
            "extra_body": {"top_k": profile["top_k"]},
        },
    )
    return Terminus2, agent


def context_accounting(context: Any) -> dict[str, Any]:
    """Return the native Harbor counters, including its summary subagent calls."""
    return {
        "prompt_tokens": context.n_input_tokens,
        "completion_tokens": context.n_output_tokens,
        "cached_tokens": context.n_cache_tokens,
        "cost_usd": context.cost_usd,
        "model_usage": (context.model_dump().get("model_usage")
                        if hasattr(context, "model_dump") else None),
    }


class CallBudgetExhausted(RuntimeError):
    """The frozen maximum number of local model generations was reached."""


class ModelCallBudget:
    """Limit generated tokens across main-agent and native summary calls."""

    def __init__(self, agent: Any, *,
                 generated_token_budget: int = DEFAULT_GENERATED_TOKEN_BUDGET,
                 maximum_tokens_per_call: int = MAX_GENERATION_TOKENS,
                 terminal_predicate: Any | None = None,
                 terminal_reason: Any | None = None,
                 http_journal_path: Path | None = None):
        self.agent = agent
        self.generated_token_budget = generated_token_budget
        self.maximum_tokens_per_call = maximum_tokens_per_call
        self.started_calls = 0
        self.blocked_calls = 0
        self.generated_tokens = 0
        self.conservative_tokens = 0
        self.prompt_tokens_reported = 0
        self.requested_token_ceiling_total = 0
        self.missing_usage_calls = 0
        self.usage_incomplete = False
        self.http_calls_started = 0
        self.http_calls_failed = 0
        self.http_calls_blocked = 0
        self.http_seconds = 0.0
        self.usage_overrun_tokens = 0
        self.failed_calls = 0
        self.terminal_predicate = terminal_predicate
        self.terminal_reason = terminal_reason
        self.terminal_finalization_responses = 0
        self.elapsed_seconds = 0.0
        self._original_call = agent._llm.call
        self.http_journal_path = Path(http_journal_path) if http_journal_path else None
        self._litellm_module = None
        self._original_acompletion = None
        try:
            import litellm

            self._litellm_module = litellm
            self._original_acompletion = litellm.acompletion
            litellm.acompletion = self._measure_http_call
        except ImportError:
            # Fake clients used in adapter tests need not install Harbor dependencies.
            pass

        async def limited_call(*args, **kwargs):
            if self.terminal_predicate is not None and self.terminal_predicate():
                from harbor.llms.base import LLMResponse
                from harbor.models.metric import UsageInfo

                self.terminal_finalization_responses += 1
                return LLMResponse(
                    content=(
                        '{"analysis":"The diagnosis opportunity limit has been reached. '
                        'Stop terminal investigation now.","plan":"Complete the task.",'
                        '"commands":[],"task_complete":true}'
                    ),
                    model_name=getattr(self.agent, "_model_name", None),
                    usage=UsageInfo(prompt_tokens=0, completion_tokens=0,
                                    cache_tokens=0, cost_usd=0.0),
                    extra={"host_control": "evaluation_terminal",
                           "termination_reason": (self.terminal_reason()
                                                  if self.terminal_reason else "terminal")},
                )
            remaining = (self.generated_token_budget - self.generated_tokens -
                         self.conservative_tokens)
            if remaining <= 0:
                self.blocked_calls += 1
                raise CallBudgetExhausted("frozen aggregate generation budget is exhausted")
            self.started_calls += 1
            kwargs["max_tokens"] = min(self.maximum_tokens_per_call, remaining)
            started = time.monotonic()
            try:
                try:
                    response = await self._original_call(*args, **kwargs)
                except Exception:
                    self.failed_calls += 1
                    raise
                return response
            finally:
                self.elapsed_seconds += time.monotonic() - started

        self.agent._llm.call = limited_call

    @staticmethod
    def _safe_json(value: Any) -> Any:
        if value is None or type(value) in {bool, int, float, str}:
            return value
        if isinstance(value, dict):
            return {str(key): ModelCallBudget._safe_json(item)
                    for key, item in value.items() if str(key).casefold() not in {
                        "api_key", "authorization", "headers", "extra_headers"}}
        if isinstance(value, (list, tuple)):
            return [ModelCallBudget._safe_json(item) for item in value]
        if hasattr(value, "model_dump"):
            try:
                return ModelCallBudget._safe_json(value.model_dump(mode="json"))
            except Exception:
                pass
        if hasattr(value, "to_dict"):
            try:
                return ModelCallBudget._safe_json(value.to_dict())
            except Exception:
                pass
        return repr(value)

    @staticmethod
    def _usage_tokens(response: Any) -> tuple[int | None, int | None]:
        usage = getattr(response, "usage", None)
        if usage is None and isinstance(response, dict):
            usage = response.get("usage")
        prompt = getattr(usage, "prompt_tokens", None) if usage is not None else None
        completion = getattr(usage, "completion_tokens", None) if usage is not None else None
        if isinstance(usage, dict):
            prompt = usage.get("prompt_tokens", prompt)
            completion = usage.get("completion_tokens", completion)
        return (prompt if type(prompt) is int and prompt >= 0 else None,
                completion if type(completion) is int and completion >= 0 else None)

    def _append_http_record(self, record: dict[str, Any]) -> None:
        if self.http_journal_path is None:
            return
        self.http_journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.http_journal_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            stream.flush()

    async def _measure_http_call(self, *args, **kwargs):
        if self._original_acompletion is None:
            raise RuntimeError("LiteLLM HTTP request hook is unavailable")
        remaining = (self.generated_token_budget - self.generated_tokens -
                     self.conservative_tokens)
        if remaining <= 0:
            self.http_calls_blocked += 1
            raise CallBudgetExhausted("frozen aggregate generation budget is exhausted")
        requested = min(int(kwargs.get("max_tokens") or self.maximum_tokens_per_call),
                        self.maximum_tokens_per_call, remaining)
        kwargs["max_tokens"] = requested
        self.requested_token_ceiling_total += requested
        self.http_calls_started += 1
        call_id = self.http_calls_started
        started = time.monotonic()
        record = {
            "event": "request_started", "call_id": call_id,
            "request": self._safe_json(kwargs), "reserved_completion_tokens": requested,
        }
        self._append_http_record(record)
        try:
            response = await self._original_acompletion(*args, **kwargs)
        except Exception as exc:
            elapsed = time.monotonic() - started
            self.http_seconds += elapsed
            self.http_calls_failed += 1
            self.usage_incomplete = True
            self.conservative_tokens += requested
            self._append_http_record({"event": "request_failed", "call_id": call_id,
                                      "elapsed_seconds": elapsed,
                                      "error": f"{type(exc).__name__}: {exc}",
                                      "usage_known": False,
                                      "conservative_completion_tokens_charged": requested})
            raise
        elapsed = time.monotonic() - started
        self.http_seconds += elapsed
        prompt_tokens, completion_tokens = self._usage_tokens(response)
        if prompt_tokens is None or completion_tokens is None:
            self.missing_usage_calls += 1
            self.usage_incomplete = True
            self.conservative_tokens += requested
            charged_conservatively = requested
        else:
            self.prompt_tokens_reported += prompt_tokens
            self.generated_tokens += completion_tokens
            self.usage_overrun_tokens += max(
                0, self.generated_tokens + self.conservative_tokens - self.generated_token_budget
            )
            charged_conservatively = 0
        self._append_http_record({
            "event": "response_received", "call_id": call_id,
            "elapsed_seconds": elapsed, "response": self._safe_json(response),
            "prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
            "conservative_completion_tokens_charged": charged_conservatively,
        })
        return response

    def close(self) -> None:
        self.agent._llm.call = self._original_call
        if self._litellm_module is not None and self._original_acompletion is not None:
            self._litellm_module.acompletion = self._original_acompletion

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_generations_started": self.started_calls,
            "model_generation_calls_blocked": self.blocked_calls,
            "model_generation_calls_failed": self.failed_calls,
            "terminal_finalization_responses": self.terminal_finalization_responses,
            "model_generation_wall_seconds": self.elapsed_seconds,
            "http_requests_started": self.http_calls_started,
            "http_requests_failed": self.http_calls_failed,
            "http_requests_blocked": self.http_calls_blocked,
            "http_request_wall_seconds": self.http_seconds,
            "prompt_tokens_reported_by_http": self.prompt_tokens_reported,
            "generated_tokens_reported": self.generated_tokens,
            "conservative_tokens_for_failed_or_unmetered_calls": self.conservative_tokens,
            "budget_tokens_charged": self.generated_tokens + self.conservative_tokens,
            "calls_without_output_token_usage": self.missing_usage_calls,
            "generated_token_budget": self.generated_token_budget,
            "reported_budget_overrun_tokens": self.usage_overrun_tokens,
            "usage_complete": not self.usage_incomplete,
            "maximum_tokens_per_generation": self.maximum_tokens_per_call,
            "maximum_requested_tokens_total": self.requested_token_ceiling_total,
        }
