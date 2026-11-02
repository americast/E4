#!/usr/bin/env python3
"""Exercise neutral short and near-24k prefill generations without Bank data."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def post_json(base: str, endpoint: str, payload: dict, timeout: float) -> tuple[int, dict]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    request = Request(base + endpoint, data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            result = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"{endpoint} returned HTTP {exc.code}") from exc
    if not isinstance(result, dict):
        raise RuntimeError(f"{endpoint} returned a non-object JSON response")
    return status, result


def render_and_count(base: str, messages: list[dict[str, str]], timeout: float) -> tuple[str, int]:
    status, rendered = post_json(base, "/apply-template", {
        "messages": messages, "chat_template_kwargs": {"enable_thinking": True}}, timeout)
    if status != 200 or not isinstance(rendered.get("prompt"), str):
        raise RuntimeError("/apply-template did not return a rendered neutral prompt")
    status, tokenized = post_json(base, "/tokenize", {
        "content": rendered["prompt"], "add_special": True, "parse_special": True}, timeout)
    tokens = tokenized.get("tokens")
    if status != 200 or not isinstance(tokens, list) or any(type(token) is not int for token in tokens):
        raise RuntimeError("/tokenize did not return a valid token list")
    return rendered["prompt"], len(tokens)


def neutral_messages(content: str) -> list[dict[str, str]]:
    return [{"role": "system", "content": "This is a neutral model-server diagnostic. Follow the user's simple instruction."},
            {"role": "user", "content": content}]


def make_long_prompt(base: str, target_tokens: int, timeout: float) -> tuple[list[dict[str, str]], int]:
    phrase = "Neutral synthetic context item. "
    low, high = 0, max(64, target_tokens * 2)
    best: tuple[list[dict[str, str]], int] | None = None
    while low <= high:
        repeats = (low + high) // 2
        content = phrase * repeats + "Return exactly READY."
        messages = neutral_messages(content)
        _rendered, token_count = render_and_count(base, messages, timeout)
        if token_count <= target_tokens:
            best = (messages, token_count)
            low = repeats + 1
        else:
            high = repeats - 1
    if best is None or best[1] < target_tokens - 256:
        raise RuntimeError("could not construct a neutral prompt within 256 tokens of target")
    return best


def complete(base: str, messages: list[dict[str, str]], timeout: float) -> dict:
    started = time.perf_counter()
    status, response = post_json(base, "/v1/chat/completions", {
        "model": "e4-gemma-m3", "messages": messages,
        "temperature": 1.0, "seed": 42, "max_tokens": 16, "stream": False}, timeout)
    elapsed = time.perf_counter() - started
    choices = response.get("choices")
    if status != 200 or not isinstance(choices, list) or len(choices) != 1:
        raise RuntimeError("completion response did not contain exactly one choice")
    choice = choices[0]
    message = choice.get("message") or {}
    content = message.get("content")
    reasoning = message.get("reasoning_content")
    usage = response.get("usage")
    if not isinstance(usage, dict) or (not isinstance(content, str) and not isinstance(reasoning, str)):
        raise RuntimeError("completion response is missing usage and both content/reasoning text")
    return {"http_status": status, "finish_reason": choice.get("finish_reason"),
        "usage": usage, "elapsed_seconds": elapsed,
        "content_sha256": hashlib.sha256((content or "").encode("utf-8")).hexdigest(),
        "content_characters": len(content or ""),
        "reasoning_sha256": hashlib.sha256((reasoning or "").encode("utf-8")).hexdigest(),
        "reasoning_characters": len(reasoning or ""),
        "timings": response.get("timings")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("E4_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--target-tokens", type=int, default=24000)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    if args.target_tokens < 1000 or args.target_tokens > 24576:
        parser.error("target token count must be between 1000 and 24576")
    short_messages = neutral_messages("Return exactly READY.")
    _short_prompt, short_tokens = render_and_count(args.base_url, short_messages, args.timeout)
    short = complete(args.base_url, short_messages, args.timeout)
    long_messages, long_tokens = make_long_prompt(args.base_url, args.target_tokens, args.timeout)
    long = complete(args.base_url, long_messages, args.timeout)
    report = {"schema_version": "e4-neutral-model-server-smoke/1", "status": "complete",
        "data_classification": "synthetic_neutral_only", "completion_requests_made": 2,
        "endpoints_used": ["/apply-template", "/tokenize", "/v1/chat/completions"],
        "generation_parameters": {"temperature": 1.0, "seed": 42, "max_tokens": 16},
        "target_long_prompt_tokens": args.target_tokens,
        "short": {"rendered_prompt_tokens": short_tokens, **short},
        "near_context": {"rendered_prompt_tokens": long_tokens, **long},
        "limitations": ["This is a neutral server/hardware smoke, not a Bank diagnostic or accuracy measurement."]}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"status": report["status"], "report": str(args.report),
        "short_tokens": short_tokens, "near_context_tokens": long_tokens,
        "completion_requests_made": 2,
        "short_finish_reason": short["finish_reason"],
        "long_finish_reason": long["finish_reason"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
