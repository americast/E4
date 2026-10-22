#!/usr/bin/env python3
"""Submit a ranked Bank diagnosis through the trusted host feedback bridge."""

from __future__ import annotations

import json
import os
import stat
import sys
import time
import uuid
from pathlib import Path
from typing import Any


IPC_ROOT = Path(os.environ.get("E4_TERMINUS_IPC", "/run/e4bank"))
REQUEST_ROOT = IPC_ROOT / "requests"
RESPONSE_ROOT = IPC_ROOT / "responses"
WAIT_SECONDS = 900
MAX_REQUEST_BYTES = 16384
MAX_RESPONSE_BYTES = 1024


def _read_answer(path: str) -> Any:
    if path == "-":
        raw = sys.stdin.read(MAX_REQUEST_BYTES + 1)
    else:
        with Path(path).open("r", encoding="utf-8") as stream:
            raw = stream.read(MAX_REQUEST_BYTES + 1)
    if len(raw.encode("utf-8", errors="replace")) > MAX_REQUEST_BYTES:
        return "[submission exceeds the fixed request size limit]"
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # The host evaluator records non-JSON output as one malformed submission.
        return raw


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("usage: submit_diagnosis ANSWER.json|-", file=sys.stderr)
        return 2
    REQUEST_ROOT.mkdir(parents=True, exist_ok=True)
    request_id = uuid.uuid4().hex
    request = REQUEST_ROOT / f"request-{request_id}.json"
    response = RESPONSE_ROOT / f"response-{request_id}.json"
    payload = {"request_id": request_id, "answer": _read_answer(args[0])}
    if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) > MAX_REQUEST_BYTES:
        payload["answer"] = "[submission exceeds the fixed request size limit]"
    temporary = request.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temporary.replace(request)
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            descriptor = os.open(response, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except FileNotFoundError:
            time.sleep(0.1)
            continue
        except OSError as exc:
            print(f"trusted evaluator response cannot be opened safely: {exc}", file=sys.stderr)
            return 3
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_RESPONSE_BYTES:
                print("trusted evaluator response is not a bounded regular file", file=sys.stderr)
                return 3
            chunks = []
            remaining = MAX_RESPONSE_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw_response = b"".join(chunks)
            if len(raw_response) > MAX_RESPONSE_BYTES:
                print("trusted evaluator response exceeds the fixed size limit", file=sys.stderr)
                return 3
        finally:
            os.close(descriptor)
        try:
            result = json.loads(raw_response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print("trusted evaluator response is malformed", file=sys.stderr)
            return 3
        request.unlink(missing_ok=True)
        if set(result) != {"accepted"} or type(result["accepted"]) is not bool:
            print("trusted evaluator response is malformed", file=sys.stderr)
            return 3
        print(json.dumps(result, separators=(",", ":")))
        return 0
    print("trusted evaluator did not respond before timeout", file=sys.stderr)
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
