"""Trusted host-side file bridge between Harbor's task container and evaluator."""

from __future__ import annotations

import json
import os
import re
import socket
import stat
import struct
import tempfile
import threading
import time
from pathlib import Path
from typing import Any


class FeedbackBridgeError(RuntimeError):
    """The terminal/evaluator IPC contract was violated."""


class EvaluationSocketProxy:
    """Trusted-evaluator client; it learns only the accepted boolean per submission."""

    def __init__(self, socket_path: Path, case_id: str, *, timeout_seconds: float = 120):
        self.socket_path = str(socket_path)
        self.case_id = case_id
        self.timeout_seconds = timeout_seconds
        self._accepted: list[bool] = []

    @property
    def attempts(self) -> int:
        return len(self._accepted)

    @property
    def terminal(self) -> bool:
        return self.attempts >= 3 or any(self._accepted)

    def submit(self, raw_answer: Any) -> dict[str, bool]:
        if self.terminal:
            raise FeedbackBridgeError("trusted evaluator session is terminal")
        request = json.dumps({"case_id": self.case_id, "raw_answer": raw_answer},
                             ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(request) > 65536:
            request = json.dumps({"case_id": self.case_id,
                                  "raw_answer": "[submission exceeds protocol size limit]"},
                                 separators=(",", ":")).encode("utf-8")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.connect(self.socket_path)
            connection.sendall(struct.pack(">I", len(request)) + request)
            length = struct.unpack(">I", self._read_exact(connection, 4))[0]
            if length > 1024:
                raise FeedbackBridgeError("trusted evaluator response exceeds size limit")
            response = json.loads(self._read_exact(connection, length).decode("utf-8"))
        if set(response) != {"accepted"} or type(response["accepted"]) is not bool:
            raise FeedbackBridgeError("trusted evaluator must return one accepted boolean")
        self._accepted.append(response["accepted"])
        return response

    @staticmethod
    def _read_exact(connection: socket.socket, count: int) -> bytes:
        chunks: list[bytes] = []
        remaining = count
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise FeedbackBridgeError("trusted evaluator closed the IPC channel")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)


class FeedbackBridge:
    """Serve terminal requests while exposing only EvaluationSession's bool reply."""

    def __init__(self, ipc_root: Path, evaluation: Any, *, poll_seconds: float = 0.05,
                 on_terminal: Any | None = None):
        self.ipc_root = Path(ipc_root)
        self.request_root = self.ipc_root / "requests"
        self.response_root = self.ipc_root / "responses"
        self.evaluation = evaluation
        self.poll_seconds = poll_seconds
        self.stop_event = threading.Event()
        self.error: str | None = None
        self.request_count = 0
        self.on_terminal = on_terminal
        self.thread = threading.Thread(target=self._serve, name="terminus-feedback", daemon=True)

    def start(self) -> None:
        self.request_root.mkdir(parents=True, exist_ok=True)
        self.response_root.mkdir(parents=True, exist_ok=True)
        self.response_root.chmod(0o755)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread.is_alive():
            self.thread.join(timeout=2)

    def _serve(self) -> None:
        while not self.stop_event.is_set():
            try:
                requests: list[Path] = []
                with os.scandir(self.request_root) as entries:
                    for entry in entries:
                        if entry.name.startswith("request-") and entry.name.endswith(".json"):
                            requests.append(self.request_root / entry.name)
                            if len(requests) > 3:
                                raise FeedbackBridgeError("too many pending feedback requests")
                requests.sort(key=lambda item: item.name)
                for request_path in requests:
                    if self.stop_event.is_set():
                        return
                    try:
                        match = re.fullmatch(r"request-([a-f0-9]{32})\.json", request_path.name)
                        if not match:
                            raise FeedbackBridgeError("terminal request identity is malformed")
                        request_id = match.group(1)
                        descriptor = os.open(request_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                        try:
                            info = os.fstat(descriptor)
                            if not stat.S_ISREG(info.st_mode) or info.st_size > 16384:
                                raise FeedbackBridgeError("terminal request is not a bounded regular file")
                            data = os.read(descriptor, 16385)
                        finally:
                            os.close(descriptor)
                        if len(data) > 16384:
                            raise FeedbackBridgeError("terminal request exceeds the fixed byte limit")
                        request = json.loads(data.decode("utf-8"))
                        if (set(request) != {"request_id", "answer"} or
                                request["request_id"] != request_id):
                            raise FeedbackBridgeError("terminal request has invalid fields or identity")
                        if self.evaluation.terminal:
                            raise FeedbackBridgeError("terminal sent a diagnosis after evaluation stopped")
                        reply = self.evaluation.submit(request["answer"])
                        if set(reply) != {"accepted"} or type(reply["accepted"]) is not bool:
                            raise FeedbackBridgeError("evaluator reply is not one accepted boolean")
                        response_path = self.response_root / f"response-{request['request_id']}.json"
                        response_data = json.dumps(reply, separators=(",", ":")).encode("utf-8")
                        descriptor, temporary_name = tempfile.mkstemp(
                            prefix=".trusted-response-", dir=self.response_root
                        )
                        temporary = Path(temporary_name)
                        try:
                            os.fchmod(descriptor, 0o600)
                            os.write(descriptor, response_data)
                            os.fsync(descriptor)
                        finally:
                            os.close(descriptor)
                        temporary.replace(response_path)
                        self.request_count += 1
                        request_path.unlink(missing_ok=True)
                        if self.evaluation.terminal and self.on_terminal is not None:
                            self.on_terminal()
                    except Exception as exc:
                        self.error = f"{type(exc).__name__}: {exc}"
                        self.stop_event.set()
                        return
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                self.stop_event.set()
                return
            time.sleep(self.poll_seconds)


def three_attempt_slots(evaluation: Any) -> list[dict[str, Any]]:
    """Return three ordered outcome slots, marking unused submissions not_run."""
    if not hasattr(evaluation, "trusted_ledger"):
        results = getattr(evaluation, "_accepted", [])
        return [
            ({"attempt": index + 1, "status": "submitted", "accepted": results[index],
              "valid": None, "correct_rank": None, "ranked_components": [],
              "validation_error": None} if index < len(results) else
             {"attempt": index + 1, "status": "not_run", "accepted": False,
              "valid": None, "correct_rank": None, "ranked_components": [],
              "validation_error": None})
            for index in range(3)
        ]
    ledger = evaluation.trusted_ledger()
    slots: list[dict[str, Any]] = []
    for index in range(3):
        if index < len(ledger):
            row = ledger[index]
            slots.append({
                "attempt": index + 1,
                "status": "submitted",
                "accepted": bool(row["accepted"]),
                "valid": bool(row["valid"]),
                "correct_rank": row["correct_rank"],
                "ranked_components": list(row["ranked_components"]),
                "validation_error": row["validation_error"],
            })
        else:
            slots.append({"attempt": index + 1, "status": "not_run",
                          "accepted": False, "valid": None, "correct_rank": None,
                          "ranked_components": [], "validation_error": None})
    return slots
