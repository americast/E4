"""Shared trusted answer validation and binary top-three feedback for M5."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import socket
import struct
import threading
from pathlib import Path
from typing import Any, Iterable


MAX_SUBMISSIONS = 3
MAX_COMPONENT_CHARS = 256
CASE_ID = re.compile(r"^bank-[0-9]{8}T[0-9]{4}-utc8$")


class EvaluationSetupError(ValueError):
    """The trusted evaluator was not initialized with complete frozen truth."""


@dataclass(frozen=True)
class SubmissionRecord:
    attempt: int
    submitted: Any
    ranked_ids: tuple[str, ...]
    valid: bool
    validation_error: str | None
    correct_rank: int | None
    accepted: bool


def _canonical_ids(values: Iterable[str], label: str) -> frozenset[str]:
    result: set[str] = set()
    for value in values:
        if type(value) is not str or not value.strip() or len(value.strip()) > MAX_COMPONENT_CHARS:
            raise EvaluationSetupError(f"{label} contains a malformed component identity")
        canonical = value.strip().casefold()
        if canonical in result:
            raise EvaluationSetupError(f"{label} contains duplicate component identities")
        result.add(canonical)
    if not result:
        raise EvaluationSetupError(f"{label} must be nonempty")
    return frozenset(result)


def validate_ranked_answer(raw: Any, allowed_component_ids: Iterable[str]) -> tuple[tuple[str, ...], str | None]:
    """Normalize a raw JSON answer. Invalid submissions are retained as failed attempts."""
    allowed = _canonical_ids(allowed_component_ids, "public component allowlist")
    if isinstance(raw, dict):
        if set(raw) != {"ranked_components"}:
            return (), "answer object must contain only ranked_components"
        raw = raw["ranked_components"]
    if not isinstance(raw, list) or not 1 <= len(raw) <= 3:
        return (), "ranked_components must contain one to three component IDs"
    if any(type(item) is not str or not item.strip() or len(item.strip()) > MAX_COMPONENT_CHARS for item in raw):
        return (), "ranked_components contains a malformed component ID"
    normalized = tuple(item.strip() for item in raw)
    folded = [item.casefold() for item in normalized]
    if len(set(folded)) != len(folded):
        return normalized, "ranked_components contains duplicate component IDs"
    if any(item not in allowed for item in folded):
        return normalized, "ranked_components contains an unknown component ID"
    return normalized, None


class EvaluationSession:
    """Trusted per-case evaluator. Agent-facing feedback is exactly one boolean."""

    def __init__(self, *, case_id: str, allowed_component_ids: Iterable[str],
                 truth_ids: Iterable[str], max_submissions: int = MAX_SUBMISSIONS):
        if type(case_id) is not str or not CASE_ID.fullmatch(case_id):
            raise EvaluationSetupError("case_id is malformed")
        if type(max_submissions) is not int or max_submissions != MAX_SUBMISSIONS:
            raise EvaluationSetupError("M5 submission limit is frozen at three")
        self.case_id = case_id
        self.allowed_component_ids = tuple(allowed_component_ids)
        self._allowed = _canonical_ids(self.allowed_component_ids, "public component allowlist")
        self._truth = _canonical_ids(truth_ids, "frozen truth")
        # A valid target may be absent from observed telemetry; that is an unsolved
        # but still valid case and remains in the fixed denominator.
        self.max_submissions = max_submissions
        self.records: list[SubmissionRecord] = []

    @property
    def terminal(self) -> bool:
        return (len(self.records) >= self.max_submissions or
                any(record.accepted for record in self.records))

    @property
    def attempts(self) -> int:
        return len(self.records)

    def submit(self, raw_answer: Any) -> dict[str, bool]:
        if self.terminal:
            raise RuntimeError("evaluation session is terminal")
        ranked, error = validate_ranked_answer(raw_answer, self.allowed_component_ids)
        correct_rank = (next((index + 1 for index, item in enumerate(ranked)
                              if item.casefold() in self._truth), None) if error is None else None)
        accepted = correct_rank is not None
        self.records.append(SubmissionRecord(len(self.records) + 1, raw_answer, ranked,
                                             error is None, error, correct_rank, accepted))
        return {"accepted": accepted}

    def trusted_ledger(self) -> list[dict[str, Any]]:
        """Return outer-process accounting, including validation details, never to the agent."""
        return [{"attempt": row.attempt, "submitted": row.submitted,
                 "ranked_components": list(row.ranked_ids), "valid": row.valid,
                 "validation_error": row.validation_error, "correct_rank": row.correct_rank,
                 "accepted": row.accepted}
                for row in self.records]


class TrustedFeedbackServer:
    """Local IPC adapter exposing only one boolean for each trusted submission."""

    MAX_REQUEST_BYTES = 65_536

    def __init__(self, socket_path: Path, evaluation: EvaluationSession):
        self.socket_path = Path(socket_path).absolute()
        self.evaluation = evaluation
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.failure: str | None = None

    def __enter__(self) -> "TrustedFeedbackServer":
        if self.socket_path.exists():
            raise FileExistsError("trusted feedback socket path must be fresh")
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.socket_path))
            listener.listen(4)
            listener.settimeout(0.2)
        except BaseException:
            # In particular, do not leak the open descriptor if the sandbox or
            # filesystem rejects AF_UNIX bind during setup.
            listener.close()
            raise
        self._listener = listener
        self._thread = threading.Thread(target=self._serve, name="m5-trusted-feedback", daemon=True)
        self._thread.start()
        return self

    def _read_exact(self, connection: socket.socket, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            block = connection.recv(count - len(chunks))
            if not block:
                raise EOFError("truncated feedback frame")
            chunks.extend(block)
        return bytes(chunks)

    def _send(self, connection: socket.socket, payload: dict[str, bool]) -> None:
        raw = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
        connection.sendall(struct.pack(">I", len(raw)) + raw)

    def _handle(self, connection: socket.socket) -> None:
        try:
            header = self._read_exact(connection, 4)
            size = struct.unpack(">I", header)[0]
            if size > self.MAX_REQUEST_BYTES:
                accepted = self.evaluation.submit(None)["accepted"]
                self._send(connection, {"accepted": accepted})
                return
            raw = self._read_exact(connection, size)
            try:
                request = json.loads(raw, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                request = None
            if (not isinstance(request, dict) or set(request) != {"case_id", "raw_answer"} or
                    request.get("case_id") != self.evaluation.case_id):
                # A protocol/case mismatch is an orchestration fault, never accepted feedback.
                self.failure = "trusted feedback request has unexpected fields or case identity"
                accepted = self.evaluation.submit(None)["accepted"]
            else:
                accepted = self.evaluation.submit(request["raw_answer"])["accepted"]
            self._send(connection, {"accepted": accepted})
        except Exception as exc:
            self.failure = f"trusted feedback service failed: {type(exc).__name__}"
            try:
                self._send(connection, {"accepted": False})
            except Exception:
                pass

    def _serve(self) -> None:
        assert self._listener is not None
        while not self._stop.is_set() and not self.evaluation.terminal:
            try:
                connection, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with connection:
                self._handle(connection)

    def close(self) -> None:
        self._stop.set()
        if self._listener is not None:
            self._listener.close()
            self._listener = None
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
        if self.socket_path.exists() and self.socket_path.is_socket():
            self.socket_path.unlink()

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.close()


def load_scored_truth_ids(*, root: Path, case_id: str,
                          experiment_manifest: Path) -> tuple[str, ...]:
    """Load one fixed gold target set in the trusted host after validating the freeze.

    Call only from the trusted outer orchestrator after development readiness. Never pass
    this return value, the evaluator path, or the parsed rows into a terminal container.
    """
    from e4.comparison.manifest import validate_frozen_manifest

    root = Path(root).resolve()
    manifest = validate_frozen_manifest(experiment_manifest, root=root, case_id=case_id,
                                         split="scored")
    if not CASE_ID.fullmatch(case_id) or case_id not in manifest["case_ids"]:
        raise EvaluationSetupError("case is outside the frozen scored allowlist")
    gold_path = root / "data/prepared/bank/evaluator/scored_ground_truth.jsonl"
    if gold_path.is_symlink() or gold_path.resolve() != gold_path.absolute() or not gold_path.is_file():
        raise EvaluationSetupError("fixed scored truth source is unavailable")
    truth_pin = manifest.get("trusted_truth_pin", {})
    if (truth_pin.get("path") != "data/prepared/bank/evaluator/scored_ground_truth.jsonl" or
            truth_pin.get("access") != "trusted_evaluator_after_m6_go_only" or
            __import__("hashlib").sha256(gold_path.read_bytes()).hexdigest() != truth_pin.get("sha256")):
        raise EvaluationSetupError("fixed scored truth differs from the frozen trusted-evaluator pin")
    expected_ids = tuple(manifest["case_ids"])
    rows: dict[str, tuple[str, ...]] = {}
    try:
        for line in gold_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (type(row) is not dict or row.get("split") != "scored" or
                    type(row.get("incident_id")) is not str or not CASE_ID.fullmatch(row["incident_id"]) or
                    row["incident_id"] not in expected_ids or row["incident_id"] in rows):
                raise EvaluationSetupError("fixed scored truth file has malformed or out-of-scope rows")
            targets = row.get("ground_truth")
            if type(targets) is not list or not targets:
                raise EvaluationSetupError("fixed scored truth row is missing component targets")
            components = []
            for target in targets:
                if type(target) is not dict:
                    raise EvaluationSetupError("fixed scored truth target is malformed")
                component = target.get("component")
                if (type(component) is not str or not component.strip() or
                        len(component.strip()) > MAX_COMPONENT_CHARS):
                    raise EvaluationSetupError("fixed scored truth component identity is malformed")
                components.append(component.strip())
            if len({value.casefold() for value in components}) != len(components):
                raise EvaluationSetupError("fixed scored truth row contains duplicate component identities")
            rows[row["incident_id"]] = tuple(components)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EvaluationSetupError("fixed scored truth file could not be validated") from exc
    if set(rows) != set(expected_ids):
        raise EvaluationSetupError("fixed scored truth file does not cover the ten frozen incidents")
    return rows[case_id]


def load_development_truth_ids(*, root: Path, case_id: str) -> tuple[str, ...]:
    """Load one fixed development target set for readiness runs in the trusted host."""
    root = Path(root).resolve()
    sample = json.loads((root / "config/bank/sample.json").read_text(encoding="utf-8"))
    expected_ids = tuple(row["incident_id"] for row in sample.get("development_cases", []))
    if not CASE_ID.fullmatch(case_id) or case_id not in expected_ids or len(expected_ids) != 2:
        raise EvaluationSetupError("case is outside the fixed development allowlist")
    gold_path = root / "data/prepared/bank/evaluator/dev_ground_truth.jsonl"
    if gold_path.is_symlink() or gold_path.resolve() != gold_path.absolute() or not gold_path.is_file():
        raise EvaluationSetupError("fixed development truth source is unavailable")
    rows: dict[str, tuple[str, ...]] = {}
    try:
        for line in gold_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (type(row) is not dict or row.get("split") != "dev" or
                    type(row.get("incident_id")) is not str or row["incident_id"] not in expected_ids or
                    row["incident_id"] in rows):
                raise EvaluationSetupError("fixed development truth file has malformed or out-of-scope rows")
            targets = row.get("ground_truth")
            if type(targets) is not list or not targets:
                raise EvaluationSetupError("fixed development truth row is missing component targets")
            components = []
            for target in targets:
                component = target.get("component") if type(target) is dict else None
                if (type(component) is not str or not component.strip() or
                        len(component.strip()) > MAX_COMPONENT_CHARS):
                    raise EvaluationSetupError("fixed development truth component identity is malformed")
                components.append(component.strip())
            if len({value.casefold() for value in components}) != len(components):
                raise EvaluationSetupError("fixed development truth has duplicate component identities")
            rows[row["incident_id"]] = tuple(components)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        raise EvaluationSetupError("fixed development truth file could not be validated") from None
    if set(rows) != set(expected_ids):
        raise EvaluationSetupError("fixed development truth file does not cover both incidents")
    return rows[case_id]
