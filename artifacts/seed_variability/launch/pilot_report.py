#!/usr/bin/env python3
"""Finalize the three-seed E4/T2 variability report after the T2 queue completes.

This artifact-only reporter verifies run provenance and request journals. It
extracts only aggregate accuracy fields from trusted parent result JSON; it
never materializes per-case evaluator rows or publishes incident identifiers.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[3]
LAUNCH = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from artifacts.seed_variability.launch import pilot_verify
EXPECTED_CASES = pilot_verify.EXPECTED_CASES
SEEDS = (42, 43, 44)
QUEUE_TERMINAL = {"complete", "completed_with_preserved_failures", "preflight_failed"}


class PilotReportError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_symlink():
        raise PilotReportError(f"refusing symlinked report input: {path}")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(ROOT.resolve()):
        raise PilotReportError(f"report input escapes workspace: {path}")
    return resolved


def _safe_json(path: Path) -> dict[str, Any]:
    path = _path(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PilotReportError(f"invalid JSON input {path.relative_to(ROOT)}: {exc}") from exc
    if not isinstance(value, dict):
        raise PilotReportError(f"expected JSON object in {path.relative_to(ROOT)}")
    return value


class _SelectiveJSON:
    """Small streaming JSON reader that skips unrequested subtrees without parsing them."""
    def __init__(self, source: Path | Any):
        if hasattr(source, "read"):
            self._stream = source
            self._owns_stream = False
        else:
            self._stream = _path(Path(source)).open("r", encoding="utf-8")
            self._owns_stream = True
        self._buffer = ""
        self._pos = 0
        self._eof = False

    def close(self) -> None:
        if self._owns_stream:
            self._stream.close()

    def _fill(self) -> None:
        if self._pos > 0:
            self._buffer = self._buffer[self._pos:]
            self._pos = 0
        if not self._eof and len(self._buffer) < 4096:
            chunk = self._stream.read(8192)
            if chunk:
                self._buffer += chunk
            else:
                self._eof = True

    def peek(self) -> str:
        self._fill()
        return self._buffer[self._pos:self._pos + 1]

    def get(self) -> str:
        value = self.peek()
        if not value:
            raise PilotReportError("unexpected end of JSON input")
        self._pos += 1
        return value

    def ws(self) -> None:
        while self.peek() and self.peek() in " \t\r\n":
            self.get()

    def expect(self, char: str) -> None:
        self.ws()
        if self.get() != char:
            raise PilotReportError(f"malformed JSON: expected {char!r}")

    def string(self, *, keep: bool) -> str | None:
        self.ws()
        if self.get() != '"':
            raise PilotReportError("malformed JSON: expected string")
        raw = ['"'] if keep else None
        escaped = False
        while True:
            char = self.get()
            if keep:
                raw.append(char)
            if escaped:
                if char not in '"\\/bfnrtu':
                    raise PilotReportError("malformed JSON escape")
                if char == "u":
                    for _ in range(4):
                        hex_char = self.get()
                        if hex_char not in "0123456789abcdefABCDEF":
                            raise PilotReportError("malformed JSON Unicode escape")
                        if keep:
                            raw.append(hex_char)
                escaped = False
                continue
            if char == "\\":
                escaped = True
            elif char == '"':
                break
        if not keep:
            return None
        try:
            return json.loads("".join(raw))
        except json.JSONDecodeError as exc:
            raise PilotReportError("malformed JSON string") from exc

    def _primitive(self) -> Any:
        self.ws()
        token = []
        while self.peek() and self.peek() not in ",]} \t\r\n":
            token.append(self.get())
        if not token:
            raise PilotReportError("malformed JSON primitive")
        try:
            return json.loads("".join(token))
        except json.JSONDecodeError as exc:
            raise PilotReportError("malformed JSON primitive") from exc

    def skip_value(self) -> None:
        self.ws()
        char = self.peek()
        if char == '"':
            self.string(keep=False)
        elif char == "{":
            self.get(); self.ws()
            if self.peek() == "}":
                self.get(); return
            while True:
                self.string(keep=False); self.expect(":"); self.skip_value(); self.ws()
                token = self.get()
                if token == "}": return
                if token != ",": raise PilotReportError("malformed JSON object")
        elif char == "[":
            self.get(); self.ws()
            if self.peek() == "]":
                self.get(); return
            while True:
                self.skip_value(); self.ws()
                token = self.get()
                if token == "]": return
                if token != ",": raise PilotReportError("malformed JSON array")
        else:
            self._primitive()

    def parse_value(self) -> Any:
        self.ws()
        char = self.peek()
        if char == '"':
            return self.string(keep=True)
        if char == "{":
            self.get(); self.ws(); result: dict[str, Any] = {}
            if self.peek() == "}": self.get(); return result
            while True:
                key = self.string(keep=True); self.expect(":"); result[key] = self.parse_value(); self.ws()
                token = self.get()
                if token == "}": return result
                if token != ",": raise PilotReportError("malformed JSON object")
        if char == "[":
            self.get(); self.ws(); result = []
            if self.peek() == "]": self.get(); return result
            while True:
                result.append(self.parse_value()); self.ws(); token = self.get()
                if token == "]": return result
                if token != ",": raise PilotReportError("malformed JSON array")
        return self._primitive()

    def select(self, node: Mapping[str, Any]) -> Any:
        if node.get("__terminal__"):
            return self.parse_value()
        self.ws()
        if self.peek() == "[" and isinstance(node.get("*"), Mapping):
            self.get(); self.ws(); result = []
            if self.peek() == "]": self.get(); return result
            while True:
                result.append(self.select(node["*"])); self.ws(); token = self.get()
                if token == "]": return result
                if token != ",": raise PilotReportError("malformed JSON array")
        if self.peek() != "{":
            self.skip_value()
            return _MISSING
        self.get(); self.ws(); result: dict[str, Any] = {}
        if self.peek() == "}": self.get(); return result
        while True:
            key = self.string(keep=True); self.expect(":")
            child = node.get(key)
            if isinstance(child, Mapping):
                result[key] = self.select(child)
            else:
                self.skip_value()
            self.ws(); token = self.get()
            if token == "}": return result
            if token != ",": raise PilotReportError("malformed JSON object")

    def finish(self) -> None:
        self.ws()
        if self.peek():
            raise PilotReportError("trailing content after JSON object")


_MISSING = object()


def extract_aggregate_fields(path: Path, fields: Sequence[Sequence[str]]) -> dict[tuple[str, ...], Any]:
    """Parse only selected aggregate paths; skip all per-case result subtrees."""
    trie: dict[str, Any] = {}
    for field in fields:
        cursor = trie
        for part in field:
            cursor = cursor.setdefault(part, {})
        cursor["__terminal__"] = True
    parser = _SelectiveJSON(path)
    try:
        parsed = parser.select(trie)
        parser.finish()
    finally:
        parser.close()
    result: dict[tuple[str, ...], Any] = {}
    for field in fields:
        value: Any = parsed
        for part in field:
            value = value.get(part, _MISSING) if isinstance(value, Mapping) else _MISSING
        if value is not _MISSING:
            result[tuple(field)] = value
    return result


def extract_json_fields_from_text(text: str, fields: Sequence[Sequence[str]]) -> dict[tuple[str, ...], Any]:
    trie: dict[str, Any] = {}
    for field in fields:
        cursor = trie
        for part in field:
            cursor = cursor.setdefault(part, {})
        cursor["__terminal__"] = True
    parser = _SelectiveJSON(io.StringIO(text))
    parsed = parser.select(trie)
    parser.finish()
    result: dict[tuple[str, ...], Any] = {}
    for field in fields:
        value: Any = parsed
        for part in field:
            value = value.get(part, _MISSING) if isinstance(value, Mapping) else _MISSING
        if value is not _MISSING:
            result[tuple(field)] = value
    return result


def _rel(path: Path) -> str:
    return Path(path).resolve(strict=True).relative_to(ROOT.resolve()).as_posix()


def _require_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise PilotReportError(f"{label} must be an integer >= {minimum}")
    return value


def _load_accuracy_source(path: Path, method: str, seed: int) -> dict[str, Any]:
    if method == "E4":
        fields = [("status",), ("case_count",), ("cumulative_top3_prefix_successes",),
            ("total_prompt_tokens",), ("total_completion_tokens",), ("total_model_calls",)]
    elif seed == 42:
        fields = [("systems", "Terminus-2", "case_count"),
                  ("systems", "Terminus-2", "accuracy_at_3_3")]
    else:
        fields = [("status",), ("seed",), ("dataset_seed",), ("case_count",),
                  ("correct_top3_case_count",), ("accuracy_at_3",)]
    values = extract_aggregate_fields(path, fields)
    count = values.get(("case_count",), values.get(("systems", "Terminus-2", "case_count")))
    if count != len(EXPECTED_CASES):
        raise PilotReportError(f"{method} seed{seed} aggregate must cover exactly ten cases")
    if method == "E4":
        if values.get(("status",)) != "complete":
            raise PilotReportError(f"E4 seed{seed} trusted aggregate is incomplete")
        prefixes = values.get(("cumulative_top3_prefix_successes",))
        if not isinstance(prefixes, Mapping):
            raise PilotReportError(f"E4 seed{seed} prefix aggregate is missing")
        expected_keys = ("stage1_prefix_successes_of_10", "stage2_prefix_successes_of_10",
                         "stage3_prefix_successes_of_10")
        counts = {key: _require_int(prefixes.get(key), f"E4 seed{seed} {key}") for key in expected_keys}
        if any(value > 10 for value in counts.values()):
            raise PilotReportError(f"E4 seed{seed} prefix count exceeds ten")
        return {"case_count": 10, "prefix_successes": counts,
                "correct_count": counts["stage3_prefix_successes_of_10"],
                "accuracy": counts["stage3_prefix_successes_of_10"] / 10,
                "raw_aggregate_usage": {"prompt_tokens": values.get(("total_prompt_tokens",)),
                    "completion_tokens": values.get(("total_completion_tokens",)),
                    "model_calls": values.get(("total_model_calls",))}}
    if seed == 42:
        accuracy = values.get(("systems", "Terminus-2", "accuracy_at_3_3"))
        if isinstance(accuracy, bool) or not isinstance(accuracy, (int, float)) or not 0 <= accuracy <= 1:
            raise PilotReportError("historical T2 top-3 accuracy must be in [0,1]")
        correct = round(accuracy * 10)
        if abs(accuracy * 10 - correct) > 1e-9:
            raise PilotReportError("historical T2 accuracy does not resolve to a whole-case count")
        return {"case_count": 10, "correct_count": correct, "accuracy": float(accuracy)}
    if (values.get(("status",)) != "complete" or values.get(("seed",)) != seed or
            values.get(("dataset_seed",)) != 42):
        raise PilotReportError(f"T2 seed{seed} result is not a completed dataset-seed42 aggregate")
    correct = _require_int(values.get(("correct_top3_case_count",)), f"T2 seed{seed} correct count")
    accuracy = values.get(("accuracy_at_3",))
    if correct > 10 or isinstance(accuracy, bool) or not isinstance(accuracy, (int, float)) or not 0 <= accuracy <= 1:
        raise PilotReportError(f"T2 seed{seed} top-3 aggregate is invalid")
    if abs(accuracy - correct / 10) > 1e-9:
        raise PilotReportError(f"T2 seed{seed} count and accuracy disagree")
    return {"case_count": 10, "correct_count": correct, "accuracy": float(accuracy)}


def _aggregate_summary(values: Sequence[float | None]) -> dict[str, Any]:
    available = [float(value) for value in values if value is not None]
    if not available:
        return {"mean": None, "observed_range": None, "available_count": len(available),
                "expected_count": len(values)}
    return {"mean": sum(available) / len(available),
            "observed_range": {"minimum": min(available), "maximum": max(available)},
            "available_count": len(available), "expected_count": len(values)}


def _mean_known(values: Sequence[int | None]) -> dict[str, Any]:
    return _aggregate_summary(values)


def _attempt_path(value: str) -> Path:
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise PilotReportError("attempt-map path is not project-relative")
    return ROOT / relative


def _validate_attempt_proof(seed: str, proof: Mapping[str, Any],
                            queue_proof: Mapping[str, Any] | None) -> None:
    if queue_proof is not None and any(queue_proof.get(key) != value for key, value in proof.items()):
        raise PilotReportError(f"seed{seed} queue proof differs from the attempt-map record")
    run_id = proof.get("run_id")
    run_root_text = proof.get("run_root")
    if not isinstance(run_id, str) or not isinstance(run_root_text, str):
        raise PilotReportError(f"seed{seed} attempt-map proof lacks run identity")
    run_root = _attempt_path(run_root_text)
    if run_root.name != run_id:
        raise PilotReportError(f"seed{seed} attempt-map root and run ID disagree")
    status = proof.get("status")
    if status == "not_started_no_run_root":
        if proof.get("run_root_absent") is not True or run_root.exists() or run_root.is_symlink():
            raise PilotReportError(f"seed{seed} not-started attempt root unexpectedly exists")
        return

    if proof.get("all_case_artifacts_absent") is True:
        for path_key, digest_key in (("run_path", "run_sha256"),
                ("outer_supervisor_path", "outer_supervisor_sha256"),
                ("launch_path", "launch_sha256"),
                ("command_stderr_path", "command_stderr_sha256")):
            path_text, expected = proof.get(path_key), proof.get(digest_key)
            if not isinstance(path_text, str) or not isinstance(expected, str):
                raise PilotReportError(f"seed{seed} pre-case proof lacks {path_key} hash")
            path = _attempt_path(path_text)
            if path.is_symlink() or not path.is_file() or _sha(path) != expected:
                raise PilotReportError(f"seed{seed} pre-case failure artifact changed: {path_key}")
        if (status != "failed_before_case_start" or queue_proof is None or
                queue_proof.get("absent_before_case_paths", proof.get("absent_before_case_paths")) is None):
            raise PilotReportError(f"seed{seed} pre-case failure lacks absence evidence")
        absent_paths = queue_proof.get("absent_before_case_paths", proof.get("absent_before_case_paths"))
        if not isinstance(absent_paths, Mapping) or not absent_paths:
            raise PilotReportError(f"seed{seed} pre-case proof has no expected-absence inventory")
        for path_text in absent_paths.values():
            if not isinstance(path_text, str):
                raise PilotReportError(f"seed{seed} absent artifact path is malformed")
            path = _attempt_path(path_text)
            if path.exists() or path.is_symlink():
                raise PilotReportError(f"seed{seed} pre-case output unexpectedly exists")
        return

    # The original attempt-1 proof pins a real, empty trusted-evaluation file;
    # hash it without parsing or exposing its contents. Its model journal and
    # Terminus output must be absent.
    if not run_root.is_dir() or proof.get("trusted_evaluation_empty") is not True or \
            proof.get("model_http_journal_absent") is not True or \
            proof.get("terminus_result_absent") is not True:
        raise PilotReportError(f"seed{seed} setup failure does not prove zero inference output")
    for path_key, digest_key in (("run_path", "run_sha256"),
            ("outer_supervisor_path", "outer_supervisor_sha256"),
            ("checkpoint_path", "checkpoint_sha256"),
            ("trusted_evaluation_path", "trusted_evaluation_sha256")):
        path_text, expected = proof.get(path_key), proof.get(digest_key)
        if not isinstance(path_text, str) or not isinstance(expected, str):
            raise PilotReportError(f"seed{seed} setup proof lacks {path_key} hash")
        path = _attempt_path(path_text)
        if path.is_symlink() or not path.is_file() or _sha(path) != expected:
            raise PilotReportError(f"seed{seed} preserved setup-failure artifact changed: {path_key}")
    journal = _attempt_path(str(proof.get("model_http_journal_path", "")))
    if journal.exists() or journal.is_symlink() or (run_root / "t2_seed_result.json").exists():
        raise PilotReportError(f"seed{seed} earlier setup failure unexpectedly has inference output")


def _verify_attempt_chain(path: Path, expected_sha: str,
                          seen: set[Path] | None = None) -> tuple[dict[str, Any], list[dict[str, str]]]:
    path = _path(path)
    seen = set() if seen is None else seen
    if path in seen:
        raise PilotReportError("attempt-map parent chain contains a cycle")
    seen.add(path)
    if _sha(path) != expected_sha:
        raise PilotReportError(f"attempt-map digest changed: {_rel(path)}")
    attempt = _safe_json(path)
    pilot_path = LAUNCH / "pilot_manifest.json"
    if (attempt.get("schema_version") != "e4-t2-seed-pilot-attempt-map/1" or
            attempt.get("status") != "approved_zero_inference_retry" or
            attempt.get("pilot_manifest_path") != _rel(pilot_path) or
            attempt.get("pilot_manifest_sha256") != _sha(pilot_path)):
        raise PilotReportError("attempt-map is not linked to the approved pilot manifest")
    seeds = {"43", "44"}
    failed, retry = attempt.get("failed_run_ids"), attempt.get("retry_run_ids")
    if (not isinstance(failed, Mapping) or not isinstance(retry, Mapping) or
            set(failed) != seeds or set(retry) != seeds):
        raise PilotReportError("attempt-map must cover both authorized decoding seeds")
    parent_queue_path = _attempt_path(str(attempt.get("parent_queue_manifest_path", "")))
    parent_queue_sha = attempt.get("parent_queue_manifest_sha256")
    if not isinstance(parent_queue_sha, str) or _sha(parent_queue_path) != parent_queue_sha:
        raise PilotReportError("attempt-map parent queue manifest hash changed")
    parent_queue = _safe_json(parent_queue_path)
    if parent_queue.get("run_ids") != dict(failed):
        raise PilotReportError("attempt-map failed IDs do not match its parent queue")
    chain = []
    prior_path_text = attempt.get("prior_attempt_map_path")
    prior_sha = attempt.get("prior_attempt_map_sha256")
    if prior_path_text is not None or prior_sha is not None:
        if not isinstance(prior_path_text, str) or not isinstance(prior_sha, str):
            raise PilotReportError("attempt-map parent link is incomplete")
        prior_path = _attempt_path(prior_path_text)
        prior, prior_chain = _verify_attempt_chain(prior_path, prior_sha, seen)
        if (parent_queue.get("attempt_map_path") != prior_path_text or
                parent_queue.get("attempt_map_sha256") != prior_sha or
                prior.get("retry_run_ids") != dict(failed)):
            raise PilotReportError("attempt-map chain does not match the parent queue retry mapping")
        chain.extend(prior_chain)
    proofs = attempt.get("setup_failure_proofs")
    if not isinstance(proofs, Mapping) or set(proofs) != seeds:
        raise PilotReportError("attempt-map lacks setup-failure proofs for both seeds")
    for seed in sorted(seeds):
        proof = proofs[seed]
        if not isinstance(proof, Mapping) or proof.get("run_id") != failed[seed]:
            raise PilotReportError(f"seed{seed} attempt-map failure proof differs from failed run ID")
        _validate_attempt_proof(seed, proof, proof)
    chain.append({"path": _rel(path), "sha256": expected_sha})
    return attempt, chain


def _verify_approved_attempt_map(manifest: Mapping[str, Any], manifest_path: Path) -> dict[str, Any]:
    attempt_path_value = manifest.get("attempt_map_path")
    attempt_digest = manifest.get("attempt_map_sha256")
    if not isinstance(attempt_path_value, str) or not isinstance(attempt_digest, str):
        raise PilotReportError("T2 queue lacks the approved zero-inference attempt-map pin")
    attempt_path = _attempt_path(attempt_path_value)
    attempt_map, chain = _verify_attempt_chain(attempt_path, attempt_digest)
    if manifest.get("run_ids") != attempt_map.get("retry_run_ids"):
        raise PilotReportError("active queue is not linked to the approved latest attempt-map")
    proofs = attempt_map.get("setup_failure_proofs")
    queue_proofs = manifest.get("setup_failure_proofs")
    if not isinstance(queue_proofs, Mapping) or set(queue_proofs) != {"43", "44"}:
        raise PilotReportError("active queue lacks the latest setup-failure proof records")
    for seed in ("43", "44"):
        proof = proofs[seed]
        queue_proof = queue_proofs[seed]
        if not isinstance(queue_proof, Mapping) or dict(queue_proof) != dict(proof):
            raise PilotReportError(f"seed{seed} active queue proof is malformed")
        _validate_attempt_proof(seed, proof, queue_proof)
        run_row = manifest.get("runs", {}).get(seed, {})
        if run_row and run_row.get("prior_setup_failure_proof") != queue_proof:
            raise PilotReportError(f"seed{seed} retry row does not retain the setup-failure proof")
    return {"path": _rel(attempt_path), "sha256": attempt_digest, "chain": chain}


_E4_LINE_FIELDS = (
    ("call_id",), ("m4_stage",), ("http_status",), ("generation_elapsed_seconds",),
    ("request_payload", "seed"), ("request_payload", "temperature"),
    ("request_payload", "top_p"), ("request_payload", "top_k"),
    ("request_payload", "max_tokens"), ("usage", "prompt_tokens"),
    ("usage", "input_tokens"), ("usage", "completion_tokens"),
    ("usage", "output_tokens"), ("error", "code"), ("error", "prompt_tokens"),
    ("error", "max_generation_tokens"), ("error", "context_tokens"),
    ("prompt_token_count",), ("m4_requested_generation_tokens",),
    ("preflight_calls", "*", "endpoint"),
    ("preflight_calls", "*", "http_status"),
    ("preflight_calls", "*", "error"),
)
_T2_LINE_FIELDS = (
    ("event",), ("call_id",), ("request", "seed"),
    ("request", "temperature"), ("request", "top_p"), ("request", "model"),
    ("request", "max_tokens"), ("request", "extra_body", "top_k"),
    ("prompt_tokens",), ("completion_tokens",),
)


def _iter_jsonl_selected(path: Path, fields: Sequence[Sequence[str]]):
    if path.is_symlink() or not path.is_file():
        raise PilotReportError(f"missing or symlinked request journal: {path}")
    with path.open("r", encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            if line.strip():
                try:
                    record = json.loads(line)
                    if not isinstance(record, Mapping):
                        raise ValueError("row is not an object")
                    def select(value: Any, parts: Sequence[str]) -> Any:
                        if not parts:
                            return value
                        part, *tail = parts
                        if part == "*":
                            if not isinstance(value, list):
                                return _MISSING
                            return [select(item, tail) for item in value]
                        if not isinstance(value, Mapping) or part not in value:
                            return _MISSING
                        return select(value[part], tail)
                    yield {tuple(field): value for field in fields
                        if (value := select(record, field)) is not _MISSING}
                except (PilotReportError, ValueError, json.JSONDecodeError) as exc:
                    raise PilotReportError(f"malformed JSON journal {path}:{line_no}") from exc


def _verify_e4_line(row: Mapping[tuple[str, ...], Any], profile: Mapping[str, Any],
                    seed: int, label: str) -> None:
    expected = {("request_payload", "seed"): seed,
        ("request_payload", "temperature"): profile["temperature"],
        ("request_payload", "top_p"): profile["top_p"],
        ("request_payload", "top_k"): profile["top_k"]}
    mismatches = {".".join(key): row.get(key) for key, value in expected.items()
        if row.get(key) != value}
    maximum = profile["max_generation_tokens"]
    max_tokens = row.get(("request_payload", "max_tokens"))
    if type(max_tokens) is not int or not 1 <= max_tokens <= maximum:
        mismatches["request_payload.max_tokens"] = max_tokens
    if mismatches:
        raise PilotReportError(f"{label} outgoing E4 request differs from profile: {mismatches}")


def _e4_line_preflight(row: Mapping[tuple[str, ...], Any], source_sha: str) -> tuple[bool, int | None, int]:
    error_code = row.get(("error", "code"))
    prompt = row.get(("error", "prompt_tokens"))
    requested = row.get(("error", "max_generation_tokens"))
    context = row.get(("error", "context_tokens"))
    endpoints = row.get(("preflight_calls", "*", "endpoint"), [])
    statuses = row.get(("preflight_calls", "*", "http_status"), [])
    errors = row.get(("preflight_calls", "*", "error"), [])
    requested_budget = row.get(("m4_requested_generation_tokens",))
    proven = (bool(source_sha) and error_code == "context_budget_exceeded" and
        type(prompt) is int and type(requested) is int and type(context) is int and
        prompt + requested > context and row.get(("http_status",)) is None and
        row.get(("generation_elapsed_seconds",)) == 0.0 and
        endpoints == ["/apply-template", "/tokenize"] and statuses == [200, 200] and
        errors == [None, None])
    return proven, prompt if proven else None, requested_budget if type(requested_budget) is int else 0


def _t2_journal_summary(path: Path, audit: Mapping[str, Any] | None, profile: Mapping[str, Any],
                        seed: int, label: str) -> dict[str, Any]:
    request_rows = []
    responses: dict[str, Mapping[tuple[str, ...], Any]] = {}
    for row in _iter_jsonl_selected(path, _T2_LINE_FIELDS):
        event = row.get(("event",))
        call_id = row.get(("call_id",))
        if call_id is None or isinstance(call_id, (dict, list)) or call_id == "":
            raise PilotReportError(f"{label} journal row lacks a call ID")
        if event == "request_started":
            request = {"seed": row.get(("request", "seed")),
                "temperature": row.get(("request", "temperature")),
                "top_p": row.get(("request", "top_p")),
                "top_k": row.get(("request", "extra_body", "top_k")),
                "max_tokens": row.get(("request", "max_tokens")),
                "model": row.get(("request", "model"))}
            expected = {"seed": seed, "temperature": profile["temperature"],
                "top_p": profile["top_p"], "top_k": profile["top_k"],
                "model": f"openai/{profile['model_alias']}"}
            if any(request.get(key) != value for key, value in expected.items()):
                raise PilotReportError(f"{label} outgoing T2 request differs from exact seed/profile")
            if type(request["max_tokens"]) is not int or not 1 <= request["max_tokens"] <= profile["max_generation_tokens"]:
                raise PilotReportError(f"{label} outgoing T2 max_tokens exceeds the frozen profile")
            request_rows.append((call_id, request))
        elif event == "response_received":
            if call_id in responses:
                raise PilotReportError(f"{label} has duplicate response records")
            responses[call_id] = row
    call_ids = [call_id for call_id, _ in request_rows]
    if len(set(call_ids)) != len(call_ids) or not call_ids:
        raise PilotReportError(f"{label} has duplicate or missing inference request records")
    if audit is not None:
        proof_requests = audit.get("requests")
        if (not isinstance(proof_requests, list) or len(proof_requests) != len(request_rows) or
                audit.get("request_count") != len(request_rows)):
            raise PilotReportError(f"{label} journal request count differs from worker proof")
        for (call_id, request), proof in zip(request_rows, proof_requests):
            if (proof.get("call_id") != call_id or proof.get("seed") != seed or
                    proof.get("temperature") != request["temperature"] or
                    proof.get("top_p") != request["top_p"] or proof.get("top_k") != request["top_k"] or
                    proof.get("max_tokens") != request["max_tokens"]):
                raise PilotReportError(f"{label} worker audit parameters differ from actual request rows")
    if set(responses) - set(call_ids):
        raise PilotReportError(f"{label} has response records without a request")
    prompt = completion = 0
    complete = True
    for call_id in call_ids:
        response = responses.get(call_id)
        p = response.get(("prompt_tokens",)) if response is not None else None
        c = response.get(("completion_tokens",)) if response is not None else None
        if type(p) is int and type(c) is int and min(p, c) >= 0:
            prompt += p; completion += c
        else:
            complete = False
    return {"controller_attempts": len(request_rows), "http_requests": len(request_rows),
        "prompt_tokens": prompt if complete else None,
        "completion_tokens": completion if complete else None,
        "token_total": prompt + completion if complete else None,
        "usage_complete": complete}


def _e4_costs(run_root: Path, audit_dir: Path, profile: Mapping[str, Any], seed: int,
              source_sha: str) -> dict[str, Any]:
    calls_by_stage: dict[str, dict[str, Any]] = {}
    all_calls = 0
    all_http = 0
    preflight_count = preflight_prompt = budget_tokens = unclassified = 0
    prompt: int | None = 0
    completion: int | None = 0
    request_journal_hashes: list[str] = []
    shard_ids = {"local": EXPECTED_CASES[:5], "wingtip": EXPECTED_CASES[5:]}
    for shard, case_ids in shard_ids.items():
        shard_path = audit_dir / f"bank10_{shard}"
        for idx, case_id in enumerate(case_ids, 1):
            call_path = shard_path / f"case{idx:02d}" / "diagnostic" / "model_calls.jsonl"
            rows = list(_iter_jsonl_selected(call_path, _E4_LINE_FIELDS))
            request_journal_hashes.append(_sha(call_path))
            for row in rows:
                call_id = row.get(("call_id",))
                if call_id is None or call_id == "":
                    raise PilotReportError(f"E4 seed{seed} attempt lacks its controller call ID")
                _verify_e4_line(row, profile, seed, f"E4 seed{seed} {case_id}")
                stage = row.get(("m4_stage",))
                if stage not in {"stage1_select", "stage2_compose", "stage3_synthesize"}:
                    raise PilotReportError(f"E4 seed{seed} contains an unrecognized call stage")
                bucket = calls_by_stage.setdefault(stage, {"attempts": 0, "http_requests": 0,
                    "prompt_tokens": 0, "completion_tokens": 0, "usage_complete": True})
                bucket["attempts"] += 1
                all_calls += 1
                status = row.get(("http_status",))
                if status is not None:
                    all_http += 1
                    bucket["http_requests"] += 1
                    usage_prompt = row.get(("usage", "prompt_tokens"), row.get(("usage", "input_tokens")))
                    usage_completion = row.get(("usage", "completion_tokens"), row.get(("usage", "output_tokens")))
                    if type(usage_prompt) is int and type(usage_completion) is int and min(usage_prompt, usage_completion) >= 0:
                        if prompt is not None and completion is not None:
                            prompt += usage_prompt; completion += usage_completion
                        bucket["prompt_tokens"] += usage_prompt
                        bucket["completion_tokens"] += usage_completion
                    else:
                        prompt = completion = None
                        bucket["usage_complete"] = False
                else:
                    is_preflight, tokenizer_count, requested_budget = _e4_line_preflight(row, source_sha)
                    budget_tokens += requested_budget
                    if is_preflight:
                        preflight_count += 1
                        if tokenizer_count is not None:
                            preflight_prompt += tokenizer_count
                    else:
                        unclassified += 1
                        prompt = completion = None
                        bucket["usage_complete"] = False
    # The prefix cost is cumulative by stage from these exact trace rows.
    stage_order = ("stage1_select", "stage2_compose", "stage3_synthesize")
    cumulative: dict[str, Any] = {}
    running_prompt = running_completion = 0
    known = True
    for stage in stage_order:
        row = calls_by_stage.get(stage, {"attempts": 0, "http_requests": 0,
            "prompt_tokens": 0, "completion_tokens": 0, "usage_complete": True})
        known = known and row["usage_complete"]
        if known:
            running_prompt += row["prompt_tokens"]
            running_completion += row["completion_tokens"]
        cumulative[stage] = {"cumulative_prompt_tokens": running_prompt if known else None,
            "cumulative_completion_tokens": running_completion if known else None,
            "cumulative_total_tokens": running_prompt + running_completion if known else None,
            "stage_attempts": row["attempts"], "stage_http_requests": row["http_requests"],
            "stage_usage_complete": row["usage_complete"]}
    return {"controller_attempts": all_calls, "http_requests": all_http,
        "preflight_rejections": preflight_count,
        "unclassified_unreached_attempts": unclassified,
        "preflight_prompt_tokens_not_billed": preflight_prompt,
        "conservative_generated_budget_tokens_charged": budget_tokens,
        "prompt_tokens": prompt, "completion_tokens": completion,
        "token_total": prompt + completion if prompt is not None and completion is not None else None,
        "usage_complete": prompt is not None and completion is not None,
        "stage_costs": cumulative, "request_journal_count": len(request_journal_hashes),
        "request_journal_sha256s": request_journal_hashes,
        "request_journal_manifest_sha256": _hash_json(request_journal_hashes)}


def _t2_seed_costs(run_root: Path, seed: int, profile: Mapping[str, Any]) -> dict[str, Any]:
    checkpoint_path = run_root / "t2_seed_checkpoint.json"
    checkpoint = _safe_json(checkpoint_path)
    if (checkpoint.get("status") != "complete" or checkpoint.get("seed") != seed or
            checkpoint.get("dataset_seed") != 42 or checkpoint.get("case_order") != list(EXPECTED_CASES)):
        raise PilotReportError(f"T2 seed{seed} checkpoint is incomplete or not the fixed ten-case chain")
    total_calls = total_http = 0
    prompt: int | None = 0
    completion: int | None = 0
    journal_hashes: list[str] = []
    jobs = checkpoint.get("jobs")
    if not isinstance(jobs, Mapping) or list(jobs) != list(EXPECTED_CASES):
        raise PilotReportError(f"T2 seed{seed} checkpoint jobs differ from fixed case order")
    for case_id in EXPECTED_CASES:
        job = jobs[case_id].get("Terminus-2") if isinstance(jobs[case_id], Mapping) else None
        if not isinstance(job, Mapping) or job.get("status") != "complete" or job.get("history"):
            raise PilotReportError(f"T2 seed{seed} has a failed or resumed first trajectory; preserving it")
        artifact_root = _path(job["artifact_root"])
        if not artifact_root.is_relative_to((run_root / "jobs").resolve()):
            raise PilotReportError(f"T2 seed{seed} job artifacts escape their run root")
        journal = artifact_root / "output" / "model_http_calls.jsonl"
        audit_path = artifact_root / "output" / "t2_seed_request_audit.json"
        binding_path = artifact_root / "output" / "t2_seed_profile_binding.json"
        audit = _safe_json(audit_path)
        binding = _safe_json(binding_path)
        if (audit.get("schema_version") != "e4-t2-seed-request-audit/1" or
                audit.get("seed") != seed or audit.get("all_request_seeds_match") is not True or
                audit.get("model_http_calls_sha256") != _sha(journal) or
                binding.get("seed") != seed or binding.get("profile_changes_only_seed") is not True):
            raise PilotReportError(f"T2 seed{seed} per-case request/profile proof does not match journal")
        if audit.get("model_http_calls_path") != _rel(journal):
            raise PilotReportError(f"T2 seed{seed} request proof points to a different journal")
        summary = _t2_journal_summary(journal, audit, profile, seed,
            f"T2 seed{seed} {case_id}")
        request_count = summary["controller_attempts"]
        recorded_cost = job.get("cost") if isinstance(job.get("cost"), Mapping) else {}
        if recorded_cost.get("model_calls") != request_count:
            raise PilotReportError(f"T2 seed{seed} call count differs from task ledger")
        if summary["usage_complete"]:
            if (recorded_cost.get("prompt_tokens") != summary["prompt_tokens"] or
                    recorded_cost.get("completion_tokens") != summary["completion_tokens"]):
                raise PilotReportError(f"T2 seed{seed} recorded task usage differs from HTTP responses")
        elif (recorded_cost.get("prompt_tokens") is not None or
              recorded_cost.get("completion_tokens") is not None):
            raise PilotReportError(f"T2 seed{seed} task ledger imputes token usage that the journal does not establish")
        total_calls += summary["controller_attempts"]
        total_http += summary["http_requests"]
        if summary["token_total"] is None:
            prompt = completion = None
        elif prompt is not None and completion is not None:
            prompt += summary["prompt_tokens"]
            completion += summary["completion_tokens"]
        journal_hashes.append(_sha(journal))
    return {"controller_attempts": total_calls, "http_requests": total_http,
        "preflight_rejections": 0, "prompt_tokens": prompt, "completion_tokens": completion,
        "token_total": prompt + completion if prompt is not None and completion is not None else None,
        "usage_complete": prompt is not None and completion is not None,
        "checkpoint_path": _rel(checkpoint_path), "checkpoint_sha256": _sha(checkpoint_path),
        "request_journal_count": len(journal_hashes),
        "request_journal_sha256s": journal_hashes,
        "request_journal_manifest_sha256": _hash_json(journal_hashes)}


def _historical_t2_costs(profile: Mapping[str, Any], *, model_checkpoint_sha256: str,
                        baseline_manifest_sha256: str) -> dict[str, Any]:
    run_root = ROOT / "artifacts/m3/model_runs/bankm6i2/m6_comparison"
    aggregate_path = run_root / "comparison_result.json"
    launch = _safe_json(ROOT / "artifacts/m3/model_runs/bankm6i2/launch.json")
    if (launch.get("run_id") != "bankm6i2" or launch.get("seed") != 42 or
            launch.get("model_sha256") != model_checkpoint_sha256):
        raise PilotReportError("historical T2 server launch does not match seed42 checkpoint")
    fields = [("run_id",), ("split",), ("case_ids",), ("experiment_manifest_sha256",)]
    selected = extract_aggregate_fields(aggregate_path, fields)
    manifest = _safe_json(ROOT / "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json")
    if (selected.get(("run_id",)) != manifest.get("run_id") or selected.get(("split",)) != "scored" or
            selected.get(("case_ids",)) != list(EXPECTED_CASES)):
        raise PilotReportError("historical T2 comparison is not the fixed seed42 ten-case run")
    if (selected.get(("experiment_manifest_sha256",)) != baseline_manifest_sha256 or
            selected.get(("experiment_manifest_sha256",)) != _sha(
                ROOT / "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json") or
            manifest.get("seed") != 42 or manifest.get("case_ids") != list(EXPECTED_CASES)):
        raise PilotReportError("historical T2 comparison does not link to original seed42 freeze")
    calls = http = 0
    prompt: int | None = 0
    completion: int | None = 0
    journal_hashes: list[str] = []
    for case_id in EXPECTED_CASES:
        task_root = run_root / "jobs" / case_id / "Terminus-2"
        attempts = sorted(path for path in task_root.glob("attempt-*") if path.is_dir())
        if not attempts:
            raise PilotReportError("historical T2 is missing a completed attempt artifact")
        for attempt in attempts:
            journal = attempt / "output" / "model_http_calls.jsonl"
            summary = _t2_journal_summary(journal, None, profile, 42,
                f"historical T2 {case_id} {attempt.name}")
            calls += summary["controller_attempts"]
            http += summary["http_requests"]
            if summary["token_total"] is None:
                prompt = completion = None
            elif prompt is not None and completion is not None:
                prompt += summary["prompt_tokens"]
                completion += summary["completion_tokens"]
            journal_hashes.append(_sha(journal))
    return {"controller_attempts": calls, "http_requests": http,
        "preflight_rejections": 0, "prompt_tokens": prompt, "completion_tokens": completion,
        "token_total": prompt + completion if prompt is not None and completion is not None else None,
        "usage_complete": prompt is not None and completion is not None,
        "aggregate_source_path": _rel(aggregate_path), "aggregate_source_sha256": _sha(aggregate_path),
        "request_journal_count": len(journal_hashes),
        "request_journal_sha256s": journal_hashes,
        "request_journal_manifest_sha256": _hash_json(journal_hashes)}


def _find_run06_e4_root(freeze_path: Path, freeze_sha256: str,
                        profile_sha256: str) -> Path:
    if _sha(freeze_path) != freeze_sha256:
        raise PilotReportError("run06 E4 freeze digest changed")
    matches = []
    runs_root = ROOT / "artifacts/m3/model_runs"
    for audit_manifest in runs_root.glob("paperstarter_bank10_*/audits/*/audit_manifest.json"):
        if audit_manifest.is_symlink() or not audit_manifest.is_file():
            continue
        fields = extract_aggregate_fields(audit_manifest,
            [("freeze_manifest_sha256",), ("model_profile_sha256",), ("case_ids",), ("status",)])
        if (fields.get(("freeze_manifest_sha256",)) == freeze_sha256 and
                fields.get(("model_profile_sha256",)) == profile_sha256 and
                fields.get(("case_ids",)) == list(EXPECTED_CASES)):
            run_root = audit_manifest.parent.parent.parent
            if ((run_root / "run.json").is_file() and
                    (run_root / "local_shard_handoff.json").is_file() and
                    (run_root / "wingtip_shard_handoff.json").is_file()):
                matches.append(run_root)
    unique = sorted(set(matches))
    if len(unique) != 1:
        raise PilotReportError("could not identify exactly one completed run06 E4 execution root from its frozen audit pin")
    run_root = unique[0]
    run_doc = _safe_json(run_root / "run.json")
    if run_doc.get("ok") is not True or run_doc.get("exit_code") != 0:
        raise PilotReportError("frozen run06 E4 execution is not complete")
    return run_root


def _run06_initial_snapshot_id(pilot: Mapping[str, Any],
                               prepared: Mapping[str, Any]) -> str:
    freeze_path = _path(ROOT / pilot["base_run06_freeze_path"])
    if _sha(freeze_path) != prepared.get("run06_freeze_sha256"):
        raise PilotReportError("run06 starter freeze digest changed")
    freeze = _safe_json(freeze_path)
    snapshot_id = freeze.get("initial_library_snapshot_id")
    if (not isinstance(snapshot_id, str) or
            snapshot_id != pilot.get("initial_bank_snapshot_id") or
            any(prepared.get("seeds", {}).get(str(seed), {}).get("initial_library_snapshot_id") != snapshot_id
                for seed in (43, 44))):
        raise PilotReportError("seed42 run06 initial starter differs from the shared frozen seed43/44 snapshot")
    return snapshot_id


def wait_for_completed_queue(queue_root: Path, *, timeout_seconds: int = 86400,
                             poll_seconds: int = 30) -> dict[str, Any]:
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise PilotReportError("queue wait and poll durations must be positive")
    queue_root = _path(queue_root)
    if (queue_root.parent != ROOT / "artifacts/seed_variability/runs" or
            not queue_root.name.startswith("t2_queue_")):
        raise PilotReportError("queue root must be one artifact-local t2_queue_* directory")
    manifest_path = queue_root / "queue_manifest.json"
    status_path = queue_root / "queue_status.json"
    deadline = time.monotonic() + timeout_seconds
    last_state = "queue artifacts are not ready"
    while time.monotonic() < deadline:
        if manifest_path.is_file() and status_path.is_file():
            try:
                manifest = _safe_json(manifest_path)
                status = _safe_json(status_path)
            except PilotReportError as exc:
                last_state = str(exc)
            else:
                if (manifest.get("schema_version") != "e4-t2-seed-variability-queue/1" or
                        status.get("schema_version") != "e4-t2-seed-queue-status/1" or
                        manifest.get("seed_order") != [43, 44] or
                        manifest.get("dataset_seed") != 42 or
                        manifest.get("case_order") != list(EXPECTED_CASES)):
                    raise PilotReportError("T2 queue differs from the fixed seed43/44 dataset42 contract")
                if status.get("queue_manifest_sha256") != _sha(manifest_path):
                    last_state = "queue status has not caught up to its manifest hash"
                elif status.get("status") in QUEUE_TERMINAL and manifest.get("status") in QUEUE_TERMINAL:
                    if status.get("status") != "complete" or manifest.get("status") != "complete":
                        raise PilotReportError("T2 queue completed with a preserved failure; no aggregate report written")
                    if (status.get("finished_seeds") != [43, 44] or
                            list(manifest.get("runs", {})) != ["43", "44"] or
                            any(manifest["runs"][str(seed)].get("status") != "complete" for seed in (43, 44))):
                        raise PilotReportError("T2 queue terminal state does not prove both first trajectories complete")
                    for relative, expected in manifest.get("helper_source_sha256", {}).items():
                        helper = _path(ROOT / relative)
                        if _sha(helper) != expected:
                            raise PilotReportError(f"T2 queue helper seal changed: {relative}")
                    archive = manifest.get("source_archive")
                    if (not isinstance(archive, Mapping) or type(archive.get("source_file_count")) is not int or
                            archive["source_file_count"] < 1):
                        raise PilotReportError("T2 queue lacks its complete source archive pin")
                    archive_path = _path(ROOT / str(archive.get("path", "")))
                    if _sha(archive_path) != archive.get("sha256"):
                        raise PilotReportError("T2 queue helper source archive hash changed")
                    attempt_map = _verify_approved_attempt_map(manifest, manifest_path)
                    return {"queue_root": _rel(queue_root), "queue_manifest_path": _rel(manifest_path),
                        "queue_manifest_sha256": _sha(manifest_path),
                        "queue_status_sha256": _sha(status_path), "attempt_map": attempt_map,
                        "source_archive": {"path": _rel(archive_path), "sha256": _sha(archive_path),
                            "source_file_count": archive["source_file_count"]},
                        "manifest": manifest,
                        "status": status}
                else:
                    last_state = f"queue status={status.get('status')} manifest={manifest.get('status')}"
        time.sleep(poll_seconds)
    raise TimeoutError(f"timed out waiting for T2 queue completion: {last_state}")


def _accuracy_inputs(queue: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    prepared = pilot_verify.verify_prepared_pilot()
    pilot = _safe_json(LAUNCH / "pilot_manifest.json")
    accuracy: dict[str, Any] = {}
    provenance: dict[str, Any] = {}
    # The frozen E4 run06 result and historical T2 comparison provide seed42.
    base_run = _find_run06_e4_root(_path(ROOT / pilot["base_run06_freeze_path"]),
        prepared["run06_freeze_sha256"], pilot["base_model_profile_sha256"])
    base_starter_id = _run06_initial_snapshot_id(pilot, prepared)
    handoff = _safe_json(base_run / "local_shard_handoff.json")
    remote_handoff = _safe_json(base_run / "wingtip_shard_handoff.json")
    base_audit = _path(handoff["audit_dir"])
    if (handoff.get("status") != "complete" or remote_handoff.get("status") != "complete" or
            remote_handoff.get("audit_dir") != _rel(base_audit)):
        raise PilotReportError("run06 E4 local/Wingtip handoffs do not reference one completed audit")
    base_audit_manifest = _safe_json(base_audit / "audit_manifest.json")
    if (base_audit_manifest.get("freeze_manifest_sha256") != prepared["run06_freeze_sha256"] or
            base_audit_manifest.get("case_ids") != list(EXPECTED_CASES) or
            base_audit_manifest.get("initial_library_snapshot_id") != base_starter_id or
            base_audit_manifest.get("model_profile_sha256") != pilot["base_model_profile_sha256"]):
        raise PilotReportError("run06 E4 baseline audit differs from frozen source/profile/starter")
    e4_t2_results = {}
    for seed, run_root in ((43, ROOT / "artifacts/m3/model_runs/e4s43"),
                           (44, ROOT / "artifacts/m3/model_runs/e4s44")):
        queue_run = queue["manifest"]["runs"][str(seed)]
        exact_root = queue_run.get("run_root")
        run_id = queue_run.get("run_id")
        if (not isinstance(exact_root, str) or not isinstance(run_id, str) or
                Path(exact_root).name != run_id or
                Path(exact_root).parent != Path("artifacts/m3/model_runs") or
                not run_id.startswith(f"t2s{seed}l") or run_id == f"t2s{seed}l"):
            raise PilotReportError(f"T2 seed{seed} queue does not name its approved fresh attempt root")
        run_root = _path(ROOT / exact_root)
        result_path = run_root / "t2_seed_result.json"
        t2_accuracy = _load_accuracy_source(result_path, "T2", seed)
        checkpoint_path = run_root / "t2_seed_checkpoint.json"
        checkpoint = _safe_json(checkpoint_path)
        profile_path = _path(ROOT / pilot["seed_profiles"][str(seed)]["path"])
        run_manifest_path = run_root / "t2_seed_manifest.json"
        run_manifest = _safe_json(run_manifest_path)
        profile_sha = _sha(profile_path)
        result_fields = extract_aggregate_fields(result_path,
            [("pilot_manifest_sha256",), ("run_root",), ("model_profile_sha256",),
             ("initial_bank_snapshot_id",), ("all_outgoing_http_requests_match_seed",)])
        if (checkpoint.get("schema_version") != "e4-t2-seed-pilot-checkpoint/1" or
                checkpoint.get("pilot_manifest_sha256") != prepared["pilot_manifest_sha256"] or
                checkpoint.get("status") != "complete" or checkpoint.get("seed") != seed or
                checkpoint.get("dataset_seed") != 42 or
                checkpoint.get("case_order") != list(EXPECTED_CASES) or
                checkpoint.get("initial_library_snapshot_id") != prepared["seeds"][str(seed)]["initial_library_snapshot_id"] or
                run_manifest.get("schema_version") != "e4-t2-seed-pilot-run/1" or
                run_manifest.get("seed") != seed or run_manifest.get("dataset_seed") != 42 or
                run_manifest.get("case_order") != list(EXPECTED_CASES) or
                run_manifest.get("pilot_manifest_sha256") != prepared["pilot_manifest_sha256"] or
                run_manifest.get("profile_sha256") != profile_sha or
                run_manifest.get("initial_bank_snapshot_id") != prepared["seeds"][str(seed)]["initial_library_snapshot_id"] or
                result_fields.get(("pilot_manifest_sha256",)) != prepared["pilot_manifest_sha256"] or
                result_fields.get(("run_root",)) != exact_root or
                result_fields.get(("model_profile_sha256",)) != profile_sha or
                result_fields.get(("initial_bank_snapshot_id",)) != prepared["seeds"][str(seed)]["initial_library_snapshot_id"] or
                result_fields.get(("all_outgoing_http_requests_match_seed",)) is not True):
            raise PilotReportError(f"T2 seed{seed} checkpoint is not linked to prepared pilot")
        queue_run = queue["manifest"]["runs"][str(seed)]
        if (queue_run.get("launcher_exit_code") != 0 or queue_run.get("pilot_status") != "complete" or
                queue_run.get("checkpoint_sha256") != _sha(checkpoint_path)):
            raise PilotReportError(f"T2 seed{seed} queue completion does not bind its finished checkpoint")
        t2_cost = _t2_seed_costs(run_root, seed, _safe_json(profile_path))
        t2_aggregate_path = result_path
        e4_audit_root = ROOT / "artifacts/m3/model_runs" / f"e4s{seed}"
        e4_reports = pilot_verify.verify_e4_seed(seed, e4_audit_root)
        e4_audit_path = _path(e4_reports["audit_dir"])
        seed_manifest = _safe_json(LAUNCH / f"seed{seed}" / "seed_manifest.json")
        derived_freeze_path = _path(ROOT / seed_manifest["derived_freeze_path"])
        e4_aggregate_path = e4_audit_path / "combined_bank10_result.json"
        e4_accuracy = _load_accuracy_source(e4_aggregate_path, "E4", seed)
        e4_cost = _e4_costs(e4_audit_root, e4_audit_path,
            _safe_json(_path(ROOT / pilot["seed_profiles"][str(seed)]["path"])), seed,
            prepared["context_preflight_source_sha256"])
        raw_aggregate = e4_accuracy["raw_aggregate_usage"]
        if (raw_aggregate.get("model_calls") != e4_cost["controller_attempts"] or
                (raw_aggregate.get("prompt_tokens") is not None and
                 raw_aggregate.get("prompt_tokens") != e4_cost["prompt_tokens"]) or
                (raw_aggregate.get("completion_tokens") is not None and
                 raw_aggregate.get("completion_tokens") != e4_cost["completion_tokens"])):
            raise PilotReportError(f"E4 seed{seed} journal totals disagree with trusted raw aggregate")
        if (e4_cost["controller_attempts"] != e4_reports["total_controller_call_attempts"] or
                e4_cost["prompt_tokens"] != e4_reports["derived_online_inference_prompt_tokens"] or
                e4_cost["completion_tokens"] != e4_reports["derived_online_inference_completion_tokens"]):
            raise PilotReportError(f"E4 seed{seed} report cost differs from independent request verifier")
        e4_t2_results[str(seed)] = {"E4": (e4_accuracy, e4_cost), "T2": (t2_accuracy, t2_cost)}
        provenance[f"seed{seed}_E4"] = {"run_root": e4_reports["execution_root"],
            "audit_dir": e4_reports["audit_dir"], "aggregate_path": _rel(e4_aggregate_path),
            "aggregate_sha256": _sha(e4_aggregate_path),
            "audit_manifest_path": _rel(e4_audit_path / "audit_manifest.json"),
            "audit_manifest_sha256": _sha(e4_audit_path / "audit_manifest.json"),
            "derived_freeze_path": _rel(derived_freeze_path),
            "derived_freeze_sha256": _sha(derived_freeze_path),
            "profile_path": pilot["seed_profiles"][str(seed)]["path"],
            "profile_sha256": _sha(_path(ROOT / pilot["seed_profiles"][str(seed)]["path"])),
            "request_journal_count": e4_cost["request_journal_count"],
            "request_journal_sha256s": e4_cost["request_journal_sha256s"],
            "request_journal_manifest_sha256": e4_cost["request_journal_manifest_sha256"]}
        provenance[f"seed{seed}_T2"] = {"run_root": exact_root, "run_id": run_id,
            "aggregate_path": _rel(t2_aggregate_path), "aggregate_sha256": _sha(t2_aggregate_path),
            "profile_path": pilot["seed_profiles"][str(seed)]["path"], "profile_sha256": profile_sha,
            "run_manifest_path": _rel(run_manifest_path), "run_manifest_sha256": _sha(run_manifest_path),
            "checkpoint_path": _rel(checkpoint_path), "checkpoint_sha256": _sha(checkpoint_path),
            "request_journal_count": t2_cost["request_journal_count"],
            "request_journal_sha256s": t2_cost["request_journal_sha256s"],
            "request_journal_manifest_sha256": t2_cost["request_journal_manifest_sha256"]}

    base_e4_aggregate = base_audit / "combined_bank10_result.json"
    base_e4_accuracy = _load_accuracy_source(base_e4_aggregate, "E4", 42)
    base_e4_cost = _e4_costs(base_run, base_audit,
        _safe_json(_path(ROOT / pilot["base_model_profile_path"])), 42,
        prepared["context_preflight_source_sha256"])
    raw_base_e4 = base_e4_accuracy["raw_aggregate_usage"]
    if (raw_base_e4.get("model_calls") != base_e4_cost["controller_attempts"] or
            raw_base_e4.get("prompt_tokens") != base_e4_cost["prompt_tokens"] or
            raw_base_e4.get("completion_tokens") != base_e4_cost["completion_tokens"]):
        raise PilotReportError("run06 E4 journal totals disagree with frozen trusted aggregate")
    historical_t2_aggregate = ROOT / "artifacts/m3/model_runs/bankm6i2/m6_comparison/comparison_result.json"
    base_t2_accuracy = _load_accuracy_source(historical_t2_aggregate, "T2", 42)
    base_t2_cost = _historical_t2_costs(
        _safe_json(_path(ROOT / pilot["base_model_profile_path"])),
        model_checkpoint_sha256=pilot["model_checkpoint_sha256"],
        baseline_manifest_sha256=prepared["historical_t2_manifest_sha256"])
    e4_t2_results["42"] = {"E4": (base_e4_accuracy, base_e4_cost), "T2": (base_t2_accuracy, base_t2_cost)}
    provenance["seed42_E4"] = {"run_root": str(base_run.relative_to(ROOT)),
        "audit_dir": _rel(base_audit), "aggregate_path": _rel(base_e4_aggregate),
        "aggregate_sha256": _sha(base_e4_aggregate),
        "freeze_path": pilot["base_run06_freeze_path"],
        "freeze_sha256": prepared["run06_freeze_sha256"],
        "audit_manifest_path": _rel(base_audit / "audit_manifest.json"),
        "audit_manifest_sha256": _sha(base_audit / "audit_manifest.json"),
        "profile_path": pilot["base_model_profile_path"],
        "profile_sha256": pilot["base_model_profile_sha256"],
        "request_journal_count": base_e4_cost["request_journal_count"],
        "request_journal_sha256s": base_e4_cost["request_journal_sha256s"],
        "request_journal_manifest_sha256": base_e4_cost["request_journal_manifest_sha256"]}
    provenance["seed42_T2"] = {"run_root": "artifacts/m3/model_runs/bankm6i2/m6_comparison",
        "aggregate_path": _rel(historical_t2_aggregate),
        "aggregate_sha256": _sha(historical_t2_aggregate),
        "baseline_manifest_path": "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json",
        "baseline_manifest_sha256": prepared["historical_t2_manifest_sha256"],
        "launch_path": "artifacts/m3/model_runs/bankm6i2/launch.json",
        "launch_sha256": _sha(ROOT / "artifacts/m3/model_runs/bankm6i2/launch.json"),
        "profile_path": pilot["base_model_profile_path"],
        "profile_sha256": pilot["base_model_profile_sha256"],
        "request_journal_count": base_t2_cost["request_journal_count"],
        "request_journal_sha256s": base_t2_cost["request_journal_sha256s"],
        "request_journal_manifest_sha256": base_t2_cost["request_journal_manifest_sha256"]}
    return e4_t2_results, provenance


def _build_report(queue: Mapping[str, Any], *, report_id: str,
                  created_utc: str | None = None) -> tuple[dict[str, Any], str]:
    results, provenance = _accuracy_inputs(queue)
    frozen_inputs = pilot_verify.verify_prepared_pilot()
    per_seed: dict[str, Any] = {}
    for seed in SEEDS:
        e4_accuracy, e4_cost = results[str(seed)]["E4"]
        t2_accuracy, t2_cost = results[str(seed)]["T2"]
        e4_total, t2_total = e4_cost["token_total"], t2_cost["token_total"]
        ratio = (100.0 * e4_total / t2_total if isinstance(e4_total, int) and
            isinstance(t2_total, int) and t2_total > 0 else None)
        per_seed[str(seed)] = {"distinct_cases": 10,
            "E4": {**e4_accuracy, **e4_cost},
            "T2": {**t2_accuracy, **t2_cost},
            "E4_cost_as_percent_of_same_seed_T2": ratio}
    summary = {}
    for system in ("E4", "T2"):
        summary[system] = {
            "accuracy": _aggregate_summary([per_seed[str(seed)][system]["accuracy"] for seed in SEEDS]),
            "top3_correct_cases": _aggregate_summary([per_seed[str(seed)][system]["correct_count"] for seed in SEEDS]),
            "input_tokens": _mean_known([per_seed[str(seed)][system]["prompt_tokens"] for seed in SEEDS]),
            "output_tokens": _mean_known([per_seed[str(seed)][system]["completion_tokens"] for seed in SEEDS]),
            "total_tokens": _mean_known([per_seed[str(seed)][system]["token_total"] for seed in SEEDS]),
        }
    ratios = [per_seed[str(seed)]["E4_cost_as_percent_of_same_seed_T2"] for seed in SEEDS]
    summary["E4_cost_as_percent_of_same_seed_T2"] = _aggregate_summary(ratios)
    report = {"schema_version": "e4-seed-variability-report/1", "report_id": report_id,
        "created_utc": created_utc or datetime.now(timezone.utc).isoformat(),
        "pilot_design": {"distinct_incidents": 10, "dataset_seed": 42,
            "decoding_seeds": list(SEEDS), "same_incidents_repeated_each_seed": True,
            "independent_observation_count": 10, "exploratory_only": True,
            "claims": "descriptive variation only; no significance, equivalence, or noninferiority claim"},
        "accuracy_source": "trusted_parent_aggregate_source_verification; root-owned independent ledger rescore remains separate",
        "cost_definition": "observed online input plus output tokens across all inference HTTP calls, including repair and recovery calls; verified preflight rejections contribute zero inference tokens and their tokenizer work is reported separately",
        "queue_provenance": {"queue_root": queue["queue_root"],
            "queue_manifest_path": queue["queue_manifest_path"],
            "queue_manifest_sha256": queue["queue_manifest_sha256"],
            "queue_status_sha256": queue["queue_status_sha256"],
            "source_archive": queue["source_archive"],
            "approved_attempt_map": queue["attempt_map"],
            "run_ids": {str(seed): queue["manifest"]["runs"][str(seed)]["run_id"]
                for seed in (43, 44)}},
        "frozen_inputs": {key: frozen_inputs[key] for key in
            ("pilot_manifest_sha256", "run06_freeze_sha256", "historical_t2_manifest_sha256")},
        "per_seed": per_seed, "summary": summary,
        "sources": provenance, "published_case_identifiers": False,
        "evaluator_labels_read_by_reporter": False}
    md = render_markdown(report)
    return report, md


def render_markdown(report: Mapping[str, Any]) -> str:
    rows = ["# Seed variability pilot results", "",
        "**Exploratory descriptive report.** These are three repeated decoding runs over the same 10 incidents (30 run-case observations, 10 distinct incidents); they are not 30 independent samples. No significance, equivalence, or noninferiority claim is made.", "",
        "Scoring values come from verified trusted-parent aggregate outputs. The root-owned independent T2 ledger rescore is a separate follow-up check.", "",
        "Costs count observed online prompt and completion tokens from all inference HTTP requests, including repair and recovery calls. A verified preflight rejection before inference contributes zero online tokens; tokenizer work is listed separately. Missing usage remains unknown.", "",
        "| Decoding seed | E4 top-3 | T2 top-3 | E4 attempts / HTTP / preflight | T2 HTTP requests | E4 input + output tokens | T2 input + output tokens | E4 cost / same-seed T2 |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for seed in SEEDS:
        item = report["per_seed"][str(seed)]
        e4, t2 = item["E4"], item["T2"]
        e4_tok = "unknown" if e4["token_total"] is None else f"{e4['token_total']:,}"
        t2_tok = "unknown" if t2["token_total"] is None else f"{t2['token_total']:,}"
        ratio = item["E4_cost_as_percent_of_same_seed_T2"]
        ratio_text = "unknown" if ratio is None else f"{ratio:.1f}%"
        rows.append(f"| {seed} | {e4['correct_count']}/10 ({e4['accuracy']:.0%}) | {t2['correct_count']}/10 ({t2['accuracy']:.0%}) | {e4['controller_attempts']} / {e4['http_requests']} / {e4['preflight_rejections']} | {t2['http_requests']} | {e4_tok} | {t2_tok} | {ratio_text} |")
    rows.extend(["", "## Across-seed descriptive summary", "",
        "| Measure | Mean | Observed range |", "|---|---:|---:|"])
    summary_rows = (("E4 top-3 accuracy", report["summary"]["E4"]["accuracy"], "percent"),
        ("T2 top-3 accuracy", report["summary"]["T2"]["accuracy"], "percent"),
        ("E4 input + output tokens", report["summary"]["E4"]["total_tokens"], "tokens"),
        ("T2 input + output tokens", report["summary"]["T2"]["total_tokens"], "tokens"),
        ("E4 cost / same-seed T2", report["summary"]["E4_cost_as_percent_of_same_seed_T2"], "ratio"))
    for label, data, style in summary_rows:
        mean = data["mean"]
        observed = data["observed_range"]
        if mean is None or observed is None:
            mean_s, range_s = "unknown", "unknown"
        elif style == "percent":
            mean_s = f"{mean * 100:.1f}%"
            range_s = f"{observed['minimum'] * 100:.1f}%–{observed['maximum'] * 100:.1f}%"
        elif style == "tokens":
            mean_s = f"{mean:,.0f}"
            range_s = f"{observed['minimum']:,.0f}–{observed['maximum']:,.0f}"
        else:
            mean_s = f"{mean:.1f}%"
            range_s = f"{observed['minimum']:.1f}%–{observed['maximum']:.1f}%"
        if mean is not None:
            mean_s += f" (n={data['available_count']}/{data['expected_count']})"
        rows.append(f"| {label} | {mean_s} | {range_s} |")
    rows.extend(["", "## E4 prefix counts and same-trace cost", "",
        "Prefix counts are the parent aggregate's cumulative top-3 successes. Prefix token costs are accumulated by E4 stage from the same request journals; all retries/repairs logged in the stage are included.", "",
        "| Seed | Stage | Top-3 prefix successes | Cumulative input + output tokens |", "|---:|---|---:|---:|"])
    stage_keys = ("stage1_prefix_successes_of_10", "stage2_prefix_successes_of_10", "stage3_prefix_successes_of_10")
    stage_names = ("Selection", "Composition", "Synthesis")
    for seed in SEEDS:
        e4 = report["per_seed"][str(seed)]["E4"]
        stage_map = e4["stage_costs"]
        for key, name, stage_key in zip(stage_keys, stage_names,
                ("stage1_select", "stage2_compose", "stage3_synthesize")):
            cost = stage_map[stage_key]["cumulative_total_tokens"]
            rows.append(f"| {seed} | {name} | {e4['prefix_successes'][key]}/10 | {'unknown' if cost is None else f'{cost:,}'} |")
    rows.extend(["", "For seed43, the controller aggregate's input and output token fields remain null. Separately, the verified request journals show 36 controller attempts, 35 inference HTTP requests, and one context-budget preflight rejection; that attempt contributes zero online inference tokens, and its tokenizer count is separate in JSON.", "",
        f"Report ID: `{report['report_id']}`", ""])
    return "\n".join(rows)


def write_immutable_report(queue_root: Path, *, report_id: str | None = None,
                           output_root: Path | None = None, timeout_seconds: int = 86400,
                           poll_seconds: int = 30) -> dict[str, Any]:
    queue = wait_for_completed_queue(queue_root, timeout_seconds=timeout_seconds,
                                     poll_seconds=poll_seconds)
    report_id = report_id or (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4))
    if not report_id or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in report_id):
        raise PilotReportError("report ID contains unsupported characters")
    report, markdown = _build_report(queue, report_id=report_id)
    parent = Path(output_root) if output_root is not None else ROOT / "artifacts/seed_variability/reports"
    if parent.is_symlink():
        raise PilotReportError("report output directory may not be a symlink")
    parent.mkdir(parents=True, exist_ok=True)
    parent = parent.resolve(strict=True)
    if not parent.is_relative_to(ROOT.resolve()):
        raise PilotReportError("report output directory escapes workspace")
    final = parent / report_id
    if final.exists() or final.is_symlink():
        raise FileExistsError(f"preserving existing report directory: {final}")
    temp = parent / ("." + report_id + ".pending-" + secrets.token_hex(4))
    temp.mkdir(mode=0o700, exist_ok=False)
    try:
        json_path = temp / "seed_variability_report.json"
        md_path = temp / "seed_variability_report.md"
        for path, content in ((json_path, json.dumps(report, ensure_ascii=False,
                sort_keys=True, indent=2, allow_nan=False) + "\n"), (md_path, markdown)):
            with path.open("x", encoding="utf-8") as stream:
                stream.write(content); stream.flush(); os.fsync(stream.fileno())
        os.rename(temp, final)
    except Exception:
        # Keep a failed partial in its hidden pending directory for audit; never
        # overwrite or clean an existing report.
        raise
    return {"status": "written", "report_dir": str(final.relative_to(ROOT)),
        "json_path": str((final / "seed_variability_report.json").relative_to(ROOT)),
        "json_sha256": _sha(final / "seed_variability_report.json"),
        "markdown_path": str((final / "seed_variability_report.md").relative_to(ROOT)),
        "markdown_sha256": _sha(final / "seed_variability_report.md"),
        "queue_manifest_sha256": queue["queue_manifest_sha256"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path, required=True,
        help="completed artifact-local t2_queue_* directory; the reporter waits for it")
    parser.add_argument("--timeout-seconds", type=int, default=86400)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--report-id")
    parser.add_argument("--output-root", type=Path,
        help="optional new artifact output parent; defaults to artifacts/seed_variability/reports")
    args = parser.parse_args()
    result = write_immutable_report(args.queue_root, report_id=args.report_id,
        output_root=args.output_root, timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
