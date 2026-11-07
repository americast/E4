"""Resumable adapter-first Bank onboarding and frozen-library evaluation.

No scored/private labels enter adapter or library prompts/gates. Model calls are
journaled by LocalLlamaClient; this orchestrator never runs a model implicitly.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.bank_benchmark import (
    LocalLlamaClient,
    load_ground_truth,
    run_benchmark,
)
from benchmarks.bank_onboarding import (
    bank_adapter_config,
    build_bank_adapter_prompt,
    validate_bank_adapter_submission,
)
from e4.bootstrap import LibrarySubmissionError, apply_library_submission, parse_library_submission
from e4.controller.controller import _to_ranked_answer
from e4.controller.library import LibraryManager
from e4.dag import SerialExecutor, verify_dag
from e4.data.record_store import TelemetryRecordStore
from e4.data.submissions import AdapterSubmissionError, load_adapter_submission, parse_adapter_submission, validate_adapter_config
from e4.onboarding import normalize_dataset_schema, prepare_library
from e4.oracle import GroundTruthOracle
from e4.prompts import render_initialization_prompt
from e4.sandbox.runner import SubprocessRunner
from e4.sandbox.views import TelemetryView
from e4.types import EntityRef, RankedAnswer, ScoredCandidates, Window

SAMPLE_PATH = ROOT / "config" / "bank_sample.json"
CASES_DIR = ROOT / "data" / "prepared" / "bank" / "public" / "cases"
GROUND_TRUTH = ROOT / "data" / "prepared" / "bank" / "evaluator" / "scored_ground_truth.jsonl"
INFERENCE_CONFIG = ROOT / "config" / "gemma_m3_inference.json"
SEEDS = (42, 43, 44)
SHARDS = (0, 1)
MODES = ("paper", "clean_slate")
OPERATOR_TIMEOUT_SECONDS = 120
OPERATOR_MEMORY_LIMIT_MB = 512
_STATE_LOCK = threading.RLock()


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        dfd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            doc = json.loads(line)
            if isinstance(doc, dict):
                rows.append(doc)
    return rows


def _load_plan() -> tuple[list[str], list[str], list[str]]:
    doc = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    selection = doc.get("selection", {})
    scored = list(selection.get("scored_ids", []))
    rows = [row["incident_id"] for row in doc.get("scored_cases", [])]
    development = list(selection.get("development_ids", []))
    if len(scored) != 10 or len(set(scored)) != 10 or scored != rows:
        raise ValueError("selection.scored_ids must match the ten scored_cases in declared order")
    if len(development) != 2 or len(set(development)) != 2 or set(development) & set(scored):
        raise ValueError("Bank onboarding requires the two disjoint public development_ids")
    labels = load_ground_truth(GROUND_TRUTH)
    if any(cid not in labels for cid in scored):
        raise ValueError("Ground truth is missing one or more configured scored IDs")
    if any(not (CASES_DIR / cid / "alert.json").is_file() for cid in scored + development):
        raise FileNotFoundError("One or more configured public Bank case clips are missing")
    return scored, development, rows


def _deadline_epoch(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _deadline_hit(deadline: float | None) -> bool:
    return deadline is not None and time.time() >= deadline


class _JournalClient:
    """Exact-prompt response replay over a durable LocalLlamaClient JSONL journal."""

    def __init__(self, base: LocalLlamaClient, endpoint: str, journal_path: Path,
                 accounted: set[int] | None = None,
                 call_owners: Mapping[str, Any] | None = None):
        self.base = base
        self.endpoint = endpoint
        self.journal_path = journal_path
        self.rows = _jsonl(journal_path)
        self.accounted = set(accounted or ())
        self.used = set(self.accounted)
        self.call_owners = dict(call_owners or {})
        self.request_context: dict[str, Any] | None = None
        self.calls: list[dict[str, Any]] = []
        self._usage = {"total_requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
                       "total_tokens": 0, "usage_complete": True, "finish_reasons": [],
                       "protocol_constrained_json": True}
        for index, record in enumerate(self.rows):
            if index in self.accounted:
                self.calls.append({**record, "journal_index": index})
                self._add_usage(record)
        self.next_index = len(self.rows)

    @property
    def seed(self) -> int:
        return self.base.seed

    @seed.setter
    def seed(self, value: int) -> None:
        self.base.seed = int(value)

    def set_context(self, context: Mapping[str, Any]) -> None:
        self.request_context = dict(context)
        self.base.request_context = dict(context)

    @property
    def usage(self) -> dict[str, Any]:
        return {**self._usage, "finish_reasons": list(self._usage["finish_reasons"])}

    def _add_usage(self, record: Mapping[str, Any]) -> None:
        status = record.get("status")
        if status in {"ok", "request_error"}:
            self._usage["total_requests"] += 1
        usage = record.get("usage") or {}
        for key in ("prompt_tokens", "completion_tokens"):
            self._usage[key] += int(usage.get(key, 0) or 0)
        self._usage["total_tokens"] = self._usage["prompt_tokens"] + self._usage["completion_tokens"]
        if record.get("usage_complete") is False or status == "request_error":
            self._usage["usage_complete"] = False
        reason = record.get("finish_reason")
        if reason:
            self._usage["finish_reasons"].append(str(reason))

    def complete(self, prompt: str) -> str:
        cached = next((i for i, row in enumerate(self.rows)
                       if i not in self.used and row.get("prompt") == prompt
                       and self._context_matches(i, row)
                       and row.get("status") in {"ok", "request_error"}), None)
        if cached is not None:
            self.used.add(cached)
            record = {**self.rows[cached], "journal_index": cached, "replayed": True}
            self.calls.append(record)
            self._add_usage(record)
            if record.get("status") != "ok":
                raise RuntimeError(f"replayed prior endpoint failure ({record.get('error_type', 'unknown')})")
            return str(record.get("response", ""))
        if self.request_context is not None:
            self.base.request_context = dict(self.request_context)
        before = len(self.base.calls)
        try:
            response = self.base.complete(prompt)
        finally:
            new_records = self.base.calls[before:]
            for original in new_records:
                record = {**original, "journal_index": self.next_index}
                self.used.add(self.next_index)
                if self.request_context is not None:
                    self.call_owners[str(self.next_index)] = dict(self.request_context)
                self.next_index += 1
                self.rows.append(dict(original))
                self.calls.append(record)
                self._add_usage(record)
        return response

    def _context_matches(self, index: int, record: Mapping[str, Any]) -> bool:
        stored = record.get("request_context") or self.call_owners.get(str(index))
        if self.request_context is None:
            return stored is None
        if not isinstance(stored, Mapping):
            return False
        return all(stored.get(key) == value for key, value in self.request_context.items())


def _client(endpoint_name: str, base_url: str, run_root: Path, state: Mapping[str, Any],
            *, seed: int, profile: Mapping[str, Any]) -> _JournalClient:
    journal_path = run_root / "model_calls" / f"{endpoint_name}.jsonl"
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    raw = LocalLlamaClient(
        base_url=base_url,
        model_name=str(profile["model_name"]),
        seed=seed,
        temperature=1.0,
        top_p=0.95,
        top_k=64,
        max_tokens=8192,
        timeout=float(profile["request_timeout_seconds"]),
        context_tokens=32768,
        reasoning_mode=str(profile.get("reasoning_mode", "on")),
        reasoning_budget=2048,
        preflight=True,
        response_format={"type": "json_object"},
        call_journal_path=journal_path,
    )
    return _JournalClient(
        raw, endpoint_name, journal_path,
        _accounted_calls(state, endpoint_name),
        state.get("call_owners", {}).get(endpoint_name, {}),
    )


def _accounted_calls(state: Mapping[str, Any], endpoint: str) -> set[int]:
    return {int(value) for value in state.get("accounted_calls", {}).get(endpoint, [])}


def _mark_calls(state: dict[str, Any], endpoint: str, calls: list[Mapping[str, Any]], owner: Mapping[str, Any]) -> None:
    with _STATE_LOCK:
        known = set(_accounted_calls(state, endpoint))
        owners = state.setdefault("call_owners", {}).setdefault(endpoint, {})
        for record in calls:
            index = record.get("journal_index")
            if isinstance(index, int):
                known.add(index)
                # Journal indices represent physical calls. A replay may be
                # attributed to a later chain, but must never replace the
                # first request context that paid for the stored response.
                owners.setdefault(str(index), dict(owner))
        state.setdefault("accounted_calls", {})[endpoint] = sorted(known)


def _save_state(run_root: Path, state: dict[str, Any]) -> None:
    with _STATE_LOCK:
        _atomic_json(run_root / "state.json", state)


def _owned_complete(client: _JournalClient, prompt: str, *, owner: Mapping[str, Any],
                    state: dict[str, Any], run_root: Path) -> tuple[str, list[dict[str, Any]]]:
    client.set_context(owner)
    before = len(client.calls)
    try:
        response = client.complete(prompt)
    except Exception:
        new_calls = client.calls[before:]
        _mark_calls(state, client.endpoint, new_calls, owner)
        _save_state(run_root, state)
        raise
    new_calls = client.calls[before:]
    _mark_calls(state, client.endpoint, new_calls, owner)
    _save_state(run_root, state)
    return response, new_calls


def _error_document(exc: Exception) -> dict[str, Any]:
    if hasattr(exc, "as_dict"):
        try:
            return dict(exc.as_dict())
        except Exception:
            pass
    return {"stage": getattr(exc, "stage", "validation"),
            "message": str(exc)[:400],
            "details": getattr(exc, "details", {})}


def _repair_prompt(base_prompt: str, response: str, errors: Any, attempt: int) -> str:
    return (
        base_prompt
        + f"\n\n## Deterministic repair attempt {attempt}\n"
        + "The prior structured response failed deterministic validation or the public-data gate.\n"
        + "Fix only the reported problems. Keep source and registration/DAG fields together; return the complete strict JSON object.\n"
        + "Do not guess missing metadata and do not add narrative/report fields.\n\n"
        + "## Exact validation errors\n"
        + json.dumps(errors, ensure_ascii=False, separators=(",", ":"))
        + "\n\n## Previous response\n"
        + response
    )


def _validate_bank_adapter(response: str, cases_dir: Path, development_ids: list[str]) -> tuple[Any | None, list[Any]]:
    report = validate_bank_adapter_submission(response, cases_dir, development_ids)
    if report.get("status") == "conforming":
        return report, []
    if report.get("status") == "needs_input":
        return None, [{"stage": "needs_input", "required_fields": report.get("required_fields", [])}]
    return None, report.get("errors") or [{"stage": report.get("stage", "adapter"), "message": "adapter validation failed"}]


def _attempt_stage(
    *, stage: str, base_prompt: str, attempt_dir: Path, client: _JournalClient,
    state: dict[str, Any], run_root: Path, deadline: float | None,
    validator: Callable[[str], tuple[Any | None, list[Any]]],
    owner: Mapping[str, Any], external_response: str | None = None,
    preloaded_attempts: Mapping[int, Mapping[str, Any]] | None = None,
    external_attempts_used: int = 0,
) -> tuple[Any | None, str | None, dict[str, Any]]:
    previous_response = ""
    previous_errors: list[Any] = []
    preloaded_attempts = dict(preloaded_attempts or {})
    attempt_dir.mkdir(parents=True, exist_ok=True)
    for attempt in range(3):
        record_path = attempt_dir / f"attempt_{attempt:02d}.json"
        prompt = base_prompt if attempt == 0 else _repair_prompt(base_prompt, previous_response, previous_errors, attempt)
        if record_path.is_file():
            record = json.loads(record_path.read_text(encoding="utf-8"))
            if record.get("origin") != "external_preseed" and record.get("prompt_sha256") != _sha(prompt.encode("utf-8")):
                return None, None, {"status": "failed", "stage": stage, "errors": ["resumed prompt hash differs from stored attempt"]}
            previous_response = str(record.get("response", ""))
        elif attempt in preloaded_attempts:
            external = preloaded_attempts[attempt]
            previous_response = str(external["response"])
            record = {
                "attempt": attempt,
                "prompt_sha256": external.get("prompt_sha256", _sha(prompt.encode("utf-8"))),
                "response": previous_response,
                "journal_indices": [],
                "endpoint": owner.get("endpoint"),
                "stage": stage,
                "origin": "external_preseed",
                "external_name": external.get("name"),
                "created_utc": _utc(),
            }
            _atomic_json(record_path, record)
        elif attempt == 0 and external_response is not None:
            previous_response = external_response
            record = {
                "attempt": attempt,
                "prompt_sha256": _sha(prompt.encode("utf-8")),
                "response": previous_response,
                "journal_indices": [],
                "endpoint": owner.get("endpoint"),
                "stage": stage,
                "origin": "external_preseed",
                "created_utc": _utc(),
            }
            _atomic_json(record_path, record)
        elif attempt < external_attempts_used:
            errors = [f"external adapter call {attempt + 1} was consumed but its response artifact is unavailable"]
            return None, None, {"status": "failed", "stage": stage,
                                "attempts_used": external_attempts_used, "errors": errors}
        else:
            if _deadline_hit(deadline):
                return None, None, {"status": "not_started", "stage": stage, "errors": ["deadline reached before this stage call"]}
            try:
                previous_response, call_records = _owned_complete(
                    client, prompt, owner={**owner, "stage": stage, "attempt": attempt}, state=state, run_root=run_root
                )
            except Exception as exc:
                prior_call = client.calls[-1] if client.calls and client.calls[-1].get("prompt") == prompt else None
                record = {
                    "attempt": attempt,
                    "prompt_sha256": _sha(prompt.encode("utf-8")),
                    "response": "",
                    "journal_indices": [prior_call["journal_index"]] if prior_call and isinstance(prior_call.get("journal_index"), int) else [],
                    "endpoint": owner.get("endpoint"),
                    "stage": stage,
                    "call_error": _error_document(exc),
                    "created_utc": _utc(),
                }
                _atomic_json(record_path, record)
                return None, None, {"status": "failed", "stage": stage, "errors": [_error_document(exc)]}
            record = {
                "attempt": attempt,
                "prompt_sha256": _sha(prompt.encode("utf-8")),
                "response": previous_response,
                "journal_indices": [row.get("journal_index") for row in call_records if row.get("journal_index") is not None],
                "endpoint": owner.get("endpoint"),
                "stage": stage,
                "created_utc": _utc(),
            }
            _atomic_json(record_path, record)
        value, errors = validator(previous_response)
        if not errors:
            result = {
                "status": "conforming",
                "stage": stage,
                "attempts_used": attempt + 1,
                "response_sha256": _sha(previous_response.encode("utf-8")),
            }
            _atomic_json(attempt_dir / "stage_result.json", result)
            return value, previous_response, result
        previous_errors = errors
        record["validation_errors"] = errors
        record["validated_utc"] = _utc()
        _atomic_json(record_path, record)
        if attempt < 2:
            _save_state(run_root, state)
    result = {"status": "failed", "stage": stage, "attempts_used": 3, "errors": previous_errors}
    _atomic_json(attempt_dir / "stage_result.json", result)
    return None, previous_response, result


def _input_fingerprint(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(file for file in root.rglob("*") if file.is_file()):
        rel = path.relative_to(root).as_posix()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        rows.append({"path": rel, "size_bytes": path.stat().st_size, "sha256": digest.hexdigest()})
    return rows


def _development_source_manifest(cases_dir: Path, development_ids: list[str]) -> dict[str, Any]:
    cases = []
    for case_id in development_ids:
        case_path = (cases_dir / case_id).resolve()
        alert_path = case_path / "alert.json"
        alert = json.loads(alert_path.read_text(encoding="utf-8"))
        files = [alert_path]
        for date in alert.get("telemetry_dates", []):
            root = case_path / "source_dates" / str(date)
            if root.is_dir():
                files.extend(sorted(root.glob("*/*.csv")))
        items = []
        for path in sorted(set(files)):
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            items.append({"path": path.relative_to(case_path).as_posix(),
                          "size_bytes": path.stat().st_size, "sha256": digest.hexdigest()})
        cases.append({"development_id": case_id, "files": items})
    return {"development_ids": list(development_ids), "cases": cases}


def _reuse_adapter_gate_receipt(response: Mapping[str, Any], report: Mapping[str, Any],
                                cases_dir: Path, development_ids: list[str]) -> bool:
    """Reuse a prior public-data gate only when source, config, and inputs still match."""
    if report.get("status") != "conforming":
        return False
    try:
        parsed = parse_adapter_submission(response)
        if parsed.get("status") != "ready":
            return False
        source_hash = hashlib.sha256(parsed["source"].encode("utf-8")).hexdigest()
        if source_hash != report.get("adapter_source_sha256"):
            return False
        if parsed.get("config_schema") != report.get("config_schema"):
            return False
        validate_adapter_config(parsed["config_schema"], {"case_dir": str((cases_dir / development_ids[0]).resolve())})
        if report.get("source_manifest") != _development_source_manifest(cases_dir, development_ids):
            return False
        case_reports = report.get("cases", ())
        if len(case_reports) != len(development_ids):
            return False
        if any(item.get("errors") or not item.get("windows_match") or not item.get("schema_match")
               or not item.get("suspect_entities_match") for item in case_reports):
            return False
        probes = report.get("timestamp_unit_probes", ())
        if len(probes) != 2 or any(item.get("passed") is not True for item in probes):
            return False
        return not report.get("errors")
    except Exception:
        return False


class _CachedAdapter:
    """Public cached adapter view backed by normalized JSONL partitions."""

    def __init__(self, directory: Path):
        from e4.data.record_store import TelemetryRecordStore
        from e4.onboarding import normalize_dataset_schema

        meta = json.loads((directory / "cache_manifest.json").read_text(encoding="utf-8"))
        self.case_id = meta["case_id"]
        self._schema = normalize_dataset_schema(meta["schema"])
        self._reference = Window(**meta["reference_window"])
        self._alert = Window(**meta["alert_window"])
        self._entities = [EntityRef(**item) for item in meta["suspect_entities"]]
        self._store = TelemetryRecordStore.from_descriptor(meta["record_store"])
        self._meta = meta

    def get_schema(self) -> Any:
        return self._schema

    def get_windows(self) -> tuple[Window, Window]:
        return self._reference, self._alert

    def extract_suspect_entities(self) -> list[EntityRef]:
        return list(self._entities)

    def stream_records(self, table_name: str, window: Window):
        yield from self._store.iter_records(table_name=table_name, window_name=window.name)

    def as_full_telemetry_view(self, case_id: str | None = None) -> TelemetryView:
        return TelemetryView(
            case_id=case_id or self.case_id,
            reference_window=self._reference,
            alert_window=self._alert,
            records=(),
            source_tables=[table.name for table in self._schema.tables],
            metadata={"truncated": False, "truncated_tables": [],
                      "record_counts": self._meta["counts_by_table_window"],
                      "record_limit_per_table_window": None},
            record_store=self._store,
        )


def _ensure_public_caches(run_root: Path, case_ids: list[str], adapter_response: Mapping[str, Any],
                          source_hash: str) -> dict[str, Path]:
    from e4.data.record_store import TelemetryRecordStore
    from e4.onboarding import normalize_dataset_schema

    cache_base = run_root / "public_cache" / source_hash
    cache_base.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for case_id in case_ids:
        case_dir = (CASES_DIR / case_id).resolve()
        cache_dir = cache_base / case_id
        fingerprint = _input_fingerprint(case_dir)
        meta_path = cache_dir / "cache_manifest.json"
        if meta_path.is_file():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if meta.get("input_fingerprint") != fingerprint or meta.get("adapter_source_sha256") != source_hash:
                raise ValueError(f"cached public input changed for {case_id}")
            paths[case_id] = cache_dir
            continue
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True)
        adapter = load_adapter_submission(
            adapter_response, bank_adapter_config(case_dir), check_conformance=False
        )
        schema = adapter.get_schema()
        if not schema.tables:
            raise ValueError(f"generated adapter declares no tables for public case {case_id}")
        ref, alert = adapter.get_windows()
        entities = adapter.extract_suspect_entities()
        if not entities or any(not isinstance(item, EntityRef) for item in entities):
            raise ValueError(f"generated adapter has no valid candidate identities for public case {case_id}")
        store = TelemetryRecordStore.from_adapter(adapter, directory=cache_dir / "records")
        normalized = normalize_dataset_schema(schema.as_dict())
        if normalized is None:
            raise ValueError("generated adapter returned no schema")
        counts = store.descriptor["counts_by_table_window"]
        meta = {
            "case_id": case_id,
            "adapter_source_sha256": source_hash,
            "input_fingerprint": fingerprint,
            "schema": normalized.as_dict(),
            "reference_window": ref.as_dict(),
            "alert_window": alert.as_dict(),
            "suspect_entities": [item.as_dict() for item in entities],
            "counts_by_table_window": counts,
            "record_store": store.descriptor,
        }
        _atomic_json(meta_path, meta)
        store.close()
        paths[case_id] = cache_dir
    return paths


def _gate_new_playbooks(candidate: LibraryManager, response: Mapping[str, Any],
                        adapter_response: Mapping[str, Any], development_ids: list[str],
                        cases_dir: Path) -> dict[str, Any]:
    from e4.sandbox.runner import SubprocessRunner

    playbooks = response.get("playbooks", [])
    if not playbooks:
        return {"status": "passed", "results": []}
    runner = SubprocessRunner(timeout_seconds=OPERATOR_TIMEOUT_SECONDS,
                              memory_limit_mb=OPERATOR_MEMORY_LIMIT_MB)
    results = []
    errors = []
    for development_id in development_ids:
        case_dir = (cases_dir / development_id).resolve()
        view = None
        try:
            adapter = load_adapter_submission(adapter_response, bank_adapter_config(case_dir), check_conformance=False)
            view = adapter.as_full_telemetry_view(case_id=development_id)
            entities = adapter.extract_suspect_entities()
            schema = adapter.get_schema()
        except Exception as exc:
            errors.append({"development_id": development_id, "stage": "adapter", "error": _error_document(exc)})
            continue
        try:
            for book_doc in playbooks:
                book_name = book_doc["name"]
                try:
                    book = next((item for item in candidate.list_playbooks() if item.name == book_name), None)
                    if book is None:
                        raise ValueError(f"submitted playbook not found after apply: {book_name}")
                    verified = verify_dag(book.dag, candidate.registry, dataset_schema=schema)
                    executed = SerialExecutor(candidate.registry).execute_verified(
                        verified,
                        runtime={"dag_depth": verified.max_depth},
                        external_values={"data": view, "telemetry": view},
                        runner=runner,
                    )
                    value = executed.return_value
                    ranked = _to_ranked_answer(value, "onboarding_gate", entities)
                    abstained = isinstance(value, ScoredCandidates) and value.status == "abstained" and not value.candidates
                    if ranked is None and not abstained:
                        raise ValueError("returned no supported ranked answer or explicit empty abstention")
                    results.append({
                        "development_id": development_id,
                        "playbook": book.name,
                        "status": "abstained" if abstained else "ranked",
                        "return_type": type(value).__name__,
                        "elapsed_ms": executed.elapsed_ms,
                    })
                except Exception as exc:
                    errors.append({"development_id": development_id, "playbook": book_name,
                                   "stage": "playbook", "error": _error_document(exc)})
        finally:
            store = getattr(view, "_record_store", None)
            if store is not None:
                store.close()
    return {"status": "failed" if errors else "passed", "results": results, "errors": errors}


def _source_artifacts(library: LibraryManager, source_dir: Path) -> None:
    source_dir.mkdir(parents=True, exist_ok=True)
    for operator in library.registry.all_operators():
        source = getattr(operator, "source", None)
        if not source:
            continue
        name = re.sub(r"[^A-Za-z0-9_.-]", "_", operator.name)
        _atomic_text(source_dir / f"{name}@{operator.version}.py", source)


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _write_markdown_report(result: Mapping[str, Any], path: Path) -> None:
    lines = ["# Bank onboarding and evaluation", "", f"Status: **{result.get('status')}**", "",
             "No model reasoning is included. Token totals use provider-reported prompt/completion counts; incomplete usage is marked.", ""]
    for mode, summary in result.get("modes", {}).items():
        lines.extend([f"## {mode}", "", f"Status: **{summary.get('status')}**"])
        if summary.get("accuracy") is not None:
            lines.append(f"Accuracy: {summary['confirmed']}/{summary['planned']} ({summary['accuracy']:.3f})")
        lines.append("")
        for seed, item in summary.get("seeds", {}).items():
            lines.append(f"- Seed {seed}: {item.get('confirmed', 0)}/{item.get('planned', 0)} confirmed; status={item.get('status')}")
        lines.append("")
    costs = result.get("token_costs", {})
    lines.extend(["## Token accounting", "", "```json", json.dumps(costs, indent=2, ensure_ascii=False), "```", ""])
    _atomic_text(path, "\n".join(lines))


def _sum_usage(records: list[Mapping[str, Any]]) -> dict[str, Any]:
    result = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0,
              "usage_complete": True}
    for call in records:
        usage = call.get("usage") or {}
        if call.get("status") in {"ok", "request_error"}:
            result["calls"] += 1
        result["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        result["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        if call.get("status") == "request_error" or call.get("usage_complete") is False:
            result["usage_complete"] = False
    result["total_tokens"] = result["prompt_tokens"] + result["completion_tokens"]
    return result


def _imported_adapter_attempts(
    *,
    response_file: Path | None,
    attempt_dir: Path | None,
    journal_file: Path | None,
    declared_attempts: int | None,
) -> tuple[dict[int, dict[str, Any]], int, list[dict[str, Any]]]:
    journal_rows = _jsonl(journal_file) if journal_file is not None else []
    actual_calls = sum(row.get("status") in {"ok", "request_error"} for row in journal_rows)
    attempts_used = int(declared_attempts) if declared_attempts is not None else actual_calls
    preloaded: dict[int, dict[str, Any]] = {}
    names = {"initial_response.txt": 0}
    if attempt_dir is not None and attempt_dir.is_dir():
        for path in sorted(attempt_dir.glob("repair_*_response.txt")):
            match = re.fullmatch(r"repair_(\d+)_response\.txt", path.name)
            if match:
                # attempt index zero is the initial generation call.
                names[path.name] = int(match.group(1))
        for name, index in names.items():
            path = attempt_dir / name
            if path.is_file():
                preloaded[index] = {"name": name, "response": path.read_text(encoding="utf-8")}
    if not preloaded and journal_rows and attempts_used:
        calls = [row for row in journal_rows if row.get("status") in {"ok", "request_error"}]
        for index, call in enumerate(calls[:attempts_used]):
            if call.get("status") == "ok" and isinstance(call.get("response"), str):
                preloaded[index] = {
                    "name": f"external_journal_call_{index + 1}",
                    "response": call["response"],
                    "prompt_sha256": _sha(str(call.get("prompt", "")).encode("utf-8")),
                }
    if response_file is not None:
        response_path = Path(response_file)
        if not response_path.is_file():
            raise FileNotFoundError(f"Adapter response file is missing: {response_path}")
        response_text = response_path.read_text(encoding="utf-8")
        found = False
        if attempt_dir is not None and response_path.parent.resolve() == Path(attempt_dir).resolve():
            for index, name in ((0, "initial_response.txt"), *[(i, f"repair_{i:02d}_response.txt") for i in range(1, 3)]):
                candidate = Path(attempt_dir) / name
                if candidate.is_file() and candidate.resolve() == response_path.resolve():
                    preloaded[index] = {"name": name, "response": response_text}
                    found = True
        if not found:
            index = max(0, attempts_used - 1) if attempts_used else 0
            preloaded[index] = {"name": response_path.name, "response": response_text}
            if attempts_used == 0:
                attempts_used = 1
    if declared_attempts is None and actual_calls == 0 and preloaded:
        attempts_used = max(preloaded) + 1
    for item in preloaded.values():
        if "prompt_sha256" in item:
            continue
        match = next((row for row in journal_rows
                      if row.get("status") == "ok" and row.get("response") == item.get("response")), None)
        if match and isinstance(match.get("prompt"), str):
            item["prompt_sha256"] = _sha(match["prompt"].encode("utf-8"))
    if attempts_used > 3:
        raise ValueError("adapter onboarding may use only one generation call and two repairs")
    if any(index < 0 or index >= 3 for index in preloaded):
        raise ValueError("imported adapter response attempt index is outside the fixed three-call budget")
    if len(journal_rows) and actual_calls == 0 and not preloaded:
        raise ValueError("external adapter journal contains no completed/attempted generation calls")
    return preloaded, attempts_used, journal_rows


def _finish_failure(run_root: Path, state: dict[str, Any], profile: Mapping[str, Any], stage: str,
                    details: Any, external_adapter_usage: Any = None) -> dict[str, Any]:
    state.update({"status": "failed", "failed_stage": stage, "failure": details, "finished_utc": _utc()})
    _atomic_json(run_root / "state.json", state)
    result = _build_campaign_result(run_root, state, external_adapter_usage)
    result.update({"status": "failed", "failed_stage": stage, "failure": details, "profile": dict(profile)})
    _write_final(run_root, result)
    return result


def _write_skipped_chain(run_root: Path, state: dict[str, Any], mode: str, seed: int,
                         shard: int, scored_ids: list[str], reason: str) -> None:
    chain_root = run_root / "modes" / mode / "chains" / f"seed{seed}" / f"shard{shard}"
    path = chain_root / "result.json"
    if path.is_file():
        return
    ids = scored_ids[shard * 5:(shard + 1) * 5]
    rows = [{"case_id": cid, "status": "not_started", "stage": None,
             "confirmed": False, "error_category": "not_started", "error": reason,
             "total_calls": 0, "call_metadata": [], "usage_delta": {}}
            for cid in ids]
    result = {"status": "not_started", "mode": mode, "seed": seed, "shard": shard,
              "planned_case_ids": ids, "total_cases": len(ids), "hits": 0,
              "accuracy": None, "results": rows, "reason": reason, "finished_utc": _utc()}
    _atomic_json(path, result)
    _atomic_json(chain_root / "checkpoint.json", result)
    with _STATE_LOCK:
        state.setdefault("chains", {})[f"{mode}:{seed}:{shard}"] = {"status": "not_started", "reason": reason}
    _atomic_json(run_root / "state.json", state)


def _write_skipped_chains(run_root: Path, state: dict[str, Any], mode: str, seed: int,
                          scored_ids: list[str], reason: str) -> None:
    for shard in SHARDS:
        _write_skipped_chain(run_root, state, mode, seed, shard, scored_ids, reason)


def _build_campaign_result(run_root: Path, state: Mapping[str, Any], external_adapter_usage: Any) -> dict[str, Any]:
    mode_results: dict[str, Any] = {}
    selected_modes = tuple(state.get("profile", {}).get("modes", MODES))
    for mode in selected_modes:
        mode_state = state.get("modes", {}).get(mode, {})
        chain_results = []
        for seed in SEEDS:
            for shard in SHARDS:
                path = run_root / "modes" / mode / "chains" / f"seed{seed}" / f"shard{shard}" / "result.json"
                if path.is_file():
                    chain_results.append(json.loads(path.read_text(encoding="utf-8")))
        rows = [row for chain in chain_results for row in chain.get("results", [])]
        planned = len(SEEDS) * len(SHARDS) * 5
        completed = sum(row.get("status") not in {"not_started"} for row in rows)
        confirmed = sum(row.get("status") == "confirmed" for row in rows)
        failed_slots = sum(row.get("status") in {"error", "not_started"} for row in rows)
        mode_status = mode_state.get("status", "not_started")
        if mode_status == "frozen":
            mode_status = "complete" if len(rows) == planned and not failed_slots else "incomplete"
        seed_summaries: dict[str, Any] = {}
        for seed in SEEDS:
            seed_chains = [chain for chain in chain_results if chain.get("seed") == seed]
            seed_rows = [row for chain in seed_chains for row in chain.get("results", [])]
            seed_confirmed = sum(row.get("status") == "confirmed" for row in seed_rows)
            seed_planned = 10
            seed_summaries[str(seed)] = {
                "status": "complete" if len(seed_rows) == 10 and not any(row.get("status") in {"error", "not_started"} for row in seed_rows) else "incomplete",
                "confirmed": seed_confirmed,
                "planned": seed_planned,
                "accuracy": seed_confirmed / seed_planned
                if any(row.get("status") != "not_started" for row in seed_rows)
                and mode_state.get("status") == "frozen" else None,
                "stage_prefix_confirmed": {
                    "select": sum(row.get("status") == "confirmed" and row.get("stage") == "select" for row in seed_rows),
                    "retrieval_plus_adaptation": sum(row.get("status") == "confirmed" and row.get("stage") in {"select", "compose"} for row in seed_rows),
                    "full": seed_confirmed,
                },
                "conditional_rescue": {
                    "adaptation": sum(row.get("status") == "confirmed" and row.get("stage") == "compose" for row in seed_rows),
                    "evolution_and_serving": sum(row.get("status") == "confirmed" and row.get("stage") == "serve" for row in seed_rows),
                },
            }
        entry = {
            "status": mode_status,
            "onboarding_status": mode_state.get("status", "not_started"),
            "planned": planned,
            "completed": completed,
            "confirmed": confirmed,
            "failed_slots": failed_slots,
            "seeds": seed_summaries,
            "chains": [{"seed": chain.get("seed"), "shard": chain.get("shard"),
                        "status": chain.get("status"), "hits": chain.get("hits"),
                        "total_cases": chain.get("total_cases"), "accuracy": chain.get("accuracy")}
                       for chain in chain_results],
        }
        # A failed or unstarted library receives no diagnosis score.
        if mode_state.get("status") == "frozen" and completed:
            entry["accuracy"] = confirmed / planned if planned else 0.0
        else:
            entry["accuracy"] = None
        mode_results[mode] = entry

    mode_values = list(mode_results.values())
    status = "complete" if mode_values and all(item["status"] == "complete" for item in mode_values) else "incomplete"
    if all(item.get("onboarding_status") in {"failed", "not_started"} for item in mode_values):
        status = "failed"
    costs = _token_costs(state, run_root)
    if external_adapter_usage is None:
        external_path = run_root / "external_adapter_calls.jsonl"
        external_rows = _jsonl(external_path)
        external_adapter_usage = _sum_usage(external_rows) if external_rows else None
    return {
        "status": status,
        "profile": state.get("profile", {}),
        "adapter": state.get("adapter", {}),
        "modes": mode_results,
        "token_costs": costs,
        "external_adapter_usage": external_adapter_usage,
        "planned": {"adapter_generation_calls": 1, "adapter_repairs": 2,
                    "library_generation_calls_per_mode": 1, "library_repairs_per_mode": 2,
                    "inference_cases_per_mode": planned if mode_results else 0},
        "finished_utc": _utc() if status in {"complete", "failed"} else None,
    }


def _write_final(run_root: Path, result: Mapping[str, Any]) -> None:
    finalized = dict(result)
    finalized.setdefault("finished_utc", _utc())
    _atomic_json(run_root / "result.json", finalized)
    _write_markdown_report(finalized, run_root / "report.md")
    state_path = run_root / "state.json"
    if state_path.is_file():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["status"] = finalized.get("status", "complete")
        state["finished_utc"] = finalized["finished_utc"]
        state["result_sha256"] = _sha((run_root / "result.json").read_bytes())
        _atomic_json(state_path, state)
    _atomic_json(run_root / "manifest.json", {
        "schema_version": "e4-bank-onboarding-run/1",
        "status": finalized.get("status"),
        "profile": finalized.get("profile", {}),
        "finished_utc": finalized.get("finished_utc"),
        "result_sha256": _sha((run_root / "result.json").read_bytes()),
    })


def _load_existing_state(run_root: Path, resume: bool) -> dict[str, Any]:
    state_path = run_root / "state.json"
    if state_path.is_file():
        if not resume:
            raise FileExistsError(f"Run root already has state; pass --resume: {run_root}")
        return json.loads(state_path.read_text(encoding="utf-8"))
    if run_root.exists() and any(run_root.iterdir()) and not resume:
        raise FileExistsError(f"Run root is not empty; choose a fresh path or pass --resume: {run_root}")
    run_root.mkdir(parents=True, exist_ok=True)
    return {
        "schema_version": "e4-bank-onboarding-run/1",
        "status": "running",
        "created_utc": _utc(),
        "modes": {},
        "chains": {},
        "accounted_calls": {"paper": [], "clean_slate": []},
        "call_owners": {"paper": {}, "clean_slate": {}},
        "events": [],
    }


def _recover_accounted_calls(run_root: Path, state: dict[str, Any]) -> None:
    for endpoint in ("paper", "clean_slate"):
        known = set(int(value) for value in state.get("accounted_calls", {}).get(endpoint, []))
        owners = state.setdefault("call_owners", {}).setdefault(endpoint, {})
        journal_path = run_root / "model_calls" / f"{endpoint}.jsonl"
        for index, call in enumerate(_jsonl(journal_path)):
            context = call.get("request_context")
            if isinstance(context, Mapping) and context:
                owners.setdefault(str(index), dict(context))
        for response_path in [
            run_root / "adapter" / "attempts",
            run_root / "modes" / "paper" / "attempts",
            run_root / "modes" / "clean_slate" / "attempts",
        ]:
            if not response_path.is_dir():
                continue
            for attempt_file in response_path.glob("attempt_*.json"):
                doc = json.loads(attempt_file.read_text(encoding="utf-8"))
                # The default adapter endpoint is clean_slate unless metadata says otherwise.
                owner_endpoint = doc.get("endpoint", "clean_slate" if response_path.parts[-2] == "adapter" else response_path.parts[-2])
                if owner_endpoint != endpoint:
                    continue
                for index in doc.get("journal_indices", []):
                    if isinstance(index, int):
                        known.add(index)
                        owners.setdefault(str(index), {"stage": doc.get("stage", "resumed_artifact")})
        for checkpoint in (run_root / "modes").glob("*/chains/seed*/shard*/checkpoint.json"):
            doc = json.loads(checkpoint.read_text(encoding="utf-8"))
            chain_endpoint = doc.get("endpoint")
            if chain_endpoint != endpoint:
                continue
            for row in doc.get("results", []):
                for call in row.get("call_metadata", []):
                    index = call.get("journal_index")
                    if isinstance(index, int):
                        known.add(index)
                        owners.setdefault(str(index), {"stage": "inference", "case_id": row.get("case_id")})
        state["accounted_calls"][endpoint] = sorted(known)


def _token_costs(state: Mapping[str, Any], run_root: Path) -> dict[str, Any]:
    totals = {endpoint: {"total_requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
                         "total_tokens": 0, "usage_complete": True}
              for endpoint in ("paper", "clean_slate")}
    by_stage: dict[str, dict[str, int]] = {}
    by_mode_seed: dict[str, dict[str, int]] = {}
    for endpoint in totals:
        path = run_root / "model_calls" / f"{endpoint}.jsonl"
        rows = _jsonl(path)
        owners = state.get("call_owners", {}).get(endpoint, {})
        for index, call in enumerate(rows):
            usage = call.get("usage") or {}
            owner = owners.get(str(index), call.get("request_context", {}))
            _accumulate_usage(totals[endpoint], usage, call)
            stage = str(owner.get("stage", "unattributed"))
            stage_bucket = by_stage.setdefault(stage, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "calls": 0})
            stage_bucket["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
            stage_bucket["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
            stage_bucket["total_tokens"] += int(usage.get("total_tokens", 0) or (int(usage.get("prompt_tokens", 0) or 0) + int(usage.get("completion_tokens", 0) or 0)))
            stage_bucket["calls"] += 1
            if owner.get("seed") is not None:
                key = f"{owner.get('mode')}:{owner.get('seed')}"
                bucket = by_mode_seed.setdefault(key, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "calls": 0})
                for field in ("prompt_tokens", "completion_tokens"):
                    bucket[field] += int(usage.get(field, 0) or 0)
                bucket["total_tokens"] += int(usage.get("total_tokens", 0) or (int(usage.get("prompt_tokens", 0) or 0) + int(usage.get("completion_tokens", 0) or 0)))
                bucket["calls"] += 1
    return {"by_endpoint": totals, "by_stage": by_stage, "by_mode_seed": by_mode_seed}


def _accumulate_usage(total: dict[str, Any], usage: Mapping[str, Any], call: Mapping[str, Any]) -> None:
    if call.get("status") in {"ok", "request_error"}:
        total["total_requests"] += 1
    prompt = int(usage.get("prompt_tokens", 0) or 0)
    completion = int(usage.get("completion_tokens", 0) or 0)
    total["prompt_tokens"] += prompt
    total["completion_tokens"] += completion
    total["total_tokens"] = total["prompt_tokens"] + total["completion_tokens"]
    if call.get("status") == "request_error" or call.get("usage_complete") is False:
        total["usage_complete"] = False


def _prepare_mode(
    mode: str,
    *,
    run_root: Path,
    state: dict[str, Any],
    schema: Any,
    schema_doc: Mapping[str, Any],
    adapter_response: Mapping[str, Any],
    development_ids: list[str],
    clients: Mapping[str, _JournalClient],
    urls: Mapping[str, str],
    deadline: float | None,
) -> dict[str, Any]:
    mode_root = run_root / "modes" / mode
    with _STATE_LOCK:
        mode_state = state.setdefault("modes", {}).setdefault(mode, {})
    frozen_path = mode_root / "frozen_library" / "library.json"
    if frozen_path.is_file() and mode_state.get("status") == "frozen":
        return mode_state
    if _deadline_hit(deadline):
        with _STATE_LOCK:
            mode_state.update({"status": "not_started", "error": "deadline reached before library generation"})
        _save_state(run_root, state)
        return mode_state

    prompt = render_initialization_prompt(
        domain_brief="OpenRCA Bank component top-three diagnosis. Use only supported schema and identity facts.",
        include_bank=True,
        clean_slate=(mode == "clean_slate"),
        schema=schema_doc,
    )
    mode_root.mkdir(parents=True, exist_ok=True)
    _atomic_text(mode_root / "prompt.md", prompt)

    def validate_library(raw: str) -> tuple[Any | None, list[Any]]:
        try:
            submission = parse_library_submission(raw)
            candidate = apply_library_submission(raw, prepare_library(schema, mode=mode), schema)
            gate = _gate_new_playbooks(candidate, submission, adapter_response, development_ids, CASES_DIR)
            if gate.get("status") != "passed":
                return None, [{"stage": "development_gate", "errors": gate.get("errors", [])}]
            return {"library": candidate, "submission": submission, "gate": gate}, []
        except Exception as exc:
            return None, [_error_document(exc)]

    endpoint = "paper" if mode == "paper" else "clean_slate"
    client = clients[endpoint]
    client.seed = 42
    value, _, stage = _attempt_stage(
        stage=f"library_{mode}",
        base_prompt=prompt,
        attempt_dir=mode_root / "attempts",
        client=client,
        state=state,
        run_root=run_root,
        deadline=deadline,
        validator=validate_library,
        owner={"phase": "onboarding", "mode": mode, "seed": 42, "endpoint": endpoint},
    )
    if value is None:
        with _STATE_LOCK:
            mode_state.update({"status": stage["status"], "generation": stage})
        _atomic_json(mode_root / "mode_result.json", {
            "status": "failed" if stage["status"] == "failed" else stage["status"],
            "mode": mode,
            "stage": "library",
            "errors": stage.get("errors", []),
        })
        _save_state(run_root, state)
        return mode_state

    library = value["library"]
    submission = value["submission"]
    gate = value["gate"]
    library.save_to_dir(mode_root / "frozen_library")
    _source_artifacts(library, mode_root / "operator_sources")
    _atomic_json(mode_root / "submission.json", submission)
    _atomic_json(mode_root / "gate.json", gate)
    with _STATE_LOCK:
        mode_state.update({"status": "frozen", "generation": stage,
                           "library_manifest": library.manifest, "gate": gate})
    _atomic_json(mode_root / "mode_result.json", {
        "status": "frozen", "mode": mode,
        "library_manifest": library.manifest,
        "gate": gate,
        "source_sha256": state.get("adapter", {}).get("source_sha256"),
    })
    _save_state(run_root, state)
    return mode_state


def _run_mode_seed(
    mode: str,
    seed: int,
    *,
    run_root: Path,
    state: dict[str, Any],
    schema: Any,
    scored_ids: list[str],
    caches: Mapping[str, Path],
    clients: Mapping[str, _JournalClient],
    urls: Mapping[str, str],
    deadline: float | None,
) -> None:
    mode_root = run_root / "modes" / mode
    mode_state = state.get("modes", {}).get(mode, {})
    endpoint = "paper" if mode == "paper" else "clean_slate"
    if mode_state.get("status") != "frozen":
        for shard in SHARDS:
            _write_skipped_chain(run_root, state, mode, seed, shard, scored_ids,
                                 "onboarding library did not freeze")
        return

    client = clients[endpoint]
    client.seed = seed
    base = prepare_library(schema, mode=mode)
    for shard in SHARDS:
        if _deadline_hit(deadline):
            _write_skipped_chain(run_root, state, mode, seed, shard, scored_ids,
                                 "deadline reached before chain")
            continue
        chain_root = mode_root / "chains" / f"seed{seed}" / f"shard{shard}"
        final_path = chain_root / "result.json"
        if final_path.is_file():
            continue
        chain_root.mkdir(parents=True, exist_ok=True)
        checkpoint_path = chain_root / "checkpoint.json"
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8")) if checkpoint_path.is_file() else {}
        completed = [row for row in checkpoint.get("results", []) if row.get("status") != "not_started"]
        snapshots_dir = chain_root / "library_snapshots"
        snapshot_rel = checkpoint.get("library_snapshot")
        snapshot_path = chain_root / snapshot_rel if isinstance(snapshot_rel, str) else None
        if snapshot_path is not None and (snapshot_path / "library.json").is_file():
            chain_library = LibraryManager.load_from_dir(
                snapshot_path, base_registry=base.registry.clone(), mode=mode
            )
        else:
            chain_library = LibraryManager.load_from_dir(
                mode_root / "frozen_library", base_registry=base.registry.clone(), mode=mode
            )
        case_ids = scored_ids[shard * 5:(shard + 1) * 5]
        checkpoint_doc = {
            "status": "running", "mode": mode, "seed": seed, "shard": shard,
            "endpoint": endpoint, "planned_case_ids": case_ids,
            "completed_case_ids": [row["case_id"] for row in completed],
            "results": completed, "library_snapshot": snapshot_rel,
            "updated_utc": _utc(),
        }
        _atomic_json(checkpoint_path, checkpoint_doc)

        def on_result(row: dict[str, Any]) -> None:
            existing = {item.get("case_id"): item for item in checkpoint_doc["results"]}
            existing[row.get("case_id")] = row
            checkpoint_doc["results"] = [existing[key] for key in case_ids if key in existing]
            checkpoint_doc["completed_case_ids"] = [
                item["case_id"] for item in checkpoint_doc["results"]
                if item.get("status") != "not_started"
            ]
            snapshot_path = snapshots_dir / row["case_id"]
            if snapshot_path.exists():
                shutil.rmtree(snapshot_path)
            chain_library.save_to_dir(snapshot_path)
            checkpoint_doc["library_snapshot"] = str(snapshot_path.relative_to(chain_root))
            checkpoint_doc["updated_utc"] = _utc()
            # The pointer is advanced only after a complete immutable snapshot exists.
            _atomic_json(checkpoint_path, checkpoint_doc)
            indices = [call.get("journal_index") for call in row.get("call_metadata", [])
                       if isinstance(call.get("journal_index"), int)]
            _mark_calls(state, endpoint, [{"journal_index": index} for index in indices], {
                "phase": "inference", "stage": "inference", "mode": mode, "seed": seed,
                "shard": shard, "case_id": row.get("case_id"), "endpoint": endpoint,
            })
            _save_state(run_root, state)

        benchmark_profile = {
            **state["profile"], "mode": mode, "seed": seed, "shard": shard,
            "case_ids": case_ids, "model_endpoint": urls[endpoint],
            "operator_timeout_seconds": OPERATOR_TIMEOUT_SECONDS,
            "operator_memory_limit_mb": OPERATOR_MEMORY_LIMIT_MB,
        }

        def context_adapter_factory(path: Path) -> _CachedAdapter:
            client.set_context({
                "phase": "inference", "stage": "inference", "mode": mode,
                "seed": seed, "shard": shard, "case_id": path.name,
                "endpoint": endpoint,
            })
            return _CachedAdapter(caches[path.name])

        summary = run_benchmark(
            CASES_DIR, GROUND_TRUTH, client, case_ids=case_ids,
            max_attempts=3, profile=benchmark_profile, on_result=on_result,
            deadline_epoch=deadline,
            operator_timeout_seconds=OPERATOR_TIMEOUT_SECONDS,
            operator_memory_limit_mb=OPERATOR_MEMORY_LIMIT_MB,
            library=chain_library,
            adapter_factory=context_adapter_factory,
            resume_results=completed,
        )
        status = "complete" if all(row.get("status") not in {"error", "not_started"}
                                   for row in summary["results"]) else "incomplete"
        summary.update({"status": status, "mode": mode, "seed": seed, "shard": shard,
                        "planned_case_ids": case_ids, "finished_utc": _utc()})
        _atomic_json(final_path, summary)
        checkpoint_doc.update(summary)
        if summary["results"]:
            last_case = summary["results"][-1].get("case_id")
            last_snapshot = snapshots_dir / str(last_case)
            if (last_snapshot / "library.json").is_file():
                checkpoint_doc["library_snapshot"] = str(last_snapshot.relative_to(chain_root))
        _atomic_json(checkpoint_path, checkpoint_doc)
        with _STATE_LOCK:
            state.setdefault("chains", {})[f"{mode}:{seed}:{shard}"] = {
                "status": status, "hits": summary["hits"], "total_cases": summary["total_cases"]
            }
        _save_state(run_root, state)


def run_campaign(
    run_root: Path,
    *,
    modes: tuple[str, ...] | list[str] = MODES,
    paper_url: str = "http://127.0.0.1:19250",
    clean_slate_url: str = "http://127.0.0.1:19414",
    adapter_endpoint: str | None = None,
    adapter_response_file: Path | None = None,
    adapter_prompt_file: Path | None = None,
    adapter_attempt_dir: Path | None = None,
    adapter_attempts_used: int | None = None,
    adapter_call_journal_file: Path | None = None,
    adapter_gate_file: Path | None = None,
    deadline_utc: str | None = None,
    resume: bool = False,
    prepare_only: bool = False,
) -> dict[str, Any]:
    run_root = Path(run_root).resolve()
    state = _load_existing_state(run_root, resume)
    scored_ids, development_ids, _ = _load_plan()
    selected_modes = tuple(modes)
    if not selected_modes or len(selected_modes) != len(set(selected_modes)) or any(mode not in MODES for mode in selected_modes):
        raise ValueError("modes must be a nonempty unique subset of paper and clean_slate")
    if adapter_endpoint is None:
        adapter_endpoint = selected_modes[0] if len(selected_modes) == 1 else "clean_slate"
    deadline = _deadline_epoch(deadline_utc)
    config_doc = json.loads(INFERENCE_CONFIG.read_text(encoding="utf-8"))
    fixed = (float(config_doc["temperature"]), float(config_doc["top_p"]), int(config_doc["top_k"]),
             int(config_doc["context_tokens"]), int(config_doc["max_generation_tokens"]), int(config_doc["reasoning_budget"]))
    if fixed != (1.0, 0.95, 64, 32768, 8192, 2048):
        raise ValueError(f"Gemma profile differs from frozen settings: {fixed}")
    if adapter_endpoint not in {"paper", "clean_slate"}:
        raise ValueError("adapter_endpoint must be paper or clean_slate")
    urls = {"paper": paper_url, "clean_slate": clean_slate_url}
    profile = {
        "config_sha256": _sha(INFERENCE_CONFIG.read_bytes()),
        "model_name": config_doc["model_alias"],
        "temperature": 1.0, "top_p": 0.95, "top_k": 64,
        "context_tokens": 32768, "max_generation_tokens": 8192,
        "reasoning_budget": 2048, "reasoning_mode": str(config_doc.get("reasoning_mode", "on")),
        "response_format": {"type": "json_object"},
        "adapter_endpoint": adapter_endpoint, "endpoints": urls,
        "modes": list(selected_modes),
        "deadline_utc": deadline_utc,
        "development_ids": development_ids, "scored_ids": scored_ids,
        "seeds": list(SEEDS), "shards": list(SHARDS),
        "operator_timeout_seconds": OPERATOR_TIMEOUT_SECONDS,
        "operator_memory_limit_mb": OPERATOR_MEMORY_LIMIT_MB,
        "external_adapter_response_sha256": _sha(Path(adapter_response_file).read_bytes()) if adapter_response_file and Path(adapter_response_file).is_file() else None,
        "external_adapter_prompt_sha256": _sha(Path(adapter_prompt_file).read_bytes()) if adapter_prompt_file and Path(adapter_prompt_file).is_file() else None,
        "external_adapter_journal_sha256": _sha(Path(adapter_call_journal_file).read_bytes()) if adapter_call_journal_file and Path(adapter_call_journal_file).is_file() else None,
        "external_adapter_gate_sha256": _sha(Path(adapter_gate_file).read_bytes()) if adapter_gate_file and Path(adapter_gate_file).is_file() else None,
        "external_adapter_attempts_used": adapter_attempts_used,
    }
    old_profile = state.get("profile")
    if old_profile:
        for key, value in profile.items():
            if key != "deadline_utc" and old_profile.get(key) != value:
                raise ValueError(f"resume profile changed frozen field {key!r}")
        if old_profile.get("deadline_utc") != deadline_utc:
            state.setdefault("deadline_history", []).append({
                "previous": old_profile.get("deadline_utc"), "updated": deadline_utc, "at_utc": _utc()
            })
    state["profile"] = profile
    state["status"] = "running"
    _recover_accounted_calls(run_root, state)
    _save_state(run_root, state)
    _atomic_json(run_root / "manifest.json", {
        "schema_version": state["schema_version"], "status": "running",
        "profile": profile, "started_utc": state.get("created_utc"), "updated_utc": _utc(),
    })

    endpoint_names = set(selected_modes) | {adapter_endpoint}
    clients = {
        endpoint: _client(
            endpoint, urls[endpoint], run_root, state, seed=42,
            profile={"model_name": config_doc["model_alias"],
                     "request_timeout_seconds": config_doc.get("request_timeout_seconds", 600),
                     "reasoning_mode": config_doc.get("reasoning_mode", "on")},
        )
        for endpoint in sorted(endpoint_names)
    }

    # Import the early adapter call without regenerating it. Preserve its exact
    # attempt files and charge its supplied provider journal exactly once.
    external_adapter_usage: dict[str, Any] | None = None
    external_copy = run_root / "external_adapter_calls.jsonl"
    if adapter_call_journal_file is not None:
        source_journal = Path(adapter_call_journal_file)
        if not source_journal.is_file():
            raise FileNotFoundError(f"Adapter call journal not found: {source_journal}")
        source_hash = _sha(source_journal.read_bytes())
        existing_hash = state.get("adapter", {}).get("external_journal_sha256")
        if existing_hash is None:
            external_copy.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_journal, external_copy)
            state.setdefault("adapter", {})["external_journal_sha256"] = source_hash
            state["adapter"]["external_journal_source"] = str(source_journal.resolve())
            _save_state(run_root, state)
        elif existing_hash != source_hash:
            raise ValueError("resumed external adapter journal changed")
        external_rows = _jsonl(external_copy)
        external_adapter_usage = _sum_usage(external_rows)
        external_adapter_usage["journal_sha256"] = source_hash

    adapter_attempt_root = run_root / "adapter" / "attempts"
    adapter_json_path = run_root / "adapter" / "submission.json"
    if adapter_prompt_file is not None:
        adapter_prompt = Path(adapter_prompt_file).read_text(encoding="utf-8")
    elif (run_root / "adapter" / "prompt.md").is_file():
        adapter_prompt = (run_root / "adapter" / "prompt.md").read_text(encoding="utf-8")
    else:
        adapter_prompt = build_bank_adapter_prompt(CASES_DIR, development_ids)
    _atomic_text(run_root / "adapter" / "prompt.md", adapter_prompt)

    preloaded_attempts, imported_attempts_used, imported_call_rows = _imported_adapter_attempts(
        response_file=Path(adapter_response_file) if adapter_response_file else None,
        attempt_dir=Path(adapter_attempt_dir) if adapter_attempt_dir else None,
        journal_file=Path(adapter_call_journal_file) if adapter_call_journal_file else None,
        declared_attempts=adapter_attempts_used,
    )
    saved_attempt_used = int(state.get("adapter", {}).get("external_attempts_used", 0) or 0)
    external_attempts_used = max(imported_attempts_used, saved_attempt_used)
    if preloaded_attempts:
        state.setdefault("adapter", {})["external_attempts_used"] = external_attempts_used
        state["adapter"]["external_attempt_names"] = [
            item.get("name") for _, item in sorted(preloaded_attempts.items())
        ]
        _save_state(run_root, state)

    adapter_gate_path = run_root / "adapter" / "gate.json"
    adapter_json_path = run_root / "adapter" / "submission.json"
    adapter_attempt_root = run_root / "adapter" / "attempts"
    adapter_response: dict[str, Any] | None = None
    raw_saved_response: str | None = None
    if adapter_json_path.is_file():
        adapter_response = json.loads(adapter_json_path.read_text(encoding="utf-8"))
        raw_saved_response = json.dumps(adapter_response, ensure_ascii=False)
    elif adapter_response_file is not None:
        raw_saved_response = Path(adapter_response_file).read_text(encoding="utf-8")
        try:
            adapter_response = json.loads(raw_saved_response)
        except json.JSONDecodeError:
            adapter_response = None

    gate_candidates: list[Path] = []
    if adapter_gate_file is not None:
        gate_candidates.append(Path(adapter_gate_file))
    gate_candidates.append(adapter_gate_path)
    if adapter_attempt_dir is not None:
        gate_candidates.append(Path(adapter_attempt_dir) / "repair_01_validation.json")
    gate_report: dict[str, Any] | None = None
    for gate_path in gate_candidates:
        if gate_path.is_file() and adapter_response is not None:
            candidate_gate = json.loads(gate_path.read_text(encoding="utf-8"))
            if _reuse_adapter_gate_receipt(adapter_response, candidate_gate, CASES_DIR, development_ids):
                gate_report = candidate_gate
                state.setdefault("adapter", {})["reused_gate_receipt"] = str(gate_path.resolve())
                break

    if gate_report is None:
        if not preloaded_attempts and raw_saved_response is not None and not any(adapter_attempt_root.glob("attempt_*.json")):
            # Revalidate an already-saved response without spending a duplicate initial call.
            preloaded_attempts = {max(0, external_attempts_used - 1): {
                "name": "saved_submission.json", "response": raw_saved_response
            }}
            external_attempts_used = max(1, external_attempts_used)
        adapter_client = clients[adapter_endpoint]
        adapter_client.seed = 42
        gate_report, response_raw, adapter_stage = _attempt_stage(
            stage="adapter",
            base_prompt=adapter_prompt,
            attempt_dir=adapter_attempt_root,
            client=adapter_client,
            state=state,
            run_root=run_root,
            deadline=deadline,
            validator=lambda raw: _validate_bank_adapter(raw, CASES_DIR, development_ids),
            owner={"phase": "onboarding", "mode": "shared", "seed": 42, "endpoint": adapter_endpoint},
            preloaded_attempts=preloaded_attempts,
            external_attempts_used=external_attempts_used,
        )
        if adapter_stage.get("status") != "conforming" or gate_report is None or response_raw is None:
            state.setdefault("adapter", {}).update({"status": adapter_stage.get("status", "failed"), "attempts": adapter_stage})
            _save_state(run_root, state)
            for mode in selected_modes:
                for seed in SEEDS:
                    _write_skipped_chains(run_root, state, mode, seed, scored_ids,
                                          "shared adapter gate failed")
            return _finish_failure(run_root, state, profile, "adapter", adapter_stage, external_adapter_usage)
        from e4.data.submissions import parse_adapter_submission
        adapter_response = parse_adapter_submission(response_raw)
    elif adapter_response is None:
        # Gate receipts cannot stand in for the exact structured source response.
        adapter_gate_path = Path(adapter_gate_file) if adapter_gate_file is not None else adapter_gate_path
        raise ValueError(f"A validated gate receipt requires its matching adapter response file: {adapter_gate_path}")

    if adapter_response is None or gate_report is None:
        raise ValueError("adapter response and validated gate are both required")
    adapter_json_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_json(adapter_json_path, adapter_response)
    _atomic_text(run_root / "adapter" / "adapter.py", adapter_response["source"])
    _atomic_json(adapter_gate_path, gate_report)
    state.setdefault("adapter", {}).update({
        "status": "conforming", "gate": gate_report,
        "source_sha256": gate_report["adapter_source_sha256"],
        "external_attempts_used": external_attempts_used,
    })
    _save_state(run_root, state)

    schema_doc = gate_report["schema"]
    schema = normalize_dataset_schema(schema_doc)
    source_hash = gate_report["adapter_source_sha256"]
    # Read-only public normalization runs alongside the independent endpoint queues.
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="bank-onboarding") as pool:
        cache_future = pool.submit(_ensure_public_caches, run_root, scored_ids, adapter_response, source_hash)
        futures = {
            pool.submit(
                _prepare_mode, mode, run_root=run_root, state=state, schema=schema,
                schema_doc=schema_doc, adapter_response=adapter_response,
                development_ids=development_ids, clients=clients, urls=urls, deadline=deadline,
            ): mode
            for mode in selected_modes
        }
        for future in as_completed(futures):
            mode = futures[future]
            try:
                future.result()
            except Exception as exc:
                with _STATE_LOCK:
                    state.setdefault("modes", {}).setdefault(mode, {}).update({
                        "status": "failed", "error": _error_document(exc)
                    })
                _save_state(run_root, state)
        try:
            caches = cache_future.result()
        except Exception as exc:
            failure = _error_document(exc)
            for mode in selected_modes:
                for seed in SEEDS:
                    _write_skipped_chains(run_root, state, mode, seed, scored_ids, "public cache failed")
            return _finish_failure(run_root, state, profile, "public_cache", failure, external_adapter_usage)

    if prepare_only:
        result = _build_campaign_result(run_root, state, external_adapter_usage)
        result["status"] = "prepared"
        _write_final(run_root, result)
        return result

    # Pair seed queues so both modes complete/start seed 42 before seed 43,
    # then both seed 43 before seed 44. Each endpoint runs one chain at a time.
    for seed in SEEDS:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"bank-seed-{seed}") as pool:
            futures = {
                pool.submit(
                    _run_mode_seed, mode, seed, run_root=run_root, state=state,
                    schema=schema, scored_ids=scored_ids, caches=caches,
                    clients=clients, urls=urls, deadline=deadline,
                ): mode
                for mode in selected_modes
            }
            for future in as_completed(futures):
                mode = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    with _STATE_LOCK:
                        state.setdefault("modes", {}).setdefault(mode, {}).setdefault("inference_errors", []).append(
                            _error_document(exc)
                        )
                    _save_state(run_root, state)
                    for shard in SHARDS:
                        _write_skipped_chain(run_root, state, mode, seed, shard, scored_ids,
                                             f"chain worker failed: {type(exc).__name__}")

    result = _build_campaign_result(run_root, state, external_adapter_usage)
    _write_final(run_root, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run adapter-first paper-bootstrap and clean-slate Bank onboarding.")
    parser.add_argument("--run-root", required=True, type=Path, help="Fresh campaign directory, or existing directory with --resume")
    parser.add_argument("--modes", nargs="+", choices=MODES, default=list(MODES),
                        help="Library modes to prepare and evaluate (default: both); use a fresh run root per selection")
    parser.add_argument("--paper-url", default="http://127.0.0.1:19250")
    parser.add_argument("--clean-slate-url", default="http://127.0.0.1:19414")
    parser.add_argument("--adapter-endpoint", choices=("paper", "clean_slate"), default=None)
    parser.add_argument("--adapter-response-file", type=Path, help="Saved strict JSON adapter response; prevents duplicate generation")
    parser.add_argument("--adapter-attempt-dir", type=Path, help="Saved initial/repair response files from a prior adapter attempt")
    parser.add_argument("--adapter-attempts-used", type=int, help="Actual adapter calls already consumed (default: count the supplied journal)")
    parser.add_argument("--adapter-prompt-file", type=Path, help="Exact adapter prompt used for a saved response")
    parser.add_argument("--adapter-call-journal-file", type=Path, help="Provider call journal associated with a saved adapter response")
    parser.add_argument("--adapter-gate-file", type=Path, help="Previously passed public-development adapter gate report to verify and reuse")
    parser.add_argument("--deadline-utc", help="Optional new UTC stop boundary; omitted means no global deadline")
    parser.add_argument("--resume", action="store_true", help="Resume a detached campaign from its durable checkpoints")
    parser.add_argument("--prepare-only", action="store_true", help="Prepare/freeze both libraries without starting diagnosis chains")
    args = parser.parse_args()
    result = run_campaign(
        args.run_root,
        modes=tuple(args.modes),
        paper_url=args.paper_url,
        clean_slate_url=args.clean_slate_url,
        adapter_endpoint=args.adapter_endpoint,
        adapter_response_file=args.adapter_response_file,
        adapter_prompt_file=args.adapter_prompt_file,
        adapter_attempt_dir=args.adapter_attempt_dir,
        adapter_attempts_used=args.adapter_attempts_used,
        adapter_call_journal_file=args.adapter_call_journal_file,
        adapter_gate_file=args.adapter_gate_file,
        deadline_utc=args.deadline_utc,
        resume=args.resume,
        prepare_only=args.prepare_only,
    )
    print(json.dumps({"status": result.get("status"), "run_root": str(args.run_root),
                      "modes": {name: value.get("status") for name, value in result.get("modes", {}).items()},
                      "token_costs": result.get("token_costs", {})}, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
