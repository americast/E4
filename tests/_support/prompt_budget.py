"""Lossless display packing used to verify saved prompt reconstruction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from e4core.m4_controller import (
    _expand_registered_signatures,
    _front_code_paper_metric_fields,
    _paper_registered_signature_catalog,
)

ROOT = Path(__file__).resolve().parents[2]
SOURCE_RUN = ROOT / "artifacts/m3/model_runs/e4s43/audits/20261024T164037Z-1f42bf1e"
ELIGIBLE_PHASES = {"select", "compose", "evolve", "stage3_serve"}
FRONT_CODE_GUIDANCE = (
    "\nAvailable metric fields use e4-front-codepoint-prefix/1: each row is "
    "[common_prefix_codepoint_count, suffix]; start with an empty previous name and "
    "reconstruct each exact next name as previous[:count] + suffix. Preserve field order and spelling. "
    "This is a descriptive observed-field coverage inventory, not a selector enum. For exact legal "
    "selector parameters, use the plainly listed paper.bank_component_metric_fields values in the "
    "shared parameter catalog."
)
PACKED_SIGNATURE_HEADER = (
    "\n\nREGISTERED OPERATOR SIGNATURES (lossless input/parameter contract references; "
    "expand each contract_ref from the declared tables and defaults)\n"
)


def _sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_after(text: str, start: int) -> tuple[Any, int, int]:
    decoder = json.JSONDecoder()
    offset = start
    while offset < len(text) and text[offset].isspace():
        offset += 1
    value, length = decoder.raw_decode(text[offset:])
    return value, offset, offset + length


def _replace_saved_signature_section(user: str) -> tuple[str, bool]:
    plain_header = "\n\nREGISTERED OPERATOR SIGNATURES\n"
    compact_header = (
        "\n\nREGISTERED OPERATOR SIGNATURES "
        "(lossless defaults encoding; expand omitted catalog fields from listed defaults)\n"
    )
    if PACKED_SIGNATURE_HEADER.strip() in user:
        return user, False
    header = next((value for value in (plain_header, compact_header) if value in user), None)
    if header is None:
        return user, False
    header_at = user.index(header)
    value, json_start, json_end = _json_after(user, header_at + len(header))
    if isinstance(value, Mapping):
        signatures = _expand_registered_signatures(value)
    elif isinstance(value, list):
        signatures = value
    else:
        raise ValueError("saved registered signatures have an unsupported shape")
    packed = _paper_registered_signature_catalog(signatures)
    packed_json = json.dumps(packed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return user[:header_at] + PACKED_SIGNATURE_HEADER + packed_json + user[json_end:], True


def _replace_saved_case_digest(user: str) -> tuple[str, bool]:
    header = "\n\nCURRENT PUBLIC CASE DIGEST\n"
    if header not in user:
        return user, False
    header_at = user.index(header)
    value, json_start, json_end = _json_after(user, header_at + len(header))
    if not isinstance(value, dict) or not isinstance(value.get("available_metric_fields"), list):
        raise ValueError("saved paper case digest lacks its public metric inventory")
    packed = _front_code_paper_metric_fields(value)
    packed_json = json.dumps(packed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return user[:json_start] + packed_json + FRONT_CODE_GUIDANCE + user[json_end:], True


def pack_request(request: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = json.loads(json.dumps(request, ensure_ascii=False, allow_nan=False))
    messages = candidate.get("messages")
    if not isinstance(messages, list) or len(messages) != 2 or messages[1].get("role") != "user":
        raise ValueError("saved request must have one system and one user message")
    before = messages[1]["content"]
    after, signatures_packed = _replace_saved_signature_section(before)
    after, digest_packed = _replace_saved_case_digest(after)
    messages[1]["content"] = after
    return candidate, {"signature_catalog_packed": signatures_packed,
        "digest_fields_front_coded": digest_packed,
        "source_user_chars": len(before), "candidate_user_chars": len(after),
        "source_user_sha256": _sha_bytes(before.encode("utf-8")),
        "candidate_user_sha256": _sha_bytes(after.encode("utf-8"))}


def _phase_maxima(source_run: Path = SOURCE_RUN) -> list[tuple[Path, dict[str, Any]]]:
    records = []
    for path in source_run.rglob("model_calls.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if row.get("m4_phase") in ELIGIBLE_PHASES:
                records.append((path, row))
    selected = {}
    for path, row in records:
        key = (row["m4_phase"], row.get("m4_static_repair") is True)
        old = selected.get(key)
        if old is None or (row.get("prompt_token_count") or 0) > (old[1].get("prompt_token_count") or 0):
            selected[key] = (path, row)
    return [selected[key] for key in sorted(selected)]
