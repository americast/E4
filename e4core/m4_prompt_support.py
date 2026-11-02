"""Lossless prompt-time interning for repeated case-support payloads.

This module only projects authoring-prompt data. It does not change trusted
capability reports, generated signatures, compiler inputs, or worker contracts.
"""
from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any


SUPPORT_PROFILE_SCHEMA = "e4-case-support-profiles/1"


class PromptSupportError(ValueError):
    pass


def _validate_json(value: Any, *, path: str = "support") -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise PromptSupportError(f"{path} contains a non-finite number")
        return
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise PromptSupportError(f"{path} mapping keys must be strings")
        for key, child in value.items():
            _validate_json(child, path=f"{path}.{key}")
        return
    if type(value) is list:
        for index, child in enumerate(value):
            _validate_json(child, path=f"{path}[{index}]")
        return
    raise PromptSupportError(f"{path} contains a non-JSON value {type(value).__name__}")


def _canonical(value: Any) -> tuple[str, Any]:
    try:
        _validate_json(value)
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False)
        decoded = json.loads(encoded)
    except PromptSupportError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PromptSupportError("case-support payload must be finite JSON data") from exc
    return encoded, decoded


def intern_case_support_profiles(selector_rows: Sequence[Mapping[str, Any]], *,
        ref_key: str = "selector_ref", support_key: str = "case_resolution"
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Replace duplicate support objects with stable refs, preserving exact rows.

    A support profile is interned only when its canonical JSON representation
    is identical. Missing support remains missing; an explicit ``None`` is a
    distinct, interned JSON null; zero counts and empty histograms remain exact.
    ``ref_key`` lets the same helper serve stable selector refs and literal
    high-level enum values without rewriting either identity.
    """
    if (not isinstance(selector_rows, Sequence) or
            isinstance(selector_rows, (str, bytes, bytearray)) or
            not isinstance(ref_key, str) or not ref_key or
            not isinstance(support_key, str) or not support_key or ref_key == support_key):
        raise PromptSupportError("rows and distinct nonempty identity/support keys are required")
    projected_base: list[dict[str, Any]] = []
    row_order: list[Any] = []
    payloads: dict[str, Any] = {}
    row_payloads: list[str | None] = []
    for index, raw in enumerate(selector_rows):
        if not isinstance(raw, Mapping) or ref_key not in raw:
            raise PromptSupportError(f"row {index} must contain its exact {ref_key!r} identity")
        identity = raw[ref_key]
        if not isinstance(identity, str):
            raise PromptSupportError(f"row {index} identity {ref_key!r} must be a string")
        row_order.append(identity)
        row = dict(raw)
        if support_key not in row:
            if "support_profile_ref" in row:
                raise PromptSupportError(f"row {index} has a support ref without a support payload")
            projected_base.append(row)
            row_payloads.append(None)
            continue
        if "support_profile_ref" in row:
            raise PromptSupportError(f"row {index} already has a support_profile_ref")
        canonical, decoded = _canonical(row.pop(support_key))
        payloads.setdefault(canonical, decoded)
        projected_base.append(row)
        row_payloads.append(canonical)
    canonical_payloads = sorted(payloads)
    payload_to_ref = {canonical: f"support_{index}"
        for index, canonical in enumerate(canonical_payloads, start=1)}
    profiles = [{"support_profile_ref": payload_to_ref[canonical],
        support_key: payloads[canonical]} for canonical in canonical_payloads]
    projected = []
    for row, payload_canonical in zip(projected_base, row_payloads):
        if payload_canonical is not None:
            row["support_profile_ref"] = payload_to_ref[payload_canonical]
        projected.append(row)
    catalog = {"schema_version": SUPPORT_PROFILE_SCHEMA,
        "ref_key": ref_key, "support_key": support_key,
        "row_order": row_order, "profiles": profiles}
    return projected, catalog


def expand_case_support_profiles(projected_rows: Sequence[Mapping[str, Any]],
        profile_catalog: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Reconstruct original rows and payloads from a prompt projection."""
    if (not isinstance(projected_rows, Sequence) or
            isinstance(projected_rows, (str, bytes, bytearray)) or
            not isinstance(profile_catalog, Mapping) or
            profile_catalog.get("schema_version") != SUPPORT_PROFILE_SCHEMA):
        raise PromptSupportError("projected rows or support profile catalog has an invalid schema")
    ref_key, support_key = profile_catalog.get("ref_key"), profile_catalog.get("support_key")
    if (not isinstance(ref_key, str) or not ref_key or not isinstance(support_key, str) or
            not support_key or ref_key == support_key):
        raise PromptSupportError("support profile catalog has invalid identity/support keys")
    rows = [dict(row) for row in projected_rows if isinstance(row, Mapping)]
    if len(rows) != len(projected_rows):
        raise PromptSupportError("every projected row must be a mapping")
    identities = [row.get(ref_key) for row in rows]
    if identities != profile_catalog.get("row_order"):
        raise PromptSupportError("projected row identities/order differ from the support catalog")
    raw_profiles = profile_catalog.get("profiles")
    if not isinstance(raw_profiles, list):
        raise PromptSupportError("support profile definitions must be an ordered list")
    profiles: dict[str, Any] = {}
    payload_to_ref: dict[str, str] = {}
    for definition in raw_profiles:
        if (not isinstance(definition, Mapping) or set(definition) !=
                {"support_profile_ref", support_key} or
                not isinstance(definition.get("support_profile_ref"), str)):
            raise PromptSupportError("support profile definition has invalid fields")
        profile_ref = definition["support_profile_ref"]
        canonical, decoded = _canonical(definition[support_key])
        if profile_ref in profiles or canonical in payload_to_ref:
            raise PromptSupportError("support profile catalog repeats a ref or exact payload")
        profiles[profile_ref] = decoded
        payload_to_ref[canonical] = profile_ref
    sorted_payloads = sorted(payload_to_ref)
    expected_ids = {canonical: f"support_{index}"
        for index, canonical in enumerate(sorted_payloads, start=1)}
    if (payload_to_ref != expected_ids or
            [row.get("support_profile_ref") for row in raw_profiles] !=
                [expected_ids[canonical] for canonical in sorted_payloads]):
        raise PromptSupportError("support profile IDs/order are not stable canonical assignments")
    expanded: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        has_profile_ref = "support_profile_ref" in row
        profile_ref = row.pop("support_profile_ref", None)
        if not has_profile_ref:
            if support_key in row:
                raise PromptSupportError(f"row {index} contains both support data and no profile ref")
        else:
            if (not isinstance(profile_ref, str) or profile_ref not in profiles or
                    support_key in row):
                raise PromptSupportError(f"row {index} has an unknown or conflicting support profile")
            row[support_key] = profiles[profile_ref]
        expanded.append(row)
    return expanded
