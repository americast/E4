"""Bounded, loss-accounted projections of public case digests for paper prompts.

The exact legal metric choices are published by separate trusted catalogs. This
module summarizes the larger observed-field inventory without turning observed
names into model-authorized selectors or implying that omitted fields are absent.
"""
from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from typing import Any


_STAGES = frozenset({"select", "compose", "evolve", "stage3_serve"})
_FIELD_EXAMPLE_LIMIT = 12
_EVOLUTION_SERIES_KEYS = ("metric_series", "app_kpi_context_series", "trace_series")


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(child) for child in value]
    return copy.deepcopy(value)


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _evolution_capability_reference(digest: Mapping[str, Any], key: str, *,
        capability_ref: str, collection: bool = False,
        denominator_ref: str | None = None) -> dict[str, Any]:
    if key not in digest:
        return {"schema_version": "e4-paper-capability-summary-reference/1",
            "status": "unavailable", "capability_summary_ref": capability_ref,
            "denominator_summary_ref": denominator_ref,
            "original_item_count": None, "original_content_sha256": None,
            "source_omitted_item_count": None, "omitted_by_prompt_projection_count": None,
            "known_total_item_count": None,
            "omission_reason": "source digest did not provide this detail; the capability reference does not imply data is present"}
    value = digest[key]
    if collection:
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"{key} must be a sequence in the public case digest")
        rows = _plain(value)
        source_omitted = digest.get(f"{key}_omitted")
        if source_omitted is not None and (type(source_omitted) is not int or source_omitted < 0):
            raise ValueError(f"{key}_omitted must be a nonnegative integer when supplied")
        known_total = len(rows) + source_omitted if type(source_omitted) is int else None
        return {"schema_version": "e4-paper-capability-summary-reference/1",
            "status": "available" if rows or (source_omitted or 0) else "available_empty",
            "capability_summary_ref": capability_ref,
            "denominator_summary_ref": denominator_ref,
            "original_item_count": len(rows),
            "original_content_sha256": _canonical_sha256(rows),
            "source_omitted_item_count": source_omitted,
            "known_total_item_count": known_total,
            "represented_item_count": 0,
            "omitted_by_prompt_projection_count": len(rows),
            "omission_reason": "detailed per-stream rows are summarized in the referenced trusted capability report; no row values are copied"}
    if not isinstance(value, Mapping):
        raise ValueError(f"{key} must be a mapping in the public case digest")
    plain = _plain(value)
    return {"schema_version": "e4-paper-capability-summary-reference/1",
        "status": "available" if plain else "available_empty",
        "capability_summary_ref": capability_ref,
        "denominator_summary_ref": denominator_ref,
        "original_item_count": len(plain),
        "original_content_sha256": _canonical_sha256(plain),
        "source_omitted_item_count": 0,
        "known_total_item_count": len(plain),
        "represented_item_count": 0,
        "omitted_by_prompt_projection_count": len(plain),
        "omission_reason": "selected KPI identity, policy, and support details are carried by the referenced trusted capability summary; no values are copied"}


def _field_inventory_summary(digest: Mapping[str, Any], *, stage: str) -> dict[str, Any]:
    selector_ref = ("metric_selector_catalog" if stage == "evolve"
        else "paper.bank_component_metric_fields")
    related_selector_refs = ([] if stage == "evolve" else
        ["paper.bank_component_metric_rule_refs"])
    if "available_metric_fields" not in digest:
        return {"schema_version": "e4-paper-observed-field-summary/1",
            "status": "unavailable", "source_table": "bank/metric_container.csv",
            "observed_unique_field_count": None, "field_names_sha256": None,
            "examples": [], "represented_field_count": 0,
            "omitted_field_count": None, "duplicate_name_count": None,
            "coverage_fraction": None,
            "omission_reason": "source digest did not provide an observed field inventory",
            "selector_catalog_ref": selector_ref,
            "related_selector_catalog_refs": related_selector_refs}
    fields = digest["available_metric_fields"]
    if not isinstance(fields, (list, tuple)) or any(type(field) is not str or not field for field in fields):
        raise ValueError("available_metric_fields must be a sequence of exact nonempty field names")
    unique = sorted(set(fields))
    examples = unique[:_FIELD_EXAMPLE_LIMIT]
    total = len(unique)
    omitted = total - len(examples)
    return {"schema_version": "e4-paper-observed-field-summary/1",
        "status": "available" if total else "available_empty",
        "source_table": "bank/metric_container.csv",
        "observed_unique_field_count": total,
        "field_names_sha256": _canonical_sha256(unique),
        "examples": examples,
        "represented_field_count": len(examples),
        "omitted_field_count": omitted,
        "duplicate_name_count": len(fields) - total,
        "coverage_fraction": 1.0 if total == 0 else len(examples) / total,
        "omission_reason": (None if omitted == 0 else
            "the full observed inventory is summarized by exact count and SHA-256; examples are descriptive and are not legal selectors"),
        "selector_catalog_ref": selector_ref,
        "related_selector_catalog_refs": related_selector_refs}


def compact_paper_case_digest(digest: Mapping[str, Any], *, stage: str) -> dict[str, Any]:
    """Return a deterministic stage-aware case digest with explicit omissions.

    `select`, `compose`, and `stage3_serve` retain all existing evidence examples
    and replace only the oversized raw metric-name inventory with a bounded,
    hash-pinned summary. `evolve` keeps identity, source, KPI, coverage and log
    summary context, omits diagnosis-sized metric/app/trace series, and replaces
    duplicated KPI/log details with references to the trusted capability summary.
    """
    if stage not in _STAGES:
        raise ValueError(f"unsupported paper case-digest stage: {stage!r}")
    if not isinstance(digest, Mapping):
        raise TypeError("paper case digest must be a mapping")
    result = _plain(digest)
    result["available_metric_fields"] = _field_inventory_summary(digest, stage=stage)
    source_digest_sha = digest.get("digest_sha256")
    if source_digest_sha is not None:
        result["source_digest_sha256"] = source_digest_sha
    result["prompt_digest_projection"] = {
        "schema_version": "e4-paper-case-digest-projection/1",
        "stage": stage,
        "selector_catalog_ref": result["available_metric_fields"]["selector_catalog_ref"],
        "source_digest_sha256": source_digest_sha,
        "semantics": "observed field inventory summary is descriptive; legal selector choices and statuses remain in the separate trusted catalog",
    }
    if stage == "evolve":
        for key in _EVOLUTION_SERIES_KEYS:
            if key not in digest:
                continue
            rows = digest[key]
            if not isinstance(rows, (list, tuple)):
                raise ValueError(f"{key} must be a sequence in the public case digest")
            existing_omitted = digest.get(f"{key}_omitted", 0)
            result[f"{key}_omitted_by_prompt_projection"] = len(rows)
            if type(existing_omitted) is int and existing_omitted >= 0:
                result[f"{key}_known_total_count"] = len(rows) + existing_omitted
            result.pop(key, None)
        result["primary_app_kpi_context"] = _evolution_capability_reference(digest,
            "primary_app_kpi_context", capability_ref="primary_kpi")
        log_key = "log_volume_series"
        result[log_key] = _evolution_capability_reference(digest, log_key,
            capability_ref="computations.native_log_pattern_composition",
            collection=True,
            denominator_ref="computations.native_log_pattern_composition.window_denominators_by_stream")
    return result


__all__ = ["compact_paper_case_digest"]
