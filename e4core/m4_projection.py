"""Bounded projection of existing Bank Aggregate summaries for generated operators."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .cache import canonical_json, digest_value
from .contracts import Aggregate, AggregateRow
from .executor import NodeContext, OperatorResult
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec


MAX_PAGE_ROWS = 256
MAX_OFFSET = 100_000
MAX_SELECTED_STRING_BYTES = 4096
MAX_OUTPUT_BYTES = 512 * 1024
_SOURCE_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
PROJECTION_VERSION = "1+" + _SOURCE_HASH[:16]


class BankProjectionError(ValueError):
    """The source Aggregate cannot be projected within the published contract."""


def _is_scalar(value: Any) -> bool:
    if value is None or type(value) in (str, bool, int):
        return True
    if type(value) is float:
        return math.isfinite(value)
    return False


def _check_string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise BankProjectionError(f"{field} must be text")
    if len(value.encode("utf-8")) > MAX_SELECTED_STRING_BYTES:
        raise BankProjectionError(f"{field} exceeds {MAX_SELECTED_STRING_BYTES} UTF-8 bytes")
    return value


def _string_filter(value: Any, name: str, *, casefold_duplicates: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise BankProjectionError(f"{name} must be a list of strings")
    if any(not isinstance(item, str) or not item.strip() or
           len(item.encode("utf-8")) > MAX_SELECTED_STRING_BYTES for item in value):
        raise BankProjectionError(f"{name} must contain nonempty strings up to {MAX_SELECTED_STRING_BYTES} UTF-8 bytes")
    canonical = [item.strip().casefold() if casefold_duplicates else item.strip() for item in value]
    if len(value) > 256 or len(set(canonical)) != len(value):
        raise BankProjectionError(f"{name} contains too many or duplicate values")
    return tuple(item.strip() for item in value)


def _row_source_field(row: AggregateRow) -> str | None:
    source_field = row.details.get("source_field")
    if isinstance(source_field, str) and source_field:
        return source_field
    # Bank trace aggregates have an undeclared duration unit and a stable native
    # field suffix, but no separate source_field details entry.
    if "bank/trace_span.csv" in row.source_tables and row.metric_key.endswith("|duration"):
        return "duration"
    return None


def _project_row(row: AggregateRow) -> AggregateRow:
    details: dict[str, Any] = {}
    omitted = []
    for key, value in row.details.items():
        if not isinstance(key, str):
            raise BankProjectionError("AggregateRow detail keys must be strings")
        _check_string(key, "detail key")
        if key == "projection_omitted_detail_keys":
            omitted.extend(item for item in value if isinstance(item, str)) if isinstance(value, (tuple, list)) else None
            continue
        if _is_scalar(value):
            if isinstance(value, str):
                _check_string(value, f"detail {key}")
            details[key] = value
        else:
            omitted.append(key)
    details["projection_omitted_detail_keys"] = tuple(sorted(set(omitted)))
    entity = row.entity
    _check_string(entity.identifier, "entity identifier")
    _check_string(entity.role, "entity role")
    _check_string(row.metric_key, "metric key")
    _check_string(row.statistic, "statistic")
    for table in row.source_tables:
        _check_string(table, "source table")
    for contributing in row.contributing_entities:
        _check_string(contributing.identifier, "contributing entity identifier")
        _check_string(contributing.role, "contributing entity role")
    if row.value is not None and type(row.value) is float and not math.isfinite(row.value):
        raise BankProjectionError("AggregateRow value must be finite")
    return AggregateRow(entity=row.entity, metric_key=row.metric_key, window=row.window,
        statistic=row.statistic, value=row.value, support=row.support,
        source_tables=row.source_tables, contributing_entities=row.contributing_entities,
        details=details)


def _project_metadata(metadata: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    projected = {}
    omitted = []
    for key, value in metadata.items():
        if not isinstance(key, str):
            raise BankProjectionError("Aggregate metadata keys must be strings")
        _check_string(key, "metadata key")
        if _is_scalar(value):
            if isinstance(value, str):
                _check_string(value, f"metadata {key}")
            projected[key] = value
        else:
            omitted.append(key)
    return projected, sorted(set(omitted))


def project_bank_aggregate(inputs: Mapping[str, Any], params: Mapping[str, Any],
                           context: NodeContext) -> OperatorResult:
    """Filter and page already-computed full-window Aggregate rows without sampling."""
    aggregate = inputs.get("data")
    if not isinstance(aggregate, Aggregate):
        raise BankProjectionError("bank.public.project requires an Aggregate input")
    if "projection" in aggregate.metadata:
        raise BankProjectionError("re-projecting a paged projection is unsupported; project the original Aggregate")
    if "public_query" in aggregate.metadata:
        raise BankProjectionError("native public-query pages are already bounded; pass them directly to the generated operator")
    source_fields = _string_filter(params.get("source_fields", ()), "source_fields")
    entities = _string_filter(params.get("entities", ()), "entities", casefold_duplicates=True)
    statistics = _string_filter(params.get("statistics", ()), "statistics")
    offset, limit = params.get("offset", 0), params.get("limit", 64)
    if type(offset) is not int or not 0 <= offset <= MAX_OFFSET:
        raise BankProjectionError(f"offset must be an integer between 0 and {MAX_OFFSET}")
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_ROWS:
        raise BankProjectionError(f"limit must be an integer between 1 and {MAX_PAGE_ROWS}")
    for table in aggregate.source_tables:
        _check_string(table, "source table")

    field_keys = set(source_fields)
    entity_keys = {item.strip().casefold() for item in entities}
    statistic_keys = set(statistics)
    matched = []
    for row in aggregate.rows:
        if field_keys:
            source_field = _row_source_field(row)
            if source_field is None or source_field not in field_keys:
                continue
        if entity_keys and row.entity.identifier.strip().casefold() not in entity_keys:
            continue
        if statistic_keys and row.statistic not in statistic_keys:
            continue
        matched.append(row)

    total = len(matched)
    selected = matched[offset:offset + limit]
    projected_rows = tuple(_project_row(row) for row in selected)
    metadata, omitted_metadata_keys = _project_metadata(aggregate.metadata)
    omitted_detail_keys = sorted({key for row in projected_rows
        for key in row.details.get("projection_omitted_detail_keys", ())})
    source_digest = digest_value(aggregate)
    before_count = min(offset, total)
    after_count = max(0, total - offset - len(projected_rows))
    projection_doc = {
        "source_aggregate_digest": source_digest,
        "source_row_count": len(aggregate.rows),
        "total_matching_row_count": total,
        "selected_row_count": len(projected_rows),
        "omitted_before": before_count,
        "remaining_after": after_count,
        "complete": offset == 0 and len(projected_rows) == total,
        "offset": offset,
        "limit": limit,
        "next_offset": offset + len(projected_rows) if after_count else None,
        "source_fields_filter": list(source_fields),
        "entities_filter": list(entities),
        "statistics_filter": list(statistics),
        "omitted_metadata_keys": omitted_metadata_keys,
        "omitted_detail_keys": omitted_detail_keys,
        "projection_policy": "full-window summary rows are filtered and paged; no samples or statistics are recomputed",
    }
    metadata["projection"] = projection_doc
    metadata["projection_omitted_metadata_keys"] = omitted_metadata_keys
    input_bytes = len(canonical_json(aggregate))
    metadata["projection"]["source_serialized_bytes"] = input_bytes
    output_bytes = 0
    for _ in range(4):
        metadata["projection"]["projection_serialized_bytes"] = output_bytes
        output = Aggregate(window=aggregate.window, rows=projected_rows,
            source_tables=aggregate.source_tables, metadata=metadata)
        measured = len(canonical_json(output))
        if measured == output_bytes:
            break
        output_bytes = measured
    if output_bytes > MAX_OUTPUT_BYTES:
        raise BankProjectionError(f"serialized projection exceeds {MAX_OUTPUT_BYTES} bytes")
    metadata["projection"]["projection_serialized_bytes"] = output_bytes
    output = Aggregate(window=aggregate.window, rows=projected_rows,
        source_tables=aggregate.source_tables, metadata=metadata)
    output_bytes = len(canonical_json(output))
    if output_bytes > MAX_OUTPUT_BYTES:
        raise BankProjectionError(f"serialized projection exceeds {MAX_OUTPUT_BYTES} bytes")
    return OperatorResult(output, rows_processed=len(aggregate.rows),
                          bytes_processed=input_bytes + output_bytes)


def register_m4_projection(registry: OperatorRegistry) -> None:
    """Register the trusted, cacheable scalar projection operator."""
    parameters = {
        "source_fields": ParameterSpec("source_fields", "string_list", default=(), has_default=True,
            description="Optional exact native source_field values; Bank trace duration falls back to duration."),
        "entities": ParameterSpec("entities", "string_list", default=(), has_default=True,
            description="Optional exact component identifiers; formatting is trimmed/casefolded only."),
        "statistics": ParameterSpec("statistics", "string_list", default=(), has_default=True,
            description="Optional exact summary statistic names."),
        "offset": ParameterSpec("offset", "integer", default=0, has_default=True,
            description="Zero-based offset over matching summary rows."),
        "limit": ParameterSpec("limit", "positive_integer", default=64, has_default=True,
            description="Maximum number of summary rows to return, at most 256."),
    }
    registry.register(OperatorSpec("bank.public.project", PROJECTION_VERSION,
        (PortSpec("data", "Aggregate"),), "Aggregate", parameters=parameters,
        family="bank_public_projection", cacheable=True, implementation=project_bank_aggregate,
        description="Filter/page already-computed Bank full-window summary rows and omit bulky detail arrays with explicit coverage metadata."))
