"""Bounded views of a window's log records, for playbooks and generated operators.

    cyber.logs.count   TelemetryWindows -> Aggregate   one row per (entity, table, kind): its count
    cyber.logs.query   TelemetryWindows -> Aggregate   one page of raw log records, in metadata

Generated operators cannot read a TelemetryWindow (it holds the whole run), so these turn one
window into an Aggregate that names the window by its TelemetryWindowRef.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .contracts import Aggregate, AggregateRow, LogRecord, TelemetryWindow, TelemetryWindows
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec

MAX_ROWS_PER_PAGE = 256
MAX_PAGE_BYTES = 512 * 1024
MAX_VALUE_CHARS = 1024


def one_window(windows: TelemetryWindows, operator: str) -> TelemetryWindow:
    if len(windows.windows) != 1:
        raise ValueError(f"{operator} reads exactly one window")
    return windows.windows[0]


def count_logs(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any) -> Aggregate:
    """One row per (entity, table, kind) in the window: how many records, first and last ms."""
    window = one_window(inputs["windows"], "cyber.logs.count")
    counts: dict[tuple, dict[str, Any]] = {}
    for record in window.logs:
        key = (record.entity, record.table, record.kind)
        if key not in counts:
            counts[key] = {"count": 0, "first_ms": record.timestamp_ms_utc,
                           "last_ms": record.timestamp_ms_utc}
        summary = counts[key]
        summary["count"] += 1
        summary["first_ms"] = min(summary["first_ms"], record.timestamp_ms_utc)
        summary["last_ms"] = max(summary["last_ms"], record.timestamp_ms_utc)

    rows = []
    tables = set()
    for (entity, table, kind), summary in counts.items():
        tables.add(table)
        rows.append(AggregateRow(
            entity=entity, metric_key=f"{table}|{kind}", window=window.ref, statistic="count",
            value=float(summary["count"]), support=summary["count"], source_tables=(table,),
            details={"table": table, "kind": kind,
                     "first_ms": summary["first_ms"], "last_ms": summary["last_ms"]}))
    return Aggregate(window.ref, tuple(rows), tuple(sorted(tables)),
                     {"summary": "record counts per entity, table and kind"})


def plain(value: Any) -> Any:
    """A frozen record value as plain JSON data, long strings cut to MAX_VALUE_CHARS."""
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            result[str(key)] = plain(item)
        return result
    if isinstance(value, (list, tuple)):
        result = []
        for item in value:
            result.append(plain(item))
        return result
    if isinstance(value, str) and len(value) > MAX_VALUE_CHARS:
        return value[:MAX_VALUE_CHARS] + f"...[{len(value) - MAX_VALUE_CHARS} chars cut]"
    return value


def record_time(record: LogRecord) -> int:
    return record.timestamp_ms_utc


def record_matches(record: LogRecord, params: Mapping[str, Any]) -> bool:
    if params.get("table") and record.table != params["table"]:
        return False
    if params.get("kind") and record.kind != params["kind"]:
        return False
    if params.get("entity") and record.entity.identifier != params["entity"]:
        return False
    return True


def record_row(record: LogRecord, fields: tuple[str, ...]) -> dict[str, Any]:
    chosen = {}
    for name, value in record.fields.items():
        if not fields or name in fields:
            chosen[name] = plain(value)
    return {"timestamp_ms": record.timestamp_ms_utc, "table": record.table,
            "entity": f"{record.entity.role}:{record.entity.identifier}", "kind": record.kind,
            "fields": chosen}


def query_logs(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any) -> Aggregate:
    """One page of the window's records that match table/kind/entity, oldest first."""
    window = one_window(inputs["windows"], "cyber.logs.query")
    offset = int(params["offset"])
    limit = int(params["limit"])
    if offset < 0:
        raise ValueError("offset must be zero or more")
    if limit > MAX_ROWS_PER_PAGE:
        raise ValueError(f"limit must be at most {MAX_ROWS_PER_PAGE}")
    fields = tuple(params["fields"])

    matching = []
    for record in window.logs:
        if record_matches(record, params):
            matching.append(record)
    matching.sort(key=record_time)

    page = []
    page_bytes = 0
    for record in matching[offset:offset + limit]:
        row = record_row(record, fields)
        row_bytes = len(json.dumps(row, default=str))
        if page and page_bytes + row_bytes > MAX_PAGE_BYTES:
            break
        page.append(row)
        page_bytes += row_bytes

    next_offset = offset + len(page)
    if next_offset >= len(matching):
        next_offset = None
    tables = set()
    for row in page:
        tables.add(row["table"])
    query = {"request": {"table": params.get("table"), "kind": params.get("kind"),
                         "entity": params.get("entity"), "offset": offset, "limit": limit,
                         "fields": list(fields)},
             "rows_data": page, "total_matching_rows": len(matching), "returned_rows": len(page),
             "next_offset": next_offset,
             "value_policy": f"string values longer than {MAX_VALUE_CHARS} characters are cut"}
    return Aggregate(window.ref, (), tuple(sorted(tables)), {"log_query": query})


def register_cyber_data_operators(registry: OperatorRegistry) -> OperatorRegistry:
    registry.register(OperatorSpec(
        "cyber.logs.count", "1", (PortSpec("windows", "TelemetryWindows"),), "Aggregate",
        family="cyber_data", implementation=count_logs,
        description="One row per (entity, table, kind) in one window: count, first_ms, last_ms."))
    registry.register(OperatorSpec(
        "cyber.logs.query", "1", (PortSpec("windows", "TelemetryWindows"),), "Aggregate",
        {"table": ParameterSpec("table", "string", nullable=True, has_default=True,
                                description="keep only this table, e.g. attacker/actions.json"),
         "kind": ParameterSpec("kind", "string", nullable=True, has_default=True,
                               description="keep only this kind, e.g. ExfiltrateData or EXECVE"),
         "entity": ParameterSpec("entity", "string", nullable=True, has_default=True,
                                 description="keep only this entity identifier, e.g. database3"),
         "offset": ParameterSpec("offset", "integer", default=0, has_default=True),
         "limit": ParameterSpec("limit", "positive_integer", default=64, has_default=True),
         "fields": ParameterSpec("fields", "string_list", default=(), has_default=True,
                                 description="record fields to return; empty returns all")},
        family="cyber_data", cacheable=False, implementation=query_logs,
        description="One page (at most 256 rows, 512 KiB) of one window's raw log records matching "
                    "table/kind/entity, oldest first; rows are in metadata.log_query.rows_data."))
    return registry
