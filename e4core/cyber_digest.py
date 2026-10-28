"""A bounded summary of a Case for the model's prompt: what each window holds, counted, never raw.

Raw records are read only through the cyber.logs.query operator inside a playbook.
"""
from __future__ import annotations

import json
from typing import Any

from .contracts import Case, TelemetryWindow

MAX_DIGEST_BYTES = 12 * 1024
TOP_KINDS = 25
ACTIONS_TABLE = "attacker/actions.json"


def count_by(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def by_count_descending(item: tuple[str, int]) -> tuple[int, str]:
    return (-item[1], item[0])


def top(counts: dict[str, int], limit: int) -> dict[str, int]:
    ranked = sorted(counts.items(), key=by_count_descending)
    return dict(ranked[:limit])


def window_summary(window: TelemetryWindow, role: str, top_kinds: int) -> dict[str, Any]:
    """Counts of what one window holds: per table, per harness action, and its busiest kinds."""
    tables = []
    high_level = []
    low_level = []
    kinds = []
    for record in window.logs:
        tables.append(record.table)
        kinds.append(f"{record.table}|{record.kind}")
        if record.table == ACTIONS_TABLE:
            if record.fields.get("type") == "HighLevelAction":
                high_level.append(record.kind)
            else:
                low_level.append(record.kind)

    params = window.telemetry.params
    return {"window_id": window.window_id, "role": role,
            "telemetry_id": window.telemetry.telemetry_id,
            "start_ms": window.start_ms, "end_ms": window.end_ms,
            "minutes": round((window.end_ms - window.start_ms) / 60000, 1),
            "model": params.get("model"), "harness": params.get("harness"),
            "environment": params.get("environment"),
            "records_per_table": count_by(tables),
            "high_level_actions": top(count_by(high_level), top_kinds),
            "low_level_actions": top(count_by(low_level), top_kinds),
            "busiest_table_kinds": top(count_by(kinds), top_kinds)}


def components_by_role(case: Case) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for component in case.components:
        grouped.setdefault(component.role, []).append(component.identifier)
    return grouped


def case_digest(case: Case) -> dict[str, Any]:
    """The Case's windows (anomalous first) and components, within MAX_DIGEST_BYTES."""
    top_kinds = TOP_KINDS
    while True:
        windows = [window_summary(case.anomalous(), "anomaly", top_kinds)]
        for baseline in case.baselines():
            windows.append(window_summary(baseline, "baseline", top_kinds))
        digest = {"case_id": case.case_id, "anomaly": case.anomaly, "windows": windows,
                  "components": components_by_role(case),
                  "time_unit": "milliseconds since each run's start"}
        size = len(json.dumps(digest))
        if size <= MAX_DIGEST_BYTES or top_kinds <= 1:
            digest["digest_bytes"] = size
            return digest
        top_kinds = top_kinds // 2
