"""Public Bank incident task card for the unmodified Terminus-2 agent."""

from __future__ import annotations

import json
from typing import Any


def build_bank_instruction(view: Any) -> str:
    """Render only one selected incident's public task context and data paths."""
    public = view.as_public_dict()
    schemas = {
        name: list(columns) for name, columns in view.telemetry_schema.items()
    }
    telemetry = "\n".join(
        f"- /data/{relative} (SHA-256 {view.telemetry_file_sha256[relative]})"
        for relative in sorted(view.telemetry_files)
    )
    context = {
        "case_id": view.case_id,
        "timezone": view.timezone,
        "alert_window_start_local": view.alert_window_start,
        "alert_window_end_local": view.alert_window_end,
        "reference_window_utc_ms": list(view.reference_window_utc_ms),
        "alert_window_utc_ms": list(view.alert_window_utc_ms),
        "telemetry_dates": list(view.telemetry_dates),
        "alert_text": view.alert_text,
        "component_ids_observed_in_selected_case": list(view.component_ids),
        "official_candidate_components": list(view.official_candidate_components),
        "kpi_context": dict(view.kpi_context),
        "source_date_timestamp_unit_mapping": dict(
            getattr(view, "source_date_timestamp_units", {})
        ),
        "source_data_interpretation": {
            "metric_app_tc": "opaque KPI identity; it is not a component identity",
            "trace_span": "contains no source success/failure or call-type field",
            "trace_duration": "absolute unit is undeclared in the source file",
            "component_aliases": "no verified named-pod or Docker alias mapping is supplied",
            "topology": "no verified vertical component topology is supplied",
            "log_value": "raw log text; derived text analysis is allowed",
        },
    }
    return f"""Investigate this single OpenRCA Bank incident using terminal-based analysis.

Your task is to rank the most likely faulty component identities from the provided
incident data. You may use Python, pandas, NumPy, SciPy, shell tools, and any analysis
you can perform from these files. The CSV data is inside `/data`; it contains only this
incident's public telemetry. Do not assume access to other incidents or outside services.

Incident context:
```json
{json.dumps(context, ensure_ascii=False, indent=2, sort_keys=True)}
```

CSV schemas:
```json
{json.dumps(schemas, ensure_ascii=False, indent=2, sort_keys=True)}
```

Selected telemetry files:
{telemetry}

The exact public alert record is available at `/data/alert.json`; use its source-date
timestamp unit mapping when interpreting timestamps. It contains only the selected case.

Submission protocol:
1. Prepare a JSON file with exactly this shape: `{{"ranked_components":["component-id-1","component-id-2","component-id-3"]}}`.
   It may contain one, two, or three distinct component IDs, ordered from most to least likely.
2. Submit it by running `submit_diagnosis answer.json` from the terminal. You may also
   pipe JSON to `submit_diagnosis -`.
3. Each call to `submit_diagnosis` is one diagnosis opportunity; terminal commands and
   model turns do not consume opportunities. You have at most three submissions.
4. The command returns exactly one boolean: `{{"accepted":true}}` or
   `{{"accepted":false}}`. This reports only whether the correct component is in your
   first three IDs. It does not identify the target or disclose the correct rank.
5. On `accepted=true`, stop diagnosis work. On `accepted=false`, use your retained
   analysis to form a different ranked list and submit again. Do not repeat a prior list.

Use exact component identities from the selected task's public component list. The final
answer is determined only by the ranked list submitted through the command. Preserve
your analysis in the terminal session; no separate report is required.
"""
