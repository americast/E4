"""Adapter-first prompt generator for new telemetry domains."""

from __future__ import annotations

import json
import inspect
import re
from importlib import resources


def get_adapter_generation_prompt(
    sample_data_text: str = "",
    *,
    domain_brief: str = "",
    public_metadata_text: str = "",
) -> str:
    """Build the strict JSON adapter-submission prompt from packaged resources."""
    from .submissions import adapter_code_template, adapter_submission_schema
    from e4.types import (
        ColumnSchema,
        DatasetSchema,
        EntityRef,
        LogRecord,
        MetricRecord,
        SpanRecord,
        TableSchema,
        Window,
    )

    template = (
        resources.files("e4.prompts")
        .joinpath("resources", "adapter_system.md")
        .read_text(encoding="utf-8")
    )
    substitutions = {
        "DOMAIN_BRIEF": domain_brief.strip() or "No domain/task prose supplied yet.",
        "PUBLIC_METADATA": public_metadata_text.strip()
        or "No public metadata supplied yet. A correct submission may request essential missing facts.",
        "SAMPLE_DATA": sample_data_text.strip() or "No sample rows supplied yet.",
        "RESPONSE_SCHEMA": json.dumps(
            adapter_submission_schema(), separators=(",", ":"), ensure_ascii=False
        ),
        "ADAPTER_TEMPLATE": adapter_code_template(),
        "CANONICAL_TYPE_REFERENCE": "```python\n"
        + "\n".join(
            f"{canonical_type.__name__}{inspect.signature(canonical_type)}"
            for canonical_type in (
                DatasetSchema,
                TableSchema,
                ColumnSchema,
                Window,
                EntityRef,
                MetricRecord,
                SpanRecord,
                LogRecord,
            )
        )
        + "\n```",
    }
    return re.sub(
        r"\{(DOMAIN_BRIEF|PUBLIC_METADATA|SAMPLE_DATA|RESPONSE_SCHEMA|ADAPTER_TEMPLATE|CANONICAL_TYPE_REFERENCE)\}",
        lambda match: substitutions[match.group(1)],
        template,
    )
