"""AI Agent prompt generator for creating E4 dataset adapters from sample telemetry."""

from __future__ import annotations


def get_adapter_generation_prompt(sample_data_text: str = "") -> str:
    """Generate the AI prompt for an assistant to implement a new BaseDatasetAdapter."""
    sample_section = sample_data_text.strip() or "[PASTE 5-10 ROWS OF YOUR METRIC/SPAN/LOG HEADERS AND ALERT METADATA HERE]"
    return f"""### AI Prompt: Generate E4 Dataset Adapter

You are an expert systems engineer. I want to adapt the E4 Root-Cause Analysis framework to my observability dataset.

E4 requires a single adapter file implementing `BaseDatasetAdapter` with 4 placeholder functions:
1. `get_schema()`: Declares tables, typed fields, units, nullability, availability, and entity roles.
2. `get_windows()`: Extracts normal baseline (`reference`) and incident (`alert`) time intervals as UTC epoch milliseconds.
3. `extract_suspect_entities()`: Discovers unique services/components to rank as `[EntityRef(role, identifier)]`.
4. `stream_records(table_name, window)`: Streams canonical typed metric (`MetricRecord`), span (`SpanRecord`), or log (`LogRecord`) records.

Here is a sample of my telemetry data:
---
{sample_section}
---

Please implement `adapters/my_custom_adapter.py` by filling in all 4 placeholder functions:
- Handle timestamp parsing into UTC epoch milliseconds.
- Extract the service/component names as `EntityRef(role, identifier)`.
- Use the shared E4 types from `e4.types`; do not redeclare them or return `dict[str, Any]`.
- Require explicit timestamp units/timezone and identity mappings when not declared by the source; preserve unknown units and missing values rather than guessing.
- Preserve source references and distinguish unavailable tables from available empty tables.
- Efficiently stream rows within the requested window without loading entire multi-gigabyte files into memory.
- Keep the code self-contained and clean with type annotations.
- Include adapter conformance checks using `check_adapter_conformance(adapter)` from `e4.data.adapter`.
"""
