"""Prompt templates and statically checked reference API examples."""

from .examples import METRIC_ANOMALY_EXAMPLE, SCORED_CANDIDATE_EXAMPLE
from .initialization import load_initialization_resource, render_initialization_prompt
from .templates import (
    COMPOSE_TEMPLATE,
    EVOLVE_TEMPLATE,
    REPAIR_TEMPLATE,
    SELECT_TEMPLATE,
    SERVE_TEMPLATE,
    SYSTEM_PROMPT,
    format_compose_prompt,
    format_evolve_prompt,
    format_repair_prompt,
    format_select_prompt,
    format_serve_prompt,
)

__all__ = [
    "SYSTEM_PROMPT",
    "SELECT_TEMPLATE",
    "COMPOSE_TEMPLATE",
    "EVOLVE_TEMPLATE",
    "SERVE_TEMPLATE",
    "REPAIR_TEMPLATE",
    "METRIC_ANOMALY_EXAMPLE",
    "SCORED_CANDIDATE_EXAMPLE",
    "format_select_prompt",
    "format_compose_prompt",
    "format_evolve_prompt",
    "format_serve_prompt",
    "format_repair_prompt",
    "load_initialization_resource",
    "render_initialization_prompt",
]
