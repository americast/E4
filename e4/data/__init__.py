"""Dataset adaptation kit, BaseDatasetAdapter, and generic folder loader."""

from .adapter import (
    AdapterConformanceError,
    BaseDatasetAdapter,
    TelemetrySource,
    check_adapter_conformance,
)
from .agent_prompt import get_adapter_generation_prompt
from .folder import FolderTelemetrySource
from .submissions import (
    AdapterInputRequired,
    AdapterSubmissionError,
    adapter_code_template,
    adapter_submission_schema,
    load_adapter_submission,
    parse_adapter_submission,
    validate_adapter_config,
)

__all__ = [
    "TelemetrySource",
    "BaseDatasetAdapter",
    "FolderTelemetrySource",
    "AdapterConformanceError",
    "check_adapter_conformance",
    "get_adapter_generation_prompt",
    "AdapterSubmissionError",
    "AdapterInputRequired",
    "adapter_submission_schema",
    "adapter_code_template",
    "parse_adapter_submission",
    "validate_adapter_config",
    "load_adapter_submission",
]
