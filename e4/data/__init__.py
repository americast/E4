"""Dataset adaptation kit, BaseDatasetAdapter, and generic folder loader."""

from .adapter import (
    AdapterConformanceError,
    BaseDatasetAdapter,
    TelemetrySource,
    check_adapter_conformance,
)
from .agent_prompt import get_adapter_generation_prompt
from .folder import FolderTelemetrySource

__all__ = [
    "TelemetrySource",
    "BaseDatasetAdapter",
    "FolderTelemetrySource",
    "AdapterConformanceError",
    "check_adapter_conformance",
    "get_adapter_generation_prompt",
]
