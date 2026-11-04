"""High-level, friendly public API for the E4 Root-Cause Analysis engine."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .controller.controller import DiagnosisController
from .controller.library import LibraryManager
from .data.adapter import BaseDatasetAdapter, TelemetrySource
from .data.agent_prompt import get_adapter_generation_prompt
from .data.folder import FolderTelemetrySource
from .oracle import AcceptanceOracle, BudgetStopPolicy, GroundTruthOracle, HumanInTheLoopOracle
from .sandbox.runner import SandboxRunner, SubprocessRunner
from .types import DiagnosticResult


class DiagnosisEngine:
    """The central Root-Cause Analysis engine for external adopters."""

    def __init__(
        self,
        model: Any,
        stopping: AcceptanceOracle | None = None,
        oracle: AcceptanceOracle | None = None,
        library: LibraryManager | Path | str | None = None,
        runner: SandboxRunner | None = None,
        max_attempts: int = 3,
        max_repairs: int = 2,
    ):
        self.model = model
        self.oracle = oracle or stopping or BudgetStopPolicy(max_attempts=max_attempts)
        if isinstance(library, (Path, str)):
            self.library = LibraryManager.load_from_dir(library)
        elif isinstance(library, LibraryManager):
            self.library = library
        else:
            self.library = LibraryManager()

        self.runner = runner or SubprocessRunner()
        self.controller = DiagnosisController(
            library=self.library,
            oracle=self.oracle,
            runner=self.runner,
            max_attempts=max_attempts,
            max_repairs=max_repairs,
        )

    def diagnose(self, telemetry: TelemetrySource, case_id: str = "case_01") -> DiagnosticResult:
        """Execute the 3-stage diagnostic loop over telemetry data."""
        return self.controller.run_diagnostic(telemetry, self.model, case_id=case_id)


def setup(target_dir: Path | str = ".") -> None:
    """Quick-start helper to initialize boilerplate files for a new telemetry dataset."""
    root = Path(target_dir)
    root.mkdir(parents=True, exist_ok=True)
    mapping_sample = {
        "windows": {
            "reference": [1614902400000, 1614904200000],
            "alert": [1614904200000, 1614906000000],
        },
        "tables": [
            {
                "name": "metrics.csv",
                "file": "metrics.csv",
                "record_type": "metric",
                "timestamp_col": "timestamp",
                "timestamp_unit": "ms",
                "entity_col": "service_name",
                "entity_role": "service",
                "metric_name_col": "metric_name",
                "value_col": "value",
            }
        ],
    }
    sample_path = root / "mapping.sample.json"
    if not sample_path.exists():
        import json
        sample_path.write_text(json.dumps(mapping_sample, indent=2), encoding="utf-8")
        print(f"Generated sample telemetry mapping: {sample_path}")
    print("E4 initialized. Use 'get_adapter_generation_prompt()' to generate a custom adapter.")
