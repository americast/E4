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
from .onboarding import prepare_library
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
        base_registry: Any = None,
        registry: Any = None,
        runner: SandboxRunner | None = None,
        max_attempts: int = 3,
        max_repairs: int = 2,
        library_mode: str | None = None,
    ):
        self.model = model
        if library_mode not in {None, "paper", "clean_slate"}:
            raise ValueError("library_mode must be 'paper' or 'clean_slate'")
        eff_max_attempts = max_attempts
        chosen_oracle = oracle or stopping
        if chosen_oracle is not None and hasattr(chosen_oracle, "max_attempts"):
            try:
                oracle_limit = int(getattr(chosen_oracle, "max_attempts"))
                eff_max_attempts = min(max_attempts, oracle_limit)
            except (ValueError, TypeError):
                pass
        self.oracle = chosen_oracle or BudgetStopPolicy(max_attempts=eff_max_attempts)
        base_reg = registry or base_registry
        self._pending_default_library = False
        explicit_library_inputs = library is not None or base_reg is not None
        if isinstance(library, (Path, str)):
            self.library = LibraryManager.load_from_dir(
                library,
                base_registry=base_reg,
                mode=library_mode,
            )
        elif isinstance(library, LibraryManager):
            self.library = library
            if library_mode is not None:
                self.library = prepare_library(None, mode=library_mode, library=library)
            if base_reg:
                if self.library.mode == "clean_slate":
                    # Validate the supplied base on a temporary manager before
                    # registering any operators into the caller's library.
                    prepare_library(
                        None,
                        mode="clean_slate",
                        library=LibraryManager(
                            registry=base_reg,
                            mode="clean_slate",
                            origin="user-supplied-base",
                        ),
                    )
                    self.library = prepare_library(None, mode="clean_slate", library=self.library)
                specs = (
                    base_reg.all_operators()
                    if hasattr(base_reg, "all_operators")
                    else (base_reg.list() if hasattr(base_reg, "list") else list(base_reg))
                )
                for s in specs:
                    if not self.library.registry.has(s.name, s.version):
                        self.library.registry.register(s)
        elif library_mode == "clean_slate":
            self.library = LibraryManager(
                registry=base_reg,
                mode="clean_slate",
                origin="user-clean-slate",
            )
            self.library = prepare_library(None, mode="clean_slate", library=self.library)
        elif library_mode == "paper":
            if base_reg is not None:
                raise ValueError(
                    "library_mode='paper' cannot be combined with an explicit base registry; "
                    "prepare a paper LibraryManager first if you need custom base operators"
                )
            self.library = LibraryManager(mode="paper", origin="pending-paper-default")
            self._pending_default_library = True
        elif base_reg:
            self.library = LibraryManager(registry=base_reg)
        elif not explicit_library_inputs:
            self.library = LibraryManager(mode="paper", origin="pending-paper-default")
            self._pending_default_library = True
        else:
            self.library = LibraryManager()

        self.runner = runner or SubprocessRunner()
        self.controller = DiagnosisController(
            library=self.library,
            oracle=self.oracle,
            runner=self.runner,
            max_attempts=eff_max_attempts,
            max_repairs=max_repairs,
        )

    def diagnose(self, telemetry: TelemetrySource, case_id: str = "case_01") -> DiagnosticResult:
        """Execute the 3-stage diagnostic loop over telemetry data."""
        if self._pending_default_library:
            schema = telemetry.get_schema()
            self.library = prepare_library(schema, mode="paper")
            self.controller.library = self.library
            self._pending_default_library = False
        if self.library.mode == "clean_slate" and (
            not self.library.registry.all_operators() or not self.library.list_playbooks()
        ):
            raise ValueError(
                "Clean-slate library is empty; generate and validate initial operators and playbooks "
                "before diagnosing incidents"
            )
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
