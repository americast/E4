"""Unit tests for the 10-line DiagnosisEngine public API and FolderTelemetrySource."""

import json
import tempfile
import unittest
from pathlib import Path

from e4 import (
    BaseDatasetAdapter,
    BudgetStopPolicy,
    DiagnosisEngine,
    FolderTelemetrySource,
    setup,
)
from e4.controller.library import LibraryManager, Playbook
from e4.registry import OperatorRegistry, OperatorSpec, PortSpec
from e4.sandbox.runner import InProcessRunner
from e4.types import RankedAnswer


class TestEngine(unittest.TestCase):
    def test_diagnosis_engine_runs_ten_line_example(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metrics_file = root / "metrics.csv"
            metrics_file.write_text(
                "timestamp,service_name,metric_name,value\n"
                "1000,auth,cpu,20.0\n"
                "2500,auth,cpu,95.0\n",
                encoding="utf-8",
            )
            mapping = {
                "windows": {
                    "reference": [0, 2000],
                    "alert": [2000, 3000],
                },
                "tables": [
                    {
                        "name": "metrics.csv",
                        "file": "metrics.csv",
                        "record_type": "metric",
                        "timestamp_col": "timestamp",
                        "entity_col": "service_name",
                        "metric_name_col": "metric_name",
                        "value_col": "value",
                    }
                ],
            }

            # 1. Telemetry source
            telemetry = FolderTelemetrySource(root, mapping=mapping)

            # 2. Engine setup with simple library
            reg = OperatorRegistry()
            reg.register(
                OperatorSpec(
                    name="simple.detector",
                    version="1",
                    inputs=(PortSpec("data", "Telemetry"),),
                    output_type="RankedAnswer",
                    implementation=lambda inp, p, r: RankedAnswer(candidates=(), stage="test"),
                )
            )
            lib = LibraryManager(registry=reg)
            lib.add_playbook(
                Playbook(
                    name="simple.playbook",
                    description="Simple test playbook",
                    dag={
                        "nodes": [
                            {
                                "id": "n1",
                                "operator": "simple.detector",
                                "version": "1",
                                "inputs": {"data": "telemetry"},
                            }
                        ],
                        "return_node": "n1",
                    },
                )
            )

            class MockModel:
                def complete(self, prompt: str) -> str:
                    return json.dumps({
                        "selected_playbook": "simple.playbook",
                        "parameter_bindings": {},
                    })

            engine = DiagnosisEngine(
                model=MockModel(),
                stopping=BudgetStopPolicy(max_attempts=1),
                library=lib,
                runner=InProcessRunner(),
            )

            # 3. Diagnose
            result = engine.diagnose(telemetry)
            self.assertEqual(result.stage, "select")
            self.assertEqual(result.status, "unverified")

    def test_setup_helper_creates_sample_mapping(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            setup(tmpdir)
            sample_file = Path(tmpdir) / "mapping.sample.json"
            self.assertTrue(sample_file.is_file())
            content = json.loads(sample_file.read_text(encoding="utf-8"))
            self.assertIn("windows", content)
            self.assertIn("tables", content)


if __name__ == "__main__":
    unittest.main()
