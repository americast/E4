"""Offline regression tests for the one-case M3 development path."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from e4core.benchmark import REPAIR_LIMIT, run_task, runtime_context
from e4core.contracts import EntityRef, MetricPoint, Span, Telemetry, Window
from e4core.development import run_development_dag, select_primary_kpi
from e4core.telecom_ops import metric_seed_dag


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/run_m3_development.py"


def _telemetry(*, roots: bool = False, app: bool = False) -> Telemetry:
    reference = Window("reference", 0, 1_800_000)
    alert = Window("alert", 1_800_000, 3_600_000)
    metrics = []
    for window in (reference, alert):
        if app:
            for minute in range(12):
                timestamp = window.start_ms_utc + minute * 120_000
                values = (10.0, 30.0) if minute == 0 else (20.0,)
                for duplicate, value in enumerate(values):
                    metrics.append(MetricPoint(
                        "metric/metric_app.csv", timestamp, EntityRef("service", "svc_api"),
                        "avg_time", "metric_app", "avg_time", value, str(value),
                        f"{window.name}-{minute}-{duplicate}"))
                metrics.append(MetricPoint(
                    "metric/metric_app.csv", timestamp, EntityRef("service", "svc_api"),
                    "num", "metric_app", "num", 100.0, "100", f"{window.name}-{minute}-num"))
    for window in (reference, alert):
        for index in range(12):
            timestamp = window.start_ms_utc + index * 120_000
            metrics.append(MetricPoint(
                "metric/metric_node.csv", timestamp, EntityRef("component", "node_app_01"),
                "CPU_util_pct", "node", "cpu", 10.0 if window.name == "reference" else 90.0,
                "10", f"{window.name}-{index}"))
    spans = []
    if roots:
        for window in (reference, alert):
            for index in range(12):
                trace_id = f"{window.name}-trace-{index}"
                span = Span(trace_id, "root", None, window.start_ms_utc + index * 120_000,
                            "100" if window.name == "reference" else "200", True, "valid", "true",
                            "OSB", EntityRef("component", "node_app_01"),
                            EntityRef("service", "svc_api"), None, False, False)
                spans.append(span)
                if index == 0:
                    # Repeated root rows for one trace must not double KPI support.
                    spans.append(span)
    return Telemetry("dev-synthetic", reference, alert, tuple(metrics), tuple(spans),
                     {"node_app_01": {"identifier": "node_app_01"}}, "a" * 64,
                     {"fixture.csv": "b" * 64}, {}, {}, "test/1", {"fixture.csv": 123})


class FakeCall:
    def __init__(self, content: str | None = None, *, error=None, finish_reason=None):
        self.call_id = "fake-call-1"
        self.content = content
        self.error = error
        self.response_metadata = {"finish_reason": finish_reason}
        self.elapsed_seconds = 0.01
        self.usage = {"prompt_tokens": 3, "completion_tokens": 4}

    def as_dict(self):
        return {"call_id": self.call_id, "content": self.content, "error": self.error,
                "response_metadata": self.response_metadata, "elapsed_seconds": self.elapsed_seconds,
                "usage": self.usage}


class QueueClient:
    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.calls = []

    def complete(self, prompt, *, task_id, attempt, max_tokens=None):
        self.calls.append({"prompt": prompt, "task_id": task_id, "attempt": attempt})
        return self.outputs.pop(0)


def _rename_source(document: dict, source_id: str) -> dict:
    document = json.loads(json.dumps(document))
    for node in document["nodes"]:
        if node["operator"] == "case.telemetry":
            old = node["id"]
            node["id"] = source_id
            for child in document["nodes"]:
                child["inputs"] = {port: ([source_id if ref == old else ref for ref in refs]
                                            if isinstance(refs, list) else
                                            source_id if refs == old else refs)
                                   for port, refs in child["inputs"].items()}
            return document
    raise AssertionError("seed DAG had no telemetry source")


class DevelopmentRunTests(unittest.TestCase):
    def _fixture_root(self, base: Path) -> tuple[Path, Path]:
        root = base / "project"
        config_dir = root / "config"
        config_dir.mkdir(parents=True)
        for filename in ("telecom_input_manifest.json", "dev_kpi_policy.json"):
            (config_dir / filename).write_bytes((ROOT / "config" / filename).read_bytes())
        case_dir = root / "data/prepared/telecom/public/cases/dev-synthetic"
        case_dir.mkdir(parents=True)
        alert = {"incident_id": "dev-synthetic", "alert_text": "synthetic alert",
                 "timezone": "Asia/Shanghai", "analysis_window_start_epoch_ms_utc": 0,
                 "alert_window_start_epoch_ms_utc": 1_800_000,
                 "alert_window_end_epoch_ms_utc": 3_600_000}
        (case_dir / "alert.json").write_text(json.dumps(alert))
        catalog_path = root / "data/prepared/telecom/public/deployment_catalog.json"
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        catalog_path.write_text(json.dumps({"entities": {"node_app_01": {"identifier": "node_app_01"}}}))
        profile_path = base / "profile.json"
        profile_path.write_text(json.dumps({"model_alias": "fake"}))
        return root, profile_path

    def test_cli_help_is_available_from_project_root(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "--help"], cwd=ROOT,
                                 capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--case-id", result.stdout)
        self.assertIn("--profile", result.stdout)

    def test_existing_e4_run_dir_uses_unique_development_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            parent = base / "launcher-run"
            parent.mkdir()
            module_spec = importlib.util.spec_from_file_location("run_m3_development_test", SCRIPT)
            module = importlib.util.module_from_spec(module_spec)
            assert module_spec and module_spec.loader
            module_spec.loader.exec_module(module)
            captured = {}

            def fake_run(case_id, split, *, profile_path, run_dir, root):
                captured.update(case_id=case_id, split=split, profile_path=profile_path,
                                run_dir=run_dir, root=root)
                return {"selected_kpi": {"kpi_id": None}, "cache_hits_warm": 4,
                        "wall_seconds_including_load_and_model_call": 0.1}

            with patch.dict(os.environ, {"E4_RUN_DIR": str(parent)}), \
                    patch.object(sys, "argv", [str(SCRIPT), "--case-id", "dev-synthetic",
                        "--profile", str(base / "profile.json"), "--run-id", "unique-1"]), \
                    patch.object(module, "run_development_dag", fake_run):
                self.assertEqual(module.main(), 0)
            self.assertEqual(captured["run_dir"], parent / "development")
            self.assertTrue(parent.is_dir())

    def test_development_run_discovers_arbitrary_external_source_and_replays_same_telemetry(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root, profile_path = self._fixture_root(base)
            run_dir = base / "run"
            telemetry = _telemetry()
            output = json.dumps(_rename_source(metric_seed_dag(), "telemetry_input"))
            client = QueueClient([FakeCall("{"), FakeCall(output)])
            calls = {"loader": 0}

            def load(*args, **kwargs):
                calls["loader"] += 1
                return telemetry

            with patch("e4core.development.load_case", side_effect=load), \
                    patch("e4core.development.build_case_runtime", return_value=runtime_context()), \
                    patch("e4core.development.LocalModelClient", return_value=client):
                result = run_development_dag("dev-synthetic", "dev", profile_path=profile_path,
                                             run_dir=run_dir, root=root)
            self.assertEqual(calls["loader"], 1)
            self.assertEqual(result["cold_execution"]["answer"], result["warm_execution"]["answer"])
            self.assertEqual(result["cold_execution"]["answer"],
                             result["cache_disabled_execution"]["answer"])
            self.assertGreater(result["cache_hits_warm"], result["cache_hits_cold"])
            manifest = json.loads((run_dir / "dev_manifest.json").read_text())
            self.assertEqual(manifest["static_repairs"], 1)
            self.assertEqual([item["attempt"] for item in client.calls], [1, 2])
            calls = [json.loads(line) for line in (run_dir / "model_calls.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 2)
            self.assertTrue((run_dir / "development_run.json").is_file())

    def test_failed_model_call_keeps_manifest_call_and_structured_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root, profile_path = self._fixture_root(base)
            run_dir = base / "run"
            telemetry = _telemetry()
            call = FakeCall(error={"code": "transport_error", "message": "offline fixture"})

            def fail_after_manifest(*args, **kwargs):
                self.assertTrue((run_dir / "dev_manifest.json").is_file())
                return call

            client = SimpleNamespace(complete=fail_after_manifest)
            with patch("e4core.development.load_case", return_value=telemetry), \
                    patch("e4core.development.build_case_runtime", return_value=runtime_context()), \
                    patch("e4core.development.LocalModelClient", return_value=client):
                with self.assertRaisesRegex(RuntimeError, "model request failed"):
                    run_development_dag("dev-synthetic", "dev", profile_path=profile_path,
                                        run_dir=run_dir, root=root)
            manifest = json.loads((run_dir / "dev_manifest.json").read_text())
            self.assertEqual(manifest["telemetry_load"]["data_sha256"], telemetry.data_sha256)
            self.assertTrue(manifest["telemetry_load"]["file_sha256"])
            calls = [json.loads(line) for line in (run_dir / "model_calls.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["error"]["code"], "transport_error")
            failure = json.loads((run_dir / "phase_status.json").read_text())
            self.assertEqual(failure["phase"], "model_call")
            self.assertEqual(failure["status"], "failed")
            self.assertEqual(failure["error"]["code"], "transport_error")
            self.assertEqual(failure["error"]["message"], "offline fixture")


class KPIAndRepairTests(unittest.TestCase):
    def test_primary_kpi_deduplicates_root_trace_ids_and_medians_duplicate_app_rows(self):
        root_kpi = select_primary_kpi(_telemetry(roots=True))
        self.assertEqual(root_kpi["kpi_id"], "root_osb_elapsedTime_mean")
        self.assertEqual(root_kpi["windows"]["reference"]["support"], 12)
        self.assertEqual(root_kpi["windows"]["alert"]["support"], 12)

        app_kpi = select_primary_kpi(_telemetry(app=True))
        self.assertEqual(app_kpi["kpi_id"], "app_avg_time_mean")
        self.assertEqual(app_kpi["windows"]["reference"]["support"], 12)
        # The repeated first timestamp contains 10 and 30; median-collapsing it
        # gives 20, so all twelve bucket medians and their mean are 20.
        self.assertEqual(app_kpi["windows"]["reference"]["mean"], 20.0)
        self.assertEqual(app_kpi["windows"]["alert"]["mean"], 20.0)

    def test_only_two_static_repairs_are_recorded_and_semantic_error_is_not_retried(self):
        task = {"id": "schema-static", "kind": "schema", "prompt": "emit enabled true",
                "check": {"exact_keys": ["enabled"], "expected": {"enabled": True}}}
        invalid = [FakeCall('{"wrong": 1}') for _ in range(REPAIR_LIMIT + 1)]
        client = QueueClient(invalid)
        with tempfile.TemporaryDirectory() as temporary:
            outcome = run_task(task, registry=None, runtime=runtime_context(), client=client,
                               run_dir=Path(temporary))
        self.assertEqual(outcome.repairs, REPAIR_LIMIT)
        self.assertEqual(len(outcome.calls), REPAIR_LIMIT + 1)
        self.assertEqual([item["attempt"] for item in client.calls], [1, 2, 3])
        self.assertEqual(len(outcome.repair_errors), REPAIR_LIMIT + 1)
        self.assertFalse(outcome.passed)

        semantic_task = {"id": "schema-semantic", "kind": "schema", "prompt": "emit enabled true",
                         "check": {"exact_keys": ["enabled"], "expected": {"enabled": True}}}
        semantic_client = QueueClient([FakeCall('{"enabled": false}'), FakeCall('{"enabled": true}')])
        with tempfile.TemporaryDirectory() as temporary:
            semantic = run_task(semantic_task, registry=None, runtime=runtime_context(),
                                client=semantic_client, run_dir=Path(temporary))
        self.assertEqual(semantic.repairs, 0)
        self.assertEqual(len(semantic.calls), 1)
        self.assertEqual([item["attempt"] for item in semantic_client.calls], [1])
        self.assertEqual(semantic.error["code"], "schema_value_mismatch")


if __name__ == "__main__":
    unittest.main()
