from __future__ import annotations
import importlib.util
import json
import shutil
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = Path(__file__).with_name("parallel_runner_v5.py")
SPEC = importlib.util.spec_from_file_location("parallel_runner_v5_under_test", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(runner)

class RunnerV4FailfastTests(unittest.TestCase):

    def test_actual_orchestrate_entrypoint_launches_chains_and_writes_final_receipt(self):
        import uuid
        run_root = runner.ARTIFACTS / f"test_orchestrate_entrypoint_{uuid.uuid4().hex}"
        run_root.mkdir(parents=True)
        self.addCleanup(lambda: shutil.rmtree(run_root, ignore_errors=True))
        audits = {}
        for seed in (42, 43):
            audit_dir = run_root / f"seed{seed}"
            audit_dir.mkdir()
            manifest_path = audit_dir / "audit_manifest.json"
            manifest_path.write_text("{}\n")
            audits[str(seed)] = {"audit_dir": manifest_path.parent.relative_to(ROOT).as_posix(),
                "audit_manifest_sha256": runner.sha(manifest_path)}
        rerun_path = run_root / "rerun_manifest.json"
        rerun_path.write_text("{}\n")
        service_map = run_root / "service_map.json"
        service_map.write_text("{}\n")
        plan = {"schema_version": runner.RUNNER_SCHEMA, "status": "prepared_no_model_calls",
            "runner_source_path": SCRIPT.relative_to(ROOT).as_posix(),
            "runner_source_sha256": runner.sha(SCRIPT),
            "rerun_manifest_path": rerun_path.relative_to(ROOT).as_posix(),
            "rerun_manifest_sha256": runner.sha(rerun_path),
            "audits": audits,
            "service_assignment": {"seed42": {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"},
                "seed43": {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"}}}
        (run_root / "parallel_manifest.json").write_text(json.dumps(plan) + "\n")
        launched = []
        class Proc:
            returncode = 0
            def poll(self): return 0
        def fake_task_process(_script, _run_root, _service_map, *, seed, shard, role):
            launched.append((seed, shard, role))
            audit_dir = run_root / f"seed{seed}"
            status_path = audit_dir / f"{shard}_status.json"
            status_path.write_text(json.dumps({"seed": seed, "shard": shard}) + "\n")
            receipt = {"status_path": status_path.relative_to(ROOT).as_posix()}
            (audit_dir / f"seed{seed}_{shard}_task_receipt.json").write_text(json.dumps(receipt) + "\n")
            return Proc()
        def fake_namespace_smoke(_service_map, smoke_root):
            smoke_root.mkdir(parents=True)
            receipt = {"status": "complete", "completion_requests_made": 0}
            (smoke_root / "namespace_smoke_suite.json").write_text(json.dumps(receipt) + "\n")
            return receipt
        def fake_combine(audit_dir, *_args):
            result = {"case_count": 10, "cumulative_top3_prefix_successes": 3}
            (Path(audit_dir) / "combined_bank10_result.json").write_text(json.dumps(result) + "\n")
            return result
        audit_module = types.SimpleNamespace(combine=fake_combine)
        self.assertEqual(plan["runner_source_sha256"], runner.sha(SCRIPT))
        with patch.dict(sys.modules, {"scripts.run_paper_starter_audit": audit_module}), \
             patch.object(runner, "_service_doc", side_effect=lambda _path, role: ({"service_role": role}, Path("proof"))), \
             patch.object(runner, "_probe_endpoint", return_value={"status": "ready"}), \
             patch.object(runner, "namespace_smoke", side_effect=fake_namespace_smoke), \
             patch.object(runner, "_run_task_process", side_effect=fake_task_process), \
             patch.object(runner, "_patch_receipt_validator"):
            record = runner.orchestrate(run_root, service_map)
        self.assertEqual({(seed, shard) for seed, shard, _ in launched},
            {(42, "local"), (42, "wingtip"), (43, "local"), (43, "wingtip")})
        self.assertEqual(len(launched), 4)
        self.assertEqual(record["status"], "complete")
        self.assertEqual(len(record["scheduled_tasks"]), 4)
        self.assertEqual(record["transport_smoke_completion_requests_made"], 0)
        self.assertTrue((run_root / "orchestration_result.json").is_file())
        self.assertEqual(json.loads((run_root / "orchestration_result.json").read_text()), record)

    def test_child_command_loads_final_runner_source_pinned_in_prepared_plan(self):
        child = runner._remote_child_command(Path("/tmp/audit"), "local", Path("/tmp/receipt"),
            {"profile_path": "artifacts/example/profile.json", "profile_sha256": "a" * 64}, 42)[-1]
        self.assertIn("parallel_runner_v5 import _patch_receipt_validator, _worker_transport_preflight, _install_failfast_case_guard", child)
        self.assertNotIn("parallel_runner_v4 import", child)
        self.assertNotIn("resume_from", child)
        self.assertNotIn("failed_attempt_accounted_and_chain_resumed", child)
        self.assertEqual(runner.sha(SCRIPT), runner.sha(runner.ROOT / SCRIPT.relative_to(ROOT)))


    def test_context_preflight_http_5xx_is_transport_failure(self):
        import tempfile
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def _run_case(self, case_id, index, input_library, shard_root, manifest, truth, shard):
                path = Path(shard_root) / f"case{index:02d}" / "diagnostic"
                path.mkdir(parents=True)
                call = {"error": {"code": "context_preflight_error",
                    "details": {"code": "http_status", "status": 503}}}
                (path / "model_calls.jsonl").write_text(json.dumps(call) + "\n")
                return {"case_id": case_id, "status": "failed"}
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", return_value=Response()):
                with self.assertRaises(runner.InferenceTransportFailure):
                    fake._run_case("case1", 1, Path(temp), Path(temp), {}, {}, "local")
            receipt = json.loads((Path(temp) / "transport_failure.json").read_text())
            self.assertEqual(receipt["reason"], "model_calls.jsonl:context_preflight_http_503")
            self.assertEqual(receipt["usage_status"], "unknown")

    def test_transport_error_recorded_in_calls_stops_before_next_case_with_health_200(self):
        import tempfile
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def __init__(self): self.calls = []
            def _run_case(self, case_id, index, input_library, shard_root, manifest, truth, shard):
                self.calls.append(case_id)
                case_root = Path(shard_root) / f"case{index:02d}" / "diagnostic"
                case_root.mkdir(parents=True)
                (case_root / "model_calls.jsonl").write_text(json.dumps({
                    "error": {"code": "transport_error", "message": "redacted"}}) + "\n")
                return {"case_id": case_id, "status": "failed"}
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", return_value=Response()):
                with self.assertRaises(runner.InferenceTransportFailure):
                    for index, case in enumerate(("case1", "case2"), 1):
                        fake._run_case(case, index, Path(temp), Path(temp), {}, {}, "local")
            self.assertEqual(fake.calls, ["case1"])
            receipt = json.loads((Path(temp) / "transport_failure.json").read_text())
            self.assertEqual(receipt["case_id"], "case1")
            self.assertEqual(receipt["usage_status"], "unknown")


    def test_completed_nonzero_worker_summary_is_validated_committed_once_and_chain_continues(self):
        import tempfile
        import types
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def __init__(self): self.calls = []; self.verified = []
            def snapshot_library(self, path): return types.SimpleNamespace(snapshot_id="input-snapshot")
            def _verify_case_summary(self, summary, expected):
                self.verified.append((summary["case_id"], expected))
            def _run_case(self, case_id, index, input_library, shard_root, manifest, truth, shard):
                self.calls.append(case_id)
                if index == 1:
                    summary = {"case_id": case_id, "worker_exit_code": 23,
                        "status": "failed", "accepted": False,
                        "input_library_snapshot_id": "input-snapshot"}
                    trusted = Path(shard_root) / "trusted"
                    trusted.mkdir(exist_ok=True)
                    (trusted / "case01_summary.json").write_text(json.dumps(summary) + "\n")
                    raise RuntimeError("worker failed after completed ordinary diagnostic")
                return {"case_id": case_id, "worker_exit_code": 0, "status": "complete"}
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        completed = []
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", return_value=Response()):
                for index, case in enumerate(("case1", "case2"), 1):
                    completed.append(fake._run_case(case, index, Path(temp), Path(temp), {}, {}, "local"))
        self.assertEqual(fake.calls, ["case1", "case2"])
        self.assertEqual(fake.verified, [("case1", "input-snapshot")])
        self.assertEqual(completed[0]["worker_exit_code"], 23)
        self.assertEqual(completed[0]["accepted"], False)

    def test_invalid_trusted_failure_summary_is_not_swallowed(self):
        import tempfile
        import types
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def snapshot_library(self, path): return types.SimpleNamespace(snapshot_id="input")
            def _verify_case_summary(self, summary, expected): return None
            def _run_case(self, case_id, index, input_library, shard_root, manifest, truth, shard):
                trusted = Path(shard_root) / "trusted"
                trusted.mkdir(exist_ok=True)
                (trusted / "case01_summary.json").write_text(json.dumps({
                    "case_id": "another-case", "worker_exit_code": 2}) + "\n")
                raise RuntimeError("worker failed")
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", return_value=Response()):
                with self.assertRaises(runner.RunnerError):
                    fake._run_case("case1", 1, Path(temp), Path(temp), {}, {}, "local")

    def test_valid_case_is_committed_before_next_case_health_check_fails(self):
        import tempfile
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def __init__(self): self.calls = []
            def _run_case(self, case_id, index, input_library, shard_root, manifest, truth, shard):
                self.calls.append(case_id)
                return {"case_id": case_id, "status": "complete", "accepted": True}
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        import itertools
        health = itertools.chain([Response()], [OSError("connection refused")])
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", side_effect=lambda *a, **k: next(health)):
                completed = fake._run_case("case1", 1, Path(temp), Path(temp), {}, {}, "local")
                with self.assertRaises(runner.InferenceTransportFailure):
                    fake._run_case("case2", 2, Path(temp), Path(temp), {}, {}, "local")
            self.assertEqual(completed["case_id"], "case1")
            self.assertEqual(fake.calls, ["case1"])
            receipt = json.loads((Path(temp) / "transport_failure.json").read_text())
            self.assertEqual(receipt["case_id"], "case2")
            self.assertEqual(receipt["usage_status"], "no_case_request_started")

    def test_r6_plan_has_two_physical_services_and_independent_seed_chains(self):
        source = SCRIPT.read_text()
        self.assertIn('"seed42": {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"}', source)
        self.assertIn('"seed43": {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"}', source)
        self.assertIn('"all_cases_new_for_this_method": False', source)
        self.assertNotIn('"dgx_v100"', source[source.index('def orchestrate'):])


    def test_failed_service_skips_only_its_dependent_seed_and_peer_drains(self):
        roles = {42: {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"},
            43: {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"}}
        class Proc:
            def __init__(self, result): self.returncode = result
            def poll(self): return self.returncode
        outcomes = {"42.local": 1, "42.wingtip": 0, "43.wingtip": 0}
        launched = []
        def launch_task(*, seed, shard, role):
            name = f"{seed}.{shard}"
            launched.append(name)
            return Proc(outcomes[name])
        status = runner._drain_two_service_chains(launch_task, roles)
        self.assertEqual(launched, ["42.local", "42.wingtip", "43.wingtip"])
        self.assertEqual(status["43.local"]["state"], "skipped_dependency_failed")
        self.assertEqual(status["43.wingtip"]["state"], "complete")

    def test_transport_outage_before_case_aborts_without_running_case(self):
        class FakeAudit:
            def _run_case(self, *args, **kwargs):
                raise AssertionError("case must not run")
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", side_effect=OSError("connection refused")):
                with self.assertRaises(runner.InferenceTransportFailure):
                    fake._run_case("case", 1, Path(temp), Path(temp), {}, {}, "local")
            receipt = json.loads((Path(temp) / "transport_failure.json").read_text())
            self.assertEqual(receipt["status"], "failed_infrastructure")
            self.assertEqual(receipt["usage_status"], "no_case_request_started")
            self.assertIsNone(receipt["reported_usage"])

    def test_ordinary_case_exception_is_not_reclassified_when_service_is_healthy(self):
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def _run_case(self, *args, **kwargs):
                raise ValueError("valid diagnostic failure")
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        with patch.object(runner.urllib.request, "urlopen", return_value=Response()):
            with self.assertRaisesRegex(ValueError, "valid diagnostic failure"):
                fake._run_case("case", 1, Path("."), Path("."), {}, {}, "local")


    def test_http_infrastructure_failure_aborts_even_if_health_recovers(self):
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def _run_case(self, *args, **kwargs):
                raise RuntimeError("inference HTTP 503 service unavailable")
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", return_value=Response()):
                with self.assertRaises(runner.InferenceTransportFailure):
                    fake._run_case("case", 1, Path(temp), Path(temp), {}, {}, "local")
            receipt = json.loads((Path(temp) / "transport_failure.json").read_text())
            self.assertEqual(receipt["usage_status"], "unknown")

    def test_outage_after_case_preserves_original_exception_and_marks_transport_failure(self):
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"status":"ready"}'
        class FakeAudit:
            def _run_case(self, *args, **kwargs):
                raise OSError("connection closed")
        fake = FakeAudit()
        runner._install_failfast_case_guard(fake)
        health = [Response(), OSError("connection closed") ]
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(runner.urllib.request, "urlopen", side_effect=health):
                with self.assertRaises(runner.InferenceTransportFailure):
                    fake._run_case("case", 1, Path(temp), Path(temp), {}, {}, "local")
            receipt = json.loads((Path(temp) / "transport_failure.json").read_text())
            self.assertEqual(receipt["usage_status"], "unknown")
            self.assertIsNone(receipt["reported_usage"])

if __name__ == "__main__":
    unittest.main()
