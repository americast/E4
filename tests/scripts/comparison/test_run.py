import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.comparison.run import (_incomplete_job_keys, _load_completed_job,
                                    _resume_e4_library, _command_for,
                                    _preflight_development, _trusted_socket_path,
                                    UNIX_SOCKET_PATH_MAX_BYTES)


class OrchestrationCheckpointTests(unittest.TestCase):
    def test_trusted_socket_path_stays_short_under_long_run_directory(self):
        run_root = Path(
            "/raid/ssinha97/E4/artifacts/m3/model_runs/"
            "bank_m5_dev_readiness_20261017_02/m5_development")
        socket_path = _trusted_socket_path(run_root)
        self.assertEqual(socket_path.parent, run_root.resolve())
        self.assertEqual(socket_path.name, "s")
        self.assertLessEqual(len(os.fsencode(str(socket_path))), UNIX_SOCKET_PATH_MAX_BYTES)

    def test_trusted_socket_path_rejects_too_long_run_root_before_bind(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / ("x" * 110)
            with self.assertRaisesRegex(ValueError, "too long for a trusted Unix socket"):
                _trusted_socket_path(run_root)

    def test_development_preflight_does_not_load_evaluator_truth(self):
        manifest = {"case_ids": ["dev-a", "dev-b"]}
        with patch("scripts.comparison.run.validate_development_manifest",
                   return_value=manifest), \
             patch("scripts.comparison.run.load_development_truth_ids",
                   side_effect=AssertionError("preflight must not open development truth")), \
             patch("scripts.comparison.run.load_scored_truth_ids",
                   side_effect=AssertionError("preflight must not open scored truth")):
            observed, case_ids = _preflight_development(Path("provisional.json"), Path("root"))
        self.assertIs(observed, manifest)
        self.assertEqual(case_ids, ["dev-a", "dev-b"])

    def test_development_pair_command_passes_one_staged_path_and_pending_budget_defaults(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            task_root = Path("/run/public_cases/case-a")
            manifest = root / "dev_manifest.json"
            manifest.write_text("provisional", encoding="utf-8")
            command = _command_for("Terminus-2", case_id="case-a", split="dev",
                job_root=Path("/run/jobs/case-a/Terminus-2/attempt-01"),
                output_root=Path("/run/jobs/case-a/Terminus-2/attempt-01/output"),
                task_root=task_root, socket_path=Path("/run/eval.sock"),
                manifest_path=manifest, library_root=root / "library",
                harbor_python=root / "harbor-python", root=root,
                budgets={"case_wall_seconds": 7200,
                         "aggregate_generated_tokens": 73728,
                         "terminus_max_turns": None})
        self.assertIn("--split", command)
        self.assertEqual(command[command.index("--split") + 1], "dev")
        self.assertEqual(command[command.index("--task-view-root") + 1], str(task_root))
        self.assertEqual(command[command.index("--max-turns") + 1], "32")
        self.assertEqual(command[command.index("--generated-token-budget") + 1], "73728")
        self.assertEqual(command[command.index("--case-wall-seconds") + 1], "7200")

    def test_completed_diagnostic_failure_is_reused_without_another_attempt(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "case_result.json"
            path.write_text(json.dumps({"case_id": "case-a", "status": "exhausted_or_failed",
                                        "attempts": [1, 2, 3]}), encoding="utf-8")
            job = {"status": "complete", "result_path": str(path),
                   "result_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            result = _load_completed_job(job, system="E4", case_id="case-a")
            self.assertEqual(result["status"], "exhausted_or_failed")
            interrupted = {"status": "infrastructure_interrupted"}
            self.assertIsNone(_load_completed_job(interrupted, system="E4", case_id="case-a"))

    def test_resume_keeps_e4_library_order_when_pair_execution_alternates(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            first_store = base / "case-a-library"
            latest_store = base / "case-c-library"
            first_store.mkdir()
            latest_store.mkdir()
            first_snapshot_file = first_store / "first.json"
            latest_snapshot_file = latest_store / "latest.json"
            snapshots = {
                first_store: SimpleNamespace(snapshot_id="first", parent_snapshot_id="initial",
                    path=first_snapshot_file, document={"lineage": []}),
                first_store / "initial.json": SimpleNamespace(snapshot_id="initial",
                    parent_snapshot_id=None, path=first_store / "initial.json", document={"lineage": []}),
                latest_store: SimpleNamespace(snapshot_id="latest", parent_snapshot_id="first",
                    path=latest_snapshot_file, document={"lineage": []}),
                latest_store / "first.json": SimpleNamespace(snapshot_id="first",
                    parent_snapshot_id="initial", path=latest_store / "first.json", document={"lineage": []}),
                latest_store / "initial.json": SimpleNamespace(snapshot_id="initial",
                    parent_snapshot_id=None, path=latest_store / "initial.json", document={"lineage": []}),
            }
            jobs = {"case-a": {"E4": {"status": "complete", "library_root": str(first_store)}},
                    "case-b": {"E4": {"status": "complete", "library_root": None},
                               "Terminus-2": {"status": "complete"}},
                    "case-c": {"E4": {"status": "complete", "library_root": str(latest_store)}}}
            with patch("scripts.comparison.run.load_library_snapshot",
                       side_effect=lambda path: snapshots[Path(path)]):
                result = _resume_e4_library(jobs, ["case-a", "case-b", "case-c"],
                                             base / "initial", "initial")
            self.assertEqual(result, latest_store)

    def test_resume_rejects_unrelated_library_lineage(self):
        with tempfile.TemporaryDirectory() as temporary:
            unrelated_path = Path(temporary) / "unrelated"
            unrelated_path.mkdir()
            snapshot_file = unrelated_path / "unrelated.json"
            snapshots = {unrelated_path: SimpleNamespace(
                snapshot_id="unrelated", parent_snapshot_id="elsewhere",
                path=snapshot_file, document={"lineage": []})}
            jobs = {"case-a": {"E4": {"status": "complete",
                                       "library_root": str(unrelated_path)}}}
            with patch("scripts.comparison.run.load_library_snapshot",
                       side_effect=lambda path: snapshots[Path(path)]):
                with self.assertRaises(KeyError):
                    _resume_e4_library(jobs, ["case-a"], unrelated_path.parent / "initial", "initial")

    def test_missing_infrastructure_result_keeps_run_incomplete(self):
        cases = {"E4": {"case-a": {"attempts": []}}, "Terminus-2": {}}
        self.assertEqual(_incomplete_job_keys(["case-a", "case-b"], cases),
                         [("case-b", "E4"), ("case-a", "Terminus-2"),
                          ("case-b", "Terminus-2")])


if __name__ == "__main__":
    unittest.main()
