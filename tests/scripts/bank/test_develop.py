import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from e4.operators.bank.operators import bank_registry
from e4.contracts import Telemetry, Window
from e4.dsl.verifier import verify_dag
from e4.dsl.executor import ExecutionFailure, SerialExecutor
from e4.dsl.cache import CacheStore
from e4.dsl.runtime import build_case_runtime
from e4.agent.model_client import ModelCall
from scripts.bank import develop as runner
from tests.adapters.test_bank import make_bank_fixture


ROOT = Path(__file__).resolve().parents[3]


class BankDevelopmentRunnerTests(unittest.TestCase):
    class FakeClient:
        base_url = "http://127.0.0.1:8080"
        context_tokens = 32768
        max_generation_tokens = 8192

        def __init__(self, answers):
            self.answers = list(answers)
            self.prompts = []

        def complete(self, prompt, *, task_id, attempt):
            self.prompts.append(prompt)
            content = self.answers.pop(0)
            now = datetime.now(timezone.utc).isoformat()
            return ModelCall(f"fake-{attempt}", task_id, attempt, now, now, 0.01, 0.005,
                10, self.context_tokens, (), {"messages": [{"role": "user", "content": prompt}]},
                "prompt-sha", 200, json.dumps({"choices": [{"message": {"content": content}}]}),
                {"choices": [{"message": {"content": content}}]}, content, None,
                {"prompt_tokens": 10, "completion_tokens": 20},
                {"finish_reason": "stop"}, None)

    def _fixture_root(self, root):
        case_id, manifest_path = make_bank_fixture(root)
        sample_path = root / "config/bank/sample.json"
        sample_path.write_text(json.dumps({"development_cases": [{"incident_id": case_id}]}),
                               encoding="utf-8")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        from e4.adapters.bank import sha256_file
        manifest["sample_config_sha256"] = sha256_file(sample_path)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
        profile_path = root / "config/test_profile.json"
        profile_path.write_text(json.dumps({"model_alias": "bank-test", "temperature": 0,
            "top_p": 1, "top_k": 1, "seed": 42, "max_generation_tokens": 128,
            "context_tokens": 32768, "reasoning_mode": "off"}), encoding="utf-8")
        (root / "config/bank/seed_playbooks.json").write_bytes(
            (ROOT / "config/bank/seed_playbooks.json").read_bytes())
        (root / "config/runtime").mkdir(exist_ok=True)
        (root / "config/runtime/schema.json").write_bytes(
            (ROOT / "config/runtime/schema.json").read_bytes())
        return case_id, profile_path

    @staticmethod
    def _launcher_env(outer, profile):
        # Match the sanitized command environment built by run_m3_model.py.
        return {"E4_BASE_URL": "http://127.0.0.1:8080",
                "E4_RUN_DIR": str(outer), "E4_MODEL_PROFILE": str(profile)}

    def test_fake_client_static_repair_executes_and_records_cold_warm_disabled_parity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = self._fixture_root(root)
            registry = bank_registry(root=root)
            seed_doc = json.loads((ROOT / "config/bank/seed_playbooks.json").read_text())
            graph = runner.materialize_bank_seed_examples(seed_doc, registry)["combined_shared_candidates"]
            client = self.FakeClient(["{malformed", json.dumps(graph, sort_keys=True)])
            outer = root / "launcher-existing"
            outer.mkdir()
            (outer / "launcher-owned.json").write_text("keep", encoding="utf-8")
            with patch.dict(os.environ, self._launcher_env(outer, profile)):
                result = runner.run_bank_development(case_id, profile_path=profile,
                    root=root, client=client)

            self.assertEqual(len(client.prompts), 2)
            self.assertIn("Static JSON/DAG validation error", client.prompts[1])
            record = result["development_run"]
            self.assertEqual(record["static_repairs"], 1)
            self.assertTrue(record["cache_parity_passed"])
            self.assertEqual(record["cache_hits"], [0,
                len(record["execution_records"][0]["records"]) - 1, 0])
            self.assertEqual(record["exact_seed_example_matches"], ["combined_shared_candidates"])
            run_dir = Path(result["run_dir"])
            self.assertTrue(run_dir.is_relative_to(outer))
            self.assertTrue((run_dir / "model_calls.jsonl").is_file())
            self.assertEqual((outer / "launcher-owned.json").read_text(), "keep")

    def test_cpu_controls_and_saved_dag_replay_make_no_http_requests(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _profile = self._fixture_root(root)
            outer = root / "existing-run-mount"
            outer.mkdir()
            graph = runner.materialize_bank_seed_examples(
                json.loads((root / "config/bank/seed_playbooks.json").read_text()),
                bank_registry(root=root))["combined_shared_candidates"]
            saved = root / "saved_bank_dag.json"
            saved.write_text(json.dumps(graph), encoding="utf-8")
            with patch.dict(os.environ, {"E4_RUN_DIR": str(outer)}), \
                    patch("urllib.request.urlopen", side_effect=AssertionError("HTTP is forbidden in CPU modes")):
                controls = runner.run_bank_controls(case_id, root=root)
                replay = runner.replay_bank_development(case_id, saved, root=root)
            self.assertEqual(len(controls["manifest"]["runs"]), 4)
            self.assertTrue(all(item["cache_parity_passed"]
                                for item in controls["manifest"]["runs"]))
            self.assertEqual(controls["manifest"]["model_requests"], 0)
            self.assertTrue(replay["manifest"]["cache_parity_passed"])
            self.assertEqual(replay["manifest"]["model_requests"], 0)
            for item in controls["manifest"]["runs"]:
                self.assertTrue((Path(controls["run_dir"]) / f"{item['name']}.result.json").is_file())

    def test_runtime_execution_error_is_retained_without_semantic_repair(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = self._fixture_root(root)
            registry = bank_registry(root=root)
            seeds = runner.materialize_bank_seed_examples(
                json.loads((ROOT / "config/bank/seed_playbooks.json").read_text()), registry)
            graph = seeds["combined_shared_candidates"]
            trace_score = next(node for node in graph["nodes"] if node["id"] == "trace_scores")
            trace_score["inputs"]["reference"], trace_score["inputs"]["alert"] = (
                trace_score["inputs"]["alert"], trace_score["inputs"]["reference"])
            client = self.FakeClient([json.dumps(graph, sort_keys=True),
                                      json.dumps(seeds["combined_shared_candidates"], sort_keys=True)])
            outer = root / "existing-run-mount"
            outer.mkdir()
            with patch.dict(os.environ, self._launcher_env(outer, profile)):
                with self.assertRaises(ExecutionFailure):
                    runner.run_bank_development(case_id, profile_path=profile,
                        root=root, client=client)
            child = outer / "development" / case_id
            self.assertEqual(len(client.prompts), 1)
            call_rows = (child / "model_calls.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(call_rows), 1)
            failure = json.loads((child / "development_run.json").read_text(encoding="utf-8"))
            self.assertEqual(failure["static_repairs"], 0)
            self.assertIn("requires adjacent", failure["error"]["message"].casefold())

    def test_cli_entrypoint_passes_explicit_child_and_uses_returned_run_path(self):
        fake_result = {"run_dir": "/tmp/fake-run", "development_run": {
            "cache_parity_passed": True, "static_repairs": 0}}
        with patch.dict(os.environ, {"E4_RUN_DIR": "/tmp/outer"}), \
                patch.object(runner, "run_bank_development", return_value=fake_result) as run:
            with patch("builtins.print") as printed:
                status = runner.main(["--case-id", "bank-20210323T0630-utc8"])
        self.assertEqual(status, 0)
        self.assertIsNone(run.call_args.kwargs["run_dir"])
        self.assertIn("/tmp/fake-run", printed.call_args.args[0])

    def test_bank_seed_examples_are_materialized_verified_executable_and_prompted(self):
        registry = bank_registry(root=ROOT)
        seed_library = json.loads((ROOT / "config/bank/seed_playbooks.json").read_text(encoding="utf-8"))
        examples = runner.materialize_bank_seed_examples(seed_library, registry)
        self.assertEqual(set(examples), set(seed_library["examples"]))
        reference, alert_window = Window("reference", 1_700_000_000_000, 1_700_001_800_000), Window(
            "alert", 1_700_001_800_000, 1_700_003_600_000)
        telemetry = Telemetry("bank-seed-fixture", reference, alert_window, (), (), {},
            "a" * 64, {}, {}, {}, "bank-seed-test/1")
        runtime = build_case_runtime({
            "analysis_window_start_epoch_ms_utc": reference.start_ms_utc,
            "alert_window_start_epoch_ms_utc": reference.end_ms_utc,
            "alert_window_end_epoch_ms_utc": alert_window.end_ms_utc})
        external = None
        with tempfile.TemporaryDirectory() as temp:
            executor = SerialExecutor(registry, CacheStore(Path(temp) / "cache", enabled=False))
            for name, document in examples.items():
                verified = verify_dag(document, registry, runtime,
                    max_nodes=int(runtime.snapshot.values["max_nodes"]),
                    max_depth=int(runtime.snapshot.values["dag_depth"]))
                runner._validate_bank_graph(document, verified, registry)
                source_id = next(node.node_id for node in verified.nodes if node.operator.external_input)
                result = executor.execute_verified(verified, runtime, {source_id: telemetry},
                    cache_enabled=False)
                self.assertIsNotNone(result.answer, name)

        case = runner.BankCase(telemetry, {
            "incident_id": "bank-synthetic-public-fixture", "alert_text": "synthetic public context",
            "timezone": "Asia/Shanghai", "source_date_timestamp_units": {}}, {},
            {"official_candidate_components": ["PodA"]}, {},
            {"component_identity_coverage": {}})
        prompt = runner.build_bank_development_prompt(case, runtime, {"kpi_id": None}, registry)
        for name, document in examples.items():
            self.assertIn(name, seed_library["examples"])
            self.assertIn(json.dumps(document, ensure_ascii=False, sort_keys=True), prompt)
        self.assertIn("log/log_service.csv", prompt)
        self.assertIn("source-neutral structural examples", seed_library["provenance"])

    def test_existing_launcher_directory_is_preserved_and_artifacts_use_fresh_child(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outer = root / "outer-model-run"
            outer.mkdir()
            marker = outer / "launcher_manifest.json"
            marker.write_text('{"owned":"by launcher"}', encoding="utf-8")
            profile = root / "profile.json"
            profile.write_text("{}", encoding="utf-8")
            (root / "config/bank").mkdir(parents=True)
            (root / "config/bank/sample.json").write_text(json.dumps({
                "development_cases": [{"incident_id": "bank-20210323T0630-utc8"}]
            }), encoding="utf-8")

            with patch.dict(os.environ, self._launcher_env(outer, profile)), \
                    patch.object(runner, "load_bank_case", side_effect=RuntimeError("fixture input unavailable")) as load_case:
                with self.assertRaisesRegex(RuntimeError, "fixture input unavailable"):
                    runner.run_bank_development(
                        "bank-20210323T0630-utc8", profile_path=profile, root=root)

            child = outer / "development/bank-20210323T0630-utc8"
            self.assertTrue((child / "phase_status.json").is_file())
            failure = json.loads((child / "dev_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(failure["status"], "failed_before_prompt")
            self.assertEqual(json.loads(marker.read_text(encoding="utf-8")), {"owned": "by launcher"})
            self.assertFalse((outer / "development_run.json").exists())
            self.assertFalse((child / "model_calls.jsonl").exists())
            load_case.assert_called_once()

    def test_explicit_run_dir_cannot_escape_outer_launcher_mount(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outer = root / "outer"
            outer.mkdir()
            profile = root / "profile.json"
            profile.write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, self._launcher_env(outer, profile)):
                with self.assertRaisesRegex(runner.BankDevelopmentError, "inside a child"):
                    runner.run_bank_development("bank-20210323T0630-utc8",
                        profile_path=profile, run_dir=root / "escaped",
                        root=root)

    def test_launcher_contract_rejects_missing_or_mismatched_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outer = root / "outer"
            outer.mkdir()
            profile = root / "profile.json"
            profile.write_text("{}", encoding="utf-8")
            other_profile = root / "other-profile.json"
            other_profile.write_text("{}", encoding="utf-8")
            env = self._launcher_env(outer, profile)
            with patch.dict(os.environ, {"E4_BASE_URL": env["E4_BASE_URL"],
                    "E4_RUN_DIR": env["E4_RUN_DIR"]}, clear=True):
                with self.assertRaisesRegex(runner.BankDevelopmentError, "E4_MODEL_PROFILE"):
                    runner.run_bank_development("bank-20210323T0630-utc8",
                        profile_path=profile, root=root)
            with patch.dict(os.environ, self._launcher_env(outer, other_profile), clear=True):
                with self.assertRaisesRegex(runner.BankDevelopmentError, "must match E4_MODEL_PROFILE"):
                    runner.run_bank_development("bank-20210323T0630-utc8",
                        profile_path=profile, root=root)

    def test_launcher_contract_rejects_nonlocal_model_endpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outer = root / "outer"
            outer.mkdir()
            profile = root / "profile.json"
            profile.write_text("{}", encoding="utf-8")
            env = self._launcher_env(outer, profile)
            env["E4_BASE_URL"] = "https://models.example.test:8080"
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaisesRegex(runner.BankDevelopmentError, "local endpoint"):
                    runner.run_bank_development("bank-20210323T0630-utc8",
                        profile_path=profile, root=root)


if __name__ == "__main__":
    unittest.main()
