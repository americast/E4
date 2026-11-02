import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from e4core.m4_bootstrap import run_bootstrap
from e4core.m4_data import make_m4_registry
from e4core.m4_fixtures import fixture_expected_top_component
from e4core.m4_library import load_library_snapshot
from e4core.m4_controller import (M4ControllerError, reject_identifier_literals,
                                  data_operator_contract_schema,
                                  typed_contract_wire_schema)
from e4core.m4_data import query_bank_public
from e4core.m4_fixtures import write_synthetic_public_case


ROOT = Path(__file__).resolve().parents[1]


def _copy_bootstrap_config(root: Path) -> Path:
    config = root / "config"
    config.mkdir(parents=True)
    for name in ("bank_metric_kind_policy.json", "bank_dev_kpi_policy.json", "m4_runtime.json",
                 "core_runtime.json", "runtime_schema.json", "bank_seed_library.json",
                 "openrca_bank_source.json"):
        shutil.copy2(ROOT / "config" / name, config / name)
    profile = config / "profile.json"
    profile.write_text(json.dumps({"model_alias": "fake-gemma", "temperature": 1.0,
        "top_p": 0.95, "top_k": 64, "seed": 42, "max_generation_tokens": 8192,
        "context_tokens": 32768, "reasoning_mode": "on"}), encoding="utf-8")
    return profile


def _generated_playbook(root: Path) -> dict:
    registry = make_m4_registry(None, [], root=root)
    versions = {}
    for row in registry.signatures():
        if row["name"] == "bank.answers.combine":
            if row["version"].startswith("1+m4."):
                versions[row["name"]] = row["version"]
        else:
            versions[row["name"]] = row["version"]
    return {"name": "native_query_delta", "dag": {"schema_version": "e4-dag/1", "nodes": [
        {"id": "data", "operator": "case.telemetry", "version": versions["case.telemetry"],
         "inputs": {}, "params": {}, "bindings": {}},
        {"id": "scores", "operator": "evolved.synthetic_metric_score", "version": "1",
         "inputs": {"data": "data"},
         "params": {}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine", "version": versions["bank.answers.combine"],
         "inputs": {"scores": ["scores"]}, "params": {"limit": 5}, "bindings": {}}],
        "return": {"node": "answer"}}}


def _composite_metric_scorer(root: Path, name="evolved.synthetic_metric_score"):
    registry = make_m4_registry(None, [], root=root)
    versions = {row["name"]: row["version"] for row in registry.signatures()}
    return {"name": name, "version": "1",
        "description": "Reusable composition of the registered Bank metric pipeline.",
        "family": "bank_metric_pipeline",
        "input_ports": [{"name": "data", "type": "Telemetry"}],
        "output_type": "ScoredCandidates", "parameters": {},
        "authoring_profile": "operator-dag-v1",
        "program": {"schema_version": "e4-operator-dag/1", "nodes": [
            {"id": "reference", "operator": "bank.metrics.aggregate",
             "version": versions["bank.metrics.aggregate"],
             "inputs": {"data": "$input.data"}, "params": {"window": "reference"}, "bindings": {}},
            {"id": "alert", "operator": "bank.metrics.aggregate",
             "version": versions["bank.metrics.aggregate"],
             "inputs": {"data": "$input.data"}, "params": {"window": "alert"}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate",
             "version": versions["bank.candidates.generate"],
             "inputs": {"data": "$input.data", "aggregates": ["reference", "alert"]},
             "params": {}, "bindings": {}},
            {"id": "scores", "operator": "bank.metrics.contrast",
             "version": versions["bank.metrics.contrast"],
             "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
             "params": {}, "bindings": {}}], "return": {"node": "scores"}}}


class FakeBootstrapClient:
    def __init__(self, content):
        self.content = content
        self.calls = []
        self.base_url = "http://127.0.0.1:8080"
        self.context_tokens = 32768
        self.max_generation_tokens = 8192

    def complete(self, prompt, *, task_id, attempt):
        self.calls.append(prompt)
        now = datetime.now(timezone.utc).isoformat()
        return _model_call(self.content, task_id, attempt, now)


def _model_call(content, task_id, attempt, now):
    from e4core.model_client import ModelCall
    raw = {"choices": [{"message": {"content": content}}]}
    return ModelCall(f"fake-bootstrap-{attempt}", task_id, attempt, now, now,
        0.01, 0.005, 20, 32768, (), {"messages": [{"role": "user", "content": "synthetic"}]},
        "synthetic-prompt-hash", 200, json.dumps(raw), raw, content, None,
        {"prompt_tokens": 20, "completion_tokens": 30}, {"finish_reason": "stop"}, None)


class M4BootstrapTests(unittest.TestCase):
    def test_composite_dag_schema_is_published_without_legacy_python_contracts(self):
        from e4core.m4_composite import composite_spec_schema
        schema = composite_spec_schema()
        self.assertEqual(schema["authoring_profile"], "operator-dag-v1")
        required = set(schema["required_fields"])
        self.assertEqual(required, {"name", "version", "description", "family", "input_ports",
            "output_type", "parameters", "authoring_profile", "program"})
        self.assertFalse(schema["additional_fields"])
        self.assertEqual(schema["fields"]["program"]["schema_version"], "e4-operator-dag/1")
        self.assertEqual(schema["limits"]["expanded_nodes"], 40)
        self.assertEqual(schema["limits"]["expanded_depth"], 12)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            fake = FakeBootstrapClient(json.dumps({"operators": [], "playbooks": []}))
            result = run_bootstrap(profile_path=profile, run_dir=root / "bootstrap",
                                   client=fake, root=root)
            bootstrap_prompt = fake.calls[0]
            self.assertIn("composite_spec_schema", bootstrap_prompt)
            self.assertIn('"input_ports"', bootstrap_prompt)
            self.assertIn('"program"', bootstrap_prompt)
            self.assertIn("e4-operator-dag/1", bootstrap_prompt)
            self.assertNotIn('"source":', bootstrap_prompt)
            self.assertNotIn('"typed_helper_api"', bootstrap_prompt)

            from e4core.m4_controller import _stage_prompt, build_m4_runtime
            from e4core.m4_data import M4QueryBudget
            registry = make_m4_registry(None, [], root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1, "alert_window_end_epoch_ms_utc": 2}, root=root)
            policy = json.loads((root / "config/m4_runtime.json").read_text(encoding="utf-8"))
            evolve_prompt = _stage_prompt("evolve", digest={}, registry=registry,
                snapshot=result["snapshot"], runtime=runtime, history=[], policy=policy,
                query_budget=M4QueryBudget(None))
            serve_prompt = _stage_prompt("stage3_serve", digest={}, registry=registry,
                snapshot=result["snapshot"], runtime=runtime, history=[], policy=policy,
                query_budget=M4QueryBudget(None),
                gap_record={"schema_version": "e4-capability-gaps/1"})
            self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG", evolve_prompt)
            self.assertIn('"authoring_profile":"typed-operator-ir-v1"', evolve_prompt)
            self.assertIn('"program"', evolve_prompt)
            self.assertNotIn('"source":', evolve_prompt)
            self.assertNotIn('"typed_record_helper_api"', evolve_prompt)
            self.assertIn("TRUSTED INPUT PRODUCER CAPABILITIES", evolve_prompt)
            self.assertNotIn("PRIVATE TYPED-OPERATOR-IR SCHEMA", serve_prompt)
            self.assertNotIn("Each operator spec has exactly", serve_prompt)
            self.assertIn("Do not return new specs or change the gap record", serve_prompt)
            self.assertIn("Return exactly {dag:", serve_prompt)
            self.assertIn("DATA COVERAGE GUIDANCE", serve_prompt)
            self.assertNotIn('"source":', serve_prompt)

    def test_synthetic_native_logs_cover_positive_stable_healthy_missing_and_renamed_entities(self):
        def error_rows(root, case, window, entity):
            page = query_bank_public(case.telemetry, root=root, table="log/log_service.csv",
                window=window, entity=entity, fields=["cmdb_id", "value"], limit=256)
            return [row for row in page.metadata["public_query"]["rows_data"]
                    if "OutOfMemoryError" in row["value"] or "GC overhead limit exceeded" in row["value"]]

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            positive = write_synthetic_public_case(root / "positive", scenario="positive",
                alpha_id="renamed-cause", beta_id="renamed-control")
            ref = error_rows(root / "positive", positive, "reference", "renamed-cause")
            alert = error_rows(root / "positive", positive, "alert", "renamed-cause")
            control = error_rows(root / "positive", positive, "alert", "renamed-control")
            self.assertGreater(len(alert), len(ref))
            self.assertEqual(control, [])

            stable = write_synthetic_public_case(root / "stable", scenario="high_stable",
                alpha_id="stable-cause", beta_id="stable-control")
            stable_ref = error_rows(root / "stable", stable, "reference", "stable-control")
            stable_alert = error_rows(root / "stable", stable, "alert", "stable-control")
            self.assertEqual(len(stable_ref), len(stable_alert))
            self.assertGreater(len(stable_ref), 0)

            unchanged = write_synthetic_public_case(root / "unchanged", scenario="no_change")
            self.assertEqual(error_rows(root / "unchanged", unchanged, "reference", "fixture_pod_alpha"), [])
            absent = write_synthetic_public_case(root / "absent", scenario="missing")
            self.assertEqual(error_rows(root / "absent", absent, "alert", "fixture_pod_alpha"), [])

    def test_typed_wire_prompt_schema_publishes_evidence_eligibility_contract(self):
        schema = typed_contract_wire_schema()
        rules = schema["evidence_requirements"]
        self.assertIn("bank/metric_container.csv", rules["allowed_native_sources"])
        self.assertIn("bank/metric/metric_container.csv", rules["allowed_native_sources"])
        self.assertIn("support > 0", rules["support"])
        self.assertIn("ranking_eligible == true", rules["eligibility"])
        self.assertIn("contributing_entities", rules["attribution"])
        self.assertIn("do not invent p-values", rules["statistics"])
        self.assertIn("category must be null", rules["bank_category"])

    def test_data_operator_contract_is_rendered_in_bootstrap_and_stage_three_prompts(self):
        schema = data_operator_contract_schema()
        self.assertIn("metadata.public_query.rows_data", schema["bank.public.query"]["aggregate_rows"])
        self.assertIn("timestamp_ms_utc", schema["bank.public.query"]["public_query"]["rows_data"])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            fake = FakeBootstrapClient(json.dumps({"operators": [], "playbooks": []}))
            result = run_bootstrap(profile_path=profile, run_dir=root / "bootstrap",
                                   client=fake, root=root)
            self.assertIn("composite_spec_schema", fake.calls[0])
            self.assertNotIn("data_operator_contract_schema", fake.calls[0])
            from e4core.m4_controller import _stage_prompt, build_m4_runtime
            from e4core.m4_data import M4QueryBudget
            registry = make_m4_registry(None, [], root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1, "alert_window_end_epoch_ms_utc": 2}, root=root)
            policy = json.loads((root / "config/m4_runtime.json").read_text())
            stage_prompt = _stage_prompt("synthesize", digest={}, runtime=runtime,
                registry=registry, snapshot=result["snapshot"], history=[], policy=policy,
                query_budget=M4QueryBudget(None))
            self.assertIn("DATA COVERAGE GUIDANCE", stage_prompt)
            self.assertIn("STAGE 3 SERVING CONTRACT", stage_prompt)
            self.assertIn("atomic serving operators", stage_prompt)
            self.assertNotIn('"source":', stage_prompt)

    def test_bootstrap_rejects_and_redacts_raw_python_operator_source(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            invalid = {"operators": [{"name": "evolved.raw_source", "version": "1",
                "description": "invalid source", "family": "fixture",
                "input_ports": [{"name": "data", "type": "Aggregate"}],
                "output_type": "Aggregate", "parameters": {},
                "source": "def operator(inputs, params, runtime): return inputs['data']"}],
                "playbooks": []}
            fake = FakeBootstrapClient(json.dumps(invalid))
            with self.assertRaises(M4ControllerError):
                run_bootstrap(profile_path=profile, run_dir=root / "bootstrap",
                    client=fake, root=root)
            self.assertEqual(len(fake.calls), 3)
            self.assertNotIn("def operator(inputs, params, runtime)", fake.calls[1])
            self.assertIn("rejected executable source omitted", fake.calls[1])
            self.assertFalse((root / "bootstrap/library").exists())

    def test_bootstrap_cli_delegates_to_restricted_supervisor_with_launcher_profile(self):
        from scripts import run_m4_bootstrap
        with tempfile.TemporaryDirectory() as temp:
            outer = Path(temp) / "existing-run"
            outer.mkdir()
            profile = ROOT / "config/gemma_m3_inference.json"
            with patch.dict("os.environ", {"E4_RUN_DIR": str(outer),
                    "E4_BASE_URL": "http://127.0.0.1:8080", "E4_MODEL_PROFILE": str(profile)}), \
                    patch.object(run_m4_bootstrap, "supervisor_main", return_value=0) as supervisor:
                status = run_m4_bootstrap.main(["--profile", str(profile), "--run-dir", "bootstrap"])
            self.assertEqual(status, 0)
            supervisor.assert_called_once_with(["--mode", "bootstrap", "--profile",
                str(profile), "--run-dir", "bootstrap"])

    def test_generated_source_literal_cannot_encode_a_component_or_incident(self):
        with self.assertRaisesRegex(M4ControllerError, "cannot embed"):
            reject_identifier_literals('def operator(inputs, params, runtime):\n    return "Tomcat01"\n',
                                       {"Tomcat01", "bank-20990101T0000-utc8"})
        reject_identifier_literals('def operator(inputs, params, runtime):\n    return inputs["data"]\n',
                                   {"Tomcat01", "bank-20990101T0000-utc8"})

    def test_seed_only_bootstrap_requires_model_response_and_materializes_protected_seeds(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            output = root / "bootstrap"
            fake = FakeBootstrapClient(json.dumps({"operators": [], "playbooks": []}))
            result = run_bootstrap(profile_path=profile, run_dir=output,
                                   client=fake, root=root)
            self.assertEqual(result["result"]["status"], "complete_seed_only")
            self.assertTrue(result["result"]["model_response_received"])
            snapshot = load_library_snapshot(result["snapshot"].path)
            self.assertEqual(set(snapshot.playbooks), {
                "native_metric_contrast", "per_trace_native_duration",
                "per_minute_log_volume", "combined_shared_candidates"})
            self.assertEqual(len(fake.calls), 1)
            prompt = fake.calls[0]
            self.assertNotIn("fixture_pod_alpha", prompt)
            self.assertNotIn("ground_truth", prompt)
            self.assertNotIn("record.csv", prompt)
            self.assertIn("bank.public.project", prompt)
            self.assertIn("maximum_public_query_rows_per_request", prompt)

    def test_schema_bootstrap_executes_closed_composite_on_renamed_controls(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            bundle = {"operators": [_composite_metric_scorer(root)],
                      "playbooks": [_generated_playbook(root)]}
            fake = FakeBootstrapClient(json.dumps(bundle, ensure_ascii=False))
            result = run_bootstrap(profile_path=profile, run_dir=root / "bootstrap",
                                   client=fake, root=root)
            self.assertEqual(result["result"]["status"], "complete_with_additions")
            self.assertEqual(len(result["result"]["accepted_generated_operators"]), 1)
            self.assertEqual(result["result"]["accepted_generated_playbooks"], ["native_query_delta"])
            provenance = result["snapshot"].document["provenance"]
            self.assertTrue(provenance["bootstrap_model_response_obtained"])
            playbook = result["snapshot"].playbooks["native_query_delta"]
            outcomes = playbook["provenance"]["synthetic_outcomes"]
            self.assertEqual(len(outcomes), 5)
            self.assertIn(fixture_expected_top_component(), outcomes[0]["top_components"])
            self.assertIn("counterfactual_component_theta", outcomes[1]["top_components"])
            self.assertFalse(outcomes[2]["top_components"])
            self.assertFalse(outcomes[3]["top_components"])
            self.assertFalse(outcomes[4]["top_components"])


if __name__ == "__main__":
    unittest.main()
