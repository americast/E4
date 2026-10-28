import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from e4.agent.bank.bootstrap import run_bootstrap
from e4.operators.bank.query import make_m4_registry
from e4.agent.bank.fixtures import fixture_expected_top_component
from e4.agent.library import load_library_snapshot
from e4.agent.bank.controller import (M4ControllerError, reject_identifier_literals,
                                      data_operator_contract_schema,
                                      generated_spec_shape_example,
                                      typed_contract_wire_schema)
from e4.operators.bank.query import query_bank_public
from e4.agent.bank.fixtures import write_synthetic_public_case
from e4.operators.generated import generated_spec_contract_schema, validate_generated_spec


ROOT = Path(__file__).resolve().parents[3]


def _copy_bootstrap_config(root: Path) -> Path:
    config = root / "config"
    config.mkdir(parents=True)
    for name in ("bank/metric_kind_policy.json", "bank/kpi_policy.json", "bank/controller_policy.json",
                 "runtime/values.json", "runtime/schema.json", "bank/seed_playbooks.json",
                 "bank/source.json"):
        (config / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "config" / name, config / name)
    profile = config / "profile.json"
    profile.write_text(json.dumps({"model_alias": "fake-gemma", "temperature": 1.0,
        "top_p": 0.95, "top_k": 64, "seed": 42, "max_generation_tokens": 8192,
        "context_tokens": 32768, "reasoning_mode": "on"}), encoding="utf-8")
    return profile


def _typed_scorer_spec():
    source = '''def operator(inputs, params, runtime):
    reference = contract_fields(inputs["reference"], "Aggregate")
    alert = contract_fields(inputs["alert"], "Aggregate")
    candidate_set = contract_fields(inputs["candidates"], "CandidateSet")
    before = {}
    after = {}
    for row in reference["metadata"]["public_query"]["rows_data"]:
        raw = row["value"]
        if raw != "" and raw != "None":
            before.setdefault(row["cmdb_id"], []).append(float(raw))
    for row in alert["metadata"]["public_query"]["rows_data"]:
        raw = row["value"]
        if raw != "" and raw != "None":
            after.setdefault(row["cmdb_id"], []).append(float(raw))
    candidates = []
    for candidate_wire in candidate_set["candidates"]:
        candidate = contract_fields(candidate_wire, "Candidate")
        entity = contract_fields(candidate["entity"], "EntityRef")
        identifier = entity["identifier"]
        left = before.get(identifier, [])
        right = after.get(identifier, [])
        score = 0.0
        support = min(len(left), len(right))
        if left and right:
            score = max(0.0, (sum(right) / len(right) - sum(left) / len(left)) * params["scale"])
        eligible = score > 0.0 and support > 0
        evidence = []
        if support > 0:
            evidence.append({"$type": "Evidence", "$fields": {
                "source": "bank/metric_container.csv",
                "window": alert["window"], "statistic": "synthetic_native_mean_delta",
                "value": score, "contributing_entities": {"$tuple": [candidate["entity"]]},
                "support": support, "details": {"$map": {"metric_key": "native_kpi",
                    "normalized_effect": score, "p_value": 0.0 if eligible else 1.0,
                    "direction": "higher_is_worse", "unit": "synthetic native value",
                    "ranking_eligible": eligible}}}})
        candidates.append({"$type": "ScoredCandidate", "$fields": {
            "entity": candidate["entity"], "score": score, "category": None,
            "evidence": {"$tuple": evidence}, "analysis_family": "bank_metric"}})
    status = "ready" if any(item["$fields"]["score"] > 0.0 for item in candidates) else "abstained"
    return {"$type": "ScoredCandidates", "$fields": {
        "candidates": {"$tuple": candidates}, "scorer": "evolved.synthetic_metric_score",
        "status": status, "status_details": {"$map": {}}}}
'''
    return {"name": "evolved.synthetic_metric_score", "version": "1",
        "description": "Compare bounded public native metric aggregates by exact observed component.",
        "family": "bank_metric",
        "input_ports": [{"name": "reference", "type": "Aggregate"},
                        {"name": "alert", "type": "Aggregate"},
                        {"name": "candidates", "type": "CandidateSet"}],
        "output_type": "ScoredCandidates",
        "parameters": {"scale": {"type": "number", "default": 1.0}},
        "source": source}


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
        {"id": "reference", "operator": "bank.public.query", "version": versions["bank.public.query"],
         "inputs": {"data": "data"}, "params": {"table": "metric/metric_container.csv",
             "window": "reference", "limit": 64, "offset": 0,
             "fields": ["timestamp", "cmdb_id", "kpi_name", "value"]}, "bindings": {}},
        {"id": "alert", "operator": "bank.public.query", "version": versions["bank.public.query"],
         "inputs": {"data": "data"}, "params": {"table": "metric/metric_container.csv",
             "window": "alert", "limit": 64, "offset": 0,
             "fields": ["timestamp", "cmdb_id", "kpi_name", "value"]}, "bindings": {}},
        {"id": "candidates", "operator": "bank.candidates.generate", "version": versions["bank.candidates.generate"],
         "inputs": {"data": "data"}, "params": {}, "bindings": {}},
        {"id": "scores", "operator": "evolved.synthetic_metric_score", "version": "1",
         "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
         "params": {}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine", "version": versions["bank.answers.combine"],
         "inputs": {"scores": ["scores"]}, "params": {"limit": 5}, "bindings": {}}],
        "return": {"node": "answer"}}}


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
    from e4.agent.model_client import ModelCall
    raw = {"choices": [{"message": {"content": content}}]}
    return ModelCall(f"fake-bootstrap-{attempt}", task_id, attempt, now, now,
        0.01, 0.005, 20, 32768, (), {"messages": [{"role": "user", "content": "synthetic"}]},
        "synthetic-prompt-hash", 200, json.dumps(raw), raw, content, None,
        {"prompt_tokens": 20, "completion_tokens": 30}, {"finish_reason": "stop"}, None)


class M4BootstrapTests(unittest.TestCase):
    def test_generated_operator_schema_matches_validator_and_is_published_in_both_prompts(self):
        schema = generated_spec_contract_schema()
        compact_schema = json.dumps(schema, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"))
        self.assertLess(len(compact_schema.encode("utf-8")), 5000)
        raw_spec = _typed_scorer_spec()
        self.assertEqual(set(raw_spec), set(schema["top_level"]["required_fields"]))
        ports = schema["top_level"]["fields"]["input_ports"]
        self.assertEqual(ports["type"], "array")
        self.assertEqual(ports["items"], "input_port")
        self.assertEqual(ports["min_items"], 1)
        self.assertEqual(ports["max_items"], 8)
        self.assertEqual(set(raw_spec["input_ports"][0]), {"name", "type"})
        self.assertIn("required", schema["input_ports"]["fields"])
        self.assertIn("variadic", schema["input_ports"]["fields"])
        self.assertEqual(schema["parameters"]["container"], "JSON object/map")
        self.assertIn("entry_fields", schema["parameters"])
        self.assertEqual(set(raw_spec["parameters"]["scale"]), {"type", "default"})
        self.assertLessEqual(set(raw_spec["parameters"]["scale"]),
                             set(schema["parameters"]["entry_allowed_fields"]))
        self.assertIn("positive_integer", schema["parameters"]["value_semantics"])
        normalized = validate_generated_spec(raw_spec)
        self.assertIn(normalized["output_type"], schema["output_types"])
        example = generated_spec_shape_example()
        self.assertEqual(set(example), set(schema["top_level"]["required_fields"]))
        self.assertIn("Shape-only", example["description"])
        self.assertIn("output_type", validate_generated_spec(example))

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            fake = FakeBootstrapClient(json.dumps({"operators": [], "playbooks": []}))
            result = run_bootstrap(profile_path=profile, run_dir=root / "bootstrap",
                                   client=fake, root=root)
            bootstrap_prompt = fake.calls[0]
            self.assertIn("generated_spec_contract_schema", bootstrap_prompt)
            self.assertIn('"input_ports"', bootstrap_prompt)
            self.assertIn('"entry_fields"', bootstrap_prompt)
            self.assertIn("Shape-only pass-through example", bootstrap_prompt)

            from e4.agent.bank.controller import _stage_prompt, build_m4_runtime
            from e4.operators.bank.query import M4QueryBudget
            registry = make_m4_registry(None, [], root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1, "alert_window_end_epoch_ms_utc": 2}, root=root)
            policy = json.loads((root / "config/bank/controller_policy.json").read_text(encoding="utf-8"))
            stage3_prompt = _stage_prompt("synthesize", digest={}, registry=registry,
                snapshot=result["snapshot"], runtime=runtime, history=[], policy=policy,
                query_budget=M4QueryBudget(None))
            self.assertIn("EXACT GENERATED OPERATOR SPEC JSON SCHEMA", stage3_prompt)
            self.assertIn('"input_ports"', stage3_prompt)
            self.assertIn('"entry_fields"', stage3_prompt)
            self.assertIn("Shape-only pass-through example", stage3_prompt)

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
            self.assertIn("data_operator_contract_schema", fake.calls[0])
            self.assertIn("rows_data", fake.calls[0])
            from e4.agent.bank.controller import _stage_prompt, build_m4_runtime
            from e4.operators.bank.query import M4QueryBudget
            registry = make_m4_registry(None, [], root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1, "alert_window_end_epoch_ms_utc": 2}, root=root)
            policy = json.loads((root / "config/bank/controller_policy.json").read_text())
            stage_prompt = _stage_prompt("synthesize", digest={}, runtime=runtime,
                registry=registry, snapshot=result["snapshot"], history=[], policy=policy,
                query_budget=M4QueryBudget(None))
            self.assertIn("NATIVE QUERY AND PROJECTION DATA LAYOUT", stage_prompt)
            self.assertIn("metadata.public_query.rows_data", stage_prompt)

    def test_bootstrap_cli_delegates_to_restricted_supervisor_with_launcher_profile(self):
        from scripts.bank import bootstrap as bootstrap_script
        with tempfile.TemporaryDirectory() as temp:
            outer = Path(temp) / "existing-run"
            outer.mkdir()
            profile = ROOT / "config/models/gemma.json"
            with patch.dict("os.environ", {"E4_RUN_DIR": str(outer),
                    "E4_BASE_URL": "http://127.0.0.1:8080", "E4_MODEL_PROFILE": str(profile)}), \
                    patch.object(bootstrap_script, "supervisor_main", return_value=0) as supervisor:
                status = bootstrap_script.main(["--profile", str(profile), "--run-dir", "bootstrap"])
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

    def test_schema_bootstrap_executes_typed_generated_scorer_on_renamed_controls(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = _copy_bootstrap_config(root)
            bundle = {"operators": [_typed_scorer_spec()],
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
