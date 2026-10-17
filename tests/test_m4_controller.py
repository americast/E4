import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from e4core.bank_adapter import sha256_file
from e4core.bank_ops import bank_registry
from e4core.cache import implementation_fingerprint
from e4core.m4_controller import (M4ControllerError, _materialize_seeds_for_registry,
    _apply_stage1_selection, _plain, _stage_prompt, _static_repair_prompt,
    build_m4_runtime, public_case_digest,
    run_diagnostic, validate_bank_answer)
from e4core.m4_data import make_m4_registry
from e4core.m4_library import (create_library_snapshot, generated_specs_from_snapshot,
    playbook_graph_hash, snapshot_library)
from e4core.model_client import ModelCall
from e4core.contracts import EntityRef, Evidence, RankedAnswer, ScoredCandidate, Telemetry
from e4core.dag import verify_dag
from e4core.runtime import resolve_parameters
from tests.test_bank_adapter import make_bank_fixture
from tests.test_m4_bootstrap import _generated_playbook, _typed_scorer_spec


ROOT = Path(__file__).resolve().parents[1]


def _fixture_root(root: Path) -> tuple[str, Path]:
    config = root / "config"
    config.mkdir(parents=True)
    for name in ("bank_metric_kind_policy.json", "bank_dev_kpi_policy.json", "m4_runtime.json",
                 "core_runtime.json", "runtime_schema.json", "bank_seed_library.json",
                 "openrca_bank_source.json"):
        shutil.copy2(ROOT / "config" / name, config / name)
    case_id, manifest_path = make_bank_fixture(root)
    sample_path = config / "bank_sample.json"
    sample_path.write_text(json.dumps({"development_cases": [{"incident_id": case_id}]}),
                           encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sample_config_sha256"] = sha256_file(sample_path)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    profile_path = config / "profile.json"
    profile_path.write_text(json.dumps({"model_alias": "fake-gemma", "temperature": 1.0,
        "top_p": 0.95, "top_k": 64, "seed": 42, "max_generation_tokens": 8192,
        "context_tokens": 32768, "reasoning_mode": "on"}), encoding="utf-8")
    return case_id, profile_path


def _initial_library(root: Path, library_dir: Path):
    registry = make_m4_registry(None, [], root=root)
    seed_library = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    seed_docs = _materialize_seeds_for_registry(seed_library, registry)
    provenance = {"implementation_fingerprint": implementation_fingerprint(),
        "m4_runtime_policy_sha256": sha256_file(root / "config/m4_runtime.json"),
        "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json")}
    return create_library_snapshot(seed_docs, [], [], provenance, library_dir,
                                   builtin_specs=registry.signatures())


def _empty_scorer_spec():
    return {"name": "evolved.test_empty_scorer", "version": "1",
        "description": "Test-only empty scorer for stage transition and admission tests.",
        "family": "test_family",
        "input_ports": [{"name": "candidates", "type": "CandidateSet"}],
        "output_type": "ScoredCandidates", "parameters": {},
        "source": '''def operator(inputs, params, runtime):
    return {"$type": "ScoredCandidates", "$fields": {
        "candidates": {"$tuple": []}, "scorer": "evolved.test_empty_scorer",
        "status": "abstained", "status_details": {"$map": {}}}}
'''}


def _stage3_graph(root: Path):
    spec = _empty_scorer_spec()
    registry = make_m4_registry(None, [], root=root)
    versions = {}
    for row in registry.signatures():
        if row["name"] == "bank.answers.combine":
            if row["version"].startswith("1+m4."):
                versions[row["name"]] = row["version"]
        else:
            versions[row["name"]] = row["version"]
    dag = {"schema_version": "e4-dag/1", "nodes": [
        {"id": "data", "operator": "case.telemetry", "version": versions["case.telemetry"],
         "inputs": {}, "params": {}, "bindings": {}},
        {"id": "candidates", "operator": "bank.candidates.generate",
         "version": versions["bank.candidates.generate"], "inputs": {"data": "data"},
         "params": {}, "bindings": {}},
        {"id": "scores", "operator": spec["name"], "version": spec["version"],
         "inputs": {"candidates": "candidates"}, "params": {}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine",
         "version": versions["bank.answers.combine"], "inputs": {"scores": ["scores"]},
         "params": {"limit": 5}, "bindings": {}}], "return": {"node": "answer"}}
    return spec, dag


class FakeClient:
    base_url = "http://127.0.0.1:8080"
    context_tokens = 32768
    max_generation_tokens = 8192
    profile = {"model_alias": "fake-gemma"}

    def __init__(self, responses):
        self.responses = list(responses)
        self.prompts = []

    def complete(self, prompt, *, task_id, attempt):
        self.prompts.append(prompt)
        scripted = self.responses.pop(0)
        error = scripted.get("_error") if isinstance(scripted, dict) else None
        truncated = scripted.get("_truncated") if isinstance(scripted, dict) else None
        content = (truncated if truncated is not None else
                   "" if error is not None else
                   json.dumps(scripted) if isinstance(scripted, dict) else scripted)
        now = datetime.now(timezone.utc).isoformat()
        raw = {"choices": [{"message": {"content": content}}]}
        return ModelCall(f"fake-{len(self.prompts)}", task_id, attempt, now, now,
            0.02, 0.01, 50, 32768, (), {"messages": [{"role": "user", "content": prompt}]},
            "synthetic-prompt-hash", 200, json.dumps(raw), raw, content, None,
            {"prompt_tokens": 50, "completion_tokens": 40},
            {"finish_reason": "length" if truncated is not None else "stop"}, error)


class FakeFeedback:
    def __init__(self, values=None, *, expected_component=None):
        self.values = list(values or [])
        self.expected_component = expected_component
        self.seen_case_ids = []

    def evaluate(self, case_id, answer):
        self.seen_case_ids.append(case_id)
        if self.expected_component is not None:
            expected = self.expected_component.strip().casefold()
            return {"accepted": any(row.entity.identifier.strip().casefold() == expected
                                    for row in answer.leads[:3])}
        return {"accepted": self.values.pop(0)}


class M4ControllerTests(unittest.TestCase):
    def test_stage3_candidate_is_not_executed_when_final_static_validation_then_call_fail(self):
        import e4core.m4_controller as controller

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed_doc = _plain(library.playbooks["combined_shared_candidates"]["document"])
            spec = _typed_scorer_spec()
            stage3_dag = _generated_playbook(root)["dag"]
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed_doc, "runtime_overrides": {}}),
                json.dumps({"operators": [spec], "dag": stage3_dag, "runtime_overrides": {}}),
                {"_error": {"code": "transport_error"}}])
            original_validate = controller._validate_graph
            calls = 0

            def fail_final_stage3(*args, **kwargs):
                nonlocal calls
                calls += 1
                # Stage 1, Stage 2, then the first Stage 3 validation all pass.
                # Fail the final Stage 3 check after the candidate graph has verified.
                if calls == 4:
                    raise M4ControllerError("synthetic_final_static_failure",
                        "injected final static validation failure", static=True)
                return original_validate(*args, **kwargs)

            with patch.object(controller, "_validate_graph", side_effect=fail_final_stage3):
                result = run_diagnostic(case_id, library_root=library.root,
                    run_dir=root / "atomic_stage3", feedback=FakeFeedback([False, False]),
                    profile_path=profile, client=client, root=root)

            stage3 = result["result"]["stages"][2]
            self.assertEqual(calls, 4)
            self.assertEqual(stage3["status"], "model_call_failed")
            self.assertEqual(stage3["static_errors"][0]["code"], "synthetic_final_static_failure")
            self.assertEqual(len(client.prompts), 4)
            self.assertIn("STATIC CONTRACT ERROR ONLY", client.prompts[-1])
            self.assertFalse((root / "atomic_stage3/attempts/stage3_synthesize/execution.json").exists())
            self.assertNotIn("stage3_synthesize", [row.get("stage") for row in result["result"]["stages"]
                if row.get("status") == "accepted"])
            self.assertFalse(any(key.startswith("evolved.synthetic_metric_score@")
                                 for key in result["snapshot"].operators))

    def test_repair_prompt_keeps_stage_contract_but_drops_large_digest_and_history(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _case_id, _profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            registry = make_m4_registry(None, generated_specs_from_snapshot(library), root=root)
            policy = json.loads((root / "config/m4_runtime.json").read_text(encoding="utf-8"))
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
                "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
                "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=root)
            from e4core.m4_data import M4QueryBudget
            budget = M4QueryBudget(None)
            huge_digest = {"payload_marker": "DIGEST_SENTINEL_" + "x" * 100_000}
            huge_history = [{"previous_result_marker": "HISTORY_SENTINEL_" + "y" * 100_000}]
            initial = _stage_prompt("synthesize", digest=huge_digest, registry=registry,
                snapshot=library, runtime=runtime, history=huge_history, policy=policy,
                query_budget=budget)
            compact = _stage_prompt("synthesize", digest={}, registry=registry,
                snapshot=library, runtime=runtime, history=[], policy=policy,
                query_budget=budget, include_case_digest=False, include_history=False)
            artifact = json.dumps({"operators": [{"name": "evolved.example",
                "source": "def operator(inputs, params, runtime): return inputs['data']"}],
                "dag": {"schema_version": "e4-dag/1", "nodes": []}})
            repair = _static_repair_prompt(compact, artifact,
                {"code": "unknown_operator", "message": "operator is not registered", "node_id": "score"})

            self.assertIn("DIGEST_SENTINEL_", initial)
            self.assertIn("HISTORY_SENTINEL_", initial)
            self.assertNotIn("DIGEST_SENTINEL_", repair)
            self.assertNotIn("HISTORY_SENTINEL_", repair)
            self.assertLess(len(repair), len(initial) // 2)
            self.assertIn("STATIC CONTRACT ERROR ONLY", repair)
            self.assertIn("unknown_operator", repair)
            self.assertIn("operator is not registered", repair)
            self.assertIn("evolved.example", repair)
            self.assertIn("def operator(inputs, params, runtime)", repair)
            self.assertIn("NATIVE QUERY AND PROJECTION DATA LAYOUT", repair)
            self.assertIn("EXACT CORE CONTRACT FIELD SCHEMA", repair)
            self.assertIn("bank.answers.combine", repair)
            self.assertIn("Nontrivial numeric constants must be declared", repair)

    def test_static_repair_budget_is_two_then_controller_advances_stage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            seed = _plain(library.playbooks["combined_shared_candidates"]["document"])
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            client = FakeClient(["{invalid", "{still-invalid", "{again-invalid",
                json.dumps({"dag": seed, "runtime_overrides": {}})])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "repair_run", feedback=FakeFeedback([True]),
                profile_path=profile, client=client, root=root)
            self.assertEqual(len(client.prompts), 4)
            first = result["result"]["stages"][0]
            self.assertEqual(first["status"], "static_validation_failed")
            self.assertEqual(first["static_repairs"], 2)
            self.assertEqual(result["result"]["stages"][1]["stage"], "stage2_compose")
            self.assertTrue(result["result"]["accepted"])

    def test_unknown_runtime_binding_is_static_and_gets_one_repair(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            invalid = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {"answer": {"limit": "runtime.not_declared"}}}
            valid = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                     "node_parameters": {}, "node_bindings": {}}
            client = FakeClient([json.dumps(invalid), json.dumps(valid)])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "binding_repair", feedback=FakeFeedback([True]),
                profile_path=profile, client=client, root=root)
            self.assertTrue(result["result"]["accepted"])
            self.assertEqual(result["result"]["stages"][0]["static_repairs"], 1)
            self.assertIn("undeclared runtime", client.prompts[1].casefold())

    def test_model_failure_and_truncation_each_consume_one_stage_attempt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            client = FakeClient([{"_error": {"code": "transport_error"}},
                {"_truncated": "{\"dag\":"}, {"_error": {"code": "transport_error"}}])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "failed_stages", feedback=FakeFeedback([]),
                profile_path=profile, client=client, root=root)
            self.assertEqual(len(client.prompts), 3)
            self.assertEqual([row["status"] for row in result["result"]["stages"]],
                             ["model_call_failed", "truncated", "model_call_failed"])
            self.assertFalse(result["result"]["accepted"])

    def test_runtime_execution_failure_is_not_repaired_with_semantic_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed = _plain(library.playbooks["combined_shared_candidates"]["document"])
            trace_node = next(node for node in seed["nodes"] if node["id"] == "trace_scores")
            trace_node["inputs"]["reference"], trace_node["inputs"]["alert"] = (
                trace_node["inputs"]["alert"], trace_node["inputs"]["reference"])
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed, "runtime_overrides": {}}),
                {"_error": {"code": "transport_error"}}])
            feedback = FakeFeedback([False])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "runtime_failure", feedback=feedback,
                profile_path=profile, client=client, root=root)
            self.assertEqual(len(client.prompts), 3)
            self.assertEqual(result["result"]["stages"][1]["status"], "runtime_execution_failed")
            self.assertEqual(result["result"]["stages"][1]["static_repairs"], 0)
            self.assertEqual(feedback.seen_case_ids, [case_id])

    def test_trusted_evaluator_failure_aborts_without_feedback_repair(self):
        class Unavailable:
            def evaluate(self, *_args):
                raise RuntimeError("synthetic evaluator unavailable")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            client = FakeClient([json.dumps(selection)])
            with self.assertRaisesRegex(M4ControllerError, "trusted evaluator"):
                run_diagnostic(case_id, library_root=library.root,
                    run_dir=root / "feedback_failure", feedback=Unavailable(),
                    profile_path=profile, client=client, root=root)
            self.assertEqual(len(client.prompts), 1)
            record = json.loads((root / "feedback_failure/diagnostic_result.json").read_text())
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["model_calls"]["model_call_count"], 1)

    def test_runtime_overrides_are_typed_bounded_and_protected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            del profile
            from e4core.bank_adapter import load_bank_case
            case = load_bank_case(case_id, "dev", root=root)
            default = build_m4_runtime(case.alert, root=root)
            self.assertEqual(default.snapshot.values["analysis_weights"], {
                "metric": 1.0, "trace_latency": 1.0, "log_volume": 1.0})
            adjusted = build_m4_runtime(case.alert, root=root, overrides={
                "minimum_support": 24, "tail_quantile": 0.95,
                "analysis_weights": {"metric": 2.0}})
            self.assertEqual(adjusted.snapshot.values["minimum_support"], 24)
            self.assertEqual(adjusted.snapshot.values["analysis_weights"]["metric"], 2.0)
            self.assertEqual(adjusted.snapshot.values["analysis_weights"]["trace_latency"], 1.0)
            for bad in ({"max_nodes": 4}, {"minimum_support": True},
                        {"analysis_weights": {"flow": 1.0}},
                        {"block_minutes": 10, "minimum_independent_blocks": 4}):
                with self.assertRaises(M4ControllerError):
                    build_m4_runtime(case.alert, root=root, overrides=bad)

    def test_case_digest_balances_component_metrics_and_separate_app_kpi_context(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _ = _fixture_root(root)
            from e4core.bank_adapter import load_bank_case
            case = load_bank_case(case_id, "dev", root=root)
            digest = public_case_digest(case, root=root, max_series=4)
            self.assertTrue(digest["metric_series"])
            self.assertTrue(all(row["role"] == "component" for row in digest["metric_series"]))
            self.assertIn("app_kpi_context_series", digest)
            self.assertIn("primary_app_kpi_context", digest)
            self.assertIsNone(digest["primary_app_kpi_context"]["kpi_id"])
            self.assertIn("trace_series", digest)
            self.assertIn("source_timestamp_units_by_date", digest)
            self.assertNotIn("raw_rows", digest)

            duplicate_point = next(point for point in case.telemetry.metrics
                if point.table == "bank/metric_container.csv")
            duplicate_telemetry = Telemetry(case.telemetry.case_id,
                case.telemetry.reference_window, case.telemetry.alert_window,
                (*case.telemetry.metrics, duplicate_point), case.telemetry.spans,
                case.telemetry.deployment_catalog, case.telemetry.data_sha256,
                case.telemetry.file_sha256, case.telemetry.duplicate_counts,
                case.telemetry.quality_counts, case.telemetry.adapter_version,
                case.telemetry.file_bytes)
            duplicate_digest = public_case_digest(replace(case, telemetry=duplicate_telemetry),
                                                  root=root, max_series=4)
            duplicate_series = next(row for row in duplicate_digest["metric_series"]
                                    if row["metric"].startswith("bank/metric_container.csv"))
            self.assertEqual(duplicate_series["windows"]["reference"]["support"], 1)
            self.assertEqual(duplicate_series["windows"]["reference"]["raw_rows"], 2)
            self.assertEqual(duplicate_series["windows"]["reference"]["duplicate_rows_collapsed"], 1)

    def test_stage1_binding_replaces_stored_explicit_parameter_without_topology_change(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _ = _fixture_root(root)
            from e4core.bank_adapter import load_bank_case
            case = load_bank_case(case_id, "dev", root=root)
            registry = make_m4_registry(case, [], root=root)
            seeds = _materialize_seeds_for_registry(
                json.loads((root / "config/bank_seed_library.json").read_text()), registry)
            doc = json.loads(json.dumps(seeds["combined_shared_candidates"]))
            answer_node = next(node for node in doc["nodes"] if node["id"] == "answer")
            answer_node["params"]["limit"] = 4
            doc = json.loads(json.dumps(doc))
            store = root / "library_binding"
            snapshot = create_library_snapshot({"examples": seeds}, [],
                [{"name": "explicit_limit", "document": doc}],
                {"test_fixture": True}, store, builtin_specs=registry.signatures(),
                verified_playbook_graph_hashes={playbook_graph_hash(doc)})
            selection = {"playbook": "explicit_limit", "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {"answer": {"limit": "runtime.lead_count"}}}
            instantiated, _ = _apply_stage1_selection(selection, snapshot, registry)
            self.assertNotIn("limit", next(node for node in instantiated["nodes"]
                                            if node["id"] == "answer")["params"])
            runtime = build_m4_runtime(case.alert, root=root)
            verified = verify_dag(instantiated, registry, runtime)
            answer = next(node for node in verified.nodes if node.node_id == "answer")
            self.assertEqual(answer.bindings["limit"], "runtime.lead_count")
            resolved = resolve_parameters(answer.operator.parameters, answer.params,
                                          answer.bindings, runtime)
            self.assertEqual(resolved.values["limit"], 5)
            self.assertEqual(resolved.provenance["limit"], "runtime.lead_count")

    def test_stage1_accepts_early_and_stage2_reuses_shared_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            stage1_client = FakeClient([json.dumps(selection)])
            stage1_feedback = FakeFeedback([True])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "stage1_run", feedback=stage1_feedback,
                profile_path=profile, client=stage1_client, root=root)
            self.assertTrue(result["result"]["accepted"])
            self.assertEqual(len(stage1_client.prompts), 1)
            self.assertEqual(len(result["result"]["stages"]), 1)
            self.assertEqual(result["result"]["stages"][0]["stage"], "stage1_select")

            seed = _plain(library.playbooks["combined_shared_candidates"]["document"])
            stage2_client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed, "runtime_overrides": {}})])
            stage2_feedback = FakeFeedback([False, True])
            result2 = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "stage2_run", feedback=stage2_feedback,
                profile_path=profile, client=stage2_client, root=root)
            self.assertTrue(result2["result"]["accepted"])
            self.assertEqual(len(stage2_client.prompts), 2)
            stage1_exec = result2["result"]["stages"][0]["execution"]
            stage2_exec = result2["result"]["stages"][1]["execution"]
            self.assertGreater(stage2_exec["cache_hits"], 0)
            history_marker = "PRIOR ATTEMPT HISTORY (diagnostic outputs and binary accepted flags only)\n"
            history_text = stage2_client.prompts[1].split(history_marker, 1)[1]
            history_doc, _history_end = json.JSONDecoder().raw_decode(history_text)
            self.assertFalse(history_doc[-1]["accepted"])
            self.assertIn("bank.public.project", stage2_client.prompts[1])
            self.assertIn("requests_used", stage2_client.prompts[1])
            self.assertEqual(stage2_feedback.seen_case_ids, [case_id, case_id])

    def test_stage3_useful_synthesized_scorer_reaches_synthetic_cause_and_is_admitted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed_doc = _plain(library.playbooks["combined_shared_candidates"]["document"])
            spec = _typed_scorer_spec()
            stage3_dag = _generated_playbook(root)["dag"]
            responses = [json.dumps(selection), json.dumps({"dag": seed_doc, "runtime_overrides": {}}),
                         json.dumps({"operators": [spec], "dag": stage3_dag, "runtime_overrides": {}})]
            client = FakeClient(responses)
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "stage3_run", feedback=FakeFeedback(expected_component="Tomcat01"),
                profile_path=profile, client=client, root=root)
            self.assertTrue(result["result"]["accepted"])
            self.assertEqual(result["result"]["accepted_stage"], "stage3_synthesize")
            self.assertTrue(any(key.startswith("evolved.synthetic_metric_score@")
                                for key in result["snapshot"].operators))
            self.assertEqual([row["accepted"] for row in result["result"]["stages"]],
                             [False, False, True])

    def test_rejected_empty_stage3_operator_is_not_admitted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed_doc = _plain(library.playbooks["combined_shared_candidates"]["document"])
            spec, stage3_dag = _stage3_graph(root)
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed_doc, "runtime_overrides": {}}),
                json.dumps({"operators": [spec], "dag": stage3_dag, "runtime_overrides": {}})])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "stage3_rejected", feedback=FakeFeedback(expected_component="Tomcat01"),
                profile_path=profile, client=client, root=root)
            self.assertFalse(result["result"]["accepted"])
            self.assertFalse(any(key.startswith("evolved.test_empty_scorer@")
                                 for key in result["snapshot"].operators))

    def test_answer_guard_rejects_unobserved_and_app_kpi_identifiers(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _ = _fixture_root(root)
            from e4core.bank_adapter import load_bank_case
            case = load_bank_case(case_id, "dev", root=root)
        ref, alt = case.telemetry.reference_window, case.telemetry.alert_window
        evidence = Evidence("bank/metric_container.csv", alt, "test", 1.0,
            (EntityRef("component", "Tomcat01"),), 5, {"ranking_eligible": True})
        answer = RankedAnswer((ScoredCandidate(EntityRef("component", "tomcat01"), 1.0,
            None, (evidence,), "bank_metric"),))
        validate_bank_answer(answer, case)
        invented = RankedAnswer((ScoredCandidate(EntityRef("component", "not-observed"), 1.0,
            None, (evidence,), "bank_metric"),))
        app_kpi = RankedAnswer((ScoredCandidate(EntityRef("app_kpi", "ServiceTest1"), 1.0,
            None, (evidence,), "bank_metric"),))
        for invalid in (invented, app_kpi):
            with self.assertRaises(M4ControllerError):
                validate_bank_answer(invalid, case)


if __name__ == "__main__":
    unittest.main()
