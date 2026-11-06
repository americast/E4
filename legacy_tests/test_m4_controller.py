import json
import shutil
import tempfile
import unittest
import csv
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from e4core.bank_adapter import sha256_file
from e4core.bank_ops import bank_registry
from e4core.cache import implementation_fingerprint
from e4core.m4_controller import (M4ControllerError, _materialize_seeds_for_registry,
    _apply_stage1_selection, _plain, _stage_prompt, _static_repair_prompt, _canonical_hash,
    _candidate_spec_mapping, _normalize_gap_record_draft, _validate_gap_record,
    _generation_allowance,
    _policy, _bounded_plan_history, M4ModelClient, M4_SYSTEM_PROMPT,
    M4_EVOLUTION_SYSTEM_PROMPTS, _compact_registered_signatures,
    _expand_registered_signatures, _compact_runtime_history,
    _expand_runtime_values_delta, _bounded_answer_history,
    build_m4_runtime, public_case_digest, _expanded_expression_hashes,
    run_diagnostic, validate_bank_answer)
from e4core.m4_data import generated_sandbox_limits, make_m4_registry
from e4core.m4_operator_ir import normalize_operator_ir_spec, typed_operator_ir_schema
from e4core.m4_library import (create_library_snapshot, generated_specs_from_snapshot,
    load_library_snapshot, playbook_graph_hash, snapshot_library)
from e4core.model_client import ModelCall
from e4core.model_client import LocalModelClient
from e4core.contracts import EntityRef, Evidence, RankedAnswer, ScoredCandidate, Telemetry, Window
from e4core.dag import DAGValidationError, verify_dag
from e4core.runtime import resolve_parameters
from tests.test_bank_adapter import make_bank_fixture
from tests.test_m4_bootstrap import _composite_metric_scorer, _generated_playbook


ROOT = Path(__file__).resolve().parents[1]


def _fixture_root(root: Path, *, positive_metric_pipeline: bool = False,
                  date_key: str = "2021_03_04") -> tuple[str, Path]:
    config = root / "config"
    config.mkdir(parents=True)
    for name in ("bank_metric_kind_policy.json", "bank_dev_kpi_policy.json", "m4_runtime.json",
                 "core_runtime.json", "runtime_schema.json", "bank_seed_library.json",
                 "openrca_bank_source.json"):
        shutil.copy2(ROOT / "config" / name, config / name)
    case_id, manifest_path = make_bank_fixture(root, date_key=date_key)
    if positive_metric_pipeline:
        case_dir = root / "data/prepared/bank/public/cases" / case_id
        alert = json.loads((case_dir / "alert.json").read_text(encoding="utf-8"))
        relative = f"source_dates/{date_key}/metric/metric_container.csv"
        metric_path = case_dir / relative
        with metric_path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            rows = list(reader)
            header = list(reader.fieldnames or ())
        for window_name, lower, base in (
                ("reference", alert["analysis_window_start_epoch_ms_utc"], 20),
                ("alert", alert["alert_window_start_epoch_ms_utc"], 80)):
            for index in range(30):
                rows.append({"timestamp": str((lower + index * 60_000) // 1000),
                    "cmdb_id": "Tomcat01", "kpi_name": "OSLinux-CPU_CPU_CPUCpuUtil",
                    "value": str(base + (index % 3))})
        with metric_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        file_row = manifest["cases"][case_id]["files"][relative]
        file_row.update({"sha256": sha256_file(metric_path), "rows": len(rows),
            "size_bytes": metric_path.stat().st_size})
        clip_path = root / "data/prepared/bank/public/clip_manifest.jsonl"
        clip_rows = [json.loads(line) for line in clip_path.read_text(encoding="utf-8").splitlines() if line]
        clip = next(row for row in clip_rows if row["relative_path"] == relative)
        clip.update({"clip_sha256": sha256_file(metric_path), "clip_rows": len(rows),
            "clip_size_bytes": metric_path.stat().st_size})
        clip_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in clip_rows),
            encoding="utf-8")
        manifest["clip_manifest_sha256"] = sha256_file(clip_path)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
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


def _initial_library(root: Path, library_dir: Path, generated_specs=None):
    registry = make_m4_registry(None, [], root=root)
    seed_library = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    seed_docs = _materialize_seeds_for_registry(seed_library, registry)
    provenance = {"implementation_fingerprint": implementation_fingerprint(),
        "m4_runtime_policy_sha256": sha256_file(root / "config/m4_runtime.json"),
        "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json")}
    return create_library_snapshot(seed_docs, generated_specs or [], [], provenance, library_dir,
                                   builtin_specs=registry.signatures())


def _empty_scorer_spec(root: Path, name="evolved.test_empty_scorer"):
    return _typed_ir_scorer_spec(root, name=name)


def _stage3_graph(root: Path):
    spec = _empty_scorer_spec(root)
    dag = _typed_ir_playbook(root, spec)
    return spec, dag


def _typed_ir_scorer_spec(root: Path, *, name="evolved.window_mean_delta"):
    program = {"schema_version": "e4-typed-operator-ir/1", "nodes": [
        {"id": "components", "kernel": "series.component_metrics", "version": 1,
         "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                    "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
        {"id": "field", "kernel": "series.select_field", "version": 1,
         "inputs": {"series": "components"},
         "params": {"source_field": "OSLinux-CPU_CPU_CPUCpuUtil"}, "bindings": {}},
        {"id": "summary", "kernel": "series.reduce", "version": 1,
         "inputs": {"series": "field"}, "params": {"statistic": "mean"},
         "bindings": {"block_minutes": "runtime.block_minutes",
                      "minimum_blocks": "runtime.minimum_independent_blocks"}},
        {"id": "change", "kernel": "numbers.window_compare", "version": 1,
         "inputs": {"values": "summary"}, "params": {"operation": "difference"}, "bindings": {}},
        {"id": "positive", "kernel": "numbers.positive_part", "version": 1,
         "inputs": {"values": "change"}, "params": {}, "bindings": {}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": 1,
         "inputs": {"values": "positive"}, "params": {"method": "mean"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": 1,
         "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "result", "kernel": "scores.assemble", "version": 1,
         "inputs": {"candidates": "$input.candidates", "scores": "scores",
                    "evidence": "evidence"}, "params": {}, "bindings": {}}],
        "return": {"node": "result"}}
    raw = {"name": name, "version": "1",
        "description": "Compares block-level mean metric changes between trusted windows.",
        "family": "bank_window_mean_delta", "output_type": "ScoredCandidates", "parameters": {},
        "authoring_profile": "typed-operator-ir-v1",
        "program": program}
    return raw


def _typed_ir_playbook(root: Path, spec):
    spec = normalize_operator_ir_spec(spec, root=root) if "declared_version" not in spec else spec
    registry = make_m4_registry(None, [], root=root)
    versions = {row["name"]: row["version"] for row in registry.signatures()}
    return {"schema_version": "e4-dag/1", "nodes": [
        {"id": "data", "operator": "case.telemetry", "version": versions["case.telemetry"],
         "inputs": {}, "params": {}, "bindings": {}},
        {"id": "reference", "operator": "bank.metrics.aggregate", "version": versions["bank.metrics.aggregate"],
         "inputs": {"data": "data"}, "params": {"window": "reference"}, "bindings": {}},
        {"id": "alert", "operator": "bank.metrics.aggregate", "version": versions["bank.metrics.aggregate"],
         "inputs": {"data": "data"}, "params": {"window": "alert"}, "bindings": {}},
        {"id": "candidates", "operator": "bank.candidates.generate", "version": versions["bank.candidates.generate"],
         "inputs": {"data": "data", "aggregates": ["reference", "alert"]}, "params": {}, "bindings": {}},
        {"id": "new_analysis", "operator": spec["name"], "version": spec["version"],
         "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
         "params": {}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine", "version": versions["bank.answers.combine"],
         "inputs": {"scores": ["new_analysis"]}, "params": {"limit": 5}, "bindings": {}}],
        "return": {"node": "answer"}}


def _gap_record(root: Path, spec, prior_dag):
    registry = make_m4_registry(None, [], root=root)
    closest = next(row for row in registry.signatures() if row["name"] == "bank.candidates.generate")
    return {"schema_version": "e4-capability-gaps/1",
        "prior_plans": [{"stage": "stage2_compose", "dag_sha256": _canonical_hash(prior_dag)}],
        "capabilities": [{"operator_name": spec["name"],
            "operator_version": spec["version"],
            "gap_hypothesis": "The prior plan lacks the computation implemented by this scorer.",
            "closest_existing_operators": [{"name": closest["name"], "version": closest["version"]}],
            "computational_difference": "Scores candidates using an additional metric comparison.",
            "intended_contribution": "Adds typed metric evidence to candidate ordering.",
            "required_inputs": ["reference", "alert", "candidates"],
            "coverage_requirement": "Use complete paired-window metric support or report incomplete coverage.",
            "validation_expectation": "Exercise a positive candidate and an empty candidate set."}]}


def _helper_typed_scorer_spec(root: Path):
    return _typed_ir_scorer_spec(root, name="evolved.synthetic_metric_score")


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
    def test_exact_expanded_computation_gate_ignores_macro_ids_but_keeps_ports_and_params(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            registry = make_m4_registry(None, [], root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
                "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
                "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=root)
            versions = {row["name"]: row["version"] for row in registry.signatures()}

            def graph(prefix, family, *, reference_window="reference", swap=False,
                      candidate_aggregates=False):
                aggregate = "bank.metrics.aggregate" if family == "metric" else "bank.traces.aggregate"
                scorer = "bank.metrics.contrast" if family == "metric" else "bank.traces.latency_contrast"
                reference, alert = ((prefix + "_alt", prefix + "_ref") if swap else
                    (prefix + "_ref", prefix + "_alt"))
                return {"schema_version": "e4-dag/1", "nodes": [
                    {"id": prefix + "_data", "operator": "case.telemetry", "version": "1",
                     "inputs": {}, "params": {}, "bindings": {}},
                    {"id": prefix + "_ref", "operator": aggregate, "version": versions[aggregate],
                     "inputs": {"data": prefix + "_data"}, "params": {"window": reference_window}, "bindings": {}},
                    {"id": prefix + "_alt", "operator": aggregate, "version": versions[aggregate],
                     "inputs": {"data": prefix + "_data"}, "params": {"window": "alert"}, "bindings": {}},
                    {"id": prefix + "_candidates", "operator": "bank.candidates.generate", "version": "1",
                     "inputs": ({"data": prefix + "_data", "aggregates": [prefix + "_ref", prefix + "_alt"]}
                         if candidate_aggregates else {"data": prefix + "_data"}),
                     "params": {}, "bindings": {}},
                    {"id": prefix + "_scores", "operator": scorer, "version": versions[scorer],
                     "inputs": {"reference": reference, "alert": alert,
                         "candidates": prefix + "_candidates"}, "params": {}, "bindings": {}}],
                    "return": {"node": prefix + "_scores"}}

            runtime_values = runtime.as_dict()["values"]
            data_pin = "test-case-data-sha256"
            for family in ("metric", "trace"):
                prior = graph("prior", family)
                expanded_macro = graph("macro_generated_id", family)
                prior_hashes = _expanded_expression_hashes(prior, registry, runtime_values,
                    case_data_sha256=data_pin)
                candidate_hashes = _expanded_expression_hashes(expanded_macro, registry, runtime_values,
                    case_data_sha256=data_pin)
                self.assertEqual(prior_hashes["prior_scores"], candidate_hashes["macro_generated_id_scores"])
                with_optional_unused_port = graph("optional_port", family, candidate_aggregates=True)
                self.assertEqual(prior_hashes["prior_scores"], _expanded_expression_hashes(
                    with_optional_unused_port, registry, runtime_values,
                    case_data_sha256=data_pin)["optional_port_scores"])
                changed_param = graph("changed", family, reference_window="alert")
                changed_wiring = graph("rewired", family, swap=True)
                self.assertNotEqual(prior_hashes["prior_scores"],
                    _expanded_expression_hashes(changed_param, registry, runtime_values,
                        case_data_sha256=data_pin)["changed_scores"])
                self.assertNotEqual(prior_hashes["prior_scores"],
                    _expanded_expression_hashes(changed_wiring, registry, runtime_values,
                        case_data_sha256=data_pin)["rewired_scores"])

            query = {"schema_version": "e4-dag/1", "nodes": [
                {"id": "data", "operator": "case.telemetry", "version": "1",
                 "inputs": {}, "params": {}, "bindings": {}},
                {"id": "page", "operator": "bank.public.query", "version": "1",
                 "inputs": {"data": "data"}, "params": {"table": "metric/metric_container.csv",
                    "window": "alert"}, "bindings": {}},
                {"id": "dependent", "operator": "bank.candidates.generate", "version": "1",
                 "inputs": {"data": "page"}, "params": {}, "bindings": {}}],
                "return": {"node": "dependent"}}
            query_hashes = _expanded_expression_hashes(query, registry, runtime_values,
                case_data_sha256=data_pin)
            self.assertIsNone(query_hashes["page"])
            self.assertIsNone(query_hashes["dependent"])

            # The answer combiner's explicit value and effective runtime binding
            # are computationally identical when they resolve to the same value.
            scorer = graph("base", "metric")
            versions = {row["name"]: row["version"] for row in registry.signatures()}
            score_id = scorer["return"]["node"]
            answer_version = next(row["version"] for row in registry.signatures()
                if row["name"] == "bank.answers.combine" and row["version"].startswith("1+m4."))
            explicit = {"schema_version": "e4-dag/1", "nodes": scorer["nodes"] + [
                {"id": "answer_explicit", "operator": "bank.answers.combine", "version": answer_version,
                 "inputs": {"scores": [score_id]}, "params": {"limit": runtime_values["lead_count"]},
                 "bindings": {}}], "return": {"node": "answer_explicit"}}
            bound = {"schema_version": "e4-dag/1", "nodes": scorer["nodes"] + [
                {"id": "answer_bound", "operator": "bank.answers.combine", "version": answer_version,
                 "inputs": {"scores": [score_id]}, "params": {},
                 "bindings": {"limit": "runtime.lead_count"}}], "return": {"node": "answer_bound"}}
            self.assertEqual(_expanded_expression_hashes(explicit, registry, runtime_values,
                case_data_sha256=data_pin)["answer_explicit"],
                _expanded_expression_hashes(bound, registry, runtime_values,
                    case_data_sha256=data_pin)["answer_bound"])
            changed_runtime = dict(runtime_values)
            changed_runtime["lead_count"] += 1
            self.assertNotEqual(_expanded_expression_hashes(bound, registry, runtime_values,
                case_data_sha256=data_pin)["answer_bound"],
                _expanded_expression_hashes(bound, registry, changed_runtime,
                    case_data_sha256=data_pin)["answer_bound"])

    def test_evolution_prompts_use_phase_system_and_contract_guidance(self):
        client = M4ModelClient({"model_alias": "fake-gemma", "temperature": 1.0,
            "top_p": 0.95, "top_k": 64, "seed": 42,
            "max_generation_tokens": 8192, "context_tokens": 32768})
        select_payload = client.payload_for_stage("select", "select user text")
        base_payload = client.payload_for_stage("base", "base user text")
        self.assertEqual(select_payload["messages"][0]["content"], M4_SYSTEM_PROMPT)
        self.assertEqual(base_payload["messages"][0]["content"], M4_SYSTEM_PROMPT)
        self.assertNotIn("Evolution stage", select_payload["messages"][0]["content"])
        self.assertIn("Stage 2 composition", M4_EVOLUTION_SYSTEM_PROMPTS["compose"])
        self.assertIn("no generated source", M4_EVOLUTION_SYSTEM_PROMPTS["compose"])
        self.assertNotIn("contract_fields", M4_EVOLUTION_SYSTEM_PROMPTS["compose"])
        evolve_system = client.payload_for_stage("evolve", "user text")["messages"][0]["content"]
        for phrase in ("structured gap record", "failure or limitation",
                       "fixed low-level typed kernels", "Do not diagnose", "typed-operator-ir-v1",
                       "No Python", "genuinely new typed numerical analysis"):
            self.assertIn(phrase, evolve_system)
        synth_system = client.payload_for_stage("stage3_serve", "user text")["messages"][0]["content"]
        for phrase in ("Use the validated gap record", "registered operator signatures",
                       "atomic node in the serving DAG",
                       "up to five leads", "first three", "no-match is not full-window absence"):
            self.assertIn(phrase, synth_system)
        self.assertEqual(M4_EVOLUTION_SYSTEM_PROMPTS["select"] if "select" in M4_EVOLUTION_SYSTEM_PROMPTS
                         else M4_SYSTEM_PROMPT, M4_SYSTEM_PROMPT)

    def test_select_user_prompt_and_evolution_response_grammar(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            registry = make_m4_registry(None, generated_specs_from_snapshot(library), root=root)
            policy = json.loads((root / "config/m4_runtime.json").read_text(encoding="utf-8"))
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
                "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
                "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=root)
            from e4core.m4_data import M4QueryBudget
            args = {"digest": {}, "registry": registry, "snapshot": library,
                "runtime": runtime, "history": [], "policy": policy,
                "query_budget": M4QueryBudget(None)}
            select = _stage_prompt("select", **args)
            self.assertIn("Use supplied runtime overrides only; do not invent threshold constants.", select)
            self.assertNotIn("genuinely distinct", select)
            compose = _stage_prompt("compose", **args)
            evolve = _stage_prompt("evolve", **args)
            synth = _stage_prompt("stage3_serve", gap_record={"schema_version": "e4-capability-gaps/1"}, **args)
            for prompt in (compose, synth):
                self.assertIn("runtime_overrides:object", prompt)
                self.assertIn("Only named, bounded runtime overrides", prompt)
                self.assertNotIn("runtime_overrides:{}}", prompt)
            self.assertIn("registered operator catalog only", compose)
            self.assertIn("DATA COVERAGE GUIDANCE", compose)
            self.assertIn("TRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS", compose)
            self.assertIn("bank_public_raw_page", compose)
            self.assertIn("bucket_samples", compose)
            self.assertIn("This call does not diagnose the incident", evolve)
            self.assertIn("exactly one top-level object with exactly two sibling fields", evolve)
            self.assertIn("`operators` MUST be a sibling of `gap_record`", evolve)
            self.assertIn("DATA COVERAGE GUIDANCE", evolve)
            self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG", evolve)
            self.assertIn("typed-operator-ir-v1", evolve)
            self.assertIn("ComponentScores", evolve)
            self.assertIn("series.correlate@1", evolve)
            self.assertNotIn("operator-dag/1", evolve)
            self.assertNotIn('"source":', evolve)
            self.assertIn("TRUSTED INPUT PRODUCER CAPABILITIES", evolve)
            self.assertIn("Aggregate.rows is empty", evolve)
            self.assertIn("row_requirements", evolve)
            self.assertIn("Do not return new specs or change the gap record", synth)
            self.assertIn("Each newly synthesized typed IR operator is one atomic registered call", synth)
            self.assertIn("DATA COVERAGE GUIDANCE", synth)
            self.assertIn("TRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS", synth)
            self.assertIn("public-query pages", synth)
            self.assertIn("no-match is not full-window absence", synth)
            self.assertIn("genuinely new typed numerical analysis", M4_EVOLUTION_SYSTEM_PROMPTS["evolve"])
            private_history_prompt = _stage_prompt("evolve", **{**args,
                "history": [{"stage": "stage2_compose", "_expanded_plan": {
                    "sentinel_private_runtime_graph": "must-not-leak"}}]})
            self.assertNotIn("sentinel_private_runtime_graph", private_history_prompt)

    def test_plan_history_is_bounded_nonmutating_and_keeps_effective_overrides(self):
        plan = {"dag_sha256": "a" * 64, "full_summary_sha256": "b" * 64,
            "nodes": [{"id": f"node_{i}", "operator": "bank.example", "version": "1",
                "inputs": {"source": "node_0"}, "params": {"window": "a" * 500},
                "bindings": {}} for i in range(100)], "return": {"node": "node_99"},
            "runtime_overrides": {"public_query_limit": 128}}
        original = json.dumps(plan, sort_keys=True)
        bounded = _bounded_plan_history([plan], max_bytes=768)
        self.assertEqual(json.dumps(plan, sort_keys=True), original)
        self.assertLessEqual(len(json.dumps(bounded, sort_keys=True,
            separators=(",", ":")).encode()), 768)
        self.assertIn("nodes_omitted", bounded[0])
        self.assertEqual(bounded[0]["dag_sha256"], "a" * 64)
        self.assertEqual(bounded[0]["runtime_overrides"], {"public_query_limit": 128})
        minimal = _bounded_plan_history([plan], max_bytes=272)
        self.assertLessEqual(len(json.dumps(minimal, sort_keys=True,
            separators=(",", ":")).encode()), 272)
        self.assertEqual(minimal[0]["dag_sha256"], "a" * 64)
        self.assertTrue(minimal[0]["runtime_overrides_omitted"])
        self.assertTrue(minimal[0]["return_omitted"])

    def test_gap_record_requires_real_history_registry_and_declared_port_references(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _case_id, _profile = _fixture_root(root)
            spec, _dag = _stage3_graph(root)
            registry = make_m4_registry(None, [], root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1, "alert_window_end_epoch_ms_utc": 2}, root=root)
            normalized, _versions = _candidate_spec_mapping([spec], registry, runtime, [])
            prior_dag = {"schema_version": "e4-dag/1", "nodes": [], "return": {"node": "x"}}
            gap = _gap_record(root, spec, prior_dag)
            history = [{"stage": "stage2_compose", "plan_summary": {
                "dag_sha256": _canonical_hash(prior_dag)}}]
            self.assertEqual(_validate_gap_record(gap, normalized, registry, history), gap)

            for field in ("operator_name", "operator_version"):
                for invalid_identity in (["unhashable"], {"unhashable": True}, None):
                    malformed = json.loads(json.dumps(gap))
                    malformed["capabilities"][0][field] = invalid_identity
                    with self.subTest(field=field, identity=invalid_identity):
                        with self.assertRaises(M4ControllerError) as raised:
                            _validate_gap_record(malformed, normalized, registry, history)
                        self.assertTrue(raised.exception.static)

            bad_plan = json.loads(json.dumps(gap))
            bad_plan["prior_plans"][0]["stage"] = "compose"
            with self.assertRaisesRegex(M4ControllerError, "prior plan reference"):
                _validate_gap_record(bad_plan, normalized, registry, history)
            bad_operator = json.loads(json.dumps(gap))
            bad_operator["capabilities"][0]["closest_existing_operators"][0]["name"] = "bank.unknown"
            with self.assertRaisesRegex(M4ControllerError, "closest operator ref"):
                _validate_gap_record(bad_operator, normalized, registry, history)
            bad_port = json.loads(json.dumps(gap))
            bad_port["capabilities"][0]["required_inputs"] = ["invented_port"]
            with self.assertRaisesRegex(M4ControllerError, "input port"):
                _validate_gap_record(bad_port, normalized, registry, history)

    def test_generated_token_budget_uses_actual_usage_and_requested_limit_fallback(self):
        calls = [
            {"usage": {"completion_tokens": 120}, "m4_requested_generation_tokens": 8192},
            {"usage": {}, "error": {"code": "transport_error"},
             "m4_requested_generation_tokens": 8192},
        ]
        self.assertEqual(_generation_allowance(calls, model_call_limit=10,
            token_budget=12000, per_call_limit=8192), 3688)
        self.assertEqual(_generation_allowance(calls, model_call_limit=2,
            token_budget=73728, per_call_limit=8192), 0)

    def test_m4_client_receives_clamped_generation_limit(self):
        client = M4ModelClient({"model_alias": "fake-gemma", "temperature": 1.0,
            "top_p": 0.95, "top_k": 64, "seed": 42,
            "max_generation_tokens": 8192, "context_tokens": 32768})
        calls = [{"usage": {"completion_tokens": 600},
                  "m4_requested_generation_tokens": 8192}]
        allowance = _generation_allowance(calls, model_call_limit=10,
            token_budget=1000, per_call_limit=8192)
        self.assertEqual(allowance, 400)
        payload = client.make_payload("bounded request", max_tokens=allowance)
        self.assertEqual(payload["max_tokens"], 400)
        with patch.object(LocalModelClient, "complete", autospec=True,
                          return_value="sentinel") as complete:
            result = client.complete_for_stage("evolve", "bounded request",
                task_id="phase-evolve", attempt=1, max_tokens=allowance)
        self.assertEqual(result, "sentinel")
        self.assertEqual(complete.call_args.kwargs["max_tokens"], 400)

    def test_stage3_repair_budget_is_shared_across_evolve_and_serve(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "per_trace_native_duration", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed = _plain(library.playbooks["per_trace_native_duration"]["document"])
            spec, dag = _stage3_graph(root)
            evolution = {"gap_record": _gap_record(root, spec, seed), "operators": [spec]}
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed, "runtime_overrides": {}}),
                json.dumps({"gap_record": {}, "operators": []}),
                json.dumps(evolution),
                json.dumps({"operators": []}),
                json.dumps({"operators": []})])
            feedback = FakeFeedback([False, False])
            run_dir = root / "shared_repair_budget"
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=run_dir, feedback=feedback, profile_path=profile,
                client=client, root=root)
            stage3 = result["result"]["stages"][-1]
            self.assertEqual(stage3["status"], "static_validation_failed")
            self.assertEqual(stage3["static_repairs"], 2)
            self.assertEqual(stage3["evolve_static_repairs"], 1)
            self.assertEqual(stage3["stage3_serve_static_repairs"], 1)
            self.assertEqual(len(client.prompts), 6)
            self.assertEqual(feedback.seen_case_ids, [case_id, case_id])
            self.assertTrue(list((run_dir / "operators").glob("evolved_test_empty_scorer_*.json")))
            self.assertTrue((run_dir / "evolution_contribution_diagnostics.json").is_file())

    def test_new_evolution_rejects_python_source_before_candidate_registration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {}}
            seed = _plain(library.playbooks["combined_shared_candidates"]["document"])
            python_spec = {"name": "evolved.rejected_python", "version": "1",
                "description": "invalid new-model source", "family": "fixture",
                "input_ports": [{"name": "data", "type": "Aggregate"}],
                "output_type": "Aggregate", "parameters": {},
                "source": "def operator(inputs, params, runtime): return inputs['data']"}
            invalid = {"gap_record": {}, "operators": [python_spec]}
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed, "runtime_overrides": {}}),
                json.dumps(invalid), json.dumps(invalid), json.dumps(invalid)])
            feedback = FakeFeedback([False, False])
            run_dir = root / "python_rejection"
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=run_dir, feedback=feedback, profile_path=profile,
                client=client, root=root)
            stage3 = result["result"]["stages"][-1]
            self.assertEqual(stage3["status"], "evolution_failed")
            self.assertEqual(stage3["evolve_static_repairs"], 2)
            self.assertEqual(stage3["static_errors"][0]["code"], "stage3_operator_profile")
            self.assertFalse((run_dir / "attempts/stage3_synthesize/execution.json").exists())
            self.assertEqual(list((run_dir / "operators").glob("*.py")), [])
            self.assertEqual(list((run_dir / "operators").glob("*.json")), [])
            self.assertNotIn("def operator(inputs, params, runtime)", client.prompts[-1])
            self.assertIn("rejected executable source omitted", client.prompts[-1])

    def test_composite_identifier_guard_checks_defaults_but_allows_runtime_values(self):
        from e4core.m4_controller import _reject_program_identifier_literals
        composite = _composite_metric_scorer(ROOT)
        composite["parameters"] = {"target": {"type": "string", "default": "Tomcat01"}}
        with self.assertRaises(M4ControllerError) as raised:
            _reject_program_identifier_literals(composite, {"Tomcat01"})
        self.assertEqual(raised.exception.code, "composite_identifier_literal")

        composite["parameters"] = {"target": {"type": "string", "runtime_field": "target_id"}}
        _reject_program_identifier_literals(composite, {"Tomcat01"})

    def test_invalid_gap_repair_replaces_candidate_before_serving(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "per_trace_native_duration", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed = _plain(library.playbooks["per_trace_native_duration"]["document"])
            stale_spec, _ = _stage3_graph(root)
            current_spec = json.loads(json.dumps(stale_spec))
            stale_spec["name"] = "evolved.stale_candidate"
            stale_spec["description"] = "Candidate from a malformed gap proposal."
            current_spec["name"] = "evolved.repaired_candidate"
            current_spec["description"] = "Candidate from the repaired, registered gap proposal."
            invalid_gap = _gap_record(root, stale_spec, seed)
            invalid_gap["prior_plans"][0]["stage"] = "invented_stage"
            valid_gap = _gap_record(root, current_spec, seed)
            _base_spec, dag = _stage3_graph(root)
            score_node = next(node for node in dag["nodes"] if node["id"] == "new_analysis")
            score_node["operator"] = current_spec["name"]
            score_node["version"] = current_spec["version"]
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed, "runtime_overrides": {}}),
                json.dumps({"gap_record": invalid_gap, "operators": [stale_spec]}),
                json.dumps({"gap_record": valid_gap, "operators": [current_spec]}),
                json.dumps({"dag": dag, "runtime_overrides": {}}),
                json.dumps({"dag": dag, "runtime_overrides": {}}),
                json.dumps({"dag": dag, "runtime_overrides": {}})])
            feedback = FakeFeedback([False, False, False])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "gap_repair", feedback=feedback,
                profile_path=profile, client=client, root=root)
            self.assertEqual(result["result"]["stages"][-1]["evolve_static_repairs"], 1)
            self.assertIn(current_spec["name"], client.prompts[-1])
            self.assertNotIn(stale_spec["name"], client.prompts[-1])
            self.assertEqual(feedback.seen_case_ids, [case_id, case_id, case_id])
            self.assertTrue(any(current_spec["name"].replace(".", "_") in path.name
                for path in (root / "gap_repair/operators").glob("*.json")))

    def test_evolution_signature_catalog_omits_only_roundtrippable_defaults(self):
        base_signature = {"name": "bank.example", "version": "2", "family": "example",
            "description": "Meaningful description remains verbatim.", "output_type": "Aggregate",
            "external_input": False,
            "inputs": [{"name": "data", "type": "Aggregate", "required": True,
                "variadic": False, "minimum_items": 1, "maximum_items": None},
                {"name": "extras", "type": "CandidateSet", "required": False,
                 "variadic": True, "minimum_items": 0, "maximum_items": 3}],
            "parameters": {"pattern": {"type": "string", "required": True,
                "nullable": True, "has_default": True, "default": "x", "choices": ["x", "y"],
                "runtime_field": "pattern", "estimator": "fixed", "description": "Keep this."},
                "limit": {"type": "positive_integer", "required": False,
                "nullable": False, "has_default": False, "default": None, "choices": [],
                "runtime_field": None, "estimator": None, "description": ""}}}
        signatures = [_plain({**base_signature, "name": f"bank.example_{index}"})
                      for index in range(4)]
        compact = _compact_registered_signatures(signatures)
        self.assertEqual(_expand_registered_signatures(compact), _plain(signatures))
        self.assertLess(len(json.dumps(compact, separators=(",", ":"))),
                        len(json.dumps(signatures, separators=(",", ":"))))
        encoded_parameter = compact["operators"][0]["parameters"]["pattern"]
        self.assertEqual(encoded_parameter["choices"], ["x", "y"])
        self.assertEqual(encoded_parameter["runtime_field"], "pattern")
        self.assertEqual(compact["operators"][0]["description"], signatures[0]["description"])
        registered = make_m4_registry(None, [], root=ROOT).signatures()
        compact_registered = _compact_registered_signatures(registered)
        self.assertEqual(_expand_registered_signatures(compact_registered), _plain(registered))
        self.assertLess(len(json.dumps(compact_registered, separators=(",", ":"))),
            len(json.dumps(_plain(registered), separators=(",", ":"))))

    def test_prior_runtime_history_is_losslessly_delta_encoded_against_current_values(self):
        current = {"seed": 42, "minimum_support": 12,
                   "analysis_weights": {"metric": 1.0, "trace_latency": 1.0}}
        prior = {"seed": 42, "minimum_support": 8,
                 "analysis_weights": {"metric": 2.0, "trace_latency": 1.0},
                 "legacy_extra": "preserve"}
        rows = [{"stage": "stage2_compose", "runtime_values": prior,
                 "plan_summary": {"dag_sha256": "a" * 64}}]
        compact = _compact_runtime_history(rows, current)
        self.assertNotIn("runtime_values", compact[0])
        self.assertEqual(compact[0]["plan_summary"], rows[0]["plan_summary"])
        reconstructed = _expand_runtime_values_delta(compact[0]["runtime_values_delta"], current)
        self.assertEqual(reconstructed, prior)
        with self.assertRaises(ValueError):
            _expand_runtime_values_delta(compact[0]["runtime_values_delta"], {**current, "seed": 43})

    def test_answer_history_keeps_eligible_support_unit_first_with_one_item(self):
        window = Window("alert", 10, 20)
        entity = EntityRef("component", "Tomcat01")
        ineligible = Evidence("bank/metric_container.csv", window, "median", 1.0,
            (entity,), 20, {"ranking_eligible": False, "support_unit": "raw_timestamps"})
        eligible = Evidence("bank/metric_container.csv", window, "block_contrast", 1.0,
            (entity,), 4, {"ranking_eligible": True, "support_unit": "independent_time_blocks"})
        answer = RankedAnswer((ScoredCandidate(entity, 1.0, None,
            (ineligible, eligible), "bank_metric"),))
        history = _bounded_answer_history(answer)
        evidence = history["leads"][0]["evidence_excerpt"]
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]["details"]["support_unit"], "independent_time_blocks")
        self.assertEqual(history["leads"][0]["eligible_evidence_count"], 1)
        self.assertEqual(history["leads"][0]["evidence_omitted"], 1)

    def test_static_repair_retains_bounded_prior_plan_and_phase_guidance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            registry = make_m4_registry(None, generated_specs_from_snapshot(library), root=root)
            policy = json.loads((root / "config/m4_runtime.json").read_text(encoding="utf-8"))
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
                "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
                "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=root)
            from e4core.m4_data import M4QueryBudget
            plan = {"dag_sha256": "c" * 64, "full_summary_sha256": "d" * 64,
                "nodes": [{"id": "score", "operator": "evolved.prior", "version": "1",
                    "inputs": {"data": "query"}, "params": {"method": "window_delta"},
                    "bindings": {}}], "return": {"node": "answer"},
                "runtime_overrides": {}}
            history = [{"stage": "stage2_compose", "accepted": False,
                        "plan_summary": plan}]
            repair_context = _stage_prompt("synthesize", digest={}, registry=registry,
                snapshot=library, runtime=runtime, history=history, policy=policy,
                query_budget=M4QueryBudget(None), include_case_digest=False)
            repair = _static_repair_prompt(repair_context, '{"invalid":true}',
                {"code": "unknown_operator", "message": "missing registered capability"})
            self.assertIn("STAGE 3 SERVE PHASE", repair)
            self.assertIn("every proposed composite in the logical DAG", repair)
            self.assertEqual(repair.count("c" * 64), 1)
            self.assertIn("window_delta", repair)
            self.assertIn("STATIC CONTRACT ERROR ONLY", repair)
            self.assertNotIn("runtime_execution_failed", repair)

    def test_m4_preflight_counts_stage_specific_system_before_generation(self):
        client = M4ModelClient({"model_alias": "fake-gemma", "temperature": 1.0,
            "top_p": 0.95, "top_k": 64, "seed": 42,
            "max_generation_tokens": 8192, "context_tokens": 32768})
        seen = []
        def fake_post(endpoint, payload):
            seen.append(endpoint)
            if endpoint == "/apply-template":
                return 200, "{}", {"prompt": payload["messages"][0]["content"] + payload["messages"][1]["content"]}, None
            if endpoint == "/tokenize":
                return 200, "{}", {"tokens": [1] * 24577}, None
            self.fail("completion endpoint must not be called after context preflight failure")
        with patch.object(client, "_post_json", side_effect=fake_post):
            call = client.complete_for_stage("synthesize", "small user prompt",
                task_id="test", attempt=1)
        self.assertEqual(call.error["code"], "context_budget_exceeded")
        self.assertEqual(seen, ["/apply-template", "/tokenize"])

    def test_paper_harness_system_prompts_match_calibrated_terminal_without_changing_legacy(self):
        profile = {"model_alias": "fake-gemma", "temperature": 1.0,
            "top_p": 0.95, "top_k": 64, "seed": 42,
            "max_generation_tokens": 8192, "context_tokens": 32768}
        legacy = M4ModelClient(profile)
        paper = M4ModelClient(profile, harness_profile="reconstructed-paper-starter-v1")
        for stage in ("compose", "stage3_serve"):
            legacy_text = legacy.payload_for_stage(stage, "prompt")["messages"][0]["content"]
            paper_text = paper.payload_for_stage(stage, "prompt")["messages"][0]["content"]
            self.assertIn("maximum raw score", legacy_text)
            self.assertNotIn("maximum raw score", paper_text)
            self.assertIn("normalize", paper_text.casefold())
            self.assertIn("trusted semantic group", paper_text.casefold())

    def test_m4_worker_resource_policy_is_m4_specific(self):
        policy = _policy(ROOT)
        self.assertEqual(policy["policy_name"], "bank-m4-controller-input16m-cpu10-wall15-v3")
        self.assertEqual(policy["query_and_generation"]["maximum_operator_input_bytes"],
                         16 * 1024 * 1024)
        self.assertEqual(generated_sandbox_limits(ROOT), {
            "input_bytes": 16 * 1024 * 1024, "cpu_seconds": 10, "wall_seconds": 15.0,
            "memory_bytes": 512 * 1024 * 1024})
        from e4core.sandbox import DEFAULT_LIMITS
        self.assertEqual(DEFAULT_LIMITS["input_bytes"], 2 * 1024 * 1024)

    def test_dag_grammar_accepts_empty_maps_scalar_and_variadic_references(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            registry = make_m4_registry(None, generated_specs_from_snapshot(library), root=root)
            dag = _plain(library.playbooks["combined_shared_candidates"]["document"])
            verified = verify_dag(dag, registry)
            self.assertEqual(verified.return_node, dag["return"]["node"])
            self.assertTrue(any(isinstance(ref, str) for node in dag["nodes"]
                                for ref in node["inputs"].values()))
            self.assertTrue(any(isinstance(refs, list) for node in dag["nodes"]
                                for refs in node["inputs"].values()))
            self.assertTrue(all(isinstance(node["inputs"], dict)
                                and isinstance(node["params"], dict)
                                and isinstance(node["bindings"], dict) for node in dag["nodes"]))

    def test_dag_map_type_errors_name_each_malformed_json_field(self):
        registry = make_m4_registry(None, [], root=ROOT)
        telemetry = next(row for row in registry.signatures() if row["name"] == "case.telemetry")
        document = {"schema_version": "e4-dag/1", "nodes": [{
            "id": "input", "operator": telemetry["name"], "version": telemetry["version"],
            "inputs": None, "params": [], "bindings": {}}], "return": {"node": "input"}}
        with self.assertRaises(DAGValidationError) as raised:
            verify_dag(document, registry)
        self.assertEqual(raised.exception.code, "node_maps")
        self.assertEqual(raised.exception.node_id, "input")
        self.assertIn("inputs=null", str(raised.exception))
        self.assertIn("params=array", str(raised.exception))

    def test_version_resolution_rejects_unhashable_operator_identity_as_static_error(self):
        from e4core.m4_controller import _resolve_generated_versions
        document = {"schema_version": "e4-dag/1", "nodes": [{
            "id": "bad", "operator": [], "version": {"x": 1},
            "inputs": {}, "params": {}, "bindings": {}}], "return": {"node": "bad"}}
        with self.assertRaises(M4ControllerError) as raised:
            _resolve_generated_versions(document, {})
        self.assertTrue(raised.exception.static)
        self.assertEqual(raised.exception.code, "node_identity")

    def test_stage3_candidate_is_not_executed_when_final_static_validation_then_call_fail(self):
        import e4core.m4_controller as controller

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed_doc = _plain(library.playbooks["combined_shared_candidates"]["document"])
            spec = _helper_typed_scorer_spec(root)
            normalized_spec = normalize_operator_ir_spec(spec, root=root)
            stage3_dag = _typed_ir_playbook(root, spec)
            evolution = {"gap_record": _gap_record(root, spec, seed_doc), "operators": [spec]}
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed_doc, "runtime_overrides": {}}),
                json.dumps(evolution),
                json.dumps({"dag": stage3_dag, "runtime_overrides": {}}),
                {"_error": {"code": "transport_error"}}])
            original_validate = controller._validate_graph
            calls = 0

            def fail_final_stage3(*args, **kwargs):
                nonlocal calls
                calls += 1
                # The logical Stage 3 plan passes; reject its expanded plan.
                if kwargs.get("stage") == "stage3_serve" and kwargs.get("new_operator_keys") is None:
                    raise M4ControllerError("synthetic_final_static_failure",
                        "injected final static validation failure", static=True)
                return original_validate(*args, **kwargs)

            with patch.object(controller, "_validate_graph", side_effect=fail_final_stage3):
                result = run_diagnostic(case_id, library_root=library.root,
                    run_dir=root / "atomic_stage3", feedback=FakeFeedback([False, False]),
                    profile_path=profile, client=client, root=root)

            stage3 = result["result"]["stages"][2]
            self.assertEqual(calls, 6)
            self.assertEqual(stage3["status"], "model_call_failed")
            self.assertEqual(stage3["static_errors"][0]["code"], "synthetic_final_static_failure")
            self.assertEqual(len(client.prompts), 5)
            self.assertIn("STATIC CONTRACT ERROR ONLY", client.prompts[-1])
            self.assertIn("VALIDATED CAPABILITY GAP RECORD", client.prompts[-1])
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
            initial = _stage_prompt("evolve", digest=huge_digest, registry=registry,
                snapshot=library, runtime=runtime, history=huge_history, policy=policy,
                query_budget=budget)
            compact = _stage_prompt("evolve", digest=huge_digest, registry=registry,
                snapshot=library, runtime=runtime, history=[], policy=policy,
                query_budget=budget, include_case_digest=False, include_history=False)
            serve = _stage_prompt("stage3_serve", digest={}, registry=registry,
                snapshot=library, runtime=runtime, history=[], policy=policy,
                query_budget=budget, include_case_digest=False, include_history=False,
                gap_record={"schema_version": "e4-capability-gaps/1"})
            compact_stage2 = _stage_prompt("compose", digest={}, registry=registry,
                snapshot=library, runtime=runtime, history=[], policy=policy,
                query_budget=budget, include_case_digest=False, include_history=False)
            artifact = json.dumps({"gap_record": {"schema_version": "e4-capability-gaps/1"},
                "operators": [{"name": "evolved.example",
                "source": "def operator(inputs, params, runtime): return inputs['data']"}]})
            repair = _static_repair_prompt(compact, artifact,
                {"code": "unknown_operator", "message": "operator is not registered", "node_id": "score"})
            stage2_repair = _static_repair_prompt(compact_stage2, artifact,
                {"code": "node_maps", "message": "inputs, params, and bindings must be objects", "node_id": "score"})

            self.assertNotIn("DIGEST_SENTINEL_", initial)
            self.assertIn('"payload_marker"', initial)
            self.assertIn("evolution_digest_semantics", initial)
            self.assertIn("HISTORY_SENTINEL_", initial)
            self.assertNotIn("DIGEST_SENTINEL_", repair)
            self.assertIn("CURRENT PUBLIC CASE DIGEST OMITTED FROM STATIC REPAIR PROMPT", repair)
            self.assertIn(_canonical_hash(huge_digest), repair)
            self.assertNotIn("HISTORY_SENTINEL_", repair)
            self.assertLess(len(repair), len(initial) // 2)
            self.assertIn("STATIC CONTRACT ERROR ONLY", repair)
            self.assertIn("unknown_operator", repair)
            self.assertIn("operator is not registered", repair)
            self.assertIn("evolved.example", repair)
            self.assertNotIn("def operator(inputs, params, runtime)", repair)
            self.assertIn("[rejected executable source omitted]", repair)
            self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA", repair)
            self.assertIn("e4-typed-operator-ir/1", repair)
            self.assertIn("authoring_profile", repair)
            from e4infra.bank_prep import BANK_OFFICIAL_CANDIDATE_COMPONENTS
            self.assertIn(json.dumps(list(BANK_OFFICIAL_CANDIDATE_COMPONENTS), separators=(",", ":")), repair)
            self.assertIn("published schema vocabulary, not observations", repair)
            self.assertNotIn('"dag":', compact)
            self.assertIn("Do not return new specs or change the gap record", serve)
            self.assertIn("bank.answers.combine", serve)
            self.assertIn("REGISTERED COMPOSITE PROGRAMS", serve)
            self.assertIn("TRUSTED INPUT PRODUCER CAPABILITIES", serve)
            for prompt in (compact_stage2, serve, stage2_repair):
                self.assertIn("use {}", prompt)
                self.assertIn("scalar_port", prompt)
                self.assertIn("variadic_port", prompt)
                self.assertIn("bank.answers.combine", prompt)
                self.assertIn("RankedAnswer", prompt)
                self.assertIn('"inputs":{}', prompt)
                self.assertIn('"params":{}', prompt)
                self.assertIn('"bindings":{}', prompt)
            self.assertIn("This call does not diagnose the incident", compact)

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
                             ["model_call_failed", "truncated", "evolution_failed"])
            self.assertFalse(result["result"]["accepted"])

    def test_wrong_window_role_is_rejected_statically_before_incident_execution(self):
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
                json.dumps({"dag": _plain(library.playbooks["combined_shared_candidates"]["document"]),
                    "runtime_overrides": {}}),
                {"_error": {"code": "transport_error"}}])
            feedback = FakeFeedback([False, False])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "wrong_window_static", feedback=feedback,
                profile_path=profile, client=client, root=root)
            self.assertEqual(len(client.prompts), 4)
            stage2 = result["result"]["stages"][1]
            self.assertEqual(stage2["status"], "rejected")
            self.assertEqual(stage2["static_repairs"], 1)
            self.assertEqual(stage2["static_errors"][0]["code"], "aggregate_window_role")
            self.assertEqual(feedback.seen_case_ids, [case_id, case_id])

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
            history_start = stage2_client.prompts[1].index("PRIOR ATTEMPT HISTORY")
            history_start = stage2_client.prompts[1].index("\n", history_start) + 1
            history_text = stage2_client.prompts[1][history_start:]
            history_doc, _history_end = json.JSONDecoder().raw_decode(history_text)
            self.assertFalse(history_doc[-1]["accepted"])
            self.assertIn("bank.public.project", stage2_client.prompts[1])
            self.assertIn("requests_used", stage2_client.prompts[1])
            self.assertEqual(stage2_feedback.seen_case_ids, [case_id, case_id])

    def test_stage3_useful_synthesized_scorer_reaches_synthetic_cause_and_is_admitted(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root, positive_metric_pipeline=True)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "per_trace_native_duration", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed_doc = _plain(library.playbooks["combined_shared_candidates"]["document"])
            prior_doc = _plain(library.playbooks["per_trace_native_duration"]["document"])
            spec = _helper_typed_scorer_spec(root)
            normalized_spec = normalize_operator_ir_spec(spec, root=root)
            stage3_dag = _typed_ir_playbook(root, spec)
            evolution = {"gap_record": _gap_record(root, spec, prior_doc), "operators": [spec]}
            responses = [json.dumps(selection), json.dumps({"dag": prior_doc, "runtime_overrides": {}}),
                         json.dumps(evolution),
                         json.dumps({"dag": stage3_dag, "runtime_overrides": {}}),
                         json.dumps({"dag": stage3_dag, "runtime_overrides": {}}),
                         json.dumps({"dag": stage3_dag, "runtime_overrides": {}})]
            client = FakeClient(responses)
            feedback = FakeFeedback([False, False, True])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "stage3_run", feedback=feedback,
                profile_path=profile, client=client, root=root)
            self.assertTrue(result["result"]["accepted"])
            self.assertEqual(result["result"]["accepted_stage"], "stage3_synthesize")
            self.assertEqual(result["answer"].leads[0].entity.identifier, "Tomcat01")
            self.assertEqual(result["result"]["model_calls"]["phase_cost"]["evolve"]["model_call_count"], 1)
            self.assertEqual(result["result"]["model_calls"]["phase_cost"]["stage3_serve"]["model_call_count"], 1)
            self.assertEqual(feedback.seen_case_ids, [case_id, case_id, case_id])
            self.assertTrue((root / "stage3_run/evolution_contribution_diagnostics.json").is_file())
            self.assertNotIn('"dag":', client.prompts[2])
            self.assertNotIn("PRIVATE TYPED-OPERATOR-IR SCHEMA", client.prompts[3])
            self.assertIn(spec["name"], client.prompts[3])
            diagnostics = json.loads((root / "stage3_run/evolution_contribution_diagnostics.json").read_text())
            scorer = diagnostics["operators"][0]
            self.assertEqual(scorer["kind"], "typed_operator_ir")
            self.assertEqual(scorer["program_sha256"], normalized_spec["program_sha256"])
            self.assertGreater(scorer["nodes"][0]["output"]["ranking_eligible_evidence_count"], 0)
            self.assertGreater(scorer["nodes"][0]["output"]["positive_eligible_component_count"], 0)
            self.assertEqual(diagnostics["counterfactual"]["status"], "evaluated")
            self.assertTrue(any(key.startswith("evolved.synthetic_metric_score@")
                                for key in result["snapshot"].operators))
            self.assertEqual([row["accepted"] for row in result["result"]["stages"]],
                             [False, False, True])

            # Replay the admitted logical playbook from its reloaded snapshot.
            reloaded = load_library_snapshot(result["snapshot"].path)
            persisted = next(row for row in generated_specs_from_snapshot(reloaded)
                             if row["name"] == spec["name"])
            self.assertEqual(persisted["authoring_profile"], "typed-operator-ir-v1")
            self.assertEqual(persisted["program"], normalize_operator_ir_spec(spec, root=root)["program"])
            self.assertNotIn("source", persisted)
            accepted_book = next(name for name in reloaded.playbooks if name.startswith("accepted_m4_"))
            replay_selection = {"playbook": accepted_book, "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {}}
            replay = run_diagnostic(case_id, library_root=reloaded.root,
                run_dir=root / "accepted_ir_replay", feedback=FakeFeedback(expected_component="Tomcat01"),
                profile_path=profile, client=FakeClient([json.dumps(replay_selection)]), root=root)
            self.assertTrue(replay["result"]["accepted"])
            self.assertEqual(replay["result"]["accepted_stage"], "stage1_select")
            replay_stage = replay["result"]["stages"][0]
            self.assertEqual(replay_stage["composite_expansion_report"]["macro_call_count"], 0)
            self.assertTrue((root / "accepted_ir_replay/dags/stage1_select_expanded.json").is_file())

    def test_stage3_typed_ir_is_served_atomically_admitted_reloaded_and_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root, positive_metric_pipeline=True)
            library = _initial_library(root, root / "source_library")
            seed = _plain(library.playbooks["combined_shared_candidates"]["document"])
            prior = _plain(library.playbooks["per_trace_native_duration"]["document"])
            raw_spec = _typed_ir_scorer_spec(root)
            spec = normalize_operator_ir_spec(raw_spec, root=root)
            dag = _typed_ir_playbook(root, spec)
            # Exercise the compact spelling emitted in the R5 model attempt:
            # bare prior DAG hashes plus one descriptive capability string.
            evolution = {"gap_record": {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [_canonical_hash(prior)],
                "capabilities": ["Temporal association between component metrics and primary app KPI."]},
                "operators": [raw_spec]}
            responses = [
                json.dumps({"playbook": "combined_shared_candidates", "runtime_overrides": {},
                    "node_parameters": {}, "node_bindings": {}}),
                json.dumps({"dag": prior, "runtime_overrides": {}}),
                json.dumps(evolution),
                json.dumps({"dag": dag, "runtime_overrides": {}}),
            ]
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "typed_ir_run", feedback=FakeFeedback([False, False, True]),
                profile_path=profile, client=FakeClient(responses), root=root)
            self.assertTrue(result["result"]["accepted"], result["result"])
            self.assertEqual(result["result"]["accepted_stage"], "stage3_synthesize")
            self.assertEqual(result["result"]["model_calls"]["phase_cost"]["evolve"]["model_call_count"], 1)
            self.assertEqual(result["result"]["model_calls"]["phase_cost"]["stage3_serve"]["model_call_count"], 1)
            admitted_gap = json.loads((root / "typed_ir_run/attempts/stage3_synthesize/evolution_record.json").read_text())
            self.assertEqual(admitted_gap["status"], "validated")
            self.assertEqual(len(admitted_gap["gap_record"]["capabilities"]), 1)
            self.assertEqual(admitted_gap["gap_record"]["capabilities"][0]["operator_name"], raw_spec["name"])
            self.assertEqual(admitted_gap["gap_record"]["capabilities"][0]["gap_hypothesis"], evolution["gap_record"]["capabilities"][0])
            self.assertTrue(any(key.startswith("evolved.window_mean_delta@")
                for key in result["snapshot"].operators))
            diagnostics = json.loads((root / "typed_ir_run/evolution_contribution_diagnostics.json").read_text())
            self.assertEqual(diagnostics["operators"][0]["kind"], "typed_operator_ir")

            reloaded = load_library_snapshot(result["snapshot"].path)
            persisted = next(row for row in generated_specs_from_snapshot(reloaded)
                if row["name"] == "evolved.window_mean_delta")
            self.assertEqual(persisted["authoring_profile"], "typed-operator-ir-v1")
            self.assertEqual(persisted["program_sha256"], spec["program_sha256"])
            self.assertEqual(persisted["program"], spec["program"])
            accepted_book = next(name for name in reloaded.playbooks if name.startswith("accepted_m4_"))
            replay_selection = {"playbook": accepted_book, "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {}}
            replay = run_diagnostic(case_id, library_root=reloaded.root,
                run_dir=root / "typed_ir_replay", feedback=FakeFeedback(expected_component="Tomcat01"),
                profile_path=profile, client=FakeClient([json.dumps(replay_selection)]), root=root)
            self.assertTrue(replay["result"]["accepted"])
            self.assertEqual(replay["result"]["accepted_stage"], "stage1_select")

            second_root = root / "second_case"
            second_case, second_profile = _fixture_root(second_root,
                positive_metric_pipeline=True, date_key="2021_03_05")
            self.assertNotEqual(second_case, case_id)
            second_replay = run_diagnostic(second_case, library_root=reloaded.root,
                run_dir=second_root / "typed_ir_cross_case_replay",
                feedback=FakeFeedback(expected_component="Tomcat01"),
                profile_path=second_profile, client=FakeClient([json.dumps(replay_selection)]),
                root=second_root)
            self.assertTrue(second_replay["result"]["accepted"])
            self.assertEqual(second_replay["result"]["accepted_stage"], "stage1_select")

    def test_stage2_rejects_unknown_native_query_columns_without_composites(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            registry = make_m4_registry(None, [], root=root)
            versions = {row["name"]: row["version"] for row in registry.signatures()}
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            bad_dag = {"schema_version": "e4-dag/1", "nodes": [
                {"id": "data", "operator": "case.telemetry", "version": versions["case.telemetry"],
                 "inputs": {}, "params": {}, "bindings": {}},
                {"id": "reference", "operator": "bank.public.query", "version": versions["bank.public.query"],
                 "inputs": {"data": "data"}, "params": {"table": "trace/trace_span.csv",
                     "window": "reference", "fields": ["success"], "limit": 64, "offset": 0}, "bindings": {}},
                {"id": "alert", "operator": "bank.metrics.aggregate", "version": versions["bank.metrics.aggregate"],
                 "inputs": {"data": "data"}, "params": {"window": "alert"}, "bindings": {}},
                {"id": "candidates", "operator": "bank.candidates.generate", "version": versions["bank.candidates.generate"],
                 "inputs": {"data": "data"}, "params": {}, "bindings": {}},
                {"id": "scores", "operator": "bank.metrics.contrast", "version": versions["bank.metrics.contrast"],
                 "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
                 "params": {}, "bindings": {}},
                {"id": "answer", "operator": "bank.answers.combine", "version": versions["bank.answers.combine"],
                 "inputs": {"scores": ["scores"]}, "params": {"limit": 5}, "bindings": {}}],
                "return": {"node": "answer"}}
            fallback = _plain(library.playbooks["combined_shared_candidates"]["document"])
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": bad_dag, "runtime_overrides": {}}),
                json.dumps({"dag": fallback, "runtime_overrides": {}})])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "query_static_stage2", feedback=FakeFeedback([False, True]),
                profile_path=profile, client=client, root=root)
            self.assertTrue(result["result"]["accepted"])
            stage2 = result["result"]["stages"][1]
            self.assertEqual(stage2["static_errors"][0]["code"], "query_field_unknown")
            self.assertEqual(result["result"]["query_budget"]["requests"], 0)

    def test_stage2_rejects_raw_query_page_for_log_summary_consumer_before_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            registry = make_m4_registry(None, [], root=root)
            versions = {row["name"]: row["version"] for row in registry.signatures()}
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {}}
            nodes = [{"id": "data", "operator": "case.telemetry", "version": versions["case.telemetry"],
                "inputs": {}, "params": {}, "bindings": {}}]
            for window in ("reference", "alert"):
                nodes.append({"id": f"raw_{window}", "operator": "bank.public.query",
                    "version": versions["bank.public.query"], "inputs": {"data": "data"},
                    "params": {"table": "log/log_service.csv", "window": window,
                        "fields": ["timestamp", "cmdb_id", "log_name", "value"],
                        "limit": 64, "offset": 0}, "bindings": {}})
            nodes.extend([
                {"id": "candidates", "operator": "bank.candidates.generate",
                 "version": versions["bank.candidates.generate"], "inputs": {"data": "data"},
                 "params": {}, "bindings": {}},
                {"id": "scores", "operator": "bank.logs.volume_contrast",
                 "version": versions["bank.logs.volume_contrast"],
                 "inputs": {"reference": "raw_reference", "alert": "raw_alert",
                     "candidates": "candidates"}, "params": {}, "bindings": {}},
                {"id": "answer", "operator": "bank.answers.combine",
                 "version": versions["bank.answers.combine"], "inputs": {"scores": ["scores"]},
                 "params": {"limit": 5}, "bindings": {}}])
            bad_dag = {"schema_version": "e4-dag/1", "nodes": nodes,
                "return": {"node": "answer"}}
            fallback = _plain(library.playbooks["combined_shared_candidates"]["document"])
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": bad_dag, "runtime_overrides": {}}),
                json.dumps({"dag": fallback, "runtime_overrides": {}})])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "raw_page_to_summary", feedback=FakeFeedback([False, True]),
                profile_path=profile, client=client, root=root)
            self.assertTrue(result["result"]["accepted"])
            stage2 = result["result"]["stages"][1]
            mismatch = stage2["static_errors"][0]
            self.assertEqual(mismatch["code"], "aggregate_capability_mismatch")
            self.assertEqual(mismatch["details"]["actual_capability"], "bank_public_raw_page")
            self.assertEqual(result["result"]["query_budget"]["requests"], 0)

    def test_stage3_rejects_retained_duplicate_ir_body_before_serving(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            retained = _empty_scorer_spec(root, name="evolved.retained_analysis")
            library = _initial_library(root, root / "source_library", [retained])
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            seed_doc = _plain(library.playbooks["combined_shared_candidates"]["document"])
            spec = _empty_scorer_spec(root, name="evolved.duplicate_analysis")
            evolution = {"gap_record": _gap_record(root, spec, seed_doc), "operators": [spec]}
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": seed_doc, "runtime_overrides": {}}),
                json.dumps(evolution), json.dumps(evolution), json.dumps(evolution)])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "stage3_duplicate", feedback=FakeFeedback([False, False]),
                profile_path=profile, client=client, root=root)
            self.assertFalse(result["result"]["accepted"])
            self.assertFalse(any(key.startswith("evolved.duplicate_analysis@")
                                 for key in result["snapshot"].operators))
            stage3 = result["result"]["stages"][-1]
            self.assertEqual(stage3["status"], "evolution_failed")
            self.assertTrue(any(row["code"] == "stage3_ir_duplicate_body"
                for row in stage3["static_errors"]))

    def test_empty_evolution_uses_separate_current_catalog_serving_call(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            library = _initial_library(root, root / "source_library")
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            prior = _plain(library.playbooks["native_metric_contrast"]["document"])
            serving = _plain(library.playbooks["per_trace_native_duration"]["document"])
            empty_evolution = {"gap_record": {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [{"stage": "stage2_compose", "dag_sha256": _canonical_hash(prior)}],
                "capabilities": []}, "operators": []}
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": prior, "runtime_overrides": {}}),
                json.dumps(empty_evolution),
                json.dumps({"dag": serving, "runtime_overrides": {}})])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "no_extension", feedback=FakeFeedback([False, False, False]),
                profile_path=profile, client=client, root=root)
            self.assertFalse(result["result"]["accepted"])
            stage3 = result["result"]["stages"][-1]
            self.assertEqual(stage3["evolve_status"], "no_extension")
            self.assertEqual(result["result"]["model_calls"]["phase_cost"]["evolve"]["model_call_count"], 1)
            self.assertEqual(result["result"]["model_calls"]["phase_cost"]["stage3_serve"]["model_call_count"], 1)
            self.assertIn("current registered catalog", client.prompts[3])
            self.assertNotIn("every proposed composite", client.prompts[3])
            record = json.loads((root / "no_extension/attempts/stage3_synthesize/evolution_record.json").read_text())
            self.assertEqual(record["status"], "no_extension")
            self.assertEqual(record["operators"], [])
            self.assertFalse(any(key.startswith("evolved.") for key in result["snapshot"].operators))

    def test_gap_record_accepts_explicit_empty_capabilities_only_for_empty_proposal(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            registry = make_m4_registry(None, [], root=root)
            prior = {"schema_version": "e4-dag/1", "nodes": [], "return": {"node": "x"}}
            history = [{"stage": "stage2_compose", "plan_summary": {
                "dag_sha256": _canonical_hash(prior)}}]
            gap = {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [{"stage": "stage2_compose", "dag_sha256": _canonical_hash(prior)}],
                "capabilities": []}
            self.assertEqual(_validate_gap_record(gap, [], registry, history)["capabilities"], [])
            gap["capabilities"] = [{"unexpected": True}]
            with self.assertRaises(M4ControllerError):
                _validate_gap_record(gap, [], registry, history)

    def test_r5_gap_shorthand_normalizes_to_host_audited_record(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            registry = make_m4_registry(None, [], root=root)
            raw_spec, _dag = _stage3_graph(root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1, "alert_window_end_epoch_ms_utc": 2}, root=root)
            specs, _versions = _candidate_spec_mapping([raw_spec], registry, runtime, [])
            ref_hash, alert_hash = "a" * 64, "b" * 64
            history = [
                {"stage": "stage1_select", "plan_summary": {"dag_sha256": ref_hash}},
                {"stage": "stage2_compose", "plan_summary": {"dag_sha256": alert_hash}},
            ]
            r5_draft = {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [ref_hash, alert_hash],
                "capabilities": ["Temporal association between component metrics and primary app KPI."]}
            normalized = _normalize_gap_record_draft(r5_draft, specs, registry, history)
            self.assertEqual(normalized["prior_plans"], [
                {"stage": "stage1_select", "dag_sha256": ref_hash},
                {"stage": "stage2_compose", "dag_sha256": alert_hash}])
            self.assertEqual(len(normalized["capabilities"]), 1)
            capability = normalized["capabilities"][0]
            self.assertEqual(capability["operator_name"], specs[0]["name"])
            self.assertEqual(capability["required_inputs"], [row["name"] for row in specs[0]["input_ports"]])
            self.assertTrue(capability["closest_existing_operators"])
            self.assertIn("host-validated", capability["coverage_requirement"])
            self.assertEqual(_validate_gap_record(normalized, specs, registry, history), normalized)
            # Stored/archive callers may still submit the full canonical form.
            legacy_full = _normalize_gap_record_draft(normalized, specs, registry, history)
            self.assertEqual(_validate_gap_record(legacy_full, specs, registry, history), legacy_full)

            failed_model_shape = {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [ref_hash], "capabilities": [{
                    "description": "A missing computation", "difference": "Temporal",
                    "registered_closest_operators": ["bank.metrics.contrast"]}]}
            with self.assertRaises(M4ControllerError) as raised:
                _normalize_gap_record_draft(failed_model_shape, specs, registry, history)
            self.assertEqual(raised.exception.code, "stage3_gap_capability_schema")
            self.assertIn('capabilities:["one gap hypothesis per operator"]', str(raised.exception))
            self.assertIn("operator_name", str(raised.exception))
            self.assertIn("missing fields", str(raised.exception))
            self.assertIn("extra fields", str(raised.exception))
            self.assertEqual(raised.exception.details["expected_draft_type"],
                "array of bounded strings, one per ordered operator")

            alias = {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [{"stage": "stage2_compose", "dag_sha256": alert_hash}],
                "gap": "Temporal association is absent from prior analysis."}
            alias_normalized = _normalize_gap_record_draft(alias, specs, registry, history)
            self.assertEqual(len(alias_normalized["capabilities"]), 1)
            self.assertEqual(alias_normalized["capabilities"][0]["gap_hypothesis"], alias["gap"])
            self.assertEqual(_validate_gap_record(alias_normalized, specs, registry, history), alias_normalized)

            for invalid in (
                    {**r5_draft, "prior_plans": ["c" * 64]},
                    {**r5_draft, "prior_plans": [ref_hash, ref_hash]},
                    {**r5_draft, "gap": "also supplied"},
                    {**r5_draft, "capabilities": ["one", "two"]}):
                with self.subTest(invalid=invalid):
                    with self.assertRaises(M4ControllerError):
                        _normalize_gap_record_draft(invalid, specs, registry, history)

            ambiguous_history = history + [
                {"stage": "stage2_compose", "plan_summary": {"dag_sha256": ref_hash}}]
            with self.assertRaisesRegex(M4ControllerError, "uniquely match"):
                _normalize_gap_record_draft({**r5_draft, "prior_plans": [ref_hash]},
                    specs, registry, ambiguous_history)

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
