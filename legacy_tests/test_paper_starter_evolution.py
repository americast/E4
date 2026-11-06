import unittest
from pathlib import Path
import json
import os
import shutil
import tempfile
from unittest.mock import patch

from e4core.cache import canonical_json, deserialize_contract_json
from e4core.executor import NodeContext
from e4core.m4_generated import validate_generated_spec
from e4core.paper_starter import make_paper_starter_registry
from e4core.registry import OperatorSpec
from e4core.m4_operator_ir import trusted_runtime_context
from e4core.m4_operator_ir_worker import execute_ir
from tests.test_m4_operator_ir import ALT, REF, RUNTIME, _spec, _trusted_inputs


class PaperStarterEvolutionTests(unittest.TestCase):
    def test_complete_v2_prompt_example_passes_strict_normalization(self):
        from e4core.m4_controller import (_PAPER_V2_AUTHORING_EXAMPLE,
            _candidate_spec_mapping, _normalize_gap_record_draft,
            _validate_gap_record, build_m4_runtime)
        from e4core.benchmark import parse_strict_json_object
        from e4core.m4_operator_ir import normalize_operator_ir_spec
        registry = make_paper_starter_registry(bank_only=True)
        runtime = build_m4_runtime({
            "analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000})
        response = {"gap_record": {"schema_version": "e4-capability-gaps/1",
            "prior_plans": [],
            "capabilities": ["Describe one missing computation and how it differs from prior analysis."]},
            "operators": [_PAPER_V2_AUTHORING_EXAMPLE]}
        parsed, _ = parse_strict_json_object(json.dumps(response))
        normalized_rows, _versions = _candidate_spec_mapping(parsed["operators"],
            registry, runtime, [], allowed_authoring_profiles=("typed-operator-ir-v2",))
        gap = _normalize_gap_record_draft(parsed["gap_record"], normalized_rows,
            registry, [])
        gap = _validate_gap_record(gap, normalized_rows, registry, [])
        self.assertEqual(len(gap["capabilities"]), 1)
        self.assertEqual(gap["capabilities"][0]["gap_hypothesis"], response["gap_record"]["capabilities"][0])
        normalized = normalize_operator_ir_spec(_PAPER_V2_AUTHORING_EXAMPLE)
        self.assertEqual(normalized["authoring_profile"], "typed-operator-ir-v2")
        self.assertEqual(normalized["program"]["nodes"][-1]["inputs"]["evidence"], "n4")

    def test_compose_prompt_contract_projection_uses_named_ports(self):
        from e4core.m4_controller import _paper_compose_port_contracts
        rows = _paper_compose_port_contracts(make_paper_starter_registry(
            bank_only=True).signatures())
        by_name = {row["name"]: row for row in rows}
        for name in ("paper.backend.mean_welch", "paper.backend.tail_permutation",
                     "paper.bank.log_pattern_contrast"):
            self.assertIn("inputs", by_name[name])
            self.assertTrue(by_name[name]["inputs"])
            self.assertNotIn("i6", by_name[name]["inputs"])
        contrast = by_name["bank.metrics.contrast"]
        self.assertEqual(set(contrast["inputs"]), {"reference", "alert", "candidates"})
        self.assertNotIn("required_params", contrast)
        self.assertEqual(by_name["bank.metrics.aggregate"]["required_params"]["window"],
            ["reference", "alert"])

    def test_paper_controller_admits_only_v2_ir_proposals(self):
        from e4core.m4_controller import (M4ControllerError,
            _candidate_spec_mapping, build_m4_runtime)
        from e4core.m4_operator_ir import PROFILE_V2
        from tests.test_m4_operator_ir_v2 import _program, _spec as v2_spec

        registry = make_paper_starter_registry(bank_only=True)
        runtime = build_m4_runtime({
            "analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000})
        normalized, versions = _candidate_spec_mapping([v2_spec(_program(with_kpi=False))],
            registry, runtime, [], allowed_authoring_profiles=(PROFILE_V2,))
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["authoring_profile"], PROFILE_V2)
        self.assertIn((normalized[0]["name"], normalized[0]["declared_version"]), versions)

        legacy = v2_spec(_program(with_kpi=False))
        legacy["authoring_profile"] = "typed-operator-ir-v1"
        with self.assertRaises(M4ControllerError) as raised:
            _candidate_spec_mapping([legacy], registry, runtime, [],
                allowed_authoring_profiles=(PROFILE_V2,))
        self.assertEqual(raised.exception.code, "stage3_operator_profile")
        self.assertIn("authoring_profile='typed-operator-ir-v2'", str(raised.exception))
        self.assertIn("program.schema_version", str(raised.exception))
        omitted = v2_spec(_program(with_kpi=False))
        omitted.pop("authoring_profile")
        with self.assertRaises(M4ControllerError) as missing:
            _candidate_spec_mapping([omitted], registry, runtime, [],
                allowed_authoring_profiles=(PROFILE_V2,))
        self.assertIn("authoring_profile='typed-operator-ir-v2'", str(missing.exception))

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
                         "set E4_RUN_SANDBOX_STRESS=1 to exercise the isolated worker")
    def test_registered_generated_operator_stamps_trusted_scorer_identity(self):
        normalized = validate_generated_spec(_spec("variance_change"))
        self.assertEqual(normalized["authoring_profile"], "typed-operator-ir-v1")
        registry = make_paper_starter_registry(bank_only=True, generated_specs=[normalized])
        operator = registry.require(normalized["name"], normalized["version"])
        inputs = _trusted_inputs()
        result = operator.implementation(inputs, {}, NodeContext("analytic-fixture", "generated",
            normalized["name"], RUNTIME, {}, REF, ALT, "c" * 64))
        self.assertEqual(result.scorer, normalized["name"])

    def test_compiled_typed_ir_score_flows_through_paper_consensus_terminal(self):
        raw = _spec("variance_change")
        normalized = validate_generated_spec(raw)
        registry = make_paper_starter_registry(bank_only=True, generated_specs=[normalized])
        registry.register(OperatorSpec(
            "fixture.aggregate.reference", "1", (), "Aggregate", external_input=True))
        registry.register(OperatorSpec(
            "fixture.aggregate.alert", "1", (), "Aggregate", external_input=True))
        registry.register(OperatorSpec(
            "fixture.candidates", "1", (), "CandidateSet", external_input=True))
        name, version = normalized["name"], normalized["version"]
        inputs = _trusted_inputs()
        wire_inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
                       for key, value in inputs.items()}
        ir_output = execute_ir(normalized["program"], wire_inputs, {}, RUNTIME,
                               trusted_runtime_context())
        def interpreter_worker(*args, **kwargs):
            return {"ok": True, "result": ir_output, "stdout": "", "stderr": ""}

        generated_operator = registry.require(name, version)
        with patch("e4core.m4_generated.run_generated_operator", side_effect=interpreter_worker):
            scored = generated_operator.implementation(inputs, {}, NodeContext(
                "analytic-fixture", "generated", name, RUNTIME, {}, REF, ALT, "c" * 64))
        self.assertEqual(scored.scorer, name)
        terminal = registry.require("paper.answers.weighted_consensus", "1")
        result = terminal.implementation({"scores": (scored,)},
            {"family_weights": {"generated_metric_ir": 1.0}, "tie_tolerance": 1e-9, "limit": 5},
            NodeContext("analytic-fixture", "answer", terminal.name, RUNTIME, {}, REF, ALT,
                        "c" * 64)).value
        self.assertGreaterEqual(len(result.leads), 1)
        self.assertEqual(result.leads[0].entity.identifier, "component-ember")
        self.assertTrue(all(lead.evidence for lead in result.leads))
        self.assertTrue(all(lead.analysis_family == "paper.weighted_consensus" for lead in result.leads))

    def test_v2_ir_registered_identity_reaches_trusted_consensus(self):
        from dataclasses import replace
        from e4core.m4_operator_ir import normalize_operator_ir_spec
        from e4core.m4_operator_ir import trusted_runtime_context_v2
        from e4core.m4_operator_ir_v2_worker import execute_ir as execute_ir_v2
        from tests.test_m4_operator_ir_v2 import (_program, _spec as v2_spec,
            _trusted_inputs as trusted_inputs_v2, REF as ref_v2, ALT as alt_v2,
            RUNTIME as runtime_v2)

        raw = v2_spec(_program(with_kpi=False))
        normalized = validate_generated_spec(normalize_operator_ir_spec(raw))
        duplicate_raw = dict(raw, name="evolved.support_probe_copy")
        duplicate = validate_generated_spec(normalize_operator_ir_spec(duplicate_raw))
        self.assertEqual(duplicate["program_sha256"], normalized["program_sha256"])
        registry = make_paper_starter_registry(bank_only=True,
                                               generated_specs=[normalized, duplicate])
        name, version = normalized["name"], normalized["version"]
        inputs = trusted_inputs_v2()
        wire_inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
                       for key, value in inputs.items()}
        ir_output = execute_ir_v2(normalized["program"], wire_inputs, {}, runtime_v2,
                                  trusted_runtime_context_v2())

        def interpreter_worker(*args, **kwargs):
            return {"ok": True, "result": ir_output, "stdout": "", "stderr": ""}

        generated_operator = registry.require(name, version)
        with patch("e4core.m4_generated.run_generated_operator", side_effect=interpreter_worker):
            scored = generated_operator.implementation(inputs, {}, NodeContext(
                "analytic-fixture", "generated", name, runtime_v2, {}, ref_v2, alt_v2, "c" * 64))
        self.assertEqual(scored.scorer, name)
        terminal = registry.require("paper.answers.weighted_consensus", "1")
        context = NodeContext("analytic-fixture", "answer", terminal.name,
                              runtime_v2, {}, ref_v2, alt_v2, "c" * 64)
        params = {"family_weights": {"generated_metric_ir": 1.0},
                  "tie_tolerance": 1e-9, "limit": 5}
        answer = terminal.implementation({"scores": (scored,)}, params, context).value
        self.assertGreaterEqual(len(answer.leads), 1)
        self.assertTrue(all(lead.evidence for lead in answer.leads))

        duplicate_answer = terminal.implementation({"scores": (scored,
            replace(scored, scorer=duplicate["name"]))}, params, context).value
        self.assertEqual([(lead.entity.identifier, lead.score) for lead in answer.leads],
                         [(lead.entity.identifier, lead.score) for lead in duplicate_answer.leads])

        # The host registry accepts its own source-bound name. A generic V2 tag
        # supplied directly to consensus still has no trusted semantic group.
        forged = replace(scored, scorer="typed-operator-ir-v2")
        with self.assertRaisesRegex(Exception, "no trusted semantic consensus group"):
            terminal.implementation({"scores": (forged,)}, params, context)

    def test_mock_three_stage_paper_profile_serves_stored_kernel_with_empty_stage3_extension(self):
        from e4core.bank_adapter import sha256_file
        from e4core.m4_controller import (_canonical_hash, _implementation_fingerprint,
            _policy, build_m4_runtime, run_diagnostic)
        from e4core.m4_library import snapshot_library
        from e4core.m4_library import create_library_snapshot
        from e4core.paper_starter import bank_profile_playbooks
        from e4core.paper_starter import make_paper_starter_registry
        from tests.test_m4_controller import FakeClient, FakeFeedback, _fixture_root

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case_id, profile_path = _fixture_root(root)
            for name in ("paper_starter_catalog.json", "paper_starter_playbooks.json",
                         "paper_starter_runtime.json", "paper_bank_adaptations.json",
                         "typed_operator_ir_v2_policy.json"):
                shutil.copy2(Path(__file__).resolve().parents[1] / "config" / name,
                             root / "config" / name)
            registry = make_paper_starter_registry(bank_only=True, root=root)
            books = bank_profile_playbooks(registry)
            book = next(row for row in books if row["name"] == "backend_mean_shift")
            adaptation_book = next(row for row in books if row["name"] == "backend_location_shift")
            adaptation_score_node = next(node for node in adaptation_book["document"]["nodes"]
                if registry.require(node["operator"], node["version"]).output_type == "ScoredCandidates")
            provenance = {"implementation_fingerprint": _implementation_fingerprint(),
                "m4_runtime_policy_sha256": sha256_file(root / "config/m4_runtime.json"),
                "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
                "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
                "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json"),
                "paper_starter_catalog_sha256": sha256_file(root / "config/paper_starter_catalog.json"),
                "paper_starter_playbooks_sha256": sha256_file(root / "config/paper_starter_playbooks.json"),
                "paper_starter_runtime_sha256": sha256_file(root / "config/paper_starter_runtime.json"),
                "paper_bank_adaptations_sha256": sha256_file(root / "config/paper_bank_adaptations.json")}
            library = create_library_snapshot({"examples": books}, [], [], provenance,
                root / "library", builtin_specs=registry.signatures(),
                capacity_policy="reconstructed-paper-starter-v1")
            from e4core.m4_controller import _stage_prompt
            from e4core.m4_data import M4QueryBudget
            from e4core.m4_operator_ir import typed_operator_ir_v2_schema
            prompt_registry = make_paper_starter_registry(bank_only=True, root=root)
            prompt_snapshot = snapshot_library(library.root)
            prompt_runtime = build_m4_runtime({
                "analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
                "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
                "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=root)
            evolve_prompt = _stage_prompt("evolve", digest={}, registry=prompt_registry,
                snapshot=prompt_snapshot, runtime=prompt_runtime, history=[],
                policy={**_policy(root), "_paper_ir_capability_prompt_summary": {
                    "schema_version": "e4-typed-ir-v2-capability-prompt-summary/2",
                    "support_summary_semantics": "synthetic schema-merge test",
                    "primary_kpi": {}, "computations": [], "provenance": {},
                    "selector_status_schema": ["status", "match_count", "semantic_profile_refs",
                        "reference_support_histogram", "alert_support_histogram"],
                    "selector_status_by_ref": {ref: ["unavailable", 0, [], [], []]
                        for ref in typed_operator_ir_v2_schema(root)["kernels"][
                            "series.select_metric_rule@1"]["params"]["selector_ref"]["choices"]},
                    "selector_semantic_profiles": {},
                    "selector_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
                    "selector_case_data_sha256": "b" * 64, "selector_field_match_count": 0,
                    "selector_unique_field_count": 0, "selector_field_mapping_sha256": "c" * 64,
                    "selector_field_mapping_omission": "synthetic",
                    "provenance": {"telemetry_data_sha256": "b" * 64}}},
                query_budget=M4QueryBudget(None))
            v2_schema = typed_operator_ir_v2_schema(root)
            self.assertIn("typed-operator-ir-v2", evolve_prompt)
            self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG", evolve_prompt)
            self.assertIn("series.select_metric_rule@1", evolve_prompt)
            self.assertIn("selector_ref", evolve_prompt)
            self.assertIn("metric_selector_catalog", evolve_prompt)
            self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog[*].support_profile_ref", evolve_prompt)
            self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.selector_semantic_profiles", evolve_prompt)
            self.assertIn("metric_selector_catalog[*].support_profile_ref -> selector_case_resolution_profiles.profiles[*].case_resolution", evolve_prompt)
            self.assertIn("comparison-only and is not an authoring format", evolve_prompt)
            self.assertIn("normal V2 node-object syntax", evolve_prompt)
            self.assertIn("COMPLETE V2 STAGE-3 RESPONSE EXAMPLE", evolve_prompt)
            # The no-prior-V2 prompt now shows a complete parseable response,
            # including the simple capability string draft and full operator.
            from e4core.m4_controller import _PAPER_V2_AUTHORING_EXAMPLE
            example_marker = "COMPLETE V2 STAGE-3 RESPONSE EXAMPLE (valid illustration only; author a genuinely new useful program and do not copy this computation): "
            self.assertIn(example_marker, evolve_prompt)
            example_offset = evolve_prompt.index(example_marker) + len(example_marker)
            example, _end = json.JSONDecoder().raw_decode(evolve_prompt[example_offset:])
            self.assertEqual(set(example), {"gap_record", "operators"})
            self.assertEqual(example["gap_record"]["capabilities"], [
                "Describe one missing computation and how it differs from prior analysis."])
            self.assertEqual(example["operators"], [_PAPER_V2_AUTHORING_EXAMPLE])
            self.assertIn("Normalize eligible nonnegative scores per scorer", evolve_prompt)
            self.assertIn('"authoring_profile":"typed-operator-ir-v2"', evolve_prompt)
            self.assertIn('"schema_version":"e4-typed-operator-ir/2"', evolve_prompt)
            self.assertIn('"reference":"$input.reference"', evolve_prompt)
            self.assertIn("For a kernel parameter named P with runtime_field F", evolve_prompt)
            self.assertIn("required window parameter is an ordinary params.window", evolve_prompt)
            self.assertNotIn("maximum raw score", evolve_prompt)
            self.assertNotIn('"kernel":"series.correlate","version":"1"', evolve_prompt)
            self.assertIn("series.select_metric_rule@1", v2_schema["kernels"])
            selection = {"playbook": "backend_mean_shift", "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            empty_gap = {"schema_version": "e4-capability-gaps/1",
                "prior_plans": [{"stage": "stage2_compose",
                                 "dag_sha256": _canonical_hash(adaptation_book["document"])}],
                "capabilities": []}
            served_dag = json.loads(json.dumps(book["document"]))
            # Stage 3 serves a distinct but valid plan so its accepted playbook
            # is not isomorphic to the already retained Stage 2 seed.
            score_node = next(node for node in served_dag["nodes"]
                              if node["operator"] == "paper.backend.mean_welch")
            score_node["params"]["alpha"] = 0.04
            model = FakeClient([
                selection,
                {"schema_version": "e4-paper-stage2-adaptation/1",
                 "adaptation_record": {"schema_version": "e4-paper-adaptation-record/1",
                    "tested_plan_refs": [_canonical_hash(book["document"])],
                    "public_observations": ["The prior mean comparison returned a supported component result."],
                    "unexplained_limit": "A distinct location statistic has not been tested.",
                    "hypothesis_change": "Use the registered location permutation statistic for the declared source.",
                    "statistic": "median location permutation contrast",
                    "source_scope": "currently allowlisted component metric fields",
                    "window_relationship": "reference versus alert window",
                    "typed_evidence_outputs": [adaptation_score_node["id"]]},
                 "dag": adaptation_book["document"], "runtime_overrides": {}},
                {"gap_record": empty_gap, "operators": []},
                {"dag": served_dag, "runtime_overrides": {}},
            ])
            feedback = FakeFeedback([False, False, True])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "paper-three-stage", feedback=feedback,
                profile_path=profile_path, client=model, root=root)
            stages = result["result"]["stages"]
            self.assertEqual([row["stage"] for row in stages],
                             ["stage1_select", "stage2_compose", "stage3_synthesize"])
            self.assertTrue(result["result"]["accepted"])
            self.assertEqual(stages[-1]["status"], "accepted")
            self.assertEqual(feedback.seen_case_ids, [case_id, case_id, case_id])
            self.assertEqual(len(model.prompts), 4)
            self.assertIn("e4-paper-stage2-adaptation/1", model.prompts[1])
            self.assertIn("PRIOR PUBLIC ANALYSIS EVIDENCE", model.prompts[1])
            self.assertIn("public_observations", model.prompts[1])
            self.assertEqual(stages[1]["adaptation_record"]["statistic"],
                             "median location permutation contrast")
            self.assertIn("median location permutation contrast", model.prompts[3])
            self.assertEqual(stages[-1]["execution"]["answer"]["$type"], "RankedAnswer")

    def test_paper_stage2_duplicate_expanded_plan_gets_one_static_repair(self):
        from e4core.bank_adapter import sha256_file
        from e4core.m4_controller import (_canonical_hash, _implementation_fingerprint,
            run_diagnostic)
        from e4core.m4_library import create_library_snapshot
        from e4core.paper_starter import bank_profile_playbooks, make_paper_starter_registry
        from tests.test_m4_controller import FakeClient, FakeFeedback, _fixture_root

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case_id, profile_path = _fixture_root(root)
            for name in ("paper_starter_catalog.json", "paper_starter_playbooks.json",
                         "paper_starter_runtime.json", "paper_bank_adaptations.json"):
                shutil.copy2(Path(__file__).resolve().parents[1] / "config" / name,
                             root / "config" / name)
            registry = make_paper_starter_registry(bank_only=True, root=root)
            books = bank_profile_playbooks(registry)
            mean_book = next(row for row in books if row["name"] == "backend_mean_shift")
            location_book = next(row for row in books if row["name"] == "backend_location_shift")
            mean_score_node = next(node for node in mean_book["document"]["nodes"]
                if registry.require(node["operator"], node["version"]).output_type == "ScoredCandidates")
            location_score_node = next(node for node in location_book["document"]["nodes"]
                if registry.require(node["operator"], node["version"]).output_type == "ScoredCandidates")
            provenance = {"implementation_fingerprint": _implementation_fingerprint(),
                "m4_runtime_policy_sha256": sha256_file(root / "config/m4_runtime.json"),
                "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
                "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
                "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json"),
                "paper_starter_catalog_sha256": sha256_file(root / "config/paper_starter_catalog.json"),
                "paper_starter_playbooks_sha256": sha256_file(root / "config/paper_starter_playbooks.json"),
                "paper_starter_runtime_sha256": sha256_file(root / "config/paper_starter_runtime.json"),
                "paper_bank_adaptations_sha256": sha256_file(root / "config/paper_bank_adaptations.json")}
            library = create_library_snapshot({"examples": books}, [], [], provenance,
                root / "library", builtin_specs=registry.signatures(),
                capacity_policy="reconstructed-paper-starter-v1")
            selection = {"playbook": mean_book["name"], "runtime_overrides": {},
                         "node_parameters": {}, "node_bindings": {}}
            def response(book, evidence_node):
                return {"schema_version": "e4-paper-stage2-adaptation/1",
                    "adaptation_record": {"schema_version": "e4-paper-adaptation-record/1",
                        "tested_plan_refs": [_canonical_hash(mean_book["document"])],
                        "public_observations": ["The previous supported mean statistic was executed."],
                        "unexplained_limit": "No location statistic has been tested.",
                        "hypothesis_change": "Test a distinct supported location statistic.",
                        "statistic": "median location permutation contrast",
                        "source_scope": "allowlisted component metric fields",
                        "window_relationship": "reference versus alert window",
                        "typed_evidence_outputs": [evidence_node["id"]]},
                    "dag": book["document"], "runtime_overrides": {}}
            served = json.loads(json.dumps(mean_book["document"]))
            score = next(node for node in served["nodes"]
                         if node["operator"] == "paper.backend.mean_welch")
            score["params"]["alpha"] = 0.04
            gap = {"schema_version": "e4-capability-gaps/1", "prior_plans": [
                {"stage": "stage2_compose", "dag_sha256": _canonical_hash(location_book["document"])}],
                "capabilities": []}
            model = FakeClient([selection, response(mean_book, mean_score_node),
                response(location_book, location_score_node),
                {"gap_record": gap, "operators": []}, {"dag": served, "runtime_overrides": {}}])
            feedback = FakeFeedback([False, False, True])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "paper-stage2-duplicate", feedback=feedback,
                profile_path=profile_path, client=model, root=root)
            stage2 = result["result"]["stages"][1]
            self.assertEqual(stage2["status"], "rejected")
            self.assertEqual(stage2["static_repairs"], 1)
            self.assertEqual(stage2["static_errors"][0]["code"], "paper_stage2_semantic_duplicate")
            self.assertIn("STATIC CONTRACT ERROR ONLY", model.prompts[2])
            self.assertIn("paper_stage2_semantic_duplicate", model.prompts[2])
            self.assertEqual(len(feedback.seen_case_ids), 3)


if __name__ == "__main__":
    unittest.main()
