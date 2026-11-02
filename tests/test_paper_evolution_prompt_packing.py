"""IR evolve display packing keeps one source-field enum and full analysis context."""

from __future__ import annotations

import unittest
import json
import hashlib
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from tests._support import prompt_budget as prompt_candidates

from e4core.m4_controller import (M4ModelClient, M4_EVOLUTION_SYSTEM_PROMPTS,
    PAPER_M4_SYSTEM_PROMPTS, M4_SYSTEM_PROMPT,
    _compact_registered_signatures,
    _expand_paper_registered_signature_catalog,
    _expand_registered_signatures, _front_code_paper_metric_fields,
    _expand_front_coded_paper_metric_fields,
    _intern_paper_ir_metric_choices, _paper_evolve_operator_catalog,
    _paper_parameter_catalog_for_prompt, _paper_registered_signature_catalog,
    _canonical_hash,
    _validate_paper_adaptation_record,
    _paper_adaptation_evidence_context,
    _paper_v2_capability_prompt_summary, _intern_paper_ir_runtime_context,
    _compact_paper_prior_ir_bodies, _expand_paper_prior_ir_bodies,
    _expand_paper_semantic_profile_prompt_table,
    _reject_exact_prior_paper_plan, PAPER_STAGE2_RESPONSE_SCHEMA,
    PAPER_ADAPTATION_RECORD_SCHEMA, M4ControllerError)
from e4core.m4_operator_ir import typed_operator_ir_schema, typed_operator_ir_v2_schema
from e4core.paper_starter import (intern_paper_parameter_choices,
    bank_profile_playbooks, make_paper_starter_registry, paper_parameter_catalog,
    make_paper_starter_input, available_bank_profile_playbooks)
from e4core.m4_library import (create_library_snapshot, snapshot_library,
    generated_specs_from_snapshot)
from e4core.m4_data import M4QueryBudget
from e4core.contracts import EntityRef, MetricPoint
from e4core.m4_controller import (_stage_prompt, _policy, _split_composite_specs,
    _logical_registry, build_m4_runtime, public_case_digest, _static_repair_prompt)

ROOT = Path(__file__).resolve().parents[1]


class PaperEvolutionPromptPackingTests(unittest.TestCase):
    def test_paper_evolve_payload_and_completion_use_evolution_system_prompt(self):
        profile = {"model_alias": "fake-gemma", "temperature": 1.0,
            "top_p": 0.95, "top_k": 64, "seed": 42,
            "max_generation_tokens": 8192, "context_tokens": 32768}
        client = M4ModelClient(profile, harness_profile="reconstructed-paper-starter-v1")
        payload = client.payload_for_stage("evolve", "stage-specific user text")
        expected = PAPER_M4_SYSTEM_PROMPTS["evolve"]
        self.assertEqual(payload["messages"][0]["content"], expected)
        for phrase in ("Stage 3 evolution", "Do not diagnose", "typed-operator-ir-v2",
                       "new proposals must use V2", "empty proposal"):
            self.assertIn(phrase, expected)

        seen = []
        def fake_post(path, request):
            seen.append((path, request))
            if path == "/apply-template":
                return 200, "{}", {"prompt": "rendered template"}, None
            if path == "/tokenize":
                return 200, "{}", {"tokens": [1, 2, 3]}, None
            raise AssertionError(f"unexpected preflight endpoint {path}")

        class FakeResponse:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self):
                return json.dumps({"choices": [{"message": {"content": "{}"}}]}).encode()

        with patch.object(client, "_post_json", side_effect=fake_post), \
             patch("e4core.model_client.urllib.request.urlopen", return_value=FakeResponse()):
            call = client.complete_for_stage("evolve", "stage-specific user text",
                task_id="paper-evolve-system", attempt=1)
        self.assertIsNone(call.error)
        self.assertEqual(call.request_payload, payload)
        self.assertEqual(seen[0][0], "/apply-template")
        self.assertEqual(seen[0][1]["messages"][0]["content"], expected)

    def test_non_evolve_system_payloads_and_legacy_profile_are_unchanged(self):
        profile = {"model_alias": "fake-gemma", "temperature": 1.0,
            "top_p": 0.95, "top_k": 64, "seed": 42,
            "max_generation_tokens": 8192, "context_tokens": 32768}
        legacy = M4ModelClient(profile)
        paper = M4ModelClient(profile, harness_profile="reconstructed-paper-starter-v1")
        for stage in ("select", "compose", "stage3_serve"):
            legacy_expected = M4_EVOLUTION_SYSTEM_PROMPTS.get(stage, M4_SYSTEM_PROMPT)
            paper_expected = PAPER_M4_SYSTEM_PROMPTS.get(stage, M4_SYSTEM_PROMPT)
            self.assertEqual(legacy.payload_for_stage(stage, "prompt")["messages"][0]["content"],
                             legacy_expected)
            self.assertEqual(paper.payload_for_stage(stage, "prompt")["messages"][0]["content"],
                             paper_expected)

    def test_native_metric_enum_is_referenced_not_repeated_in_ir_catalogs(self):
        registry = make_paper_starter_registry(bank_only=True)
        signatures = intern_paper_parameter_choices(registry.signatures())
        ir_schema = _intern_paper_ir_metric_choices(typed_operator_ir_schema())
        ir_v2_schema = _intern_paper_ir_metric_choices(typed_operator_ir_v2_schema())
        parameter_catalog = paper_parameter_catalog()
        kernel_choice = ir_schema["kernels"]["series.select_field@1"]["params"]["source_field"]
        self.assertNotIn("choices", kernel_choice)
        self.assertEqual(kernel_choice["choices_ref"], "paper.bank_component_metric_fields")
        self.assertEqual(ir_schema["approved_component_metric_fields"]["choices_ref"],
                         "paper.bank_component_metric_fields")
        selector_parameter = ir_v2_schema["kernels"]["series.select_metric_rule@1"][
            "params"]["selector_ref"]
        self.assertNotIn("choices", selector_parameter)
        self.assertEqual(selector_parameter["choices_ref"], "metric_selector_catalog")
        self.assertTrue(ir_v2_schema["metric_selector_catalog"])
        self.assertEqual({row["selector_ref"] for row in ir_v2_schema["metric_selector_catalog"]},
            set(typed_operator_ir_v2_schema()["kernels"]["series.select_metric_rule@1"][
                "params"]["selector_ref"]["choices"]))
        producer_caps = _intern_paper_ir_metric_choices({"component_metrics": {
            "approved_source_fields": parameter_catalog["enums"][
                "paper.bank_component_metric_fields"]["values"]}})
        self.assertEqual(producer_caps["component_metrics"]["approved_source_fields"]["choices_ref"],
                         "paper.bank_component_metric_fields")
        field_values = parameter_catalog["enums"]["paper.bank_component_metric_fields"]["values"]
        self.assertEqual(sum(parameter.get("choices_ref") == "paper.bank_component_metric_fields"
            for row in signatures for parameter in row.get("parameters", {}).values()),
            sum(parameter.get("choices") == field_values for row in registry.signatures()
                for parameter in row.get("parameters", {}).values()))
        self.assertGreater(len(field_values), 1)

    def test_v2_capability_prompt_summary_uses_exact_fields_and_bounds_kpi_detail(self):
        field = paper_parameter_catalog()["enums"][
            "paper.bank_component_metric_fields"]["values"][0]
        report = {"primary_kpi": {"computation": "primary_kpi_policy_selection",
            "data_state": "present", "computability_state": "computable",
            "ranking_state": "unknown", "selected_identity": "opaque-kpi-1",
            "selection_reason": "below_policy", "details": {
                "minimum_timestamps_per_window": 24,
                "meets_primary_kpi_policy": False,
                "best_observed_support_per_window": 11,
                "observed_kpi_candidate_count": 100,
                "observed_kpi_candidates": [{"opaque_id": f"kpi-{i}"} for i in range(64)],
                "omitted_candidate_count": 36,
                "selection_policy_sha256": "a" * 64}},
            "computations": [{"computation": "component_univariate_window_change",
                "data_state": "present", "computability_state": "computable",
                "ranking_state": "unknown", "details": {"series_support": [
                    {"field": field, "unit": "percent", "kind": "gauge",
                     "reference_unique_timestamps": 18, "alert_unique_timestamps": 18,
                     "reference_constant": False, "alert_constant": False,
                     "computable_arithmetic_change": True}]}}],
            "metric_selector_resolutions": [
                {"selector_ref": "rule.one@1",
                    "source_table": "bank/metric_container.csv", "status": "unavailable",
                    "match_count": 0, "matches": [], "policy_sha256": "c" * 64,
                    "case_data_sha256": "d" * 64},
                {"selector_ref": "rule.many@1",
                    "source_table": "bank/metric_container.csv", "status": "resolved_multiple",
                    "match_count": 2, "matches": [{"field_name": "hidden-a",
                        "kind": "gauge", "unit": "percent", "direction": "decrease"},
                        {"field_name": "hidden-b", "kind": "gauge", "unit": "percent",
                         "direction": "decrease"}], "policy_sha256": "c" * 64,
                    "case_data_sha256": "d" * 64},
                {"selector_ref": "rule.one@1", "source_table": "bank/metric_app.csv",
                    "status": "resolved_one", "match_count": 1, "matches": [{}],
                    "policy_sha256": "c" * 64, "case_data_sha256": "d" * 64}],
            "provenance": {"metric_policy_sha256": "b" * 64}}
        summary = _paper_v2_capability_prompt_summary(report)
        primary = summary["primary_kpi"]["selection_support"]
        self.assertEqual(primary["best_observed_support_per_window"], 11)
        self.assertEqual(primary["observed_kpi_candidate_count"], 100)
        self.assertEqual(primary["omitted_candidate_count"], 36)
        self.assertNotIn("observed_kpi_candidates", primary)
        metric_summary = summary["computations"][0]
        self.assertEqual(metric_summary["support_series_count"], 1)
        self.assertEqual(metric_summary["computable_series_count"], 1)
        self.assertEqual(metric_summary["support_histograms"][
            "reference_unique_timestamps"], {"18": 1})
        self.assertIn("exact stable selector_ref", summary["support_summary_semantics"])
        statuses = summary["selector_status_by_ref"]
        self.assertEqual(statuses["rule.one@1"][0], "unavailable")
        self.assertEqual(statuses["rule.many@1"][1], 2)
        self.assertEqual(len(statuses), 2)
        self.assertEqual(summary["selector_status_schema"], ["status", "match_count",
            "semantic_profile_refs", "reference_support_histogram", "alert_support_histogram"])
        self.assertEqual(summary["selector_semantic_profiles"][
            statuses["rule.many@1"][2][0]]["direction"], "decrease")
        self.assertEqual(summary["selector_case_data_sha256"], "d" * 64)

        context = _intern_paper_ir_runtime_context({
            "component_metric_allowlist": paper_parameter_catalog()["enums"][
                "paper.bank_component_metric_fields"]["values"],
            "primary_metric_allowlist": paper_parameter_catalog()["enums"][
                "paper.app_kpi_fields"]["values"]})
        self.assertEqual(context["component_metric_allowlist"]["choices_ref"],
            "paper.bank_component_metric_fields")

    def test_prompt_selector_profiles_keep_all_exact_parameter_strings_and_report_missing(self):
        catalog = paper_parameter_catalog()
        exact_values = catalog["enums"]["paper.bank_component_metric_fields"]["values"]
        rule_values = catalog["enums"]["paper.bank_component_metric_rule_refs"]["values"]
        exact_field = next(value for value in exact_values if value)
        rule_ref = next(value for value in rule_values if value)
        report = {"metric_selector_catalog": [
                {"selector_ref": "exact-ref@1", "source_table": "bank/metric_container.csv",
                 "selector_kind": "exact", "exact_field_name": exact_field,
                 "description": exact_field},
                {"selector_ref": rule_ref, "source_table": "bank/metric_container.csv",
                 "selector_kind": "pattern", "description": "family rule"}],
            "metric_selector_resolutions": [
                {"selector_ref": "exact-ref@1", "source_table": "bank/metric_container.csv",
                 "status": "resolved_one", "match_count": 1,
                 "policy_sha256": "a" * 64, "case_data_sha256": "b" * 64,
                 "matches": [{"field_name": exact_field, "kind": "gauge", "unit": "percent",
                    "unit_status": "working_interpretation", "direction": "increase",
                    "transform": "identity", "reset_policy": "not_applicable",
                    "classification_status": "working_interpretation",
                    "window_support": {"reference": 30, "alert": 29}}]},
                {"selector_ref": rule_ref, "source_table": "bank/metric_container.csv",
                 "status": "resolved_multiple", "match_count": 2,
                 "policy_sha256": "a" * 64, "case_data_sha256": "b" * 64,
                 "matches": [{"field_name": "dynamic-a", "kind": "rate",
                    "unit": "native rate", "unit_status": "undeclared", "direction": "increase",
                    "transform": "rate_from_counter", "reset_policy": "reset_excluded",
                    "classification_status": "working_interpretation",
                    "window_support": {"reference": 20, "alert": 18}},
                    {"field_name": "dynamic-b", "kind": "rate",
                    "unit": "native rate", "unit_status": "undeclared", "direction": "increase",
                    "transform": "rate_from_counter", "reset_policy": "reset_excluded",
                    "classification_status": "working_interpretation",
                    "window_support": {"reference": 20, "alert": 18}}]}],
            "provenance": {"bank_metric_policy_sha256": "a" * 64,
                "telemetry_data_sha256": "b" * 64,
                "telemetry_source_hashes_sha256": "c" * 64}}
        prompt_catalog = _paper_parameter_catalog_for_prompt({"_paper_ir_capability_report": report})
        semantic_profiles = _expand_paper_semantic_profile_prompt_table(
            prompt_catalog["case_semantic_profiles"])
        exact_enum = prompt_catalog["enums"]["paper.bank_component_metric_fields"]
        exact_profiles = {row["support_profile_ref"]: row["case_support"] for row in
            prompt_catalog["case_support_profiles"]["paper.bank_component_metric_fields"]["profiles"]}
        exact_expanded = [{"value": row["value"], "case_support": exact_profiles[
            row["support_profile_ref"]]} for row in exact_enum["values"]]
        self.assertEqual([row["value"] for row in exact_expanded], exact_values)
        self.assertEqual(exact_enum["value_field"], "value")
        self.assertEqual(exact_enum["parameter_value_type"], "string")
        self.assertEqual(exact_enum["case_support_profile_catalog_ref"],
            "case_support_profiles.paper.bank_component_metric_fields")
        by_value = {row["value"]: row["case_support"] for row in exact_expanded}
        self.assertEqual(by_value[exact_field]["status"], "available")
        for support in by_value.values():
            self.assertTrue(set(support.get("semantic_profile_refs", ())) <=
                set(semantic_profiles))
        missing = next(value for value in exact_values if value and value != exact_field)
        self.assertEqual(by_value[missing]["status"], "unavailable")

        rule_enum = prompt_catalog["enums"]["paper.bank_component_metric_rule_refs"]
        rule_profiles = {row["support_profile_ref"]: row["case_support"] for row in
            prompt_catalog["case_support_profiles"]["paper.bank_component_metric_rule_refs"]["profiles"]}
        rules_expanded = [{"value": row["value"], "case_support": rule_profiles[
            row["support_profile_ref"]]} for row in rule_enum["values"]]
        self.assertEqual([row["value"] for row in rules_expanded], rule_values)
        by_rule = {row["value"]: row["case_support"] for row in rules_expanded}
        self.assertEqual(by_rule[rule_ref]["status"], "resolved_multiple")
        self.assertEqual(by_rule[rule_ref]["match_count"], 2)
        self.assertTrue(set(by_rule[rule_ref].get("semantic_profile_refs", ())) <=
            set(semantic_profiles))
        self.assertEqual(prompt_catalog["case_support_provenance"]["telemetry_data_sha256"], "b" * 64)

    def test_prior_ir_columnar_projection_roundtrips_every_kernel_body_field(self):
        from e4core.m4_generated import validate_generated_spec
        from tests.test_m4_operator_ir_v2 import _program, _spec
        spec = validate_generated_spec(_spec(_program(with_kpi=True)))
        row = {key: spec[key] for key in ("name", "version", "family",
            "authoring_profile", "program_sha256", "program")}
        compact = _compact_paper_prior_ir_bodies([row])
        self.assertEqual(_expand_paper_prior_ir_bodies(compact), [row])
        self.assertIn("No operator body, node, parameter, binding, or return is omitted",
            compact["decode_rule"])

    def test_prior_public_evidence_points_to_ranked_answer_without_repeating_leads(self):
        adaptation = {"schema_version": PAPER_ADAPTATION_RECORD_SCHEMA,
            "tested_plan_refs": ["a" * 64]}
        public = {"ranked_leads": [{"rank": 1, "component_id": "component-x", "score": 0.9}],
            "effective_scorer_groups": {"backend_metric": {"lead_count": 1}},
            "operator_coverage_by_operator": {"paper.backend.mean_welch@1": {"success_nodes": 1}},
            "input_capability_checks": [{"status": "passed"}]}
        history = [{"stage": "stage2_compose", "status": "rejected",
            "plan_summary": {"dag_sha256": "b" * 64},
            "ranked_answer": {"lead_count": 1, "leads": [{"component_id": "component-x"}]},
            "public_analysis_evidence": public,
            "adaptation_record": adaptation,
            "adaptation_record_origin": "structured source record"}]
        projected = _paper_adaptation_evidence_context(history)
        self.assertEqual(projected[0]["ranked_answer_ref"], {
            "history_ref": "h1", "field": "ranked_answer",
            "semantics": "the complete bounded ranked-answer excerpt appears once in PRIOR ATTEMPT HISTORY"})
        self.assertNotIn("ranked_leads", projected[0]["public_analysis_evidence"])
        self.assertEqual(projected[0]["public_analysis_evidence"]["effective_scorer_groups"],
            public["effective_scorer_groups"])
        self.assertEqual(projected[0]["adaptation_record"], adaptation)

    def test_evolve_operator_catalog_preserves_identity_description_capability(self):
        registry = make_paper_starter_registry(bank_only=True)
        signatures = registry.signatures()
        packed = _paper_evolve_operator_catalog(signatures)
        original = {(row["name"], row["version"]): row for row in signatures}
        displayed = {(row["name"], row["version"]): row for row in packed["operators"]}
        self.assertEqual(set(original), set(displayed))
        for key, before in original.items():
            after = displayed[key]
            self.assertEqual(after["description"], before["description"])
            self.assertEqual(after["family"], before["family"])
            self.assertEqual(after["output_type"], before["output_type"])
            self.assertEqual(after.get("external_input", False), before.get("external_input", False))
            if "input_requirements" in before:
                ref = after.get("input_requirements_ref")
                self.assertIsInstance(ref, str)
                self.assertEqual(packed["input_requirement_contracts"][ref],
                    before["input_requirements"])
            self.assertIn("omitted_serving_dag_fields", after)
            self.assertNotIn("parameters", after)
            self.assertNotIn("inputs", after)
        self.assertEqual(_compact_registered_signatures(signatures)["schema"],
                         "e4-registered-signatures-defaults/1")

    def test_signature_contract_catalog_roundtrips_and_reduces_max_retained_operator_fixture(self):
        operators = []
        shared_port = {"type": "Aggregate", "required": True, "variadic": False,
            "minimum_items": 1, "maximum_items": None}
        shared_parameter = {"type": "integer", "required": False, "nullable": False,
            "has_default": True, "default": 8, "choices": [],
            "runtime_field": "minimum_support", "estimator": None,
            "description": "Minimum supported observations."}
        for index in range(19 + 6 + 3):
            operators.append({"name": f"pilot.operator.{index:02d}", "version": "1",
                "family": "generated_metric_ir" if index >= 25 else f"family_{index % 4}",
                "output_type": "ScoredCandidates", "description": f"Distinct registered operator {index}.",
                "external_input": False,
                "inputs": [{"name": "data", **shared_port}],
                "parameters": {"minimum_support": dict(shared_parameter)},
                "implementation": "callable", "input_requirements": {"source":"aggregate"}})
        expanded = _compact_registered_signatures(operators)
        packed = _paper_registered_signature_catalog(operators)
        self.assertEqual(_expand_paper_registered_signature_catalog(packed), expanded)
        packed_bytes = len(json.dumps(packed, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        original_bytes = len(json.dumps(expanded, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        self.assertLess(packed_bytes, original_bytes)
        self.assertEqual(len(packed["operators"]), 28)
        self.assertEqual(packed["operators"][-1]["description"], "Distinct registered operator 27.")
        self.assertEqual(packed["operators"][0]["parameters"]["minimum_support"],
                         packed["operators"][1]["parameters"]["minimum_support"])
        self.assertEqual(_expand_registered_signatures(_expand_paper_registered_signature_catalog(packed))[0]["inputs"][0]["type"],
                         "Aggregate")

    def test_front_coded_metric_fields_roundtrip_exactly(self):
        fields = ["OSLinux-CPU_CPU_CPUCpuUtil", "OSLinux-CPU_CPU_CPULoad",
            "Container-DOCKER_CONTAINER_long-prefix-node1_CpuPercent",
            "Container-DOCKER_CONTAINER_long-prefix-node2_CpuPercent"]
        packed = _front_code_paper_metric_fields({"available_metric_fields": fields, "other": [1, 2]})
        self.assertEqual(_expand_front_coded_paper_metric_fields(packed["available_metric_fields"]), fields)
        self.assertEqual(packed["other"], [1, 2])

    @unittest.skipUnless((ROOT / "artifacts/m3/model_runs/e4s43/audits/20261024T164037Z-1f42bf1e/bank10_wingtip/case01/diagnostic/model_calls.jsonl").is_file(),
        "optional captured-prompt integration: ignored seed43 model-call log is unavailable")
    def test_saved_seed43_wingtip_serve_overflow_reconstruction_preserves_full_context(self):
        calls_path = ROOT / "artifacts/m3/model_runs/e4s43/audits/20261024T164037Z-1f42bf1e/bank10_wingtip/case01/diagnostic/model_calls.jsonl"
        records = [json.loads(line) for line in calls_path.read_text(encoding="utf-8").splitlines()]
        row = next(record for record in records if record.get("m4_phase") == "stage3_serve")
        self.assertEqual(row["prompt_token_count"], 24774)
        self.assertEqual(row["error"]["code"], "context_budget_exceeded")
        system, original = [message["content"] for message in row["request_payload"]["messages"]]
        # The captured request preserves the pre-trim calibrated system text.
        # Compare that immutable record to its own source contracts, while the
        # active prompt is checked against the current compact source below.
        self.assertIn("active typed operator registry and validated extension gap", system)
        self.assertIn("registered answer and evidence contracts", system)
        self.assertIn("host-pinned program digest", system)
        current_system = PAPER_M4_SYSTEM_PROMPTS["stage3_serve"]
        self.assertIn("trusted semantic group", current_system)
        self.assertIn("bounded weights", current_system)
        self.assertIn("association is noncausal", current_system.casefold())
        self.assertIn("unsupported stays unknown", current_system)
        self.assertIn("all generated ir scorers share one capped group", current_system.casefold())
        self.assertIn("family labels add no votes", current_system)
        paper_prompt_text = (system + "\n" + original).casefold()
        self.assertNotIn("maximum raw score", paper_prompt_text)
        self.assertNotIn("maximum raw per component", paper_prompt_text)
        self.assertIn("trusted consensus weight catalog", paper_prompt_text)

        signature_header = (
            "REGISTERED OPERATOR SIGNATURES (lossless defaults encoding; "
            "expand omitted catalog fields from listed defaults)\n"
        )
        signature_start = original.index(signature_header)
        signature_json_start = signature_start + len(signature_header)
        signature_end = original.index("\n\nPRIOR ATTEMPT HISTORY", signature_json_start)
        original_signature_doc = json.loads(original[signature_json_start:signature_end])
        full_signatures = _expand_registered_signatures(original_signature_doc)
        catalog = _paper_registered_signature_catalog(full_signatures)
        packed_header = (
            "REGISTERED OPERATOR SIGNATURES (lossless input/parameter contract references; "
            "expand each contract_ref from the declared tables and defaults)\n"
        )
        packed = (original[:signature_start] + packed_header +
            json.dumps(catalog, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            original[signature_end:])
        self.assertEqual(_expand_paper_registered_signature_catalog(catalog), original_signature_doc)

        digest_header = "CURRENT PUBLIC CASE DIGEST\n"
        digest_start = packed.index(digest_header)
        digest_json_start = digest_start + len(digest_header)
        digest_end = packed.index("\n\nCURRENT ALLOWLISTED RUNTIME VALUES", digest_json_start)
        original_digest = json.loads(packed[digest_json_start:digest_end])
        digest = _front_code_paper_metric_fields(original_digest)
        packed = (packed[:digest_json_start] +
            json.dumps(digest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\nAvailable metric fields use e4-front-codepoint-prefix/1: each row is "
            "[common_prefix_codepoint_count, suffix]; reconstruct each exact next name as "
            "previous[:count] + suffix. Preserve field order and spelling." +
            packed[digest_end:])
        expanded_fields = _expand_front_coded_paper_metric_fields(
            json.loads(packed[digest_json_start:packed.index("\nAvailable metric fields", digest_json_start)])
                ["available_metric_fields"])
        self.assertEqual(expanded_fields, original_digest["available_metric_fields"])
        self.assertLess(len(packed), len(original))

        for start, end in (("PRIOR ATTEMPT HISTORY", "PRIOR ANALYSIS PLANS"),
                           ("PRIOR ANALYSIS PLANS", "DATA COVERAGE GUIDANCE"),
                           ("VALIDATED CAPABILITY GAP RECORD", "REGISTERED COMPOSITE PROGRAMS"),
                           ("PRIOR ATOMIC IR BODIES", "STAGE 3 EVOLVE PHASE")):
            if start in original and end in original:
                self.assertEqual(original[original.index(start):original.index(end, original.index(start))],
                    packed[packed.index(start):packed.index(end, packed.index(start))])
        self.assertEqual(len(row["request_payload"]["messages"]), 2)
        self.assertEqual(row["request_payload"]["max_tokens"], 8192)

    @unittest.skipUnless((ROOT / "artifacts/m3/model_runs/e4s43/audits/20261024T164037Z-1f42bf1e").is_dir(),
        "optional captured-prompt integration: ignored seed43 model-call logs are unavailable")
    def test_all_saved_phase_and_repair_maxima_pack_losslessly(self):
        selected = prompt_candidates._phase_maxima()
        self.assertEqual(len(selected), 8)
        groups = {(row["m4_phase"], row.get("m4_static_repair") is True)
                  for _path, row in selected}
        self.assertEqual(groups, {(phase, repair) for phase in
            ("select", "compose", "evolve", "stage3_serve") for repair in (False, True)})
        for source_path, record in selected:
            original_request = record["request_payload"]
            packed_request, _metadata = prompt_candidates.pack_request(original_request)
            self.assertEqual({key: value for key, value in packed_request.items() if key != "messages"},
                {key: value for key, value in original_request.items() if key != "messages"})
            self.assertEqual(packed_request["messages"][0], original_request["messages"][0])
            before = original_request["messages"][1]["content"]
            after = packed_request["messages"][1]["content"]
            self.assertEqual(after, prompt_candidates.pack_request(original_request)[0]["messages"][1]["content"])

            old_plain = "\n\nREGISTERED OPERATOR SIGNATURES\n"
            old_compact = ("\n\nREGISTERED OPERATOR SIGNATURES "
                "(lossless defaults encoding; expand omitted catalog fields from listed defaults)\n")
            new_ref = ("\n\nREGISTERED OPERATOR SIGNATURES (lossless input/parameter contract references; "
                "expand each contract_ref from the declared tables and defaults)\n")
            reverse = after
            if new_ref in after:
                pos = after.index(new_ref)
                start = pos + len(new_ref)
                packed_doc, consumed = json.JSONDecoder().raw_decode(after[start:])
                suffix_start = start + consumed
                expanded_compact = _expand_paper_registered_signature_catalog(packed_doc)
                if old_compact in before:
                    restored_doc = expanded_compact
                    old_header = old_compact
                else:
                    restored_doc = _expand_registered_signatures(expanded_compact)
                    old_header = old_plain
                original_sig_pos = before.index(old_header)
                original_sig_start = original_sig_pos + len(old_header)
                original_sig, _ = json.JSONDecoder().raw_decode(before[original_sig_start:])
                self.assertEqual(restored_doc, original_sig)
                reverse = (reverse[:pos] + old_header +
                    json.dumps(original_sig, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
                    reverse[suffix_start:])

            digest_header = "\n\nCURRENT PUBLIC CASE DIGEST\n"
            if digest_header in before:
                self.assertIn(digest_header, reverse)
                pos = reverse.index(digest_header)
                start = pos + len(digest_header)
                packed_digest, consumed = json.JSONDecoder().raw_decode(reverse[start:])
                suffix_start = start + consumed
                guidance = prompt_candidates.FRONT_CODE_GUIDANCE
                if reverse.startswith(guidance, suffix_start):
                    suffix_start += len(guidance)
                restored_digest = dict(packed_digest)
                restored_digest["available_metric_fields"] = _expand_front_coded_paper_metric_fields(
                    packed_digest["available_metric_fields"])
                original_pos = before.index(digest_header)
                original_start = original_pos + len(digest_header)
                original_digest, _ = json.JSONDecoder().raw_decode(before[original_start:])
                self.assertEqual(restored_digest, original_digest)
                reverse = (reverse[:start] +
                    json.dumps(original_digest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
                    reverse[suffix_start:])
            self.assertEqual(reverse, before, f"prompt packing changed noncatalog context: {source_path.name}/{record['m4_phase']}/{record['m4_static_repair']}")

    def test_paper_stage_prompts_and_repairs_select_phase_specific_catalog_views(self):
        from e4core.m4_fixtures import write_synthetic_public_case
        fixture_root = Path(tempfile.mkdtemp(prefix="paper-prompt-case-test-"))
        self.addCleanup(shutil.rmtree, fixture_root, True)
        case = write_synthetic_public_case(fixture_root)
        (fixture_root / "config").mkdir()
        shutil.copy2(ROOT / "config/paper_starter_runtime.json",
                     fixture_root / "config/paper_starter_runtime.json")
        app_kpi = EntityRef("app_kpi", "synthetic_tc")
        app_points = tuple(MetricPoint("bank/metric_app.csv", timestamp, app_kpi,
            "mrt", None, "synthetic_tc", 10.0 if window == "reference" else 20.0,
            str(10.0 if window == "reference" else 20.0), f"synthetic-app:{window}:{minute}")
            for window, start in (("reference", case.telemetry.reference_window.start_ms_utc),
                                  ("alert", case.telemetry.alert_window.start_ms_utc))
            for minute in range(30) for timestamp in (start + minute * 60_000,))
        case = replace(case, telemetry=replace(case.telemetry,
            metrics=case.telemetry.metrics + app_points))
        starter = make_paper_starter_input(case.telemetry, root=fixture_root)
        registry = make_paper_starter_registry(bank_only=True, starter_input=starter, root=ROOT)
        books = bank_profile_playbooks(registry)
        snapshot_root = Path(tempfile.mkdtemp(prefix="paper-prompt-stage-test-"))
        self.addCleanup(shutil.rmtree, snapshot_root, True)
        snapshot = create_library_snapshot({"examples": books}, [], [],
            {"fixture": "current-registry-prompt-test"}, snapshot_root,
            builtin_specs=registry.signatures(),
            capacity_policy="reconstructed-paper-starter-v1")
        composites = []
        runtime = build_m4_runtime(case.alert, root=ROOT)
        from e4core.m4_operator_ir import operator_ir_capability_report
        report = operator_ir_capability_report(starter, registry,
            runtime_values=runtime.as_dict()["values"], root=ROOT)
        from e4core.m4_controller import _paper_v2_capability_prompt_summary
        capability_summary = _paper_v2_capability_prompt_summary(report)
        policy = _policy(ROOT)
        available = {row["name"] for row in available_bank_profile_playbooks(
            [dict(row) for row in snapshot.playbooks.values()], starter)}
        policy = {**policy, "_paper_case_capabilities": dict(starter.capabilities),
                  "_paper_case_available_books": sorted(available),
                  "_paper_ir_capability_report": report,
                  "_paper_ir_capability_prompt_summary": capability_summary}
        digest = public_case_digest(case, root=ROOT)
        gap = {"schema_version": "e4-capability-gaps/1", "prior_plans": [], "capabilities": []}
        exact_fields = paper_parameter_catalog()["enums"]["paper.bank_component_metric_fields"]["values"]
        evolve_system = PAPER_M4_SYSTEM_PROMPTS["evolve"]
        self.assertIn("host derives fixed input ports", evolve_system)
        self.assertIn("Use only the closed V2 kernel catalog", evolve_system)
        self.assertIn("No Python, source strings, imports", evolve_system)
        self.assertIn("starter_input` is required only for reachable log kernels", evolve_system)
        self.assertIn("V1 bodies are provided for novelty comparison/replay", evolve_system)
        self.assertIn("but new proposals must use V2", evolve_system)
        serve_system = PAPER_M4_SYSTEM_PROMPTS["stage3_serve"]
        self.assertIn("Normalize each scorer's eligible nonnegative scores", serve_system)
        self.assertIn("association is noncausal", serve_system.casefold())
        self.assertIn("unsupported stays unknown", serve_system.casefold())

        def parse_section(prompt: str, header: str):
            offset = prompt.index(header) + len(header)
            while offset < len(prompt) and prompt[offset].isspace():
                offset += 1
            return json.JSONDecoder().raw_decode(prompt[offset:])[0]

        def assert_exact_choice_catalog(prompt: str) -> None:
            self.assertIn('"paper.bank_component_metric_fields"', prompt)
            self.assertIn('"value_field":"value"', prompt)
            self.assertIn('"parameter_value_type":"string"', prompt)
            for field in exact_fields:
                self.assertIn(json.dumps(field, ensure_ascii=False), prompt)
        for stage in ("select", "compose", "evolve", "stage3_serve"):
            prompt = _stage_prompt(stage, digest=digest, registry=registry, snapshot=snapshot,
                runtime=runtime, history=[], policy=policy, query_budget=M4QueryBudget(None),
                gap_record=gap, composite_operator_specs=composites)
            self.assertIn("e4-paper-observed-field-summary/1", prompt)
            self.assertIn("summarized by exact unique count, SHA-256", prompt)
            self.assertIn("unsupported is not zero", prompt)
            self.assertIn("trace duration units are undeclared", prompt)
            if stage in {"compose", "evolve", "stage3_serve"}:
                self.assertIn("abstention only for the affected computation or input", prompt)
                self.assertIn("independent supported analyses may proceed", prompt)
            if stage == "evolve":
                self.assertIn("e4-paper-evolve-operator-catalog/2", prompt)
                self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG", prompt)
                self.assertNotIn("PAPER PARAMETER ENUMS AND CONSTRAINTS", prompt)
                self.assertIn("metric_selector_catalog", prompt)
                self.assertIn('"choices_ref":"metric_selector_catalog"', prompt)
                self.assertIn("selector_ref", prompt)
                self.assertIn("Cite up to three exact prior plan references from the available history", prompt)
                self.assertIn("do not emit outer `inputs`, `input_ports`, or `input_requirements`", prompt)
                capability = parse_section(prompt,
                    "TRUSTED TYPED-IR V2 DATA/SUPPORT CAPABILITY SUMMARY\n")
                schema = parse_section(prompt,
                    "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG\n")
                self.assertNotIn("selector_status_by_ref", capability)
                self.assertTrue(schema["selector_case_resolution_profiles"]["profiles"])
                self.assertTrue(all("support_profile_ref" in row
                    for row in schema["metric_selector_catalog"]))
                self.assertTrue(schema["selector_semantic_profiles"])
            elif stage == "compose":
                assert_exact_choice_catalog(prompt)
                self.assertIn(PAPER_STAGE2_RESPONSE_SCHEMA, prompt)
                self.assertIn(PAPER_ADAPTATION_RECORD_SCHEMA, prompt)
                self.assertIn("soft preference", prompt)
                self.assertIn("a ranking change is not required", prompt)
            else:
                assert_exact_choice_catalog(prompt)
            if stage in {"compose", "stage3_serve"}:
                self.assertIn("PRIOR PUBLIC ANALYSIS EVIDENCE", prompt)
                self.assertIn("TRUSTED PAPER INPUT CONTRACTS", prompt)
                self.assertIn("duration_unit", prompt)
                self.assertIn("raw_log_text", prompt)
                capability = parse_section(prompt,
                    "TRUSTED TYPED-IR V2 DATA/SUPPORT CAPABILITY SUMMARY\n")
                log = next(row for row in capability["computations"]
                    if row.get("computation") == "native_log_pattern_composition")
                self.assertNotIn("pattern_features_by_support", log)
                self.assertEqual(log["pattern_features_omitted_from_prompt"],
                    log["pattern_feature_count"])
                self.assertIn("window_denominators_by_stream", log)
                self.assertEqual(capability["full_host_report_sha256"], _canonical_hash(report))
                semantics = log["derived_count_semantics"]
                source_semantics = next(row["details"]["derived_count_semantics"]
                    for row in report["computations"]
                    if row.get("computation") == "native_log_pattern_composition")
                for key in ("field_name", "kind", "unit", "unit_status", "direction",
                            "transform", "classification_status", "ranking_eligible",
                            "reset_policy", "source_table", "selector_ref"):
                    self.assertEqual(semantics[key], source_semantics[key])
                self.assertEqual(log["derived_count_semantics_ref"],
                    "full_host_report.derived_count_semantics")
                self.assertNotIn("provenance", semantics)
                self.assertNotIn("derived_from", semantics)
            if stage != "evolve":
                self.assertIn("e4-paper-signature-contract-refs/2", prompt)
                self.assertIn("input_contracts", prompt)
                self.assertIn("parameter_contracts", prompt)
                self.assertIn("input_requirement_contracts", prompt)
                signature_header = prompt.index(
                    "REGISTERED OPERATOR SIGNATURES (columnar host contract view")
                signature_start = prompt.index("\n", signature_header) + 1
                signature_catalog = json.JSONDecoder().raw_decode(
                    prompt[signature_start:])[0]
                omission = signature_catalog["parameter_description_omission"]
                self.assertEqual(omission["omitted_parameter_description_count"], 15)
                self.assertRegex(omission["omitted_parameter_description_map_sha256"],
                    r"^[0-9a-f]{64}$")
                self.assertEqual(omission["full_host_catalog_ref"],
                    "trusted_registered_operator_signatures.parameter_contracts")
                contract_columns = {key for shape in signature_catalog[
                    "parameter_contract_table"]["shape_tables"]
                    for key in shape["column_order"]}
                self.assertNotIn("description", contract_columns)
                self.assertTrue({"type", "required", "choices"} <= contract_columns)
                parameter_catalog = parse_section(prompt,
                    "PAPER PARAMETER ENUMS AND CONSTRAINTS (choices_ref values are declared once here)\n")
                exact_enum = parameter_catalog["enums"]["paper.bank_component_metric_fields"]
                self.assertEqual([row["value"] for row in exact_enum["values"]], exact_fields)
                self.assertEqual(exact_enum["value_field"], "value")
                self.assertTrue(all("support_profile_ref" in row for row in exact_enum["values"]))
                self.assertIn("case_support_provenance", parameter_catalog)
            digest_doc = parse_section(prompt, "CURRENT PUBLIC CASE DIGEST\n")
            kpi = digest_doc["primary_app_kpi_context"]
            if stage == "evolve":
                primary = parse_section(prompt,
                    "TRUSTED TYPED-IR V2 DATA/SUPPORT CAPABILITY SUMMARY\n")["primary_kpi"]
                self.assertIn("selected_identity", primary)
                self.assertIn("selection_support", primary)
            else:
                self.assertNotIn("selection_reason", kpi)
                self.assertNotIn("source_policy", kpi)
                self.assertIn("selection_policy_ref", kpi)
                self.assertIn("policy_sha256", kpi)
                self.assertEqual(kpi["interpretation"], "field-name working interpretation only")
                self.assertTrue(any(key in kpi for key in ("kpi_id", "selected_identity")))
                self.assertTrue(any(key in kpi for key in ("reference_support", "paired_support", "support")))
                source_kpi = digest["primary_app_kpi_context"]
                self.assertEqual(kpi["reference"], source_kpi["reference"])
                self.assertEqual(kpi["alert"], source_kpi["alert"])
            inventory = digest_doc["available_metric_fields"]
            self.assertEqual(inventory["examples"], [])
            self.assertEqual(inventory["represented_field_count"], 0)
            self.assertEqual(inventory["omitted_field_count"],
                inventory["observed_unique_field_count"])
            self.assertRegex(inventory["field_names_sha256"], r"^[0-9a-f]{64}$")
            self.assertNotIn("file_sha256", digest_doc)
            self.assertIn("source_file_inventory", digest_doc)
            runtime_values = parse_section(prompt, "CURRENT ALLOWLISTED RUNTIME VALUES\n")
            self.assertEqual(runtime_values["values"], runtime.as_dict()["values"])
            self.assertNotIn("provenance", runtime_values)
            self.assertRegex(runtime_values["provenance_map_sha256"], r"^[0-9a-f]{64}$")
            repair = _static_repair_prompt(prompt, '{"not":"valid"}',
                {"code": "static_contract", "message": "bounded repair smoke"})
            self.assertIn("STATIC CONTRACT ERROR ONLY", repair)
            self.assertIn("bounded repair smoke", repair)
            if stage == "evolve":
                self.assertIn("metric_selector_catalog", repair)
                self.assertNotIn("PAPER PARAMETER ENUMS AND CONSTRAINTS", repair)
            else:
                assert_exact_choice_catalog(repair)

    def test_paper_adaptation_record_requires_real_prior_ref_and_scored_output(self):
        registry = make_paper_starter_registry(bank_only=True)
        score = next(row for row in registry.signatures() if row["output_type"] == "ScoredCandidates")
        dag = {"schema_version": "e4-dag/1", "nodes": [{"id": "score",
            "operator": score["name"], "version": score["version"],
            "inputs": {}, "params": {}, "bindings": {}}],
            "return": {"node": "score"}}
        prior_ref = "a" * 64
        history = [{"stage": "stage1_select", "plan_summary": {"dag_sha256": prior_ref}}]
        record = {"schema_version": PAPER_ADAPTATION_RECORD_SCHEMA,
            "tested_plan_refs": [prior_ref],
            "public_observations": ["The previous supported plan used a mean-comparison statistic."],
            "unexplained_limit": "A distinct location statistic has not been tested.",
            "hypothesis_change": "Use a supported location-shift statistic with the same declared source.",
            "statistic": "median permutation contrast",
            "source_scope": "allowlisted component metric fields",
            "window_relationship": "reference versus alert window",
            "typed_evidence_outputs": ["score"]}
        validated = _validate_paper_adaptation_record(record, history, dag, registry)
        self.assertEqual(validated, record)
        bad = {**record, "tested_plan_refs": ["b" * 64]}
        with self.assertRaises(M4ControllerError):
            _validate_paper_adaptation_record(bad, history, dag, registry)
        bad_outputs = {**record, "typed_evidence_outputs": ["missing-node"]}
        with self.assertRaises(M4ControllerError):
            _validate_paper_adaptation_record(bad_outputs, history, dag, registry)

    def test_expanded_graph_duplicate_rejection_ignores_node_names_only(self):
        registry = make_paper_starter_registry(bank_only=True)
        source = next(row for row in registry.signatures() if row["name"] == "case.telemetry")
        first = {"schema_version": "e4-dag/1", "nodes": [{"id": "source_a",
            "operator": source["name"], "version": source["version"],
            "inputs": {}, "params": {}, "bindings": {}}], "return": {"node": "source_a"}}
        renamed = {"schema_version": "e4-dag/1", "nodes": [{"id": "different_node_id",
            "operator": source["name"], "version": source["version"],
            "inputs": {}, "params": {}, "bindings": {}}], "return": {"node": "different_node_id"}}
        history = [{"stage": "stage1_select", "_expanded_plan": {"dag": first}}]
        with self.assertRaises(M4ControllerError) as raised:
            _reject_exact_prior_paper_plan(renamed, history)
        self.assertEqual(raised.exception.code, "paper_stage2_semantic_duplicate")


if __name__ == "__main__":
    unittest.main()
