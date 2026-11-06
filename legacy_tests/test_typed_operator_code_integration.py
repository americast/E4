from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import json
import shutil
from unittest.mock import patch

from e4core.m4_generated import (TYPED_OPERATOR_CODE_PROFILE,
    typed_operator_code_contract_schema, validate_generated_spec)
from e4core.m4_library import (PAPER_STARTER_CAPACITY_POLICY, create_library_snapshot,
    load_library_snapshot, stage3_authoring_profile, admit_after_accepted)
from e4core.cache import implementation_fingerprint
from e4core.bank_adapter import sha256_file
from e4core.paper_starter import bank_profile_playbooks, make_paper_starter_registry
from tests.test_m4_controller import FakeClient, FakeFeedback, _fixture_root


SOURCE = '''def operator(inputs, params, runtime):
    telemetry = telemetry_view(inputs["data"])
    result = metric_points(telemetry, "bank/metric_container.csv", telemetry.alert_window)
    return make_scored_candidates([], "typed-operator-code-v1")
'''


def _spec(name: str, source: str = SOURCE) -> dict:
    return {"name": name, "version": "v1", "description": "Typed code test",
        "family": "metric", "input_ports": [{"name": "data", "type": "Telemetry"}],
        "output_type": "ScoredCandidates", "parameters": {},
        "authoring_profile": TYPED_OPERATOR_CODE_PROFILE,
        "input_requirements": {"data": {"source_columns": {
            "bank/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"]},
            "statistics": ["raw"], "group_by": [], "support_unit": None,
            "coverage": "full_window"}}, "source": source}


class TypedOperatorCodeIntegrationTests(unittest.TestCase):
    def test_schema_publishes_general_python_api_and_table_specific_capabilities(self):
        schema = typed_operator_code_contract_schema()
        self.assertEqual(schema["authoring_profile"], TYPED_OPERATOR_CODE_PROFILE)
        self.assertEqual(schema["input_ports"]["fields"]["type"]["enum"],
            ["Aggregate", "CandidateSet", "ScoredCandidates", "Telemetry"])
        requirements = schema["optional_fields"]["input_requirements"]["port_fields"]
        self.assertIn("group_by_by_table", requirements)
        api = schema["typed_operator_code_api"]
        self.assertIn("aggregate_records", {row["name"] for row in api["helpers"]})
        self.assertIn("log_records", {row["name"] for row in api["helpers"]})
        self.assertIn("metric_key", api["semantics"]["metric_fields"])
        self.assertIn("source_field names the native value column", api["semantics"]["metric_fields"])
        self.assertNotIn("kernels", api)
        self.assertLessEqual(schema["source"]["max_utf8_bytes"], 8192)

    def test_unicode_source_limit_is_measured_in_utf8_bytes(self):
        base = SOURCE + "\n#"
        remaining = 8192 - len(base.encode("utf-8"))
        exact = base + "é" * (remaining // 2) + "x" * (remaining % 2)
        self.assertEqual(len(exact.encode("utf-8")), 8192)
        self.assertEqual(validate_generated_spec(_spec("evolved.utf8_exact", exact))["authoring_profile"],
            TYPED_OPERATOR_CODE_PROFILE)
        with self.assertRaisesRegex(ValueError, "8192 UTF-8 bytes"):
            validate_generated_spec(_spec("evolved.utf8_over", exact + "é"))

    def test_statistical_cutoffs_are_parameters_but_math_constants_remain_available(self):
        source = '''def operator(inputs, params, runtime):
    telemetry = telemetry_view(inputs["data"])
    points = metric_points(telemetry, "bank/metric_container.csv", telemetry.alert_window)
    threshold = params["decision_threshold"]
    accumulated = 0.0
    for point in points.rows:
        if point.value is not None:
            accumulated = max(0.0, accumulated + float(point.value))
            if accumulated >= threshold:
                accumulated = threshold
    return make_scored_candidates([], "typed-operator-code-v1")
'''
        spec = _spec("evolved.parameterized_cusum", source)
        spec["parameters"] = {"decision_threshold": {"type": "number", "required": True}}
        self.assertEqual(validate_generated_spec(spec)["authoring_profile"], TYPED_OPERATOR_CODE_PROFILE)
        alias_cutoff = SOURCE.replace(
            "result = metric_points(telemetry, \"bank/metric_container.csv\", telemetry.alert_window)",
            "result = metric_points(telemetry, \"bank/metric_container.csv\", telemetry.alert_window)\n    minimum_support = 20\n    if result.coverage.support >= minimum_support:\n        return make_scored_candidates([], \"typed-operator-code-v1\")")
        with self.assertRaisesRegex(ValueError, "statistical decision cutoffs"):
            validate_generated_spec(_spec("evolved.hardcoded_support", alias_cutoff))
        direct_cutoff = SOURCE.replace(
            "return make_scored_candidates([], \"typed-operator-code-v1\")",
            "p_value = 0.02\n    if p_value < 0.05:\n        return make_scored_candidates([], \"typed-operator-code-v1\")\n    return make_scored_candidates([], \"typed-operator-code-v1\")")
        with self.assertRaisesRegex(ValueError, "statistical decision cutoffs"):
            validate_generated_spec(_spec("evolved.hardcoded_pvalue", direct_cutoff))

    def test_model_context_preflight_uses_tokens_and_never_sends_an_overflowing_prompt(self):
        from e4core.m4_controller import M4ModelClient
        profile = {"model_alias": "fixture", "temperature": 0.0, "top_p": 1.0,
            "top_k": 1, "seed": 1, "max_generation_tokens": 8192,
            "context_tokens": 32768, "reasoning_mode": "off"}
        client = M4ModelClient(profile, base_url="http://127.0.0.1:1", context_tokens=32768)
        requests = []
        def preflight(path, request):
            requests.append((path, request))
            if path == "/apply-template":
                return 200, "{}", {"prompt": "synthetic prompt"}, None
            return 200, "{}", {"tokens": [0] * 24_577}, None
        with patch.object(client, "_post_json", side_effect=preflight):
            call = client.complete("synthetic prompt", task_id="preflight", attempt=1)
        self.assertEqual(call.error["code"], "context_budget_exceeded")
        self.assertEqual(call.error["prompt_tokens"], 24_577)
        self.assertEqual(call.error["max_generation_tokens"], 8192)
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0][0], "/apply-template")
        self.assertEqual(requests[1][0], "/tokenize")

    def test_model_client_routes_only_persisted_code_mode_to_python_system_prompt(self):
        from e4core.m4_controller import M4ModelClient
        profile = {"model_alias": "fixture", "temperature": 0.0, "top_p": 1.0,
            "top_k": 1, "seed": 1, "max_generation_tokens": 8192,
            "context_tokens": 32768, "reasoning_mode": "off"}
        code = M4ModelClient(profile, harness_profile=
            "reconstructed-paper-starter-v1-typed-operator-code-v1")
        code_payload = code.payload_for_stage("evolve", "USER SOURCE PROMPT")
        code_system = code_payload["messages"][0]["content"]
        self.assertIn("authors new Python operators", code_system)
        self.assertIn("typed-operator-code-v1", code_system)
        self.assertNotIn("closed V2 kernel catalog", code_system)
        legacy = M4ModelClient(profile, harness_profile="reconstructed-paper-starter-v1")
        legacy_system = legacy.payload_for_stage("evolve", "LEGACY V2 PROMPT")["messages"][0]["content"]
        self.assertIn("typed-operator-ir-v2", legacy_system)
        self.assertNotIn("authors new Python operators", legacy_system)

    def test_code_source_normalizes_and_renamed_ast_duplicate_is_rejected(self):
        from e4core.m4_controller import _candidate_spec_mapping, build_m4_runtime, M4ControllerError
        from e4core.paper_starter import make_paper_starter_registry

        registry = make_paper_starter_registry(bank_only=True)
        runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000})
        first = validate_generated_spec(_spec("evolved.first_code"))
        self.assertEqual(first["authoring_profile"], TYPED_OPERATOR_CODE_PROFILE)
        self.assertEqual(first["version"].split("+")[0], "v1")
        with self.assertRaises(M4ControllerError) as raised:
            _candidate_spec_mapping([_spec("evolved.renamed_code")], registry,
                runtime, [], [first], allowed_authoring_profiles=(TYPED_OPERATOR_CODE_PROFILE,))
        self.assertEqual(raised.exception.code, "stage3_source_duplicate_body")
        self.assertIn("does not establish mathematical equivalence", str(raised.exception))

    def test_case_specific_default_and_source_literals_are_rejected_but_public_selectors_are_allowed(self):
        from e4core.m4_controller import _reject_program_identifier_literals, M4ControllerError
        value = _spec("evolved.literal_guard")
        value["parameters"] = {"target": {"type": "string", "default": "Tomcat01"}}
        normalized = validate_generated_spec(value)
        with self.assertRaises(M4ControllerError) as caught:
            _reject_program_identifier_literals(normalized, {"Tomcat01", "bank-test-case"})
        self.assertEqual(caught.exception.code, "operator_parameter_identifier_literal")
        safe = validate_generated_spec(_spec("evolved.public_selector"))
        _reject_program_identifier_literals(safe, {"Tomcat01", "bank-test-case"})

    def test_missing_capability_evidence_invalid_field_and_helper_fail_closed(self):
        from e4core.m4_controller import (_normalize_gap_record_draft, _validate_gap_record,
            M4ControllerError, build_m4_runtime)
        from e4core.paper_starter import make_paper_starter_registry

        registry = make_paper_starter_registry(bank_only=True)
        runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000})
        spec = validate_generated_spec(_spec("evolved.missing_gap"))
        history = [{"stage": "stage2_compose", "plan_summary": {"dag_sha256": "a" * 64}}]
        gap = _normalize_gap_record_draft({"schema_version": "e4-capability-gaps/1",
            "prior_plans": [], "capabilities": ["a distinct typed computation"]},
            [spec], registry, history)
        with self.assertRaises(M4ControllerError) as missing:
            _validate_gap_record(gap, [spec], registry, history)
        self.assertEqual(missing.exception.code, "stage3_gap_prior_missing")

        invalid_column = _spec("evolved.invalid_column")
        invalid_column["input_requirements"]["data"]["source_columns"][
            "bank/metric_container.csv"].append("private_raw_text")
        with self.assertRaisesRegex(ValueError, "unknown or duplicate column"):
            validate_generated_spec(invalid_column)

        invalid_helper = _spec("evolved.invalid_helper",
            SOURCE.replace("metric_points(", "private_metric_kernel("))
        with self.assertRaisesRegex(ValueError, "private_metric_kernel"):
            validate_generated_spec(invalid_helper)

    def test_code_profile_can_author_a_typed_operator_without_telemetry_port(self):
        value = _spec("evolved.candidate_only")
        value["input_ports"] = [{"name": "candidates", "type": "CandidateSet"}]
        value["input_requirements"] = {"candidates": {
            "source_columns": {}, "statistics": [], "group_by": ["component"],
            "support_unit": None, "coverage": "full_window"}}
        value["source"] = '''def operator(inputs, params, runtime):
    candidates = candidate_views(inputs["candidates"])
    return make_scored_candidates([], "typed-operator-code-v1")
'''
        normalized = validate_generated_spec(value)
        self.assertEqual(normalized["input_ports"][0]["type"], "CandidateSet")

    def test_incompatible_actual_aggregate_producer_is_rejected_before_serving(self):
        from e4core.dag import verify_dag
        from e4core.m4_capabilities import check_typed_input_capabilities, InputCapabilityError
        from e4core.m4_controller import build_m4_runtime

        value = _spec("evolved.trace_consumer",
            '''def operator(inputs, params, runtime):
    aggregate = aggregate_view(inputs["data"])
    return make_scored_candidates([], "typed-operator-code-v1")
''')
        value["input_ports"] = [{"name": "data", "type": "Aggregate"}]
        value["input_requirements"] = {"data": {
            "source_columns": {"bank/trace_span.csv": ["timestamp", "cmdb_id", "trace_id", "duration"]},
            "statistics": ["per_trace_median"], "group_by": [],
            "group_by_by_table": {"bank/trace_span.csv": ["component"]},
            "support_unit": "trace_id", "coverage": "full_window"}}
        spec = validate_generated_spec(value)
        from e4core.paper_starter import make_paper_starter_registry
        registry = make_paper_starter_registry(bank_only=True, generated_specs=[spec])
        versions = {row["name"]: row["version"] for row in registry.signatures()}
        dag = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "telemetry", "operator": "case.telemetry", "version": "1",
                "inputs": {}, "params": {}, "bindings": {}},
            {"id": "metric", "operator": "bank.metrics.aggregate",
                "version": versions["bank.metrics.aggregate"], "inputs": {"data": "telemetry"},
                "params": {"window": "alert"}, "bindings": {}},
            {"id": "trace_consumer", "operator": spec["name"], "version": spec["version"],
                "inputs": {"data": "metric"}, "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
                "inputs": {"scores": ["trace_consumer"]}, "params": {}, "bindings": {}}],
            "return": {"node": "answer"}}
        runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000})
        verified = verify_dag(dag, registry, runtime, max_nodes=40, max_depth=12)
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(verified, [spec])
        self.assertEqual(raised.exception.code, "input_source_column_unavailable")

    def test_snapshot_persists_new_profile_and_rejected_admission_preserves_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "library"
            snapshot = create_library_snapshot([], [], [], {"source": "synthetic"}, path,
                capacity_policy=PAPER_STARTER_CAPACITY_POLICY,
                stage3_authoring_profile=TYPED_OPERATOR_CODE_PROFILE)
            loaded = load_library_snapshot(snapshot.path)
            self.assertEqual(stage3_authoring_profile(loaded), TYPED_OPERATOR_CODE_PROFILE)
            same = admit_after_accepted(loaded, [], [], {}, accepted=False,
                verified_reachable_operator_keys=[])
            self.assertIs(same, loaded)
            self.assertEqual(stage3_authoring_profile(same), TYPED_OPERATOR_CODE_PROFILE)

    def test_authored_family_cannot_exempt_retention_or_assign_a_consensus_group(self):
        from e4core.m4_controller import build_m4_runtime
        from e4core.paper_starter import make_paper_starter_registry
        from e4core.contracts import ScoredCandidates
        from e4core.executor import NodeContext

        spec = _spec("evolved.claimed_utility")
        spec["family"] = "utility"
        normalized = validate_generated_spec(spec)
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = create_library_snapshot([], [normalized], [], {},
                Path(temporary) / "library", capacity_policy=PAPER_STARTER_CAPACITY_POLICY,
                stage3_authoring_profile=TYPED_OPERATOR_CODE_PROFILE)
            self.assertEqual(snapshot.document["policy"]["paper_counts"]["diagnostic_operator_count"], 1)
        registry = make_paper_starter_registry(bank_only=True, generated_specs=[normalized])
        terminal = registry.require("paper.answers.weighted_consensus", "1")
        runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000})
        context = NodeContext("synthetic", "consensus", "paper.answers.weighted_consensus",
            runtime.as_dict()["values"], {}, None, None, "a" * 64)
        with self.assertRaisesRegex(Exception, "no trusted semantic consensus group"):
            terminal.implementation({"scores": (ScoredCandidates((), "typed-operator-code-v1"),)},
                {"family_weights": {}, "tie_tolerance": 1e-9, "limit": 5}, context)

    def test_absent_profile_keeps_legacy_v2_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = create_library_snapshot([], [], [], {"source": "synthetic"},
                Path(temporary) / "library", capacity_policy=PAPER_STARTER_CAPACITY_POLICY)
            self.assertEqual(stage3_authoring_profile(snapshot), "typed-operator-ir-v2")

    def test_explicit_code_snapshot_rejects_non_code_stage3_admission(self):
        from e4core.m4_library import LibraryError
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = create_library_snapshot([], [], [], {"source": "synthetic"},
                Path(temporary) / "library", capacity_policy=PAPER_STARTER_CAPACITY_POLICY,
                stage3_authoring_profile=TYPED_OPERATOR_CODE_PROFILE)
            with self.assertRaisesRegex(LibraryError, "Stage 3 authoring profile"):
                admit_after_accepted(snapshot, [{"name": "evolved.wrong_profile",
                    "version": "v1", "authoring_profile": "typed-operator-ir-v2"}], [], {},
                    accepted=True, verified_reachable_operator_keys={"evolved.wrong_profile@v1"},
                    invocation_case_id="synthetic")

    @unittest.skipUnless(shutil.which("bwrap"), "requires the isolated bwrap worker")
    def test_controller_authors_serves_admits_and_reuses_new_python_operator(self):
        from e4core.m4_controller import _canonical_hash, run_diagnostic
        from e4core.m4_library import snapshot_library
        from tests.test_bank_adapter import make_bank_fixture

        candidate_source = '''def operator(inputs, params, runtime):
    telemetry = telemetry_view(inputs["data"])
    points = metric_points(telemetry, "bank/metric_container.csv", telemetry.alert_window)
    if not points.coverage.complete:
        return make_candidate_set([], "evolved.windowed_python_candidates")
    identities: dict[tuple[str, str], bool] = {}
    for point in points.rows:
        if point.value is not None:
            identities[(point.entity_role, point.entity_identifier)] = True
    candidates = []
    for role, identifier in identities:
        candidates.append(make_candidate(make_entity_ref(role, identifier)))
    return make_candidate_set(candidates, "evolved.windowed_python_candidates")
'''
        scorer_source = '''def operator(inputs, params, runtime):
    reference = aggregate_view(inputs["reference"])
    alert = aggregate_view(inputs["alert"])
    reference_by_entity: dict[str, AggregateRowView] = {}
    alert_by_entity: dict[str, AggregateRowView] = {}
    for row in reference.rows:
        if row.statistic == "median" and row.value is not None:
            reference_by_entity[row.entity.identifier] = row
    for row in alert.rows:
        if row.statistic == "median" and row.value is not None:
            alert_by_entity[row.entity.identifier] = row
    candidates = candidate_views(inputs["candidates"])
    window = make_window(alert.window.name, alert.window.start_ms_utc, alert.window.end_ms_utc)
    scored = []
    for candidate in candidates:
        identifier = candidate.entity.identifier
        before = reference_by_entity.get(identifier)
        after = alert_by_entity.get(identifier)
        if before is None or after is None or before.value is None or after.value is None:
            continue
        entity = make_entity_ref(candidate.entity.role, identifier)
        delta = float(after.value) - float(before.value)
        support = min(before.support, after.support)
        evidence = make_evidence("bank/metric_container.csv", window, "median_delta",
            delta, [entity], support, True,
            {"ranking_eligible": True, "source_tables": ["bank/metric_container.csv"],
             "support_unit": "timestamp"})
        scored.append(make_scored_candidate(entity, abs(delta), None, [evidence], "metric"))
    return make_scored_candidates(scored, "typed-operator-code-v1")
'''
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case_id, profile_path = _fixture_root(root, positive_metric_pipeline=True)
            manifest_path = root / "config" / "bank_input_manifest.json"
            first_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            candidate_path = root / "data/prepared/bank/candidate_incidents.jsonl"
            dev_path = root / "data/prepared/bank/dev_incidents.jsonl"
            first_candidate = candidate_path.read_text(encoding="utf-8")
            first_dev = dev_path.read_text(encoding="utf-8")
            clip_path = root / "data/prepared/bank/public/clip_manifest.jsonl"
            first_clips = [json.loads(line) for line in clip_path.read_text(encoding="utf-8").splitlines() if line]
            second_case_id, _ = make_bank_fixture(root, date_key="2021_03_05")
            second_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            candidate_path.write_text(first_candidate + candidate_path.read_text(encoding="utf-8"), encoding="utf-8")
            dev_path.write_text(first_dev + dev_path.read_text(encoding="utf-8"), encoding="utf-8")
            second_clips = [json.loads(line) for line in clip_path.read_text(encoding="utf-8").splitlines() if line]
            clip_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n"
                for row in [*first_clips, *second_clips]), encoding="utf-8")
            sample_path = root / "config" / "bank_sample.json"
            sample_path.write_text(json.dumps({"development_cases": [
                {"incident_id": case_id}, {"incident_id": second_case_id}]}), encoding="utf-8")
            second_manifest["cases"] = {**first_manifest["cases"], **second_manifest["cases"]}
            second_manifest.update({"sample_config_sha256": sha256_file(sample_path),
                "candidate_manifest_sha256": sha256_file(candidate_path),
                "dev_manifest_sha256": sha256_file(dev_path),
                "clip_manifest_sha256": sha256_file(clip_path),
                "deployment_catalog_file_sha256": sha256_file(
                    root / "data/prepared/bank/public/deployment_catalog.json"),
                "scored_manifest_sha256": sha256_file(root / "data/prepared/bank/scored_incidents.jsonl")})
            manifest_path.write_text(json.dumps(second_manifest, sort_keys=True), encoding="utf-8")
            repo = Path(__file__).resolve().parents[1]
            config_files = ("paper_starter_catalog.json", "paper_starter_playbooks.json",
                "paper_starter_runtime.json", "paper_bank_adaptations.json",
                "paper_starter_authoring.json")
            for name in config_files:
                shutil.copy2(repo / "config" / name, root / "config" / name)

            from e4core.bank_adapter import load_bank_case
            case = load_bank_case(case_id, "dev", root=root)
            from e4core.paper_starter import make_paper_starter_input
            starter = make_paper_starter_input(case.telemetry, root=root)
            registry = make_paper_starter_registry(bank_only=True,
                starter_input=starter, root=root)
            books = list(bank_profile_playbooks(registry, starter=starter))
            stage1_book = next(book for book in books if book["name"] == "backend_mean_shift")
            stage2_book = next(book for book in books if book["name"] == "backend_location_shift")
            stage2_score = next(node for node in stage2_book["document"]["nodes"]
                if registry.require(node["operator"], node["version"]).output_type == "ScoredCandidates")
            pin = lambda name: sha256_file(root / "config" / name)
            provenance = {"implementation_fingerprint": implementation_fingerprint(),
                "m4_runtime_policy_sha256": pin("m4_runtime.json"),
                "bank_metric_policy_sha256": pin("bank_metric_kind_policy.json"),
                "bank_kpi_policy_sha256": pin("bank_dev_kpi_policy.json"),
                "bank_seed_library_sha256": pin("bank_seed_library.json"),
                "paper_starter_catalog_sha256": pin("paper_starter_catalog.json"),
                "paper_starter_playbooks_sha256": pin("paper_starter_playbooks.json"),
                "paper_starter_runtime_sha256": pin("paper_starter_runtime.json"),
                "paper_bank_adaptations_sha256": pin("paper_bank_adaptations.json"),
                "paper_starter_authoring_sha256": pin("paper_starter_authoring.json")}
            library_root = root / "library"
            initial = create_library_snapshot({"examples": books}, [], [], provenance,
                library_root, builtin_specs=registry.signatures(),
                capacity_policy=PAPER_STARTER_CAPACITY_POLICY,
                stage3_authoring_profile=TYPED_OPERATOR_CODE_PROFILE)

            candidate_spec = _spec("evolved.windowed_python_candidates", candidate_source)
            candidate_spec["output_type"] = "CandidateSet"
            candidate_spec["input_requirements"]["data"]["group_by"] = []
            scorer_spec = _spec("evolved.windowed_python_score", scorer_source)
            scorer_spec["input_ports"] = [
                {"name": "reference", "type": "Aggregate"},
                {"name": "alert", "type": "Aggregate"},
                {"name": "candidates", "type": "CandidateSet"}]
            aggregate_requirement = {"source_columns": {
                "bank/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"]},
                "statistics": ["median"], "group_by": [],
                "group_by_by_table": {"bank/metric_container.csv": ["component"]},
                "support_unit": None, "coverage": "full_window"}
            scorer_spec["input_requirements"] = {
                "reference": aggregate_requirement, "alert": aggregate_requirement,
                "candidates": {
                "source_columns": {}, "statistics": [], "group_by": [],
                "support_unit": None, "coverage": "any"}}
            registered_versions = {row["name"]: row["version"] for row in registry.signatures()}
            serving_dag = {"schema_version": "e4-dag/1", "nodes": [
                {"id": "telemetry", "operator": "case.telemetry", "version": "1",
                 "inputs": {}, "params": {}, "bindings": {}},
                {"id": "candidates", "operator": candidate_spec["name"], "version": "v1",
                 "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
                {"id": "reference", "operator": "bank.metrics.aggregate",
                 "version": registered_versions["bank.metrics.aggregate"],
                 "inputs": {"data": "telemetry"}, "params": {"window": "reference"}, "bindings": {}},
                {"id": "alert", "operator": "bank.metrics.aggregate",
                 "version": registered_versions["bank.metrics.aggregate"],
                 "inputs": {"data": "telemetry"}, "params": {"window": "alert"}, "bindings": {}},
                {"id": "scores", "operator": scorer_spec["name"], "version": "v1",
                 "inputs": {"reference": "reference", "alert": "alert", "candidates": "candidates"},
                 "params": {}, "bindings": {}},
                {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
                 "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {}}],
                "return": {"node": "answer"}}
            stage2_doc = stage2_book["document"]
            responses = [
                {"playbook": stage1_book["name"], "runtime_overrides": {},
                 "node_parameters": {}, "node_bindings": {}},
                {"schema_version": "e4-paper-stage2-adaptation/1",
                 "adaptation_record": {"schema_version": "e4-paper-adaptation-record/1",
                    "tested_plan_refs": [_canonical_hash(stage1_book["document"])],
                    "public_observations": ["The prior registered comparison returned public supported output."],
                    "unexplained_limit": "A distinct metric relation remains untested.",
                    "hypothesis_change": "Test the alert-window typed metric records with a separately authored statistic.",
                    "statistic": "alert-window metric mean",
                    "source_scope": "public bank metric container records",
                    "window_relationship": "alert window",
                    "typed_evidence_outputs": [stage2_score["id"]]},
                 "dag": stage2_doc, "runtime_overrides": {}},
                {"gap_record": {"schema_version": "e4-capability-gaps/1",
                    "prior_plans": [{"stage": "stage2_compose",
                        "dag_sha256": _canonical_hash(stage2_doc)}],
                    "capabilities": [
                        "Earlier analyses did not derive an exact component candidate set from the bounded typed metric records.",
                        "Earlier analyses did not compare the registered full-window reference and alert metric aggregates with a new Python scorer."]},
                 "operators": [candidate_spec, scorer_spec]},
                {"dag": serving_dag, "runtime_overrides": {}},
            ]
            first_client = FakeClient(responses)
            first = run_diagnostic(case_id, library_root=library_root,
                run_dir=root / "code-authoring-run", feedback=FakeFeedback([False, False, True]),
                profile_path=profile_path, client=first_client, root=root)
            if not first["result"]["accepted"]:
                stages = [{"stage": row.get("stage"), "status": row.get("status"),
                    "execution_error": (row.get("execution") or {}).get("message"),
                    "static_errors": row.get("static_errors")} for row in first["result"]["stages"]]
                self.fail(f"synthetic controller flow did not reach acceptance: {stages}")
            self.assertEqual(first["result"]["stages"][-1]["execution"]["answer"]["$type"], "RankedAnswer")
            self.assertIn("operator(inputs, params, runtime)", first_client.prompts[2])
            self.assertIn("bounded neutral-data API", first_client.prompts[2])
            self.assertNotIn("PRIVATE TYPED-OPERATOR-IR SCHEMA", first_client.prompts[2])
            self.assertIn("proposed Python operators are atomic calls", first_client.prompts[3])
            self.assertEqual(len(first["result"]["stages"][-1]["generated_operator_keys"]), 2)
            self.assertTrue(any("evolved.windowed_python_score" in key
                for key in first["result"]["stages"][-1]["generated_operator_keys"]))
            worker_audit = root / "code-authoring-run" / "audit" / "generated_operator_calls.jsonl"
            self.assertTrue(worker_audit.is_file())
            worker_rows = [json.loads(line) for line in worker_audit.read_text(encoding="utf-8").splitlines() if line]
            self.assertEqual(len(worker_rows), 2)
            self.assertTrue(all(row.get("ok") is True for row in worker_rows))
            self.assertTrue(all(isinstance(row.get("isolation"), dict) and
                row["isolation"].get("backend") == "bubblewrap" and
                row["isolation"].get("active") is True for row in worker_rows))
            self.assertEqual(snapshot_library(library_root).snapshot_id, initial.snapshot_id,
                "controller must preserve the frozen input library and write its admitted child into the run copy")
            output_library = root / "code-authoring-run" / "library"
            active = snapshot_library(output_library)
            self.assertEqual(stage3_authoring_profile(active), TYPED_OPERATOR_CODE_PROFILE)
            code_specs = [row for row in active.document["operators"]
                if row.get("signature", {}).get("authoring_profile") == TYPED_OPERATOR_CODE_PROFILE]
            self.assertEqual(len(code_specs), 2)
            self.assertEqual(code_specs[0]["signature"]["source_sha256"],
                validate_generated_spec(candidate_spec)["source_sha256"])
            admitted_book_names = set(active.playbooks) - set(initial.playbooks)
            self.assertEqual(len(admitted_book_names), 1)
            accepted_book = next(iter(admitted_book_names))
            reused = run_diagnostic(second_case_id, library_root=output_library,
                run_dir=root / "code-reuse-run", feedback=FakeFeedback([True]),
                profile_path=profile_path, client=FakeClient([{"playbook": accepted_book,
                    "runtime_overrides": {}, "node_parameters": {}, "node_bindings": {}}]), root=root)
            self.assertTrue(reused["result"]["accepted"])
            self.assertEqual(reused["result"]["stages"][0]["playbook_name"], accepted_book)


if __name__ == "__main__":
    unittest.main()
