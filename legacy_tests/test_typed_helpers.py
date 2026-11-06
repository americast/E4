"""Synthetic contract tests for isolated generated-worker typed helpers."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import MappingProxyType

from e4core.bank_adapter import load_bank_case
from e4core.cache import deserialize_contract_json, _encode, canonical_json
from e4core.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
    EntityRef, Evidence, ScoredCandidate, ScoredCandidates, Window)
from e4core.executor import NodeContext
from e4core.m4_controller import build_m4_runtime, validate_bank_answer
from e4core.m4_data import make_m4_registry
from e4core.sandbox import run_generated_operator
from e4core.sandbox_worker import (HELPER_API, HELPER_NAMES, TYPED_HELPER_FUNCTIONS,
    aggregate_rows, candidate_items, contract_fields, entity_id, entity_identifier,
    generated_helper_guidance, helper_api_schema, make_aggregate, make_aggregate_row,
    make_candidate, make_candidate_set, make_entity_ref, make_evidence, make_scored_candidate,
    make_scored_candidates, make_window, native_query_rows)
from tests.test_m4_controller import _fixture_root

ROOT = Path(__file__).resolve().parents[1]


class TypedHelperTests(unittest.TestCase):
    def setUp(self):
        self.window = make_window("alert", 1_700_001_800_000, 1_700_003_600_000)
        self.entity = make_entity_ref("component", "Tomcat01")
        self.evidence = make_evidence("bank/metric_container.csv", self.window,
            "native_contrast", 1.5, [self.entity], 4, True,
            {"support_unit": "independent_time_blocks", "details_nested": {
                "labels": ["a", "b"], "flag": True}})

    def test_api_registry_is_single_source_and_schema_is_deterministic(self):
        metadata = helper_api_schema()
        self.assertEqual(HELPER_NAMES, {row["name"] for row in HELPER_API})
        self.assertEqual(set(TYPED_HELPER_FUNCTIONS), HELPER_NAMES - {
            "read_case_json", "read_catalog_json", "contract_fields"})
        self.assertEqual(json.dumps(metadata, sort_keys=True),
                         json.dumps(helper_api_schema(), sort_keys=True))
        self.assertIn("plain CSV dictionaries", generated_helper_guidance())
        self.assertIn("ranking_eligible", generated_helper_guidance())
        self.assertIn("make_evidence", {row["name"] for row in metadata["helpers"]})
        evidence_api = next(row for row in metadata["helpers"] if row["name"] == "make_evidence")
        self.assertIn("ranking_eligible", evidence_api["required_parameters"])
        self.assertEqual(evidence_api["output_fields"], list(("source", "window", "statistic",
            "value", "contributing_entities", "support", "details")))
        self.assertIn("contract_fields(inputs['data']", metadata["neutral_example"])

    def test_all_typed_constructors_round_trip_through_host_deserializer(self):
        window = deserialize_contract_json(self.window, "Window")
        entity = deserialize_contract_json(self.entity, "EntityRef")
        evidence = deserialize_contract_json(self.evidence, "Evidence")
        candidate = make_candidate(self.entity, "neutral typed-construction test")
        candidate_obj = deserialize_contract_json(candidate, "Candidate")
        candidates = make_candidate_set([candidate], "synthetic")
        candidates_obj = deserialize_contract_json(candidates, "CandidateSet")
        row = make_aggregate_row(self.entity, "source/table.csv|field", self.window,
            "summary", 1.5, 4, ["source/table.csv"], [self.entity],
            {"bucket_samples": [[0, "x"], [1, "y"]]})
        row_obj = deserialize_contract_json(row, "AggregateRow")
        aggregate = make_aggregate(self.window, [row], ["source/table.csv"],
            {"plain_details": {"items": [1, 2], "valid": True}})
        aggregate_obj = deserialize_contract_json(aggregate, "Aggregate")
        score = make_scored_candidate(self.entity, 0.5, None, [self.evidence], "bank_metric")
        score_obj = deserialize_contract_json(score, "ScoredCandidate")
        scores = make_scored_candidates([score], "synthetic_scorer", "ready",
            {"audit": {"flags": ["computed"]}})
        scores_obj = deserialize_contract_json(scores, "ScoredCandidates")

        self.assertEqual(window, Window("alert", 1_700_001_800_000, 1_700_003_600_000))
        self.assertEqual(entity, EntityRef("component", "Tomcat01"))
        self.assertEqual(candidate_obj, Candidate(entity, "neutral typed-construction test"))
        self.assertEqual(candidates_obj, CandidateSet((candidate_obj,), "synthetic"))
        self.assertEqual(row_obj.entity, entity)
        self.assertEqual(row_obj.window, window)
        self.assertEqual(row_obj.details["bucket_samples"], ((0, "x"), (1, "y")))
        self.assertEqual(aggregate_obj.rows[0], row_obj)
        self.assertEqual(aggregate_obj.metadata["plain_details"]["items"], (1, 2))
        self.assertEqual(score_obj.evidence[0], evidence)
        self.assertEqual(scores_obj.candidates, (score_obj,))
        for value, contract in ((self.window, "Window"), (self.entity, "EntityRef"),
                (self.evidence, "Evidence"), (candidate, "Candidate"),
                (candidates, "CandidateSet"), (row, "AggregateRow"),
                (aggregate, "Aggregate"), (score, "ScoredCandidate"),
                (scores, "ScoredCandidates")):
            self.assertEqual(value["$type"], contract)

    def test_accessors_preserve_typed_rows_and_distinguish_plain_query_rows(self):
        row = make_aggregate_row(self.entity, "source/table.csv|field", self.window,
            "summary", 1.5, 4, ["source/table.csv"], [self.entity],
            {"bucket_samples": [(0, "x"), (1, "y")]})
        aggregate = make_aggregate(self.window, [row], ["source/table.csv"])
        rows = aggregate_rows(aggregate)
        self.assertEqual(len(rows), 1)
        self.assertEqual(contract_fields(rows[0], "AggregateRow")["statistic"], "summary")
        with self.assertRaises(TypeError):
            rows[0]["$fields"]["statistic"] = "changed"
        with self.assertRaises(TypeError):
            contract_fields(rows[0], "AggregateRow")["details"]["bucket_samples"][0] = "changed"
        candidate = make_candidate(self.entity, "")
        candidates = make_candidate_set([candidate], "synthetic")
        items = candidate_items(candidates)
        self.assertEqual(entity_identifier(contract_fields(items[0], "Candidate")["entity"]), "Tomcat01")
        with self.assertRaises(TypeError):
            items[0]["$fields"]["rationale"] = "changed"
        self.assertEqual(entity_id(self.entity), "Tomcat01")

        canonical_aggregate = Aggregate(Window("alert", 1_700_001_800_000,
            1_700_003_600_000), (), ("bank/log/log_service.csv",),
            {"public_query": {"rows_data": [{"log_id": "l1", "value": "raw value",
                "timestamp_ms_utc": 1_700_001_800_000}], "coverage": {"truncated": True}}})
        canonical_wire = json.loads(canonical_json(canonical_aggregate))
        query_rows = native_query_rows(canonical_wire)
        self.assertEqual(len(aggregate_rows(canonical_wire)), 0)
        self.assertEqual(query_rows[0]["value"], "raw value")
        self.assertEqual(query_rows[0]["timestamp_ms_utc"], 1_700_001_800_000)
        self.assertIsInstance(query_rows[0], MappingProxyType)
        with self.assertRaises(TypeError):
            query_rows[0]["value"] = "changed"
        with self.assertRaisesRegex(ValueError, "AggregateRow"):
            contract_fields(query_rows[0], "AggregateRow")
        with self.assertRaisesRegex(ValueError, "public_query"):
            native_query_rows(aggregate)
        malformed_query = make_aggregate(self.window, [], ["bank/log/log_service.csv"],
            {"public_query": {"rows_data": [{"value": {"nested": "not a CSV scalar"}}]}})
        with self.assertRaisesRegex(ValueError, "flat finite CSV scalar"):
            native_query_rows(malformed_query)

    def test_canonical_bucket_details_survive_row_accessor_and_constructor(self):
        entity = EntityRef("component", "Tomcat01")
        window = Window("alert", 1_700_001_800_000, 1_700_003_600_000)
        row = AggregateRow(entity, "metric|field", window, "bucketed", 1.0, 4,
            ("bank/metric_container.csv",), (entity,),
            {"bucket_samples": tuple({"timestamp_ms_utc": index,
                "values": ("sample-a", "sample-b")} for index in range(40))})
        aggregate = Aggregate(window, (row,), ("bank/metric_container.csv",), {})
        wire = json.loads(canonical_json(aggregate))
        row_envelope = aggregate_rows(wire)[0]
        fields = contract_fields(row_envelope, "AggregateRow")
        self.assertEqual(len(fields["details"]["bucket_samples"]), 40)
        with self.assertRaises(TypeError):
            fields["details"]["bucket_samples"][0]["timestamp_ms_utc"] = 99
        rebuilt = make_aggregate(make_window("alert", 1_700_001_800_000,
            1_700_003_600_000), [row_envelope], ["bank/metric_container.csv"])
        host = deserialize_contract_json(rebuilt, "Aggregate")
        self.assertEqual(host.rows[0].details["bucket_samples"][0]["timestamp_ms_utc"], 0)

    def test_constructors_reject_plain_contract_mappings_and_invalid_eligibility(self):
        plain_window = {"name": "alert", "start_ms_utc": 1, "end_ms_utc": 2}
        plain_entity = {"role": "component", "identifier": "Tomcat01"}
        with self.assertRaisesRegex(ValueError, "Window \{\$type,\$fields\}"):
            make_aggregate(plain_window, [], [])
        with self.assertRaisesRegex(ValueError, "EntityRef \{\$type,\$fields\}"):
            make_candidate(plain_entity)
        with self.assertRaises(TypeError):
            make_evidence("source", self.window, "statistic", 1.0, [self.entity], 4,
                          details={})
        for support in (-1, 1.5, True):
            with self.subTest(support=support), self.assertRaisesRegex(ValueError, "support"):
                make_evidence("source", self.window, "statistic", 1.0,
                    [self.entity], support, True, {})
        with self.assertRaisesRegex(ValueError, "finite"):
            make_evidence("source", self.window, "statistic", float("nan"),
                [self.entity], 4, True, {})
        with self.assertRaisesRegex(ValueError, "positive support"):
            make_evidence("source", self.window, "statistic", 1.0,
                [self.entity], 0, True, {})
        with self.assertRaisesRegex(ValueError, "nonempty typed contributors"):
            make_evidence("source", self.window, "statistic", 1.0,
                [], 4, True, {})
        with self.assertRaisesRegex(ValueError, "contradicts"):
            make_evidence("source", self.window, "statistic", 1.0,
                [self.entity], 4, True, {"ranking_eligible": False})
        unsupported = make_evidence("unknown/source", self.window, "statistic", None,
            [], 0, False, {"ranking_eligible": False,
                           "coverage": {"complete": False, "truncated": True}})
        self.assertFalse(deserialize_contract_json(unsupported, "Evidence").details["ranking_eligible"])

    def test_malformed_nested_records_cycles_and_depth_are_rejected(self):
        malformed_entity = {"$type": "EntityRef", "$fields": {"role": "component"}}
        with self.assertRaisesRegex(ValueError, "EntityRef fields mismatch"):
            make_candidate(malformed_entity)
        cyclic = {}
        cyclic["self"] = cyclic
        with self.assertRaisesRegex(ValueError, "cyclic mapping"):
            make_aggregate_row(self.entity, "metric", self.window, "summary", None, 0,
                [], details=cyclic)
        deep = "x"
        for _ in range(45):
            deep = [deep]
        with self.assertRaisesRegex(ValueError, "nesting exceeds"):
            make_aggregate_row(self.entity, "metric", self.window, "summary", None, 0,
                [], details={"nested": deep})
        with self.assertRaisesRegex(ValueError, "duplicate entity"):
            candidate = make_candidate(self.entity)
            make_candidate_set([candidate, candidate], "synthetic")

    def test_isolated_generated_scorer_uses_typed_helpers_and_trusted_combiner(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _profile = _fixture_root(root)
            case = load_bank_case(case_id, "dev", root=root)
            runtime = build_m4_runtime(case.alert, root=root)
            candidates = make_candidate_set([
                make_candidate(make_entity_ref("component", "Tomcat01"), "from synthetic input")],
                "synthetic_candidates")
            source = '''def operator(inputs, params, runtime):
    candidate = candidate_items(inputs["candidates"])[0]
    candidate_fields = contract_fields(candidate, "Candidate")
    input_entity = contract_fields(candidate_fields["entity"], "EntityRef")
    entity = make_entity_ref(input_entity["role"], entity_identifier(candidate_fields["entity"]))
    alert = runtime.alert_window
    window = make_window(alert["name"], alert["start_ms_utc"], alert["end_ms_utc"])
    evidence = make_evidence("bank/metric_container.csv", window, "synthetic_contrast",
        params["value"], [entity], params["support"], True,
        {"ranking_eligible": True, "support_unit": "independent_time_blocks",
         "primitive_details": {"labels": ["left", "right"], "measured": True}})
    score = make_scored_candidate(entity, params["score"], None, [evidence], "bank_metric")
    return make_scored_candidates([score], "evolved.typed_helper_test", "ready",
        {"constructor": "typed"})
'''
            worker_runtime = {"alert_window": {"name": "alert",
                "start_ms_utc": case.telemetry.alert_window.start_ms_utc,
                "end_ms_utc": case.telemetry.alert_window.end_ms_utc}}
            params = {"value": 1.5, "score": 0.8, "support": 4}
            result = run_generated_operator(source,
                {"candidates": candidates}, params, worker_runtime, "ScoredCandidates")
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["output_validation"], "checked_typed_contract")
            scored = deserialize_contract_json(result["result"], "ScoredCandidates")
            self.assertEqual(scored.candidates[0].entity, EntityRef("component", "Tomcat01"))
            self.assertEqual(scored.candidates[0].evidence[0].window, case.telemetry.alert_window)
            self.assertEqual(scored.candidates[0].evidence[0].contributing_entities,
                             (EntityRef("component", "Tomcat01"),))
            self.assertEqual(scored.candidates[0].evidence[0].details["primitive_details"]["labels"],
                             ("left", "right"))
            registry = make_m4_registry(None, [], root=root)
            combiner = next(spec for spec in registry._operators.values()
                            if spec.name == "bank.answers.combine")
            context = NodeContext(case_id, "answer", combiner.qualified_name,
                runtime.snapshot.values, runtime.snapshot.provenance,
                case.telemetry.reference_window, case.telemetry.alert_window,
                case.telemetry.data_sha256)
            answer = combiner.implementation({"scores": (scored,)}, {"limit": 5}, context)
            validate_bank_answer(answer, case)
            self.assertEqual(answer.leads[0].entity.identifier, "Tomcat01")

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
                         "large nested Aggregate accessor stress is opt-in")
    def test_large_nested_aggregate_accessor_stays_within_m4_limits(self):
        window_obj = Window("alert", 1_700_001_800_000, 1_700_003_600_000)
        entity = EntityRef("component", "Tomcat01")
        sample = tuple("x" * 90 for _ in range(24))
        rows = tuple(AggregateRow(entity, f"metric|field-{index}", window_obj,
            "nested_sample", None, 24, ("bank/metric_container.csv",), (entity,),
            {"bucket_samples": sample, "group": {"members": tuple(range(4))}})
            for index in range(5000))
        aggregate = Aggregate(window_obj, rows, ("bank/metric_container.csv",), {})
        typed_input = _encode(aggregate)
        request_bytes = len(json.dumps(typed_input, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8"))
        self.assertGreater(request_bytes, 12 * 1024 * 1024)
        self.assertLess(request_bytes, 16 * 1024 * 1024)
        source = '''def operator(inputs, params, runtime):
    rows = aggregate_rows(inputs["data"])
    first = contract_fields(rows[0], "AggregateRow")
    entity_fields = contract_fields(first["entity"], "EntityRef")
    entity = make_entity_ref(entity_fields["role"], entity_identifier(first["entity"]))
    window = first["window"]
    evidence = make_evidence("bank/metric_container.csv", window, "bounded_read_check",
        1.0, [entity], first["support"], False, {"ranking_eligible": False})
    score = make_scored_candidate(entity, 0.0, None, [evidence], "stress_check")
    return make_scored_candidates([score], "evolved.aggregate_read_stress", "abstained", {})
'''
        result = run_generated_operator(source, {"data": typed_input}, {}, {},
            "ScoredCandidates", limits={"input_bytes": 16 * 1024 * 1024,
                "cpu_seconds": 10, "wall_seconds": 15.0,
                "memory_bytes": 512 * 1024 * 1024})
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["output_validation"], "checked_typed_contract")
        self.assertEqual(deserialize_contract_json(result["result"], "ScoredCandidates").status,
                         "abstained")


if __name__ == "__main__":
    unittest.main()
