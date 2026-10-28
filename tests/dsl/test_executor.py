import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

from e4.dsl.cache import CacheStore, _encode, deserialize_contract_json
from e4.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
                          EntityRef, Evidence, ExecutionResult, MetricPoint, NodeRecord,
                          RankedAnswer, ScoredCandidate, ScoredCandidates, Span,
                          Telemetry, Window)
from e4.dsl.verifier import DAGValidationError, verify_dag
from e4.dsl.executor import ExecutionFailure, OperatorResult, SerialExecutor
from tests.support.fixtures import fixture_dag, fixture_registry
from e4.dsl.registry import OperatorSpec, PortSpec
from e4.dsl.runtime import build_case_runtime


CASE_META = {"incident_id": "synthetic-1", "analysis_window_start_epoch_ms_utc": 0,
             "alert_window_start_epoch_ms_utc": 30_000,
             "alert_window_end_epoch_ms_utc": 60_000,
             "analysis_window_start_local": "1970-01-01T08:00:00+08:00",
             "alert_window_start_local": "1970-01-01T08:00:30+08:00",
             "alert_window_end_local": "1970-01-01T08:01:00+08:00"}


def synthetic_telemetry(data_hash=None, offset=0):
    ref = Window("reference", 0 + offset, 30_000 + offset)
    alert = Window("alert", 30_000 + offset, 60_000 + offset)
    rows = [
        MetricPoint("metric/metric_app.csv", 31_000 + offset, EntityRef("service", "osb_001"), "avg_time", "metric_app", "avg_time", 2.0, "2", "a1"),
        MetricPoint("metric/metric_app.csv", 32_000 + offset, EntityRef("service", "osb_001"), "avg_time", "metric_app", "avg_time", 4.0, "4", "a2"),
        MetricPoint("metric/metric_node.csv", 31_000 + offset, EntityRef("component", "os_017"), "cpu", "b1", "i1", 6.0, "6", "n1"),
    ]
    return Telemetry("synthetic-1", ref, alert, tuple(rows), (),
                     MappingProxyType({}), data_hash or ("d" * 64 if offset == 0 else "f" * 64), MappingProxyType({}),
                     MappingProxyType({}), MappingProxyType({}), "test-adapter/1")


def runtime(overrides=None, offset=0):
    case = dict(CASE_META)
    for key in ("analysis_window_start_epoch_ms_utc", "alert_window_start_epoch_ms_utc", "alert_window_end_epoch_ms_utc"):
        case[key] += offset
    return build_case_runtime(case, overrides=overrides or {})


class SerialExecutorTests(unittest.TestCase):
    def test_worker_core_json_must_decode_through_full_typed_contract(self):
        entity = EntityRef("component", "os_017")
        value = CandidateSet((Candidate(entity, "public catalog"),), "fixture")
        wire = _encode(value)
        self.assertEqual(deserialize_contract_json(wire, "CandidateSet"), value)
        malformed = dict(wire)
        malformed["$fields"] = dict(wire["$fields"], injected=True)
        with self.assertRaisesRegex(ValueError, "malformed"):
            deserialize_contract_json(malformed, "CandidateSet")
        with self.assertRaisesRegex(ValueError, "malformed"):
            deserialize_contract_json(_encode(Window("alert", 10, 20)), "CandidateSet")

    def test_typed_decoder_roundtrips_nested_values_and_rejects_annotation_mismatches(self):
        entity = EntityRef("component", "os_017")
        ref, alert = Window("reference", 0, 30), Window("alert", 30, 60)
        metric = MetricPoint("metric/node.csv", 35, entity, "CPU", "bomc", "item", 2.0, "2", "r1")
        span = Span("t1", "s1", None, 35, "3", True, "valid", "TRUE", "OSB",
                    entity, None, None, False, False)
        aggregate = Aggregate(alert, (AggregateRow(entity, "metric/node.csv|CPU|b|i", alert,
            "median", 2.0, 1, ("metric/node.csv",)),), ("metric/node.csv",))
        telemetry = Telemetry("fixture", ref, alert, (metric,), (span,), {"os_017": {}},
                              "a" * 64, {}, {}, {}, "adapter/1")
        evidence = Evidence("trace", alert, "delta", 2.0, (entity,), 1)
        scores = ScoredCandidates((ScoredCandidate(entity, 1.0, None, (evidence,), "metric"),), "test")
        node = NodeRecord("n1", "metrics.aggregate", "1", "success",
                          {"data": ("abc", "def"), "$external": "hash"}, {}, {}, {},
                          "2026-01-01T00:00:00Z", 1.5, 3, 4, "key", "miss", "digest", "impl")
        execution = ExecutionResult(RankedAnswer(()), (node,), 0, "fixture", "impl")
        for typed in (metric, span, aggregate, telemetry, scores, execution):
            with self.subTest(contract=type(typed).__name__):
                self.assertEqual(deserialize_contract_json(_encode(typed), type(typed).__name__), typed)

        bad_candidate = _encode(CandidateSet((Candidate(entity, "catalog"),), "fixture"))
        bad_candidate["$fields"]["source"] = 5
        with self.assertRaisesRegex(TypeError, "str"):
            deserialize_contract_json(bad_candidate, "CandidateSet")
        bad_scores = _encode(scores)
        bad_scores["$fields"]["candidates"]["$tuple"][0]["$fields"]["score"] = True
        with self.assertRaisesRegex(TypeError, "number"):
            deserialize_contract_json(bad_scores, "ScoredCandidates")
        bad_evidence = _encode(scores)
        bad_evidence["$fields"]["candidates"]["$tuple"][0]["$fields"]["evidence"]["$tuple"] = ["junk"]
        with self.assertRaisesRegex(TypeError, "Evidence"):
            deserialize_contract_json(bad_evidence, "ScoredCandidates")
        bad_telemetry = _encode(telemetry)
        bad_telemetry["$fields"]["file_sha256"]["$map"]["trace"] = 7
        with self.assertRaisesRegex(TypeError, "str"):
            deserialize_contract_json(bad_telemetry, "Telemetry")
        from e4.contracts import RuntimeSnapshot
        bad_runtime = _encode(RuntimeSnapshot({}, {"seed": "default"}, "runtime/1"))
        bad_runtime["$fields"]["provenance"]["$map"]["seed"] = False
        with self.assertRaisesRegex(TypeError, "str"):
            deserialize_contract_json(bad_runtime, "RuntimeSnapshot")

    def test_hand_calculated_branching_dag_cache_replay_and_runtime_invalidation(self):
        registry = fixture_registry()
        with tempfile.TemporaryDirectory() as temporary:
            cache = CacheStore(Path(temporary) / "cache", enabled=True)
            executor = SerialExecutor(registry, cache)
            doc = fixture_dag()
            data = synthetic_telemetry()
            load_count = []
            loader = lambda: (load_count.append("loaded") or {"data": data})
            first = executor.execute_document(doc, runtime(), loader)
            second = executor.execute_document(doc, runtime(), loader)
            uncached = executor.execute_document(doc, runtime(), loader, cache_enabled=False)
            self.assertEqual(load_count, ["loaded", "loaded", "loaded"])
            self.assertEqual(first.answer, second.answer)
            self.assertEqual(first.answer, uncached.answer)
            self.assertEqual([(x.entity.identifier, x.score) for x in first.answer.leads],
                             [("os_017", 6.0), ("osb_001", 1.5)])
            self.assertEqual(len([record for record in first.records if record.node_id == "data"]), 1)
            self.assertGreaterEqual(second.cache_hits, 8)
            self.assertEqual(first.records[-1].runtime_snapshot["measured_graph_depth"], 6)
            self.assertNotEqual(first.records[-1].runtime_snapshot["values"]["dag_depth"],
                                first.records[-1].runtime_snapshot["measured_graph_depth"])
            changed_runtime = executor.execute_document(doc, runtime({"seed": 911}), loader)
            self.assertEqual(changed_runtime.cache_hits, 0)
            changed_bound_runtime = executor.execute_document(doc, runtime({"lead_count": 4}), loader)
            self.assertEqual(changed_bound_runtime.cache_hits, 0)
            changed_window_runtime = executor.execute_document(doc, runtime(offset=-60_000),
                lambda: {"data": synthetic_telemetry(offset=-60_000)})
            self.assertEqual(changed_window_runtime.cache_hits, 0)
            changed_data = executor.execute_document(doc, runtime(),
                lambda: {"data": synthetic_telemetry("e" * 64)})
            self.assertEqual(changed_data.cache_hits, 0)
            changed_doc = fixture_dag()
            changed_doc["nodes"][7]["params"]["factor"] = 0.75
            changed_params = executor.execute_document(changed_doc, runtime(), loader)
            statuses = {record.node_id: record.cache_status for record in changed_params.records}
            self.assertEqual(statuses["metric_scores"], "hit")
            self.assertNotEqual(statuses["adjusted_metric_scores"], "hit")
            score_v1 = registry.require("fixture.score", "1")
            registry.register(replace(score_v1, version="2"))
            versioned_doc = fixture_dag()
            for node in versioned_doc["nodes"]:
                if node["operator"] == "fixture.score":
                    node["version"] = "2"
            versioned = executor.execute_document(versioned_doc, runtime(), loader)
            self.assertEqual(versioned.answer, first.answer)
            version_status = {record.node_id: record.cache_status for record in versioned.records}
            self.assertEqual(version_status["metrics"], "hit")
            self.assertNotEqual(version_status["metric_scores"], "hit")

    def test_verification_happens_before_large_input_loader(self):
        executor = SerialExecutor(fixture_registry(), CacheStore(Path("/tmp/e4-verifier-cache"), enabled=False))
        bad = fixture_dag()
        bad["return"]["node"] = "metrics"
        calls = []
        with self.assertRaises(DAGValidationError):
            executor.execute_document(bad, runtime(), lambda: calls.append("loaded") or {"data": synthetic_telemetry()})
        self.assertEqual(calls, [])

    def test_failed_nodes_are_structured_and_never_cached(self):
        registry = fixture_registry()
        counter = {"calls": 0}
        def fail(inputs, params, context):
            counter["calls"] += 1
            raise RuntimeError("intentional fixture failure")
        registry.register(OperatorSpec("fixture.fail", "1", (PortSpec("data", "Telemetry"),),
                                       "Aggregate", implementation=fail))
        dag = fixture_dag()
        dag["nodes"][1] = {"id": "metrics", "operator": "fixture.fail", "version": "1",
                           "inputs": {"data": "data"}, "params": {}, "bindings": {}}
        with tempfile.TemporaryDirectory() as temporary:
            executor = SerialExecutor(registry, CacheStore(Path(temporary) / "cache", enabled=True))
            for _ in range(2):
                with self.assertRaises(ExecutionFailure) as error:
                    executor.execute_document(dag, runtime(), lambda: {"data": synthetic_telemetry()})
                self.assertEqual(error.exception.records[-1].status, "failed")
                self.assertEqual(error.exception.records[-1].cache_status, "failure_not_cached")
                self.assertEqual(error.exception.as_dict()["code"], "operator_exception")
            self.assertEqual(counter["calls"], 2)
            self.assertEqual(len(list((Path(temporary) / "cache").glob("*.json"))), 0)

    def test_external_data_content_and_cache_disabled_output_digests(self):
        registry = fixture_registry()
        window = Window("alert", 30_000, 60_000)
        dag = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "input", "operator": "fixture.aggregate_input", "version": "1",
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "fixture.candidates", "version": "1",
             "inputs": {"aggregate": "input"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": "fixture.score", "version": "1",
             "inputs": {"aggregate": "input", "candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "fixture.rank", "version": "1",
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {}}],
            "return": {"node": "rank"}}
        def aggregate(value):
            row = AggregateRow(EntityRef("service", "osb_001"), "avg_time", window, "mean", value, 3,
                               ("metric_app",), (), {"input_value": value})
            return Aggregate(window, (row,), ("metric_app",))
        with tempfile.TemporaryDirectory() as temporary:
            cache = CacheStore(Path(temporary) / "cache", enabled=True)
            executor = SerialExecutor(registry, cache)
            invoke = lambda value: executor.execute_document(dag, runtime(), lambda: {"input": aggregate(value)})
            first = invoke(2.0)
            replay = invoke(2.0)
            changed = invoke(5.0)
            self.assertEqual(first.answer.leads[0].score, 2.0)
            self.assertEqual(replay.answer, first.answer)
            self.assertGreater(replay.cache_hits, 0)
            self.assertEqual(changed.answer.leads[0].score, 5.0)
            self.assertEqual(changed.cache_hits, 0)
            no_cache = executor.execute_document(fixture_dag(), runtime(),
                lambda: {"data": synthetic_telemetry()}, cache_enabled=False)
            self.assertTrue(all(len(record.output_digest) == 64 for record in no_cache.records))


if __name__ == "__main__":
    unittest.main()
