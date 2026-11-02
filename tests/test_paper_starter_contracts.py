import tempfile
import unittest
import hashlib
from pathlib import Path

from e4core.cache import CacheStore, _encode, canonical_json, deserialize_contract_json
from e4core.contracts import (EntityRef, MetricPoint, PaperCandidate, PaperCandidateSet,
                              PaperLogPatternSummary, PaperLogSummary,
                              PaperSessionObservation, PaperStarterInput, RankedAnswer,
                              ScoredCandidate, ScoredCandidates, Telemetry, Window)
from e4core.executor import OperatorResult, SerialExecutor
from e4core.registry import OperatorRegistry, OperatorSpec, PortSpec
from e4core.runtime import (ParameterSpec, RuntimeContext, RuntimeField, RuntimeSchema,
                            resolve_parameters)


def starter_input(alert_value=20.0, *, data_hash="a" * 64):
    reference = Window("reference", 0, 100)
    alert = Window("alert", 100, 200)
    entity = EntityRef("component", "component-1")
    metrics = (
        MetricPoint("metric/container.csv", 10, entity, "load", None, None,
                    2.0, "2", "ref-1"),
        MetricPoint("metric/container.csv", 110, entity, "load", None, None,
                    alert_value, str(alert_value), "alert-1"),
    )
    telemetry = Telemetry("synthetic-paper", reference, alert, metrics, (), {},
                          data_hash, {}, {}, {}, "synthetic/1")
    sessions = (
        PaperSessionObservation("s-ref", 20, {"region": "east"}, kpi_value=3.0),
        PaperSessionObservation("s-alert", 120, {"region": "west"}, kpi_value=alert_value),
    )
    tokens = ("connection", "timeout")
    pattern = PaperLogPatternSummary("component-1", "service", alert, 100,
        60_000, tokens, hashlib.sha256("\0".join(tokens).encode()).hexdigest(),
        1, 1, 1, ("c" * 64,))
    summary = PaperLogSummary("component-1", "service", alert, 1, 1, ("c" * 64,))
    return PaperStarterInput(telemetry, sessions, None,
        {"sessions": "available", "urls": "unsupported"}, 60_000, (summary,), (pattern,))


class PaperStarterContractsTests(unittest.TestCase):
    def test_starter_input_cache_roundtrip_and_strict_nested_types(self):
        value = starter_input()
        wire = _encode(value)
        self.assertEqual(deserialize_contract_json(wire, "PaperStarterInput"), value)
        self.assertIn(b'"$type":"PaperStarterInput"', canonical_json(value))
        with tempfile.TemporaryDirectory() as directory:
            cache = CacheStore(Path(directory))
            cache.put("paper-input-key", "PaperStarterInput", value)
            restored, digest, status = cache.get("paper-input-key", "PaperStarterInput")
            self.assertEqual(status, "hit")
            self.assertTrue(digest)
            self.assertEqual(restored, value)

        bad_timestamp = _encode(value)
        bad_timestamp["$fields"]["sessions"]["$tuple"][0]["$fields"]["timestamp_ms_utc"] = True
        with self.assertRaises((TypeError, ValueError)):
            deserialize_contract_json(bad_timestamp, "PaperStarterInput")
        bad_capability = _encode(value)
        bad_capability["$fields"]["capabilities"]["$map"]["sessions"] = 1
        with self.assertRaises((TypeError, ValueError)):
            deserialize_contract_json(bad_capability, "PaperStarterInput")

    def test_default_auto_fallback_estimator_receives_reference_only_starter_view(self):
        schema = RuntimeSchema({
            "optional_estimate": RuntimeField("optional_estimate", "number",
                "optional estimator override", "operator"),
        }, "paper-test/1")
        runtime = RuntimeContext.build(schema, {})
        parameter = ParameterSpec("estimate", "number", runtime_field="optional_estimate",
            default="auto", has_default=True, estimator="reference_load_mean")
        estimates = []

        def estimator(reference_view):
            # Reference view contains only the reference side of all supported
            # PaperStarterInput sources, even if alert telemetry is changed.
            result = (sum(point.value for point in reference_view["metrics"] if point.value is not None) /
                      sum(point.value is not None for point in reference_view["metrics"]))
            estimates.append((result, tuple(row.session_id for row in reference_view["sessions"])))
            return result

        fallback_parameter = ParameterSpec("estimate", "number", runtime_field="optional_estimate",
            default="auto", has_default=True, estimator="reference_load_mean")
        fallback = resolve_parameters({"estimate": fallback_parameter}, {}, {}, runtime,
            {"reference_load_mean": lambda reference: estimator(reference)},
            reference_data={"metrics": (starter_input().telemetry.metrics[0],),
                            "sessions": (starter_input().sessions[0],)})
        self.assertEqual(fallback.values["estimate"], 2.0)
        self.assertEqual(fallback.provenance["estimate"],
            "static_default_after_missing_runtime -> auto:reference_load_mean:reference_window")
        estimates.clear()

        registry = OperatorRegistry()
        registry.register(OperatorSpec("paper.test.input", "1", (), "PaperStarterInput",
            external_input=True))
        def score(inputs, params, context):
            candidate = ScoredCandidate(EntityRef("component", "c1"), float(params["estimate"]),
                                         None, (), "reference-estimator")
            return OperatorResult(ScoredCandidates((candidate,), "reference-estimator"))
        registry.register(OperatorSpec("paper.test.estimate", "1",
            (PortSpec("data", "PaperStarterInput"),), "ScoredCandidates",
            {"estimate": parameter}, implementation=score))
        registry.register(OperatorSpec("paper.test.rank", "1",
            (PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),),
            "RankedAnswer", implementation=lambda inputs, params, context:
                OperatorResult(RankedAnswer((ScoredCandidate(EntityRef("component", "c1"),
                    float(inputs["scores"][0].candidates[0].score), None, (), None),)))))
        registry.register_estimator("reference_load_mean", estimator)
        document = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "source", "operator": "paper.test.input", "version": "1",
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "estimate", "operator": "paper.test.estimate", "version": "1",
             "inputs": {"data": "source"}, "params": {}, "bindings": {}},
            {"id": "rank", "operator": "paper.test.rank", "version": "1",
             "inputs": {"scores": ["estimate"]}, "params": {}, "bindings": {}},
        ], "return": {"node": "rank"}}
        first = starter_input(20.0, data_hash="a" * 64)
        second = starter_input(2000.0, data_hash="b" * 64)
        executor = SerialExecutor(registry)
        first_result = executor.execute_document(document, runtime, lambda: {"source": first},
                                                  cache_enabled=False)
        second_result = executor.execute_document(document, runtime, lambda: {"source": second},
                                                   cache_enabled=False)
        self.assertEqual(estimates, [(2.0, ("s-ref",)), (2.0, ("s-ref",))])
        self.assertEqual(first_result.answer, second_result.answer)
        record = next(row for row in first_result.records if row.node_id == "estimate")
        self.assertEqual(record.resolved_params["estimate"], 2.0)
        self.assertEqual(record.parameter_provenance["estimate"],
            "static_default_after_missing_runtime -> auto:reference_load_mean:reference_window")

    def test_unknown_consensus_family_is_ignored_and_rescaled_alias_is_one_vote(self):
        from e4core.paper_starter_math import VoteRecord, combine_votes

        votes = [
            VoteRecord("metric", "trusted-computation-1", (("component-a", 4), ("component-b", 1))),
            # Same trusted computation and relative scores through another alias.
            VoteRecord("metric", "trusted-computation-1", (("component-a", 40), ("component-b", 10))),
            VoteRecord("tree", "trusted-computation-2", (("component-a", 1), ("component-b", 3))),
            VoteRecord("unconfigured-family", "untrusted-family-vote",
                       (("component-a", 0), ("component-b", 1000))),
        ]
        result = combine_votes(votes, family_weights={"metric": 1.0, "tree": 1.0},
                               tie_tolerance=0.0)
        expected = combine_votes(votes[::2][:1] + [votes[2]],
            family_weights={"metric": 1.0, "tree": 1.0}, tie_tolerance=0.0)
        self.assertEqual(result.scores, expected.scores)
        self.assertEqual(result.support, 2)
        self.assertAlmostEqual(dict(result.scores)["component-a"], 2.0 / 3.0)
        self.assertEqual(dict(result.scores)["component-b"], 0.625)
        unknown_only = combine_votes([votes[-1]], family_weights={"metric": 1.0}, tie_tolerance=0.0)
        self.assertEqual(unknown_only.status, "abstain")
        self.assertEqual(unknown_only.reason, "no_usable_independent_votes")

    def test_paper_candidate_contract_cache_roundtrip_and_strict_type_rejection(self):
        value = PaperCandidateSet((PaperCandidate("cohort", "region=east",
            attributes={"region": "east"}), PaperCandidate("event_pattern", "timeout",
            tokens=("request", "timeout"))), "synthetic")
        self.assertEqual(deserialize_contract_json(_encode(value), "PaperCandidateSet"), value)
        with tempfile.TemporaryDirectory() as directory:
            cache = CacheStore(Path(directory))
            cache.put("paper-candidates-key", "PaperCandidateSet", value)
            restored, _digest, status = cache.get("paper-candidates-key", "PaperCandidateSet")
            self.assertEqual(status, "hit")
            self.assertEqual(restored, value)
        bad_token = _encode(value)
        bad_token["$fields"]["candidates"]["$tuple"][1]["$fields"]["tokens"]["$tuple"][0] = 5
        with self.assertRaises((TypeError, ValueError)):
            deserialize_contract_json(bad_token, "PaperCandidateSet")


if __name__ == "__main__":
    unittest.main()
