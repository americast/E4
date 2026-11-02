import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from e4core.cache import CacheStore
from e4core.contracts import (Aggregate, EntityRef, Evidence, ExecutionResult,
    NodeRecord, RankedAnswer, ScoredCandidate, ScoredCandidates, Window)
from e4core.dag import VerifiedDAG, VerifiedNode
from e4core.executor import ExecutionFailure, NodeContext
from e4core.m4_controller import build_m4_runtime
from e4core.m4_data import make_m4_registry
from e4core.m4_evolution_diagnostics import build_evolution_contribution_diagnostics
from e4core.m4_generated import validate_generated_spec
from e4core.registry import OperatorSpec, PortSpec

ROOT = Path(__file__).resolve().parents[1]


def _runtime():
    return build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
        "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
        "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=ROOT)


def _record(node_id, spec, runtime, *, cache_key="", output_digest=None,
            status="success", error=None, elapsed=1.0):
    return NodeRecord(node_id=node_id, operator=spec.name, version=spec.version,
        status=status, input_digests={}, resolved_params={"limit": 5} if spec.name == "bank.answers.combine" else {},
        parameter_provenance={}, runtime_snapshot={"values": dict(runtime.snapshot.values),
            "provenance": dict(runtime.snapshot.provenance)}, started_at="test",
        elapsed_ms=elapsed, rows_processed=0, bytes_processed=0, cache_key=cache_key,
        cache_status="miss_stored" if cache_key else "not_cacheable", output_digest=output_digest,
        implementation_fingerprint="f" * 64, error=error)


def _evidence(entity, *, eligible=True, support_unit="independent_time_blocks", statistic="contrast"):
    return Evidence("bank/metric_container.csv", Window("alert", 1_700_001_800_000,
        1_700_003_600_000), statistic, 1.0, (entity,), 4,
        {"ranking_eligible": eligible, "support_unit": support_unit, "unit": "native value"})


class EvolutionContributionDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.runtime = _runtime()
        self.registry = make_m4_registry(None, [], root=ROOT)
        self.combine = next(spec for spec in self.registry._operators.values()
                           if spec.name == "bank.answers.combine")
        self.base_scorer = self.registry.require("bank.metrics.contrast",
            next(row["version"] for row in self.registry.signatures()
                 if row["name"] == "bank.metrics.contrast"))
        self.generated = OperatorSpec("evolved.test_diagnostic", "1",
            (PortSpec("data", "Telemetry"),), "ScoredCandidates", family="test_family")
        self.registry.register(self.generated)
        self.case_id = "synthetic-diagnostic-case"
        self.cache_dir = None

    def _construct(self, generated_value, base_value, *, second_generated_value=None,
                   cache_generated=True,
                   cache_base=True, generated_status="success", mismatch_generated_digest=False, gap=None,
                   audit_rows=None):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        run_dir = Path(temp.name)
        cache = CacheStore(run_dir / "cache", enabled=True)
        self.cache_dir = cache
        data_spec = self.registry.require("case.telemetry", "1")
        gen_node = VerifiedNode("new_score", self.generated, {"data": ("data",)}, {}, {}, 2)
        nodes = [VerifiedNode("data", data_spec, {}, {}, {}, 1), gen_node]
        score_refs = ["new_score"]
        if second_generated_value is not None:
            nodes.append(VerifiedNode("new_score_2", self.generated, {"data": ("data",)}, {}, {}, 2))
            score_refs.append("new_score_2")
        base_node = VerifiedNode("base_score", self.base_scorer, {}, {}, {}, 1)
        combine_node = VerifiedNode("answer", self.combine,
            {"scores": tuple([*score_refs, "base_score"])}, {"limit": 5}, {}, 3)
        nodes.extend((base_node, combine_node))
        verified = VerifiedDAG(tuple(nodes), "answer", 3, len(nodes))
        node_records = []
        outputs = [("new_score", self.generated, generated_value, cache_generated, generated_status),
                   ("base_score", self.base_scorer, base_value, cache_base, "success")]
        if second_generated_value is not None:
            outputs.insert(1, ("new_score_2", self.generated, second_generated_value,
                               cache_generated, "success"))
        for node_id, spec, output, cache_it, state in outputs:
            cache_key, digest = "", None
            if cache_it:
                cache_key = f"{node_id}-cache-key"
                digest = cache.put(cache_key, spec.output_type, output)
                if node_id == "new_score" and mismatch_generated_digest:
                    digest = "d" * 64
            else:
                cache_key, digest = f"{node_id}-missing-cache-key", "d" * 64
            node_records.append(_record(node_id, spec, self.runtime,
                cache_key=cache_key, output_digest=digest, status=state,
                error={"code": "synthetic_error"} if state == "failed" else None))
        context = NodeContext(self.case_id, "answer", self.combine.qualified_name,
            self.runtime.snapshot.values, self.runtime.snapshot.provenance,
            self.runtime.snapshot.values["reference_window"],
            self.runtime.snapshot.values["alert_window"], "data-hash")
        score_values = [generated_value]
        if second_generated_value is not None:
            score_values.append(second_generated_value)
        answer = self.combine.implementation({"scores": tuple([*score_values, base_value])},
            {"limit": 5}, context)
        node_records.append(_record("answer", self.combine, self.runtime))
        execution = ExecutionResult(answer, tuple(node_records), 0, self.case_id, "f" * 64)
        (run_dir / "audit").mkdir()
        if audit_rows:
            (run_dir / "audit/public_query_calls.jsonl").write_text("".join(
                json.dumps(row) + "\n" for row in audit_rows))
        specs = [{"name": self.generated.name, "version": self.generated.version,
                  "output_type": self.generated.output_type}]
        if gap is None:
            gap = {"schema_version": "e4-capability-gaps/1", "capabilities": [{
                "operator_name": self.generated.name, "operator_version": self.generated.version,
                "gap_hypothesis": "synthetic gap", "closest_existing_operators": [],
                "computational_difference": "synthetic difference", "intended_contribution": "test",
                "required_inputs": ["Telemetry"], "coverage_requirement": "bounded",
                "validation_expectation": "typed output"}]}
        result = build_evolution_contribution_diagnostics(gap_record=gap,
            new_specs=specs, dag_document=None, verified=verified, execution=execution,
            failure=None, cache=cache, runtime=self.runtime, registry=self.registry,
            run_dir=run_dir, case_id=self.case_id)
        return result, answer, cache, run_dir, verified, node_records

    def _scored(self, entity, score, family, evidence=None, *, status="ready"):
        evidence = [_evidence(entity)] if evidence is None else evidence
        return ScoredCandidates((ScoredCandidate(entity, score, None, tuple(evidence), family),),
            scorer=family, status=status)

    def test_positive_generated_score_has_trusted_combiner_counterfactual(self):
        a, b = EntityRef("component", "Apache01"), EntityRef("component", "Tomcat01")
        generated = self._scored(a, 0.9, "generated_family")
        baseline = self._scored(b, 0.8, "bank_metric")
        result, _answer, *_ = self._construct(generated, baseline)
        self.assertEqual(result["counterfactual"]["status"], "evaluated")
        joint = result["counterfactual"]["joint_direct_generated_removal"]
        self.assertEqual(joint["status"], "evaluated")
        self.assertEqual(joint["effect"]["top3_removed_count"], 1)
        self.assertTrue(joint["effect"]["answer_changed"])
        operator = result["operators"][0]
        self.assertEqual(operator["capability_gap"]["gap_hypothesis"], "synthetic gap")
        self.assertEqual(operator["nodes"][0]["output"]["positive_eligible_component_count"], 1)
        overlap = result["final_answer_eligible_evidence_overlap"]["per_operator"][0]
        self.assertEqual(overlap["eligible_component_overlap_top3_count"], 1)
        self.assertEqual(overlap["comparison_status"], "observed_evidence_overlap_not_causal_contribution")

    def test_zero_or_redundant_branch_reports_no_top_rank_effect(self):
        a = EntityRef("component", "Tomcat01")
        generated = ScoredCandidates((), scorer="generated_family", status="abstained")
        baseline = self._scored(a, 0.8, "bank_metric")
        result, _answer, *_ = self._construct(generated, baseline)
        effect = result["counterfactual"]["joint_direct_generated_removal"]["effect"]
        self.assertFalse(effect["answer_changed"])
        self.assertFalse(effect["top3_order_changed"])
        self.assertEqual(effect["top3_added_count"], 0)
        self.assertEqual(effect["top3_removed_count"], 0)

    def test_overlapping_direct_generated_inputs_have_node_and_joint_ablations(self):
        a, b = EntityRef("component", "Apache01"), EntityRef("component", "Tomcat01")
        generated_a = self._scored(a, 0.9, "generated_family")
        generated_a2 = self._scored(a, 0.7, "generated_family")
        baseline = self._scored(b, 0.8, "bank_metric")
        result, *_ = self._construct(generated_a, baseline,
            second_generated_value=generated_a2)
        counterfactual = result["counterfactual"]
        self.assertEqual(len(counterfactual["individual_node_removals"]), 2)
        self.assertEqual(len(counterfactual["individual_operator_removals"]), 1)
        self.assertEqual(counterfactual["overlapping_direct_inputs"][0][
            "positive_eligible_component_overlap_count"], 1)
        self.assertEqual(counterfactual["joint_direct_generated_removal"]["status"], "evaluated")

    def test_candidate_summary_cap_is_explicit_and_component_ids_are_not_dumped(self):
        candidates = []
        for index, identifier in enumerate(("Apache01", "Tomcat01", "Mysql01")):
            entity = EntityRef("component", identifier)
            candidates.append(ScoredCandidate(entity, 0.9 - index * 0.1, None,
                (_evidence(entity),), "generated_family"))
        generated = ScoredCandidates(tuple(candidates), scorer="generated_family")
        baseline = self._scored(EntityRef("component", "Redis01"), 0.5, "bank_metric")
        with patch("e4core.m4_evolution_diagnostics.MAX_OUTPUT_CANDIDATES", 1):
            result, *_ = self._construct(generated, baseline)
        summary = result["operators"][0]["nodes"][0]["output"]
        self.assertEqual(summary["candidate_count"], 3)
        self.assertEqual(summary["candidate_count_summarized"], 1)
        self.assertTrue(summary["candidate_counts_truncated"])
        serialized = json.dumps(result)
        for component in ("Apache01", "Tomcat01", "Mysql01", "Redis01"):
            self.assertNotIn(component, serialized)

    def test_cache_miss_and_failed_generated_node_are_explicitly_unavailable(self):
        a = EntityRef("component", "Tomcat01")
        generated = self._scored(a, 0.8, "generated_family")
        baseline = self._scored(a, 0.7, "bank_metric")
        result, *_ = self._construct(generated, baseline, cache_generated=False)
        self.assertEqual(result["operators"][0]["nodes"][0]["cache_output_status"], "cache_miss")
        self.assertEqual(result["counterfactual"]["status"], "counterfactual_not_evaluated")
        self.assertEqual(result["counterfactual"]["reason"], "combiner_score_input_cache_unavailable")

        mismatched, *_ = self._construct(generated, baseline, mismatch_generated_digest=True)
        self.assertEqual(mismatched["operators"][0]["nodes"][0]["cache_output_status"],
                         "cache_record_digest_mismatch")
        self.assertEqual(mismatched["counterfactual"]["status"], "counterfactual_not_evaluated")

        failed_result, *_ = self._construct(generated, baseline, cache_generated=True,
            generated_status="failed")
        self.assertEqual(failed_result["operators"][0]["nodes"][0]["execution_status"], "failed")
        self.assertEqual(failed_result["operators"][0]["nodes"][0]["output"], None)

        _result, _answer, cache, run_dir, verified, records = self._construct(
            generated, baseline, generated_status="failed")
        failure = ExecutionFailure("synthetic generated failure", (records[0],),
            {"code": "operator_exception", "node_id": "new_score"})
        failure_result = build_evolution_contribution_diagnostics(gap_record=None,
            new_specs=[{"name": self.generated.name, "version": self.generated.version}],
            dag_document=None, verified=verified, execution=None, failure=failure,
            cache=cache, runtime=self.runtime, registry=self.registry, run_dir=run_dir,
            case_id=self.case_id)
        self.assertEqual(failure_result["execution_failure"]["code"], "operator_exception")
        self.assertEqual(failure_result["operators"][0]["nodes"][0]["execution_status"], "failed")

    def test_intermediate_aggregate_and_truncated_query_coverage_are_summarized_only(self):
        aggregate_spec = OperatorSpec("evolved.test_aggregate", "1",
            (PortSpec("data", "Telemetry"),), "Aggregate", family="test_family")
        self.registry.register(aggregate_spec)
        window = self.runtime.snapshot.values["alert_window"]
        agg = Aggregate(window, (), ("bank/log/log_service.csv",), {"public_query": {
            "rows_data": [{"value": "private-ish raw row"}],
            "coverage": {"total_matching_rows": 10, "returned_rows": 2,
                "truncated": True, "page_complete": False, "complete": False,
                "next_offset": 2}}})
        a = EntityRef("component", "Tomcat01")
        baseline = self._scored(a, 0.5, "bank_metric")
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        run_dir = Path(temp.name)
        cache = CacheStore(run_dir / "cache", enabled=True)
        agg_key, agg_digest = "aggregate-key", cache.put("aggregate-key", "Aggregate", agg)
        base_key, base_digest = "base-key", cache.put("base-key", "ScoredCandidates", baseline)
        data_spec = self.registry.require("case.telemetry", "1")
        agg_node = VerifiedNode("new_aggregate", aggregate_spec, {"data": ("data",)}, {}, {}, 2)
        scorer_node = VerifiedNode("base_score", self.base_scorer,
            {"reference": ("new_aggregate",), "alert": ("new_aggregate",),
             "candidates": ("new_aggregate",)}, {}, {}, 3)
        combine_node = VerifiedNode("answer", self.combine, {"scores": ("base_score",)},
            {"limit": 5}, {}, 4)
        data_node = VerifiedNode("data", data_spec, {}, {}, {}, 1)
        verified = VerifiedDAG((data_node, agg_node, scorer_node, combine_node), "answer", 4, 4)
        agg_record = _record("new_aggregate", aggregate_spec, self.runtime,
            cache_key=agg_key, output_digest=agg_digest)
        base_record = _record("base_score", self.base_scorer, self.runtime,
            cache_key=base_key, output_digest=base_digest)
        context = NodeContext(self.case_id, "answer", self.combine.qualified_name,
            self.runtime.snapshot.values, self.runtime.snapshot.provenance,
            self.runtime.snapshot.values["reference_window"],
            self.runtime.snapshot.values["alert_window"], "data-hash")
        answer = self.combine.implementation({"scores": (baseline,)}, {"limit": 5}, context)
        combine_record = _record("answer", self.combine, self.runtime)
        (run_dir / "audit").mkdir()
        (run_dir / "audit/public_query_calls.jsonl").write_text(json.dumps({
            "node_id": "query_1", "table": "log/log_service.csv", "window": "alert",
            "offset": 0, "limit": 2, "status": "success", "rows": 2,
            "truncated": True, "next_offset": 2}) + "\n")
        execution = ExecutionResult(answer, (agg_record, base_record, combine_record),
            0, self.case_id, "f" * 64)
        gap = {"schema_version": "e4-capability-gaps/1", "capabilities": [{
            "operator_name": aggregate_spec.name, "operator_version": aggregate_spec.version,
            "gap_hypothesis": "query aggregate", "closest_existing_operators": [],
            "computational_difference": "synthetic", "intended_contribution": "test",
            "required_inputs": ["Telemetry"], "coverage_requirement": "paged",
            "validation_expectation": "cache summary"}]}
        result = build_evolution_contribution_diagnostics(gap_record=gap,
            new_specs=[{"name": aggregate_spec.name, "version": aggregate_spec.version,
                "output_type": "Aggregate"}], dag_document=None, verified=verified,
            execution=execution, failure=None, cache=cache, runtime=self.runtime,
            registry=self.registry, run_dir=run_dir, case_id=self.case_id)
        coverage = result["query_coverage"]
        self.assertEqual(coverage["scope"], "case_cumulative")
        self.assertEqual(coverage["incomplete_call_count"], 1)
        self.assertIn("not attributed", coverage["attribution"])
        self.assertNotIn("rows_data", json.dumps(result))
        output = result["operators"][0]["nodes"][0]["output"]
        self.assertEqual(output["public_query_coverage"]["next_offset"], 2)
        self.assertTrue(result["operators"][0]["counterfactual_not_evaluated"])
        self.assertEqual(result["counterfactual"]["status"], "counterfactual_not_evaluated")

    def test_preverification_failure_is_recorded_without_execution(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        run_dir = Path(temp.name)
        cache = CacheStore(run_dir / "cache", enabled=True)
        gap = {"schema_version": "e4-capability-gaps/1", "capabilities": [{
            "operator_name": self.generated.name, "operator_version": self.generated.version,
            "gap_hypothesis": "gap", "closest_existing_operators": [],
            "computational_difference": "different", "intended_contribution": "test",
            "required_inputs": [], "coverage_requirement": "none", "validation_expectation": "valid"}]}
        result = build_evolution_contribution_diagnostics(gap_record=gap,
            new_specs=[{"name": self.generated.name, "version": self.generated.version,
                "output_type": "ScoredCandidates"}],
            dag_document={"nodes": [{"id": "new_score", "operator": self.generated.name,
                "version": self.generated.version, "inputs": {}}]}, verified=None,
            execution=None, failure=None, cache=cache, runtime=self.runtime,
            registry=self.registry, run_dir=run_dir,
            preverification_error={"code": "static_invalid", "message": "synthetic"})
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["operators"][0]["nodes"][0]["execution_status"],
                         "not_executed_preverification_failed")
        self.assertEqual(result["counterfactual"]["status"], "counterfactual_not_evaluated")

    def test_gap_record_joins_declared_version_to_normalized_validated_spec(self):
        raw = {"name": "evolved.gap_version_join", "version": "draft1",
            "description": "A synthetic diagnostic fixture operator.", "family": "test_family",
            "input_ports": [{"name": "candidates", "type": "CandidateSet"}],
            "output_type": "ScoredCandidates", "parameters": {},
            "source": "def operator(inputs, params, runtime):\n    return inputs['candidates']\n"}
        normalized = validate_generated_spec(raw)
        gap = {"schema_version": "e4-capability-gaps/1", "prior_plans": [],
            "capabilities": [{"operator_name": raw["name"],
                "operator_version": normalized["declared_version"],
                "gap_hypothesis": "version join check", "closest_existing_operators": [],
                "computational_difference": "synthetic", "intended_contribution": "test",
                "required_inputs": [], "coverage_requirement": "none",
                "validation_expectation": "joined"}]}
        result = build_evolution_contribution_diagnostics(gap_record=gap,
            new_specs=[normalized], dag_document={"nodes": [{"id": "gap_node",
                "operator": raw["name"], "version": normalized["version"], "inputs": {}}]},
            verified=None, execution=None, failure=None,
            cache=CacheStore(Path(tempfile.mkdtemp()) / "cache", enabled=True),
            runtime=self.runtime, registry=self.registry, run_dir=Path(tempfile.mkdtemp()),
            preverification_error={"code": "synthetic_static"})
        self.assertEqual(result["operators"][0]["operator"],
                         f"{raw['name']}@{normalized['version']}")
        self.assertEqual(result["operators"][0]["declared_version"], "draft1")
        self.assertEqual(result["operators"][0]["capability_gap"]["gap_hypothesis"],
                         "version join check")


if __name__ == "__main__":
    unittest.main()
