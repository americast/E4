from __future__ import annotations

import tempfile
import unittest
import math
import copy
import json
from pathlib import Path

from e4.controller.library import LibraryManager
from e4.controller.controller import _to_ranked_answer
from e4.dag import SerialExecutor, verify_dag
from e4.registry import OperatorRegistry
from e4.starters.paper import (
    build_paper_registry,
    get_paper_playbooks,
    paper_availability,
    paper_catalog,
    paper_reference_examples,
)
from e4.starters.statistics import permutation_test, welch_mean_test
from e4.sandbox.views import TelemetryView
from e4.types import (
    Candidate,
    CandidateSet,
    ColumnSchema,
    DatasetSchema,
    EntityRef,
    Evidence,
    MetricRecord,
    RankedAnswer,
    ScoredCandidate,
    ScoredCandidates,
    SpanRecord,
    TableSchema,
    Window,
)


METRIC_NAME = "response_latency"


def _schema(
    *,
    metric_role: str = "service",
    with_trace: bool = True,
    trace_columns: tuple[str, ...] = ("trace_id", "span_id", "parent_id"),
) -> DatasetSchema:
    tables = [
        TableSchema(
            "observations",
            "metric",
            # The adapter is free to map these source names to canonical records.
            (ColumnSchema("ts", "timestamp"), ColumnSchema("reading", "number")),
            entity_roles=(metric_role,),
        )
    ]
    if with_trace:
        tables.append(TableSchema(
            "calls",
            "span",
            tuple(ColumnSchema(name, "string") for name in trace_columns),
            entity_roles=("service",),
        ))
    return DatasetSchema(tuple(tables))


def _telemetry(*, tail_change: bool = False, spans: bool = False, truncated: bool = False) -> TelemetryView:
    reference = Window("reference", 0, 8 * 60000)
    alert = Window("alert", 8 * 60000, 16 * 60000)
    service = EntityRef("service", "checkout")
    records = []
    reference_values = [1.0] * 8
    alert_values = ([0.0] * 6 + [4.0] * 2) if tail_change else [2.0] * 8
    for idx, value in enumerate(reference_values):
        records.append(MetricRecord("observations", idx * 60000 + 100, service, METRIC_NAME, value, unit="ms"))
    for idx, value in enumerate(alert_values):
        records.append(MetricRecord("observations", alert.start_ms_utc + idx * 60000 + 100, service, METRIC_NAME, value, unit="ms"))
    if spans:
        caller = EntityRef("service", "frontend")
        callee = EntityRef("service", "storage")
        records.extend((
            SpanRecord("calls", alert.start_ms_utc + 100, "trace-a", "parent", None, 30.0, True, executing_component=caller),
            SpanRecord("calls", alert.start_ms_utc + 200, "trace-a", "child", "parent", 20.0, True, executing_component=callee),
            # Same span identifier in another trace must not create a false edge.
            SpanRecord("calls", alert.start_ms_utc + 300, "trace-b", "child", "parent", 10.0, True, executing_component=caller),
        ))
    return TelemetryView(
        "paper-test",
        reference,
        alert,
        records=tuple(records),
        source_tables=("observations", "calls") if spans else ("observations",),
        metadata={"truncated_tables": ["observations"]} if truncated else {},
    )


def _scored(entity: EntityRef, score: float, *, family: str, eligible: bool = True) -> ScoredCandidate:
    evidence = Evidence(
        source="test",
        window=Window("alert", 8 * 60000, 16 * 60000),
        statistic="mean_change",
        value=2.0,
        contributing_entities=(entity,),
        support=8,
        ranking_eligible=eligible,
    )
    return ScoredCandidate(entity, score, evidence=(evidence,), analysis_family=family)


class PaperDefaultTests(unittest.TestCase):
    def test_catalog_keeps_full_reconstructed_inventory_and_explicit_gaps(self) -> None:
        catalog = paper_catalog()
        self.assertEqual(catalog["counts"]["operators"], 19)
        self.assertEqual(catalog["counts"]["playbooks"], 11)
        self.assertEqual(len(catalog["operators"]), 19)
        self.assertEqual(len(catalog["playbooks"]), 11)
        implemented = [row for row in catalog["operators"] if row["implementation"]["status"] == "implemented_generic"]
        self.assertEqual(len(implemented), 6)
        unavailable = [row for row in catalog["operators"] if row["implementation"]["status"] == "not_implemented"]
        self.assertTrue(all(row["implementation"].get("reason") for row in unavailable))
        self.assertTrue(all(row.get("purpose") for row in catalog["operators"]))
        self.assertTrue(all(row.get("description") for row in catalog["playbooks"]))
        examples = paper_reference_examples()
        self.assertEqual(len(examples), 8)
        self.assertTrue(all(example["reference_only"] and not example["active_registration"] for example in examples))
        self.assertTrue(any(example["kind"] == "operator" and example["inputs"] for example in examples))
        book_examples = [example for example in examples if example["kind"] == "playbook"]
        self.assertEqual(len(book_examples), 2)
        self.assertTrue(all(example["rebind_to_validated_schema"] for example in book_examples))
        json.dumps(examples)
        self.assertIn("not equations specified", catalog["formula_disclosure"])

    def test_schema_availability_uses_canonical_metric_type_and_explicit_roles(self) -> None:
        schema = _schema()
        availability = paper_availability(schema)
        self.assertIn("paper.backend.metric_conditions", availability["active_operator_names"])
        self.assertIn("paper.backend.mean_welch", availability["active_operator_names"])
        self.assertIn("paper.scores.dependency_adjust", availability["active_operator_names"])
        self.assertEqual(len(get_paper_playbooks(schema)), 4)
        self.assertTrue(all(book.metadata["bound_source_table"] == "observations" for book in get_paper_playbooks(schema)))
        generic_specs = {op.name: op.as_dict() for op in build_paper_registry().all_operators()}
        schema_specs = {op.name: op.as_dict() for op in build_paper_registry(schema).all_operators()}
        self.assertEqual(generic_specs, schema_specs)

        app_schema = _schema(metric_role="application", with_trace=False)
        app_availability = paper_availability(app_schema)
        self.assertEqual(app_availability["active_playbook_names"], [])
        self.assertNotIn("paper.backend.metric_conditions", app_availability["active_operator_names"])
        self.assertEqual(len(build_paper_registry(app_schema).all_operators()), 1)  # generic consensus only
        metric_row = next(row for row in app_availability["metric_tables"] if row["name"] == "observations")
        self.assertIn("explicit recognized", metric_row["reason"])

    def test_span_schema_allows_adapter_mapped_raw_headers_and_marks_adjustment_conditional(self) -> None:
        schema = _schema(trace_columns=("trace_token", "call_id", "parent_call"))
        availability = paper_availability(schema)
        self.assertIn("paper.scores.dependency_adjust", availability["active_operator_names"])
        dependency = next(
            row for row in availability["operators"]
            if row["name"] == "paper.scores.dependency_adjust"
        )
        self.assertIn("conditional", dependency["availability_reason"])
        book = next(
            row for row in availability["playbooks"]
            if row["name"] == "dependency_adjusted_backend_consensus"
        )
        self.assertTrue(book["active"])
        self.assertIn("conditional", book["availability_reason"])

    def test_tail_scorer_remains_reachable_when_window_means_match(self) -> None:
        telemetry = _telemetry(tail_change=True)
        registry = build_paper_registry(_schema(with_trace=False))
        generator = registry.require("paper.backend.metric_conditions", "1")
        candidates = generator.implementation(
            {"telemetry": telemetry},
            {"source_table": "observations", "eligible_roles": ["service"], "bucket_ms": 60000},
            None,
        )
        self.assertEqual(len(candidates.candidates), 1)
        scorer = registry.require("paper.backend.tail_permutation", "1")
        output = scorer.implementation(
            {"candidates": candidates, "telemetry": telemetry},
            {
                "source_table": "observations",
                "eligible_roles": ["service"],
                "bucket_ms": 60000,
                "minimum_blocks": 4,
                "minimum_support": 4,
                "quantile": 0.95,
                "alpha": 0.5,
                "resamples": 1999,
                "seed": 13,
            },
            None,
        )
        self.assertEqual(len(output.candidates), 1)
        evidence = output.candidates[0].evidence[0]
        self.assertIn("quantile_0.95", evidence.statistic)
        self.assertEqual(evidence.details["unit"], "ms")
        self.assertTrue(evidence.ranking_eligible)

    def test_ineligible_strong_metric_cannot_launder_scorer_weight(self) -> None:
        reference = Window("reference", 0, 8 * 60000)
        alert = Window("alert", 8 * 60000, 16 * 60000)
        entity = EntityRef("service", "checkout")
        records = []
        material_reference = [0.0, 2.0] * 4
        material_alert = [1.0, 3.0] * 4
        for index in range(8):
            timestamp = index * 60000 + 100
            records.append(MetricRecord("observations", timestamp, entity, "material", material_reference[index], unit="ms"))
            records.append(MetricRecord("observations", timestamp, entity, "subtle", 0.0, unit="ms"))
            alert_timestamp = alert.start_ms_utc + index * 60000 + 100
            records.append(MetricRecord("observations", alert_timestamp, entity, "material", material_alert[index], unit="ms"))
            records.append(MetricRecord("observations", alert_timestamp, entity, "subtle", 0.001, unit="ms"))
        telemetry = TelemetryView(
            "weight-boundary",
            reference,
            alert,
            records=tuple(records),
            source_tables=("observations",),
        )
        registry = build_paper_registry(_schema(with_trace=False))
        candidate_set = registry.require("paper.backend.metric_conditions", "1").implementation(
            {"telemetry": telemetry},
            {"source_table": "observations", "eligible_roles": ["service"], "bucket_ms": 60000},
            None,
        )
        output = registry.require("paper.backend.mean_welch", "1").implementation(
            {"candidates": candidate_set, "telemetry": telemetry},
            {
                "source_table": "observations",
                "eligible_roles": ["service"],
                "bucket_ms": 60000,
                "minimum_blocks": 4,
                "minimum_support": 4,
                "minimum_effect": 0.1,
                "alpha": 0.5,
            },
            None,
        )
        self.assertEqual(len(output.candidates), 1)
        candidate = output.candidates[0]
        subtle = next(evidence for evidence in candidate.evidence if "subtle" in evidence.statistic)
        material = next(evidence for evidence in candidate.evidence if "material" in evidence.statistic)
        self.assertFalse(subtle.ranking_eligible)
        self.assertTrue(material.ranking_eligible)
        self.assertLess(subtle.details["p_value"], material.details["p_value"])
        expected_weight = -math.log10(max(material.details["p_value"], 1e-300))
        self.assertAlmostEqual(candidate.score, expected_weight)
        self.assertLess(candidate.score, 10.0)

    def test_incomplete_table_coverage_abstains_without_producing_scores(self) -> None:
        telemetry = _telemetry(tail_change=True, truncated=True)
        registry = build_paper_registry(_schema(with_trace=False))
        entity = EntityRef("service", "checkout")
        candidate_set = CandidateSet((Candidate(entity, "schema-supported metric candidate"),), "fixture")
        scorer = registry.require("paper.backend.mean_welch", "1")
        output = scorer.implementation(
            {"candidates": candidate_set, "telemetry": telemetry},
            {"source_table": "observations", "eligible_roles": ["service"]},
            None,
        )
        self.assertEqual(output.status, "abstained")
        self.assertEqual(output.candidates, ())

    def test_dependency_adjustment_uses_trace_local_parent_edges_and_preserves_evidence(self) -> None:
        registry = build_paper_registry(_schema())
        operator = registry.require("paper.scores.dependency_adjust", "1")
        caller = EntityRef("service", "frontend")
        callee = EntityRef("service", "storage")
        score_set = ScoredCandidates(
            (_scored(caller, 0.8, family="paper.backend_metric"), _scored(callee, 0.4, family="paper.backend_metric")),
            scorer="paper.backend.mean_welch",
        )
        output = operator.implementation(
            {"scores": (score_set,), "telemetry": _telemetry(spans=True)},
            {"trace_table": "calls", "downstream_penalty": 0.5},
            None,
        )
        adjusted = {candidate.entity.identifier: candidate for candidate in output.candidates}
        self.assertLess(adjusted["frontend"].score, 0.8)
        self.assertEqual(adjusted["frontend"].evidence, score_set.candidates[0].evidence)
        self.assertEqual(output.status_details["observed_edge_count"], 1)

    def test_consensus_weights_independent_families_and_ignores_ineligible_votes(self) -> None:
        registry = build_paper_registry()
        operator = registry.require("paper.answers.weighted_consensus", "1")
        first = EntityRef("service", "first")
        second = EntityRef("service", "second")
        family_a = ScoredCandidates((_scored(first, 3.0, family="family_a"),), "scorer_a")
        family_b = ScoredCandidates((
            _scored(first, 2.0, family="family_b"),
            _scored(second, 2.0, family="family_b"),
        ), "scorer_b")
        output = operator.implementation({"scores": (family_a, family_b)}, {"family_weights": {}}, None)
        self.assertIsInstance(output, RankedAnswer)
        self.assertEqual(output.candidates[0], first)
        self.assertGreater(output.scores[0], output.scores[1])
        self.assertEqual(len(output.evidence), 2)

        ignored = ScoredCandidates((_scored(first, 1000.0, family="untrusted", eligible=False),), "untrusted")
        alone = operator.implementation({"scores": (family_a, ignored)}, {"family_weights": {}}, None)
        self.assertEqual(alone.scores, (1.0,))
        duplicated = operator.implementation({"scores": (family_a, family_a)}, {"family_weights": {}}, None)
        self.assertEqual(duplicated.scores, (1.0,))
        self.assertEqual(len(duplicated.evidence), 1)

    def test_schema_bound_default_book_verifies_executes_and_reloads(self) -> None:
        schema = _schema(with_trace=False)
        registry = build_paper_registry(schema)
        playbooks = get_paper_playbooks(schema)
        book = next(item for item in playbooks if item.metadata["reconstruction_id"] == "B6")
        document = copy.deepcopy(dict(book.dag))
        score_node = next(node for node in document["nodes"] if node["operator"] == "paper.backend.tail_permutation")
        score_node["params"].update({"alpha": 0.5, "resamples": 1999, "quantile": 0.95, "minimum_blocks": 4})
        verified = verify_dag(document, registry, dataset_schema=schema)
        telemetry = _telemetry(tail_change=True)
        result = SerialExecutor(registry).execute_verified(
            verified,
            runtime=None,
            external_values={"telemetry": telemetry},
        )
        self.assertIsInstance(result.return_value, RankedAnswer)
        accepted = _to_ranked_answer(
            result.return_value,
            "serve",
            (EntityRef("service", "checkout"),),
        )
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted.top_candidate.identifier, "checkout")

        with tempfile.TemporaryDirectory() as directory:
            library = LibraryManager(registry=registry, playbooks=playbooks)
            library.save_to_dir(directory)
            reloaded = LibraryManager.load_from_dir(directory, base_registry=registry)
            self.assertIsNotNone(reloaded.registry.require("paper.backend.tail_permutation", "1").implementation)
            self.assertEqual(len(reloaded.list_playbooks()), 3)

    def test_statistical_reconstructions_are_deterministic_finite_and_scale_robust(self) -> None:
        effect, p_value = welch_mean_test((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))
        self.assertEqual(effect, 1.0)
        self.assertTrue(0.0 <= p_value <= 1.0)
        first = permutation_test((0.0, 0.0, 0.0), (0.0, 0.0, 4.0), statistic="quantile", quantile_probability=0.95, resamples=300, seed=7)
        repeated = permutation_test((0.0, 0.0, 0.0), (0.0, 0.0, 4.0), statistic="quantile", quantile_probability=0.95, resamples=300, seed=7)
        scaled = permutation_test((0.0, 0.0, 0.0), (0.0, 0.0, 4000.0), statistic="quantile", quantile_probability=0.95, resamples=300, seed=7)
        self.assertEqual(first, repeated)
        self.assertEqual(first[1], scaled[1])
        self.assertTrue(all(math.isfinite(value) for value in (first[0], first[1], scaled[0], scaled[1])))
        self.assertEqual(permutation_test((0.0,), (0.0,), statistic="mean", quantile_probability=0.5, resamples=10, seed=1), (0.0, 1.0))
        with self.assertRaises(ValueError):
            permutation_test((float("nan"),), (1.0,), statistic="mean", quantile_probability=0.5, resamples=10, seed=1)


if __name__ == "__main__":
    unittest.main()
