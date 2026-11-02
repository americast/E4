"""Independent behavior and trust-boundary checks for typed M4 operator IR."""
from __future__ import annotations

import copy
import unittest
from pathlib import Path

from e4core.bank_ops import aggregate_bank_metrics
from e4core.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
                              EntityRef, MetricPoint, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.m4_generated import (GeneratedSpecError, register_generated_specs,
                                validate_generated_spec)
from e4core.m4_controller import M4_EVOLUTION_SYSTEM_PROMPTS
from e4core.m4_operator_ir import (OperatorIRError, normalize_operator_ir_spec,
                                   typed_operator_ir_schema)
from e4core.m4_operator_ir_worker import IR_SCHEMA
from e4core.registry import OperatorRegistry
from e4core.sandbox import _ir_statistic


ROOT = Path(__file__).resolve().parents[1]
REF = Window("reference", 0, 1_800_000)
ALT = Window("alert", 1_800_000, 3_600_000)
SOURCE_HASHES = {"bank/metric_app.csv": "a" * 64,
                 "bank/metric_container.csv": "b" * 64}
DATA_SHA = "c" * 64
RUNTIME = {"minimum_support": 24, "minimum_independent_blocks": 4,
           "block_minutes": 5, "absolute_scale_floor": 1e-9,
           "lead_count": 10, "seed": 17}


def _node(ident, kernel, inputs, params=None, bindings=None):
    return {"id": ident, "kernel": kernel, "version": "1", "inputs": inputs,
            "params": params or {}, "bindings": bindings or {}}


def _program(kind):
    nodes = [
        _node("components", "series.component_metrics", {
            "reference": "$input.reference", "alert": "$input.alert",
            "candidates": "$input.candidates"}),
        _node("field", "series.select_field", {"series": "components"},
              {"source_field": "OSLinux-CPU_CPU_CPUCpuUtil"}),
    ]
    if kind == "kpi_weighted_mean_change":
        nodes += [
            _node("kpi", "series.primary_kpi", {"reference": "$input.reference",
                  "alert": "$input.alert"}),
            _node("aligned", "series.align_kpi", {"components": "field", "kpi": "kpi"}),
            _node("association", "series.correlate", {"pairs": "aligned"},
                  {"method": "spearman"}, {"minimum_pairs": "runtime.minimum_support"}),
            _node("summaries", "series.reduce", {"series": "field"},
                  {"statistic": "mean"}, {"block_minutes": "runtime.block_minutes",
                                               "minimum_blocks": "runtime.minimum_independent_blocks"}),
            _node("change", "numbers.window_compare", {"values": "summaries"},
                  {"operation": "relative_change"}),
            _node("magnitude", "numbers.absolute", {"values": "change"}),
            _node("weighted", "numbers.multiply", {"left": "magnitude", "right": "association"}),
            _node("scores", "numbers.reduce_component", {"values": "weighted"}, {"method": "mean"}),
        ]
    elif kind == "variance_change":
        nodes += [
            _node("summaries", "series.reduce", {"series": "field"},
                  {"statistic": "variance"}, {"block_minutes": "runtime.block_minutes",
                                                   "minimum_blocks": "runtime.minimum_independent_blocks"}),
            _node("change", "numbers.window_compare", {"values": "summaries"},
                  {"operation": "difference"}),
            _node("magnitude", "numbers.absolute", {"values": "change"}),
            _node("scores", "numbers.reduce_component", {"values": "magnitude"}, {"method": "max"}),
        ]
    else:
        raise AssertionError(kind)
    nodes += [_node("evidence", "evidence.from_scores", {"scores": "scores"}),
              _node("result", "scores.assemble", {"candidates": "$input.candidates",
                  "scores": "scores", "evidence": "evidence"})]
    return {"schema_version": IR_SCHEMA, "nodes": nodes, "return": {"node": "result"}}


def _spec(kind):
    return {"name": f"evolved.{kind}", "version": "1", "description": f"Synthetic {kind} analysis",
            "family": kind, "output_type": "ScoredCandidates", "parameters": {},
            "authoring_profile": "typed-operator-ir-v1",
            "program": _program(kind)}


def _pure_correlation_spec():
    raw = _spec("kpi_weighted_mean_change")
    raw.update({"name": "evolved.component_kpi_association",
        "description": "Computes a signed component/KPI association.",
        "family": "bank_metric_association"})
    raw["program"] = {"schema_version": IR_SCHEMA, "nodes": [
        _node("components", "series.component_metrics", {
            "reference": "$input.reference", "alert": "$input.alert",
            "candidates": "$input.candidates"}),
        _node("field", "series.select_field", {"series": "components"},
            {"source_field": "OSLinux-CPU_CPU_CPUCpuUtil"}),
        _node("kpi", "series.primary_kpi", {"reference": "$input.reference",
            "alert": "$input.alert"}),
        _node("aligned", "series.align_kpi", {"components": "field", "kpi": "kpi"}),
        _node("association", "series.correlate", {"pairs": "aligned"},
            {"method": "spearman"}, {"minimum_pairs": "runtime.minimum_support"}),
        _node("scores", "numbers.reduce_component", {"values": "association"}, {"method": "max"}),
        _node("evidence", "evidence.from_scores", {"scores": "scores"}),
        _node("result", "scores.assemble", {"candidates": "$input.candidates",
            "scores": "scores", "evidence": "evidence"})], "return": {"node": "result"}}
    return raw


def _all_fields_variance_program():
    nodes = [
        _node("components", "series.component_metrics", {
            "reference": "$input.reference", "alert": "$input.alert",
            "candidates": "$input.candidates"}),
        _node("summaries", "series.reduce", {"series": "components"},
              {"statistic": "variance"}, {"block_minutes": "runtime.block_minutes",
                                               "minimum_blocks": "runtime.minimum_independent_blocks"}),
        _node("change", "numbers.window_compare", {"values": "summaries"},
              {"operation": "difference"}),
        _node("magnitude", "numbers.absolute", {"values": "change"}),
        _node("scores", "numbers.reduce_component", {"values": "magnitude"}, {"method": "max"}),
        _node("evidence", "evidence.from_scores", {"scores": "scores"}),
        _node("result", "scores.assemble", {"candidates": "$input.candidates",
            "scores": "scores", "evidence": "evidence"}),
    ]
    return {"schema_version": IR_SCHEMA, "nodes": nodes, "return": {"node": "result"}}


def _mismatched_evidence_program():
    nodes = [
        _node("components", "series.component_metrics", {
            "reference": "$input.reference", "alert": "$input.alert",
            "candidates": "$input.candidates"}),
        _node("field", "series.select_field", {"series": "components"},
              {"source_field": "OSLinux-CPU_CPU_CPUCpuUtil"}),
        _node("summaries", "series.reduce", {"series": "field"},
              {"statistic": "variance"}, {"block_minutes": "runtime.block_minutes",
                                               "minimum_blocks": "runtime.minimum_independent_blocks"}),
        _node("change", "numbers.window_compare", {"values": "summaries"},
              {"operation": "difference"}),
        _node("magnitude", "numbers.absolute", {"values": "change"}),
        _node("scores_a", "numbers.reduce_component", {"values": "magnitude"}, {"method": "max"}),
        _node("evidence_a", "evidence.from_scores", {"scores": "scores_a"}),
        _node("relative", "numbers.window_compare", {"values": "summaries"},
              {"operation": "relative_change"}),
        _node("relative_magnitude", "numbers.absolute", {"values": "relative"}),
        _node("scores_b", "numbers.reduce_component", {"values": "relative_magnitude"},
              {"method": "max"}),
        _node("result", "scores.assemble", {"candidates": "$input.candidates",
            "scores": "scores_b", "evidence": "evidence_a"}),
    ]
    return {"schema_version": IR_SCHEMA, "nodes": nodes, "return": {"node": "result"}}


def _trusted_inputs(*, kpi_support=30, constant_components=False,
                    include_second_field=False):
    """Use the actual Bank aggregation operator on deterministic analytic points."""
    names = ("component-coral", "component-ember", "component-jade")
    points = []
    for index in range(30):
        for window, start in (("reference", 0), ("alert", 1_800_000)):
            timestamp = start + index * 60_000
            kpi = 10.0 if window == "reference" else 10.0 + index
            if index < kpi_support or window == "reference":
                points.append(MetricPoint("bank/metric_app.csv", timestamp,
                    EntityRef("app_kpi", "opaque-tc-a"), "mrt", None, None,
                    kpi, str(kpi), f"app:{window}:{index}"))
            for component_index, name in enumerate(names):
                if constant_components:
                    value = 4.0
                elif component_index == 0:  # Small mean rise, monotone with selected KPI.
                    value = 10.0 if window == "reference" else 10.0 + (index + 1) / 30
                elif component_index == 1:  # Large variance rise with no KPI association.
                    value = 10.0 if window == "reference" else (5.0 if index % 2 == 0 else 15.0)
                else:  # Stable control.
                    value = 8.0
                points.append(MetricPoint("bank/metric_container.csv", timestamp,
                    EntityRef("component", name), "OSLinux-CPU_CPU_CPUCpuUtil",
                    "bank.metric_container", "fixture", value, str(value),
                    f"container:{window}:{index}:{name}"))
                if include_second_field:
                    memory_value = (5.0 if window == "reference" else
                                    5.0 + ((index % 2) * 8.0) + component_index)
                    points.append(MetricPoint("bank/metric_container.csv", timestamp,
                        EntityRef("component", name),
                        "OSLinux-OSLinux_MEMORY_MEMORY_MEMUsedMemPerc",
                        "bank.metric_container", "fixture", memory_value, str(memory_value),
                        f"memory:{window}:{index}:{name}"))

    def aggregate(window):
        telemetry = Telemetry("analytic-fixture", REF, ALT, tuple(points), (), {},
            DATA_SHA, SOURCE_HASHES, {}, {}, "m4-ir-test/1")
        value = aggregate_bank_metrics({"data": telemetry}, {"window": window},
            NodeContext("analytic-fixture", window, "bank.metrics.aggregate", RUNTIME, {}, REF, ALT, DATA_SHA)).value
        metadata = dict(value.metadata)
        metadata.update({"source_hashes": SOURCE_HASHES, "case_data_sha256": DATA_SHA})
        return Aggregate(value.window, value.rows, value.source_tables, metadata)

    candidates = CandidateSet(tuple(Candidate(EntityRef("component", name), "fixture-observed")
                                    for name in names), source="analytic fixture")
    return {"reference": aggregate("reference"), "alert": aggregate("alert"),
            "candidates": candidates}


def _run(spec, inputs, runtime=None):
    registry = OperatorRegistry()
    normalized = register_generated_specs(registry, [spec])[0]
    operator = registry.require(normalized["name"], normalized["version"])
    return operator.implementation(inputs, {}, NodeContext("analytic-fixture", "ir", normalized["name"],
        runtime or RUNTIME, {}, REF, ALT, DATA_SHA))


class OperatorIRTests(unittest.TestCase):
    def test_support_metadata_tracks_units_without_changing_scores_or_scalar_support(self):
        inputs = _trusted_inputs()
        block = _run(_spec("variance_change"), inputs)
        correlation = _run(_pure_correlation_spec(), inputs)
        mixed = _run(_spec("kpi_weighted_mean_change"), inputs)

        def rows(result):
            return {row.entity.identifier: row for row in result.candidates}

        block_rows, corr_rows, mixed_rows = map(rows, (block, correlation, mixed))
        expected = {
            "block": {"component-coral": 0.08101851851851859,
                "component-ember": 25.0, "component-jade": 0.0},
            "correlation": {"component-coral": 1.0,
                "component-ember": 0.05776712868787001, "component-jade": 0.0},
            "mixed": {"component-coral": 0.05166666666666675,
                "component-ember": 0.0, "component-jade": 0.0}}
        for label, result_rows in (("block", block_rows), ("correlation", corr_rows), ("mixed", mixed_rows)):
            self.assertEqual(set(result_rows), set(expected[label]))
            for component, score in expected[label].items():
                self.assertAlmostEqual(result_rows[component].score, score, places=14)
        self.assertEqual({name for name, row in block_rows.items() if row.evidence},
            {"component-coral", "component-ember"})
        self.assertEqual({name for name, row in corr_rows.items() if row.evidence},
            {"component-coral", "component-ember"})
        self.assertEqual({name for name, row in mixed_rows.items() if row.evidence},
            {"component-coral"})

        block_evidence = block_rows["component-coral"].evidence[0]
        self.assertEqual(block_evidence.support, 6)
        self.assertEqual(dict(block_evidence.details["support_counts"]),
            {"independent_time_blocks": 6})
        self.assertEqual(block_evidence.details["support_unit"], "independent_time_blocks")

        corr_evidence = corr_rows["component-coral"].evidence[0]
        self.assertEqual(corr_evidence.support, 30)
        self.assertEqual(dict(corr_evidence.details["support_counts"]),
            {"exact_timestamp_pairs": 30})
        self.assertEqual(corr_evidence.details["support_unit"], "exact_timestamp_pairs")
        self.assertEqual(corr_evidence.statistic, "component_kpi_association")

        mixed_evidence = mixed_rows["component-coral"].evidence[0]
        self.assertEqual(mixed_evidence.support, 6)
        self.assertEqual(dict(mixed_evidence.details["support_counts"]), {
            "exact_timestamp_pairs": 30, "independent_time_blocks": 6})
        self.assertEqual(mixed_evidence.details["support_unit"],
            "minimum_across_mixed_support_counts")
        self.assertEqual(mixed_evidence.statistic,
            "component_kpi_association_weighted_mean_relative_change")

    def test_ir_statistic_names_match_string_and_integer_kernel_versions(self):
        program = _program("variance_change")
        self.assertEqual(_ir_statistic(program), "component_variance_difference")
        integer_versions = copy.deepcopy(program)
        for node in integer_versions["nodes"]:
            node["version"] = 1
        self.assertEqual(_ir_statistic(integer_versions), "component_variance_difference")
        self.assertEqual(_ir_statistic(_pure_correlation_spec()["program"]),
            "component_kpi_association")

    def test_numeric_score_and_combiner_semantics_are_neutral_and_explicit(self):
        schema = typed_operator_ir_schema()
        semantics = schema["kernel_semantics"]
        self.assertIn("[-1,1]", semantics["series.correlate"])
        self.assertIn("not a change test", semantics["series.correlate"])
        self.assertIn("strictly positive", semantics["numbers.reduce_component"])
        self.assertIn("maximum raw score", schema["score_combination_semantics"])
        self.assertIn("maximum raw score", M4_EVOLUTION_SYSTEM_PROMPTS["compose"])
        self.assertIn("[-1,1]", M4_EVOLUTION_SYSTEM_PROMPTS["stage3_serve"])
        self.assertIn("Do not rename families, inflate scores, or force a different ranking",
            M4_EVOLUTION_SYSTEM_PROMPTS["stage3_serve"])

    def test_two_distinct_closed_programs_produce_different_rankings(self):
        inputs = _trusted_inputs()
        mean_result = _run(_spec("kpi_weighted_mean_change"), inputs)
        variance_result = _run(_spec("variance_change"), inputs)

        def order(result):
            return [item.entity.identifier for item in sorted(result.candidates,
                key=lambda row: (-row.score, row.entity.identifier)) if item.score > 0]

        self.assertEqual(order(mean_result)[0], "component-coral")
        self.assertEqual(order(variance_result)[0], "component-ember")
        self.assertNotEqual(order(mean_result), order(variance_result))
        coral = next(item for item in mean_result.candidates if item.entity.identifier == "component-coral")
        self.assertEqual(coral.evidence[0].details["unit"], "dimensionless")
        for result in (mean_result, variance_result):
            self.assertTrue(all(item.category is None for item in result.candidates))
            self.assertTrue(all(item.evidence for item in result.candidates if item.score > 0))
            evidence = next(item.evidence[0] for item in result.candidates if item.score > 0)
            self.assertEqual(evidence.details["case_data_sha256"], DATA_SHA)
            self.assertEqual(dict(evidence.details["source_hashes"]), SOURCE_HASHES)
            self.assertTrue(evidence.details["score_lineage_sha256"])

    def test_constant_series_and_missing_kpi_abstain_without_invented_evidence(self):
        inputs = _trusted_inputs(constant_components=True)
        inputs["reference"] = _without_kpi(inputs["reference"])
        inputs["alert"] = _without_kpi(inputs["alert"])
        result = _run(_spec("kpi_weighted_mean_change"), inputs)
        self.assertEqual(result.status, "abstained")
        self.assertTrue(all(item.score == 0 and not item.evidence for item in result.candidates))

    def test_kpi_support_gate_and_tie_break_are_deterministic(self):
        # Policy requires 24 unique timestamps in both windows; 23 cannot select a KPI.
        unsupported = _trusted_inputs(kpi_support=23)
        result = _run(_spec("kpi_weighted_mean_change"), unsupported)
        self.assertEqual(result.status, "abstained")

        inputs = _trusted_inputs()
        # Add equal-support KPI tc identity; canonical opaque identity wins the selection.
        for port in ("reference", "alert"):
            agg = inputs[port]
            rows = list(agg.rows)
            for row in tuple(rows):
                if row.entity == EntityRef("app_kpi", "opaque-tc-a") and row.statistic == "median":
                    rows.append(AggregateRow(EntityRef("app_kpi", "opaque-tc-0"), row.metric_key,
                        row.window, row.statistic, row.value, row.support, row.source_tables, (), row.details))
            inputs[port] = Aggregate(agg.window, tuple(rows), agg.source_tables, agg.metadata)
        result = _run(_spec("kpi_weighted_mean_change"), inputs)
        scored = next(item for item in result.candidates if item.score > 0)
        selected = scored.evidence[0].details["metrics"][0]["details"]["right"]
        self.assertEqual(selected["selected_kpi_id"], "opaque-tc-0")

    def test_exact_schema_types_runtime_bindings_and_source_free_identity(self):
        raw = _spec("variance_change")
        normalized = normalize_operator_ir_spec(raw)
        self.assertNotIn("source", normalized)
        self.assertEqual(normalized["program_sha256"], normalized["source_sha256"])
        self.assertEqual(validate_generated_spec(normalized), normalized)

        mutations = []
        extra = copy.deepcopy(raw); extra["python"] = "return 3"; mutations.append(extra)
        extra = copy.deepcopy(raw); extra["parameters"] = {"code": {}}; mutations.append(extra)
        extra = copy.deepcopy(raw); extra["program"]["nodes"][0]["surprise"] = True; mutations.append(extra)
        extra = copy.deepcopy(raw); extra["program"]["nodes"][0]["kernel"] = "python.exec"; mutations.append(extra)
        extra = copy.deepcopy(raw); extra["program"]["nodes"][3]["bindings"]["block_minutes"] = "runtime.seed"; mutations.append(extra)
        extra = copy.deepcopy(raw); extra["program"]["nodes"][1]["inputs"]["series"] = "$input.alert"; mutations.append(extra)
        extra = copy.deepcopy(raw); extra["program"]["nodes"][1]["params"]["source_field"] = "mrt"; mutations.append(extra)
        # A flattened series cannot feed a windowed series selector, and KPISeries
        # cannot enter a component-only selector even when both are produced above.
        extra = copy.deepcopy(raw)
        extra["program"]["nodes"].insert(2, _node("kpi", "series.primary_kpi",
            {"reference": "$input.reference", "alert": "$input.alert"}))
        extra["program"]["nodes"][3]["inputs"]["series"] = "kpi"
        mutations.append(extra)
        for invalid in mutations:
            with self.subTest(invalid=invalid):
                with self.assertRaises((OperatorIRError, GeneratedSpecError, ValueError)):
                    normalize_operator_ir_spec(invalid)

    def test_compact_authored_spec_injects_fixed_host_abi_and_legacy_full_spec_still_works(self):
        compact = _spec("variance_change")
        normalized = normalize_operator_ir_spec(compact)
        self.assertEqual([row["name"] for row in normalized["input_ports"]],
                         ["reference", "alert", "candidates"])
        self.assertEqual(set(normalized["input_requirements"]),
                         {"reference", "alert", "candidates"})

        from e4core.m4_operator_ir import typed_operator_ir_schema
        fixed = typed_operator_ir_schema()["fixed_host_abi"]
        self.assertNotIn("input_ports", typed_operator_ir_schema()["spec_fields"])
        self.assertNotIn("input_requirements", typed_operator_ir_schema()["spec_fields"])
        legacy = {**compact, "input_ports": fixed["input_ports"],
                  "input_requirements": fixed["input_requirements"]}
        self.assertEqual(normalize_operator_ir_spec(legacy), normalized)
        self.assertEqual(validate_generated_spec(legacy), normalized)

        invalid = copy.deepcopy(compact)
        invalid["inputs"] = {"reference": "Aggregate"}
        with self.assertRaises(OperatorIRError):
            normalize_operator_ir_spec(invalid)

        # Public registration and sandbox execution use the compact raw form.
        result = _run(compact, _trusted_inputs())
        self.assertEqual(result.status, "ready")

    def test_normalized_identity_rejects_program_abi_worker_and_policy_tampering(self):
        normalized = normalize_operator_ir_spec(_spec("variance_change"))
        mutations = []
        changed = copy.deepcopy(normalized)
        changed["program"]["nodes"][1]["params"]["source_field"] = "OSLinux-CPU_CPU_CPULoad"
        mutations.append(changed)
        for field in ("kernel_abi_sha256", "interpreter_sha256"):
            changed = copy.deepcopy(normalized)
            changed[field] = "0" * 64
            mutations.append(changed)
        changed = copy.deepcopy(normalized)
        changed["policy_pins"]["component_metric_policy_sha256"] = "0" * 64
        mutations.append(changed)
        for changed in mutations:
            with self.subTest(field=next((key for key in changed if changed[key] != normalized.get(key)), "program")):
                with self.assertRaises(GeneratedSpecError):
                    validate_generated_spec(changed)

    def test_provenance_pair_pins_and_runtime_bounds_are_enforced(self):
        inputs = _trusted_inputs()
        inputs["alert"] = _with_metadata(inputs["alert"],
            case_data_sha256="d" * 64,
            source_hashes={**SOURCE_HASHES, "bank/metric_container.csv": "e" * 64})
        with self.assertRaisesRegex(RuntimeError, "provenance"):
            _run(_spec("variance_change"), inputs)

        bad_runtime = {**RUNTIME, "minimum_support": 0}
        # The profile binds to the runtime field; execution rejects values outside policy bounds.
        with self.assertRaises(RuntimeError):
            _run(_spec("kpi_weighted_mean_change"), _trusted_inputs(), bad_runtime)

    def test_component_score_unit_compatibility_is_preserved(self):
        result = _run(_spec("variance_change"), _trusted_inputs())
        for candidate in result.candidates:
            if candidate.score > 0:
                self.assertIn("variance", candidate.evidence[0].details["unit"])

    def test_native_variance_from_distinct_fields_cannot_be_reduced_together(self):
        raw = _spec("variance_change")
        raw["program"] = _all_fields_variance_program()
        inputs = _trusted_inputs(include_second_field=True)
        expected_fields = {"OSLinux-CPU_CPU_CPUCpuUtil",
                           "OSLinux-OSLinux_MEMORY_MEMORY_MEMUsedMemPerc"}
        for side in ("reference", "alert"):
            rows = [row for row in inputs[side].rows if row.entity.role == "component" and
                    row.statistic == "median" and row.details["source_field"] in expected_fields]
            self.assertEqual({row.details["unit"] for row in rows}, {"undeclared native value"})
            self.assertEqual({row.details["source_field"] for row in rows}, expected_fields)
        result = _run(raw, inputs)
        self.assertEqual(result.status, "abstained")
        self.assertTrue(all(candidate.score == 0 and not candidate.evidence
                            for candidate in result.candidates))

    def test_evidence_cannot_be_assembled_from_a_different_score_branch(self):
        raw = _spec("variance_change")
        raw["program"] = _mismatched_evidence_program()
        with self.assertRaisesRegex(OperatorIRError, "evidence|score"):
            normalize_operator_ir_spec(raw)

    def test_finite_inputs_that_overflow_variance_abstain_without_nonfinite_scores(self):
        inputs = _trusted_inputs()
        aggregate = inputs["alert"]
        rows = []
        for row in aggregate.rows:
            if row.entity == EntityRef("component", "component-coral") and row.statistic == "median":
                details = dict(row.details)
                details["bucket_samples"] = tuple((stamp, 1e308 if index % 2 else -1e308)
                    for index, (stamp, _value) in enumerate(details["bucket_samples"]))
                row = AggregateRow(row.entity, row.metric_key, row.window, row.statistic,
                    row.value, row.support, row.source_tables, row.contributing_entities, details)
            rows.append(row)
        inputs["alert"] = Aggregate(aggregate.window, tuple(rows), aggregate.source_tables,
                                    aggregate.metadata)
        result = _run(_spec("variance_change"), inputs)
        coral = next(item for item in result.candidates if item.entity.identifier == "component-coral")
        self.assertEqual(coral.score, 0.0)
        self.assertFalse(coral.evidence)
        self.assertTrue(all(item.score >= 0 and item.score < float("inf") for item in result.candidates))


def _with_metadata(aggregate, **changes):
    metadata = dict(aggregate.metadata)
    metadata.update(changes)
    return Aggregate(aggregate.window, aggregate.rows, aggregate.source_tables, metadata)


def _without_kpi(aggregate):
    rows = tuple(row for row in aggregate.rows if row.entity.role != "app_kpi")
    return Aggregate(aggregate.window, rows, aggregate.source_tables, aggregate.metadata)


if __name__ == "__main__":
    unittest.main()
