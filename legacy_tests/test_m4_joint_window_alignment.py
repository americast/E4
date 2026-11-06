import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from e4core.bank_ops import aggregate_bank_metrics
from e4core.cache import canonical_json, deserialize_contract_json
from e4core.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
    EntityRef, MetricPoint, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.m4_generated import register_generated_specs
from e4core.m4_operator_ir import normalize_operator_ir_spec, trusted_runtime_context
from e4core.m4_operator_ir_worker import (_align_kpi_both_windows, _component_series,
    _correlate, _decode, _primary_kpi, execute_ir)
from e4core.registry import OperatorRegistry
from e4core.sandbox import _ir_statistic
from tests.test_m4_operator_ir import DATA_SHA, REF, ALT, RUNTIME


ROOT = Path(__file__).resolve().parents[1]
SOURCE_HASHES = {"bank/metric_app.csv": "a" * 64,
                 "bank/metric_container.csv": "b" * 64}
METRIC_FIELD = "OSLinux-CPU_CPU_CPUCpuUtil"


def _typed_inputs(*, scenario="step", misalign_alert=False):
    candidates = CandidateSet((Candidate(EntityRef("component", "component-true")),
                               Candidate(EntityRef("component", "component-decoy"))),
                             "joint-window fixture")
    points = []
    for index in range(30):
        ref_ts = REF.start_ms_utc + index * 60_000
        alt_ts = ALT.start_ms_utc + index * 60_000
        kpi_reference = 10.0 + index if scenario == "reference_temporal" else 10.0
        true_reference = 5.0 + index if scenario == "reference_temporal" else 5.0
        points.append(MetricPoint("bank/metric_app.csv", ref_ts,
            EntityRef("app_kpi", "fixture-mrt"), "mrt", None, None,
            kpi_reference, str(kpi_reference), f"kpi-ref-{index}"))
        if scenario == "temporal":
            kpi_alert = 20.0 + index
            true_alert = 6.0 + index
        else:
            kpi_alert = 13.0
            true_alert = 7.0
        points.append(MetricPoint("bank/metric_app.csv", alt_ts,
            EntityRef("app_kpi", "fixture-mrt"), "mrt", None, None,
            kpi_alert, str(kpi_alert), f"kpi-alert-{index}"))
        points.append(MetricPoint("bank/metric_container.csv", ref_ts,
            EntityRef("component", "component-true"), METRIC_FIELD,
            "bank.metric_container", "fixture", true_reference, str(true_reference), f"true-ref-{index}"))
        points.append(MetricPoint("bank/metric_container.csv", ref_ts,
            EntityRef("component", "component-decoy"), METRIC_FIELD,
            "bank.metric_container", "fixture", 5.0, "5", f"decoy-ref-{index}"))
        points.append(MetricPoint("bank/metric_container.csv", alt_ts + int(misalign_alert),
            EntityRef("component", "component-true"), METRIC_FIELD,
            "bank.metric_container", "fixture", true_alert, str(true_alert), f"true-alert-{index}"))
        points.append(MetricPoint("bank/metric_container.csv", alt_ts,
            EntityRef("component", "component-decoy"), METRIC_FIELD,
            "bank.metric_container", "fixture", 5.0, "5", f"decoy-alert-{index}"))
    telemetry = Telemetry("joint-window-fixture", REF, ALT, tuple(points), (), {},
        DATA_SHA, SOURCE_HASHES, {}, {}, "joint-window-test/1")

    def aggregate(window_name):
        value = aggregate_bank_metrics({"data": telemetry}, {"window": window_name},
            NodeContext(telemetry.case_id, window_name, "bank.metrics.aggregate", {}, {},
                        REF, ALT, DATA_SHA)).value
        metadata = dict(value.metadata)
        metadata.update({"source_hashes": SOURCE_HASHES, "case_data_sha256": DATA_SHA})
        return Aggregate(value.window, value.rows, value.source_tables, metadata)

    return {"reference": aggregate("reference"), "alert": aggregate("alert"),
            "candidates": candidates}


def _program(alignment_kernel):
    nodes = [
        {"id": "components", "kernel": "series.component_metrics", "version": 1,
         "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                    "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
        {"id": "field", "kernel": "series.select_field", "version": 1,
         "inputs": {"series": "components"},
         "params": {"source_field": METRIC_FIELD}, "bindings": {}},
        {"id": "kpi", "kernel": "series.primary_kpi", "version": 1,
         "inputs": {"reference": "$input.reference", "alert": "$input.alert"},
         "params": {}, "bindings": {}},
        {"id": "aligned", "kernel": alignment_kernel, "version": 1,
         "inputs": {"components": "field", "kpi": "kpi"}, "params": {}, "bindings": {}},
        {"id": "correlation", "kernel": "series.correlate", "version": 1,
         "inputs": {"pairs": "aligned"}, "params": {"method": "spearman"},
         "bindings": {"minimum_pairs": "runtime.minimum_support"}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": 1,
         "inputs": {"values": "correlation"}, "params": {"method": "max"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": 1,
         "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "result", "kernel": "scores.assemble", "version": 1,
         "inputs": {"candidates": "$input.candidates", "scores": "scores",
                    "evidence": "evidence"}, "params": {}, "bindings": {}},
    ]
    return {"schema_version": "e4-typed-operator-ir/1", "nodes": nodes,
            "return": {"node": "result"}}


def _execute(alignment_kernel, scenario="step", misalign_alert=False):
    raw = {"name": "evolved.joint_alignment_fixture", "version": "1",
        "description": "Generic timestamp correlation over a declared window scope.",
        "family": "joint_window_association", "output_type": "ScoredCandidates",
        "parameters": {}, "authoring_profile": "typed-operator-ir-v1",
        "program": _program(alignment_kernel)}
    normalized = normalize_operator_ir_spec(raw)
    inputs = _typed_inputs(scenario=scenario, misalign_alert=misalign_alert)
    wire_inputs = {name: json.loads(canonical_json(value).decode("utf-8"))
                   for name, value in inputs.items()}
    runtime = {**RUNTIME, "ir_family": "joint_window_association",
               "ir_statistic": "joint_window_spearman"}
    result_wire = execute_ir(normalized["program"], wire_inputs, {}, runtime,
                             trusted_runtime_context())
    return deserialize_contract_json(result_wire, "ScoredCandidates")


class JointWindowAlignmentTests(unittest.TestCase):
    def test_joint_alignment_captures_coordinated_constant_step_and_unchanged_decoy(self):
        result = _execute("series.align_kpi_both_windows")
        by_id = {item.entity.identifier: item for item in result.candidates}
        self.assertEqual(by_id["component-true"].score, 1.0)
        self.assertTrue(by_id["component-true"].evidence)
        self.assertEqual(by_id["component-true"].evidence[0].details["metrics"][0]["details"]["timestamp_scope"],
            "reference_and_alert_exact_no_interpolation")
        self.assertEqual(by_id["component-true"].evidence[0].details["metrics"][0]["support"], 60)
        self.assertEqual(by_id["component-decoy"].score, 0.0)
        self.assertFalse(by_id["component-decoy"].evidence)

    def test_existing_alert_only_alignment_keeps_its_constant_step_behavior(self):
        result = _execute("series.align_kpi")
        self.assertTrue(all(item.score == 0.0 for item in result.candidates))
        self.assertFalse(any(item.evidence for item in result.candidates))

    def test_regular_alert_temporal_correlation_remains_supported_by_both_kernels(self):
        old = _execute("series.align_kpi", scenario="temporal")
        new = _execute("series.align_kpi_both_windows", scenario="temporal")
        old_true = next(item for item in old.candidates if item.entity.identifier == "component-true")
        new_true = next(item for item in new.candidates if item.entity.identifier == "component-true")
        self.assertAlmostEqual(old_true.score, 1.0)
        self.assertAlmostEqual(new_true.score, 1.0)
        old_details = old_true.evidence[0].details["metrics"][0]["details"]
        self.assertEqual(old_details["timestamp_alignment"], "exact_no_interpolation")
        self.assertNotIn("timestamp_scope", old_details)

    def test_joint_alignment_uses_exact_timestamps_without_interpolation(self):
        result = _execute("series.align_kpi_both_windows", scenario="reference_temporal",
                          misalign_alert=True)
        true = next(item for item in result.candidates if item.entity.identifier == "component-true")
        self.assertEqual(true.score, 0.0)
        self.assertFalse(true.evidence)
        self.assertEqual(result.status_details["reason"],
            "joint_window_exact_support_missing_from_one_or_more_windows")
        typed_inputs = _typed_inputs(scenario="reference_temporal", misalign_alert=True)
        wire_inputs = {name: json.loads(canonical_json(value).decode("utf-8"))
                       for name, value in typed_inputs.items()}
        decoded = {name: _decode(value) for name, value in wire_inputs.items()}
        trusted = trusted_runtime_context()
        components = _component_series({"reference": decoded["reference"],
            "alert": decoded["alert"], "candidates": decoded["candidates"]}, trusted)
        true_series = next(row for row in components["rows"]
                           if row["key"][1] == "component-true")
        true_series["alert"]["samples"] = [(timestamp + 1, value)
            for timestamp, value in true_series["alert"]["samples"]]
        kpi = _primary_kpi({"reference": decoded["reference"],
                            "alert": decoded["alert"]}, trusted)
        aligned = _align_kpi_both_windows(components, kpi)
        true_alignment = next(row for row in aligned["rows"] if row["key"][1] == "component-true")
        self.assertEqual(len(true_alignment["pairs"]), 30)
        scored = _correlate(aligned, "spearman", int(RUNTIME["minimum_support"]))
        true_correlation = next(row for row in scored["rows"] if row["key"][1] == "component-true")
        self.assertFalse(true_correlation["eligible"])
        self.assertEqual(true_correlation["details"]["support_counts"], {
            "exact_timestamp_pairs": 30, "reference_window_timestamp_pairs": 30,
            "alert_window_timestamp_pairs": 0})

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
                         "set E4_RUN_SANDBOX_STRESS=1 to exercise the isolated worker")
    def test_joint_kernel_executes_through_registered_isolated_worker(self):
        raw = {"name": "evolved.joint_alignment_fixture", "version": "1",
            "description": "Generic timestamp correlation over a declared window scope.",
            "family": "joint_window_association", "output_type": "ScoredCandidates",
            "parameters": {}, "authoring_profile": "typed-operator-ir-v1",
            "program": _program("series.align_kpi_both_windows")}
        registry = OperatorRegistry()
        normalized = register_generated_specs(registry, [raw])[0]
        inputs = _typed_inputs()
        output = registry.require(normalized["name"], normalized["version"]).implementation(
            inputs, {}, NodeContext("joint-window-fixture", "worker", normalized["name"],
                RUNTIME, {}, REF, ALT, DATA_SHA))
        true = next(item for item in output.candidates if item.entity.identifier == "component-true")
        self.assertEqual(true.score, 1.0)


if __name__ == "__main__":
    unittest.main()
