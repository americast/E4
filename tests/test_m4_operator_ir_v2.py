"""Versioned V2 IR contracts keep the V1 interpreter ABI replayable."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
import unittest

from e4core.m4_operator_ir import (PROFILE_V2, IR_SCHEMA_V2,
    normalize_operator_ir_spec, typed_operator_ir_v2_schema)
from e4core.m4_operator_ir_worker import PROFILE as PROFILE_V1, execute_ir as execute_ir_v1
from e4core.cache import canonical_json
from e4core.m4_operator_ir import (trusted_runtime_context_v2,
    compact_operator_ir_inputs_v2, operator_ir_log_input, trusted_runtime_context,
    model_facing_operator_ir_v2_schema, model_facing_trusted_runtime_context_v2)
from e4core.m4_operator_ir_v2_worker import execute_ir as execute_ir_v2
from e4core.m4_generated import validate_generated_spec, register_generated_specs
from e4core.executor import NodeContext
from e4core.registry import OperatorRegistry
from e4core.contracts import (OperatorIRLogBlockDenominator, OperatorIRLogInput,
    Aggregate, AggregateRow, Candidate, CandidateSet, EntityRef, MetricPoint,
    PaperLogPatternSummary, PaperLogSummary, PaperStarterInput, Telemetry, Window)
from e4core.bank_metric_policy import classify_bank_metric
REF = Window("reference", 0, 1_800_000)
ALT = Window("alert", 1_800_000, 3_600_000)
DATA_SHA = "c" * 64
SOURCE_HASHES = {"bank/metric_app.csv": "a" * 64,
    "bank/metric_container.csv": "b" * 64}
RUNTIME = {"minimum_support": 12, "minimum_independent_blocks": 6,
    "block_minutes": 1, "absolute_scale_floor": 1e-9}


def _trusted_inputs(*, kpi_support: int = 30, constant_components: bool = False):
    names = ("component-coral", "component-ember", "component-jade")
    def aggregate(window: Window, *, alert: bool) -> Aggregate:
        rows = []
        for index, name in enumerate(names):
            samples = []
            for minute in range(30):
                timestamp = window.start_ms_utc + minute * 60_000
                value = (4.0 if constant_components else
                    10.0 + (minute % 4) + (3.0 if alert and index == 0 else 0.0))
                samples.append((timestamp, value))
            details = {"source_table": "bank/metric_container.csv",
                "source_field": "OSLinux-CPU_CPU_CPUCpuUtil", "metric_kind": "gauge",
                "ranking_eligible": True, "unit": "undeclared native value",
                "bucket_samples": tuple(samples)}
            rows.append(AggregateRow(EntityRef("component", name),
                "bank/metric_container.csv|OSLinux-CPU_CPU_CPUCpuUtil|value",
                window, "median", 12.0, 30, ("bank/metric_container.csv",), (), details))
        if window.name == "reference" or kpi_support > 0:
            count = 30 if window.name == "reference" else kpi_support
            samples = tuple((window.start_ms_utc + minute * 60_000, 100.0 + minute)
                for minute in range(count))
            details = {"source_table": "bank/metric_app.csv", "source_field": "mrt",
                "metric_kind": "gauge", "ranking_eligible": True,
                "unit": "undeclared", "bucket_samples": samples}
            rows.append(AggregateRow(EntityRef("app_kpi", "opaque-kpi"),
                "bank/metric_app.csv|mrt", window, "median", 100.0, len(samples),
                ("bank/metric_app.csv",), (), details))
        return Aggregate(window, tuple(rows), tuple(sorted(SOURCE_HASHES)),
            {"source_hashes": SOURCE_HASHES, "case_data_sha256": DATA_SHA})
    candidates = CandidateSet(tuple(Candidate(EntityRef("component", name), "fixture")
        for name in names), source="analytic fixture")
    return {"reference": aggregate(REF, alert=False),
        "alert": aggregate(ALT, alert=True), "candidates": candidates}


def _program(*, with_kpi: bool):
    nodes = [{"id": "components", "kernel": "series.component_metrics", "version": "1",
        "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                   "candidates": "$input.candidates"}, "params": {}, "bindings": {}}]
    if with_kpi:
        nodes.extend([
            {"id": "kpi", "kernel": "series.observed_kpi", "version": "1",
                "inputs": {"reference": "$input.reference", "alert": "$input.alert"},
                "params": {}, "bindings": {}},
            {"id": "aligned", "kernel": "series.align_kpi_both_windows", "version": "1",
                "inputs": {"components": "components", "kpi": "kpi"}, "params": {}, "bindings": {}},
            {"id": "stat", "kernel": "series.correlate", "version": "1",
                "inputs": {"pairs": "aligned"}, "params": {"method": "spearman"},
                "bindings": {"minimum_pairs": "runtime.minimum_support"}},
        ])
    else:
        nodes.extend([
            {"id": "reduced", "kernel": "series.reduce", "version": "1",
                "inputs": {"series": "components"},
                "params": {"statistic": "median"},
                "bindings": {"block_minutes": "runtime.block_minutes",
                    "minimum_blocks": "runtime.minimum_independent_blocks"}},
            {"id": "stat", "kernel": "numbers.window_compare", "version": "1",
                "inputs": {"values": "reduced"}, "params": {"operation": "difference"},
                "bindings": {}},
        ])
    nodes.extend([
        {"id": "scores", "kernel": "numbers.reduce_component", "version": "1",
            "inputs": {"values": "stat"}, "params": {"method": "max"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": "1",
            "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "answer", "kernel": "scores.assemble", "version": "1",
            "inputs": {"candidates": "$input.candidates", "scores": "scores",
                "evidence": "evidence"}, "params": {}, "bindings": {}},
    ])
    return {"schema_version": IR_SCHEMA_V2, "nodes": nodes, "return": {"node": "answer"}}


def _window_association_program():
    nodes = [
        {"id": "components", "kernel": "series.component_metrics", "version": "1",
            "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
        {"id": "kpi", "kernel": "series.observed_kpi", "version": "1",
            "inputs": {"reference": "$input.reference", "alert": "$input.alert"},
            "params": {}, "bindings": {}},
        {"id": "aligned", "kernel": "series.align_kpi_both_windows", "version": "1",
            "inputs": {"components": "components", "kpi": "kpi"},
            "params": {}, "bindings": {}},
        {"id": "association", "kernel": "series.window_association", "version": "1",
            "inputs": {"pairs": "aligned"}, "params": {"method": "spearman"},
            "bindings": {"minimum_pairs": "runtime.minimum_support"}},
        {"id": "delta", "kernel": "numbers.window_compare", "version": "1",
            "inputs": {"values": "association"}, "params": {"operation": "difference"},
            "bindings": {}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": "1",
            "inputs": {"values": "delta"}, "params": {"method": "max"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": "1",
            "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "answer", "kernel": "scores.assemble", "version": "1",
            "inputs": {"candidates": "$input.candidates", "scores": "scores",
                "evidence": "evidence"}, "params": {}, "bindings": {}},
    ]
    return {"schema_version": IR_SCHEMA_V2, "nodes": nodes, "return": {"node": "answer"}}


def _association_inputs(*, changed_coupling: bool):
    inputs = _trusted_inputs()
    from dataclasses import replace as dc_replace
    result = dict(inputs)
    for window_name in ("reference", "alert"):
        aggregate = inputs[window_name]
        rows = []
        for row in aggregate.rows:
            details = dict(row.details)
            if (row.entity.identifier == "component-coral" and
                    details.get("source_field") == "OSLinux-CPU_CPU_CPUCpuUtil"):
                samples = []
                for index in range(30):
                    timestamp = (REF if window_name == "reference" else ALT).start_ms_utc + index * 60_000
                    coupled = window_name == "alert" or not changed_coupling
                    value = float(index if coupled else (index * 7) % 30)
                    samples.append((timestamp, value))
                details["bucket_samples"] = tuple(samples)
            rows.append(dc_replace(row, details=details))
        result[window_name] = dc_replace(aggregate, rows=tuple(rows))
    return result


def _spec(program):
    return {"name": "evolved.support_probe", "version": "v2",
        "description": "Probe computation-specific availability", "family": "metric",
        "output_type": "ScoredCandidates", "parameters": {},
        "authoring_profile": PROFILE_V2, "program": program}


def _v1_spec():
    nodes = [
        {"id": "components", "kernel": "series.component_metrics", "version": "1",
            "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
        {"id": "summaries", "kernel": "series.reduce", "version": "1",
            "inputs": {"series": "components"}, "params": {"statistic": "variance"},
            "bindings": {"block_minutes": "runtime.block_minutes",
                "minimum_blocks": "runtime.minimum_independent_blocks"}},
        {"id": "change", "kernel": "numbers.window_compare", "version": "1",
            "inputs": {"values": "summaries"}, "params": {"operation": "difference"},
            "bindings": {}},
        {"id": "magnitude", "kernel": "numbers.absolute", "version": "1",
            "inputs": {"values": "change"}, "params": {}, "bindings": {}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": "1",
            "inputs": {"values": "magnitude"}, "params": {"method": "max"},
            "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": "1",
            "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "answer", "kernel": "scores.assemble", "version": "1",
            "inputs": {"candidates": "$input.candidates", "scores": "scores",
                "evidence": "evidence"}, "params": {}, "bindings": {}},
    ]
    return {"name": "evolved.legacy_variance", "version": "v1",
        "description": "Legacy V1 source-free replay fixture", "family": "metric",
        "output_type": "ScoredCandidates", "parameters": {},
        "authoring_profile": "typed-operator-ir-v1",
        "program": {"schema_version": "e4-typed-operator-ir/1", "nodes": nodes,
            "return": {"node": "answer"}}}


def _log_program(pattern_sha256: str, *, statistic: str = "mean_rate"):
    nodes = [
        {"id": "patterns", "kernel": "logs.pattern_counts", "version": "1",
            "inputs": {"log_input": "$input.starter_input",
                "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
        {"id": "selected", "kernel": "logs.select_pattern", "version": "1",
            "inputs": {"counts": "patterns"},
            "params": {"pattern_sha256": pattern_sha256, "log_name": "service_log"},
            "bindings": {}},
        {"id": "rates", "kernel": "logs.exposure_rate", "version": "1",
            "inputs": {"counts": "selected"}, "params": {"statistic": statistic},
            "bindings": {"minimum_blocks": "runtime.minimum_independent_blocks"}},
        {"id": "delta", "kernel": "numbers.window_compare", "version": "1",
            "inputs": {"values": "rates"}, "params": {"operation": "difference"},
            "bindings": {}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": "1",
            "inputs": {"values": "delta"}, "params": {"method": "max"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": "1",
            "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "answer", "kernel": "scores.assemble", "version": "1",
            "inputs": {"candidates": "$input.candidates", "scores": "scores",
                "evidence": "evidence"}, "params": {}, "bindings": {}},
    ]
    return {"schema_version": IR_SCHEMA_V2, "nodes": nodes, "return": {"node": "answer"}}


def _exceedance_program(*, sigma: bool = False, selector_ref: str | None = None):
    exceedance = ({"id": "exceed", "kernel": "series.sigma_exceedance", "version": "1",
        "inputs": {"series": "normalized"}, "params": {"sigma_multiple": 2.0}, "bindings": {}}
        if sigma else {"id": "exceed", "kernel": "series.reference_quantile_exceedance", "version": "1",
            "inputs": {"series": "normalized"}, "params": {},
            "bindings": {"tail_quantile": "runtime.tail_quantile"}})
    nodes = [
        {"id": "components", "kernel": "series.component_metrics", "version": "1",
            "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
    ]
    series_ref = "components"
    if selector_ref is not None:
        nodes.append({"id": "selected", "kernel": "series.select_metric_rule", "version": "1",
            "inputs": {"series": "components"}, "params": {"selector_ref": selector_ref},
            "bindings": {}})
        series_ref = "selected"
    nodes.extend([
        {"id": "normalized", "kernel": "series.normalize_reference_scale", "version": "1",
            "inputs": {"series": series_ref}, "params": {}, "bindings": {
                "block_minutes": "runtime.block_minutes",
                "minimum_blocks": "runtime.minimum_independent_blocks",
                "relative_scale_floor": "runtime.relative_scale_floor",
                "absolute_scale_floor": "runtime.absolute_scale_floor"}},
        exceedance,
        {"id": "delta", "kernel": "numbers.window_compare", "version": "1",
            "inputs": {"values": "exceed"}, "params": {"operation": "difference"}, "bindings": {}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": "1",
            "inputs": {"values": "delta"}, "params": {"method": "max"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": "1",
            "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "answer", "kernel": "scores.assemble", "version": "1",
            "inputs": {"candidates": "$input.candidates", "scores": "scores",
                "evidence": "evidence"}, "params": {}, "bindings": {}},
    ])
    return {"schema_version": IR_SCHEMA_V2, "nodes": nodes, "return": {"node": "answer"}}


def _metric_input_with_burst(*, scale: float = 1.0):
    inputs = _trusted_inputs()
    from dataclasses import replace as dc_replace
    output = dict(inputs)
    for window_name in ("reference", "alert"):
        aggregate = inputs[window_name]
        rows = []
        for row in aggregate.rows:
            details = dict(row.details)
            if (row.entity.identifier == "component-coral" and
                    details.get("source_field") == "OSLinux-CPU_CPU_CPUCpuUtil"):
                samples = []
                for index in range(30):
                    timestamp = (REF if window_name == "reference" else ALT).start_ms_utc + index * 60_000
                    value = 10.0 * scale
                    if window_name == "alert" and index >= 27:
                        value = 20.0 * scale
                    samples.append((timestamp, value))
                details["bucket_samples"] = tuple(samples)
            rows.append(dc_replace(row, details=details))
        output[window_name] = dc_replace(aggregate, rows=tuple(rows))
    return output


def _mixed_native_unit_inputs(*, changed: bool):
    """Two allowlisted native units per candidate, with optional window shifts."""
    from dataclasses import replace as dc_replace
    inputs = _trusted_inputs()
    output = dict(inputs)
    rate_field = "OSLinux-OSLinux_LOCALDISK_LOCALDISK-sda_DSKBps"
    for side in ("reference", "alert"):
        aggregate = inputs[side]
        rows = []
        for row in aggregate.rows:
            if row.entity.role != "component":
                rows.append(row)
                continue
            entity = row.entity.identifier
            details = dict(row.details)
            samples = tuple((stamp, value) for stamp, value in details["bucket_samples"])
            if side == "alert" and not changed:
                samples = tuple((stamp, value - (3.0 if entity == "component-coral" else 0.0))
                    for stamp, value in samples)
                details["bucket_samples"] = samples
                row = dc_replace(row, details=details)
            rows.append(row)
            rate_samples = tuple((aggregate.window.start_ms_utc + minute * 60_000,
                100.0 + minute % 4 + (3.0 if changed and side == "alert" and
                    entity == "component-coral" else 0.0)) for minute in range(30))
            rows.append(AggregateRow(row.entity,
                f"bank/metric_container.csv|{rate_field}|value", aggregate.window,
                "median", 101.5, len(rate_samples), ("bank/metric_container.csv",), (),
                {"source_table": "bank/metric_container.csv", "source_field": rate_field,
                    "metric_kind": "rate", "ranking_eligible": True,
                    "unit": "rate-like by field suffix; exact unit undeclared",
                    "bucket_samples": rate_samples}))
        output[side] = dc_replace(aggregate, rows=tuple(rows))
    return output


def _log_inputs(*, positive: bool, burst: bool = False):
    component = "component-coral"
    log_name = "service_log"
    block_ms = 60_000
    source_hash = "d" * 64
    tokens = ("connection", "refused", "503")
    pattern_sha = hashlib.sha256("\0".join(tokens).encode()).hexdigest()
    summaries = (
        PaperLogSummary(component, log_name, REF, 120, 400, (source_hash,)),
        PaperLogSummary(component, log_name, ALT, 240, 400, (source_hash,)))
    denominators = []
    patterns = []
    for window, total, count in ((REF, 4, 1),
            (ALT, 8, 4 if positive else 2)):
        for minute in range(30):
            block_count = count
            if burst:
                block_count = (1 if minute < 6 else 0) if window.name == "reference" else (
                    4 if minute < 3 else 0)
            start = window.start_ms_utc + minute * block_ms
            denominators.append(OperatorIRLogBlockDenominator(component, log_name,
                window, start, total))
            if block_count:
                patterns.append(PaperLogPatternSummary(component, log_name, window, start,
                    block_ms, tokens, pattern_sha, block_count, total, 400, (source_hash,)))
    count_semantics = classify_bank_metric("bank/log/log_service.csv", "log_volume_count").to_dict()
    count_semantics["analysis_block_duration_ms"] = block_ms
    log_input = OperatorIRLogInput("available", True, block_ms, summaries,
        tuple(patterns), tuple(denominators), count_semantics, (source_hash,), "f" * 64)
    log_metrics = tuple(MetricPoint("bank/log/log_service.csv", row.block_start_ms_utc,
        EntityRef("component", row.component_id), "log_volume_count", "bank.log_service",
        row.log_name, float(row.row_count), str(row.row_count),
        f"{row.window.name}:{row.block_start_ms_utc}") for row in denominators)
    telemetry = Telemetry("v2-log-fixture", REF, ALT, log_metrics, (), {},
        "f" * 64, {"source_dates/fixture/log/log_service.csv": source_hash}, {}, {},
        "ir-v2-log-fixture/1")
    starter = PaperStarterInput(telemetry, None, None,
        {"sessions": "unsupported", "urls": "unsupported", "native_logs": "available",
         "native_log_patterns": "available"}, block_ms, summaries, tuple(patterns))
    candidates = _trusted_inputs()["candidates"]
    from e4core.cache import canonical_json
    wire = {"starter_input": json.loads(canonical_json(operator_ir_log_input(starter)).decode("utf-8")),
        "candidates": json.loads(canonical_json(candidates).decode("utf-8"))}
    return wire, pattern_sha, starter


class OperatorIRV2ContractTests(unittest.TestCase):
    def test_v2_static_errors_name_required_ports_and_runtime_bindings(self):
        bad_input = _program(with_kpi=False)
        bad_input["nodes"][0]["inputs"]["reference"] = "reference"
        with self.assertRaisesRegex(ValueError, r"\$input\.reference"):
            normalize_operator_ir_spec(_spec(bad_input))

        bad_binding = _program(with_kpi=True)
        bad_binding["nodes"][3]["bindings"] = {"minimum_pairs": "runtime.minimum_pairs"}
        with self.assertRaisesRegex(ValueError,
                r"bindings\.minimum_pairs=runtime\.minimum_support"):
            normalize_operator_ir_spec(_spec(bad_binding))

    def test_v2_schema_documents_authoring_reference_and_window_grammar(self):
        schema = model_facing_operator_ir_v2_schema()
        rules = schema["authoring_rules"]
        self.assertIn("$input.reference", rules["external_references"])
        self.assertIn("prior node id", rules["external_references"])
        self.assertIn("params.window", rules["window_parameters"])

    def test_v2_univariate_requirements_do_not_require_application_kpi(self):
        normalized = normalize_operator_ir_spec(_spec(_program(with_kpi=False)))
        requirements = normalized["input_requirements"]
        assert "metric/metric_container.csv" in requirements["reference"]["source_columns"]
        assert "metric/metric_app.csv" not in requirements["reference"]["source_columns"]
        assert "metric/metric_app.csv" not in requirements["alert"]["source_columns"]
        assert normalized["authoring_profile"] == PROFILE_V2
        assert normalized["program"]["schema_version"] == IR_SCHEMA_V2


    def test_v2_kpi_association_adds_app_requirements_only_when_reachable(self):
        normalized = normalize_operator_ir_spec(_spec(_program(with_kpi=True)))
        requirements = normalized["input_requirements"]
        assert requirements["reference"]["source_columns"]["metric/metric_app.csv"] == [
            "timestamp", "tc", "mrt"]
        assert requirements["alert"]["source_columns"]["metric/metric_app.csv"] == [
            "timestamp", "tc", "mrt"]
        expected_groups = {"metric/metric_container.csv": ["component"],
            "metric/metric_app.csv": ["app_kpi"]}
        assert requirements["reference"]["group_by"] == []
        assert requirements["alert"]["group_by"] == []
        assert requirements["reference"]["group_by_by_table"] == expected_groups
        assert requirements["alert"]["group_by_by_table"] == expected_groups
        assert normalized["policy_pins"]["primary_kpi_policy_sha256"]
        assert normalized["policy_pins"]["typed_operator_ir_v2_policy_sha256"]

    def test_v2_missing_per_table_group_is_rejected_by_host_normalizer(self):
        spec = _spec(_program(with_kpi=True))
        normalized = normalize_operator_ir_spec(spec)
        malformed = {**_spec(_program(with_kpi=True)),
            "input_ports": normalized["input_ports"],
            "program": normalized["program"],
            "input_requirements": json.loads(json.dumps(normalized["input_requirements"]))}
        malformed["input_requirements"]["reference"]["group_by_by_table"].pop(
            "metric/metric_container.csv")
        with self.assertRaisesRegex(ValueError, "input requirements do not match"):
            normalize_operator_ir_spec(malformed)

    def test_v2_log_ir_has_a_conditional_typed_full_window_summary_port(self):
        pattern_sha = "a" * 64
        normalized = validate_generated_spec(_spec(_log_program(pattern_sha)))
        ports = {row["name"]: row for row in normalized["input_ports"]}
        assert ports["starter_input"]["type"] == "PaperStarterInput"
        assert ports["starter_input"]["required"] is True
        assert ports["reference"]["required"] is False
        assert "log/log_service.csv" in normalized["input_requirements"][
            "starter_input"]["source_columns"]
        assert "metric/metric_app.csv" not in normalized["input_requirements"][
            "reference"]["source_columns"]

    def test_log_pattern_exposure_is_volume_normalized_and_keeps_full_window_denominators(self):
        positive_inputs, pattern_sha, positive_starter = _log_inputs(positive=True)
        volume_inputs, _, volume_starter = _log_inputs(positive=False)
        positive = execute_ir_v2(_log_program(pattern_sha), positive_inputs, {},
            {"absolute_scale_floor": 1e-9, "minimum_independent_blocks": 6},
            trusted_runtime_context_v2(inputs={"starter_input": operator_ir_log_input(positive_starter)}))
        volume = execute_ir_v2(_log_program(pattern_sha), volume_inputs, {},
            {"absolute_scale_floor": 1e-9, "minimum_independent_blocks": 6},
            trusted_runtime_context_v2(inputs={"starter_input": operator_ir_log_input(volume_starter)}))
        def rates(result):
            trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
            row = next(item for item in trace if item["kernel"] == "logs.exposure_rate")
            pattern = row["availability"]["support_details"]["patterns"][0]
            return pattern["reference_exposure_rate"], pattern["alert_exposure_rate"], pattern
        positive_reference, positive_alert, positive_meta = rates(positive)
        volume_reference, volume_alert, volume_meta = rates(volume)
        assert positive_reference == 0.25 and positive_alert == 0.5
        assert volume_reference == volume_alert == 0.25
        assert positive_meta["reference_total_rows"] == 120
        assert positive_meta["alert_total_rows"] == 240
        assert positive_meta["reference_exact_blocks"] == 30
        assert positive_meta["alert_exact_blocks"] == 30
        assert positive_meta["numeric_unit"] == "dimensionless"
        assert positive_meta["source_unit"] == "fraction of full-window source log rows"
        assert positive_meta["support_counts"] == {
            "reference_window_independent_time_blocks": 30,
            "alert_window_independent_time_blocks": 30}
        assert positive["$fields"]["status"] == "ready"  # descriptive composition may be ranked with its support shown
        assert volume["$fields"]["status"] == "abstained"

    def test_log_block_variance_exposes_burstiness_even_when_window_rate_matches(self):
        wire, pattern_sha, starter = _log_inputs(positive=False, burst=True)
        result = execute_ir_v2(_log_program(pattern_sha, statistic="variance_rate"),
            wire, {}, {"minimum_independent_blocks": 6, "absolute_scale_floor": 1e-9},
            trusted_runtime_context_v2(inputs={"starter_input": operator_ir_log_input(starter)}))
        trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
        details = next(row["availability"]["support_details"]["patterns"][0]
            for row in trace if row["kernel"] == "logs.exposure_rate")
        self.assertEqual(details["reference_exposure_rate"], details["alert_exposure_rate"])
        self.assertEqual(details["statistic"], "variance_rate")
        self.assertGreater(details["reference_statistic_value"], 0.0)
        self.assertGreater(details["alert_statistic_value"],
            details["reference_statistic_value"])
        self.assertEqual(details["ranking_state"], "unknown_until_declared_uncertainty_test")

    def test_log_projection_is_typed_full_window_and_contains_no_raw_lines(self):
        block_ms = 60_000
        source_hash = "e" * 64
        tokens = ("connection", "refused", "503")
        pattern_sha = hashlib.sha256("\0".join(tokens).encode()).hexdigest()
        metrics = tuple(MetricPoint("bank/log/log_service.csv", window.start_ms_utc + minute * block_ms,
                EntityRef("component", "component-coral"), "log_volume_count", "bank.log_service",
                "service_log", 4.0, "4", f"{window.name}:{minute}")
            for window in (REF, ALT) for minute in range(30))
        telemetry = Telemetry("projection-fixture", REF, ALT, metrics, (), {},
            "f" * 64, {}, {}, {}, "ir-log-projection/1")
        summaries = tuple(PaperLogSummary("component-coral", "service_log", window,
            120, 400, (source_hash,)) for window in (REF, ALT))
        patterns = tuple(PaperLogPatternSummary("component-coral", "service_log", window,
            window.start_ms_utc, block_ms, tokens, pattern_sha, 1, 4, 400, (source_hash,))
            for window in (REF, ALT))
        starter = PaperStarterInput(telemetry, None, None,
            {"sessions": "unsupported", "urls": "unsupported",
             "native_logs": "available", "native_log_patterns": "available"},
            block_ms, summaries, patterns)
        wire = compact_operator_ir_inputs_v2({**_trusted_inputs(), "starter_input": starter},
            _log_program(pattern_sha))
        log_input = wire["starter_input"]
        self.assertIsInstance(log_input, OperatorIRLogInput)
        self.assertEqual(log_input.status, "available")
        self.assertEqual(len(log_input.block_denominators), 60)
        serialized = canonical_json(log_input).decode("utf-8")
        self.assertIn(pattern_sha, serialized)
        self.assertNotIn('"value":"', serialized)
        self.assertNotIn("raw log text", serialized)

    def test_unsupported_log_input_abstention_origin_survives_downstream_kernels(self):
        wire, pattern_sha, _starter = _log_inputs(positive=True)
        from e4core.cache import deserialize_contract_json
        log_input = deserialize_contract_json(wire["starter_input"], "OperatorIRLogInput")
        unsupported = replace(log_input, status="unsupported", complete=False, patterns=())
        inputs = {"starter_input": json.loads(canonical_json(unsupported).decode()),
            "candidates": wire["candidates"]}
        result = execute_ir_v2(_log_program(pattern_sha), inputs, {},
            {"absolute_scale_floor": 1e-9, "minimum_independent_blocks": 6},
            trusted_runtime_context_v2())
        trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
        by_kernel = {row["kernel"]: row["availability"] for row in trace}
        origin = by_kernel["logs.pattern_counts"]["upstream_abstention"]
        self.assertEqual(origin["code"], "native_log_pattern_summary_unsupported_or_incomplete")
        self.assertEqual(by_kernel["logs.select_pattern"]["upstream_abstention"]["code"], origin["code"])
        self.assertEqual(by_kernel["logs.exposure_rate"]["upstream_abstention"]["code"], origin["code"])

    def test_reference_scale_and_empirical_exceedance_are_scale_invariant_and_explicit(self):
        def run(inputs, *, sigma=False):
            trusted = trusted_runtime_context_v2(inputs=inputs)
            wire = {key: json.loads(canonical_json(value).decode("utf-8"))
                for key, value in inputs.items()}
            runtime = {**RUNTIME, "tail_quantile": 0.9,
                "relative_scale_floor": 0.01, "absolute_scale_floor": 1e-9}
            return execute_ir_v2(_exceedance_program(sigma=sigma), wire, {}, runtime, trusted)

        base = run(_metric_input_with_burst())
        scaled = run(_metric_input_with_burst(scale=100.0))

        def component_result(result, kernel="series.reference_quantile_exceedance"):
            trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
            row = next(item for item in trace if item["kernel"] == kernel)
            found = next(item for item in row["availability"]["support_details"]["series_results"]
                if item["component_id"] == "component-coral")
            return {**found, **found["exceedance_details"]}

        base_row, scaled_row = component_result(base), component_result(scaled)
        self.assertEqual(base_row["alert_exceedance_rate"], 0.1)
        self.assertEqual(base_row["reference_exceedance_rate"], 0.0)
        self.assertEqual(scaled_row["alert_exceedance_rate"], base_row["alert_exceedance_rate"])
        self.assertEqual(base_row["ranking_state"], "unknown")
        self.assertEqual(base_row["computable"], True)

        sigma_result = component_result(run(_metric_input_with_burst(), sigma=True),
            kernel="series.sigma_exceedance")
        self.assertIsNotNone(sigma_result["thresholds"])
        self.assertIn("parametric_sigma_exceedance", sigma_result["reason"])

    def test_v2_metric_selector_uses_stable_ref_and_exact_case_resolution(self):
        from e4core.bank_metric_policy import load_bank_metric_policy
        inputs = _metric_input_with_burst()
        policy = load_bank_metric_policy()
        selector = next(row.selector_ref for row in
            policy.selector_catalog("bank/metric_container.csv")
            if row.exact_field_name == "OSLinux-CPU_CPU_CPUCpuUtil")
        spec = normalize_operator_ir_spec(_spec(_exceedance_program(selector_ref=selector)))
        catalog = typed_operator_ir_v2_schema()["metric_selector_catalog"]
        self.assertIn(selector, {row["selector_ref"] for row in catalog})
        wire = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in inputs.items()}
        result = execute_ir_v2(spec["program"], wire, {}, {
            **RUNTIME, "tail_quantile": 0.9, "relative_scale_floor": 0.01,
            "absolute_scale_floor": 1e-9},
            trusted_runtime_context_v2(inputs=inputs))
        trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
        selection = next(row["availability"] for row in trace
            if row["kernel"] == "series.select_metric_rule")
        self.assertEqual(selection["support_details"]["selector_ref"], selector)
        self.assertEqual(selection["support_details"]["matched_field_count"], 1)
        self.assertEqual(selection["support_details"]["selector_resolution"]["status"],
            "resolved_one")

    def test_v2_metric_selector_keeps_every_resolved_match_without_first_match(self):
        from e4core.m4_operator_ir_v2_worker import _select_metric_rule
        from e4core.bank_metric_policy import load_bank_metric_policy
        policy = load_bank_metric_policy()
        selector_ref = next(item.selector_ref for item in
            policy.selector_catalog("bank/metric_container.csv")
            if item.rule_id == "bank.redis.info.used_memory")
        case_sha = "b" * 64
        fields = ("redis-Redis_1_Redis  (used_memory)",
            "redis-Redis_2_Redis  (used_memory)")
        resolution = policy.resolve_selector(selector_ref,
            "bank/metric_container.csv", fields, case_data_sha256=case_sha).to_dict()
        self.assertEqual(resolution["status"], "resolved_multiple")
        series = {"status": "ready", "rows": [{"key": ("component", f"component-{index}", field),
            "reference": {"source_field": field}, "alert": {"source_field": field}}
            for index, field in enumerate(fields)]}
        trusted = {"bank_metric_policy_sha256": policy.sha256,
            "case_data_sha256": case_sha,
            "metric_selector_resolutions": {selector_ref: resolution}}
        result = _select_metric_rule(series, selector_ref, trusted)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["matched_field_count"], 2)
        self.assertEqual({row["reference"]["source_field"] for row in result["rows"]}, set(fields))

    def test_generated_spec_validator_round_trips_the_versioned_v2_signature(self):
        normalized = validate_generated_spec(_spec(_program(with_kpi=False)))
        reloaded = validate_generated_spec(normalized)
        assert reloaded == normalized
        assert reloaded["authoring_profile"] == PROFILE_V2
        assert reloaded["interpreter_sha256"]
        assert reloaded["kernel_abi_sha256"]

    def test_v2_library_snapshot_reloads_with_versioned_worker_and_policy_pins(self):
        from e4core.m4_library import (create_library_snapshot,
            generated_specs_from_snapshot, load_library_snapshot)
        from e4core.m4_operator_ir import normalize_operator_ir_spec
        from tests.test_m4_library import builtins
        spec = _spec(_program(with_kpi=False))
        normalized = normalize_operator_ir_spec(spec)
        with tempfile.TemporaryDirectory(prefix="m4-ir-v2-library-") as directory:
            snapshot = create_library_snapshot({"examples": {}}, [spec], [],
                {"source": "IR V2 replay fixture"}, Path(directory), builtin_specs=builtins())
            loaded = load_library_snapshot(snapshot.path)
            restored = generated_specs_from_snapshot(loaded)
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0]["authoring_profile"], PROFILE_V2)
        self.assertEqual(restored[0]["program"], normalized["program"])
        self.assertEqual(restored[0]["kernel_abi_sha256"], normalized["kernel_abi_sha256"])

    def test_model_facing_v2_projection_preserves_all_legal_schema_and_interns_host_context(self):
        full = typed_operator_ir_v2_schema()
        projected = model_facing_operator_ir_v2_schema()
        self.assertEqual(projected["schema_version"], full["schema_version"])
        self.assertEqual(projected["authoring_profile"], full["authoring_profile"])
        self.assertEqual(projected["spec_fields"], {
            key: value for key, value in full["spec_fields"].items()
            if key not in {"input_ports", "input_requirements"}})
        self.assertEqual(set(projected["spec_fields"]), {
            "name", "version", "description", "family", "output_type",
            "parameters", "authoring_profile", "program"})
        self.assertEqual(projected["fixed_host_abi"]["input_ports"],
            full["fixed_host_abi"]["input_ports"])
        external_log = projected["fixed_host_abi"]["optional_log_port"]
        self.assertEqual(external_log["name"], "starter_input")
        self.assertEqual(external_log["type"], "PaperStarterInput")
        self.assertEqual(external_log["kernel_input_type"], "OperatorIRLogInput")
        self.assertIn("host converts", external_log["host_projection"])
        self.assertNotIn("kernel_input_type", full["fixed_host_abi"]["optional_log_port"])
        self.assertEqual(projected["kernels"]["logs.pattern_counts@1"]["inputs"]["log_input"],
            external_log["kernel_input_type"])
        self.assertEqual(set(projected["kernels"]), set(full["kernels"]))
        for identity, full_kernel in full["kernels"].items():
            projected_kernel = projected["kernels"][identity]
            self.assertEqual(projected_kernel["inputs"], full_kernel["inputs"], identity)
            self.assertEqual(projected_kernel["output"], full_kernel["output"], identity)
            if identity != "series.select_metric_rule@1":
                self.assertEqual(projected_kernel["params"], full_kernel["params"], identity)
        full_selectors = {row["selector_ref"]: row["description"]
            for row in full["metric_selector_catalog"]}
        prompt_selectors = {row["selector_ref"]: row["description"]
            for row in projected["metric_selector_catalog"]}
        self.assertEqual(prompt_selectors, full_selectors)
        self.assertEqual(projected["metric_selector_catalog_source_table"],
            "bank/metric_container.csv")
        selector_param = projected["kernels"]["series.select_metric_rule@1"][
            "params"]["selector_ref"]
        self.assertEqual(selector_param["choices_ref"], "metric_selector_catalog")
        self.assertNotIn("choices", selector_param)
        self.assertEqual(projected["support_semantics"], full["support_semantics"])
        self.assertEqual(projected["log_summary_semantics"], full["log_summary_semantics"])
        self.assertLess(len(canonical_json(projected)), len(canonical_json(full)))

        trusted = trusted_runtime_context_v2()
        prompt_context = model_facing_trusted_runtime_context_v2(trusted)
        self.assertEqual(prompt_context["primary_metric_allowlist"], ["metric_app.mrt"])
        self.assertEqual(prompt_context["primary_kpi_minimum_support"],
            trusted["primary_kpi_minimum_support"])
        self.assertEqual(prompt_context["metric_selector_catalog_ref"],
            "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog")
        self.assertEqual(prompt_context["metric_selector_resolutions_ref"],
            "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog[*].support_profile_ref -> selector_case_resolution_profiles.profiles[*].case_resolution")
        self.assertEqual(prompt_context["semantic_profile_catalog_ref"],
            "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.selector_semantic_profiles")
        for host_only in ("metric_selector_catalog", "metric_selector_catalog_by_table",
                          "metric_selector_resolutions", "typed_operator_ir_v2_policy",
                          "observed_metric_semantics", "v2_base_worker_sha256"):
            self.assertNotIn(host_only, prompt_context)
        self.assertLess(len(canonical_json(prompt_context)), 1000)


    def test_v1_interpreter_and_spec_abi_remain_pinned(self):
        normalized = normalize_operator_ir_spec(_v1_spec())
        worker_path = Path(__import__("e4core.m4_operator_ir_worker",
            fromlist=["__file__"]).__file__)
        worker_sha = hashlib.sha256(worker_path.read_bytes()).hexdigest()
        assert normalized["authoring_profile"] == PROFILE_V1
        assert normalized["interpreter_sha256"] == worker_sha
        assert all("group_by_by_table" not in requirement
            for requirement in normalized["input_requirements"].values())

    def test_v2_dispatch_restores_v1_module_state_after_direct_execution(self):
        inputs = _trusted_inputs()
        wire = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in inputs.items()}
        execute_ir_v2(_program(with_kpi=False), wire, {}, RUNTIME,
            trusted_runtime_context_v2())

        legacy = normalize_operator_ir_spec(_v1_spec())
        result = execute_ir_v1(legacy["program"], wire, {}, RUNTIME,
            trusted_runtime_context())
        self.assertEqual(result["$fields"]["scorer"], "typed-operator-ir-v1")

    def test_policy_config_mutations_change_cache_fingerprint_and_v1_remains_explicit(self):
        from e4core.cache import implementation_fingerprint

        root = Path(__file__).resolve().parents[1]
        base = implementation_fingerprint()
        original_read_bytes = Path.read_bytes
        for relative in ("config/bank_metric_kind_policy.json",
                         "config/typed_operator_ir_v2_policy.json"):
            target = (root / relative).resolve()

            def altered_read_bytes(path, *, _target=target):
                content = original_read_bytes(path)
                return content + b"\n " if path.resolve() == _target else content

            with self.subTest(config=relative), patch.object(Path, "read_bytes", altered_read_bytes):
                self.assertNotEqual(implementation_fingerprint(), base)

        legacy = normalize_operator_ir_spec(_v1_spec())
        worker_path = Path(__import__("e4core.m4_operator_ir_worker",
            fromlist=["__file__"]).__file__)
        self.assertEqual(legacy["authoring_profile"], PROFILE_V1)
        self.assertEqual(legacy["interpreter_sha256"], hashlib.sha256(
            worker_path.read_bytes()).hexdigest())
        v2_program = _program(with_kpi=False)
        inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in _trusted_inputs().items()}
        with self.assertRaisesRegex(ValueError, "unsupported or malformed IR program"):
            execute_ir_v1(v2_program, inputs, {}, RUNTIME, trusted_runtime_context())

    def test_v2_shared_worker_bundle_preserves_availability_trace(self):
        inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in _trusted_inputs().items()}
        result = execute_ir_v2(_program(with_kpi=False), inputs, {}, RUNTIME,
            trusted_runtime_context_v2())
        assert result["$fields"]["scorer"] == "typed-operator-ir-v2"
        trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
        assert [row["kernel"] for row in trace] == [
            "series.component_metrics", "series.reduce", "numbers.window_compare",
            "numbers.reduce_component", "evidence.from_scores", "scores.assemble"]
        assert all(row["availability"]["ranking_state"] == "unknown" for row in trace)

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
        "run in the approved restricted-worker namespace with E4_RUN_SANDBOX_STRESS=1")
    def test_v2_executes_through_the_actual_restricted_worker_bundle(self):
        registry = OperatorRegistry()
        normalized = register_generated_specs(registry, [_spec(_program(with_kpi=False))])[0]
        operator = registry.require(normalized["name"], normalized["version"])
        result = operator.implementation(_trusted_inputs(), {}, NodeContext(
            "analytic-fixture", "ir-v2-worker", normalized["name"], RUNTIME, {},
            REF, ALT, DATA_SHA))
        self.assertEqual(result.scorer, "typed-operator-ir-v2")
        self.assertIn("operator_ir_availability", result.status_details)

        from e4core.bank_metric_policy import load_bank_metric_policy
        selector_ref = next(item.selector_ref for item in
            load_bank_metric_policy().selector_catalog("bank/metric_container.csv")
            if item.exact_field_name == "OSLinux-CPU_CPU_CPUCpuUtil")
        selector_registry = OperatorRegistry()
        selected_spec = register_generated_specs(selector_registry,
            [_spec(_exceedance_program(selector_ref=selector_ref))])[0]
        selected_operator = selector_registry.require(selected_spec["name"], selected_spec["version"])
        selected_result = selected_operator.implementation(_metric_input_with_burst(), {}, NodeContext(
            "analytic-fixture", "ir-v2-selector-worker", selected_spec["name"],
            {**RUNTIME, "tail_quantile": 0.9, "relative_scale_floor": 0.01},
            {}, REF, ALT, DATA_SHA))
        selector_trace = selected_result.status_details["operator_ir_availability"]
        selector_node = next(row for row in selector_trace
            if row["kernel"] == "series.select_metric_rule")
        self.assertEqual(selector_node["availability"]["support_details"]["matched_field_count"], 1)

        log_wire, pattern_sha, starter = _log_inputs(positive=True)
        log_spec = _spec(_log_program(pattern_sha))
        log_registry = OperatorRegistry()
        normalized_log = register_generated_specs(log_registry, [log_spec])[0]
        log_operator = log_registry.require(normalized_log["name"], normalized_log["version"])
        from e4core.cache import deserialize_contract_json
        log_inputs = {"starter_input": starter,
            "candidates": deserialize_contract_json(log_wire["candidates"], "CandidateSet")}
        log_result = log_operator.implementation(log_inputs, {}, NodeContext(
            "analytic-fixture", "ir-v2-log-worker", normalized_log["name"],
            {"absolute_scale_floor": 1e-9, "minimum_independent_blocks": 6},
            {}, REF, ALT, DATA_SHA))
        self.assertEqual(log_result.scorer, "typed-operator-ir-v2")
        self.assertIn("operator_ir_availability", log_result.status_details)

    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
        "run in the approved restricted-worker namespace with E4_RUN_SANDBOX_STRESS=1")
    def test_v2_dimensionless_derived_units_reduce_mixed_native_fields_in_bwrap(self):
        from e4core.m4_generated import register_generated_specs

        runtime = {**RUNTIME, "relative_scale_floor": 0.01, "tail_quantile": 0.9}

        def run(program, inputs, task_id):
            registry = OperatorRegistry()
            spec = register_generated_specs(registry, [_spec(program)])[0]
            operator = registry.require(spec["name"], spec["version"])
            return operator.implementation(inputs, {}, NodeContext(
                "ir-v2-mixed-native-unit-fixture", task_id, spec["name"],
                runtime, {}, REF, ALT, DATA_SHA))

        changed = run(_exceedance_program(sigma=True),
            _mixed_native_unit_inputs(changed=True), "dimensionless-mixed-units")
        coral = next(row for row in changed.candidates
            if row.entity.identifier == "component-coral")
        self.assertGreater(coral.score, 0.0)
        self.assertTrue(coral.evidence)
        trace = changed.status_details["operator_ir_availability"]
        exceedance = next(row["availability"]["support_details"]["series_results"]
            for row in trace if row["kernel"] == "series.sigma_exceedance")
        self.assertTrue(exceedance)
        self.assertEqual(exceedance[0]["numeric_unit"], "dimensionless")
        self.assertIn("alert_window_independent_time_blocks",
            exceedance[0]["support_counts"])
        self.assertEqual(exceedance[0]["support_counts"][
            "alert_window_independent_time_blocks"], 30)

        unchanged = run(_exceedance_program(sigma=True),
            _mixed_native_unit_inputs(changed=False), "dimensionless-no-change")
        self.assertEqual(unchanged.status, "abstained")
        self.assertFalse(any(row.score > 0.0 for row in unchanged.candidates))

        raw_mixed_units = run(_program(with_kpi=False),
            _mixed_native_unit_inputs(changed=True), "raw-mixed-units")
        self.assertEqual(raw_mixed_units.status, "abstained")
        self.assertFalse(any(row.score > 0.0 for row in raw_mixed_units.candidates))

    def test_v2_abstention_reason_survives_alignment_and_correlation(self):
        program = _program(with_kpi=True)
        for node in program["nodes"]:
            if node["kernel"] == "series.observed_kpi":
                node["kernel"] = "series.primary_kpi"
        typed_inputs = _trusted_inputs(kpi_support=11)
        inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in typed_inputs.items()}
        result = execute_ir_v2(program, inputs, {}, RUNTIME,
            trusted_runtime_context_v2(inputs=typed_inputs))
        trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
        by_kernel = {row["kernel"]: row["availability"] for row in trace}
        original = by_kernel["series.primary_kpi"]["upstream_abstention"]
        assert original["code"] == "no_primary_kpi_meets_policy_support"
        support = by_kernel["series.primary_kpi"]["support_details"]["kpi_support_by_candidate"][0]
        assert (support["reference_unique_timestamps"],
                support["alert_unique_timestamps"]) == (30, 11)
        assert by_kernel["series.align_kpi_both_windows"]["upstream_abstention"] == original | {
            "chain": ["series.primary_kpi", "series.align_kpi_both_windows"]}
        assert by_kernel["series.correlate"]["upstream_abstention"]["code"] == original["code"]

    def test_window_association_separates_stable_shared_load_from_new_coupling(self):
        def run(changed_coupling):
            typed = _association_inputs(changed_coupling=changed_coupling)
            wire = {key: json.loads(canonical_json(value).decode("utf-8"))
                for key, value in typed.items()}
            return execute_ir_v2(_window_association_program(), wire, {}, RUNTIME,
                trusted_runtime_context_v2(inputs=typed))

        def coral_association(result):
            trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
            node = next(row for row in trace if row["kernel"] == "series.window_association")
            row = next(item for item in node["availability"]["support_details"]["association_rows"]
                if item["component_id"] == "component-coral")
            return row

        stable = coral_association(run(False))
        changed = coral_association(run(True))
        self.assertAlmostEqual(stable["reference_rho"], 1.0)
        self.assertAlmostEqual(stable["alert_rho"], 1.0)
        self.assertAlmostEqual(stable["correlation_change"], 0.0)
        self.assertLess(changed["reference_rho"], 0.2)
        self.assertAlmostEqual(changed["alert_rho"], 1.0)
        self.assertGreater(changed["correlation_change"], 0.8)
        self.assertEqual(changed["confidence_state"], "not_assessed_by_this_kernel")
        self.assertEqual(changed["causal_attribution"], "not_established")
        self.assertEqual(changed["numeric_unit"], "dimensionless")
        self.assertEqual(set(changed["source_units"]), {"component", "kpi"})
        self.assertEqual(changed["support_counts"], {
            "reference_window_exact_pairs": 30, "alert_window_exact_pairs": 30})

    def test_complete_v2_prompt_example_scores_mixed_native_units_dimensionlessly(self):
        from e4core.cache import canonical_json
        from e4core.m4_controller import _PAPER_V2_AUTHORING_EXAMPLE
        from e4core.m4_operator_ir import normalize_operator_ir_spec
        from e4core.m4_operator_ir_v2_worker import execute_ir as execute_ir_v2
        from e4core.m4_operator_ir import trusted_runtime_context_v2

        spec = normalize_operator_ir_spec(_PAPER_V2_AUTHORING_EXAMPLE)
        typed_inputs = _mixed_native_unit_inputs(changed=True)
        wire_inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in typed_inputs.items()}
        runtime = {**RUNTIME, "absolute_scale_floor": 1e-9,
            "minimum_independent_blocks": 6, "block_minutes": 1}
        result = execute_ir_v2(spec["program"], wire_inputs, {}, runtime,
            trusted_runtime_context_v2(inputs=typed_inputs))
        self.assertEqual(result["$fields"]["status"], "ready")
        candidates = result["$fields"]["candidates"]["$tuple"]
        coral = next(row["$fields"] for row in candidates
            if row["$fields"]["entity"]["$fields"]["identifier"] == "component-coral")
        self.assertGreater(coral["score"], 0.0)
        self.assertTrue(coral["evidence"]["$tuple"])

    def test_v2_dimensionless_derived_scores_mix_native_units_but_raw_values_do_not(self):
        from e4core.m4_operator_ir_v2_worker import _component_series_v2

        runtime = {**RUNTIME, "relative_scale_floor": 0.01, "tail_quantile": 0.9}

        def run(program, typed_inputs):
            wire = {key: json.loads(canonical_json(value).decode("utf-8"))
                for key, value in typed_inputs.items()}
            return execute_ir_v2(program, wire, {}, runtime,
                trusted_runtime_context_v2(inputs=typed_inputs))

        changed = run(_exceedance_program(sigma=True),
            _mixed_native_unit_inputs(changed=True))
        fields = changed["$fields"]
        self.assertEqual(fields["status"], "ready")
        candidates = fields["candidates"]["$tuple"]
        coral = next(row["$fields"] for row in candidates
            if row["$fields"]["entity"]["$fields"]["identifier"] == "component-coral")
        self.assertGreater(coral["score"], 0.0)
        self.assertTrue(coral["evidence"]["$tuple"])
        trace = fields["status_details"]["$map"]["operator_ir_availability"]
        normalize = next(row["availability"]["support_details"]["series_results"]
            for row in trace if row["kernel"] == "series.normalize_reference_scale")
        self.assertTrue(normalize)
        self.assertEqual(normalize[0]["support_counts"][
            "reference_window_independent_time_blocks"], 30)
        exceedance = next(row["availability"]["support_details"]["series_results"]
            for row in trace if row["kernel"] == "series.sigma_exceedance")
        self.assertEqual(exceedance[0]["numeric_unit"], "dimensionless")
        self.assertEqual(exceedance[0]["source_unit"], "undeclared native value")
        self.assertEqual(exceedance[1]["source_unit"],
            "rate-like by field suffix; exact unit undeclared")

        unchanged = run(_exceedance_program(sigma=True),
            _mixed_native_unit_inputs(changed=False))
        self.assertEqual(unchanged["$fields"]["status"], "abstained")
        self.assertFalse(any(row["$fields"]["score"] > 0.0 for row in
            unchanged["$fields"]["candidates"]["$tuple"]))

        raw_inputs = _mixed_native_unit_inputs(changed=True)
        raw_mixed = run(_program(with_kpi=False), raw_inputs)
        self.assertEqual(raw_mixed["$fields"]["status"], "abstained")
        self.assertFalse(any(row["$fields"]["score"] > 0.0 for row in
            raw_mixed["$fields"]["candidates"]["$tuple"]))
        from e4core import m4_operator_ir_worker as v1_worker
        raw_wire = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in raw_inputs.items()}
        raw_wire = {key: v1_worker._decode(value) for key, value in raw_wire.items()}
        components = _component_series_v2(raw_wire)
        summaries = v1_worker._reduce_series(components, "median", 1, 6, runtime)
        deltas = v1_worker._window_compare({"rows": summaries["rows"]}, "difference", runtime)
        protected = v1_worker._reduce_component(deltas, "max")
        coral_score = next(row for row in protected["rows"]
            if row["key"][1] == "component-coral")
        self.assertFalse(coral_score["eligible"])
        self.assertEqual(coral_score["details"]["score_unit"], "incompatible_units")

    def test_v2_large_series_trace_keeps_exact_summary_and_bounded_examples(self):
        from e4core.m4_operator_ir_v2_worker import _support_details

        rows = [{"key": ("component", f"component-{index:02d}", f"field-{index}"),
            "component_id": f"component-{index:02d}", "source_field": f"field-{index}",
            "metric_semantics": {"direction": "two_sided"},
            "windows": {"reference": {"support": 30}, "alert": {"support": 29}},
            "source_record": {"unit": "native", "source_hashes": {"metric.csv": "a" * 64},
                "case_data_sha256": "b" * 64},
            "alert_record": {"source_hashes": {"metric.csv": "a" * 64},
                "case_data_sha256": "b" * 64},
            "details": {"numeric_unit": "dimensionless"},
            "support_counts": {"reference_window_independent_time_blocks": 30,
                "alert_window_independent_time_blocks": 29},
            "computable": index % 2 == 0,
            "reason": None if index % 2 == 0 else "reference_scale_unavailable"}
            for index in range(40)]
        details = _support_details("series.sigma_exceedance", {"rows": rows}, {}, {})
        summary = details["series_trace_summary"]
        self.assertEqual(len(details["series_results"]), 32)
        self.assertEqual(summary["total_rows"], 40)
        self.assertEqual(summary["omitted_rows"], 8)
        self.assertEqual(details["series_rows_omitted"], 8)
        self.assertEqual(summary["all_rows_sha256"], hashlib.sha256(json.dumps(
            rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False).encode("utf-8")).hexdigest())
        self.assertEqual(summary["computability_counts"], {
            "computable": 20, "not_computable": 20, "unknown": 0})
        self.assertEqual(summary["reason_counts"], {
            "none": 20, "reference_scale_unavailable": 20})
        self.assertEqual(summary["support_counts_histogram"], [{
            "support_counts": {"alert_window_independent_time_blocks": 29,
                "reference_window_independent_time_blocks": 30}, "rows": 40}])
        self.assertEqual(summary["source_sha256"], [{"source": "metric.csv",
            "sha256": "a" * 64}])
        self.assertEqual(summary["case_data_sha256"], ["b" * 64])

    def test_observed_kpi_kernel_reports_sparse_support_without_promoting_policy_eligibility(self):
        typed_inputs = _trusted_inputs(kpi_support=11)
        inputs = {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in typed_inputs.items()}
        program = _program(with_kpi=True)
        result = execute_ir_v2(program, inputs, {}, RUNTIME,
            trusted_runtime_context_v2(inputs=typed_inputs))
        trace = result["$fields"]["status_details"]["$map"]["operator_ir_availability"]
        observed = next(row["availability"] for row in trace
            if row["kernel"] == "series.observed_kpi")
        candidate = observed["support_details"]["kpi_support_by_candidate"][0]
        assert (candidate["windows"]["reference"]["finite_unique_timestamps"],
                candidate["windows"]["alert"]["finite_unique_timestamps"]) == (30, 11)
        assert candidate["meets_primary_policy"] is False
        assert observed["computability_state"] == "computable"
        assert observed["ranking_state"] == "unknown"
