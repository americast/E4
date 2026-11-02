"""Label-blind typed availability/support reports for the host IR authoring API."""
from __future__ import annotations

import hashlib
import unittest
from dataclasses import replace

from e4core.m4_operator_ir import operator_ir_capability_report
from e4core.paper_starter_fixtures import _rich_fixture
from e4core.registry import OperatorRegistry


def _sparse_starter(*, remove_kpi: bool = False, constant_component: bool = False):
    telemetry, starter = _rich_fixture()
    ref_window, alert_window = telemetry.reference_window, telemetry.alert_window
    app_by_window = {"reference": [], "alert": []}
    component_by_window = {"reference": [], "alert": []}
    for point in telemetry.metrics:
        if point.table == "bank/metric_app.csv" and point.metric_name == "mrt":
            name = "reference" if ref_window.contains(point.timestamp_ms_utc) else "alert"
            app_by_window[name].append(point)
        elif (point.table == "bank/metric_container.csv" and
              point.entity.identifier == "component-a" and
              point.metric_name == "OSLinux-CPU_CPU_CPUCpuUtil"):
            name = "reference" if ref_window.contains(point.timestamp_ms_utc) else "alert"
            component_by_window[name].append(point)
    sparse = []
    if not remove_kpi:
        for name, count in (("reference", 11), ("alert", 12)):
            sparse.extend(app_by_window[name][:count])
    for name in ("reference", "alert"):
        for point in component_by_window[name][:12]:
            sparse.append(replace(point, value=5.0 if constant_component else point.value))
    digest = hashlib.sha256(repr([(row.table, row.entity.identifier, row.metric_name,
        row.timestamp_ms_utc, row.value) for row in sparse]).encode()).hexdigest()
    derived_telemetry = replace(telemetry, metrics=tuple(sparse), data_sha256=digest)
    return replace(starter, telemetry=derived_telemetry)


def _computation(report, name):
    return next(row for row in report["computations"] if row["computation"] == name)


class OperatorIRAvailabilityTests(unittest.TestCase):
    def test_dense_report_separates_presence_computability_and_uncomputed_rankability(self):
        _telemetry, starter = _rich_fixture()
        report = operator_ir_capability_report(starter, OperatorRegistry(),
            runtime_values={"minimum_support": 12, "minimum_independent_blocks": 6})
        primary = report["primary_kpi"]
        assert primary["data_state"] == "present"
        assert primary["computability_state"] == "computable"
        assert primary["ranking_state"] == "unknown"
        assert primary["selected_identity"] == "opaque-kpi"
        assert primary["details"]["meets_primary_kpi_policy"] is True
        univariate = _computation(report, "component_univariate_window_change")
        assert "bank/metric_app.csv:mrt" not in univariate["required_inputs"]
        assert univariate["computability_state"] == "computable"
        assert univariate["ranking_state"] == "unknown"
        assert "synthetic-paper-fixture" not in repr(report)
        assert report["provenance"]["primary_kpi_policy_sha256"]
        selector_ref = "exact-exact/bank/metric_container.csv/OSLinux-CPU_CPU_CPUCpuUtil@1"
        catalog = {row["selector_ref"]: row for row in report["metric_selector_catalog"]}
        resolutions = {row["selector_ref"]: row for row in report["metric_selector_resolutions"]}
        self.assertIn(selector_ref, catalog)
        self.assertEqual(resolutions[selector_ref]["status"], "resolved_one")
        self.assertEqual(resolutions[selector_ref]["match_count"], 1)
        self.assertEqual(resolutions[selector_ref]["case_data_sha256"], starter.telemetry.data_sha256)


    def test_sparse_11_12_kpi_is_computable_but_not_primary_policy_selected(self):
        report = operator_ir_capability_report(_sparse_starter(), OperatorRegistry(),
            runtime_values={"minimum_support": 12, "minimum_independent_blocks": 6})
        primary = report["primary_kpi"]
        assert primary["data_state"] == "present"
        assert primary["computability_state"] == "computable"
        assert primary["ranking_state"] == "unknown"
        assert primary["selected_identity"] is None
        assert primary["details"]["meets_primary_kpi_policy"] is False
        candidate = primary["details"]["observed_kpi_candidates"][0]
        assert (candidate["reference_unique_timestamps"], candidate["alert_unique_timestamps"]) == (11, 12)
        association = _computation(report, "kpi_association_both_windows")
        supported = association["details"]["series_support"][0]
        assert (supported["reference_exact_shared_pairs"],
                supported["alert_exact_shared_pairs"]) == (11, 12)
        assert association["computability_state"] == "not_computable"  # active minimum_support is 12
        assert association["ranking_state"] == "unknown"
        univariate = _computation(report, "component_univariate_window_change")
        assert univariate["computability_state"] == "computable"
        assert univariate["required_inputs"] == ["bank/metric_container.csv:exact-classified gauge/rate"]


    def test_computation_requirement_is_local_and_not_a_global_kpi_support_override(self):
        report = operator_ir_capability_report(_sparse_starter(), OperatorRegistry(),
            runtime_values={"minimum_support": 11, "minimum_independent_blocks": 6})
        association = _computation(report, "kpi_association_both_windows")
        assert association["computability_state"] == "computable"
        assert association["ranking_state"] == "unknown"
        assert association["selected_identity"] == "opaque-kpi"
        assert association["selection_reason"].startswith("observed_support_first_candidate")
        assert association["details"]["primary_kpi_policy_selected"] is False


    def test_missing_kpi_does_not_abstain_univariate_and_constants_are_explicit(self):
        no_kpi = operator_ir_capability_report(_sparse_starter(remove_kpi=True),
            OperatorRegistry(), runtime_values={"minimum_support": 12, "minimum_independent_blocks": 6})
        assert no_kpi["primary_kpi"]["data_state"] == "absent"
        assert _computation(no_kpi, "kpi_association_both_windows")["computability_state"] == "not_computable"
        assert _computation(no_kpi, "component_univariate_window_change")["computability_state"] == "computable"

        constant = operator_ir_capability_report(_sparse_starter(constant_component=True),
            OperatorRegistry(), runtime_values={"minimum_support": 11, "minimum_independent_blocks": 6})
        series = _computation(constant, "component_univariate_window_change")["details"]["series_support"][0]
        assert series["reference_constant"] is True
        assert series["alert_constant"] is True
        association = _computation(constant, "kpi_association_both_windows")
        candidate = association["details"]["series_support"][0]
        assert candidate["meets_declared_pair_support"] is True
        assert candidate["has_constant_input_series"] is True
        assert candidate["computable_by_declared_minimum_pairs"] is False
        assert association["computability_state"] == "not_computable"
