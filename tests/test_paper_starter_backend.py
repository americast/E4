import unittest

from e4core.contracts import (Candidate, CandidateSet, EntityRef, Evidence, MetricPoint,
    PaperStarterInput, ScoredCandidate, ScoredCandidates, Span, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.paper_starter import _bank_metric_tree, _dependency_adjust, _score_backend


def telemetry(metrics=(), spans=(), *, case_id="synthetic-backend"):
    reference = Window("reference", 0, 2_400_000)
    alert = Window("alert", 2_400_000, 3_600_000)
    return Telemetry(case_id, reference, alert, tuple(metrics), tuple(spans),
        {name: {"role": "component"} for name in ("caller", "callee", "sparse")},
        "a" * 64, {}, {}, {}, "backend-fixture/1")


def starter(value, *, grain=60_000):
    return PaperStarterInput(value, None, None,
        {"sessions": "unsupported", "urls": "unsupported"}, grain)


def context(value, operator="paper.test"):
    return NodeContext(value.case_id, "test", operator, {}, {},
                       value.reference_window, value.alert_window, value.data_sha256)


class PaperStarterBackendOperatorTests(unittest.TestCase):
    def test_allowlisted_field_scoring_keeps_low_scale_change_visible(self):
        ref, alert = Window("reference", 0, 2_400_000), Window("alert", 2_400_000, 3_600_000)
        component = EntityRef("component", "component-1")
        rows = []
        high = "OSLinux-CPU_CPU_CPULoad"
        low = "OSLinux-OSLinux_MEMORY_MEMORY_MEMUsedMemPerc"
        for index, value in enumerate((0.0, 10.0, 20.0, 30.0)):
            ts = ref.start_ms_utc + index * 60_000
            rows.append(MetricPoint("bank/metric_container.csv", ts, component,
                high, "bank.metric_container", "fixture", 1e9 + value,
                str(1e9 + value), f"high-ref-{index}"))
            rows.append(MetricPoint("bank/metric_container.csv", ts, component,
                low, "bank.metric_container", "fixture", float(index % 2),
                str(index % 2), f"low-ref-{index}"))
        for index, value in enumerate((0.0, 10.0, 20.0, 30.0)):
            ts = alert.start_ms_utc + index * 60_000
            rows.append(MetricPoint("bank/metric_container.csv", ts, component,
                high, "bank.metric_container", "fixture", 1e9 + value,
                str(1e9 + value), f"high-alert-{index}"))
            shifted = 10.0 + float(index % 2)
            rows.append(MetricPoint("bank/metric_container.csv", ts, component,
                low, "bank.metric_container", "fixture", shifted,
                str(shifted), f"low-alert-{index}"))
        source = telemetry(rows)
        result = _score_backend({"data": starter(source), "candidates": CandidateSet(
            (Candidate(component, "observed"),), "fixture")},
            {"minimum_samples": 4, "alpha": 0.05, "resamples": 99,
             "relative_scale_floor": 0.05, "absolute_scale_floor": 1e-9,
             "quantile": 0.9, "metric": ""}, context(source), "welch", "mean").value
        score = result.candidates[0]
        self.assertGreater(score.score, 0)
        self.assertEqual(score.evidence[0].details["metric_field"], low)
        self.assertEqual(score.evidence[0].details["metric_field_test_count"], 2)

    def test_duplicate_alert_rows_at_same_timestamps_do_not_create_a_block_change(self):
        reference, alert = Window("reference", 0, 2_400_000), Window("alert", 2_400_000, 3_600_000)
        component = EntityRef("component", "component-1")
        metric = "OSLinux-CPU_CPU_CPUCpuUtil"
        base_values = (1.0, 3.0, 2.0, 4.0)
        rows = []
        for index, value in enumerate(base_values):
            rows.append(MetricPoint("bank/metric_container.csv", reference.start_ms_utc + index * 60_000,
                component, metric, "bank.metric_container", "fixture", value, str(value), f"ref-{index}"))
        for index, value in enumerate(base_values):
            timestamp = alert.start_ms_utc + index * 60_000
            for duplicate in range(100):
                rows.append(MetricPoint("bank/metric_container.csv", timestamp, component, metric,
                    "bank.metric_container", "fixture", value, str(value), f"alert-{index}-{duplicate}"))
        source = telemetry(rows)
        result = _score_backend({"data": starter(source), "candidates": CandidateSet(
            (Candidate(component, "observed"),), "fixture")},
            {"minimum_samples": 4, "alpha": 0.05, "resamples": 99,
             "relative_scale_floor": 0.05, "absolute_scale_floor": 1e-9,
             "quantile": 0.9, "metric": metric}, context(source), "welch", "mean").value
        score = result.candidates[0]
        self.assertEqual(score.score, 0.0)
        self.assertEqual(score.evidence[0].support, 4)

    def test_dependency_graph_rejects_duplicate_ids_and_cycles_without_changing_valid_ranking(self):
        reference, alert = Window("reference", 0, 1000), Window("alert", 1000, 2000)
        caller, callee = EntityRef("component", "caller"), EntityRef("component", "callee")
        valid = (
            Span("valid", "root", None, 1100, "4", None, "missing", "", "unknown",
                 caller, None, None, False, False),
            Span("valid", "child", "root", 1101, "2", None, "missing", "", "unknown",
                 callee, None, None, True, False),
        )
        duplicate_ids = (
            Span("duplicate", "root", None, 1100, "4", None, "missing", "", "unknown",
                 callee, None, None, False, False),
            Span("duplicate", "same", "root", 1101, "2", None, "missing", "", "unknown",
                 caller, None, None, True, False),
            Span("duplicate", "same", "root", 1102, "1", None, "missing", "", "unknown",
                 callee, None, None, True, False),
        )
        cycle = (
            Span("cycle", "root", None, 1100, "4", None, "missing", "", "unknown",
                 callee, None, None, False, False),
            Span("cycle", "x", "y", 1101, "2", None, "missing", "", "unknown",
                 callee, None, None, True, False),
            Span("cycle", "y", "x", 1102, "2", None, "missing", "", "unknown",
                 caller, None, None, True, False),
        )
        entities = (caller, callee)
        source_scores = ScoredCandidates(tuple(ScoredCandidate(entity, score, None,
            (Evidence("bank/metric_container.csv", alert, "fixture", score,
                      (entity,), 4, {"ranking_eligible": True}),), "fixture")
            for entity, score in zip(entities, (8.0, 7.0))), "fixture")

        def run(spans):
            data = Telemetry("trace-graph-fixture", reference, alert, (), spans,
                {"caller": {"role": "component"}, "callee": {"role": "component"}},
                "b" * 64, {}, {}, {}, "trace-fixture/1")
            result = _dependency_adjust({"data": starter(data), "scores": source_scores},
                {"downstream_penalty": 0.5}, context(data)).value
            return result

        clean = run(valid)
        polluted = run(valid + duplicate_ids + cycle)
        clean_scores = {item.entity.identifier: item.score for item in clean.candidates}
        polluted_scores = {item.entity.identifier: item.score for item in polluted.candidates}
        self.assertEqual(polluted_scores, clean_scores)
        self.assertEqual(clean_scores, {"caller": 4.5, "callee": 7.0})
        self.assertEqual(polluted.status_details["skipped_trace_counts"]["duplicate_span_ids"], 1)
        self.assertEqual(polluted.status_details["skipped_trace_counts"]["cyclic_trace_graph"], 1)

    def test_component_tree_uses_sparse_timestamp_alignment_and_rejects_unexplained_common_shift(self):
        reference = Window("reference", 0, 2_400_000)
        alert = Window("alert", 2_400_000, 3_600_000)
        metric_name = "OSLinux-CPU_CPU_CPUCpuUtil"
        true_component = EntityRef("component", "true")
        sparse_component = EntityRef("component", "sparse")

        def make_data(*, alert_offset=0.0, alert_features=None, include_sparse=True):
            metrics = []
            for index in range(40):
                timestamp = index * 60_000
                x = float(index % 10)
                metrics.append(MetricPoint("bank/metric_app.csv", timestamp,
                    EntityRef("app_kpi", "kpi-a"), "mrt", None, None, x, str(x), f"kpi-r-{index}"))
                metrics.append(MetricPoint("bank/metric_container.csv", timestamp,
                    true_component, metric_name, None, None, x, str(x), f"true-r-{index}"))
                if include_sparse and index < 6:
                    metrics.append(MetricPoint("bank/metric_container.csv", timestamp,
                        sparse_component, metric_name, None, None, x, str(x), f"sparse-r-{index}"))
            if alert_features is None:
                alert_features = [float(5 + index % 5) for index in range(20)]
            for index, x in enumerate(alert_features):
                timestamp = alert.start_ms_utc + index * 60_000
                kpi = x + alert_offset
                metrics.append(MetricPoint("bank/metric_app.csv", timestamp,
                    EntityRef("app_kpi", "kpi-a"), "mrt", None, None, kpi, str(kpi), f"kpi-a-{index}"))
                metrics.append(MetricPoint("bank/metric_container.csv", timestamp,
                    true_component, metric_name, None, None, x, str(x), f"true-a-{index}"))
                if include_sparse and index < 3:
                    metrics.append(MetricPoint("bank/metric_container.csv", timestamp,
                        sparse_component, metric_name, None, None, x, str(x), f"sparse-a-{index}"))
            return Telemetry("tree-fixture", reference, alert, tuple(metrics), (), {},
                             "c" * 64, {}, {}, {}, "tree-fixture/1")

        params = {"app_kpi_field": "mrt", "component_metric": metric_name,
            "minimum_samples": 8, "max_depth": 10, "min_samples_leaf": 1,
            "minimum_validation_samples": 5, "validation_fraction": 0.25,
            "minimum_validation_r2": 0.9, "maximum_validation_normalized_error": 0.2,
            "residual_scale_floor": 0.1, "minimum_observed_kpi_change": 1.0,
            "minimum_contribution_fraction": 0.1, "seed": 7}
        candidates = CandidateSet((Candidate(true_component), Candidate(sparse_component)), "fixture")
        positive_data = make_data()
        positive = _bank_metric_tree({"data": starter(positive_data), "candidates": candidates},
                                     params, context(positive_data)).value
        scores = {item.entity.identifier: item.score for item in positive.candidates}
        self.assertGreater(scores["true"], 0)
        self.assertEqual(scores["sparse"], 0.0)
        sparse_result = next(item for item in positive.candidates if item.entity.identifier == "sparse")
        self.assertFalse(sparse_result.evidence)

        unchanged_features = [float(index % 10) for index in range(20)]
        unexplained_data = make_data(alert_offset=8.0, alert_features=unchanged_features,
                                     include_sparse=False)
        unexplained = _bank_metric_tree({"data": starter(unexplained_data),
            "candidates": CandidateSet((Candidate(true_component),), "fixture")},
            params, context(unexplained_data)).value
        self.assertEqual(unexplained.candidates[0].score, 0.0)
        self.assertEqual(unexplained.status, "abstained")


if __name__ == "__main__":
    unittest.main()
