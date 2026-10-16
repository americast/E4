import math
import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from collections import defaultdict
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

import numpy as np
from scipy import stats

from e4core.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
                              EntityRef, MetricPoint, RankedAnswer, ScoredCandidate,
                              ScoredCandidates, Span, Telemetry, Window)
from e4core.dag import DAGValidationError, verify_dag
from e4core.development import (build_development_prompt, replay_saved_development_dag,
                               select_primary_kpi)
from e4core.executor import NodeContext, SerialExecutor
from e4core.registry import OperatorRegistry
from e4core.runtime import build_case_runtime
from e4core.telecom_ops import (aggregate_metrics, aggregate_traces, combined_seed_dag,
                                dependency_seed_dag, flow_seed_dag,
                                generate_candidates, metric_seed_dag,
                                score_conditional_flow, score_dependency,
                                score_metrics, score_trace_errors,
                                score_trace_latency, telecom_registry,
                                trace_error_seed_dag, trace_latency_seed_dag,
                                combine_scores, _pair_rows)
from e4core.numerics import permutation_contrast

ROOT = Path(__file__).resolve().parents[1]


def make_telemetry(*, traffic=100, low_request=False, flow=True, error=True, error_count=6):
    base = 1_700_000_000_000
    ref = Window("reference", base, base + 30 * 60_000)
    alert = Window("alert", ref.end_ms_utc, ref.end_ms_utc + 30 * 60_000)
    metrics = []
    spans = []
    # App duplicate rows differ; one distinct timestamp is still one support unit.
    for window in (ref, alert):
        for minute in range(30):
            timestamp = window.start_ms_utc + minute * 60_000
            during = window.name == "alert"
            app_value = 1.0 + (0.5 if during else 0.0)
            request_count = 1 if low_request else traffic
            metrics.extend([
                MetricPoint("metric/metric_app.csv", timestamp, EntityRef("service", "osb_001"), "avg_time", "metric_app", "avg_time", app_value, str(app_value), f"{window.name}-{minute}:avg"),
                MetricPoint("metric/metric_app.csv", timestamp, EntityRef("service", "osb_001"), "num", "metric_app", "num", float(request_count), str(request_count), f"{window.name}-{minute}:num"),
                MetricPoint("metric/metric_node.csv", timestamp, EntityRef("component", "os_017"), "CPU_util_pct", "cpu_bomc", "cpu_item", 5.0 + (5.0 if during else 0.0), "5", f"{window.name}-{minute}:cpu"),
                MetricPoint("metric/metric_node.csv", timestamp, EntityRef("component", "os_018"), "CPU_util_pct", "cpu_bomc", "cpu_item", 2.0, "2", f"{window.name}-{minute}:constant"),
            ])
            if minute == 0:
                # Repeated key must be median-aggregated, not counted as a minute.
                metrics.append(MetricPoint("metric/metric_app.csv", timestamp, EntityRef("service", "osb_001"), "avg_time", "metric_app", "avg_time", 9.0, "9", f"{window.name}-{minute}:avg-dup"))
    # Trace data: 30 independent trace units/window; same span ID is reused
    # across traces and must never create cross-trace parent edges.
    for window in (ref, alert):
        for index in range(30):
            trace_id = f"{window.name}-trace-{index:02}"
            timestamp = window.start_ms_utc + index * 60_000
            child_fails = bool(error and window.name == "alert" and index < error_count)
            root_fails = child_fails
            if flow:
                # Two independently varying backend features avoid a collinear
                # duplicate-identity fixture; root elapsed is additive.
                if window.name == "reference":
                    child_elapsed = float(index + 1)
                    db_elapsed = float((index * 7) % 30 + 1)
                else:
                    child_elapsed = float(index + 31)
                    db_elapsed = float((index * 7) % 30 + 31)
                root_elapsed = 100.0 + 2.0 * child_elapsed + 3.0 * db_elapsed
            else:
                child_elapsed = 10.0 if window.name == "alert" else 5.0
                db_elapsed = child_elapsed
                root_elapsed = 100.0
            spans.append(Span(trace_id, "same-root-id", None, timestamp, str(root_elapsed),
                not root_fails, "valid", "TRUE" if not root_fails else "false", "OSB",
                EntityRef("component", "osb_001"), EntityRef("service", "osb_001"), None,
                False, False))
            spans.append(Span(trace_id, "same-child-id", "same-root-id", timestamp + 2,
                str(child_elapsed), not child_fails, "valid", "False" if child_fails else "TRUE", "JDBC",
                EntityRef("component", "docker_001"), None, None,
                True, False))
            spans.append(Span(trace_id, "same-db-id", "same-root-id", timestamp + 3,
                str(db_elapsed), True, "valid", "TRUE", "JDBC",
                None, None, EntityRef("database", "db_001"),
                True, False))
    catalog = {
        "osb_001": {"deployment_type": "service", "host": None, "container_links": None},
        "os_017": {"deployment_type": "VM", "host": None, "container_links": None},
        "os_018": {"deployment_type": "VM", "host": None, "container_links": None},
        "docker_001": {"deployment_type": "container_001", "host": "os_017", "container_links": None},
        "db_001": {"deployment_type": "oracle", "host": None, "container_links": None},
    }
    return Telemetry("synthetic-telecom", ref, alert, tuple(metrics), tuple(spans),
        MappingProxyType({key: MappingProxyType(value) for key, value in catalog.items()}),
        "a" * 64, MappingProxyType({}), MappingProxyType({}), MappingProxyType({}), "test-adapter")


def runtime(telemetry):
    return build_case_runtime({"analysis_window_start_epoch_ms_utc": telemetry.reference_window.start_ms_utc,
        "alert_window_start_epoch_ms_utc": telemetry.alert_window.start_ms_utc,
        "alert_window_end_epoch_ms_utc": telemetry.alert_window.end_ms_utc})


def counter_telemetry():
    base = 1_700_100_000_000
    ref = Window("reference", base, base + 30 * 60_000)
    alert = Window("alert", ref.end_ms_utc, ref.end_ms_utc + 30 * 60_000)
    points = []
    level = 1000.0
    for window in (ref, alert):
        for minute in range(30):
            timestamp = window.start_ms_utc + minute * 60_000
            if window.name == "alert" and minute == 12:
                level = 0.0  # observed reset to zero
            else:
                level += 10.0
            points.append(MetricPoint("metric/metric_middleware.csv", timestamp,
                EntityRef("component", "redis_001"), "total_commands_processed", "redis", "counter",
                level, str(level), f"counter-{window.name}-{minute}"))
            unknown_value = 1.0 if window.name == "reference" else 100.0
            points.append(MetricPoint("metric/metric_middleware.csv", timestamp,
                EntityRef("component", "redis_001"), "Mystery", "redis", "unknown",
                unknown_value, str(unknown_value), f"unknown-{window.name}-{minute}"))
    catalog = {"redis_001": MappingProxyType({"deployment_type": "redis", "host": None, "container_links": None})}
    return Telemetry("counter-case", ref, alert, tuple(points), (), MappingProxyType(catalog),
        "9" * 64, MappingProxyType({}), MappingProxyType({}), MappingProxyType({}), "test")


def context(telemetry, node="test"):
    rt = runtime(telemetry)
    return NodeContext(telemetry.case_id, node, "test@1", rt.snapshot.values,
        rt.snapshot.provenance, telemetry.reference_window, telemetry.alert_window,
        telemetry.data_sha256)


def aggregate_metric_windows(telemetry):
    return tuple(aggregate_metrics({"data": telemetry}, {"window": name}, context(telemetry)).value
                 for name in ("reference", "alert"))


def aggregate_trace_windows(telemetry):
    return tuple(aggregate_traces({"data": telemetry}, {"window": name}, context(telemetry)).value
                 for name in ("reference", "alert"))


def candidates(telemetry, *aggregates):
    return generate_candidates({"data": telemetry, "aggregates": aggregates}, {}, context(telemetry))


class TelecomOperatorTests(unittest.TestCase):
    def test_dependency_score_is_stable_across_python_hash_seeds(self):
        code = (
            "from tests.test_e4core_telecom_ops import make_telemetry, aggregate_metric_windows, "
            "aggregate_trace_windows, candidates, context; "
            "from e4core.telecom_ops import score_dependency; "
            "t=make_telemetry(); rm,am=aggregate_metric_windows(t); "
            "rt,at=aggregate_trace_windows(t); c=candidates(t,rm,am,rt,at); "
            "r=score_dependency({'data':t,'candidates':c},{},context(t)); "
            "x=next(v for v in r.candidates if v.entity.identifier=='docker_001'); print(repr(x.score))"
        )
        outputs = []
        for hash_seed in ("1", "3"):
            environment = os.environ.copy()
            environment["PYTHONHASHSEED"] = hash_seed
            completed = subprocess.run([sys.executable, "-c", code], cwd=ROOT,
                env=environment, capture_output=True, text=True, timeout=20, check=True)
            outputs.append(completed.stdout.strip())
        self.assertEqual(outputs[0], outputs[1])

    def test_saved_dag_replay_cache_parity_is_opt_in(self):
        telemetry = make_telemetry()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alert_path = root / "data/prepared/telecom/public/cases" / telemetry.case_id / "alert.json"
            alert_path.parent.mkdir(parents=True)
            alert_path.write_text(json.dumps({
                "analysis_window_start_epoch_ms_utc": telemetry.reference_window.start_ms_utc,
                "alert_window_start_epoch_ms_utc": telemetry.alert_window.start_ms_utc,
                "alert_window_end_epoch_ms_utc": telemetry.alert_window.end_ms_utc}))
            dag_path = root / "saved_dag.json"
            dag_path.write_text(json.dumps(metric_seed_dag()))
            with patch("e4core.development.load_case", return_value=telemetry):
                default = replay_saved_development_dag(telemetry.case_id, "dev", dag_path,
                    run_dir=root / "default", root=root)
                parity = replay_saved_development_dag(telemetry.case_id, "dev", dag_path,
                    run_dir=root / "parity", root=root, verify_cache_parity=True)
        self.assertFalse(default["cache_parity_requested"])
        self.assertIsNone(default["cache_parity_passed"])
        self.assertEqual(len(default["execution_seconds"]), 1)
        self.assertTrue(parity["cache_parity_requested"])
        self.assertTrue(parity["cache_parity_passed"])
        self.assertEqual(len(parity["execution_seconds"]), 3)
        self.assertEqual(parity["cache_hits"][0], 0)
        self.assertGreater(parity["cache_hits"][1], 0)
        self.assertEqual(parity["cache_hits"][2], 0)

    def test_public_dev_kpi_selection_is_support_first_and_prompt_has_no_gold(self):
        telemetry = make_telemetry()
        selected = select_primary_kpi(telemetry)
        self.assertEqual(selected["kpi_id"], "root_osb_elapsedTime_mean")
        self.assertEqual(selected["entity"], "osb_001")
        prompt = build_development_prompt("dev-case", {"alert_text": "unknown failure",
            "timezone": "Asia/Shanghai", "analysis_window_start_epoch_ms_utc": 0,
            "alert_window_start_epoch_ms_utc": 30, "alert_window_end_epoch_ms_utc": 60},
            runtime(telemetry), selected, {"entities": {}}, telecom_registry())
        self.assertIn("one single-window Aggregate", prompt)
        self.assertNotIn("gold", prompt.casefold())
        self.assertIn('"choices": ["reference", "alert"]', prompt)

    def test_exact_small_sample_permutation_matches_scipy_reference(self):
        before = [1.0, 2.0, 3.0, 4.0]
        after = [8.0, 9.0, 10.0, 11.0]
        reference = stats.permutation_test((before, after),
            lambda x, y: float(np.median(y) - np.median(x)),
            permutation_type="independent", alternative="two-sided", n_resamples=np.inf)
        result = permutation_contrast(before, after, seed=42, resamples=499,
                                      statistic="median", minimum_blocks=4)
        self.assertAlmostEqual(result.raw_delta, reference.statistic)
        self.assertAlmostEqual(result.p_value, reference.pvalue)

    def test_no_fault_scores_do_not_fill_rank_and_family_support_uses_common_denominator(self):
        telemetry = make_telemetry()
        ctx = context(telemetry)
        a = EntityRef("component", "api_a")
        b = EntityRef("service", "api_b")
        c = EntityRef("component", "api_c")
        metric = ScoredCandidates((ScoredCandidate(a, 1.0, None, (), "metric"),
                                   ScoredCandidate(b, 0.8, None, (), "metric"),
                                   ScoredCandidate(c, 0.0, None, (), "metric")), "metric", "ready")
        traces = ScoredCandidates((ScoredCandidate(a, 0.0, None, (), "trace_error"),
                                   ScoredCandidate(b, 0.8, None, (), "trace_error"),
                                   ScoredCandidate(c, 0.0, None, (), "trace_error")), "trace", "ready")
        answer = combine_scores({"scores": (metric, traces)}, {"limit": 5}, ctx)
        self.assertEqual([item.entity.identifier for item in answer.leads], ["api_b", "api_a"])
        self.assertGreater(answer.leads[0].score, answer.leads[1].score)
        all_zero = ScoredCandidates((ScoredCandidate(a, 0.0, None, (), "metric"),
                                     ScoredCandidate(b, 0.0, None, (), "metric")), "metric", "ready")
        self.assertEqual(combine_scores({"scores": (all_zero,)}, {"limit": 5}, ctx).leads, ())
        duplicates = ScoredCandidates((ScoredCandidate(EntityRef("component", "same"), 1.0, None, (), "metric"),
                                       ScoredCandidate(EntityRef("service", " SAME "), 0.8, None, (), "metric")),
                                      "metric", "ready")
        deduped = combine_scores({"scores": (duplicates,)}, {"limit": 5}, ctx)
        self.assertEqual(len(deduped.leads), 1)

    def test_tail_only_increase_changes_q90_while_median_stays_fixed(self):
        telemetry = make_telemetry(error=False, flow=False)
        adjusted = []
        alert_app_index = 0
        for point in telemetry.metrics:
            if point.table == "metric/metric_node.csv" and point.entity.identifier == "os_017":
                value = 5.0
                if point.timestamp_ms_utc >= telemetry.alert_window.start_ms_utc:
                    value = 5.0
                adjusted.append(replace(point, value=value, raw_value=str(value)))
            elif (point.table == "metric/metric_app.csv" and point.metric_name == "avg_time"
                  and telemetry.alert_window.contains(point.timestamp_ms_utc)):
                # Replicated tail flapping: two bad samples in each five-minute
                # block leaves the median unchanged but moves block q90.
                value = 10.0 if alert_app_index % 5 >= 3 else 1.0
                alert_app_index += 1
                adjusted.append(replace(point, value=value, raw_value=str(value)))
            else:
                adjusted.append(point)
        tail_case = replace(telemetry, metrics=tuple(adjusted), data_sha256="c" * 64)
        ref, alert = aggregate_metric_windows(tail_case)
        traces_ref, traces_alert = aggregate_trace_windows(tail_case)
        candidate_set = candidates(tail_case, ref, alert, traces_ref, traces_alert)
        result = score_metrics({"reference": ref, "alert": alert, "candidates": candidate_set},
                               {}, context(tail_case))
        osb = next(item for item in result.candidates if item.entity.role == "service" and
                   item.entity.identifier == "osb_001")
        median_ev = [e for e in osb.evidence if "median_block" in e.statistic and e.details.get("metric_key", "").startswith("metric/metric_app.csv|avg_time|")]
        tail_ev = [e for e in osb.evidence if "tail_quantile_block" in e.statistic and e.details.get("metric_key", "").startswith("metric/metric_app.csv|avg_time|")]
        self.assertTrue(median_ev)
        self.assertTrue(tail_ev)
        self.assertTrue(all(abs(e.value) < 1e-12 for e in median_ev))
        self.assertTrue(any(abs(e.value) > 0.5 and e.details["ranking_eligible"] for e in tail_ev))
        self.assertGreater(osb.score, 0.0)

    def test_trace_tail_uses_request_distribution_and_ranks_replicated_minority(self):
        telemetry = make_telemetry(flow=False, error=False)
        expanded = []
        for window in (telemetry.reference_window, telemetry.alert_window):
            templates = [s for s in telemetry.spans if window.contains(s.start_ms_utc)]
            for repeat in range(4):
                for template in templates:
                    new_index = repeat * 30 + int(template.trace_id.rsplit("-", 1)[1])
                    trace_id = f"{window.name}-wide-{new_index:03}"
                    elapsed = template.elapsed_raw
                    if (window.name == "alert" and template.executing_component is not None
                            and template.executing_component.identifier == "docker_001"
                            and new_index < 48):
                        elapsed = "50"
                    elif (window.name == "alert" and template.executing_component is not None
                          and template.executing_component.identifier == "docker_001"):
                        elapsed = "5"
                    expanded.append(replace(template, trace_id=trace_id,
                                            start_ms_utc=window.start_ms_utc + new_index * 1000,
                                            elapsed_raw=elapsed))
        wide = replace(telemetry, spans=tuple(expanded), data_sha256="d" * 64)
        ref, alert = aggregate_trace_windows(wide)
        candidate_set = candidates(wide, *aggregate_metric_windows(wide), ref, alert)
        scored = score_trace_latency({"reference": ref, "alert": alert,
                                      "candidates": candidate_set}, {}, context(wide))
        component = next(item for item in scored.candidates
                         if item.entity.role == "component" and item.entity.identifier == "docker_001")
        tail = [item for item in component.evidence
                if str(item.details.get("trace_metric", "")).endswith("|elapsed_tail")]
        medians = [item for item in component.evidence
                   if str(item.details.get("trace_metric", "")).endswith("|elapsed_median")]
        self.assertTrue(tail)
        self.assertTrue(any(item.value > 0 and item.details["ranking_eligible"] for item in tail),
                        [(item.value, item.details.get("p_value"), item.details.get("normalized_effect"))
                         for item in tail])
        self.assertTrue(medians)
        self.assertTrue(all(abs(item.value) < 1e-12 for item in medians
                            if item.details["trace_metric"].endswith("|elapsed_median")))
        self.assertGreater(component.score, 0.0)
        # A 15% slow-request minority can leave q90 borderline under a 499-draw
        # test, while the independent-unit mean retains power without shifting
        # the median. This is a declared latency variant in the same family.
        sparse = replace(wide, spans=tuple(
            replace(span, elapsed_raw="5")
            if (span.trace_id.startswith("alert-wide-")
                and span.executing_component is not None
                and span.executing_component.identifier == "docker_001"
                and int(span.trace_id.rsplit("-", 1)[1]) >= 18)
            else span for span in wide.spans), data_sha256="f" * 64)
        sparse_ref, sparse_alert = aggregate_trace_windows(sparse)
        sparse_candidates = candidates(sparse, *aggregate_metric_windows(sparse), sparse_ref, sparse_alert)
        mean_scores = score_trace_latency({"reference": sparse_ref, "alert": sparse_alert,
                                           "candidates": sparse_candidates}, {}, context(sparse))
        mean_component = next(item for item in mean_scores.candidates
                              if item.entity.identifier == "docker_001")
        mean_evidence = [item for item in mean_component.evidence
                         if str(item.details.get("trace_metric", "")).endswith("|elapsed_mean")]
        self.assertTrue(any(item.value > 0 and item.details["ranking_eligible"]
                            for item in mean_evidence),
                        [(item.value, item.details.get("p_value")) for item in mean_evidence])

    def test_metric_aggregator_collapses_repeated_keys_and_finds_known_shift(self):
        telemetry = make_telemetry()
        ref_metric, alert_metric = aggregate_metric_windows(telemetry)
        app_avg = [row for row in ref_metric.rows if row.entity.identifier == "osb_001"
                   and row.metric_key.startswith("metric/metric_app.csv|avg_time|")
                   and row.window.name == "reference" and row.statistic == "median"][0]
        self.assertEqual(app_avg.support, 30)
        self.assertEqual(app_avg.details["raw_rows"], 31)
        self.assertEqual(app_avg.details["duplicate_timestamp_rows"], 1)
        self.assertEqual(app_avg.details["bucket_samples"][0][1], 5.0)  # median(1,9)
        ref_trace, alert_trace = aggregate_trace_windows(telemetry)
        candidate_set = candidates(telemetry, ref_metric, alert_metric, ref_trace, alert_trace)
        scored = score_metrics({"reference": ref_metric, "alert": alert_metric,
                                "candidates": candidate_set}, {}, context(telemetry))
        by_id = {item.entity.identifier: item for item in scored.candidates}
        self.assertGreater(by_id["os_017"].score, by_id["os_018"].score)
        self.assertTrue(by_id["os_017"].evidence)
        constant = [ev for ev in by_id["os_018"].evidence if ev.statistic.endswith("permutation_contrast")]
        self.assertTrue(constant)
        self.assertTrue(all(ev.details["p_value"] == 1.0 for ev in constant))

    def test_low_traffic_abstains_only_from_average_latency_kpi(self):
        telemetry = make_telemetry(low_request=True)
        ref_metric, alert_metric = aggregate_metric_windows(telemetry)
        ref_trace, alert_trace = aggregate_trace_windows(telemetry)
        candidate_set = candidates(telemetry, ref_metric, alert_metric, ref_trace, alert_trace)
        scored = score_metrics({"reference": ref_metric, "alert": alert_metric,
                                "candidates": candidate_set}, {}, context(telemetry))
        osb = next(item for item in scored.candidates if item.entity.role == "service" and item.entity.identifier == "osb_001")
        self.assertTrue(any(ev.statistic == "low_traffic_abstention" for ev in osb.evidence))

    def test_cumulative_counter_levels_are_reset_aware_rates_and_unknowns_do_not_rank(self):
        telemetry = counter_telemetry()
        reference, alert = aggregate_metric_windows(telemetry)
        counter_rows = [row for row in alert.rows if "total_commands_processed" in row.metric_key
                        and row.statistic == "median"]
        self.assertEqual(len(counter_rows), 1)
        self.assertEqual(counter_rows[0].details["metric_kind"], "counter_rate")
        self.assertEqual(counter_rows[0].details["counter_resets"], 1)
        self.assertAlmostEqual(counter_rows[0].value, 10.0)
        unknown = [row for row in alert.rows if "Mystery" in row.metric_key and row.statistic == "median"]
        self.assertEqual(unknown[0].details["metric_kind"], "unknown")
        traces = aggregate_trace_windows(telemetry)
        candidate_set = candidates(telemetry, reference, alert, *traces)
        result = score_metrics({"reference": reference, "alert": alert,
                                "candidates": candidate_set}, {}, context(telemetry))
        redis = next(item for item in result.candidates if item.entity.identifier == "redis_001")
        self.assertEqual(redis.score, 0.0)
        self.assertTrue(any(ev.statistic == "unknown_metric_kind_excluded" for ev in redis.evidence))

    def test_trace_error_latency_and_dependency_distinguish_direct_child_from_propagated_root(self):
        telemetry = make_telemetry()
        ref_trace, alert_trace = aggregate_trace_windows(telemetry)
        ref_metric, alert_metric = aggregate_metric_windows(telemetry)
        candidate_set = candidates(telemetry, ref_metric, alert_metric, ref_trace, alert_trace)
        errors = score_trace_errors({"reference": ref_trace, "alert": alert_trace,
                                     "candidates": candidate_set}, {}, context(telemetry))
        latencies = score_trace_latency({"reference": ref_trace, "alert": alert_trace,
                                         "candidates": candidate_set}, {}, context(telemetry))
        dep = score_dependency({"data": telemetry, "candidates": candidate_set}, {}, context(telemetry))
        def score(result, identifier):
            return next(item for item in result.candidates if item.entity.identifier == identifier).score
        self.assertGreater(score(errors, "docker_001"), 0)
        error_rows = next(item for item in errors.candidates if item.entity.identifier == "docker_001")
        error_evidence = [item for item in error_rows.evidence
                          if item.details.get("trace_metric", "").endswith("|failure_rate")]
        self.assertTrue(any(item.value > 0 and item.details["ranking_eligible"]
                            for item in error_evidence))  # 20% failure increase, below median-of-fractions threshold
        self.assertGreater(score(latencies, "docker_001"), 0)
        self.assertGreater(score(dep, "docker_001"), score(dep, "osb_001"))
        self.assertTrue(all(span.span_id != span.parent_id for span in telemetry.spans))

    def test_unknown_success_rows_do_not_dilute_dependency_failure_rate(self):
        telemetry = make_telemetry()
        ref_metric, alert_metric = aggregate_metric_windows(telemetry)
        ref_trace, alert_trace = aggregate_trace_windows(telemetry)
        base_candidates = candidates(telemetry, ref_metric, alert_metric, ref_trace, alert_trace)
        base = score_dependency({"data": telemetry, "candidates": base_candidates}, {}, context(telemetry))
        extra = []
        for window in (telemetry.reference_window, telemetry.alert_window):
            for index in range(60):
                extra.append(Span(f"unknown-{window.name}-{index}", "unknown-span", None,
                    window.start_ms_utc + index * 1000, "5", None, "invalid", "maybe", "JDBC",
                    EntityRef("component", "docker_001"), None, None, False, False))
        expanded = replace(telemetry, spans=(*telemetry.spans, *extra), data_sha256="e" * 64)
        expanded_candidates = candidates(expanded, ref_metric, alert_metric, ref_trace, alert_trace)
        changed = score_dependency({"data": expanded, "candidates": expanded_candidates}, {}, context(expanded))
        get_score = lambda result: next(item.score for item in result.candidates
                                        if item.entity.identifier == "docker_001")
        self.assertAlmostEqual(get_score(base), get_score(changed))

    def test_dependency_weights_are_runtime_declared_and_recorded(self):
        telemetry = make_telemetry()
        reference, alert = aggregate_trace_windows(telemetry)
        ref_metric, alert_metric = aggregate_metric_windows(telemetry)
        candidate_set = candidates(telemetry, ref_metric, alert_metric, reference, alert)
        ctx = context(telemetry)
        evidence = next(item for candidate in score_dependency(
            {"data": telemetry, "candidates": candidate_set}, {}, ctx).candidates
            if candidate.entity.identifier == "docker_001" for item in candidate.evidence)
        self.assertEqual(evidence.details["propagation_weight"],
                         ctx.runtime_values["trace_propagation_weight"])
        self.assertEqual(evidence.details["deployment_host_weight"],
                         ctx.runtime_values["deployment_host_weight"])

    def test_conditional_flow_recovers_additive_same_field_contribution_and_abstains(self):
        telemetry = make_telemetry(flow=True, error=False)
        ref_metric, alert_metric = aggregate_metric_windows(telemetry)
        ref_trace, alert_trace = aggregate_trace_windows(telemetry)
        candidate_set = candidates(telemetry, ref_metric, alert_metric, ref_trace, alert_trace)
        flow = score_conditional_flow({"data": telemetry, "candidates": candidate_set}, {}, context(telemetry))
        db = next(item for item in flow.candidates if item.entity.role == "database" and item.entity.identifier == "db_001")
        self.assertGreater(db.score, 0)
        self.assertGreater(db.evidence[0].details["fit_r2"], 0.9)
        unsupported = score_conditional_flow({"data": make_telemetry(flow=False, error=False),
            "candidates": candidate_set}, {}, context(telemetry))
        self.assertEqual(unsupported.status, "abstained")

    def test_conditional_flow_abstains_when_backend_features_are_collinear(self):
        telemetry = make_telemetry(flow=True, error=False)
        adjusted = []
        children = {}
        for span in telemetry.spans:
            if span.span_id == "same-child-id":
                children[span.trace_id] = span.elapsed_raw
                adjusted.append(span)
            elif span.span_id == "same-db-id":
                adjusted.append(replace(span, elapsed_raw=children[span.trace_id]))
            else:
                adjusted.append(span)
        collinear = replace(telemetry, spans=tuple(adjusted), data_sha256="b" * 64)
        ref_metric, alert_metric = aggregate_metric_windows(collinear)
        ref_trace, alert_trace = aggregate_trace_windows(collinear)
        candidate_set = candidates(collinear, ref_metric, alert_metric, ref_trace, alert_trace)
        flow = score_conditional_flow({"data": collinear, "candidates": candidate_set}, {}, context(collinear))
        self.assertEqual(flow.status, "abstained")
        self.assertEqual(flow.status_details["reason"], "conditional_features_collinear")

    def test_conditional_flow_abstains_when_matched_roots_mix_logical_services(self):
        telemetry = make_telemetry(flow=True, error=False)
        changed = []
        for span in telemetry.spans:
            trace_index = int(span.trace_id.rsplit("-", 1)[1])
            if (span.call_type.casefold() == "osb" and span.parent_id is None
                    and span.logical_service is not None and trace_index % 2 == 0):
                changed.append(replace(span, logical_service=EntityRef("service", "other_osb")))
            else:
                changed.append(span)
        mixed = replace(telemetry, spans=tuple(changed), data_sha256="9" * 64)
        ref_metric, alert_metric = aggregate_metric_windows(mixed)
        ref_trace, alert_trace = aggregate_trace_windows(mixed)
        candidate_set = candidates(mixed, ref_metric, alert_metric, ref_trace, alert_trace)
        result = score_conditional_flow({"data": mixed, "candidates": candidate_set}, {}, context(mixed))
        self.assertEqual(result.status, "abstained")
        self.assertEqual(result.status_details["reason"], "ambiguous_root_kpi_service")
        self.assertEqual(result.status_details["root_service_ids"], ("osb_001", "other_osb"))

    def test_six_seed_dags_verify_and_execute_with_flow_visible_as_abstention_when_unfit(self):
        telemetry = make_telemetry()
        registry = telecom_registry()
        rt = runtime(telemetry)
        documents = [metric_seed_dag(), trace_error_seed_dag(), trace_latency_seed_dag(),
                     flow_seed_dag(), dependency_seed_dag(), combined_seed_dag()]
        executor = SerialExecutor(registry)
        for document in documents:
            with self.subTest(seed=document["nodes"][-2]["operator"] if len(document["nodes"]) > 1 else "?"):
                verified = verify_dag(document, registry, rt)
                self.assertEqual(verified.return_node, "rank")
                result = executor.execute_verified(verified, rt, {"data": telemetry}, cache_enabled=False)
                self.assertLessEqual(len(result.answer.leads), 5)
                self.assertEqual(result.answer.acceptance_contract, "component_identity_top3")


    def test_invalid_window_enum_fails_before_external_loader(self):
        telemetry = make_telemetry()
        doc = metric_seed_dag()
        next(node for node in doc["nodes"] if node["id"] == "metrics_reference")["params"]["window"] = "both"
        called = []
        executor = SerialExecutor(telecom_registry())
        with self.assertRaises(DAGValidationError) as error:
            executor.execute_document(doc, runtime(telemetry), lambda: called.append(True) or {})
        self.assertEqual(error.exception.code, "parameter_value_not_allowed")
        self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
