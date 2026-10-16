import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

from e4core.bank_ops import (aggregate_bank_metrics, aggregate_bank_trace_durations, bank_registry,
                             combine_bank_scores, score_bank_log_volume,
                             score_bank_metrics, score_bank_trace_duration)
from e4core.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet,
                              EntityRef, Span, Telemetry, Window, ScoredCandidate,
                              ScoredCandidates, Evidence)
from e4core.executor import NodeContext


REFERENCE = Window("reference", 0, 1_800_000)
ALERT = Window("alert", 1_800_000, 3_600_000)
RUNTIME = {"minimum_support": 4, "minimum_independent_blocks": 4,
           "block_minutes": 5, "permutation_resamples": 999,
           "relative_scale_floor": 0.05, "absolute_scale_floor": 0.001,
           "permutation_p_score_floor": 0.001,
           "minimum_normalized_effect": 0.5, "maximum_change_p_value": 0.1,
           "tail_quantile": 0.9, "seed": 42, "lead_count": 3}


def context(case_id="bank-fixture", op="bank.operator"):
    return NodeContext(case_id, "node", op, RUNTIME, {}, REFERENCE, ALERT, "a" * 64)


def telemetry(metrics=(), spans=()):
    return Telemetry("bank-fixture", REFERENCE, ALERT, tuple(metrics), tuple(spans), {},
                     "b" * 64, {}, {}, {}, "bank-test/1")


def metric(entity, table, name, item, timestamp, value, *, bomc="bank.metric_container"):
    return __import__("e4core.contracts", fromlist=["MetricPoint"]).MetricPoint(
        table, timestamp, entity, name, bomc, item, value, str(value),
        f"{table}:{timestamp}:{name}:{item}")


def contrast_aggregate(window, values, *, entity=None, source="bank/metric_container.csv",
                       metric_name="CPUUtil", item_id="value", stats=("median", "tail_quantile"),
                       kind="gauge"):
    entity = entity or EntityRef("component", "PodA")
    samples = tuple((window.start_ms_utc + index * 300_000, value)
                    for index, value in enumerate(values))
    rows = []
    for statistic in stats:
        rows.append(AggregateRow(entity, f"{source}|{metric_name}|bank.metric_container|{item_id}",
            window, statistic, float(values[len(values)//2]), len(values), (source,), (),
            {"metric_kind": kind, "ranking_eligible": True, "unit": "native fixture unit",
             "direction": "two_sided", "bucket_samples": samples,
             "item_id": item_id}))
    return Aggregate(window, tuple(rows), (source,))


def trace_aggregate(window, entity, values, statistic):
    trace_values = tuple((f"trace-{index:02d}", float(value))
                         for index, value in enumerate(values))
    row = AggregateRow(entity, "bank/trace_span.csv|duration||duration", window,
        statistic, float(values[len(values)//2]), len(values), ("bank/trace_span.csv",), (),
        {"trace_values": trace_values, "duration_unit": "undeclared"})
    return Aggregate(window, (row,), ("bank/trace_span.csv",))


def scored(entity, value, family, *, eligible=True):
    evidence = Evidence("bank/metric_container.csv", ALERT, "test", value, (entity,), 5,
                       {"ranking_eligible": eligible})
    return ScoredCandidate(entity, value, None, (evidence,), analysis_family=family)


class BankOperatorTests(unittest.TestCase):
    def test_registry_versions_and_metric_behavior_use_the_same_root_policy_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "config").mkdir()
            metric_path = root / "config/bank_metric_kind_policy.json"
            kpi_path = root / "config/bank_dev_kpi_policy.json"
            shutil.copyfile(Path(__file__).resolve().parents[1] / "config/bank_metric_kind_policy.json", metric_path)
            shutil.copyfile(Path(__file__).resolve().parents[1] / "config/bank_dev_kpi_policy.json", kpi_path)
            policy = json.loads(metric_path.read_text(encoding="utf-8"))
            metric_name = next(iter(policy["source_semantics"]["metric_container"]["ranking_allowlist"]))
            entity = EntityRef("component", "PodA")
            data = telemetry(metrics=(metric(entity, "bank/metric_container.csv", metric_name,
                "value", 0, 12),))
            before = bank_registry(root=root)
            before_signature = next(item for item in before.signatures()
                                    if item["name"] == "bank.metrics.aggregate")
            before_operator = before.require("bank.metrics.aggregate", before_signature["version"])
            before_result = before_operator.implementation({"data": data}, {"window": "reference"}, context()).value
            self.assertTrue(next(row for row in before_result.rows if row.statistic == "median")
                            .details["ranking_eligible"])

            policy["source_semantics"]["metric_container"]["ranking_allowlist"][metric_name][
                "ranking_eligible"] = False
            metric_path.write_text(json.dumps(policy), encoding="utf-8")
            after = bank_registry(root=root)
            after_signature = next(item for item in after.signatures()
                                  if item["name"] == "bank.metrics.aggregate")
            after_operator = after.require("bank.metrics.aggregate", after_signature["version"])
            after_result = after_operator.implementation({"data": data}, {"window": "reference"}, context()).value
            self.assertNotEqual(before_signature["version"], after_signature["version"])
            self.assertFalse(next(row for row in after_result.rows if row.statistic == "median")
                             .details["ranking_eligible"])

            kpi_before = next(item for item in after.signatures() if item["name"] == "bank.answers.combine")
            kpi_policy = json.loads(kpi_path.read_text(encoding="utf-8"))
            kpi_policy["family_weights"]["bank_metric"] = 3.0
            kpi_path.write_text(json.dumps(kpi_policy), encoding="utf-8")
            kpi_after = next(item for item in bank_registry(root=root).signatures()
                             if item["name"] == "bank.answers.combine")
            self.assertNotEqual(kpi_before["version"], kpi_after["version"])

    def test_metric_aggregate_uses_true_even_median_tail_and_source_identity(self):
        entity = EntityRef("component", "PodA")
        metrics = [
            metric(entity, "bank/metric_container.csv", "SameKpi", "value", 0, 2),
            metric(entity, "bank/metric_container.csv", "SameKpi", "value", 300_000, 4),
            metric(entity, "bank/metric_other.csv", "SameKpi", "value", 0, 100, bomc="other"),
            metric(entity, "bank/metric_other.csv", "SameKpi", "value", 300_000, 200, bomc="other"),
        ]
        result = aggregate_bank_metrics({"data": telemetry(metrics)}, {"window": "reference"},
                                        context()).value
        rows = defaultdict(dict)
        for row in result.rows:
            rows[row.metric_key.split("|")[0]][row.statistic] = row
        self.assertEqual(rows["bank/metric_container.csv"]["median"].value, 3.0)
        self.assertAlmostEqual(rows["bank/metric_container.csv"]["tail_quantile"].value, 3.8)
        self.assertEqual(rows["bank/metric_other.csv"]["median"].value, 150.0)
        self.assertEqual(len(rows), 2)

    def test_trace_aggregate_preserves_median_mean_tail_and_excludes_invalid_ids_and_negative_duration(self):
        spans = []
        for window, prefix, ts in ((REFERENCE, "r", 0), (ALERT, "a", 1_800_000)):
            for index in range(6):
                trace_id = f"{prefix}{index}"
                base = ts + index * 300_000
                for n, duration in enumerate((2, 4, -99) if prefix == "r" and index == 0 else
                                              (5, 9, -99) if prefix == "a" and index == 0 else
                                              (2, 4) if prefix == "r" else (5, 9)):
                    spans.append(Span(trace_id, f"{trace_id}-{n}", None, base + n,
                        str(duration), None, "missing", "", "unprovided",
                        EntityRef("component", "PodA"), None, None, False, False))
        spans.append(Span("", "", None, 1000, "999", None, "missing", "", "unprovided",
                          EntityRef("component", "PodA"), None, None, False, False))
        before = aggregate_bank_trace_durations({"data": telemetry(spans=spans)},
                                                {"window": "reference"}, context()).value
        rows = {row.statistic: row for row in before.rows}
        self.assertEqual(rows["per_trace_median"].value, 3.0)
        self.assertEqual(rows["per_trace_mean"].value, 3.0)
        self.assertAlmostEqual(rows["per_trace_tail_quantile"].value, 3.8)
        self.assertEqual(rows["per_trace_median"].support, 6)
        self.assertEqual(len(rows["per_trace_median"].details["trace_values"]), 6)

    def test_window_pairing_and_metric_support_effect_and_significance_gates(self):
        candidate = CandidateSet((Candidate(EntityRef("component", "PodA")),), "fixture")
        reference = contrast_aggregate(REFERENCE, [10] * 6)
        alert = contrast_aggregate(ALERT, [20] * 6)
        result = score_bank_metrics({"reference": reference, "alert": alert,
                                     "candidates": candidate}, {}, context())
        self.assertEqual(result.status, "ready")
        self.assertGreater(result.candidates[0].score, 0)
        self.assertTrue(any(item.details.get("ranking_eligible") for item in result.candidates[0].evidence))

        weak = score_bank_metrics({"reference": contrast_aggregate(REFERENCE, [10] * 6),
                                   "alert": contrast_aggregate(ALERT, [10] * 6),
                                   "candidates": candidate}, {}, context())
        self.assertEqual(weak.status, "abstained")
        self.assertEqual(weak.candidates[0].score, 0)
        self.assertFalse(any(item.details.get("ranking_eligible") for item in weak.candidates[0].evidence))

        reversed_window = Aggregate(Window("alert", 0, 1_800_000), (), ())
        with self.assertRaisesRegex(ValueError, "adjacent reference and alert"):
            score_bank_metrics({"reference": reversed_window, "alert": alert,
                                "candidates": candidate}, {}, context())

    def test_sparse_slow_tail_is_retained_when_median_does_not_move(self):
        policy = json.loads((Path(__file__).resolve().parents[1] /
                             "config/bank_metric_kind_policy.json").read_text(encoding="utf-8"))
        metric_name = next(iter(policy["source_semantics"]["metric_container"]["ranking_allowlist"]))
        entity = EntityRef("component", "PodA")
        points = []
        for window, start in (("reference", REFERENCE.start_ms_utc),
                              ("alert", ALERT.start_ms_utc)):
            for block in range(6):
                for minute in range(5):
                    value = 100.0 if window == "alert" and minute == 4 else 1.0
                    timestamp = start + (block * 5 + minute) * 60_000
                    points.append(metric(entity, "bank/metric_container.csv", metric_name,
                                         "value", timestamp, value))
        data = telemetry(metrics=points)
        before = aggregate_bank_metrics({"data": data}, {"window": "reference"}, context()).value
        after = aggregate_bank_metrics({"data": data}, {"window": "alert"}, context()).value
        candidate = CandidateSet((Candidate(entity),), "fixture")
        result = score_bank_metrics({"reference": before, "alert": after,
                                     "candidates": candidate}, {}, context())
        median_evidence = next(item for item in result.candidates[0].evidence
                               if item.details.get("summary_statistic") == "median")
        tail_evidence = next(item for item in result.candidates[0].evidence
                             if item.details.get("summary_statistic") == "tail_quantile")
        self.assertEqual(median_evidence.value, 0.0)
        self.assertFalse(median_evidence.details["ranking_eligible"])
        self.assertGreater(tail_evidence.value, 0.0)
        self.assertTrue(tail_evidence.details["ranking_eligible"])
        self.assertGreater(result.candidates[0].score, 0.0)

    def test_trace_improvement_is_descriptive_not_a_ranked_fault(self):
        entity = EntityRef("component", "PodA")
        candidate = CandidateSet((Candidate(entity),), "fixture")
        before = trace_aggregate(REFERENCE, entity, [20] * 6, "per_trace_median")
        after = trace_aggregate(ALERT, entity, [10] * 6, "per_trace_median")
        result = score_bank_trace_duration({"reference": before, "alert": after,
                                            "candidates": candidate}, {}, context())
        self.assertEqual(result.status, "abstained")
        self.assertEqual(result.candidates[0].score, 0)
        self.assertEqual(result.candidates[0].evidence[0].value, -10)
        self.assertFalse(result.candidates[0].evidence[0].details["ranking_eligible"])

    def test_combiner_uses_one_vote_per_family_common_denominator_and_canonical_identity(self):
        pod_a, pod_a_case, pod_b, pod_c = (EntityRef("component", "PodA"),
            EntityRef("component", "poda"), EntityRef("component", "PodB"),
            EntityRef("component", "PodC"))
        family_one = ScoredCandidates((scored(pod_a, 2.0, "bank_metric"),
                                       scored(pod_b, 1.0, "bank_metric"),
                                       scored(pod_c, 0.0, "bank_metric", eligible=False)),
                                      "bank.metric.1", "ready")
        family_one_repeat = ScoredCandidates((scored(pod_a_case, 1.0, "bank_metric"),
                                              scored(pod_b, 4.0, "bank_metric")),
                                             "bank.metric.2", "ready")
        family_two = ScoredCandidates((scored(pod_a_case, 1.0, "bank_trace_latency"),
                                       scored(pod_b, 0.0, "bank_trace_latency", eligible=False)),
                                      "bank.trace", "ready")
        answer = combine_bank_scores({"scores": (family_one, family_one_repeat, family_two)}, {}, context())
        self.assertEqual([row.entity.canonical_id for row in answer.leads], ["poda", "podb"])
        self.assertEqual(len({row.entity.canonical_id for row in answer.leads}), 2)
        self.assertAlmostEqual(answer.leads[0].score, 0.75)
        self.assertAlmostEqual(answer.leads[1].score, 0.5)
        self.assertNotIn("podc", {row.entity.canonical_id for row in answer.leads})

    def test_combiner_serialization_is_stable_across_hash_seeds_and_family_order(self):
        script = r'''import json, sys
from e4core.bank_ops import combine_bank_scores
from e4core.contracts import EntityRef, Evidence, RankedAnswer, ScoredCandidate, ScoredCandidates, Window
from e4core.executor import NodeContext
reference = Window("reference", 0, 1800000)
alert = Window("alert", 1800000, 3600000)
runtime = {"lead_count": 3}
ctx = NodeContext("case", "node", "bank.answers.combine", runtime, {}, reference, alert, "a" * 64)
definitions = {
  "bank_metric": [("PodA", .7, "unclassified_metric_excluded"), ("PodB", .4, "metric_container_median"), ("PodC", .2, "metric_container_tail")],
  "bank_trace_latency": [("PodA", .5, "trace_duration_median"), ("PodB", .8, "trace_duration_tail"), ("PodC", .1, "trace_duration_mean")],
  "bank_log_volume": [("PodA", .9, "log_volume_median"), ("PodB", .3, "log_volume_records"), ("PodC", .6, "log_volume_tail")],
}
families = {}
for family_name, rows in definitions.items():
  candidates = []
  for identifier, score, statistic in rows:
    entity = EntityRef("component", identifier)
    evidence = Evidence("bank/" + family_name + ".csv", alert, statistic, score, (entity,), 7,
      {"ranking_eligible": True, "family": family_name})
    candidates.append(ScoredCandidate(entity, score, None, (evidence,), analysis_family=family_name))
  families[family_name] = ScoredCandidates(tuple(candidates), family_name, "ready")
order = list(families)
if sys.argv[1] == "reverse":
  order.reverse()
answer = combine_bank_scores({"scores": tuple(families[name] for name in order)}, {}, ctx)
def evidence_json(item):
  return {"source": item.source, "window": item.window.name,
    "statistic": item.statistic, "value": item.value,
    "entities": [[entity.role, entity.identifier] for entity in item.contributing_entities],
    "support": item.support, "details": dict(item.details)}
print(json.dumps([{"entity": [row.entity.role, row.entity.identifier], "score": row.score,
  "analysis_family": row.analysis_family,
  "evidence": [evidence_json(item) for item in row.evidence]} for row in answer.leads],
  sort_keys=True, separators=(",", ":"), allow_nan=False))
'''
        outputs = []
        for seed in ("1", "987"):
            for family_order in ("forward", "reverse"):
                env = os.environ.copy()
                env["PYTHONHASHSEED"] = seed
                result = subprocess.run([sys.executable, "-c", script, family_order],
                    cwd=Path(__file__).resolve().parents[1], env=env,
                    capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                outputs.append(result.stdout)
        self.assertEqual(len(set(outputs)), 1)
        answer = json.loads(outputs[0])
        self.assertEqual(len(answer), 3)
        self.assertTrue(all(len(row["evidence"]) == 3 for row in answer))


if __name__ == "__main__":
    unittest.main()
