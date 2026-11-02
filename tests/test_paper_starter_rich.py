import unittest
from collections import defaultdict

from e4core.contracts import (PaperCandidate, PaperCandidateSet, PaperSessionObservation,
    PaperStarterInput, PaperURLObservation, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.paper_starter import (_candidate_id, _flow_scorer, _rich_candidates,
                                  _score_rich)


def starter(*, sessions=(), urls=()):
    reference = Window("reference", 0, 1000)
    alert = Window("alert", 1000, 2000)
    telemetry = Telemetry("synthetic-rich", reference, alert, (), (), {},
                          "d" * 64, {}, {}, {}, "test/1")
    return PaperStarterInput(telemetry, tuple(sessions), tuple(urls),
        {"sessions": "available", "urls": "available"}, 60_000)


def context():
    return NodeContext("synthetic-rich", "test", "paper.test", {}, {},
                       Window("reference", 0, 1000), Window("alert", 1000, 2000), "d" * 64)


class PaperStarterRichOperatorTests(unittest.TestCase):
    def test_url_failure_detection_and_scoring_do_not_depend_on_latency(self):
        urls = []
        baseline_failures = (0, 0, 1, 2)
        alert_failures = (1, 2, 3, 3)
        for index in range(4):
            sid = f"session-{index}"
            for window_name, timestamp, failures in (
                    ("reference", 100 + index, baseline_failures[index]),
                    ("alert", 1100 + index, alert_failures[index])):
                for request in range(3):
                    urls.append(PaperURLObservation(sid, timestamp + request, "/checkout",
                        request >= failures, 10.0, "ms"))
        data = starter(urls=urls)
        candidates = _rich_candidates({"data": data}, {"minimum_support": 3,
            "minimum_change": 0.1},
                                      context(), "url_failure").value
        self.assertEqual(len(candidates.candidates), 1)
        candidate = candidates.candidates[0]
        self.assertEqual(candidate.attributes, {"url_signal": "failure"})
        output = _score_rich({"data": data, "candidates": candidates},
            {"minimum_samples": 3, "alpha": 0.2}, context(), "url_welch").value
        scored = output.candidates[0]
        self.assertGreater(scored.score, 0)
        self.assertEqual(scored.evidence[0].details["outcome_denominator"],
                         "distinct session-window valid outcome observations")

    def test_event_pattern_with_zero_success_matches_can_be_a_perfect_discriminator(self):
        sessions = []
        for window, base_time in (("reference", 100), ("alert", 1100)):
            for index in range(3):
                sessions.append(PaperSessionObservation(f"failed-{window}-{index}", base_time + index,
                    {}, events=("disk", "write", "failed"), outcome=False))
                sessions.append(PaperSessionObservation(f"ok-{window}-{index}", base_time + 10 + index,
                    {}, events=("request", "completed"), outcome=True))
        data = starter(sessions=sessions)
        candidates = _rich_candidates({"data": data}, {"minimum_support": 2,
            "maximum_length": 3, "maximum_candidates": 64}, context(), "event_pattern").value
        pattern = next(candidate for candidate in candidates.candidates
                       if candidate.tokens == ("disk", "write", "failed"))
        output = _score_rich({"data": data, "candidates": PaperCandidateSet((pattern,), "fixture")},
            {"minimum_samples": 2, "minimum_effect": 0.1,
             "maximum_patterns": 64, "maximum_pattern_length": 16},
            context(), "event_support").value
        scored = output.candidates[0]
        self.assertGreater(scored.score, 0.8)
        details = scored.evidence[0].details
        self.assertEqual(details["failed_session_support"], 6)
        self.assertEqual(details["successful_session_support"], 0)
        self.assertEqual(details["valid_successful_outcome_denominator"], 6)

    def test_flow_scores_use_candidate_ids_and_component_specific_contribution(self):
        urls = []
        sessions = []
        base_features = [(float(index), float(index % 3 + 1)) for index in range(12)]
        alert_features = [(float(index + 6), float(index % 3 + 1)) for index in range(6)]
        for window_name, start, features in (("reference", 100, base_features),
                                              ("alert", 1100, alert_features)):
            for index, (duration_a, duration_b) in enumerate(features):
                sid = f"{window_name}-{index}"
                timestamp = start + index
                sessions.append(PaperSessionObservation(sid, timestamp, {},
                    kpi_value=2.0 * duration_a + 3.0 * duration_b))
                urls.append(PaperURLObservation(sid, timestamp, "/a", True, duration_a, "ms"))
                urls.append(PaperURLObservation(sid, timestamp, "/b", True, duration_b, "ms"))
        data = starter(sessions=sessions, urls=urls)
        candidates = PaperCandidateSet(tuple(PaperCandidate("url",
            _candidate_id("url", (), (), url), url=url) for url in ("/a", "/b")), "fixture")
        params = {"fit_mode": "historical", "validation_fraction": 0.25,
            "minimum_validation_samples": 2, "minimum_validation_skill": 0.9,
            "maximum_validation_normalized_error": 0.2, "residual_scale_floor": 0.1,
            "ridge": 0.0, "max_condition": 100.0,
            "maximum_relative_residual": 0.1, "minimum_rows": 4}
        result = _flow_scorer({"data": data, "candidates": candidates}, params, context()).value
        scores = {candidate.entity.identifier: candidate.score for candidate in result.candidates}
        self.assertGreater(scores[_candidate_id("url", (), (), "/a")], 0)
        self.assertAlmostEqual(scores[_candidate_id("url", (), (), "/b")], 0.0)

    def test_flow_historical_holdout_is_ordered_by_timestamp_not_session_label(self):
        before_features = [(float(index % 7 + 1), float(index % 3 + 1)) for index in range(12)]
        after_features = [(float(index + 4), float(index % 3 + 1)) for index in range(6)]

        def run(reverse_names):
            urls, sessions = [], []
            for window_name, start, features in (("reference", 100, before_features),
                                                  ("alert", 1100, after_features)):
                for index, (duration_a, duration_b) in enumerate(features):
                    label_index = 11 - index if reverse_names else index
                    session_id = f"session-{label_index:02d}"
                    timestamp = start + index
                    sessions.append(PaperSessionObservation(session_id, timestamp, {},
                        kpi_value=2.0 * duration_a + 3.0 * duration_b))
                    urls.append(PaperURLObservation(session_id, timestamp, "/a", True, duration_a, "ms"))
                    urls.append(PaperURLObservation(session_id, timestamp, "/b", True, duration_b, "ms"))
            data = starter(sessions=sessions, urls=urls)
            candidates = PaperCandidateSet(tuple(PaperCandidate("url",
                _candidate_id("url", (), (), url), url=url) for url in ("/a", "/b")), "fixture")
            params = {"fit_mode": "historical", "validation_fraction": 0.25,
                "minimum_validation_samples": 2, "minimum_validation_skill": 0.9,
                "maximum_validation_normalized_error": 0.2, "residual_scale_floor": 0.1,
                "ridge": 0.0, "max_condition": 100.0,
                "maximum_relative_residual": 0.1, "minimum_rows": 4}
            return _flow_scorer({"data": data, "candidates": candidates}, params, context()).value

        chronological = run(False)
        renamed = run(True)
        self.assertEqual({item.entity.identifier: item.score for item in chronological.candidates},
                         {item.entity.identifier: item.score for item in renamed.candidates})
        self.assertEqual(chronological.candidates[0].evidence[0].details["fit_status"], "ok")
        self.assertEqual(renamed.candidates[0].evidence[0].details["fit_status"], "ok")
        self.assertEqual(chronological.candidates[0].evidence[0].details["validation_samples"], 3)

    def test_activity_cohort_uses_derived_activity_without_raw_attribute_lookup(self):
        sessions = []
        event_counts = (1, 2, 4, 5, 6, 7)
        for index, count in enumerate(event_counts):
            sid = f"activity-{index}"
            for timestamp, kpi in ((100 + index, float(index)),
                                   (1100 + index, 10.0 + float(index) if index >= 4 else float(index))):
                sessions.append(PaperSessionObservation(sid, timestamp, {},
                    events=tuple(f"event-{j}" for j in range(count)), kpi_value=kpi))
        data = starter(sessions=sessions)
        candidates = _rich_candidates({"data": data}, {"minimum_support": 2},
                                      context(), "cohort_activity").value
        high = next(candidate for candidate in candidates.candidates
                    if candidate.attributes == {"activity": "high_activity"})
        output = _score_rich({"data": data, "candidates": PaperCandidateSet((high,), "fixture")},
            {"minimum_samples": 2, "alpha": 0.2}, context(), "cohort_welch").value
        self.assertEqual(output.candidates[0].evidence[0].details["status"], "ok")
        self.assertEqual(output.candidates[0].evidence[0].support, 2)

    def test_alert_only_category_is_not_discovered_or_silently_encoded_as_baseline_feature(self):
        sessions = [PaperSessionObservation(f"base-{index}", 100 + index,
            {"region": "east" if index % 2 == 0 else "west"}, kpi_value=float(index % 2))
            for index in range(20)]
        sessions.extend(PaperSessionObservation(f"alert-{index}", 1100 + index,
            {"region": "new-alert-only"}, kpi_value=float(index)) for index in range(4))
        data = starter(sessions=sessions)
        discovered = _rich_candidates({"data": data}, {"minimum_support": 2},
                                      context(), "cohort_attribute").value
        self.assertFalse(any(candidate.attributes.get("region") == "new-alert-only"
                             for candidate in discovered.candidates))
        candidate = PaperCandidate("cohort", "east", attributes={"region": "east"})
        params = {"max_depth": 3, "min_samples_leaf": 1, "minimum_samples": 4,
            "validation_fraction": 0.25, "minimum_validation_samples": 2,
            "minimum_validation_r2": -1.0, "maximum_validation_normalized_error": 10.0,
            "residual_scale_floor": 0.1, "minimum_excess_nll_per_observation": 0.01,
            "seed": 0}
        output = _score_rich({"data": data, "candidates": PaperCandidateSet((candidate,), "fixture")},
                             params, context(), "tree_likelihood").value
        self.assertEqual(output.candidates[0].evidence[0].details["reason"],
                         "alert_contains_category_absent_from_baseline")


if __name__ == "__main__":
    unittest.main()
