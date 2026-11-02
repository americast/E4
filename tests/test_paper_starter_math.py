import unittest

from e4core.cache import canonical_json
from e4core.paper_starter_math import (
    VoteRecord,
    adjust_dependency_scores,
    combine_votes,
    fit_flow_contributions,
    permutation_test,
    token_sequence_matches,
    tree_attribution,
    tree_prediction_contributions,
    welch_test,
)


class PaperStarterMathTests(unittest.TestCase):
    def test_welch_and_permutation_tests_have_real_effects_and_abstain(self):
        before = [1.0, 1.1, 0.9, 1.05, 0.95, 1.0]
        after = [4.0, 4.1, 3.9, 4.05, 3.95, 4.0]
        welch = welch_test(before, after, minimum_samples=3)
        self.assertEqual(welch.status, "ok")
        self.assertGreater(welch.effect, 0)
        self.assertLess(welch.p_value, 0.01)
        nearly_constant_before = [1e12 + value for value in (0.0, 0.01, 0.02, 0.03)]
        nearly_constant_after = [1e12 + value for value in (0.2, 0.21, 0.22, 0.23)]
        near = welch_test(nearly_constant_before, nearly_constant_after, minimum_samples=3)
        self.assertEqual(near.status, "ok")
        self.assertGreater(near.effect, 0)
        self.assertTrue(0 <= near.p_value <= 1)
        perm = permutation_test(before, after, seed=7, resamples=499,
            minimum_blocks=4, relative_scale_floor=0.05,
            absolute_scale_floor=1e-9, statistic="median", quantile=0.9)
        self.assertEqual(perm.status, "ok")
        self.assertGreater(perm.effect, 0)
        self.assertEqual(welch_test([1, 1], [1, 1], minimum_samples=2).status, "abstain")
        canonical_json(welch_test([1e308, -1e308], [1e308, -1e308],
                                  minimum_samples=2))
        for scale in (1e80, 1e120):
            result = welch_test([0, scale, 0, scale],
                                [scale, 2 * scale, scale, 2 * scale], minimum_samples=2)
            self.assertEqual(result.status, "ok")
            self.assertTrue(0 <= result.p_value <= 1)
            canonical_json(result)
        extreme = welch_test([1e308, 1e308], [-1e308, -1e308], minimum_samples=2)
        self.assertIsNone(extreme.details["mean_difference"])
        canonical_json(extreme)
        self.assertEqual(permutation_test([], [1], seed=1, resamples=20,
            minimum_blocks=2, relative_scale_floor=0.05,
            absolute_scale_floor=1e-9, statistic="median", quantile=0.9).status, "abstain")

    def test_family_balanced_votes_are_scale_invariant_and_deduplicate_fingerprints(self):
        def combine(a_scale, b_scale, duplicate=False):
            votes = [VoteRecord("stats", "data-a", (("a", 4*a_scale), ("b", a_scale))),
                     VoteRecord("tree", "data-b", (("a", b_scale), ("b", 4*b_scale)))]
            if duplicate:
                votes.append(VoteRecord("stats", "data-a", (("a", 400), ("b", 100))))
            return combine_votes(votes, family_weights={"stats": 1, "tree": 1},
                                 tie_tolerance=1e-12)
        baseline = combine(1, 1)
        self.assertEqual(baseline.status, "ambiguous")
        self.assertEqual(baseline.ranked, ("a", "b"))
        self.assertEqual(combine(100, 0.01).scores, baseline.scores)
        deduped = combine(1, 1, duplicate=True)
        self.assertEqual(deduped.scores, baseline.scores)
        self.assertEqual(deduped.support, 2)
        self.assertEqual(combine_votes([
            VoteRecord("stats", "zero", (("a", 0), ("b", 0))),
            VoteRecord("tree", "live", (("a", 1), ("b", 0))),
            VoteRecord("unknown", "ignored", (("a", 0), ("b", 9))),
        ], family_weights={"stats": 1, "tree": 1}, tie_tolerance=0).ranked, ("a", "b"))
        with self.assertRaises(ValueError):
            combine_votes([
                VoteRecord("stats", "conflict", (("a", 1), ("b", 0))),
                VoteRecord("stats", "conflict", (("a", 0), ("b", 1))),
            ], family_weights={"stats": 1, "tree": 1}, tie_tolerance=0)
        with self.assertRaises(ValueError):
            combine_votes([
                VoteRecord("stats", "family-conflict", (("a", 1),)),
                VoteRecord("tree", "family-conflict", (("a", 1),)),
            ], family_weights={"stats": 1, "tree": 1}, tie_tolerance=0)
        positive = combine_votes([
            VoteRecord("stats", "one", (("root", 9), ("symptom", 1))),
            VoteRecord("tree", "two", (("root", 8), ("symptom", 2))),
        ], family_weights={"stats": 1, "tree": 1}, tie_tolerance=0.01)
        self.assertEqual(positive.ranked[0], "root")

    def test_tree_attribution_uses_continuous_kpi_and_alert_residual_likelihood(self):
        x = [[float(i % 10), float(i % 2)] for i in range(40)]
        y = [float(i % 10) for i in range(40)]
        alert_y = [float(2 * (i % 10)) for i in range(40)]
        result = tree_attribution(x, y, x, alert_y,
            feature_names=("changed", "noise"), max_depth=10,
            min_samples_leaf=1, minimum_samples=10, validation_fraction=0.25,
            minimum_validation_samples=5, minimum_validation_r2=0.9,
            maximum_validation_normalized_error=0.2, residual_scale_floor=0.5,
            minimum_excess_nll_per_observation=0.05, seed=7)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.ranked, ("alert_excess_nll_per_observation",))
        self.assertFalse(result.details["causal"])
        self.assertEqual(result.details["residual_model"].split(",")[0], "Gaussian")
        self.assertGreater(result.details["excess_nll_per_observation"], 0.05)
        self.assertAlmostEqual(result.details["validation_r2"], 1.0)
        self.assertEqual(result.details["seed"], 7)
        self.assertEqual(result.details["validation_row_indices"], tuple(range(30, 40)))
        self.assertEqual(len(result.details["validation_log_likelihoods"]), 10)
        healthy = tree_attribution(x, y, x, y,
            feature_names=("changed", "noise"), max_depth=10,
            min_samples_leaf=1, minimum_samples=10, validation_fraction=0.25,
            minimum_validation_samples=5, minimum_validation_r2=0.9,
            maximum_validation_normalized_error=0.2, residual_scale_floor=0.5,
            minimum_excess_nll_per_observation=0.05, seed=7)
        self.assertEqual(healthy.status, "abstain")
        self.assertAlmostEqual(healthy.details["excess_nll_per_observation"], 0.0)
        cohort_x = [[float(i % 2), float(1 - (i % 2))] for i in range(40)]
        cohort_y = [1.0 + 4.0 * (i % 2) for i in range(40)]
        cohort_alert_y = [10.0 if i % 2 == 0 else 5.0 for i in range(40)]
        cohort = tree_attribution(cohort_x, cohort_y, cohort_x, cohort_alert_y,
            feature_names=("A", "B"), max_depth=5, min_samples_leaf=1,
            minimum_samples=10, validation_fraction=0.25, minimum_validation_samples=5,
            minimum_validation_r2=0.9, maximum_validation_normalized_error=0.2,
            residual_scale_floor=0.5, minimum_excess_nll_per_observation=0.05, seed=7)
        self.assertEqual(cohort.status, "ok")
        validation = dict(zip(cohort.details["validation_row_indices"],
                              cohort.details["validation_log_likelihoods"]))
        alert = dict(zip(cohort.details["alert_row_indices"], cohort.details["alert_log_likelihoods"]))
        a_delta = (-sum(alert[i] for i in alert if i % 2 == 0) / 20) - (-sum(
            validation[i] for i in validation if i % 2 == 0) / 5)
        b_delta = (-sum(alert[i] for i in alert if i % 2 == 1) / 20) - (-sum(
            validation[i] for i in validation if i % 2 == 1) / 5)
        self.assertGreater(a_delta, 0)
        self.assertAlmostEqual(b_delta, 0.0)
        bad_validation = tree_attribution([[float(i), 0.0] for i in range(20)],
            [float(i) for i in range(20)], [[float(i), 0.0] for i in range(5)],
            [float(i) for i in range(5)], feature_names=("trend", "constant"),
            max_depth=3, min_samples_leaf=1, minimum_samples=5,
            validation_fraction=0.25, minimum_validation_samples=3,
            minimum_validation_r2=0.5, maximum_validation_normalized_error=0.2,
            residual_scale_floor=0.5, minimum_excess_nll_per_observation=0.05, seed=1)
        self.assertEqual(bad_validation.reason, "baseline_validation_quality_below_threshold")
        out_of_support = tree_attribution(x, y, [[100.0, 0.0], [101.0, 1.0]],
            [200.0, 202.0], feature_names=("changed", "noise"), max_depth=10,
            min_samples_leaf=1, minimum_samples=10, validation_fraction=0.25,
            minimum_validation_samples=5, minimum_validation_r2=0.9,
            maximum_validation_normalized_error=0.2, residual_scale_floor=0.5,
            minimum_excess_nll_per_observation=0.05, seed=7)
        self.assertEqual(out_of_support.reason, "alert_outside_baseline_feature_support")
        huge_kpi = [1e308 if (i % 10) >= 5 else -1e308 for i in range(40)]
        self.assertEqual(tree_attribution(x, huge_kpi, x, huge_kpi,
            feature_names=("changed", "noise"), max_depth=10, min_samples_leaf=1,
            minimum_samples=10, validation_fraction=0.25, minimum_validation_samples=5,
            minimum_validation_r2=-1e308, maximum_validation_normalized_error=1e308,
            residual_scale_floor=0.5, minimum_excess_nll_per_observation=0.05,
            seed=7).status, "abstain")
        self.assertEqual(tree_attribution([[1], [1], [1], [1]], [1, 1, 1, 1],
            [[1], [1]], [1, 1], feature_names=("constant",), max_depth=2,
            min_samples_leaf=1, minimum_samples=2, validation_fraction=0.5,
            minimum_validation_samples=2, minimum_validation_r2=-1,
            maximum_validation_normalized_error=1, residual_scale_floor=0.5,
            minimum_excess_nll_per_observation=0.05, seed=1).status,
            "abstain")

    def test_nnls_flow_fit_uses_window_feature_means_and_checks_degeneracy(self):
        xb = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0],
              [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 1.0]]
        xa = [[2.0, 0.0], [0.0, 1.0], [2.0, 1.0], [2.0, 0.0],
              [0.0, 1.0], [2.0, 1.0], [2.0, 0.0], [0.0, 1.0]]
        yb = [2 * row[0] + 3 * row[1] for row in xb]
        ya = [2 * row[0] + 3 * row[1] for row in xa]
        flow_params = {"component_names": ("caller", "callee"), "fit_mode": "historical",
            "validation_fraction": 0.25, "minimum_validation_samples": 2,
            "minimum_validation_skill": 0.9, "maximum_validation_normalized_error": 0.2,
            "residual_scale_floor": 0.5, "ridge": 0, "max_condition": 100,
            "maximum_relative_residual": 1e-8, "minimum_rows": 3}
        result = fit_flow_contributions(xb, yb, xa, ya, **flow_params)
        self.assertEqual(result.status, "ok")
        self.assertAlmostEqual(dict(result.changes)["caller"], 1.25)
        self.assertAlmostEqual(dict(result.changes)["callee"], 0.0)
        self.assertTrue(all(value >= 0 for value in dict(result.contributions_before).values()))
        self.assertEqual(result.details["fit_mode"], "historical")
        pooled_result = fit_flow_contributions(xb, yb, xa, ya,
            **{**flow_params, "fit_mode": "pooled"})
        self.assertEqual(pooled_result.status, "ok")
        self.assertAlmostEqual(dict(pooled_result.changes)["caller"], 1.25)
        collinear = fit_flow_contributions([[1, 1], [2, 2], [3, 3]], [2, 4, 6],
            [[1, 1], [2, 2], [3, 3]], [3, 6, 9], **{**flow_params,
            "component_names": ("x", "y"), "minimum_validation_samples": 2})
        self.assertEqual(collinear.status, "abstain")
        invalid_yb = list(yb)
        invalid_yb[-1] = float("nan")
        self.assertEqual(fit_flow_contributions(xb, invalid_yb, xa, ya,
            **flow_params).status, "abstain")
        self.assertEqual(fit_flow_contributions(xb, [-1] + yb[1:], xa, ya,
            **flow_params).status, "abstain")
        huge = fit_flow_contributions(xb, [1e308] * len(xb), xa, [1e308] * len(xa),
            **{**flow_params, "maximum_relative_residual": 1e308})
        self.assertEqual(huge.status, "abstain")

    def test_tree_prediction_contributions_rank_real_explaining_group_not_decoy(self):
        before = [[float(i % 10), float((i // 10) % 2)] for i in range(200)]
        y_before = [row[0] for row in before]
        alert = [[float(5 + j), float(rep % 2)] for rep in range(10) for j in range(5)]
        y_alert = [row[0] for row in alert]
        kwargs = {"feature_names": ("true_metric", "decoy_metric"),
            "feature_groups": {"true_component": ("true_metric",),
                               "decoy_component": ("decoy_metric",)},
            "max_depth": 3, "min_samples_leaf": 1, "minimum_samples": 10,
            "validation_fraction": 0.25, "minimum_validation_samples": 10,
            "minimum_validation_r2": 0.9, "maximum_validation_normalized_error": 0.2,
            "residual_scale_floor": 0.5, "minimum_observed_kpi_change": 1.0,
            "minimum_contribution_fraction": 0.5, "seed": 7}
        result = tree_prediction_contributions(before, y_before, alert, y_alert, **kwargs)
        self.assertEqual(result.status, "ok")
        scores = dict(result.scores)
        self.assertAlmostEqual(scores["true_component"], 2.5)
        self.assertEqual(scores["decoy_component"], 0)
        unexplained = tree_prediction_contributions(before, y_before, alert,
            [value + 20 for value in y_alert], **kwargs)
        self.assertEqual(unexplained.reason, "no_validated_component_explains_observed_shift")

    def test_aho_corasick_matches_distinct_overlapping_token_sequences(self):
        tokens = ("new", "york", "city", "york", "city")
        patterns = (("new", "york"), ("york", "city"), ("city",), ("new", "york", "city"))
        self.assertEqual(token_sequence_matches(tokens, patterns, sequence_length=5,
            max_patterns=10, max_pattern_length=10),
            ((0, 0, 2), (3, 0, 3), (1, 1, 3), (2, 2, 3),
             (1, 3, 5), (2, 4, 5)))
        with self.assertRaises(ValueError):
            token_sequence_matches(tokens, patterns, sequence_length=4,
                max_patterns=10, max_pattern_length=10)
        with self.assertRaises(ValueError):
            token_sequence_matches(tokens, patterns, sequence_length=5,
                max_patterns=2, max_pattern_length=10)

    def test_observed_dependency_penalty_can_reorder_caller_and_callee(self):
        scores = {"caller": 8, "callee-a": 7, "callee-b": 5}
        edges = (("caller", "callee-a"), ("caller", "callee-b"))
        result = adjust_dependency_scores(scores, edges, downstream_penalty=0.5)
        reversed_input = adjust_dependency_scores(scores, tuple(reversed(edges)), downstream_penalty=0.5)
        self.assertEqual(result.ranked[0], "callee-a")
        self.assertEqual(dict(result.scores)["caller"], 4.5)
        self.assertEqual(result.scores, reversed_input.scores)
        reverse_graph = adjust_dependency_scores({"caller": 8, "callee": 7},
            (("callee", "caller"),), downstream_penalty=0.5)
        self.assertEqual(reverse_graph.ranked[0], "caller")
        self.assertEqual(adjust_dependency_scores({"caller": 1, "callee": 4},
            (("caller", "callee"),), downstream_penalty=1).status, "ok")
        self.assertEqual(adjust_dependency_scores({"caller": 1, "callee": 4},
            (("caller", "callee"),), downstream_penalty=1).scores,
            (("callee", 4.0), ("caller", 0.0)))
        self.assertEqual(adjust_dependency_scores({"a": 1, "b": 1},
            (("a", "b"), ("b", "a")), downstream_penalty=1).status, "abstain")
        self.assertEqual(adjust_dependency_scores({"caller": 8}, (),
            downstream_penalty=0.5).status, "abstain")


if __name__ == "__main__":
    unittest.main()
