from __future__ import annotations

import unittest

from scripts import audit_bank_metric_coverage as audit


class BankMetricCoverageTests(unittest.TestCase):
    def test_exact_documentation_match_does_not_expand_to_substrings(self):
        redis = audit._semantic_name_evidence(
            "redis-Redis_6379_Redis  (used_memory_rss)")
        self.assertEqual(redis["family"], "redis_info_exact_key_candidate")
        self.assertEqual(redis["documented_candidate"]["kind"], "gauge")
        self.assertEqual(redis["documented_candidate"]["unit"], "bytes")
        self.assertTrue(redis["candidate_ranking_eligible"])
        self.assertEqual(audit._semantic_name_evidence(
            "redis-Redis_6379_Redis  (used_memory_rss_proxy)")["family"],
            "redis_info_exact_key_candidate")
        self.assertIsNone(audit._semantic_name_evidence(
            "redis-Redis_6379_Redis  (used_memory_rss_proxy)")["documented_candidate"])
        self.assertFalse(audit._semantic_name_evidence(
            "redis-Redis_6379_Redis  (evicted_keys)")["candidate_ranking_eligible"])

        bmc = audit._semantic_name_evidence(
            "OSLinux-OSLinux_MEMORY_MEMORY_MEMUsedMemPerc")
        self.assertEqual(bmc["family"], "bmc_patrol_parameter_name_candidate")
        self.assertEqual(bmc["documented_candidate"]["unit"], "percent (0-100)")
        self.assertEqual(audit._semantic_name_evidence(
            "OSLinux-OSLinux_MEMORY_MEMORY_NoCacheMemPerc")["family"],
            "no exact documented match")

    def test_net_output_error_is_not_latency_and_unknown_fields_stay_unknown(self):
        raw = audit._semantic_name_evidence("OSLinux-OSLinux_NETWORK_ens160_NETOutErr")
        self.assertEqual(raw["family"], "no exact documented match")
        documented = audit._semantic_name_evidence("OSLinux-OSLinux_NETWORK_ens160_NETOutErrPrc")
        self.assertIn("not request latency", documented["documented_candidate"]["meaning"])
        self.assertEqual(documented["candidate_ranking_eligible"], True)

    def test_component_identifier_does_not_change_metric_semantics(self):
        left = audit._semantic_name_evidence(
            "Container-DOCKER_CONTAINER_2c2336e2994f--role-a_CpuPercent")
        right = audit._semantic_name_evidence(
            "Container-DOCKER_CONTAINER_f861998c398e--role-b_CpuPercent")
        self.assertEqual(left, right)
        self.assertEqual(left["documented_candidate"]["kind"], "gauge")
        self.assertFalse(left["candidate_ranking_eligible"])

    def test_temporal_monotonicity_is_only_a_shape_summary(self):
        self.assertEqual(audit._seq_shape([(1, 1.0), (2, 2.0), (3, 3.0)]), "nondecreasing")
        self.assertEqual(audit._seq_shape([(1, 3.0), (2, 2.0), (3, 1.0)]), "nonincreasing")
        self.assertEqual(audit._seq_shape([(1, 1.0), (2, 3.0), (3, 2.0)]), "mixed")
        self.assertEqual(audit._seq_shape([(1, 1.0)]), "insufficient_points")
        self.assertEqual(audit._semantic_name_evidence("VendorMetric_maybeCounter")["family"],
            "no exact documented match")

    def test_per_component_coverage_keeps_windows_missingness_and_allowlist_separate(self):
        fields = {"Known": {"entities": {"component-a": {"dev-case": {
            "reference": {"rows": 2, "valid": 2, "timestamps": {1: [1.0], 2: [2.0]}},
            "alert": {"rows": 1, "valid": 0, "timestamps": {}}}}},
            "cases": {"dev-case"}, "rows": 3, "finite": 2, "min": 1.0, "max": 2.0,
            "missing": 0, "invalid": 1, "shape_counts": {"reference:nondecreasing": 1}},
            "Unknown": {"entities": {"component-b": {"dev-case": {
            "reference": {"rows": 1, "valid": 1, "timestamps": {1: [5.0]}}}}},
            "cases": {"dev-case"}, "rows": 1, "finite": 1, "min": 5.0, "max": 5.0,
            "missing": 0, "invalid": 0, "shape_counts": {"reference:insufficient_points": 1}}}
        inventory = audit._field_inventory(fields, {"Known": {"kind": "gauge",
            "unit": "undeclared", "ranking_eligible": True}}, ["dev-case"])
        self.assertEqual(inventory["distinct_field_count"], 2)
        self.assertEqual(inventory["current_allowlist_fields_observed"], 1)
        known = inventory["fields"][0]
        self.assertEqual(known["component_case_pairs_with_both_windows"], 0)
        self.assertEqual(known["finite_numeric_rows"], 2)
        self.assertEqual(known["nonnumeric_or_nonfinite_value_rows"], 1)
        unknown = inventory["fields"][1]
        self.assertFalse(unknown["current_policy"]["ranking_eligible"])
        self.assertEqual(unknown["per_component_coverage"][0]["component_id"], "component-b")

    def test_family_size_is_counted_after_support_but_before_effect_or_p_value(self):
        fields = []
        for name, eligible, support in (("a", True, 6), ("b", False, 6), ("c", True, 3)):
            fields.append({"field_name": name,
                "current_policy": {"kind": "gauge" if eligible else "unknown",
                    "ranking_eligible": eligible},
                "per_component_coverage": [{"component_id": "opaque",
                    "development_cases": [{"development_case_id": "dev",
                        "reference": {"unique_timestamps": support,
                            "nonempty_independent_blocks_by_minutes": {"5": 6}},
                        "alert": {"unique_timestamps": support,
                            "nonempty_independent_blocks_by_minutes": {"5": 6}}}]}]})
        resolution = audit._hypothesis_resolution({"fields": fields},
            minimum_timestamps=4, minimum_blocks=4, block_minutes=5,
            resamples=499, family_alpha=.05)
        case = resolution["cases"]["dev"]
        self.assertEqual(case["eligible_component_field_pairs_before_effect_or_p_selection"], 1)
        self.assertEqual(case["current_policy_family_resolution"]["hypotheses"], 2)
        self.assertEqual(case["all_dev_fields_upper_bound_resolution"]["hypotheses"], 4)
        self.assertEqual(case["current_policy_family_resolution"]["monte_carlo_minimum_p_value"], .002)

    def test_paper_family_is_per_candidate_and_uses_one_minute_exact_grid(self):
        fields = []
        for name, eligible, component, support in (
                ("a", True, "caller", 3), ("b", True, "caller", 4),
                ("c", True, "callee", 2), ("d", False, "caller", 6)):
            fields.append({"field_name": name,
                "current_policy": {"kind": "gauge" if eligible else "unknown",
                    "ranking_eligible": eligible},
                "per_component_coverage": [{"component_id": component,
                    "development_cases": [{"development_case_id": "dev",
                        "reference": {"unique_timestamps": support,
                            "nonempty_independent_blocks_by_minutes": {"1": support}},
                        "alert": {"unique_timestamps": support,
                            "nonempty_independent_blocks_by_minutes": {"1": support}}}]}]})
        result = audit._paper_backend_family_resolution({"fields": fields},
            block_minutes=1, minimum_samples=3, resamples=999, alpha=.05)
        case = result["cases"]["dev"]
        self.assertEqual(case["support_qualified_fields_by_component"], {"caller": 2})
        self.assertEqual(case["maximum_per_candidate_field_family"], 2)
        self.assertFalse(case["all_support_qualified_tests_can_reach_cutoff"])
        self.assertEqual(case["actual_test_resolution_p_floor_range"]["maximum"], .1)


if __name__ == "__main__":
    unittest.main()
