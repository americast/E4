import unittest

from e4core.m5_comparison import paired_comparison


def slots(success_at=None, rank=3):
    result = []
    for attempt in range(1, 4):
        if attempt == success_at:
            result.append({"attempt": attempt, "status": "submitted", "accepted": True,
                           "valid": True, "correct_rank": rank})
        elif success_at is not None and attempt > success_at:
            result.append({"attempt": attempt, "status": "not_run", "accepted": False,
                           "valid": None, "correct_rank": None})
        else:
            result.append({"attempt": attempt, "status": "submitted", "accepted": False,
                           "valid": True, "correct_rank": None})
    return result


class ComparisonTests(unittest.TestCase):
  def test_accuracy_at_3_by_attempt_prefix_not_rank_cutoff(self):
    cases = {"case_a": {"attempts": slots(success_at=1, rank=3),
                         "cost": {"model_calls": 2, "wall_seconds": 10}},
             "case_b": {"attempts": slots(success_at=3, rank=1),
                         "cost": {"model_calls": 3, "wall_seconds": 20}}}
    result = paired_comparison({"E4": cases, "Terminus-2": cases},
                               expected_case_ids=["case_a", "case_b"],
                               bootstrap_costs={"E4": {"model_calls": 6, "wall_seconds": 6}})
    e4 = result["systems"]["E4"]
    self.assertEqual(e4["accuracy_at_3_1"], 0.5)
    self.assertEqual(e4["accuracy_at_3_2"], 0.5)
    self.assertEqual(e4["accuracy_at_3_3"], 1)
    self.assertEqual(e4["aggregate_cost"]["wall_seconds"], 30)
    self.assertEqual(e4["aggregate_cost_with_bootstrap"]["wall_seconds"], 36)
    self.assertEqual(e4["aggregate_cost_with_bootstrap_amortized_per_case"]["wall_seconds"], 18)


  def test_compile_failure_consumes_original_slot_and_unrun_slots_remain_false(self):
    case = {"attempts": [
        {"attempt": 1, "status": "failed", "accepted": False, "valid": False,
         "correct_rank": None},
        {"attempt": 2, "status": "submitted", "accepted": True, "valid": True,
         "correct_rank": 2},
        {"attempt": 3, "status": "not_run", "accepted": False, "valid": None,
         "correct_rank": None}], "cost": {}}
    result = paired_comparison({"E4": {"case_a": case},
                                "Terminus-2": {"case_a": case}},
                               expected_case_ids=["case_a"])
    self.assertEqual(result["systems"]["E4"]["accuracy_at_3_1"], 0)
    self.assertEqual(result["systems"]["E4"]["accuracy_at_3_2"], 1)
    self.assertIsNone(result["systems"]["E4"]["aggregate_cost"]["model_calls"])


  def test_partial_cost_has_null_total_and_explicit_known_subtotal(self):
    cases = {"a": {"attempts": slots(), "cost": {"model_calls": 10}},
             "b": {"attempts": slots(), "cost": {"model_calls": None}}}
    report = paired_comparison({"E4": cases, "Terminus-2": cases},
                               expected_case_ids=["a", "b"])
    summary = report["systems"]["E4"]
    self.assertIsNone(summary["aggregate_cost"]["model_calls"])
    self.assertEqual(summary["known_cost_subtotals_not_complete"]["model_calls"], 10)
    self.assertEqual(summary["cost_measurement_coverage"]["model_calls"],
                     {"measured_cases": 1, "total_cases": 2})


  def test_comparison_rejects_case_mismatch_overrun_and_inconsistent_acceptance(self):
    with self.assertRaises(ValueError):
        paired_comparison({"E4": {}, "Terminus-2": {}}, expected_case_ids=["case_a"])
    malformed = {"attempts": slots(), "cost": {}}
    malformed["attempts"][0] = {"attempt": 1, "status": "submitted", "accepted": True,
                                "valid": False, "correct_rank": None}
    with self.assertRaises(ValueError):
        paired_comparison({"E4": {"case_a": malformed},
                           "Terminus-2": {"case_a": {"attempts": slots(), "cost": {}}}},
                          expected_case_ids=["case_a"])


if __name__ == "__main__":
    unittest.main()
