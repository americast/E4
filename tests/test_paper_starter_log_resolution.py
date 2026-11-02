import hashlib
import unittest

from e4core.contracts import (Candidate, CandidateSet, EntityRef, PaperLogPatternSummary,
    PaperLogSummary, PaperStarterInput, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.paper_starter import (_bank_log_pattern_contrast,
    validate_log_pattern_configuration)
from e4core.runtime import ParameterSpec
from e4core.paper_starter import make_paper_starter_registry


def make_starter(*, patterns=64, block_count=30, volume_only=False, target_shift=True):
    reference = Window("reference", 0, block_count * 60_000)
    alert = Window("alert", block_count * 60_000, 2 * block_count * 60_000)
    component, log_name = "Tomcat-fixture", "application-log"
    summaries = []
    source_hashes = ("f" * 64,)
    rows_scanned = 0
    rows_by_window = {}
    for window, start in ((reference, reference.start_ms_utc),
                          (alert, alert.start_ms_utc)):
        window_rows = 0
        for block_index in range(block_count):
            block_start = start + block_index * 60_000
            denominator = (256 if window.name == "reference" or not volume_only else 512)
            window_rows += denominator
            for index in range(patterns):
                tokens = (f"motif_{index:03d}", "request")
                if index == 0 and target_shift and window.name == "alert":
                    row_support = 90
                elif volume_only and window.name == "alert":
                    row_support = 2
                else:
                    row_support = 1
                pattern_hash = hashlib.sha256("\0".join(tokens).encode()).hexdigest()
                summaries.append(PaperLogPatternSummary(component, log_name, window,
                    block_start, 60_000, tokens, pattern_hash, row_support,
                    denominator, denominator, source_hashes))
        rows_by_window[window.name] = window_rows
        rows_scanned += window_rows
    summaries = [PaperLogPatternSummary(row.component_id, row.log_name, row.window,
        row.block_start_ms_utc, row.block_duration_ms, row.tokens, row.pattern_sha256,
        row.row_support, row.block_row_count, rows_scanned, row.source_sha256)
        for row in summaries]
    log_summaries = tuple(PaperLogSummary(component, log_name, window,
        rows_by_window[window.name], rows_scanned, source_hashes)
        for window in (reference, alert))
    telemetry = Telemetry("log-resolution-fixture", reference, alert, (), (),
        {component: {"role": "component"}}, "a" * 64, {}, {}, {}, "log-fixture/1")
    starter = PaperStarterInput(telemetry, None, None,
        {"sessions": "unsupported", "urls": "unsupported", "native_logs": "available",
         "native_log_patterns": "available"}, 60_000, log_summaries, tuple(summaries))
    return starter, CandidateSet((Candidate(EntityRef("component", component)),), "fixture")


def run_log_scorer(starter, candidates, *, maximum_patterns=64, resamples=1999,
                   minimum_blocks=3):
    params = {"minimum_pattern_support": 2, "minimum_blocks": minimum_blocks,
        "maximum_patterns": maximum_patterns, "maximum_pattern_length": 4,
        "alpha": 0.05, "resamples": resamples,
        "relative_scale_floor": 0.05, "absolute_scale_floor": 1e-9}
    context = NodeContext("log-resolution-fixture", "log-score",
        "paper.bank.log_pattern_contrast", {}, {}, starter.telemetry.reference_window,
        starter.telemetry.alert_window, starter.telemetry.data_sha256)
    return _bank_log_pattern_contrast({"data": starter, "candidates": candidates},
                                      params, context).value


class PaperStarterLogResolutionTests(unittest.TestCase):
    def test_default_registered_configuration_has_enough_resolution_for_full_family(self):
        registry = make_paper_starter_registry(bank_only=True)
        spec = registry.require("paper.bank.log_pattern_contrast", "1")
        params = {name: parameter.default for name, parameter in spec.parameters.items()
                  if parameter.has_default}
        result = validate_log_pattern_configuration(params)
        self.assertEqual(params["maximum_patterns"], 64)
        self.assertEqual(params["resamples"], 1999)
        self.assertEqual(result["minimum_resamples"], 1279)
        self.assertLessEqual(result["minimum_monte_carlo_p"], result["local_alpha"])
        with self.assertRaisesRegex(ValueError, "resolution"):
            validate_log_pattern_configuration({**params, "resamples": 999})

    def test_strong_shift_in_one_of_64_patterns_can_pass_familywise_gate(self):
        starter, candidates = make_starter()
        result = run_log_scorer(starter, candidates)
        self.assertEqual(result.status, "ready")
        self.assertGreater(result.candidates[0].score, 0)
        details = result.candidates[0].evidence[0].details
        self.assertEqual(details["patterns_tested"], 64)
        self.assertEqual(details["patterns_limited_by_exact_p_resolution"], 0)
        self.assertLessEqual(details["permutation_resolution"]["minimum_monte_carlo_p"],
                             details["permutation_resolution"]["local_alpha"])

    def test_changing_only_log_volume_keeps_pattern_composition_score_zero(self):
        starter, candidates = make_starter(volume_only=True, target_shift=False)
        result = run_log_scorer(starter, candidates)
        self.assertEqual(result.status, "abstained")
        self.assertEqual(result.candidates[0].score, 0.0)
        self.assertEqual(result.status_details["exact_resolution_limited_pattern_count"], 0)

    def test_exact_test_with_three_pairs_abstains_when_its_grid_cannot_reach_alpha(self):
        starter, candidates = make_starter(patterns=1, block_count=3)
        result = run_log_scorer(starter, candidates, maximum_patterns=1, minimum_blocks=3)
        self.assertEqual(result.status, "abstained")
        self.assertEqual(result.candidates[0].score, 0.0)
        details = result.candidates[0].evidence[0].details
        self.assertEqual(details["exact_permutation_tests"], 1)
        self.assertEqual(details["patterns_limited_by_exact_p_resolution"], 1)
        self.assertEqual(result.status_details["reason"],
            "exact_permutation_resolution_coarser_than_local_alpha")


if __name__ == "__main__":
    unittest.main()
