from __future__ import annotations

import hashlib
import json
import unittest

from e4core.m4_prompt_digest import compact_paper_case_digest


def _sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class PaperPromptDigestTests(unittest.TestCase):
    def test_field_inventory_is_bounded_hash_pinned_and_not_a_selector_enum(self):
        fields = [f"OSLinux-OSLinux_FILESYSTEM_-mount-{index:03d}_FSCapacity"
            for index in range(337)]
        digest = {"case_id": "public-case", "available_metric_fields": fields,
            "digest_sha256": "a" * 64,
            "metric_selector_catalog": ["bank.bmc.oslinux.filesystem_capacity_percent@1"]}
        compact = compact_paper_case_digest(digest, stage="compose")
        inventory = compact["available_metric_fields"]
        self.assertEqual(inventory["status"], "available")
        self.assertEqual(inventory["observed_unique_field_count"], 337)
        self.assertEqual(inventory["field_names_sha256"], _sha(sorted(fields)))
        self.assertEqual(inventory["represented_field_count"], 12)
        self.assertEqual(inventory["omitted_field_count"], 325)
        self.assertEqual(inventory["selector_catalog_ref"], "paper.bank_component_metric_fields")
        self.assertEqual(inventory["examples"], sorted(fields)[:12])
        self.assertLess(len(json.dumps(compact, ensure_ascii=False)),
            len(json.dumps(digest, ensure_ascii=False)))
        self.assertEqual(compact["metric_selector_catalog"], digest["metric_selector_catalog"])
        self.assertEqual(inventory["related_selector_catalog_refs"],
            ["paper.bank_component_metric_rule_refs"])
        self.assertIn("not legal selectors", inventory["omission_reason"])
        self.assertEqual(compact["source_digest_sha256"], "a" * 64)
        self.assertNotIn("available_metric_fields", repr(compact["available_metric_fields"]["examples"]))

    def test_field_inventory_distinguishes_absent_from_explicitly_empty(self):
        absent = compact_paper_case_digest({"case_id": "x"}, stage="select")
        self.assertEqual(absent["available_metric_fields"]["status"], "unavailable")
        self.assertIsNone(absent["available_metric_fields"]["observed_unique_field_count"])
        self.assertIsNone(absent["available_metric_fields"]["omitted_field_count"])

        empty = compact_paper_case_digest({"case_id": "x", "available_metric_fields": []},
            stage="select")
        self.assertEqual(empty["available_metric_fields"]["status"], "available_empty")
        self.assertEqual(empty["available_metric_fields"]["observed_unique_field_count"], 0)
        self.assertEqual(empty["available_metric_fields"]["omitted_field_count"], 0)
        self.assertEqual(empty["available_metric_fields"]["field_names_sha256"], _sha([]))

        evolve_absent = compact_paper_case_digest({}, stage="evolve")
        self.assertEqual(evolve_absent["primary_app_kpi_context"]["status"], "unavailable")
        self.assertIsNone(evolve_absent["primary_app_kpi_context"]["original_item_count"])
        self.assertEqual(evolve_absent["log_volume_series"]["status"], "unavailable")
        self.assertIsNone(evolve_absent["log_volume_series"]["known_total_item_count"])

        evolve_empty = compact_paper_case_digest({"primary_app_kpi_context": {},
            "log_volume_series": [], "log_volume_series_omitted": 0,
            "available_metric_fields": []}, stage="evolve")
        self.assertEqual(evolve_empty["primary_app_kpi_context"]["status"], "available_empty")
        self.assertEqual(evolve_empty["log_volume_series"]["status"], "available_empty")
        self.assertEqual(evolve_empty["log_volume_series"]["known_total_item_count"], 0)

    def test_evolve_references_kpi_and_log_capability_summaries_with_omission_hashes(self):
        digest = {"case_id": "public-case", "timezone": "Asia/Shanghai",
            "reference_window_utc_ms": [1, 2], "alert_window_utc_ms": [2, 3],
            "file_sha256": {"metric.csv": "b" * 64},
            "exact_observed_component_ids": ["component-a"],
            "available_metric_fields": ["field-a", "field-b"],
            "primary_app_kpi_context": {"selected_identity": "opaque-kpi", "support": 40},
            "primary_app_kpi_policy_sha256": "c" * 64,
            "component_identity_coverage": {"unresolved": 2},
            "quality_counts": {"invalid": 1}, "log_summary": {"pattern_count": 5},
            "metric_series": [{"metric": f"m{i}"} for i in range(20)],
            "metric_series_omitted": 3,
            "app_kpi_context_series": [{"metric": "mrt"}],
            "trace_series": [{"metric": "duration"}],
            "log_volume_series": [{"log_name": f"log-{i}"} for i in range(18)],
            "log_volume_series_omitted": 4}
        compact = compact_paper_case_digest(digest, stage="evolve")
        for key in ("case_id", "timezone", "reference_window_utc_ms", "alert_window_utc_ms",
                    "file_sha256", "exact_observed_component_ids",
                    "primary_app_kpi_policy_sha256", "component_identity_coverage",
                    "quality_counts", "log_summary"):
            self.assertEqual(compact[key], digest[key])
        self.assertNotIn("metric_series", compact)
        self.assertNotIn("app_kpi_context_series", compact)
        self.assertNotIn("trace_series", compact)
        self.assertEqual(compact["metric_series_omitted"], 3)
        self.assertEqual(compact["metric_series_omitted_by_prompt_projection"], 20)
        self.assertEqual(compact["metric_series_known_total_count"], 23)
        kpi_ref = compact["primary_app_kpi_context"]
        self.assertEqual(kpi_ref["capability_summary_ref"], "primary_kpi")
        self.assertEqual(kpi_ref["original_item_count"], 2)
        self.assertEqual(kpi_ref["original_content_sha256"], _sha(digest["primary_app_kpi_context"]))
        self.assertEqual(kpi_ref["omitted_by_prompt_projection_count"], 2)
        log_ref = compact["log_volume_series"]
        self.assertNotIn("log-0", repr(log_ref))
        self.assertEqual(log_ref["capability_summary_ref"],
            "computations.native_log_pattern_composition")
        self.assertEqual(log_ref["denominator_summary_ref"],
            "computations.native_log_pattern_composition.window_denominators_by_stream")
        self.assertEqual(log_ref["original_item_count"], 18)
        self.assertEqual(log_ref["original_content_sha256"], _sha(digest["log_volume_series"]))
        self.assertEqual(log_ref["omitted_by_prompt_projection_count"], 18)
        self.assertEqual(log_ref["source_omitted_item_count"], 4)
        self.assertEqual(log_ref["known_total_item_count"], 22)
        self.assertEqual(compact["prompt_digest_projection"]["selector_catalog_ref"],
            "metric_selector_catalog")
        self.assertEqual(compact["available_metric_fields"]["related_selector_catalog_refs"], [])

    def test_compose_and_serve_keep_examples_and_deterministic_stage_refs(self):
        digest = {"available_metric_fields": ["field-b", "field-a"],
            "metric_series": [{"metric": "field-a"}],
            "log_volume_series": [{"log_name": "service.log", "count": 2}]}
        compose = compact_paper_case_digest(digest, stage="compose")
        serve = compact_paper_case_digest(digest, stage="stage3_serve")
        self.assertEqual(compose["metric_series"], digest["metric_series"])
        self.assertEqual(serve["log_volume_series"], digest["log_volume_series"])
        self.assertEqual(compose["available_metric_fields"]["selector_catalog_ref"],
            "paper.bank_component_metric_fields")
        self.assertEqual(serve["available_metric_fields"]["selector_catalog_ref"],
            "paper.bank_component_metric_fields")

    def test_invalid_stage_or_malformed_metric_inventory_fails_closed(self):
        with self.assertRaises(ValueError):
            compact_paper_case_digest({}, stage="legacy_v1")
        with self.assertRaises(ValueError):
            compact_paper_case_digest({"available_metric_fields": ["ok", None]}, stage="compose")
        with self.assertRaises(TypeError):
            compact_paper_case_digest([], stage="compose")


if __name__ == "__main__":
    unittest.main()
