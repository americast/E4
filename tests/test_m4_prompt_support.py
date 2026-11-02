"""Lossless prompt-only interning of repeated V2 case support payloads."""
from __future__ import annotations

import unittest
import json

from e4core.m4_prompt_support import (PromptSupportError,
    expand_case_support_profiles, intern_case_support_profiles)


class PromptSupportProfileTests(unittest.TestCase):
    def test_identical_full_case_support_payloads_share_one_definition_losslessly(self):
        support = {"status": "resolved_multiple", "match_count": 2,
            "semantic_profile_refs": ["gauge.cpu@1"],
            "reference_support_histogram": {"11": 2},
            "alert_support_histogram": {"12": 2}}
        rows = [
            {"selector_ref": "rule.one@1", "description": "one rule",
                "case_resolution": support},
            {"selector_ref": "rule.two@1", "description": "another rule",
                "case_resolution": support},
            {"selector_ref": "rule.three@1", "description": "unsupported",
                "case_resolution": {"status": "unavailable", "match_count": 0,
                    "semantic_profile_refs": [],
                    "reference_support_histogram": {}, "alert_support_histogram": {}}},
        ]
        projected, catalog = intern_case_support_profiles(rows)
        self.assertEqual([row["selector_ref"] for row in projected],
            [row["selector_ref"] for row in rows])
        self.assertEqual(catalog["row_order"], [row["selector_ref"] for row in rows])
        self.assertEqual(len(catalog["profiles"]), 2)
        self.assertEqual(projected[0]["support_profile_ref"], projected[1]["support_profile_ref"])
        self.assertNotIn("case_resolution", projected[0])
        self.assertEqual(expand_case_support_profiles(projected, catalog), rows)

    def test_missing_null_zero_and_near_equal_payloads_remain_distinct(self):
        rows = [
            {"selector_ref": "missing"},
            {"selector_ref": "explicit-null", "case_resolution": None},
            {"selector_ref": "zero-int", "case_resolution": {"match_count": 0}},
            {"selector_ref": "zero-float", "case_resolution": {"match_count": 0.0}},
            {"selector_ref": "missing-field", "case_resolution": {"status": "unavailable"}},
            {"selector_ref": "explicit-field-null", "case_resolution": {
                "status": "unavailable", "match_count": None}},
        ]
        projected, catalog = intern_case_support_profiles(rows)
        self.assertNotIn("support_profile_ref", projected[0])
        self.assertEqual(len(catalog["profiles"]), 5)
        refs = {row["selector_ref"]: row.get("support_profile_ref") for row in projected}
        self.assertNotEqual(refs["zero-int"], refs["zero-float"])
        self.assertNotEqual(refs["missing-field"], refs["explicit-field-null"])
        self.assertEqual(expand_case_support_profiles(projected, catalog), rows)

    def test_high_level_enum_values_keep_exact_literal_order_and_round_trip(self):
        rows = [
            {"value": "OSLinux-CPU_CPU_CPUCpuUtil", "case_support": {"status": "resolved_one",
                "match_count": 1, "reference_support_histogram": {"18": 2}}},
            {"value": "", "case_support": {"status": "unavailable", "match_count": 0}},
            {"value": "field with spaces / punctuation", "description": "retain exact name"},
        ]
        projected, catalog = intern_case_support_profiles(rows,
            ref_key="value", support_key="case_support")
        self.assertEqual(catalog["ref_key"], "value")
        self.assertEqual(catalog["support_key"], "case_support")
        self.assertEqual(catalog["row_order"], [row["value"] for row in rows])
        self.assertEqual(projected[2]["value"], rows[2]["value"])
        self.assertEqual(expand_case_support_profiles(projected, catalog), rows)

    def test_profile_refs_are_stable_and_semantic_refs_are_part_of_identity(self):
        left = {"status": "resolved_one", "match_count": 1,
            "semantic_profile_refs": ["gauge.a@1"], "reference_support_histogram": {"18": 1}}
        same = dict(left)
        different = {**left, "semantic_profile_refs": ["counter.b@1"]}
        rows = [{"selector_ref": "r1", "case_resolution": left},
            {"selector_ref": "r2", "case_resolution": same},
            {"selector_ref": "r3", "case_resolution": different}]
        first_rows, first_catalog = intern_case_support_profiles(rows)
        _second_rows, second_catalog = intern_case_support_profiles(rows)
        self.assertEqual(first_catalog, second_catalog)
        self.assertEqual(first_rows[0]["support_profile_ref"], first_rows[1]["support_profile_ref"])
        self.assertNotEqual(first_rows[0]["support_profile_ref"], first_rows[2]["support_profile_ref"])
        _reordered_rows, reordered_catalog = intern_case_support_profiles(list(reversed(rows)))
        self.assertEqual(first_catalog["profiles"], reordered_catalog["profiles"])
        ids_by_payload = {json.dumps(original["case_resolution"], sort_keys=True): projected[
            "support_profile_ref"] for original, projected in zip(rows, first_rows)}
        reordered = list(reversed(rows))
        reordered_ids = {json.dumps(original["case_resolution"], sort_keys=True): projected[
            "support_profile_ref"] for original, projected in zip(reordered, _reordered_rows)}
        self.assertEqual(ids_by_payload, reordered_ids)

    def test_malformed_identities_and_tampered_projection_fail_closed(self):
        with self.assertRaises(PromptSupportError):
            intern_case_support_profiles([{"description": "missing ref"}])
        with self.assertRaises(PromptSupportError):
            intern_case_support_profiles([{"selector_ref": 1, "case_resolution": {}}])
        with self.assertRaisesRegex(PromptSupportError, "keys must be strings"):
            intern_case_support_profiles([{"selector_ref": "r", "case_resolution": {1: "a"}}])
        with self.assertRaisesRegex(PromptSupportError, "non-JSON value tuple"):
            intern_case_support_profiles([{"selector_ref": "r", "case_resolution": (1, 2)}])
        projected, catalog = intern_case_support_profiles([
            {"selector_ref": "r", "case_resolution": {"status": "unavailable"}}])
        altered = [dict(projected[0], selector_ref="different")]
        with self.assertRaisesRegex(PromptSupportError, "identities/order"):
            expand_case_support_profiles(altered, catalog)


if __name__ == "__main__":
    unittest.main()
