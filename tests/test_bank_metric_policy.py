from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

from e4core.bank_metric_policy import (MetricSemantics, classify_bank_metric,
    load_bank_metric_policy)
from e4core.paper_starter import make_paper_starter_registry

ROOT = Path(__file__).resolve().parents[1]


class BankMetricPolicyTests(unittest.TestCase):
    def test_current_exact_allowlist_entry_is_preserved_with_undeclared_unit(self):
        result = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_MEMORY_MEMORY_MEMUsedMemPerc")
        self.assertIsInstance(result, MetricSemantics)
        self.assertEqual(result.kind, "gauge")
        self.assertEqual(result.unit, "undeclared native value")
        self.assertEqual(result.unit_status, "undeclared")
        self.assertEqual(result.direction, "two_sided")
        self.assertTrue(result.ranking_eligible)
        self.assertIn("working", result.classification_status)
        self.assertTrue(any("development day" in row for row in result.provenance))
        with self.assertRaises(FrozenInstanceError):
            result.kind = "counter"

    def test_unknown_names_and_wrong_tables_never_match_by_substring(self):
        for field in ("MEMUsedMemPercExtra", "Redis used_memory_rss", "NETOutErr", "x_CPULoad"):
            result = classify_bank_metric("bank/metric_container.csv", field)
            self.assertEqual(result.kind, "unknown", field)
            self.assertFalse(result.ranking_eligible, field)
            self.assertEqual(result.unit_status, "undeclared")
        with self.assertRaises(ValueError):
            classify_bank_metric("bank/metric_container.csv.bak", "MEMUsedMemPerc")

    def test_application_trace_and_raw_log_fields_remain_conservative(self):
        for table, field in (("bank/metric_app.csv", "mrt"),
                             ("bank/metric_app.csv", "rr"),
                             ("bank/trace_span.csv", "duration"),
                             ("bank/log/log_service.csv", "value")):
            result = classify_bank_metric(table, field)
            self.assertEqual(result.kind, "unknown")
            self.assertFalse(result.ranking_eligible)
        log_count = classify_bank_metric("bank/log/log_service.csv", "log_volume_count")
        self.assertEqual(log_count.kind, "event_count")
        self.assertEqual(log_count.unit_status, "derived_from_source_rows")
        self.assertEqual(log_count.transform, "count_rows_per_fixed_minute_bucket")
        self.assertTrue(log_count.ranking_eligible)
        self.assertIn("log_name", log_count.derived_from)

    def test_exact_policy_counter_override_keeps_reset_transform_provenance(self):
        policy_path = ROOT / "config/bank_metric_kind_policy.json"
        doc = json.loads(policy_path.read_text(encoding="utf-8"))
        doc["source_semantics"]["metric_container"]["ranking_allowlist"]["ExampleCounter"] = {
            "kind": "counter", "unit": "requests", "unit_status": "documented",
            "direction": "two_sided", "ranking_eligible": False,
            "transform": "counter_rate", "reset_policy": "reset_to_zero_or_abstain",
            "classification_status": "documented", "rule_id": "test/example-counter/v1",
            "provenance": ["independent synthetic policy fixture"]}
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "policy.json"
            fixture.write_text(json.dumps(doc), encoding="utf-8")
            result = classify_bank_metric("bank/metric_container.csv", "ExampleCounter",
                policy_path=fixture)
        self.assertEqual(result.kind, "counter")
        self.assertEqual(result.transform, "counter_rate")
        self.assertEqual(result.reset_policy, "reset_to_zero_or_abstain")
        self.assertFalse(result.ranking_eligible)
        self.assertEqual(result.rule_id, "test/example-counter/v1")

    def test_policy_pattern_requires_explicit_anchors_and_exact_source_table(self):
        doc = json.loads((ROOT / "config/bank_metric_kind_policy.json").read_text(encoding="utf-8"))
        doc["source_semantics"]["metric_container"]["rules"] = [{
            "rule_id": "test/container-family/v1",
            "field_pattern": r"^Vendor_([0-9]+)_CpuPercent$",
            "kind": "gauge", "unit": "percent", "unit_status": "documented",
            "direction": "two_sided", "ranking_eligible": True,
            "classification_status": "documented_rule",
            "provenance": ["synthetic anchored rule fixture"]}]
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "policy.json"
            fixture.write_text(json.dumps(doc), encoding="utf-8")
            self.assertEqual(classify_bank_metric("bank/metric_container.csv",
                "Vendor_12_CpuPercent", policy_path=fixture).rule_id,
                "test/container-family/v1")
            unknown = classify_bank_metric("bank/metric_container.csv",
                "prefix_Vendor_12_CpuPercent_suffix", policy_path=fixture)
            self.assertEqual(unknown.kind, "unknown")
            wrong_table = classify_bank_metric("bank/metric_app.csv",
                "Vendor_12_CpuPercent", policy_path=fixture)
            self.assertEqual(wrong_table.kind, "unknown")

    def test_selector_catalog_is_stable_and_exact_resolution_is_case_bound(self):
        policy = load_bank_metric_policy()
        fields = policy.exact_rankable_fields("bank/metric_container.csv")
        catalog = policy.selector_catalog("bank/metric_container.csv")
        self.assertEqual(len(fields), 30)
        self.assertEqual(sum(row.selector_kind == "exact" for row in catalog), 30)
        self.assertEqual(sum(row.selector_kind == "pattern" for row in catalog), 18)
        entry = next(row for row in catalog
            if row.exact_field_name == "OSLinux-OSLinux_MEMORY_MEMORY_MEMUsedMemPerc")
        digest = "a" * 64
        matched = policy.resolve_selector(entry.selector_ref, "bank/metric_container.csv",
            [entry.exact_field_name], case_data_sha256=digest)
        self.assertEqual(matched.status, "resolved_one")
        self.assertEqual(matched.matches[0].field_name, entry.exact_field_name)
        self.assertEqual(matched.policy_sha256, policy.sha256)
        absent = policy.resolve_selector(entry.selector_ref, "bank/metric_container.csv",
            [], case_data_sha256=digest)
        self.assertEqual(absent.status, "unavailable")
        self.assertEqual(absent.matches, ())

    def test_curated_versioned_redis_bmc_and_log_rules_keep_units_and_exclusions_explicit(self):
        redis = classify_bank_metric("bank/metric_container.csv",
            "redis-Redis_6379_Redis  (used_memory_rss)")
        self.assertEqual(redis.kind, "gauge")
        self.assertTrue(redis.ranking_eligible)
        self.assertEqual(redis.unit, "undeclared native value")
        self.assertEqual(redis.rule_id, "bank.redis.info.used_memory_rss")
        self.assertEqual(redis.rule_version, "1")

        rate = classify_bank_metric("bank/metric_container.csv",
            "redis-Redis_6379_Redis  (instantaneous_ops_per_sec)")
        self.assertEqual(rate.kind, "rate")
        self.assertTrue(rate.ranking_eligible)
        self.assertEqual(rate.transform, "identity")

        network_read = classify_bank_metric("bank/metric_container.csv",
            "redis-Redis_6379_Redis  (instantaneous_input_kbps)")
        network_write = classify_bank_metric("bank/metric_container.csv",
            "redis-Redis_6379_Redis  (instantaneous_output_kbps)")
        self.assertEqual((network_read.kind, network_write.kind), ("rate", "rate"))
        self.assertTrue(network_read.ranking_eligible and network_write.ranking_eligible)
        self.assertEqual(network_read.unit, "undeclared native value")
        self.assertTrue(any("kilobytes per second" in item
            for item in network_read.provenance))
        self.assertFalse(classify_bank_metric("bank/metric_container.csv",
            "redis-Redis_6379_Redis  (NETOutErr)").ranking_eligible)

        counter = classify_bank_metric("bank/metric_container.csv",
            "redis-Redis_6379_Redis  (evicted_keys)")
        self.assertEqual(counter.kind, "unknown")
        self.assertFalse(counter.ranking_eligible)

        bmc = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_LOCALDISK_LOCALDISK-sdc_DSKPercentBusy")
        self.assertEqual(bmc.rule_id, "bank.bmc.oslinux.disk_busy_percent")
        self.assertTrue(bmc.ranking_eligible)
        self.assertEqual(bmc.unit, "undeclared native value")

        filesystem_capacity = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSCapacity")
        inode_usage = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSInodeUsedPercent")
        available_space = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSAvailableSpace")
        used_space = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSUsedSpace")
        self.assertEqual((filesystem_capacity.kind, filesystem_capacity.direction),
            ("gauge", "higher_is_worse"))
        self.assertEqual((inode_usage.kind, inode_usage.direction),
            ("gauge", "higher_is_worse"))
        self.assertEqual((available_space.kind, available_space.direction),
            ("gauge", "lower_is_worse"))
        self.assertEqual(used_space.kind, "gauge")
        self.assertTrue(all(item.ranking_eligible and item.unit == "undeclared native value"
            for item in (filesystem_capacity, inode_usage, available_space, used_space)))

        output_errors = classify_bank_metric("bank/metric_container.csv",
            "OSLinux-OSLinux_NETWORK_ens160_NETOutErrPrc")
        self.assertEqual(output_errors.rule_id, "bank.bmc.oslinux.network_output_error_percent")
        self.assertTrue(output_errors.ranking_eligible)
        self.assertEqual(output_errors.unit, "undeclared native value")
        for ambiguous in ("OSLinux-OSLinux_NETWORK_ens160_NETOutErr",
                          "OSLinux-OSLinux_NETWORK_ens160_NETOutErrPrcc",
                          "OSLinux-OSLinux_FILE_-home-zabbix_DirSizeMB"):
            result = classify_bank_metric("bank/metric_container.csv", ambiguous)
            self.assertEqual(result.kind, "unknown")
            self.assertFalse(result.ranking_eligible)

        exposure = classify_bank_metric("bank/log/log_service.csv", "log_pattern_exposure_rate")
        self.assertEqual(exposure.kind, "rate")
        self.assertEqual(exposure.unit, "fraction of full-window source log rows")
        self.assertTrue(exposure.ranking_eligible)
        self.assertIn("full_window_log_row_count", exposure.derived_from)

    def test_curated_rule_catalog_resolves_every_redis_disk_variant_without_first_match(self):
        policy = load_bank_metric_policy()
        catalog = {entry.selector_ref: entry for entry in
            policy.selector_catalog("bank/metric_container.csv")}
        disk_rule = catalog["bank.bmc.oslinux.disk_busy_percent@1"]
        resolved = policy.resolve_selector(disk_rule.selector_ref,
            "bank/metric_container.csv", [
                "OSLinux-OSLinux_LOCALDISK_LOCALDISK-sda_DSKPercentBusy",
                "OSLinux-OSLinux_LOCALDISK_LOCALDISK-sdb_DSKPercentBusy",
                "OSLinux-OSLinux_LOCALDISK_LOCALDISK-sdc_DSKPercentBusy"],
            case_data_sha256="c" * 64)
        self.assertEqual(resolved.status, "resolved_one")
        self.assertEqual([row.field_name for row in resolved.matches],
            ["OSLinux-OSLinux_LOCALDISK_LOCALDISK-sdc_DSKPercentBusy"])
        redis_rule = catalog["bank.redis.info.used_memory_rss@1"]
        redis = policy.resolve_selector(redis_rule.selector_ref,
            "bank/metric_container.csv", [
                "redis-Redis_6379_Redis  (used_memory)",
                "redis-Redis_6379_Redis  (used_memory_rss)",
                "redis-Redis_6379_Redis  (used_memory_peak)"],
            case_data_sha256="d" * 64)
        self.assertEqual(redis.status, "resolved_one")
        self.assertEqual({row.field_name for row in redis.matches}, {
            "redis-Redis_6379_Redis  (used_memory_rss)"})
        fs_ref = catalog["bank.bmc.oslinux.filesystem_capacity_percent@1"]
        fs = policy.resolve_selector(fs_ref.selector_ref, "bank/metric_container.csv", [
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSCapacity",
            "OSLinux-OSLinux_FILESYSTEM_-tmp_FSCapacity",
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSInodeUsedPercent",
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSCapacity_extra"],
            case_data_sha256="e" * 64)
        self.assertEqual(fs.status, "resolved_multiple")
        self.assertEqual([row.field_name for row in fs.matches], [
            "OSLinux-OSLinux_FILESYSTEM_-boot_FSCapacity",
            "OSLinux-OSLinux_FILESYSTEM_-tmp_FSCapacity"])

    def test_paper_metric_selector_signatures_pin_catalog_digest_and_rule_refs(self):
        policy = load_bank_metric_policy()
        registry = make_paper_starter_registry(bank_only=True)
        for name in ("paper.backend.metric_conditions", "paper.backend.mean_welch",
                     "paper.backend.location_permutation", "paper.backend.tail_permutation"):
            operator = registry.require(name, "1")
            metric_param = operator.parameters["metric"]
            rule_param = operator.parameters["metric_rule_ref"]
            self.assertIn(policy.sha256, metric_param.description)
            self.assertIn(policy.sha256, rule_param.description)
            self.assertIn("bank.redis.info.instantaneous_input_kbps@1", rule_param.choices)
            self.assertIn("bank.redis.info.instantaneous_output_kbps@1", rule_param.choices)

    def test_pattern_selector_returns_all_exact_case_matches_not_first(self):
        doc = json.loads((ROOT / "config/bank_metric_kind_policy.json").read_text(encoding="utf-8"))
        doc["source_semantics"]["metric_container"]["rules"] = [{
            "rule_id": "test/container-percent-gauge",
            "rule_version": "2",
            "description": "Synthetic per-container CPU percentage family",
            "field_pattern": r"^Vendor_[0-9]+_CpuPercent$",
            "kind": "gauge", "unit": "undeclared native value",
            "unit_status": "undeclared", "direction": "two_sided",
            "ranking_eligible": True, "classification_status": "working_interpretation",
            "transform": "identity", "reset_policy": "not_applicable",
            "provenance": ["synthetic rule test"]}]
        doc["source_semantics"]["metric_container"]["ranking_allowlist"]["Vendor_1_CpuPercent"] = {
            "kind": "gauge", "unit": "undeclared", "unit_status": "undeclared",
            "direction": "two_sided", "ranking_eligible": True,
            "rule_id": "test/exact-vendor-1", "rule_version": "1",
            "provenance": ["synthetic exact override test"]}
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "policy.json"
            fixture.write_text(json.dumps(doc), encoding="utf-8")
            policy = load_bank_metric_policy(fixture)
            catalog = policy.selector_catalog("bank/metric_container.csv")
            entry = next(row for row in catalog if row.rule_id == "test/container-percent-gauge")
            resolved = policy.resolve_selector(entry.selector_ref, "bank/metric_container.csv",
                ["Vendor_1_CpuPercent", "Vendor_2_CpuPercent", "Other"] ,
                case_data_sha256="b" * 64)
            self.assertEqual(resolved.status, "resolved_one")
            self.assertEqual([row.field_name for row in resolved.matches], ["Vendor_2_CpuPercent"])
            with self.assertRaises(ValueError):
                policy.resolve_selector("unknown@99", "bank/metric_container.csv",
                    ["Vendor_1_CpuPercent"], case_data_sha256="b" * 64)

    def test_json_projection_is_plain_and_complete(self):
        result = classify_bank_metric("bank/log/log_service.csv", "log_volume_count")
        projected = result.to_dict()
        self.assertEqual(projected["source_table"], "bank/log/log_service.csv")
        self.assertIsInstance(projected["provenance"], list)
        self.assertIsInstance(projected["derived_from"], list)
        self.assertEqual(projected["kind"], "event_count")


if __name__ == "__main__":
    unittest.main()
