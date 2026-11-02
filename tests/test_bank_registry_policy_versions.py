from __future__ import annotations

import unittest
from pathlib import Path

from e4core.bank_ops import _policy_version, aggregate_bank_metrics, bank_registry
from e4core.bank_metric_policy import load_bank_metric_policy
from e4core.contracts import EntityRef, MetricPoint, Telemetry, Window
from e4core.executor import NodeContext
from e4core.m4_data import make_m4_registry
from e4core.m4_operator_ir import compact_operator_ir_inputs_v2


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = Window("reference", 0, 1_800_000)
ALERT = Window("alert", 1_800_000, 3_600_000)
REDIS_FIELD = "redis-Redis_6379_Redis  (used_memory_rss)"


def _telemetry():
    entity = EntityRef("component", "Redis01")
    points = []
    for window in (REFERENCE, ALERT):
        for minute in range(0, 30, 5):
            points.append(MetricPoint("bank/metric_container.csv",
                window.start_ms_utc + minute * 60_000, entity, REDIS_FIELD,
                "bank.metric_container", "value", 100.0 + minute,
                str(100 + minute), f"redis:{window.name}:{minute}"))
    return Telemetry("synthetic-registry-policy", REFERENCE, ALERT, tuple(points), (),
        {"Redis01": {"role": "component"}}, "a" * 64, {}, {}, {},
        "bank-registry-policy-test/1")


def _context(telemetry):
    return NodeContext(telemetry.case_id, "aggregate", "bank.metrics.aggregate", {}, {},
        telemetry.reference_window, telemetry.alert_window, telemetry.data_sha256)


class BankRegistryPolicyVersionTests(unittest.TestCase):
    def test_fresh_registry_has_only_current_versions_and_ten_m4_builtins(self):
        signatures = bank_registry().signatures()
        versions_by_name = {}
        for row in signatures:
            versions_by_name.setdefault(row["name"], []).append(row["version"])
        self.assertTrue(all(len(versions) == 1 for versions in versions_by_name.values()))
        self.assertEqual(versions_by_name["bank.metrics.aggregate"],
            [_policy_version(ROOT / "config/bank_metric_kind_policy.json")])
        m4_signatures = make_m4_registry(None, []).signatures()
        self.assertEqual(len(m4_signatures), 10)

    def test_current_registry_and_v2_compaction_include_new_classified_redis_rows(self):
        telemetry = _telemetry()
        registry = bank_registry()
        aggregate_signature = next(row for row in registry.signatures()
            if row["name"] == "bank.metrics.aggregate")
        aggregate_operator = registry.require("bank.metrics.aggregate",
            aggregate_signature["version"])
        aggregates = {
            window.name: aggregate_operator.implementation({"data": telemetry},
                {"window": window.name}, _context(telemetry)).value
            for window in (REFERENCE, ALERT)}
        row = next(item for item in aggregates["reference"].rows
            if item.metric_key.split("|")[1] == REDIS_FIELD and item.statistic == "median")
        self.assertTrue(row.details["ranking_eligible"])
        self.assertEqual(row.details["metric_rule_id"], "bank.redis.info.used_memory_rss")

        compacted = compact_operator_ir_inputs_v2(aggregates,
            {"nodes": [{"id": "series", "kernel": "series.component_metrics"}]})
        compacted_row = next(item for item in compacted["reference"].rows
            if item.details.get("source_field") == REDIS_FIELD)
        self.assertTrue(compacted_row.details["ranking_eligible"])
        self.assertEqual(compacted_row.details["rule_id"], "bank.redis.info.used_memory_rss")
        self.assertEqual(compacted["reference"].metadata["ir_compaction"]["bank_metric_policy_sha256"],
            load_bank_metric_policy().sha256)

    def test_explicit_legacy_registry_pins_legacy_version_and_semantics(self):
        telemetry = _telemetry()
        legacy_policy = ROOT / "config/bank_metric_kind_policy_v1_legacy.json"
        registry = bank_registry(metric_policy_path=legacy_policy)
        versions = [row["version"] for row in registry.signatures()
            if row["name"] == "bank.metrics.aggregate"]
        self.assertEqual(versions, [_policy_version(legacy_policy)])
        operator = registry.require("bank.metrics.aggregate", versions[0])
        aggregate = operator.implementation({"data": telemetry}, {"window": "reference"},
            _context(telemetry)).value
        row = next(item for item in aggregate.rows
            if item.metric_key.split("|")[1] == REDIS_FIELD and item.statistic == "median")
        self.assertFalse(row.details["ranking_eligible"])
        self.assertEqual(row.details["metric_kind"], "unknown")


if __name__ == "__main__":
    unittest.main()
