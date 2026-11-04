"""Unit tests for BankAdapter, DemoServiceAdapter, and adapter conformance."""

import unittest
from pathlib import Path

from adapters.bank_adapter import BankAdapter
from adapters.demo_adapter import DemoServiceAdapter
from e4.data.adapter import check_adapter_conformance
from e4.types import EntityRef, MetricRecord, Window


class TestAdapters(unittest.TestCase):
    def test_bank_adapter_conformance(self):
        case_path = Path("data/prepared/bank/public/cases/bank-20210304T0100-utc8")
        if not case_path.is_dir():
            self.skipTest(f"Bank case {case_path} not found")

        adapter = BankAdapter(case_path)
        result = check_adapter_conformance(adapter)
        self.assertEqual(result["status"], "conforming")
        self.assertGreater(result["table_count"], 0)
        self.assertGreater(result["suspect_entities_count"], 0)

        # Check windows
        ref_w, alt_w = adapter.get_windows()
        self.assertEqual(ref_w.name, "reference")
        self.assertEqual(alt_w.name, "alert")
        self.assertGreater(alt_w.end_ms_utc, alt_w.start_ms_utc)

        # Check streaming
        records = list(adapter.stream_records("bank/metric_container.csv", alt_w))
        self.assertGreater(len(records), 0)
        first = records[0]
        self.assertIsInstance(first, MetricRecord)
        self.assertTrue(alt_w.contains(first.timestamp_ms_utc))

    def test_demo_service_adapter_conformance(self):
        """Phase 4 Portability Proof: Independent OTel adapter passes strict conformance."""
        adapter = DemoServiceAdapter()
        result = check_adapter_conformance(adapter)
        self.assertEqual(result["status"], "conforming")
        self.assertEqual(result["table_count"], 2)
        self.assertEqual(result["suspect_entities_count"], 3)

        # Check entities
        entities = adapter.extract_suspect_entities()
        self.assertIn(EntityRef("node", "worker-node-1"), entities)
        self.assertIn(EntityRef("node", "worker-node-2"), entities)

        # Check stream
        ref_w, alt_w = adapter.get_windows()
        alt_records = list(adapter.stream_records("otel/node_metrics.csv", alt_w))
        self.assertEqual(len(alt_records), 3)


if __name__ == "__main__":
    unittest.main()
