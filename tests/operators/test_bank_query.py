import csv
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from e4.operators.bank.operators import bank_registry
from e4.contracts import (EntityRef, Evidence, ScoredCandidate, ScoredCandidates,
                          Telemetry, Window)
from e4.dsl.executor import NodeContext
from e4.operators.bank.query import (BankQueryError, M4QueryBudget, bank_m4_analysis_weights,
                                     make_m4_registry, query_bank_public)
from e4.agent.bank.controller import data_operator_contract_schema


ROOT = Path(__file__).resolve().parents[2]
CASE_ID = "synthetic-public-case"
REFERENCE = Window("reference", 1_700_000_000_000, 1_700_001_800_000)
ALERT = Window("alert", 1_700_001_800_000, 1_700_003_600_000)


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class BankM4PublicQueryTests(unittest.TestCase):
    def test_runtime_weights_copy_frozen_bank_defaults_and_bound_explicit_overrides(self):
        defaults = bank_m4_analysis_weights(root=ROOT)
        self.assertEqual(defaults, {"metric": 1.0, "trace_latency": 1.0, "log_volume": 1.0})
        changed = bank_m4_analysis_weights(root=ROOT, overrides={"metric": 2.0})
        self.assertEqual(changed, {"metric": 2.0, "trace_latency": 1.0, "log_volume": 1.0})
        with self.assertRaisesRegex(BankQueryError, "unsupported M4 analysis weight"):
            bank_m4_analysis_weights(root=ROOT, overrides={"flow": 1.0})
        with self.assertRaisesRegex(BankQueryError, "between zero and 5"):
            bank_m4_analysis_weights(root=ROOT, overrides={"metric": 6.0})

    def test_shared_query_budget_enforces_case_request_row_and_byte_caps(self):
        budget = M4QueryBudget(None)
        for _ in range(32):
            budget.reserve("synthetic-case", 64)
        with self.assertRaisesRegex(BankQueryError, "request budget"):
            budget.reserve("synthetic-case", 1)
        with self.assertRaisesRegex(BankQueryError, "scoped to one Bank case"):
            budget.reserve("other-case", 1)
        too_many = M4QueryBudget(None)
        for _ in range(8):
            too_many.reserve("synthetic-case", 256)
            too_many.commit("synthetic-case", 256, 1)
        with self.assertRaisesRegex(BankQueryError, "row budget"):
            too_many.reserve("synthetic-case", 1)

    def _fixture(self, root, table="metric/metric_container.csv", rows=None):
        config = root / "config"
        config.mkdir(parents=True)
        for name in ("bank/metric_kind_policy.json", "bank/kpi_policy.json"):
            (config / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / "config" / name, config / name)
        case_dir = root / "data/prepared/bank/public/cases" / CASE_ID
        (case_dir / "source_dates/2021_01_01" / Path(table).parent).mkdir(parents=True)
        table_path = case_dir / "source_dates/2021_01_01" / table
        if rows is None:
            rows = [
                {"timestamp": str(REFERENCE.start_ms_utc + 1000), "cmdb_id": "synthetic-a",
                 "kpi_name": "CPUUtil", "value": "5"},
                {"timestamp": str(ALERT.start_ms_utc + 1000), "cmdb_id": "synthetic-a",
                 "kpi_name": "CPUUtil", "value": "9"},
                {"timestamp": str(ALERT.start_ms_utc + 2000), "cmdb_id": "synthetic-a",
                 "kpi_name": "MemoryUtil", "value": "7"},
            ]
        with table_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=tuple(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        relative = f"source_dates/2021_01_01/{table}"
        alert_path = case_dir / "alert.json"
        alert_path.write_text(json.dumps({"incident_id": CASE_ID,
            "telemetry_dates": ["2021_01_01"],
            "source_date_timestamp_units": {"2021_01_01": {table: "milliseconds"}}}),
            encoding="utf-8")
        hashes = {"alert.json": _sha(alert_path), relative: _sha(table_path)}
        telemetry = Telemetry(CASE_ID, REFERENCE, ALERT, (), (), {}, "d" * 64,
            hashes, {}, {}, "synthetic-bank/1", {key: path.stat().st_size
                for key, path in (("alert.json", alert_path), (relative, table_path))})
        return telemetry, relative, table_path

    def test_query_returns_bounded_native_pages_with_explicit_window_coverage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            telemetry, relative, path = self._fixture(root)
            first = query_bank_public(telemetry, root=root,
                table="metric/metric_container.csv", window="alert", limit=1)
            query = first.metadata["public_query"]
            self.assertEqual(query["rows_data"][0]["kpi_name"], "CPUUtil")
            self.assertEqual(query["rows_data"][0]["timestamp_ms_utc"], ALERT.start_ms_utc + 1000)
            self.assertEqual(query["coverage"]["total_matching_rows"], 2)
            self.assertTrue(query["coverage"]["truncated"])
            self.assertFalse(query["coverage"]["complete"])
            self.assertEqual(query["next_offset"], 1)
            self.assertEqual(query["source_hashes"][relative], _sha(path))
            self.assertEqual(query["timestamp_units"][relative], "milliseconds")
            self.assertEqual(query["value_semantics"],
                "native source columns only; no inferred metric unit or component alias")

            later = query_bank_public(telemetry, root=root,
                table="metric/metric_container.csv", window="alert", offset=1, limit=1)
            self.assertFalse(later.metadata["public_query"]["coverage"]["complete"])
            self.assertTrue(later.metadata["public_query"]["coverage"]["page_complete"])
            self.assertEqual(later.metadata["public_query"]["coverage"]["omitted_before_rows"], 1)
            all_rows = query_bank_public(telemetry, root=root,
                table="metric/metric_container.csv", window="alert", limit=2)
            self.assertTrue(all_rows.metadata["public_query"]["coverage"]["complete"])

    def test_published_query_layout_matches_real_native_query_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            telemetry, _relative, _path = self._fixture(root)
            result = query_bank_public(telemetry, root=root,
                table="metric/metric_container.csv", window="alert", limit=1)
            schema = data_operator_contract_schema()["bank.public.query"]
            query = result.metadata["public_query"]
            self.assertEqual(result.rows, ())
            self.assertEqual(result.metadata.keys(), {"public_query"})
            self.assertEqual(set(schema["public_query"]["coverage_fields"]),
                             set(query["coverage"]))
            self.assertEqual(set(schema["public_query"]["page_fields"]),
                             set(query) - {"rows_data", "coverage"} -
                             set(schema["public_query"]["request_fields"]))
            self.assertEqual(set(query["rows_data"][0]),
                             set(schema["public_query"]["table_columns"]["metric/metric_container.csv"]) |
                             set(query["canonical_extra_fields"]))
            self.assertIs(type(query["rows_data"][0]["timestamp_ms_utc"]), int)
            self.assertTrue(all(isinstance(value, str) for key, value in query["rows_data"][0].items()
                                if key != "timestamp_ms_utc"))

    def test_trace_duration_field_selection_does_not_filter_out_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = [{"timestamp": str(ALERT.start_ms_utc + 1000), "cmdb_id": "synthetic-a",
                     "parent_id": "", "span_id": "span-a", "trace_id": "trace-a",
                     "duration": "12"}]
            telemetry, _relative, _path = self._fixture(root, "trace/trace_span.csv", rows)
            result = query_bank_public(telemetry, root=root, table="trace/trace_span.csv",
                window="alert", metric="duration", fields=["cmdb_id", "duration"])
            self.assertEqual(len(result.metadata["public_query"]["rows_data"]), 1)
            self.assertEqual(result.metadata["public_query"]["rows_data"][0]["duration"], "12")

    def test_query_rejects_out_of_range_controls_and_unpinned_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            telemetry, _relative, table_path = self._fixture(root)
            with self.assertRaisesRegex(BankQueryError, "between one and 256"):
                query_bank_public(telemetry, root=root, table="metric/metric_container.csv",
                                  window="reference", limit=257)
            table_path.write_text(table_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            with self.assertRaisesRegex(BankQueryError, "hash mismatch"):
                query_bank_public(telemetry, root=root, table="metric/metric_container.csv",
                                  window="reference")

    def test_unselected_large_log_text_is_safe_but_selected_text_is_explicitly_capped(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            long_value = "x" * 5000
            rows = [{"log_id": "log-a", "timestamp": str(ALERT.start_ms_utc + 1000),
                     "cmdb_id": "synthetic-a", "log_name": "service", "value": long_value}]
            telemetry, _relative, _path = self._fixture(root, "log/log_service.csv", rows)
            result = query_bank_public(telemetry, root=root, table="log/log_service.csv",
                window="alert", fields=["log_id", "cmdb_id", "log_name"])
            self.assertEqual(result.metadata["public_query"]["rows_data"][0]["log_name"], "service")
            with self.assertRaisesRegex(BankQueryError, "exceeds 4096 UTF-8 bytes"):
                query_bank_public(telemetry, root=root, table="log/log_service.csv",
                    window="alert", fields=["value"])

    def test_registry_exposes_query_as_existing_typed_aggregate_and_audits_calls(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            telemetry, _relative, _path = self._fixture(root)
            audit = root / "audit"
            registry = make_m4_registry(None, [], root=root, audit_dir=audit)
            spec = registry.require("bank.public.query", "1")
            self.assertEqual(spec.output_type, "Aggregate")
            self.assertFalse(spec.cacheable)
            result = spec.implementation({"data": telemetry}, {
                "table": "metric/metric_container.csv", "window": "reference",
                "entity": None, "metric": None, "offset": 0, "limit": 1, "fields": ()},
                NodeContext(CASE_ID, "query", spec.qualified_name, {}, {},
                            REFERENCE, ALERT, telemetry.data_sha256))
            self.assertEqual(len(result.value.metadata["public_query"]["rows_data"]), 1)
            self.assertGreater(result.bytes_processed, 0)
            audit_rows = (audit / "public_query_calls.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(json.loads(audit_rows[0])["status"], "success")

    def test_m4_combiner_defaults_match_m3_and_runtime_weight_override_is_effective(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config"
            config.mkdir()
            for name in ("bank/metric_kind_policy.json", "bank/kpi_policy.json"):
                (config / name).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / "config" / name, config / name)
            registry = make_m4_registry(None, [], root=root)
            rows = registry.signatures()
            m4_signature = next(row for row in rows
                                if row["name"] == "bank.answers.combine")
            self.assertIn("+m4.", m4_signature["version"])
            self.assertEqual(len(rows), 10)
            m3_registry = bank_registry(root=root)
            m3_signature = next(row for row in m3_registry.signatures()
                                if row["name"] == "bank.answers.combine")
            m3 = m3_registry.require("bank.answers.combine", m3_signature["version"])
            m4 = registry.require("bank.answers.combine", m4_signature["version"])
            window = ALERT

            def scored(identifier, family):
                entity = EntityRef("component", identifier)
                evidence = Evidence("bank/synthetic.csv", window, "positive_fixture", 2.0,
                    (entity,), 1, {"ranking_eligible": True})
                return ScoredCandidates((ScoredCandidate(entity, 2.0, None, (evidence,),
                    analysis_family=family),), family, "ready")

            score_inputs = {"scores": (scored("fixture-a", "bank_metric"),
                                        scored("fixture-b", "bank_trace_latency"))}
            m3_context = NodeContext("synthetic-weight-case", "answer", m3.qualified_name,
                {"lead_count": 3}, {}, REFERENCE, ALERT, "a" * 64)
            defaults = bank_m4_analysis_weights(root=root)
            m4_context = NodeContext("synthetic-weight-case", "answer", m4.qualified_name,
                {"lead_count": 3, "analysis_weights": defaults},
                {"analysis_weights": "frozen_bank_defaults"}, REFERENCE, ALERT, "a" * 64)
            original = m3.implementation(score_inputs, {"limit": 3}, m3_context)
            defaulted = m4.implementation(score_inputs, {"limit": 3}, m4_context)
            self.assertEqual(defaulted, original)
            overrides = {**defaults, "metric": 5.0}
            weighted_context = NodeContext("synthetic-weight-case", "answer", m4.qualified_name,
                {"lead_count": 3, "analysis_weights": overrides},
                {"analysis_weights": "explicit_test_override"}, REFERENCE, ALERT, "a" * 64)
            weighted = m4.implementation(score_inputs, {"limit": 3}, weighted_context)
            self.assertEqual([lead.entity.identifier for lead in weighted.leads],
                             ["fixture-a", "fixture-b"])
            self.assertGreater(weighted.leads[0].score, weighted.leads[1].score)

    def test_m4_seed_materialization_uses_the_m4_combiner_version(self):
        from scripts.bank.develop import materialize_bank_seed_examples

        registry = make_m4_registry(None, [], root=ROOT)
        m4_version = next(row["version"] for row in registry.signatures()
                          if row["name"] == "bank.answers.combine" and "+m4." in row["version"])
        seed_library = json.loads((ROOT / "config/bank/seed_playbooks.json").read_text())
        examples = materialize_bank_seed_examples(seed_library, registry)
        for document in examples.values():
            ranker = next(node for node in document["nodes"]
                          if node["operator"] == "bank.answers.combine")
            self.assertEqual(ranker["version"], m4_version)


if __name__ == "__main__":
    unittest.main()
