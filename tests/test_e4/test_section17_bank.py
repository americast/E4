"""Boundary regressions for native Bank rows and complete record transport."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from adapters.bank_adapter import BankAdapter
from adapters.bank_library import bank_metric_aggregate_impl, bank_metrics_contrast_impl
from e4.data.record_store import TelemetryRecordStore
from e4.sandbox.views import TelemetryView, metric_points


class TestSection17Bank(unittest.TestCase):
    def _case(self, root: Path) -> BankAdapter:
        date_dir = root / "source_dates" / "2021_03_04"
        (date_dir / "metric").mkdir(parents=True)
        (date_dir / "metric" / "metric_app.csv").write_text(
            "timestamp,rr,sr,cnt,mrt,tc\n"
            "1000,99,98,12,25.0,checkout\n"
            "1001,98,97,10,31.0,checkout\n"
            "1002,97,96,9,40.0,\n",
            encoding="utf-8",
        )
        (date_dir / "metric" / "metric_container.csv").write_text(
            "timestamp,cmdb_id,kpi_name,value\n"
            "1000,svc-a,cpu,0.2\n"
            "1001,svc-a,cpu,0.8\n"
            "1002,svc-a,cpu,nan\n",
            encoding="utf-8",
        )
        (date_dir / "trace").mkdir(parents=True)
        (date_dir / "trace" / "trace_span.csv").write_text(
            "timestamp,cmdb_id,trace_id,span_id,parent_id,duration\n"
            "1000.5,svc-a,t-ref,s-ref,,4.0\n"
            "1002.5,svc-a,t-alert,s-alert,,8.0\n",
            encoding="utf-8",
        )
        (root / "alert.json").write_text(
            json.dumps({
                "analysis_window_start_epoch_ms_utc": 999000,
                "alert_window_start_epoch_ms_utc": 1001000,
                "alert_window_end_epoch_ms_utc": 1003000,
                "telemetry_dates": ["2021_03_04"],
                "source_date_timestamp_units": {"2021_03_04": {
                    "trace/trace_span.csv": "seconds",
                    "metric/metric_app.csv": "seconds",
                    "metric/metric_container.csv": "seconds",
                }},
            }),
            encoding="utf-8",
        )
        return BankAdapter(root)

    def test_native_app_kpis_keep_unknown_units_and_opaque_role(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = self._case(Path(tmp))
            schema = next(t for t in adapter.get_schema().tables if t.name == "bank/metric_app.csv")
            self.assertTrue(all(col.unit is None for col in schema.columns if col.name in {"rr", "sr", "cnt", "mrt"}))

            reference, alert = adapter.get_windows()
            ref_rows = list(adapter.stream_records("bank/metric_app.csv", reference))
            alert_rows = list(adapter.stream_records("bank/metric_app.csv", alert))
            self.assertEqual([(r.metric_name, r.value) for r in ref_rows], [
                ("rr", 99.0), ("sr", 98.0), ("cnt", 12.0), ("mrt", 25.0),
            ])
            self.assertEqual([(r.metric_name, r.value) for r in alert_rows], [
                ("rr", 98.0), ("sr", 97.0), ("cnt", 10.0), ("mrt", 31.0),
            ])
            self.assertTrue(all(r.unit is None for r in ref_rows + alert_rows))
            self.assertTrue(all(r.entity.role == "app_kpi" and r.entity.identifier == "checkout" for r in ref_rows + alert_rows))
            self.assertTrue(all(r.source_table == "bank/metric_app.csv" for r in ref_rows + alert_rows))
            self.assertEqual(len({r.details["origin_row_id"] for r in ref_rows}), 1)
            self.assertEqual(len({r.details["origin_row_id"] for r in alert_rows}), 1)
            self.assertNotEqual(ref_rows[0].details["origin_row_id"], alert_rows[0].details["origin_row_id"])
            self.assertIn("source_dates/2021_03_04/metric/metric_app.csv", ref_rows[0].details["origin_row_id"])
            tel = adapter.as_telemetry_view()
            projected_ids = {
                metric_points(
                    tel,
                    "bank/metric_app.csv",
                    tel.reference_window,
                    metric_name=name,
                ).rows[0].origin_row_id
                for name in ("rr", "sr", "cnt", "mrt")
            }
            self.assertEqual(projected_ids, {ref_rows[0].details["origin_row_id"]})
            self.assertNotIn("checkout", {e.identifier for e in adapter.extract_suspect_entities()})

    def test_trace_timestamp_uses_audited_source_date_unit(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = self._case(Path(tmp))
            ref, alert = adapter.get_windows()
            ref_spans = list(adapter.stream_records("bank/trace_span.csv", ref))
            alert_spans = list(adapter.stream_records("bank/trace_span.csv", alert))
            self.assertEqual([s.timestamp_ms_utc for s in ref_spans], [1000500])
            self.assertEqual([s.timestamp_ms_utc for s in alert_spans], [1002500])
            self.assertEqual(ref_spans[0].executing_component.identifier, "svc-a")
            self.assertEqual(alert_spans[0].source_table, "bank/trace_span.csv")

    def test_capped_view_reports_partial_input_and_store_roundtrips_complete_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            adapter = self._case(Path(tmp))
            self.assertNotIn("bank/metric_service.csv", {t.name for t in adapter.get_schema().tables})
            # Presence, even with only the CSV header, declares table availability.
            service_path = Path(tmp) / "source_dates" / "2021_03_04" / "metric" / "metric_service.csv"
            service_path.write_text("timestamp,cmdb_id,kpi_name,value\n", encoding="utf-8")
            adapter = BankAdapter(Path(tmp))
            self.assertIn("bank/metric_service.csv", {t.name for t in adapter.get_schema().tables})

            full = adapter.as_full_telemetry_view(case_id="full-view-test")
            self.assertEqual(full.case_id, "full-view-test")
            self.assertFalse(full.truncated)
            self.assertGreater(full.point_count, 0)
            self.assertEqual(full.metadata["record_limit_per_table_window"], None)

            capped = adapter.as_telemetry_view(max_records_per_table=1)
            self.assertTrue(capped.truncated)
            self.assertEqual(capped.metadata["record_counts"]["bank/metric_container.csv"], {"reference": 1, "alert": 2})
            q = metric_points(capped, "bank/metric_container.csv", capped.alert_window)
            self.assertFalse(q.coverage.complete)
            self.assertEqual(q.coverage.total_matches, 1)
            ref_agg = bank_metric_aggregate_impl(
                {"telemetry": capped}, {"table": "bank/metric_container.csv", "window": "reference"}, None
            )
            alert_agg = bank_metric_aggregate_impl(
                {"telemetry": capped}, {"table": "bank/metric_container.csv", "window": "alert"}, None
            )
            partial_scores = bank_metrics_contrast_impl({"reference": ref_agg, "alert": alert_agg}, {}, None)
            self.assertTrue(partial_scores.candidates)
            self.assertFalse(partial_scores.candidates[0].evidence[0].ranking_eligible)

            with TelemetryRecordStore.from_adapter(adapter) as store:
                descriptor = store.descriptor
                self.assertEqual(descriptor["counts_by_table_window"]["bank/metric_container.csv"], {"reference": 1, "alert": 2})
                reopened = TelemetryRecordStore.from_descriptor(descriptor)
                all_records = list(reopened.iter_records("bank/metric_container.csv", "alert"))
                self.assertEqual(len(all_records), 2)
                self.assertTrue(all(r.source_table == "bank/metric_container.csv" for r in all_records))
                complete_view = TelemetryView(
                    case_id="case",
                    reference_window=adapter.get_windows()[0],
                    alert_window=adapter.get_windows()[1],
                    records=list(store.iter_records("bank/metric_container.csv")),
                    source_tables=["bank/metric_container.csv"],
                )
                full_q = metric_points(
                    complete_view,
                    "bank/metric_container.csv",
                    complete_view.alert_window,
                    limit=10,
                )
                self.assertTrue(full_q.coverage.complete)
                self.assertEqual(full_q.coverage.total_matches, 2)

    def test_trusted_builtin_aggregates_full_store_past_query_page_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = self._case(root)
            metric_file = root / "source_dates" / "2021_03_04" / "metric" / "metric_container.csv"
            metric_file.write_text(
                "timestamp,cmdb_id,kpi_name,value\n"
                + "".join(f"1001,svc-a,cpu,{i}\n" for i in range(10001)),
                encoding="utf-8",
            )
            with TelemetryRecordStore.from_adapter(adapter) as store:
                view = TelemetryView(
                    case_id="large-public-fixture",
                    reference_window=adapter.get_windows()[0],
                    alert_window=adapter.get_windows()[1],
                    records=(),
                    source_tables=["bank/metric_container.csv"],
                    record_store=store,
                )
                agg = bank_metric_aggregate_impl(
                    {"telemetry": view}, {"table": "bank/metric_container.csv", "window": "alert"}, None
                )
                self.assertEqual(len(agg.rows), 1)
                self.assertEqual(agg.rows[0].support, 10001)
                self.assertFalse(agg.metadata["truncated"])


if __name__ == "__main__":
    unittest.main()
