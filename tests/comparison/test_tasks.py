import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e4.adapters.bank import load_bank_case
from e4.agent.bank.controller import _select_primary_app_kpi_context
from e4.comparison.tasks import TaskViewError, load_bank_case_view


ROOT = Path(__file__).resolve().parents[2]


class TaskViewTests(unittest.TestCase):
    def test_public_kpi_context_matches_frozen_m4_selector(self):
        sample = json.loads((ROOT / "config/bank/sample.json").read_text(encoding="utf-8"))
        if not (ROOT / "data/prepared/bank/public/clip_manifest.jsonl").is_file():
            self.skipTest("prepared public Bank data is unavailable; synthetic selector fixture still runs")
        for row in sample["development_cases"]:
            with self.subTest(case_id=row["incident_id"]):
                case_id = row["incident_id"]
                view = load_bank_case_view(case_id, split="dev", root=ROOT)
                case = load_bank_case(case_id, split="dev", root=ROOT)
                expected = _select_primary_app_kpi_context(case, root=ROOT)
                self.assertEqual(view.kpi_context["selected_app_context_kpi"], expected)
                self.assertEqual(view.alert_sha256,
                                 __import__("hashlib").sha256(
                                     (view.telemetry_root / "alert.json").read_bytes()).hexdigest())

    def test_synthetic_app_kpi_selector_collapses_duplicate_timestamp_rows(self):
        from e4.comparison.tasks import _select_app_context_kpi

        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source_dates/2021_03_23/metric/metric_app.csv"
            path.parent.mkdir(parents=True)
            rows = ["timestamp,rr,sr,cnt,mrt,tc", "1,,,,1,api", "1,,,,3,api", "2,,,,4,api",
                    "4,,,,10,api", "4,,,,12,api", "5,,,,20,api"]
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            result = _select_app_context_kpi(
                {"source_dates/2021_03_23/metric/metric_app.csv": path},
                {"analysis_window_start_epoch_ms_utc": 0,
                 "alert_window_start_epoch_ms_utc": 3000,
                 "alert_window_end_epoch_ms_utc": 6000,
                 "source_date_timestamp_units": {
                     "2021_03_23": {"metric/metric_app.csv": "seconds"}}},
                {"minimum_support_per_window": 2,
                 "primary_metric_allowlist": ["metric_app.mrt"],
                 "selection": "frozen selection", "selection_provenance": "fixture"},
                "fixture-policy-sha")
        self.assertEqual(result["tc_id"], "api")
        self.assertEqual(result["reference_support"], 2)
        self.assertEqual(result["alert_support"], 2)
        self.assertEqual(result["raw_rows"], {"reference": 3, "alert": 3})
        self.assertEqual(result["duplicate_rows_collapsed"], {"reference": 1, "alert": 1})
        self.assertEqual(result["reference"]["mean"], 3.0)
        self.assertEqual(result["alert"]["mean"], 15.5)

    def test_staged_destination_must_be_fresh_and_never_deletes_existing_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary) / "run"
            run_dir.mkdir()
            existing = run_dir / "case-view"
            existing.mkdir()
            sentinel = existing / "keep.txt"
            sentinel.write_text("user data", encoding="utf-8")
            with patch.dict(os.environ, {"E4_RUN_DIR": str(run_dir)}):
                with self.assertRaisesRegex(TaskViewError, "must be fresh"):
                    load_bank_case_view("bank-20210323T0630-utc8", split="dev", root=ROOT,
                                        staged_root=existing)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "user data")

    def test_fresh_staged_destination_copies_every_pinned_public_file(self):
        import hashlib

        case_id = "bank-20210323T0630-utc8"
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary) / "run"
            run_dir.mkdir()
            staged_root = run_dir / "public_cases" / case_id
            with patch.dict(os.environ, {"E4_RUN_DIR": str(run_dir)}):
                source = load_bank_case_view(case_id, split="dev", root=ROOT)
                staged = load_bank_case_view(case_id, split="dev", root=ROOT,
                                             staged_root=staged_root)
            self.assertEqual(staged.staged_root, staged_root)
            self.assertEqual(staged.component_ids, source.component_ids)
            expected = dict(source.telemetry_file_sha256)
            expected["alert.json"] = source.alert_sha256
            observed = {path.relative_to(staged_root).as_posix():
                        hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in staged_root.rglob("*") if path.is_file()}
            self.assertEqual(observed, expected)

    def test_public_case_allowlist_rejects_case_from_other_split(self):
        sample = json.loads((ROOT / "config/bank/sample.json").read_text(encoding="utf-8"))
        scored_id = sample["scored_cases"][0]["incident_id"]
        with self.assertRaisesRegex(TaskViewError, "requested frozen split"):
            load_bank_case_view(scored_id, split="dev", root=ROOT)


if __name__ == "__main__":
    unittest.main()
