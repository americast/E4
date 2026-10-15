import csv
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from e4infra.telecom_prep import (audit_metric_app, intervals_overlap,
                                  parse_alert_window, prepare_metadata,
                                  sample_cases)

LOCAL = ZoneInfo("Asia/Shanghai")


class TelecomMetadataTests(unittest.TestCase):
    def test_parses_supplied_query_windows_in_two_wordings(self):
        first = parse_alert_window("During April 11, 2020, from 00:00 to 00:30, a failure occurred.")
        second = parse_alert_window("On May 30, 2020, between 02:00 and 02:30, a failure occurred.")
        self.assertEqual(first[0].isoformat(), "2020-04-11T00:00:00+08:00")
        self.assertEqual((second[0].hour, second[1].minute), (2, 30))

    def test_preparation_groups_window_gold_and_never_emits_scoring_points(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "raw"
            raw.mkdir()
            base = 1586534400  # 2020-04-11 00:00 Asia/Shanghai; record epoch unit is seconds.
            records = [
                {"level": "pod", "reason": "CPU fault", "component": "docker_003",
                 "timestamp": str(base + 300), "datetime": "2020-04-11 00:05:00"},
                {"level": "node", "reason": "network delay", "component": "os_001",
                 "timestamp": str(base + 360), "datetime": "2020-04-11 00:06:00"},
            ]
            records.append(dict(records[0]))  # exact duplicate row is deduplicated.
            with (raw / "record.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(records[0]))
                writer.writeheader()
                writer.writerows(records)
            with (raw / "query.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["task_index", "instruction", "scoring_points"])
                writer.writeheader()
                for _ in records:
                    writer.writerow({"task_index": "task_1",
                                     "instruction": "On April 11, 2020, from 00:00 to 00:30, a failure occurred.",
                                     "scoring_points": "SECRET_COMPONENT and SECRET_CAUSE"})
            output, audit = root / "prepared", root / "audit"
            report = prepare_metadata(raw, output, audit)
            self.assertEqual(report["physical_incident_count"], 1)
            self.assertEqual(report["exact_duplicate_record_rows_dropped"], 1)
            self.assertEqual(report["query_onset_window_mismatches"], 0)
            public = (output / "candidate_alerts.jsonl").read_text()
            self.assertNotIn("SECRET_", public)
            self.assertNotIn("CPU fault", public)
            gold = json.loads((output / "evaluator/candidate_ground_truth.jsonl").read_text())
            self.assertEqual(len(gold["ground_truth"]), 2)
            self.assertEqual(gold["ground_truth"][0]["timestamp_epoch_seconds"], base + 300)

    def test_metric_app_audit_preserves_duplicate_rows_and_uses_unique_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            date_dir = root / "2020_04_11" / "metric"
            date_dir.mkdir(parents=True)
            metric_path = date_dir / "metric_app.csv"
            fields = ["serviceName", "startTime", "avg_time", "num", "succee_num", "succee_rate"]
            start = datetime(2020, 4, 11, 0, 30, tzinfo=LOCAL)
            rows = []
            for minute in range(-30, 30):
                timestamp = int((start + timedelta(minutes=minute)).timestamp() * 1000)
                rows.append({"serviceName": "osb_001", "startTime": str(timestamp),
                             "avg_time": "1.0", "num": "2", "succee_num": "2", "succee_rate": "1.0"})
            rows.append(dict(rows[0], avg_time="9.0"))
            with metric_path.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            public_path = root / "candidate.jsonl"
            public_path.write_text(json.dumps({"incident_id": "case1", "telemetry_date": "2020_04_11",
                                               "alert_window_start_local": start.isoformat(timespec="seconds"),
                                               "alert_window_end_local": (start + timedelta(minutes=30)).isoformat(timespec="seconds")}) + "\n")
            result = audit_metric_app(root, public_path, root / "audit", min_unique_timestamps=24)
            self.assertEqual(result["eligible_count"], 1)
            date_stats = result["date_files"]["2020_04_11"]
            self.assertEqual(date_stats["rows"], 61)
            self.assertEqual(date_stats["unique_startTime_ms"], 60)
            self.assertEqual(date_stats["duplicate_service_timestamp_keys"], 1)
            per_case = result["per_incident"][0]
            self.assertEqual(per_case["baseline_unique_timestamps"], 30)
            self.assertEqual(per_case["baseline_row_count"], 31)


class TelecomSplitTests(unittest.TestCase):
    def test_half_open_intervals_allow_touching_boundary(self):
        left = (datetime(2020, 1, 1, tzinfo=LOCAL), datetime(2020, 1, 1, 1, tzinfo=LOCAL))
        right = (left[1], left[1] + timedelta(minutes=30))
        overlap = (left[1] - timedelta(microseconds=1), left[1] + timedelta(minutes=30))
        self.assertFalse(intervals_overlap(left, right))
        self.assertTrue(intervals_overlap(left, overlap))

    def test_seeded_stratified_sample_has10_scored_and2_nonoverlapping_dev(self):
        public, gold = [], {}
        reasons = ["CPU fault", "db close", "db connection limit", "network delay", "network loss"]
        for reason_index, reason in enumerate(reasons):
            for offset in range(3):
                start = datetime(2020, 5, 1, tzinfo=LOCAL) + timedelta(hours=reason_index * 8 + offset * 2)
                case_id = f"c{reason_index}-{offset}"
                public.append({"incident_id": case_id, "alert_window_start_local": start.isoformat(),
                               "alert_window_end_local": (start + timedelta(minutes=30)).isoformat(),
                               "alert_text": "generic alert", "telemetry_date": start.strftime("%Y_%m_%d"),
                               "timezone": "Asia/Shanghai"})
                gold[case_id] = [{"reason": reason}]
        with tempfile.TemporaryDirectory() as temporary:
            output, audit = Path(temporary) / "prepared", Path(temporary) / "audit"
            result = sample_cases(public, gold, set(gold), output, audit, seed=42)
            self.assertEqual(len(result["scored_ids"]), 10)
            self.assertEqual(len(result["development_ids"]), 2)
            self.assertFalse(set(result["scored_ids"]) & set(result["development_ids"]))
            self.assertTrue(all(value["scored"] == 2 for value in result["strata"].values()))
            for dev_id in result["development_ids"]:
                dev = next(row for row in public if row["incident_id"] == dev_id)
                dev_start = datetime.fromisoformat(dev["alert_window_start_local"]) - timedelta(minutes=30)
                dev_end = datetime.fromisoformat(dev["alert_window_end_local"])
                for scored_id in result["scored_ids"]:
                    scored = next(row for row in public if row["incident_id"] == scored_id)
                    scored_start = datetime.fromisoformat(scored["alert_window_start_local"]) - timedelta(minutes=30)
                    scored_end = datetime.fromisoformat(scored["alert_window_end_local"])
                    self.assertFalse(intervals_overlap((dev_start, dev_end), (scored_start, scored_end)))

    def test_freeze_replays_identically_and_refuses_resampling(self):
        public, gold = [], {}
        reasons = ["CPU fault", "db close", "db connection limit", "network delay", "network loss"]
        for reason_index, reason in enumerate(reasons):
            for offset in range(4):
                start = datetime(2020, 5, 1, tzinfo=LOCAL) + timedelta(hours=reason_index * 12 + offset * 2)
                case_id = f"f{reason_index}-{offset}"
                public.append({"incident_id": case_id, "alert_window_start_local": start.isoformat(),
                               "alert_window_end_local": (start + timedelta(minutes=30)).isoformat(),
                               "alert_text": "generic alert", "telemetry_date": start.strftime("%Y_%m_%d"),
                               "timezone": "Asia/Shanghai"})
                gold[case_id] = [{"reason": reason}]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            freeze = root / "telecom_sample.json"
            output, audit = root / "prepared", root / "audit"
            first = sample_cases(public, gold, set(gold), output, audit, seed=42, freeze_path=freeze)
            manifest_before = (output / "scored_incidents.jsonl").read_bytes()
            second = sample_cases(public, gold, set(gold), output, audit, seed=42, freeze_path=freeze)
            self.assertEqual(first["public_manifest_sha256"], second["public_manifest_sha256"])
            with self.assertRaisesRegex(RuntimeError, "frozen sample differs"):
                sample_cases(public, gold, set(gold), output, audit, seed=43, freeze_path=freeze)
            self.assertEqual(manifest_before, (output / "scored_incidents.jsonl").read_bytes())

    def test_sampler_refuses_to_drop_a_stratum_after_coverage_filter(self):
        public, gold = [], {}
        reasons = ["CPU fault", "db close", "db connection limit", "network delay", "network loss"]
        for index, reason in enumerate(reasons):
            for j in range(3):
                start = datetime(2020, 5, 1, tzinfo=LOCAL) + timedelta(hours=index * 8 + j * 2)
                case_id = f"c{index}-{j}"
                public.append({"incident_id": case_id, "alert_window_start_local": start.isoformat(),
                               "alert_window_end_local": (start + timedelta(minutes=30)).isoformat()})
                gold[case_id] = [{"reason": reason}]
        eligible = {key for key, value in gold.items() if value[0]["reason"] != "network loss"}
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "removed entire reason strata"):
                sample_cases(public, gold, eligible, Path(temporary) / "prepared", Path(temporary) / "audit")


if __name__ == "__main__":
    unittest.main()
