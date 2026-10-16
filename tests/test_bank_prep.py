import csv
import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from e4infra.bank_prep import (audit_metric_app, bank_case_id, jsonl_digest,
                               build_public_catalog, clip_selected_bank, parse_query_window, prepare_metadata,
                               sample_bank_cases, write_jsonl, _parse_epoch_ms,
                               _spillover_from_source_day, _source_year_bounds_ms)


LOCAL = ZoneInfo("Asia/Shanghai")


def _public_case(start: datetime) -> dict:
    end = start + timedelta(minutes=30)
    ref = start - timedelta(minutes=30)
    return {
        "incident_id": bank_case_id(start),
        "alert_window_start_local": start.isoformat(timespec="seconds"),
        "alert_window_end_local": end.isoformat(timespec="seconds"),
        "analysis_window_start_local": ref.isoformat(timespec="seconds"),
        "analysis_window_end_local": end.isoformat(timespec="seconds"),
        "analysis_window_start_epoch_ms_utc": int(ref.timestamp() * 1000),
        "alert_window_start_epoch_ms_utc": int(start.timestamp() * 1000),
        "alert_window_end_epoch_ms_utc": int(end.timestamp() * 1000),
        "timezone": "Asia/Shanghai", "telemetry_date": start.strftime("%Y_%m_%d"),
        "telemetry_dates": sorted({ref.strftime("%Y_%m_%d"),
                                    (end - timedelta(milliseconds=1)).strftime("%Y_%m_%d")}),
        "timestamp_unit_in_source_files": {"metric/metric_app.csv": "seconds"},
        "alert_text": "A failure was reported during the stated interval; exact cause and component unknown.",
    }


class BankPrepTests(unittest.TestCase):
    def test_real_clip_producer_publishes_a_loadable_hash_pinned_case(self):
        from e4core.bank_adapter import load_bank_case
        from e4infra import bank_prep
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepared, raw, audit = root / "data/prepared/bank", root / "data/raw", root / "audit"
            date = "2021_03_04"
            start = datetime(2021, 3, 4, 14, 30, tzinfo=LOCAL)
            case = _public_case(start)
            pinned_source_config = json.loads((Path(__file__).parents[1] / "config/openrca_bank_source.json").read_text())
            case.update({"timestamp_unit_in_source_files": {key: pinned_source_config["time_units"][f"{key}.timestamp"] for key in bank_prep.TABLE_SPECS},
                "upstream_timestamp_unit_provenance": "prompt.py",
                "source_date_timestamp_units": {date: {key: spec["timestamp_unit"] for key, spec in bank_prep.TABLE_SPECS.items()}},
                "source_date_timestamp_unit_evidence": {date: {key: "fixture" for key in bank_prep.TABLE_SPECS}}})
            # Source rows cover reference and alert windows and retain native headers.
            timestamp_values = [str(int((start - timedelta(minutes=15)).timestamp())),
                                str(int((start + timedelta(minutes=15)).timestamp()))]
            rows = {
                "metric/metric_app.csv": [[ts, "1", "2", "3", "4", "tc-a"] for ts in timestamp_values],
                "metric/metric_container.csv": [[ts, "pod-a", "cpu", "5"] for ts in timestamp_values],
                "trace/trace_span.csv": [[str(int(ts) * 1000), "pod-a", "", f"s{i}", f"t{i}", "8"]
                    for i, ts in enumerate(timestamp_values)],
                "log/log_service.csv": [[f"l{i}", ts, "pod-a", "INFO", "ok"] for i, ts in enumerate(timestamp_values)],
            }
            dates = root / "data/raw/telemetry" / date
            downloads = []
            for relative, spec in bank_prep.TABLE_SPECS.items():
                target = dates / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("w", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(spec["header"])
                    writer.writerows(rows[relative])
                payload = target.read_bytes()
                source_rel = f"Bank/telemetry/{date}/{relative}"
                downloads.append({"path": source_rel, "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(), "hub_lfs_sha256": hashlib.sha256(payload).hexdigest(),
                    "hub_git_blob_sha1": None, "integrity": "hub_lfs_sha256"})
            audit.mkdir(parents=True)
            download_log = audit / "downloads.jsonl"
            download_log.write_text("".join(json.dumps(row) + "\n" for row in downloads))
            source_config = pinned_source_config
            source_config["empirical_timestamp_units_by_source_date"] = {"dates": {date: {"units":
                {key: spec["timestamp_unit"] for key, spec in bank_prep.TABLE_SPECS.items()},
                "evidence": {key: "fixture" for key in bank_prep.TABLE_SPECS}}}}
            source_path = root / "config/source.json"
            source_path.parent.mkdir(parents=True)
            source_path.write_text(json.dumps(source_config))
            prompt = source_config["official_source"]["schema_document"]
            (root / prompt).parent.mkdir(parents=True, exist_ok=True)
            (root / prompt).write_bytes((Path(__file__).parents[1] / prompt).read_bytes())
            sample_path = root / "config/bank_sample.json"
            sample_path.write_text("{}\n")
            prepared.mkdir(parents=True)
            for name in ("candidate_incidents.jsonl", "scored_incidents.jsonl", "dev_incidents.jsonl"):
                write_jsonl(prepared / name, [case])
            with patch.object(bank_prep, "ROOT", root), patch.object(bank_prep, "SOURCE_CONFIG", source_path):
                clip_selected_bank(raw, prepared, audit, sample_path=sample_path, download_log_path=download_log)
                loaded = load_bank_case(case["incident_id"], "dev", root=root,
                    metric_policy_path=Path(__file__).parents[1] / "config/bank_metric_kind_policy.json",
                    kpi_policy_path=Path(__file__).parents[1] / "config/bank_dev_kpi_policy.json")
            self.assertEqual(sum(key.endswith(tuple(bank_prep.TABLE_SPECS)) for key in loaded.telemetry.file_sha256), 4)
            self.assertEqual(loaded.alert["source_date_timestamp_units"][date]["trace/trace_span.csv"], "milliseconds")
            self.assertTrue(loaded.telemetry.metrics)
            self.assertTrue(loaded.telemetry.spans)
            self.assertTrue(loaded.log_summary)

    def test_recovery_rejects_incomplete_progress_without_touching_stage(self):
        from e4infra.bank_prep import finalize_staged_bank
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepared = root / "prepared"
            prepared.mkdir()
            case = _public_case(datetime(2021, 3, 4, 14, 30, tzinfo=LOCAL))
            write_jsonl(prepared / "scored_incidents.jsonl", [case])
            write_jsonl(prepared / "dev_incidents.jsonl", [])
            write_jsonl(prepared / "candidate_incidents.jsonl", [case])
            sample = root / "sample.json"
            sample.write_text(json.dumps({"prepared_public_manifest_sha256": {
                "scored": __import__("hashlib").sha256((prepared / "scored_incidents.jsonl").read_bytes()).hexdigest(),
                "dev": __import__("hashlib").sha256((prepared / "dev_incidents.jsonl").read_bytes()).hexdigest()}}))
            audit = root / "audit"
            audit.mkdir()
            progress = audit / "progress.jsonl"
            progress.write_text(json.dumps({"event": "clip_start", "selected_dates": case["telemetry_dates"]}) + "\n")
            with self.assertRaisesRegex(ValueError, "completed source progress set"):
                finalize_staged_bank(root, prepared, audit, sample_path=sample,
                    download_log_path=root / "missing.jsonl", progress_path=progress)
    def test_integer_timestamp_fast_path_matches_decimal_conversion(self):
        samples = [("1616428799", "seconds"), ("1616428799.125", "seconds"),
                   ("1616428799123", "milliseconds"), ("1616428799123.0", "milliseconds")]
        for raw, unit in samples:
            expected = int(Decimal(raw) * (1000 if unit == "seconds" else 1))
            self.assertEqual(_parse_epoch_ms(raw, unit, path="fixture.csv", row_number=2), expected)

    def test_ten_digit_bank_trace_timestamp_uses_seconds_not_1970_milliseconds(self):
        epoch_ms = _parse_epoch_ms("1616428799", "seconds", path="trace_span.csv", row_number=2)
        local = datetime.fromtimestamp(epoch_ms / 1000, LOCAL)
        self.assertEqual(local.strftime("%Y-%m-%d %H:%M:%S"), "2021-03-22 23:59:59")
        with self.assertRaisesRegex(ValueError, "magnitude contradicts"):
            _parse_epoch_ms("1616428799", "milliseconds", path="trace_span.csv", row_number=2)

    def test_source_day_spillover_uses_exact_precomputed_epoch_bounds(self):
        start = datetime(2021, 3, 4, tzinfo=LOCAL)
        start_ms = int(start.timestamp() * 1000)
        end_ms = start_ms + 24 * 60 * 60 * 1000
        self.assertEqual(_spillover_from_source_day(start_ms - 400, start_ms, end_ms), ("before", 400))
        self.assertEqual(_spillover_from_source_day(end_ms + 180_000, start_ms, end_ms), ("after", 180_000))
        self.assertEqual(_spillover_from_source_day(start_ms + 123, start_ms, end_ms), (None, 0))

    def test_same_year_offdate_rows_are_valid_but_outside_year_is_detectable(self):
        year_start, year_end = _source_year_bounds_ms("2021_03_09")
        prior_day = _parse_epoch_ms("1615204174572", "milliseconds", path="trace.csv", row_number=2)
        self.assertTrue(year_start <= prior_day < year_end)
        self.assertEqual(_spillover_from_source_day(
            prior_day, int(datetime(2021, 3, 9, tzinfo=LOCAL).timestamp() * 1000),
            int(datetime(2021, 3, 10, tzinfo=LOCAL).timestamp() * 1000))[0], "before")
        prior_year = year_start - 1
        self.assertFalse(year_start <= prior_year < year_end)

    def test_catalog_publisher_matches_bank_source_catalog_contract(self):
        source = {"official_source": {"repository": "microsoft/OpenRCA", "revision": "rev",
                                     "schema_document": "prompt.py"},
                  "working_mirror": {"repository": "tracer-cloud/opensre", "revision": "mirror"}}
        catalog = build_public_catalog(source, {"dockerA2": {"trace/trace_span.csv:cmdb_id"}}, "a" * 64)
        self.assertEqual(catalog["schema_version"], "bank-source-catalog/1")
        self.assertEqual(catalog["source"]["official_revision"], "rev")
        self.assertEqual(catalog["entities"]["dockerA2"], {
            "source_fields": ["trace/trace_span.csv:cmdb_id"], "relationship": "unknown"})
        self.assertEqual(catalog["topology"]["relationships"], [])

    def test_query_windows_parse_both_public_phrasings_in_utc8(self):
        cases = [
            "On March 4, 2021, within the time range of 14:30 to 15:00, a failure was detected.",
            "On March 4, 2021, between 18:00 and 18:30, a failure was detected.",
            "During the specified time range of March 6, 2021, from 06:00 to 06:30, failure occurred.",
            "On March 6, 2021, from 23:30 to March 7, 2021, at 00:00, failure occurred.",
        ]
        starts = []
        for text in cases:
            start, end = parse_query_window(text)
            self.assertEqual(start.utcoffset(), timedelta(hours=8))
            self.assertEqual(end - start, timedelta(minutes=30))
            starts.append(start)
        self.assertEqual(starts[-1].date().day, 6)
        self.assertEqual((starts[-1] + timedelta(minutes=30)).date().day, 7)

    def test_metadata_prep_keeps_scoring_points_and_fault_fields_private(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, prepared, audit = root / "raw", root / "prepared", root / "audit"
            raw.mkdir()
            start = datetime(2021, 3, 4, 14, 30, tzinfo=LOCAL)
            onset = start + timedelta(minutes=5)
            with (raw / "query.csv").open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["task_index", "instruction", "scoring_points"])
                writer.writerow(["task_1", "On March 4, 2021, within the time range of 14:30 to 15:00, one failure.",
                                 "PRIVATE exact target timestamp and component sentinel"])
            with (raw / "record.csv").open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["level", "component", "timestamp", "datetime", "reason"])
                writer.writerow(["pod", "secret-pod", str(int(onset.timestamp())),
                                 onset.strftime("%Y-%m-%d %H:%M:%S"), "secret-reason"])
            prepare_metadata(raw, prepared, audit)
            public = json.loads((prepared / "candidate_incidents.jsonl").read_text().splitlines()[0])
            private = json.loads((prepared / "evaluator/candidate_ground_truth.jsonl").read_text().splitlines()[0])
            self.assertNotIn("scoring_points", public)
            self.assertNotIn("secret-pod", json.dumps(public))
            self.assertNotIn("secret-reason", json.dumps(public))
            self.assertNotIn("PRIVATE exact", json.dumps(public))
            self.assertEqual(private["ground_truth"][0]["component"], "secret-pod")

    def test_jsonl_manifest_digest_matches_exact_written_public_bytes(self):
        rows = [{"incident_id": "bank-fixture", "alert_text": "unknown", "timezone": "Asia/Shanghai"}]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "public.jsonl"
            write_jsonl(output, rows)
            self.assertEqual(jsonl_digest(rows), __import__("hashlib").sha256(output.read_bytes()).hexdigest())
            line = json.loads(output.read_text())
            self.assertNotIn("ground_truth", line)

    def test_seeded_stratified_sample_is_disjoint_and_manifest_hashes_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            public_path = root / "candidate.jsonl"
            gold_path = root / "gold.jsonl"
            public_rows, gold_rows = [], []
            start = datetime(2021, 3, 4, 0, 0, tzinfo=LOCAL)
            for reason_index in range(8):
                for repetition in range(4):
                    when = start + timedelta(hours=(reason_index * 4 + repetition) * 2)
                    case = _public_case(when)
                    public_rows.append(case)
                    gold_rows.append({"incident_id": case["incident_id"],
                        "ground_truth": [{"component": f"pod-{reason_index}-{repetition}",
                                          "reason": f"private-reason-{reason_index}",
                                          "timestamp_epoch_seconds": int(when.timestamp()) + 10,
                                          "timestamp_local": (when + timedelta(seconds=10)).isoformat()}]})
            # A multi-record window remains in evaluator candidate gold but is
            # excluded from the single-component pilot sample.
            multi = _public_case(start + timedelta(hours=100))
            public_rows.append(multi)
            gold_rows.append({"incident_id": multi["incident_id"], "ground_truth": [
                {"component": "pod-a", "reason": "private-multi-a"},
                {"component": "pod-b", "reason": "private-multi-b"}]})
            write_jsonl(public_path, public_rows)
            write_jsonl(gold_path, gold_rows)
            eligible_path = root / "eligible.json"
            eligible_path.write_text(json.dumps([row["incident_id"] for row in public_rows]))
            audit_root = root / "audit"
            audit_root.mkdir(parents=True)
            (audit_root / "bank_metadata_audit.json").write_text(json.dumps({"metadata": "fixture"}))
            (audit_root / "bank_metric_app_coverage.json").write_text(json.dumps({
                "date_files": {"2021_03_04": {"sha256": "a" * 64}}}))
            freeze = root / "config/bank_sample.json"
            first = sample_bank_cases(public_path, gold_path, eligible_path, root / "prepared",
                                      audit_root, seed=42, freeze_path=freeze)
            second = sample_bank_cases(public_path, gold_path, eligible_path, root / "prepared2",
                                       audit_root, seed=42, freeze_path=root / "config/bank_sample2.json")
            self.assertEqual(first["scored_ids"], second["scored_ids"])
            self.assertEqual(first["development_ids"], second["development_ids"])
            self.assertEqual(len(first["scored_ids"]), 10)
            self.assertEqual(len(first["development_ids"]), 2)
            self.assertTrue(first["all_development_intervals_disjoint_from_scored"])
            frozen = json.loads(freeze.read_text())
            for split in ("scored", "dev"):
                output = root / "prepared" / f"{split}_incidents.jsonl"
                import hashlib
                self.assertEqual(frozen["prepared_public_manifest_sha256"][split],
                                 hashlib.sha256(output.read_bytes()).hexdigest())
                for line in output.read_text().splitlines():
                    public_case = json.loads(line)
                    self.assertNotIn("ground_truth", public_case)
                    self.assertNotIn("reason", public_case)
                    self.assertNotIn("component", public_case)

            # Eligibility-source changes require an explicit re-freeze, even
            # when the deterministic sample IDs happen to remain unchanged.
            (audit_root / "bank_metric_app_coverage.json").write_text(json.dumps({
                "date_files": {"2021_03_04": {"sha256": "b" * 64}}}))
            with self.assertRaises(FileExistsError):
                sample_bank_cases(public_path, gold_path, eligible_path, root / "prepared3",
                                  audit_root, seed=42, freeze_path=freeze)

    def test_metric_app_coverage_uses_actual_utc8_timestamps_across_date_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metric_root = root / "raw"
            start = datetime(2021, 3, 4, 23, 30, tzinfo=LOCAL)
            case = _public_case(start)
            public_path = root / "candidate.jsonl"
            write_jsonl(public_path, [case])
            rows_by_date = {"2021_03_04": [], "2021_03_05": []}
            for i in range(120):
                instant = start - timedelta(minutes=30) + timedelta(seconds=30 * i)
                day = instant.strftime("%Y_%m_%d")
                row = [str(int(instant.timestamp())), "99.0", "98.0", "12", "53.27", "ServiceTest1"]
                rows_by_date[day].append(row)
            for day, rows in rows_by_date.items():
                path = metric_root / day / "metric" / "metric_app.csv"
                path.parent.mkdir(parents=True)
                with path.open("w", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(["timestamp", "rr", "sr", "cnt", "mrt", "tc"])
                    writer.writerows(rows)
            result = audit_metric_app(metric_root, public_path, root / "audit")
            self.assertEqual(result["expected_cadence_seconds_global_median"], 30)
            self.assertEqual(result["eligible_count"], 1)
            self.assertEqual(result["per_incident_coverage"][0]["reference_unique_timestamps"], 60)
            self.assertEqual(result["per_incident_coverage"][0]["alert_unique_timestamps"], 60)
            self.assertEqual(result["per_incident_coverage"][0]["missing_telemetry_dates"], [])


if __name__ == "__main__":
    unittest.main()
