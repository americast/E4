import csv
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

from e4.adapters.bank import (BankAdapterError, _iter_csv, load_bank_case, sha256_file)
from e4.prep.bank import build_public_catalog


ROOT = Path(__file__).resolve().parents[2]
HEADERS = {
    "metric/metric_app.csv": ("timestamp", "rr", "sr", "cnt", "mrt", "tc"),
    "metric/metric_container.csv": ("timestamp", "cmdb_id", "kpi_name", "value"),
    "trace/trace_span.csv": ("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"),
    "log/log_service.csv": ("log_id", "timestamp", "cmdb_id", "log_name", "value"),
}
UNITS = {"metric/metric_app.csv": "seconds", "metric/metric_container.csv": "seconds",
         "trace/trace_span.csv": "milliseconds", "log/log_service.csv": "seconds"}


def _write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def make_bank_fixture(root: Path, *, date_key: str = "2021_03_04",
                      trace_unit: str = "milliseconds") -> tuple[str, Path]:
    day = datetime.strptime(date_key, "%Y_%m_%d")
    case_id = f"bank-{day:%Y%m%d}T0100-utc8"
    base = day.replace(hour=0, minute=30, tzinfo=timezone(timedelta(hours=8)))
    local = {
        "analysis_window_start_local": base.isoformat(),
        "alert_window_start_local": (base + timedelta(minutes=30)).isoformat(),
        "alert_window_end_local": (base + timedelta(minutes=60)).isoformat(),
    }
    ms = {name: int(datetime.fromisoformat(value).timestamp() * 1000)
          for name, value in local.items()}
    sec_ref = ms["analysis_window_start_local"] // 1000 + 900
    sec_alert = ms["alert_window_start_local"] // 1000 + 600
    telemetry_dates = [date_key]
    units = dict(UNITS)
    units["trace/trace_span.csv"] = trace_unit
    alert = {"incident_id": case_id, "timezone": "Asia/Shanghai",
             "alert_text": "A Bank platform incident was reported in the supplied interval. Diagnose from public telemetry.",
             **local, "analysis_window_start_epoch_ms_utc": ms["analysis_window_start_local"],
             "alert_window_start_epoch_ms_utc": ms["alert_window_start_local"],
             "alert_window_end_epoch_ms_utc": ms["alert_window_end_local"],
             "telemetry_date": telemetry_dates[0], "telemetry_dates": telemetry_dates,
             "timestamp_unit_in_source_files": UNITS,
             "upstream_timestamp_unit_provenance": "official Bank prompt schema; empirical units are pinned per source date",
             "source_date_timestamp_units": {telemetry_dates[0]: units}}
    case_path = root / "data/prepared/bank/public/cases" / case_id
    case_path.mkdir(parents=True)
    alert_path = case_path / "alert.json"
    alert_path.write_text(json.dumps(alert, sort_keys=True), encoding="utf-8")
    rows_by_file = {
        "metric/metric_app.csv": [
            {"timestamp": sec_ref, "rr": "10", "sr": "0.99", "cnt": "12", "mrt": "4.5", "tc": "ServiceTest1"},
            {"timestamp": sec_alert, "rr": "11", "sr": "0.98", "cnt": "14", "mrt": "5.5", "tc": "ServiceTest1"}],
        "metric/metric_container.csv": [
            {"timestamp": sec_ref, "cmdb_id": "Tomcat01", "kpi_name": "OSLinux-CPU_CPU_CPUCpuUtil", "value": "25"},
            {"timestamp": sec_alert, "cmdb_id": "Tomcat01", "kpi_name": "OSLinux-CPU_CPU_CPUCpuUtil", "value": "50"}],
        "trace/trace_span.csv": [
            {"timestamp": ms["analysis_window_start_local"] + 600000, "cmdb_id": "Tomcat01", "parent_id": "", "span_id": "root", "trace_id": "t1", "duration": "8"},
            {"timestamp": ms["analysis_window_start_local"] + 601000, "cmdb_id": "dockerA1", "parent_id": "root", "span_id": "child1", "trace_id": "t1", "duration": "3"},
            {"timestamp": ms["alert_window_start_local"] + 600000, "cmdb_id": "Tomcat01", "parent_id": "", "span_id": "root", "trace_id": "t2", "duration": "12"},
            {"timestamp": ms["alert_window_start_local"] + 601000, "cmdb_id": "dockerA1", "parent_id": "child1", "span_id": "child2", "trace_id": "t2", "duration": "6"}],
        "log/log_service.csv": [
            {"log_id": "l1", "timestamp": sec_ref, "cmdb_id": "Tomcat01", "log_name": "gc", "value": "arbitrary raw line"},
            {"log_id": "l2", "timestamp": sec_ref + 20, "cmdb_id": "Tomcat01", "log_name": "gc", "value": "another raw line"},
            {"log_id": "l3", "timestamp": sec_alert, "cmdb_id": "dockerA1", "log_name": "gc", "value": "unparsed value"}],
    }
    file_docs, clip_docs = {}, []
    for relative, rows in rows_by_file.items():
        manifest_relative = f"source_dates/{telemetry_dates[0]}/{relative}"
        if relative == "trace/trace_span.csv" and trace_unit == "seconds":
            rows = [{**row, "timestamp": row["timestamp"] // 1000} for row in rows]
        path = case_path / manifest_relative
        _write_csv(path, HEADERS[relative], rows)
        sha = sha256_file(path)
        source_sha = hashlib.sha256(("source:" + relative).encode()).hexdigest()
        blob_sha = hashlib.sha1(("git blob:" + relative).encode()).hexdigest()
        lfs_sha = hashlib.sha256(("lfs:" + relative).encode()).hexdigest()
        file_docs[manifest_relative] = {"sha256": sha, "source_sha256": source_sha,
                               "source_git_blob_sha1": blob_sha,
                               "source_lfs_sha256": lfs_sha,
                               "rows": len(rows), "size_bytes": path.stat().st_size,
                               "header": list(HEADERS[relative]), "timestamp_unit": units[relative]}
        clip_docs.append({"case_id": case_id, "relative_path": manifest_relative,
                          "source_date": "2021_03_04", "source_path": f"{relative}",
                          "source_size_bytes": 12345, "source_sha256": source_sha,
                          "source_git_blob_sha1": blob_sha, "source_lfs_sha256": lfs_sha,
                          "clip_size_bytes": path.stat().st_size, "clip_sha256": sha,
                          "clip_rows": len(rows), "source_rows_scanned": 1000,
                          "timestamp_field": "timestamp", "timestamp_source_unit": units[relative],
                          "reference_window_utc_ms": [ms["analysis_window_start_local"], ms["alert_window_start_local"]],
                          "alert_window_utc_ms": [ms["alert_window_start_local"], ms["alert_window_end_local"]],
                          "header": list(HEADERS[relative])})
    public_row = {"incident_id": case_id, "alert_text": alert["alert_text"],
                  "timezone": alert["timezone"], **local,
                  "analysis_window_start_epoch_ms_utc": ms["analysis_window_start_local"],
                  "alert_window_start_epoch_ms_utc": ms["alert_window_start_local"],
                  "alert_window_end_epoch_ms_utc": ms["alert_window_end_local"],
                  "telemetry_date": telemetry_dates[0], "telemetry_dates": telemetry_dates,
                  "timestamp_unit_in_source_files": UNITS,
                  "upstream_timestamp_unit_provenance": "official Bank prompt schema; empirical units are pinned per source date",
                  "source_date_timestamp_units": {telemetry_dates[0]: units}}
    prepared_root = root / "data/prepared/bank"
    prepared_root.mkdir(parents=True, exist_ok=True)
    candidate_path = prepared_root / "candidate_incidents.jsonl"
    dev_path = prepared_root / "dev_incidents.jsonl"
    scored_path = prepared_root / "scored_incidents.jsonl"
    row_line = json.dumps(public_row, sort_keys=True) + "\n"
    candidate_path.write_text(row_line, encoding="utf-8")
    dev_path.write_text(row_line, encoding="utf-8")
    scored_path.write_text("", encoding="utf-8")
    sample_path = root / "config/bank/sample.json"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    sample_path.write_text(json.dumps({"sample": "fixture"}), encoding="utf-8")
    (root / "config/bank/metric_kind_policy.json").write_bytes(
        (ROOT / "config/bank/metric_kind_policy.json").read_bytes())
    (root / "config/bank/kpi_policy.json").write_bytes(
        (ROOT / "config/bank/kpi_policy.json").read_bytes())
    prompt_path = root / "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text("pinned source prompt fixture", encoding="utf-8")
    prompt_sha = sha256_file(prompt_path)
    catalog_path = root / "data/prepared/bank/public/deployment_catalog.json"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    source_config = json.loads((ROOT / "config/bank/source.json").read_text(encoding="utf-8"))
    catalog = build_public_catalog(source_config,
        {"Tomcat01": {"metric/metric_container.csv.cmdb_id"}}, prompt_sha)
    catalog_path.write_text(json.dumps(catalog, sort_keys=True), encoding="utf-8")
    clip_path = root / "data/prepared/bank/public/clip_manifest.jsonl"
    clip_path.write_text("".join(json.dumps(item, sort_keys=True) + "\n" for item in clip_docs), encoding="utf-8")
    manifest = {"schema_version": "e4-bank-input-manifest/1",
        "sample_config_sha256": sha256_file(sample_path),
        "candidate_manifest_sha256": sha256_file(candidate_path),
        "dev_manifest_sha256": sha256_file(dev_path),
        "scored_manifest_sha256": sha256_file(scored_path),
        "clip_manifest_sha256": sha256_file(clip_path),
        "deployment_catalog_file_sha256": sha256_file(catalog_path),
        "deployment_catalog_schema_sha256": prompt_sha,
        "cases": {case_id: {"alert_sha256": sha256_file(alert_path),
            "windows": {"reference": {"start_epoch_ms_utc": ms["analysis_window_start_local"], "end_epoch_ms_utc": ms["alert_window_start_local"]},
                        "alert": {"start_epoch_ms_utc": ms["alert_window_start_local"], "end_epoch_ms_utc": ms["alert_window_end_local"]}},
            "files": file_docs}}}
    manifest_path = root / "config/bank/input_manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return case_id, manifest_path


def set_upstream_unit_labels(root: Path, case_id: str, *, bad_value: str | None = None) -> None:
    alert_path = root / "data/prepared/bank/public/cases" / case_id / "alert.json"
    alert = json.loads(alert_path.read_text(encoding="utf-8"))
    labels = {relative: f"epoch {unit}" for relative, unit in
              alert["timestamp_unit_in_source_files"].items()}
    if bad_value is not None:
        labels["metric/metric_app.csv"] = bad_value
    alert["timestamp_unit_in_source_files"] = labels
    alert_path.write_text(json.dumps(alert, sort_keys=True), encoding="utf-8")

    public_path = root / "data/prepared/bank/dev_incidents.jsonl"
    rows = [json.loads(line) for line in public_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    for row in rows:
        if row["incident_id"] == case_id:
            row["timestamp_unit_in_source_files"] = labels
    public_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                           encoding="utf-8")

    manifest_path = root / "config/bank/input_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["cases"][case_id]["alert_sha256"] = sha256_file(alert_path)
    manifest["dev_manifest_sha256"] = sha256_file(public_path)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


class BankAdapterTests(unittest.TestCase):
    def test_actual_prep_catalog_builder_roundtrips_through_loader(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            case = load_bank_case(case_id, "dev", root=root)
        self.assertEqual(case.deployment_catalog["schema_version"], "bank-source-catalog/1")
        self.assertEqual(case.deployment_catalog["source"]["official_revision"],
                         "c121a483a5a91dbae50708e5942c42a6609098b2")
        self.assertEqual(case.deployment_catalog["topology"]["relationships"], [])

    def test_loader_preserves_source_roles_units_and_unknown_trace_semantics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            case = load_bank_case(case_id, "dev", root=root)
        telemetry = case.telemetry
        self.assertEqual(telemetry.reference_window.end_ms_utc,
                         telemetry.alert_window.start_ms_utc)
        app = next(point for point in telemetry.metrics if point.table == "bank/metric_app.csv")
        self.assertEqual(app.entity.role, "app_kpi")
        self.assertEqual(app.entity.identifier, "ServiceTest1")
        container = next(point for point in telemetry.metrics
                         if point.table == "bank/metric_container.csv")
        self.assertEqual((container.entity.role, container.entity.identifier),
                         ("component", "Tomcat01"))
        self.assertEqual(container.metric_name, "OSLinux-CPU_CPU_CPUCpuUtil")
        traces = telemetry.spans
        self.assertEqual(len(traces), 4)
        t1_child = next(span for span in traces if span.trace_id == "t1" and span.span_id == "child1")
        t2_child = next(span for span in traces if span.trace_id == "t2" and span.span_id == "child2")
        self.assertTrue(t1_child.parent_resolved_in_clip)
        self.assertFalse(t2_child.parent_resolved_in_clip)
        self.assertTrue(t2_child.parent_unresolved_in_clip)
        self.assertIsNone(t1_child.success)
        self.assertEqual(t1_child.success_status, "missing")
        self.assertEqual(t1_child.call_type, "unprovided")
        self.assertEqual(t1_child.elapsed_raw, "3")
        self.assertEqual(telemetry.quality_counts["trace_span_success_status_missing_rows"], 4)
        self.assertEqual(telemetry.quality_counts["bank_unknown_component_id_rows"], 3)
        log_points = [p for p in telemetry.metrics if p.table == "bank/log/log_service.csv"]
        self.assertEqual(len(log_points), 120)
        tomcat_counts = [p.value for p in log_points if p.entity.identifier == "Tomcat01" and p.item_id == "gc"]
        docker_counts = [p.value for p in log_points if p.entity.identifier == "dockerA1" and p.item_id == "gc"]
        self.assertEqual(sum(tomcat_counts[:30]), 2)
        self.assertEqual(sum(tomcat_counts[30:]), 0)
        self.assertEqual(sum(docker_counts[:30]), 0)
        self.assertEqual(sum(docker_counts[30:]), 1)
        self.assertIn("not evidence of healthy logging", case.log_summary["summary_policy"])
        self.assertEqual(case.log_summary["raw_rows"], 3)
        self.assertEqual(case.log_summary["summary_truncated"], False)

    def test_only_dev_and_scored_frozen_splits_are_loadable_not_candidate_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            with self.assertRaisesRegex(BankAdapterError, "scored or dev"):
                load_bank_case(case_id, "candidate", root=root)

    def test_per_source_date_empirical_trace_units_map_to_same_epoch_without_heuristics(self):
        for date_key, unit in (("2021_03_04", "milliseconds"),
                               ("2021_03_23", "seconds")):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                case_id, _ = make_bank_fixture(root, date_key=date_key, trace_unit=unit)
                case = load_bank_case(case_id, "dev", root=root)
            child = next(span for span in case.telemetry.spans
                         if span.trace_id == "t1" and span.span_id == "child1")
            self.assertEqual(child.start_ms_utc,
                             case.telemetry.reference_window.start_ms_utc + 601000)
            self.assertEqual(case.provenance["empirical_timestamp_units_by_source_file"][
                f"source_dates/{date_key}/trace/trace_span.csv"], unit)

    def test_official_epoch_unit_labels_are_accepted_but_empirical_labels_stay_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            set_upstream_unit_labels(root, case_id)
            case = load_bank_case(case_id, "dev", root=root)
            self.assertEqual(case.provenance["upstream_declared_timestamp_units"][
                "trace/trace_span.csv"], "epoch milliseconds")
            self.assertEqual(case.provenance["empirical_timestamp_units_by_source_file"][
                "source_dates/2021_03_04/trace/trace_span.csv"], "milliseconds")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            set_upstream_unit_labels(root, case_id, bad_value="epoch seconds approx")
            with self.assertRaisesRegex(BankAdapterError, "upstream-declared timestamp units"):
                load_bank_case(case_id, "dev", root=root)

    def test_hash_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            file_path = root / "data/prepared/bank/public/cases" / case_id / "source_dates/2021_03_04/metric/metric_app.csv"
            original = file_path.read_bytes()
            file_path.write_bytes(original.replace(b",10,0.99,", b",99,0.99,"))
            with self.assertRaisesRegex(BankAdapterError, "SHA-256 mismatch"):
                load_bank_case(case_id, "dev", root=root)

    def test_log_projection_fails_after_full_file_read_when_group_cap_is_exceeded(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_id, _ = make_bank_fixture(root)
            policy = json.loads((ROOT / "config/bank/metric_kind_policy.json").read_text())
            policy["loader_limits"]["max_log_groups"] = 1
            policy_path = root / "config/bank/metric_kind_policy.json"
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            with self.assertRaisesRegex(BankAdapterError, "fully read and hash/row-count verified"):
                load_bank_case(case_id, "dev", root=root, metric_policy_path=policy_path)

    def test_malformed_csv_row_width_is_rejected_but_empty_cells_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixture.csv"
            path.write_text("a,b\n1,\n2\n3,4,5\n", encoding="utf-8")
            with self.assertRaisesRegex(BankAdapterError, "row 3 has a different number of fields"):
                list(_iter_csv(path, ("a", "b"), 100))
            path.write_text("a,b\n1,\n", encoding="utf-8")
            rows = list(_iter_csv(path, ("a", "b"), 100))
            self.assertEqual(rows, [(2, {"a": "1", "b": ""})])


if __name__ == "__main__":
    unittest.main()
