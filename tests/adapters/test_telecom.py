import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from e4.adapters.telecom import AdapterError, load_case
from e4.prep.telecom import sha256


LOCAL = ZoneInfo("Asia/Shanghai")
CASE = "telecom-20200523T0030-utc8"
HEADERS = {
    "metric/metric_app.csv": ("serviceName", "startTime", "avg_time", "num", "succee_num", "succee_rate"),
    **{f"metric/metric_{table}.csv": ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id")
       for table in ("container", "middleware", "node", "service")},
    "trace/trace_span.csv": ("callType", "startTime", "elapsedTime", "success", "traceId", "id", "pid", "cmdb_id", "dsName", "serviceName"),
}


def _ms(hour, minute):
    return int(datetime(2020, 5, 23, hour, minute, tzinfo=LOCAL).timestamp() * 1000)


def make_fixture(root: Path) -> tuple[Path, Path]:
    prepared = root / "data/prepared/telecom"
    public = prepared / "public"
    case_dir = public / "cases" / CASE
    case_dir.mkdir(parents=True)
    baseline_ms, alert_ms = _ms(0, 0), _ms(0, 30)
    alert_text = "Generic unknown-component incident during the supplied window."
    public_case = {"incident_id": CASE, "alert_window_start_local": "2020-05-23T00:30:00+08:00",
                   "alert_window_end_local": "2020-05-23T01:00:00+08:00", "telemetry_date": "2020_05_23",
                   "timezone": "Asia/Shanghai", "alert_text": alert_text}
    (case_dir / "alert.json").write_text(json.dumps({**public_case,
        "analysis_window_start_local": "2020-05-23T00:00:00+08:00",
        "analysis_window_end_local": "2020-05-23T01:00:00+08:00",
        "analysis_window_start_epoch_ms_utc": baseline_ms,
        "alert_window_start_epoch_ms_utc": alert_ms,
        "alert_window_end_epoch_ms_utc": _ms(1, 0),
        "timestamp_unit_in_source_files": "milliseconds", "raw_rows_preserved": True}, indent=2))
    source_digests = {}
    clip_manifest = []
    for relative, header in HEADERS.items():
        path = case_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        if relative == "metric/metric_app.csv":
            rows = [
                {"serviceName": "osb_001", "startTime": str(baseline_ms), "avg_time": "1", "num": "2", "succee_num": "2", "succee_rate": "1"},
                {"serviceName": "osb_001", "startTime": str(baseline_ms), "avg_time": "9", "num": "4", "succee_num": "4", "succee_rate": "1"},
                {"serviceName": "osb_001", "startTime": str(alert_ms), "avg_time": "2", "num": "3", "succee_num": "2", "succee_rate": "0.66"},
            ]
        elif relative == "trace/trace_span.csv":
            rows = [
                {"callType": "OSB", "startTime": str(baseline_ms), "elapsedTime": "3", "success": "True", "traceId": "t1", "id": "s1", "pid": "None", "cmdb_id": "osb_001", "dsName": "", "serviceName": "osb_001"},
                {"callType": "JDBC", "startTime": str(alert_ms), "elapsedTime": "8", "success": "FALSE", "traceId": "t1", "id": "s2", "pid": "s1", "cmdb_id": "docker_001", "dsName": "db_001", "serviceName": ""},
                {"callType": "JDBC", "startTime": str(alert_ms), "elapsedTime": "bad", "success": "maybe", "traceId": "t2", "id": "s1", "pid": "NULL", "cmdb_id": "docker_002", "dsName": "db_001", "serviceName": ""},
                {"callType": "CSF", "startTime": str(alert_ms), "elapsedTime": "4", "success": "", "traceId": "t3", "id": "s3", "pid": "s1", "cmdb_id": "csf_001", "dsName": "", "serviceName": "csf_001"},
            ]
        else:
            table = relative.rsplit("_", 1)[-1].removesuffix(".csv")
            rows = [
                {"itemid": "i1", "name": "cpu", "bomc_id": "b1", "timestamp": str(baseline_ms), "value": "1.5", "cmdb_id": "docker_001"},
                {"itemid": "i1", "name": "cpu", "bomc_id": "b1", "timestamp": str(alert_ms), "value": "nan", "cmdb_id": "docker_001"},
            ]
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=header)
            writer.writeheader(); writer.writerows(rows)
        digest = sha256(path)
        source_digests[relative] = digest
        clip_manifest.append({"incident_id": CASE, "source_file": f"mirror/{relative}",
                              "source_sha256": "a" * 64, "clip_file": str(path),
                              "clip_sha256": digest, "clip_rows": len(rows),
                              "analysis_start_epoch_ms_utc": baseline_ms,
                              "analysis_end_epoch_ms_utc": _ms(1, 0)})
    catalog = {"source_sha256": "b" * 64,
               "entities": {"docker_001": {"deployment_type": "container_001", "host": "os_017", "container_links": None},
                           "osb_001": {"deployment_type": "service", "host": None, "container_links": None}}}
    catalog_path = public / "deployment_catalog.json"
    catalog_path.write_text(json.dumps(catalog))
    clip_path = public / "clip_manifest.jsonl"
    clip_path.write_text("".join(json.dumps(row) + "\n" for row in clip_manifest))
    scored_path = prepared / "scored_incidents.jsonl"
    dev_path = prepared / "dev_incidents.jsonl"
    scored_path.parent.mkdir(parents=True, exist_ok=True)
    scored_path.write_text("")
    dev_path.write_text(json.dumps({**public_case, "split": "dev"}) + "\n")
    sample = {"prepared_public_manifest_sha256": {"scored": sha256(scored_path), "dev": sha256(dev_path)}}
    config = root / "config/telecom/sample.json"; config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(json.dumps(sample))
    files = {relative: {"sha256": source_digests[relative], "bytes": (case_dir / relative).stat().st_size,
                        "source_sha256": "a" * 64, "rows": next(x for x in clip_manifest if x["clip_file"] == str(case_dir / relative))["clip_rows"]}
             for relative in HEADERS}
    input_manifest = {"schema_version": "e4-input-manifest/1",
                      "sample_config_sha256": sha256(config),
                      "scored_manifest_sha256": sha256(scored_path), "dev_manifest_sha256": sha256(dev_path),
                      "clip_manifest_sha256": sha256(clip_path),
                      "deployment_catalog_file_sha256": sha256(catalog_path),
                      "deployment_mapping_source_sha256": "b" * 64,
                      "cases": {CASE: {"split": "dev", "alert_window_start_local": public_case["alert_window_start_local"],
                                       "alert_window_end_local": public_case["alert_window_end_local"], "files": files}}}
    input_path = root / "config/telecom/input_manifest.json"
    input_path.write_text(json.dumps(input_manifest))
    return root, input_path


class TelecomAdapterTests(unittest.TestCase):
    def test_loads_roles_half_open_windows_duplicate_rows_and_boolean_parse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, input_path = make_fixture(Path(temporary))
            telemetry = load_case(CASE, "dev", root=root, input_manifest_path=input_path)
            self.assertEqual(len(telemetry.spans), 4)
            self.assertEqual(len(telemetry.metrics), 3 * 4 + 2 * 4)  # app values plus four metric tables
            self.assertEqual(telemetry.duplicate_counts["metric/metric_app.csv"], 1)
            self.assertEqual(telemetry.quality_counts["metric_app_distinct_time_buckets"], 2)
            self.assertEqual(telemetry.spans[1].success, False)
            self.assertEqual(telemetry.spans[1].success_status, "valid")
            self.assertEqual(telemetry.spans[2].success, None)
            self.assertEqual(telemetry.spans[2].success_status, "invalid")
            self.assertTrue(telemetry.spans[1].parent_resolved_in_clip)
            self.assertIsNone(telemetry.spans[0].parent_id)
            self.assertIsNone(telemetry.spans[2].parent_id)
            self.assertTrue(telemetry.spans[3].parent_unresolved_in_clip)
            self.assertEqual(telemetry.quality_counts["trace_parent_unresolved_within_clip"], 1)
            self.assertEqual(telemetry.spans[1].executing_component.identifier, "docker_001")
            self.assertEqual(telemetry.spans[1].database_target.identifier, "db_001")
            self.assertEqual(telemetry.metrics[0].entity.role, "service")
            self.assertEqual(telemetry.metrics[12].entity.role, "component")
            self.assertEqual(telemetry.reference_window.end_ms_utc, telemetry.alert_window.start_ms_utc)
            self.assertEqual(telemetry.quality_counts["metric/metric_container.csv_value_nonfinite"], 1)
            with self.assertRaises(TypeError):
                telemetry.deployment_catalog["new"] = {}

    def test_refuses_seconds_valued_clip_timestamp_and_tampered_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, input_path = make_fixture(Path(temporary))
            path = root / "data/prepared/telecom/public/cases" / CASE / "metric/metric_node.csv"
            lines = path.read_text().splitlines()
            fields = lines[1].split(",")
            fields[3] = str(int(fields[3]) // 1000)
            lines[1] = ",".join(fields)
            path.write_text("\n".join(lines) + "\n")
            new_hash = sha256(path)
            input_manifest = json.loads(input_path.read_text())
            input_manifest["cases"][CASE]["files"]["metric/metric_node.csv"]["sha256"] = new_hash
            input_manifest["cases"][CASE]["files"]["metric/metric_node.csv"]["bytes"] = path.stat().st_size
            clip_path = root / "data/prepared/telecom/public/clip_manifest.jsonl"
            clip_rows = [json.loads(line) for line in clip_path.read_text().splitlines()]
            for row in clip_rows:
                if row["incident_id"] == CASE and row["clip_file"] == str(path):
                    row["clip_sha256"] = new_hash
            clip_path.write_text("".join(json.dumps(row) + "\n" for row in clip_rows))
            input_manifest["clip_manifest_sha256"] = sha256(clip_path)
            input_path.write_text(json.dumps(input_manifest))
            with self.assertRaisesRegex(AdapterError, "outside half-open case windows"):
                load_case(CASE, "dev", root=root, input_manifest_path=input_path)

    def test_reference_window_cannot_be_widened_in_unpinned_alert_json(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, input_path = make_fixture(Path(temporary))
            alert_path = root / "data/prepared/telecom/public/cases" / CASE / "alert.json"
            alert = json.loads(alert_path.read_text())
            alert["analysis_window_start_epoch_ms_utc"] -= 60_000
            alert["analysis_window_start_local"] = "2020-05-22T23:59:00+08:00"
            alert_path.write_text(json.dumps(alert))
            with self.assertRaisesRegex(AdapterError, "exactly the frozen preceding 30 minutes"):
                load_case(CASE, "dev", root=root, input_manifest_path=input_path)


if __name__ == "__main__":
    unittest.main()
