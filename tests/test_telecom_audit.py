import csv
import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from e4infra.telecom_audit import (FILE_SPECS, audit_and_clip,
                                   read_deployment_mapping)
from scripts.audit_clip_telecom import with_output_lock


def make_mapping(path: Path) -> None:
    workbook = '''<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'''
    rels = '''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml" Type="worksheet"/></Relationships>'''
    sheet = '''<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
      <row r="1"><c r="A1" t="inlineStr"><is><t>Type</t></is></c><c r="D1" t="inlineStr"><is><t>Name</t></is></c><c r="E1" t="inlineStr"><is><t>Container</t></is></c></row>
      <row r="2"><c r="A2" t="inlineStr"><is><t>CSF</t></is></c><c r="D2" t="inlineStr"><is><t>csf_001</t></is></c><c r="E2" t="inlineStr"><is><t>container_002</t></is></c></row>
      <row r="3"><c r="D3" t="inlineStr"><is><t>csf_002</t></is></c><c r="E3"/></row>
      <row r="4"><c r="A4" t="inlineStr"><is><t>container_001</t></is></c><c r="D4" t="inlineStr"><is><t>docker_001</t></is></c></row>
    </sheetData><mergeCells count="2"><mergeCell ref="A2:A3"/><mergeCell ref="E2:E3"/></mergeCells></worksheet>'''
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", rels)
        archive.writestr("xl/worksheets/sheet1.xml", sheet)


class TelecomAuditTests(unittest.TestCase):
    def test_lock_rejects_concurrent_clip_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "public/cases"
            first_audit = Path(temporary) / "first-audit.json"
            second_audit = Path(temporary) / "second-audit.json"
            with self.assertRaisesRegex(RuntimeError, "another telemetry clip run"):
                # A different audit path cannot bypass the lock because it
                # is derived from the physical public output directory.
                with_output_lock(output, first_audit,
                    lambda: with_output_lock(output, second_audit, lambda: None))

    def test_mapping_expands_merged_type_and_link_without_aliasing(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "mapping.xlsx"
            make_mapping(path)
            mapping = read_deployment_mapping(path)
            self.assertEqual(mapping["csf_002"]["deployment_type"], "CSF")
            self.assertEqual(mapping["csf_002"]["container_links"], ["container_002"])
            self.assertEqual(mapping["docker_001"]["deployment_type"], "container_001")
            self.assertNotIn("container_001", mapping["docker_001"]["container_links"] or [])

    def test_clips_half_open_rows_and_audits_trace_parent_scope(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            date = "2020_04_11"
            start = datetime(2020, 4, 11, 1, 0)
            stamps = [int(datetime(2020, 4, 11, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp() * 1000)
                      for hour, minute in [(0, 29), (0, 30), (1, 0), (1, 29), (1, 30)]]
            day_dir = root / "raw" / date
            for relative, spec in FILE_SPECS.items():
                path = day_dir / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                fields = spec["required"]
                with path.open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    for index, stamp in enumerate(stamps):
                        row = {field: "" for field in fields}
                        row[spec["time"]] = str(stamp)
                        for component_field in spec["component_fields"]:
                            row[component_field] = "svc_001"
                        if relative == "trace/trace_span.csv":
                            # Parent s1 resolves only within trace t1; t2/s3 has the
                            # same pid but must not resolve against t1/s1.
                            trace_id, span_id, parent = [("t1", "s1", ""),
                                                         ("t1", "s2", "s1"),
                                                         ("t2", "s3", "s1"),
                                                         ("t2", "s4", ""),
                                                         ("t1", "s5", "s1")][index]
                            row.update({"traceId": trace_id, "id": span_id, "pid": parent,
                                        "success": "False" if index == 1 else "True",
                                        "callType": "JDBC", "elapsedTime": "4", "dsName": "db_001"})
                        elif relative == "metric/metric_app.csv":
                            row.update({"serviceName": "osb_001", "avg_time": "1", "num": "2",
                                        "succee_num": "2", "succee_rate": "1"})
                        else:
                            row.update({"itemid": "item", "name": "metric", "bomc_id": "bomc",
                                        "value": "1", "cmdb_id": "svc_001"})
                        writer.writerow(row)
            mapping_path = root / "mapping.xlsx"
            make_mapping(mapping_path)
            case = {"incident_id": "case-1", "alert_window_start_local": start.isoformat() + "+08:00",
                    "alert_window_end_local": "2020-04-11T01:30:00+08:00",
                    "telemetry_date": date, "timezone": "Asia/Shanghai",
                    "alert_text": "Unknown failure during interval."}
            result = audit_and_clip([case], root / "raw", mapping_path, root / "public",
                                    root / "audit.json", root / "scratch")
            app = root / "public/cases/case-1/metric/metric_app.csv"
            with app.open() as stream:
                self.assertEqual(sum(1 for _ in csv.DictReader(stream)), 3)
            self.assertEqual(result["file_audits"][f"{date}/trace/trace_span.csv"]["trace_parent_integrity"]["resolved_parent_references_same_trace"], 2)
            self.assertEqual(result["file_audits"][f"{date}/trace/trace_span.csv"]["trace_parent_integrity"]["unresolved_parent_references_same_trace"], 1)
            self.assertEqual(result["file_audits"][f"{date}/trace/trace_span.csv"]["success_value_counts_preserving_source_spelling"]["False"], 1)
            self.assertIn("deployment_catalog.json", [path.name for path in (root / "public").iterdir()])
            self.assertNotIn("reason", (root / "public/cases/case-1/alert.json").read_text())
            # A seconds-valued timestamp in a telemetry file must fail loudly,
            # rather than silently producing empty clips around 1970.
            bad_path = day_dir / "metric/metric_app.csv"
            with bad_path.open(newline="") as stream:
                rows = list(csv.DictReader(stream))
                fields = list(rows[0])
            rows[0]["startTime"] = str(int(rows[0]["startTime"]) // 1000)
            with bad_path.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(ValueError, "check epoch units"):
                audit_and_clip([case], root / "raw", mapping_path, root / "public",
                               root / "bad-unit-audit.json", root / "scratch")


if __name__ == "__main__":
    unittest.main()
