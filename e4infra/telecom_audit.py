"""Streaming audit and blind, per-incident clipping of OpenRCA Telecom telemetry."""

from __future__ import annotations

import csv
import json
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .telecom_prep import LOCAL_ZONE, sha256, write_jsonl

FILE_SPECS = {
    "metric/metric_app.csv": {"time": "startTime", "required": ("serviceName", "startTime", "avg_time", "num", "succee_num", "succee_rate"), "component_fields": ("serviceName",)},
    "metric/metric_container.csv": {"time": "timestamp", "required": ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id"), "component_fields": ("cmdb_id",)},
    "metric/metric_middleware.csv": {"time": "timestamp", "required": ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id"), "component_fields": ("cmdb_id",)},
    "metric/metric_node.csv": {"time": "timestamp", "required": ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id"), "component_fields": ("cmdb_id",)},
    "metric/metric_service.csv": {"time": "timestamp", "required": ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id"), "component_fields": ("cmdb_id",)},
    "trace/trace_span.csv": {"time": "startTime", "required": ("callType", "startTime", "elapsedTime", "success", "traceId", "id", "pid", "cmdb_id", "dsName", "serviceName"), "component_fields": ("cmdb_id", "dsName", "serviceName")},
}
XLSX_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
           "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
           "rel": "http://schemas.openxmlformats.org/package/2006/relationships"}


def _column_number(reference: str) -> int:
    letters = "".join(ch for ch in reference if ch.isalpha())
    value = 0
    for ch in letters:
        value = value * 26 + ord(ch.upper()) - ord("A") + 1
    return value - 1


def read_deployment_mapping(path: Path) -> dict[str, dict[str, Any]]:
    """Read the one-sheet XLSX using ZIP/XML from the standard library."""
    with zipfile.ZipFile(path) as archive:
        shared = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            shared = ["".join(node.text or "" for node in item.findall(".//m:t", XLSX_NS))
                      for item in root.findall("m:si", XLSX_NS)]
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relation_id = workbook.find("m:sheets/m:sheet", XLSX_NS).attrib[
            "{" + XLSX_NS["r"] + "}id"]
        relations = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        target = next(rel.attrib["Target"] for rel in relations.findall("rel:Relationship", XLSX_NS)
                      if rel.attrib["Id"] == relation_id)
        worksheet_path = target.lstrip("/") if target.startswith("/") else "xl/" + target
        sheet = ET.fromstring(archive.read(worksheet_path))
        rows: dict[int, dict[int, str]] = {}
        for row in sheet.findall(".//m:sheetData/m:row", XLSX_NS):
            values = {}
            for cell in row.findall("m:c", XLSX_NS):
                value_node = cell.find("m:v", XLSX_NS)
                value = value_node.text if value_node is not None else ""
                if cell.attrib.get("t") == "s" and value:
                    value = shared[int(value)]
                elif cell.attrib.get("t") == "inlineStr":
                    value = "".join(node.text or "" for node in cell.findall(".//m:t", XLSX_NS))
                values[_column_number(cell.attrib["r"])] = value
            rows[int(row.attrib["r"])] = values
        # Expand XLSX merged cells. In this workbook deployment groups and
        # container links are represented by merged ranges; they are links,
        # not aliases for the named deployment entities.
        for merged in sheet.findall(".//m:mergeCells/m:mergeCell", XLSX_NS):
            first, last = merged.attrib["ref"].split(":")
            def cell_pos(reference: str) -> tuple[int, int]:
                letters = "".join(ch for ch in reference if ch.isalpha())
                digits = "".join(ch for ch in reference if ch.isdigit())
                return int(digits) - 1, _column_number(letters)
            top_row, left_col = cell_pos(first)
            bottom_row, right_col = cell_pos(last)
            # cell_pos returns zero-based rows/columns; worksheet row numbers
            # are one-based and explicit empty cells must be filled as well.
            source_value = rows.get(top_row + 1, {}).get(left_col, "")
            for row_number in range(top_row + 1, bottom_row + 2):
                rows.setdefault(row_number, {})
                for col_number in range(left_col, right_col + 1):
                    if not rows[row_number].get(col_number):
                        rows[row_number][col_number] = source_value
    catalog: dict[str, dict[str, Any]] = {}
    current_type = ""
    for row_number in sorted(rows):
        if row_number == 1:
            continue
        values = rows[row_number]
        current_type = values.get(0, "") or current_type
        name = values.get(3, "").strip()
        if not name:
            continue
        container_links = [values.get(index, "").strip() for index in range(4, 24)
                           if values.get(index, "").strip()]
        catalog[name] = {"deployment_type": current_type,
                         "host": values.get(2, "").strip() or None,
                         "container_links": container_links or None}
    if not catalog:
        raise ValueError(f"no component names found in deployment mapping {path}")
    return catalog


def _ms(local: datetime) -> int:
    return int(local.astimezone(timezone.utc).timestamp() * 1000)


def _clean_id(value: str | None) -> str | None:
    if value is None or value.strip() in {"", "None", "none", "NULL", "null"}:
        return None
    return value.strip()


def _write_case_metadata(case: dict, destination: Path) -> dict[str, Any]:
    start_local = datetime.fromisoformat(case["alert_window_start_local"])
    end_local = datetime.fromisoformat(case["alert_window_end_local"])
    baseline_local = start_local - timedelta(minutes=30)
    public_case = {key: case[key] for key in ("incident_id", "alert_window_start_local",
                   "alert_window_end_local", "telemetry_date", "timezone", "alert_text")}
    public_case.update({"analysis_window_start_local": baseline_local.isoformat(timespec="seconds"),
                        "analysis_window_end_local": end_local.isoformat(timespec="seconds"),
                        "analysis_window_start_epoch_ms_utc": _ms(baseline_local),
                        "alert_window_start_epoch_ms_utc": _ms(start_local),
                        "alert_window_end_epoch_ms_utc": _ms(end_local),
                        "timestamp_unit_in_source_files": "milliseconds",
                        "raw_rows_preserved": True})
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "alert.json").write_text(json.dumps(public_case, indent=2) + "\n")
    return public_case


def _trace_integrity(path: Path, scratch: Path, batch_size: int = 20000) -> dict[str, Any]:
    """Index distinct same-trace span keys in memory and resolve parent references."""
    started = time.monotonic()
    counts = Counter()
    missing = Counter()
    success = Counter()
    call_type = Counter()
    span_keys: set[tuple[str, str]] = set()
    parent_refs: list[tuple[str, str]] = []
    trace_ids: set[str] = set()
    duplicate_keys = 0
    min_ms = max_ms = None
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        header = tuple(reader.fieldnames or ())
        for row in reader:
            counts["rows"] += 1
            for column in header:
                if not row[column].strip():
                    missing[column] += 1
            timestamp = _clean_id(row.get("startTime"))
            if timestamp:
                try:
                    value = int(timestamp)
                    min_ms = value if min_ms is None else min(min_ms, value)
                    max_ms = value if max_ms is None else max(max_ms, value)
                except ValueError:
                    counts["invalid_startTime"] += 1
            trace_id = _clean_id(row.get("traceId"))
            span_id = _clean_id(row.get("id"))
            parent_id = _clean_id(row.get("pid"))
            if not trace_id:
                counts["missing_traceId"] += 1
            if not span_id:
                counts["missing_id"] += 1
            if not parent_id:
                counts["root_or_missing_parent_pid"] += 1
            if trace_id and span_id:
                counts["indexed_rows"] += 1
                key = (trace_id, span_id)
                trace_ids.add(trace_id)
                if key in span_keys:
                    duplicate_keys += 1
                else:
                    span_keys.add(key)
                if parent_id:
                    parent_refs.append((trace_id, parent_id))
            success[row.get("success", "").strip()] += 1
            call_type[row.get("callType", "").strip()] += 1
    linked = sum((trace_id, parent_id) in span_keys for trace_id, parent_id in parent_refs)
    parent_count = len(parent_refs)
    return {"rows": counts["rows"], "indexed_rows": counts["indexed_rows"],
            "missing_by_column": dict(missing), "missing_traceId": counts["missing_traceId"],
            "missing_span_id": counts["missing_id"],
            "rows_without_parent_pid_or_root": counts["root_or_missing_parent_pid"],
            "distinct_trace_ids": len(trace_ids), "duplicate_trace_span_keys": duplicate_keys,
            "nonroot_parent_references": parent_count,
            "resolved_parent_references_same_trace": linked,
            "unresolved_parent_references_same_trace": parent_count - linked,
            "timestamp_unit": "milliseconds", "timestamp_min_ms": min_ms,
            "timestamp_max_ms": max_ms,
            "timestamp_min_local": datetime.fromtimestamp(min_ms / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat() if min_ms is not None else None,
            "timestamp_max_local": datetime.fromtimestamp(max_ms / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat() if max_ms is not None else None,
            "success_value_counts_preserving_source_spelling": dict(success),
            "call_type_counts": dict(call_type), "audit_seconds": round(time.monotonic() - started, 3)}


def audit_and_clip(public_cases: list[dict], telemetry_root: Path, mapping_path: Path,
                   output_root: Path, audit_path: Path,
                   scratch_root: Path | None = None) -> dict[str, Any]:
    """Audit selected full-day files and stream only each case's baseline+alert rows to blind clips."""
    output_root.mkdir(parents=True, exist_ok=True)
    # Never leave a prior success marker beside clips being regenerated.
    (output_root / "clip_manifest.jsonl").unlink(missing_ok=True)
    audit_path.unlink(missing_ok=True)
    mapping = read_deployment_mapping(mapping_path)
    (output_root / "deployment_catalog.json").write_text(
        json.dumps({"source_sha256": sha256(mapping_path), "entities": mapping}, indent=2) + "\n")
    by_date: dict[str, list[dict]] = defaultdict(list)
    case_paths = {}
    for case in public_cases:
        case_id = case["incident_id"]
        case_path = output_root / "cases" / case_id
        by_date[case["telemetry_date"]].append(case)
        case_paths[case_id] = case_path
        _write_case_metadata(case, case_path)

    file_reports: dict[str, Any] = {}
    clip_reports = []
    observed_component_ids: dict[str, Counter] = defaultdict(Counter)
    scratch_parent = scratch_root or output_root.parent / ".trace-audit-tmp"
    for date, cases in sorted(by_date.items()):
        for relative, spec in FILE_SPECS.items():
            source = telemetry_root / date / relative
            print(f"auditing {date}/{relative} ({source.stat().st_size if source.is_file() else 'missing'} bytes)",
                  file=sys.stderr, flush=True)
            if not source.is_file():
                raise FileNotFoundError(f"selected date {date} is missing required telemetry file {source}")
            with source.open("r", newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream)
                fields = tuple(reader.fieldnames or ())
                missing_fields = sorted(set(spec["required"]) - set(fields))
                if missing_fields:
                    raise ValueError(f"{source}: missing documented fields {missing_fields}; actual header={fields}")
                missing = Counter()
                row_count = 0
                min_ms = max_ms = None
                invalid_time = 0
                wrong_date_timestamp = None
                key_counts = Counter()
                exact_row_counts = Counter() if relative == "metric/metric_app.csv" else None
                successes = Counter()
                component_field_counts: dict[str, Counter] = {field: Counter() for field in spec["component_fields"]}
                writers = {}
                destinations = {}
                per_case_rows = Counter()
                per_case_time = {}
                per_case_epoch_interval = {}
                for case in cases:
                    start = datetime.fromisoformat(case["alert_window_start_local"])
                    end = datetime.fromisoformat(case["alert_window_end_local"])
                    analysis_start = start - timedelta(minutes=30)
                    out_path = case_paths[case["incident_id"]] / relative
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    target = out_path.open("w", newline="", encoding="utf-8")
                    writer = csv.DictWriter(target, fieldnames=fields)
                    writer.writeheader()
                    writers[case["incident_id"]] = writer
                    destinations[case["incident_id"]] = target
                    per_case_time[case["incident_id"]] = [None, None]
                    per_case_epoch_interval[case["incident_id"]] = (_ms(analysis_start), _ms(end))
                local_day_start = datetime.strptime(date, "%Y_%m_%d").replace(tzinfo=LOCAL_ZONE)
                local_day_end = local_day_start + timedelta(days=1)
                min_day_ms, max_day_ms = _ms(local_day_start), _ms(local_day_end)
                trace_count = None
                if relative == "trace/trace_span.csv":
                    # Full-day topology is indexed once, independent of clipped spans.
                    trace_count = _trace_integrity(source, scratch_parent)
                stream.seek(0)
                reader = csv.DictReader(stream)
                for row in reader:
                    row_count += 1
                    for column in fields:
                        if not row[column].strip():
                            missing[column] += 1
                    try:
                        timestamp_ms = int(row[spec["time"]])
                        min_ms = timestamp_ms if min_ms is None else min(min_ms, timestamp_ms)
                        max_ms = timestamp_ms if max_ms is None else max(max_ms, timestamp_ms)
                    except (ValueError, TypeError):
                        timestamp_ms = None
                        invalid_time += 1
                    for field, counter in component_field_counts.items():
                        component = _clean_id(row.get(field))
                        if component:
                            counter[component] += 1
                            if field in {"cmdb_id", "serviceName", "dsName"}:
                                observed_component_ids[field][component] += 1
                    if "success" in row:
                        successes[row["success"].strip()] += 1
                    if relative == "metric/metric_app.csv":
                        key_counts[(row.get("serviceName", ""), row.get("startTime", ""))] += 1
                        exact_row_counts[tuple(row[field] for field in fields)] += 1
                    if timestamp_ms is None:
                        continue
                    if not min_day_ms <= timestamp_ms < max_day_ms:
                        wrong_date_timestamp = timestamp_ms
                        continue
                    for case in cases:
                        case_id = case["incident_id"]
                        analysis_start_ms, analysis_end_ms = per_case_epoch_interval[case_id]
                        if analysis_start_ms <= timestamp_ms < analysis_end_ms:
                            writers[case_id].writerow(row)
                            per_case_rows[case_id] += 1
                            bounds = per_case_time[case_id]
                            bounds[0] = timestamp_ms if bounds[0] is None else min(bounds[0], timestamp_ms)
                            bounds[1] = timestamp_ms if bounds[1] is None else max(bounds[1], timestamp_ms)
                for target in destinations.values():
                    target.close()
                if wrong_date_timestamp is not None:
                    raise ValueError(f"{source}: {spec['time']} value {wrong_date_timestamp} does not resolve to directory date {date}; check epoch units")
                duplicate_key_count = (trace_count["duplicate_trace_span_keys"]
                                       if relative == "trace/trace_span.csv" else
                                       sum(count - 1 for count in key_counts.values() if count > 1))
                file_report = {"path": str(source), "sha256": sha256(source), "bytes": source.stat().st_size,
                               "header": list(fields), "rows": row_count,
                               "missing_by_column": {field: missing[field] for field in fields},
                               "timestamp_field": spec["time"], "timestamp_unit": "milliseconds",
                               "invalid_timestamp_rows": invalid_time,
                               "timestamp_min_ms": min_ms, "timestamp_max_ms": max_ms,
                               "timestamp_min_local": datetime.fromtimestamp(min_ms / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat() if min_ms is not None else None,
                               "timestamp_max_local": datetime.fromtimestamp(max_ms / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat() if max_ms is not None else None,
                               "duplicate_key": "(serviceName,startTime)" if relative == "metric/metric_app.csv" else ("(traceId,id)" if relative == "trace/trace_span.csv" else None),
                               "duplicate_key_rows": duplicate_key_count if relative in {"metric/metric_app.csv", "trace/trace_span.csv"} else None,
                               "duplicate_exact_rows_metric_app": (sum(count - 1 for count in exact_row_counts.values() if count > 1) if exact_row_counts is not None else None),
                               "component_values_by_field": {field: dict(sorted(counter.items())) for field, counter in component_field_counts.items()},
                               "success_value_counts_preserving_source_spelling": dict(successes)}
                if trace_count is not None:
                    file_report["trace_parent_integrity"] = trace_count
                file_reports[f"{date}/{relative}"] = file_report
                for case in cases:
                    case_id = case["incident_id"]
                    clip_path = case_paths[case_id] / relative
                    bounds = per_case_time[case_id]
                    clip_reports.append({"incident_id": case_id, "source_file": str(source),
                                         "source_sha256": file_report["sha256"],
                                         "clip_file": str(clip_path), "clip_sha256": sha256(clip_path),
                                         "clip_rows": per_case_rows[case_id],
                                         "clip_time_min_local": datetime.fromtimestamp(bounds[0] / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat() if bounds[0] is not None else None,
                                         "clip_time_max_local": datetime.fromtimestamp(bounds[1] / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat() if bounds[1] is not None else None,
                                         "analysis_start_epoch_ms_utc": _ms(datetime.fromisoformat(case["alert_window_start_local"]) - timedelta(minutes=30)),
                                         "analysis_end_epoch_ms_utc": _ms(datetime.fromisoformat(case["alert_window_end_local"]))})

    mapping_report = {}
    for field, counts in observed_component_ids.items():
        mapping_report[field] = {"distinct_values": len(counts),
                                 "mapped_values": len(set(counts) & set(mapping)),
                                 "unmapped_values": sorted(set(counts) - set(mapping)),
                                 "counts": dict(sorted(counts.items())),
                                 "entities": {name: mapping[name] for name in sorted(set(counts) & set(mapping))}}
    audit = {"timezone": "Asia/Shanghai", "timestamp_units": {"metric": "milliseconds", "trace_startTime": "milliseconds", "record.csv": "seconds"},
             "source_revision": json.loads((Path(__file__).resolve().parents[1] / "config/openrca_telecom_source.json").read_text())["metadata_mirror"]["revision"],
             "deployment_mapping": {"path": str(mapping_path), "sha256": sha256(mapping_path),
                                    "component_catalog_count": len(mapping)},
             "selected_dates": sorted(by_date), "selected_case_count": len(public_cases),
             "file_audits": file_reports, "component_mapping_audit": mapping_report,
             "clip_count": len(clip_reports), "clips": clip_reports,
             "public_data_root": str(output_root),
             "limits": ["metric_app duplicate keys and all source rows are preserved",
                        "trace parent relationships use same-trace (traceId,id) keys referenced by pid",
                        "unresolved parent references are reported, not repaired",
                        "trace elapsedTime units are preserved as source values and not assumed here"]}
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(audit, indent=2) + "\n")
    write_jsonl(output_root / "clip_manifest.jsonl", clip_reports)
    return audit
