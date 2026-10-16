"""Hash-checked public-case loader and explicit Telecom field adapter."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from e4infra.telecom_prep import sha256
from . import ADAPTER_VERSION
from .contracts import EntityRef, MetricPoint, Span, Telemetry, Window


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_ROOT = ROOT / "data/prepared/telecom/public"
PREPARED_ROOT = ROOT / "data/prepared/telecom"
FROZEN_INPUT_MANIFEST = ROOT / "config/telecom_input_manifest.json"

TABLES = {
    "metric/metric_app.csv": ("startTime", ("serviceName", "startTime", "avg_time", "num", "succee_num", "succee_rate")),
    "metric/metric_container.csv": ("timestamp", ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id")),
    "metric/metric_middleware.csv": ("timestamp", ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id")),
    "metric/metric_node.csv": ("timestamp", ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id")),
    "metric/metric_service.csv": ("timestamp", ("itemid", "name", "bomc_id", "timestamp", "value", "cmdb_id")),
    "trace/trace_span.csv": ("startTime", ("callType", "startTime", "elapsedTime", "success", "traceId", "id", "pid", "cmdb_id", "dsName", "serviceName")),
}
APP_VALUE_FIELDS = ("avg_time", "num", "succee_num", "succee_rate")


class AdapterError(ValueError):
    def __init__(self, code: str, message: str, path: str | None = None):
        super().__init__(message)
        self.code, self.path = code, path

    def as_dict(self) -> dict[str, str | None]:
        return {"code": self.code, "message": str(self), "path": self.path}


def _jsonl_rows(path: Path) -> list[dict[str, Any]]:
    try:
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise AdapterError("manifest_read", f"cannot read public manifest: {exc}", str(path)) from exc


def _manifest_rows(split: str, frozen: Mapping[str, Any], root: Path) -> tuple[list[dict[str, Any]], Path]:
    if split not in {"scored", "dev"}:
        raise AdapterError("invalid_split", "split must be scored or dev")
    path = root / "data/prepared/telecom" / f"{split}_incidents.jsonl"
    expected = frozen["prepared_public_manifest_sha256"][split]
    actual = sha256(path)
    if actual != expected:
        raise AdapterError("public_manifest_hash", f"{split} manifest SHA-256 differs from frozen config", str(path))
    return _jsonl_rows(path), path


def load_case(case_id: str, split: str, *, root: Path = ROOT,
              input_manifest_path: Path | None = None) -> Telemetry:
    """Load one frozen public case and catalog; never opens gold or raw date files."""
    frozen = json.loads((root / "config/telecom_sample.json").read_text())
    rows, _ = _manifest_rows(split, frozen, root)
    public_row = next((row for row in rows if row["incident_id"] == case_id), None)
    if public_row is None:
        raise AdapterError("case_not_in_split", f"{case_id} is not in the requested public split")

    frozen_path = input_manifest_path or root / "config/telecom_input_manifest.json"
    input_manifest = json.loads(frozen_path.read_text())
    if input_manifest.get("schema_version") != "e4-input-manifest/1":
        raise AdapterError("input_manifest_version", "unsupported frozen input manifest")
    if sha256(root / "config/telecom_sample.json") != input_manifest["sample_config_sha256"]:
        raise AdapterError("sample_config_hash", "sample configuration differs from frozen input manifest")
    public_clip_manifest = root / "data/prepared/telecom/public/clip_manifest.jsonl"
    if sha256(public_clip_manifest) != input_manifest["clip_manifest_sha256"]:
        raise AdapterError("clip_manifest_hash", "public clip manifest differs from frozen input manifest", str(public_clip_manifest))
    expected_case_manifest_hash = input_manifest[f"{split}_manifest_sha256"]
    if sha256(root / "data/prepared/telecom" / f"{split}_incidents.jsonl") != expected_case_manifest_hash:
        raise AdapterError("input_case_manifest_hash", f"{split} manifest differs from frozen input manifest")
    catalog_path = root / "data/prepared/telecom/public/deployment_catalog.json"
    catalog_file_hash = sha256(catalog_path)
    if catalog_file_hash != input_manifest["deployment_catalog_file_sha256"]:
        raise AdapterError("catalog_hash", "deployment catalog SHA-256 differs from the frozen input manifest", str(catalog_path))
    catalog_doc = json.loads(catalog_path.read_text())
    if catalog_doc.get("source_sha256") != input_manifest["deployment_mapping_source_sha256"]:
        raise AdapterError("catalog_source_hash", "deployment catalog source checksum differs from frozen manifest", str(catalog_path))
    case_path = (root / "data/prepared/telecom/public/cases" / case_id).resolve()
    public_root = (root / "data/prepared/telecom/public").resolve()
    if public_root not in case_path.parents:
        raise AdapterError("case_path_escape", "case path escapes the public input directory")
    alert_path = case_path / "alert.json"
    alert = json.loads(alert_path.read_text())
    for field in ("incident_id", "alert_window_start_local", "alert_window_end_local",
                  "analysis_window_start_epoch_ms_utc", "alert_window_start_epoch_ms_utc",
                  "alert_window_end_epoch_ms_utc", "timezone", "alert_text"):
        if field not in alert:
            raise AdapterError("case_metadata", f"public alert metadata is missing {field}", str(alert_path))
    if (alert["incident_id"] != case_id or alert["timezone"] != "Asia/Shanghai"
            or alert["alert_text"] != public_row["alert_text"]
            or alert["alert_window_start_local"] != public_row["alert_window_start_local"]
            or alert["alert_window_end_local"] != public_row["alert_window_end_local"]):
        raise AdapterError("case_metadata", "public alert identity/timezone does not match manifest", str(alert_path))
    reference = Window("reference", int(alert["analysis_window_start_epoch_ms_utc"]),
                       int(alert["alert_window_start_epoch_ms_utc"]))
    alert_window = Window("alert", int(alert["alert_window_start_epoch_ms_utc"]),
                          int(alert["alert_window_end_epoch_ms_utc"]))
    local_reference_start = datetime.fromisoformat(alert["analysis_window_start_local"])
    local_alert_start = datetime.fromisoformat(alert["alert_window_start_local"])
    local_alert_end = datetime.fromisoformat(alert["alert_window_end_local"])
    for value, timestamp in ((local_reference_start, reference.start_ms_utc),
                              (local_alert_start, alert_window.start_ms_utc),
                              (local_alert_end, alert_window.end_ms_utc)):
        if int(value.timestamp() * 1000) != timestamp:
            raise AdapterError("time_unit_or_timezone", "local time and UTC millisecond boundaries disagree", str(alert_path))
    if reference.end_ms_utc != alert_window.start_ms_utc:
        raise AdapterError("window_boundary", "reference and alert windows are not adjacent")
    if reference.start_ms_utc != alert_window.start_ms_utc - 30 * 60 * 1000:
        raise AdapterError("reference_window", "reference interval must be exactly the frozen preceding 30 minutes")

    manifest_doc = input_manifest["cases"].get(case_id)
    if not manifest_doc:
        raise AdapterError("case_hash_manifest", f"no pinned clip hashes for {case_id}")
    if manifest_doc["alert_window_start_local"] != alert["alert_window_start_local"] or manifest_doc["alert_window_end_local"] != alert["alert_window_end_local"]:
        raise AdapterError("case_window_hash", "case window differs from frozen input manifest")
    clip_manifest_rows = _jsonl_rows(public_clip_manifest)
    clip_records = {"/".join(Path(row["clip_file"]).parts[-2:]): row
                    for row in clip_manifest_rows if row["incident_id"] == case_id}
    if set(clip_records) != set(manifest_doc["files"]):
        raise AdapterError("clip_manifest_case_files", f"clip manifest file list differs for {case_id}")
    metrics: list[MetricPoint] = []
    span_identity: set[tuple[str, str]] = set()
    entity_cache: dict[tuple[str, str], EntityRef] = {}
    def cached_entity(role: str, identifier: str) -> EntityRef | None:
        value = identifier.strip()
        if not value or value.lower() in {"none", "null"}:
            return None
        key = (role, value)
        if key not in entity_cache:
            entity_cache[key] = EntityRef(role, value)
        return entity_cache[key]
    file_hashes = {}
    file_bytes = {}
    duplicate_counts = Counter()
    quality_counts = Counter()
    identity_counts: dict[str, Counter] = {}
    data_parts = []
    for relative, (time_field, required) in TABLES.items():
        input_file = manifest_doc["files"].get(relative)
        if not input_file:
            raise AdapterError("clip_manifest_missing", f"frozen manifest lacks {relative}")
        path = case_path / relative
        expected_hash = input_file["sha256"]
        actual_hash = sha256(path)
        clip_record = clip_records.get(relative)
        if (clip_record is None or clip_record["clip_sha256"] != expected_hash
                or clip_record["source_sha256"] != input_file["source_sha256"]
                or clip_record["clip_rows"] != input_file["rows"]):
            raise AdapterError("clip_manifest_record", f"clip manifest record differs for {relative}", str(path))
        if actual_hash != expected_hash:
            raise AdapterError("clip_hash", f"clip hash mismatch for {relative}", str(path))
        file_hashes[relative] = actual_hash
        file_bytes[relative] = path.stat().st_size
        data_parts.append((relative, actual_hash))
        app_keys, series_keys, span_keys = Counter(), Counter(), Counter()
        row_count = 0
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            fields = tuple(reader.fieldnames or ())
            if tuple(required) != fields:
                raise AdapterError("column_schema", f"unexpected header for {relative}: {fields}", str(path))
            for line_number, row in enumerate(reader, start=2):
                row_count += 1
                try:
                    timestamp_ms = int(row[time_field])
                except (TypeError, ValueError) as exc:
                    raise AdapterError("timestamp_parse", f"invalid {time_field} at row {line_number}", str(path)) from exc
                if not (reference.start_ms_utc <= timestamp_ms < alert_window.end_ms_utc):
                    raise AdapterError("clip_window", f"row {line_number} is outside half-open case windows", str(path))
                if relative == "metric/metric_app.csv":
                    service = row["serviceName"].strip()
                    if not service:
                        quality_counts["metric_app_missing_serviceName"] += 1
                        continue
                    app_keys[(service, timestamp_ms)] += 1
                    for metric_name in APP_VALUE_FIELDS:
                        value, status = _number(row[metric_name])
                        quality_counts[f"metric_app_{metric_name}_{status}"] += 1
                        metrics.append(MetricPoint(
                            table=relative, timestamp_ms_utc=timestamp_ms,
                            entity=cached_entity("service", service), metric_name=sys.intern(metric_name),
                            bomc_id="metric_app", item_id=metric_name, value=value,
                            raw_value=row[metric_name], row_key=str(line_number)))
                elif relative == "trace/trace_span.csv":
                    key = (row["traceId"].strip(), row["id"].strip())
                    span_keys[key] += 1
                    if key[0] and key[1]:
                        span_identity.add(key)
                else:
                    component = row["cmdb_id"].strip()
                    if not component:
                        quality_counts[f"{relative}_missing_cmdb_id"] += 1
                        continue
                    series_keys[(component, row["name"], row["bomc_id"], row["itemid"], timestamp_ms)] += 1
                    value, status = _number(row["value"])
                    quality_counts[f"{relative}_value_{status}"] += 1
                    metrics.append(MetricPoint(
                        table=relative, timestamp_ms_utc=timestamp_ms,
                        entity=cached_entity("component", component), metric_name=sys.intern(row["name"]),
                        bomc_id=sys.intern(row["bomc_id"]) if row["bomc_id"] else None,
                        item_id=sys.intern(row["itemid"]) if row["itemid"] else None,
                        value=value, raw_value=row["value"],
                        row_key=str(line_number)))
        if row_count != input_file["rows"] or row_count != clip_record["clip_rows"]:
            raise AdapterError("clip_row_count", f"clip row count differs for {relative}", str(path))
        if relative == "metric/metric_app.csv":
            duplicate_counts[relative] = sum(v - 1 for v in app_keys.values() if v > 1)
            quality_counts["metric_app_distinct_time_buckets"] += len(app_keys)
            quality_counts["metric_app_raw_rows"] += sum(app_keys.values())
        elif relative == "trace/trace_span.csv":
            duplicate_counts[relative] = sum(v - 1 for v in span_keys.values() if v > 1)
        elif series_keys:
            duplicate_counts[relative] = sum(v - 1 for v in series_keys.values() if v > 1)

    spans: list[Span] = []
    trace_path = case_path / "trace/trace_span.csv"
    with trace_path.open("r", newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            trace_id, span_id = row["traceId"].strip(), row["id"].strip()
            raw_success = sys.intern(row["success"].strip())
            success, status = _parse_success(raw_success)
            parent_id = _clean_identifier(row["pid"])
            resolved = bool(parent_id and (trace_id, parent_id) in span_identity)
            if parent_id and not resolved:
                quality_counts["trace_parent_unresolved_within_clip"] += 1
            if status != "valid":
                quality_counts[f"trace_success_{status}"] += 1
            spans.append(Span(
                trace_id=trace_id, span_id=span_id, parent_id=parent_id,
                start_ms_utc=int(row["startTime"]), elapsed_raw=row["elapsedTime"],
                success=success, success_status=status, success_raw=raw_success,
                call_type=sys.intern(row["callType"]),
                executing_component=cached_entity("component", row["cmdb_id"]),
                logical_service=cached_entity("service", row["serviceName"]),
                database_target=cached_entity("database", row["dsName"]),
                parent_resolved_in_clip=resolved,
                parent_unresolved_in_clip=bool(parent_id and not resolved)))
    file_hashes["deployment_catalog.json"] = catalog_file_hash
    file_bytes["deployment_catalog.json"] = catalog_path.stat().st_size
    data_parts.append(("deployment_catalog.json", catalog_file_hash))
    digest_doc = {"case_id": case_id, "files": sorted(data_parts),
                  "reference": [reference.start_ms_utc, reference.end_ms_utc],
                  "alert": [alert_window.start_ms_utc, alert_window.end_ms_utc],
                  "adapter_version": ADAPTER_VERSION}
    data_hash = hashlib.sha256(json.dumps(digest_doc, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    catalog = MappingProxyType({key: MappingProxyType(value) for key, value in catalog_doc["entities"].items()})
    return Telemetry(case_id, reference, alert_window, tuple(metrics), tuple(spans), catalog,
                     data_hash, MappingProxyType(file_hashes), MappingProxyType(dict(duplicate_counts)),
                     MappingProxyType(dict(quality_counts)), ADAPTER_VERSION,
                     MappingProxyType(file_bytes))


def _clean_identifier(identifier: str) -> str | None:
    value = identifier.strip()
    return None if value.casefold() in {"", "none", "null"} else value


def _number(raw: str) -> tuple[float | None, str]:
    text = raw.strip()
    if not text:
        return None, "missing"
    try:
        value = float(text)
    except ValueError:
        return None, "invalid"
    if not math.isfinite(value):
        return None, "nonfinite"
    return value, "valid"


def _parse_success(raw: str) -> tuple[bool | None, str]:
    value = raw.strip().casefold()
    if value == "true":
        return True, "valid"
    if value == "false":
        return False, "valid"
    if not value:
        return None, "missing"
    return None, "invalid"
