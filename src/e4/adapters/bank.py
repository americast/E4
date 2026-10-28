"""Hash-checked Bank telemetry adapter; never reads evaluator or source-date trees."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from e4 import ADAPTER_VERSION
from e4.contracts import EntityRef, MetricPoint, Span, Telemetry, Window
from e4.paths import ROOT


INPUT_MANIFEST = ROOT / "config/bank/input_manifest.json"
SAMPLE_CONFIG = ROOT / "config/bank/sample.json"
METRIC_POLICY = ROOT / "config/bank/metric_kind_policy.json"
KPI_POLICY = ROOT / "config/bank/kpi_policy.json"
BANK_SOURCE_REVISION = "c121a483a5a91dbae50708e5942c42a6609098b2"
BANK_PROMPT_PATH = "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py"

TABLES: Mapping[str, tuple[tuple[str, ...], str, str]] = {
    "metric/metric_app.csv": (("timestamp", "rr", "sr", "cnt", "mrt", "tc"), "timestamp"),
    "metric/metric_container.csv": (("timestamp", "cmdb_id", "kpi_name", "value"), "timestamp"),
    "trace/trace_span.csv": (("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"), "timestamp"),
    "log/log_service.csv": (("log_id", "timestamp", "cmdb_id", "log_name", "value"), "timestamp"),
}
UPSTREAM_DECLARED_UNITS = {"metric/metric_app.csv": "seconds",
                           "metric/metric_container.csv": "seconds",
                           "trace/trace_span.csv": "milliseconds",
                           "log/log_service.csv": "seconds"}
UPSTREAM_UNIT_LABELS = {
    relative: {unit, f"epoch {unit}"} for relative, unit in UPSTREAM_DECLARED_UNITS.items()
}
SPLIT_PATHS = {
    "scored": "scored_incidents.jsonl",
    "dev": "dev_incidents.jsonl",
}
FORBIDDEN_LABEL_KEYS = {"scoring_points", "ground_truth", "root_cause", "fault_component",
                        "task_index", "onset", "cause", "reason", "component"}
CASE_ID = re.compile(r"^bank-[0-9]{8}T[0-9]{4}-utc8$")
SOURCE_DATE = re.compile(r"^[0-9]{4}_[0-9]{2}_[0-9]{2}$")


class BankAdapterError(ValueError):
    def __init__(self, code: str, message: str, path: str | None = None):
        super().__init__(message)
        self.code, self.path = code, path

    def as_dict(self) -> dict[str, str | None]:
        return {"code": self.code, "message": str(self), "path": self.path}


@dataclass(frozen=True)
class BankCase:
    telemetry: Telemetry
    alert: Mapping[str, Any]
    public_manifest_row: Mapping[str, Any]
    deployment_catalog: Mapping[str, Any]
    log_summary: Mapping[str, Any]
    provenance: Mapping[str, Any]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path, code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BankAdapterError(code, f"cannot read JSON metadata: {exc}", str(path)) from exc
    if type(value) is not dict:
        raise BankAdapterError(code, "metadata must be a JSON object", str(path))
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise BankAdapterError("manifest_read", f"cannot read public JSONL manifest: {exc}",
                               str(path)) from exc
    if any(type(row) is not dict for row in rows):
        raise BankAdapterError("manifest_row", "public manifest rows must be JSON objects", str(path))
    for row in rows:
        if FORBIDDEN_LABEL_KEYS.intersection(key.casefold() for key in row):
            raise BankAdapterError("public_manifest_label_field",
                                   "public input manifest contains a forbidden label field", str(path))
    return rows


def _reject_label_keys(value: Mapping[str, Any], path: Path) -> None:
    if FORBIDDEN_LABEL_KEYS.intersection(key.casefold() for key in value):
        raise BankAdapterError("public_metadata_label_field",
                               "public metadata contains a forbidden label field", str(path))


def _decimal(raw: Any) -> Decimal | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value or value.casefold() in {"none", "null", "nan", "na", "n/a"}:
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _numeric(raw: Any) -> tuple[float | None, str]:
    number = _decimal(raw)
    if number is None:
        text = "" if raw is None else str(raw).strip()
        return None, "missing" if not text or text.casefold() in {"none", "null", "nan", "na", "n/a"} else "invalid"
    try:
        value = float(number)
    except (OverflowError, ValueError):
        return None, "invalid"
    return (value, "valid") if math.isfinite(value) else (None, "invalid")


def _timestamp_ms(raw: Any, unit: str, path: Path, row_number: int) -> int:
    number = _decimal(raw)
    if number is None:
        raise BankAdapterError("timestamp_parse", f"invalid timestamp at row {row_number}", str(path))
    millis = number * 1000 if unit == "seconds" else number if unit == "milliseconds" else None
    if millis is None or millis != millis.to_integral_value():
        raise BankAdapterError("timestamp_unit_or_precision",
                               f"timestamp is not exact integer milliseconds under {unit}", str(path))
    return int(millis)


def _missing_identifier(raw: Any) -> bool:
    if raw is None:
        return True
    return str(raw).strip().casefold() in {"", "none", "null"}


def _identifier(raw: Any) -> str | None:
    return None if _missing_identifier(raw) else str(raw).strip()


def _date_and_table(relative: str) -> tuple[str, str]:
    parts = Path(relative).parts
    if (len(parts) != 4 or parts[0] != "source_dates" or not SOURCE_DATE.fullmatch(parts[1]) or
            "/".join(parts[2:]) not in TABLES):
        raise BankAdapterError("input_manifest_file_path",
                               f"Bank table path must be source_dates/<date>/<native table>: {relative}")
    return parts[1], "/".join(parts[2:])


def _local_epoch_ms(value: Any, name: str, path: Path) -> int:
    if not isinstance(value, str):
        raise BankAdapterError("alert_local_time", f"{name} must be ISO-8601 text", str(path))
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise BankAdapterError("alert_local_time", f"invalid {name}: {exc}", str(path)) from exc
    if dt.tzinfo is None:
        raise BankAdapterError("alert_timezone", f"{name} must include a UTC+8 offset", str(path))
    offset = dt.utcoffset()
    if offset is None or offset.total_seconds() != 8 * 3600:
        raise BankAdapterError("alert_timezone", f"{name} must use UTC+8", str(path))
    return int(dt.timestamp() * 1000)


def _window_map(alert: Mapping[str, Any], path: Path) -> tuple[Window, Window]:
    required = ("incident_id", "timezone", "alert_text", "analysis_window_start_local",
                "alert_window_start_local", "alert_window_end_local",
                "analysis_window_start_epoch_ms_utc", "alert_window_start_epoch_ms_utc",
                "alert_window_end_epoch_ms_utc", "timestamp_unit_in_source_files",
                "upstream_timestamp_unit_provenance", "source_date_timestamp_units",
                "telemetry_dates")
    missing = [key for key in required if key not in alert]
    if missing:
        raise BankAdapterError("alert_schema", f"alert metadata missing {missing}", str(path))
    _reject_label_keys(alert, path)
    if alert["timezone"] != "Asia/Shanghai" or not isinstance(alert["alert_text"], str):
        raise BankAdapterError("alert_schema", "Bank alert must use Asia/Shanghai and public text", str(path))
    ref_start = alert["analysis_window_start_epoch_ms_utc"]
    ref_end = alert["alert_window_start_epoch_ms_utc"]
    alert_end = alert["alert_window_end_epoch_ms_utc"]
    if any(type(value) is not int for value in (ref_start, ref_end, alert_end)):
        raise BankAdapterError("alert_window", "window boundaries must be integer UTC milliseconds", str(path))
    reference = Window("reference", ref_start, ref_end)
    alert_window = Window("alert", ref_end, alert_end)
    if ref_end - ref_start != 1_800_000 or alert_end - ref_end != 1_800_000:
        raise BankAdapterError("alert_window", "both Bank windows must be adjacent half-open 30-minute intervals", str(path))
    for key, timestamp in (("analysis_window_start_local", ref_start),
                           ("alert_window_start_local", ref_end),
                           ("alert_window_end_local", alert_end)):
        if _local_epoch_ms(alert[key], key, path) != timestamp:
            raise BankAdapterError("alert_time_consistency", f"{key} disagrees with UTC milliseconds", str(path))
    declared_units = alert["timestamp_unit_in_source_files"]
    if (not isinstance(declared_units, Mapping) or set(declared_units) != set(TABLES) or
            any(value not in UPSTREAM_UNIT_LABELS[relative]
                for relative, value in declared_units.items())):
        raise BankAdapterError("timestamp_unit_manifest",
                               "Bank upstream-declared timestamp units differ from pinned source metadata", str(path))
    dates = alert["telemetry_dates"]
    date_units = alert["source_date_timestamp_units"]
    if (not isinstance(dates, list) or not dates or any(not isinstance(date, str) for date in dates) or
            not isinstance(date_units, Mapping) or set(date_units) != set(dates)):
        raise BankAdapterError("source_date_unit_map", "Bank source-date unit map does not cover the declared dates", str(path))
    for date, units in date_units.items():
        if not isinstance(units, Mapping) or set(units) != set(TABLES) or any(
                unit not in {"seconds", "milliseconds"} for unit in units.values()):
            raise BankAdapterError("source_date_unit_map", f"invalid empirical timestamp units for {date}", str(path))
    return reference, alert_window


def _hash_row(row: Mapping[str, Any], relative: str, clip_rows: Mapping[str, Mapping[str, Any]],
              case_path: Path) -> tuple[Path, dict[str, Any]]:
    file_doc = row["files"].get(relative)
    clip_doc = clip_rows.get(relative)
    if not isinstance(file_doc, dict) or not isinstance(clip_doc, Mapping):
        raise BankAdapterError("clip_manifest_missing", f"pinned file metadata missing for {relative}")
    path = (case_path / relative).resolve()
    if case_path.resolve() not in path.parents:
        raise BankAdapterError("case_path_escape", f"table path escapes public case: {relative}")
    for name in ("sha256", "source_sha256", "rows", "size_bytes", "header", "timestamp_unit"):
        if name not in file_doc:
            raise BankAdapterError("input_manifest_schema", f"per-file metadata lacks {name}: {relative}")
    comparisons = (("sha256", "clip_sha256"), ("source_sha256", "source_sha256"),
                   ("source_git_blob_sha1", "source_git_blob_sha1"),
                   ("source_lfs_sha256", "source_lfs_sha256"),
                   ("rows", "clip_rows"), ("size_bytes", "clip_size_bytes"),
                   ("header", "header"), ("timestamp_unit", "timestamp_source_unit"))
    for field_name, clip_name in comparisons:
        if file_doc[field_name] != clip_doc.get(clip_name):
            raise BankAdapterError("clip_manifest_record",
                                   f"clip manifest {clip_name} differs for {relative}", str(path))
    if not path.is_file():
        raise BankAdapterError("clip_missing", f"prepared public clip missing: {relative}", str(path))
    size = path.stat().st_size
    if type(file_doc["size_bytes"]) is not int or size != file_doc["size_bytes"]:
        raise BankAdapterError("clip_size", f"clip size mismatch for {relative}", str(path))
    actual = sha256_file(path)
    if actual != file_doc["sha256"]:
        raise BankAdapterError("clip_hash", f"clip SHA-256 mismatch for {relative}", str(path))
    return path, file_doc


def _iter_csv(path: Path, expected_header: tuple[str, ...], max_field_chars: int):
    old_limit = csv.field_size_limit()
    csv.field_size_limit(max_field_chars)
    stream = path.open("r", newline="", encoding="utf-8-sig")
    try:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != expected_header:
            raise BankAdapterError("column_schema",
                                   f"unexpected header: {tuple(reader.fieldnames or ())}", str(path))
        for line_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise BankAdapterError("csv_row_width",
                    f"row {line_number} has a different number of fields than the pinned header", str(path))
            yield line_number, row
    except csv.Error as exc:
        raise BankAdapterError("csv_parse", f"invalid or oversized CSV field: {exc}", str(path)) from exc
    finally:
        stream.close()
        csv.field_size_limit(old_limit)


def _catalog_doc(root: Path, input_manifest: Mapping[str, Any]) -> tuple[dict[str, Any], Path, str]:
    path = root / "data/prepared/bank/public/deployment_catalog.json"
    expected_hash = input_manifest.get("deployment_catalog_file_sha256")
    if not isinstance(expected_hash, str) or sha256_file(path) != expected_hash:
        raise BankAdapterError("catalog_hash", "Bank public catalog differs from frozen input manifest", str(path))
    catalog = _read_json(path, "catalog_read")
    if catalog.get("schema_version") != "bank-source-catalog/1":
        raise BankAdapterError("catalog_version", "unsupported Bank catalog schema", str(path))
    source = catalog.get("source")
    expected_schema = input_manifest.get("deployment_catalog_schema_sha256")
    if (not isinstance(source, Mapping) or
            source.get("official_revision") != BANK_SOURCE_REVISION or
            source.get("official_repository") != "https://github.com/microsoft/OpenRCA" or
            source.get("prompt_path") != BANK_PROMPT_PATH):
        raise BankAdapterError("catalog_source", "Bank catalog provenance is not the pinned source", str(path))
    prompt_sha = source.get("prompt_sha256")
    prompt_path = root / BANK_PROMPT_PATH
    if (not isinstance(prompt_sha, str) or prompt_sha != expected_schema or
            not prompt_path.is_file() or sha256_file(prompt_path) != prompt_sha):
        raise BankAdapterError("catalog_schema_hash", "pinned Bank prompt/schema hash mismatch", str(prompt_path))
    candidates = catalog.get("official_candidate_components")
    entities = catalog.get("entities")
    topology = catalog.get("topology")
    if (not isinstance(candidates, list) or any(not isinstance(item, str) for item in candidates) or
            not isinstance(entities, Mapping) or not isinstance(topology, Mapping) or
            topology.get("relationships") != []):
        raise BankAdapterError("catalog_schema", "Bank catalog must preserve IDs without inferred relationships", str(path))
    return catalog, path, expected_hash


def load_bank_case(case_id: str, split: str = "dev", *, root: Path = ROOT,
                   input_manifest_path: Path | None = None,
                   metric_policy_path: Path | None = None,
                   kpi_policy_path: Path | None = None) -> BankCase:
    """Load one public Bank case with hash-pinned, source-explicit field mappings."""
    if split not in SPLIT_PATHS:
        raise BankAdapterError("invalid_split", "Bank adapter split must be scored or dev")
    if not CASE_ID.fullmatch(case_id):
        raise BankAdapterError("invalid_case_id", "Bank case ID is not a frozen opaque identifier")
    public_root = (root / "data/prepared/bank/public").resolve()
    case_path = (public_root / "cases" / case_id).resolve()
    if public_root not in case_path.parents:
        raise BankAdapterError("case_path_escape", "Bank case path escapes prepared public input")
    manifest_path = input_manifest_path or root / "config/bank/input_manifest.json"
    sample_path = root / "config/bank/sample.json"
    manifest = _read_json(manifest_path, "input_manifest_read")
    if manifest.get("schema_version") != "e4-bank-input-manifest/1":
        raise BankAdapterError("input_manifest_version", "unsupported Bank input manifest", str(manifest_path))
    if not sample_path.is_file() or sha256_file(sample_path) != manifest.get("sample_config_sha256"):
        raise BankAdapterError("sample_config_hash", "Bank selection config differs from frozen input manifest", str(sample_path))

    split_hash_key = f"{split}_manifest_sha256"
    split_path = root / "data/prepared/bank" / SPLIT_PATHS[split]
    if not split_path.is_file() or sha256_file(split_path) != manifest.get(split_hash_key):
        raise BankAdapterError("public_manifest_hash", f"{split} Bank manifest hash mismatch", str(split_path))
    public_rows = _read_jsonl(split_path)
    public_row = next((row for row in public_rows if row.get("incident_id") == case_id), None)
    if public_row is None:
        raise BankAdapterError("case_not_in_split", f"{case_id} is not in the requested public split")
    if public_row.get("timezone") != "Asia/Shanghai":
        raise BankAdapterError("public_manifest_timezone", "Bank public manifest must declare Asia/Shanghai", str(split_path))

    catalog, catalog_path, catalog_hash = _catalog_doc(root, manifest)
    clip_path = public_root / "clip_manifest.jsonl"
    if not clip_path.is_file() or sha256_file(clip_path) != manifest.get("clip_manifest_sha256"):
        raise BankAdapterError("clip_manifest_hash", "Bank clip manifest differs from frozen config", str(clip_path))
    clip_rows_all = _read_jsonl(clip_path)
    clip_rows = {row.get("relative_path"): row for row in clip_rows_all if row.get("case_id") == case_id}
    case_manifest = manifest.get("cases", {}).get(case_id)
    if not isinstance(case_manifest, Mapping):
        raise BankAdapterError("case_hash_manifest", f"no frozen file hashes for {case_id}")
    alert_path = case_path / "alert.json"
    if not alert_path.is_file():
        raise BankAdapterError("alert_missing", "prepared Bank alert metadata is missing", str(alert_path))
    alert_hash = sha256_file(alert_path)
    if alert_hash != case_manifest.get("alert_sha256"):
        raise BankAdapterError("alert_hash", "Bank public alert metadata SHA-256 mismatch", str(alert_path))
    alert = _read_json(alert_path, "alert_read")
    for key in ("incident_id", "alert_text", "timezone", "alert_window_start_local",
                "alert_window_end_local", "analysis_window_start_local",
                "analysis_window_start_epoch_ms_utc", "alert_window_start_epoch_ms_utc",
                "alert_window_end_epoch_ms_utc", "timestamp_unit_in_source_files",
                "upstream_timestamp_unit_provenance", "source_date_timestamp_units",
                "telemetry_date", "telemetry_dates"):
        if key not in alert or (key in public_row and alert[key] != public_row[key]):
            raise BankAdapterError("alert_manifest_mismatch", f"Bank alert {key} differs from public manifest", str(alert_path))
    reference, alert_window = _window_map(alert, alert_path)
    dates = alert["telemetry_dates"]
    expected_paths = {f"source_dates/{date}/{table}" for date in dates for table in TABLES}
    if set(clip_rows) != expected_paths or set(case_manifest.get("files", {})) != expected_paths:
        raise BankAdapterError("clip_manifest_case_files",
                               "Bank case must include the four native tables for each declared source date", str(clip_path))

    expected_windows = case_manifest.get("windows")
    expected_window_values = {
        "reference": {"start_epoch_ms_utc": reference.start_ms_utc,
                      "end_epoch_ms_utc": reference.end_ms_utc},
        "alert": {"start_epoch_ms_utc": alert_window.start_ms_utc,
                  "end_epoch_ms_utc": alert_window.end_ms_utc},
    }
    if expected_windows != expected_window_values:
        raise BankAdapterError("window_manifest_mismatch", "Bank windows differ from frozen manifest", str(manifest_path))

    metric_policy_path = metric_policy_path or root / "config/bank/metric_kind_policy.json"
    kpi_policy_path = kpi_policy_path or root / "config/bank/kpi_policy.json"
    metric_policy = _read_json(metric_policy_path, "metric_policy_read")
    kpi_policy = _read_json(kpi_policy_path, "kpi_policy_read")
    if metric_policy.get("schema_version") != "e4-bank-metric-kind-policy/1" or \
            kpi_policy.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise BankAdapterError("bank_policy_version", "unsupported Bank metric/KPI policy")
    limits = metric_policy.get("loader_limits", {})
    max_file_bytes = int(limits.get("max_file_bytes", 0))
    max_rows = int(limits.get("max_rows_per_file", 0))
    max_field_chars = int(limits.get("max_csv_field_chars", 0))
    max_log_groups = int(limits.get("max_log_groups", 0))
    max_log_summary_groups = int(limits.get("max_log_summary_groups", 0))
    if min(max_file_bytes, max_rows, max_field_chars, max_log_groups, max_log_summary_groups) <= 0:
        raise BankAdapterError("bank_loader_limits", "Bank loader bounds must be positive")

    file_hashes: dict[str, str] = {"alert.json": alert_hash,
        "public/deployment_catalog.json": catalog_hash,
        "config/bank/sample.json": sha256_file(sample_path),
        "config/bank/metric_kind_policy.json": sha256_file(metric_policy_path),
        "config/bank/kpi_policy.json": sha256_file(kpi_policy_path)}
    file_bytes: dict[str, int] = {"alert.json": alert_path.stat().st_size,
        "public/deployment_catalog.json": catalog_path.stat().st_size}
    quality = Counter()
    duplicates = Counter()
    metrics: list[MetricPoint] = []
    span_rows: list[tuple[str, str, str | None, str | None, int, str, str]] = []
    observed_ids: dict[str, set[str]] = {"metric_container": set(), "trace_span": set(), "log_service": set()}
    recognized_rows = Counter()
    unknown_rows = Counter()
    official_candidates = set(catalog["official_candidate_components"])
    log_groups: dict[tuple[str, str, str, int], int] = defaultdict(int)
    log_group_limit_exceeded = False
    log_group_overflow_observations = 0
    log_id_counts = Counter()
    table_counts: dict[str, int] = {table: 0 for table in TABLES}

    for relative in sorted(case_manifest["files"]):
        source_date, table_path = _date_and_table(relative)
        header, timestamp_field = TABLES[table_path]
        path, file_doc = _hash_row(case_manifest, relative, clip_rows, case_path)
        expected_unit = file_doc["timestamp_unit"]
        if (expected_unit not in {"seconds", "milliseconds"} or
                alert["source_date_timestamp_units"][source_date].get(table_path) != expected_unit or
                tuple(file_doc["header"]) != header):
            raise BankAdapterError("input_manifest_schema", f"Bank source schema mismatch for {relative}", str(path))
        if path.stat().st_size > max_file_bytes:
            raise BankAdapterError("input_limit", f"Bank file exceeds configured byte limit: {relative}", str(path))
        if file_doc["rows"] > max_rows:
            raise BankAdapterError("row_limit", f"Bank file exceeds configured complete-row limit: {relative}", str(path))
        file_hashes[relative] = file_doc["sha256"]
        file_bytes[relative] = file_doc["size_bytes"]
        count = 0
        app_seen, series_seen, trace_seen, file_log_ids = Counter(), Counter(), Counter(), Counter()
        for line_number, row in _iter_csv(path, header, max_field_chars):
            count += 1
            if count > max_rows:
                raise BankAdapterError("row_limit", f"Bank file exceeds configured complete-row limit: {relative}", str(path))
            timestamp_ms = _timestamp_ms(row.get(timestamp_field), expected_unit, path, line_number)
            if not reference.contains(timestamp_ms) and not alert_window.contains(timestamp_ms):
                raise BankAdapterError("clip_window", f"row {line_number} is outside the two Bank windows", str(path))
            window_name = "reference" if reference.contains(timestamp_ms) else "alert"
            if table_path == "metric/metric_app.csv":
                tc = _identifier(row.get("tc"))
                if tc is None:
                    quality["metric_app_missing_tc_rows"] += 1
                    continue
                app_seen[(tc, timestamp_ms)] += 1
                for metric_name in ("rr", "sr", "cnt", "mrt"):
                    value, status = _numeric(row.get(metric_name))
                    quality[f"metric_app_{metric_name}_{status}"] += 1
                    metrics.append(MetricPoint(
                        table="bank/metric_app.csv", timestamp_ms_utc=timestamp_ms,
                        entity=EntityRef("app_kpi", tc), metric_name=metric_name,
                        bomc_id="bank.metric_app", item_id=metric_name,
                        value=value, raw_value="" if row.get(metric_name) is None else str(row[metric_name]),
                        row_key=f"{relative}:{line_number}:{metric_name}"))
            elif table_path == "metric/metric_container.csv":
                component = _identifier(row.get("cmdb_id"))
                metric_name = _identifier(row.get("kpi_name"))
                if component is None or metric_name is None:
                    quality["metric_container_missing_identity_rows"] += 1
                    continue
                observed_ids["metric_container"].add(component)
                if component in official_candidates:
                    recognized_rows["metric_container"] += 1
                else:
                    unknown_rows["metric_container"] += 1
                series_seen[(component, metric_name, timestamp_ms)] += 1
                value, status = _numeric(row.get("value"))
                quality[f"metric_container_value_{status}"] += 1
                metrics.append(MetricPoint(
                    table="bank/metric_container.csv", timestamp_ms_utc=timestamp_ms,
                    entity=EntityRef("component", component), metric_name=metric_name,
                    bomc_id="bank.metric_container", item_id="value", value=value,
                    raw_value="" if row.get("value") is None else str(row["value"]),
                    row_key=f"{relative}:{line_number}"))
            elif table_path == "trace/trace_span.csv":
                component = _identifier(row.get("cmdb_id"))
                trace_id = _identifier(row.get("trace_id")) or ""
                span_id = _identifier(row.get("span_id")) or ""
                parent_id = _identifier(row.get("parent_id"))
                duration_value, duration_status = _numeric(row.get("duration"))
                if duration_status != "valid":
                    quality[f"trace_duration_{duration_status}"] += 1
                elif duration_value is not None and duration_value < 0:
                    quality["trace_duration_negative_sentinel_rows"] += 1
                else:
                    quality["trace_duration_finite_nonnegative_rows"] += 1
                if not trace_id:
                    quality["trace_missing_trace_id_rows"] += 1
                if not span_id:
                    quality["trace_missing_span_id_rows"] += 1
                if component is not None:
                    observed_ids["trace_span"].add(component)
                    if component in official_candidates:
                        recognized_rows["trace_span"] += 1
                    else:
                        unknown_rows["trace_span"] += 1
                trace_seen[(trace_id, span_id)] += 1
                span_rows.append((trace_id, span_id, parent_id, component,
                                  timestamp_ms, str(row.get("duration") or ""),
                                  f"{relative}:{line_number}"))
            elif table_path == "log/log_service.csv":
                log_id = _identifier(row.get("log_id"))
                component = _identifier(row.get("cmdb_id"))
                log_name = _identifier(row.get("log_name"))
                if log_id is not None:
                    log_id_counts[log_id] += 1
                    file_log_ids[log_id] += 1
                if component is None or log_name is None:
                    quality["log_service_missing_identity_rows"] += 1
                    continue
                observed_ids["log_service"].add(component)
                if component in official_candidates:
                    recognized_rows["log_service"] += 1
                else:
                    unknown_rows["log_service"] += 1
                minute_index = (timestamp_ms - reference.start_ms_utc) // 60_000
                bucket_ts = reference.start_ms_utc + minute_index * 60_000
                bucket_window = "reference" if reference.contains(bucket_ts) else "alert"
                group_key = (bucket_window, component, log_name, bucket_ts)
                if group_key in log_groups:
                    log_groups[group_key] += 1
                elif len(log_groups) < max_log_groups:
                    log_groups[group_key] = 1
                else:
                    # Continue reading and hashing the whole raw clip, but fail
                    # closed after its full row count is verified.
                    log_group_limit_exceeded = True
                    log_group_overflow_observations += 1
        table_counts[table_path] += count
        if count != file_doc["rows"]:
            raise BankAdapterError("clip_row_count", f"row count mismatch for {relative}: {count}!={file_doc['rows']}", str(path))
        if sha256_file(path) != file_doc["sha256"]:
            raise BankAdapterError("clip_changed_during_read", f"Bank clip changed while reading {relative}", str(path))
        duplicates[relative] = (sum(value - 1 for value in app_seen.values())
                                if table_path == "metric/metric_app.csv" else
                                sum(value - 1 for value in series_seen.values())
                                if table_path == "metric/metric_container.csv" else
                                sum(value - 1 for value in trace_seen.values())
                                if table_path == "trace/trace_span.csv" else
                                sum(value - 1 for value in file_log_ids.values()))

    if log_group_limit_exceeded:
        raise BankAdapterError("log_group_limit",
            f"All Bank log source-date clips were fully read and hash/row-count verified, but their count projection exceeded {max_log_groups} distinct groups ({log_group_overflow_observations} overflow observations); no partial Telemetry was returned")

    # Resolve only the source-declared (trace_id, span_id) parent identity.
    # The textual Bank catalog supplies no graph edges or vertical aliases.
    span_counts = Counter((trace_id, span_id) for trace_id, span_id, *_ in span_rows
                          if trace_id and span_id)
    spans: list[Span] = []
    for trace_id, span_id, parent_id, component, timestamp_ms, duration_raw, _line in span_rows:
        verified_identity = bool(trace_id and span_id)
        matches = span_counts.get((trace_id, parent_id), 0) if verified_identity and parent_id is not None else 0
        resolved = bool(verified_identity and parent_id and matches == 1)
        unresolved = bool(verified_identity and parent_id and matches != 1)
        if parent_id and not verified_identity:
            quality["trace_parent_resolution_missing_identity"] += 1
        if verified_identity and matches > 1:
            quality["trace_parent_ambiguous_within_trace"] += 1
        elif verified_identity and parent_id and not resolved:
            quality["trace_parent_unresolved_in_clip"] += 1
        spans.append(Span(
            trace_id=trace_id, span_id=span_id, parent_id=parent_id,
            start_ms_utc=timestamp_ms, elapsed_raw=duration_raw,
            success=None, success_status="missing", success_raw="",
            call_type="unprovided",
            executing_component=EntityRef("component", component) if component is not None else None,
            logical_service=None, database_target=None,
            parent_resolved_in_clip=resolved, parent_unresolved_in_clip=unresolved))

    by_component_name: dict[tuple[str, str], int] = defaultdict(int)
    for (_window_name, component, log_name, _old_timestamp_ms), count in log_groups.items():
        by_component_name[(component, log_name)] += count
    projected_log_points = len(by_component_name) * 60
    if projected_log_points > max_log_groups:
        raise BankAdapterError("log_group_limit",
            f"Bank log file was fully read and hash/row-count verified, but the zero-filled per-minute projection needs {projected_log_points} points (limit {max_log_groups}); no partial Telemetry was returned")
    zero_filled = 0
    for component, log_name in sorted(by_component_name):
        for minute_index in range(60):
            timestamp_ms = reference.start_ms_utc + minute_index * 60_000
            window_name = "reference" if minute_index < 30 else "alert"
            count = log_groups.get((window_name, component, log_name, timestamp_ms), 0)
            metrics.append(MetricPoint(
                table="bank/log/log_service.csv", timestamp_ms_utc=timestamp_ms,
                entity=EntityRef("component", component), metric_name="log_volume_count",
                bomc_id="bank.log_service", item_id=log_name, value=float(count),
                raw_value=str(count), row_key=f"{window_name}:{timestamp_ms}:{component}:{log_name}"))
            quality[f"log_service_{window_name}_rows"] += count
            if count == 0:
                zero_filled += 1
    quality["log_service_zero_filled_buckets"] = zero_filled
    quality["log_service_projected_minute_points"] = projected_log_points
    sorted_summary = sorted(by_component_name.items(), key=lambda item: (-item[1], item[0]))
    summary_items = sorted_summary[:max_log_summary_groups]
    log_summary = {
        "raw_rows": table_counts["log/log_service.csv"],
        "observed_nonzero_minute_groups": len(log_groups),
        "projected_groups": projected_log_points,
        "zero_filled_buckets": zero_filled,
        "summary_group_count": len(summary_items),
        "summary_omitted_groups": max(0, len(sorted_summary) - len(summary_items)),
        "summary_truncated": len(sorted_summary) > len(summary_items),
        "summary_policy": f"top {max_log_summary_groups} component/log_name pairs by row count; source rows fully parsed; zero means no recorded rows in the supplied clip, not evidence of healthy logging",
        "top_component_log_counts": [
            {"component_id": component, "log_name": log_name, "rows": count}
            for (component, log_name), count in summary_items],
    }

    # The public catalog contributes exact observed identifiers only; no alias
    # or service/database relationship is synthesized from a pod name.
    catalog_entities = {}
    catalog_entity_doc = catalog["entities"]
    all_observed = set().union(*observed_ids.values())
    for identifier in sorted(all_observed):
        info = catalog_entity_doc.get(identifier, {})
        catalog_entities[identifier] = dict(info) if isinstance(info, Mapping) else {}
        catalog_entities[identifier].setdefault("role", "component")
        catalog_entities[identifier].setdefault("relationship", "unknown")
    quality.update({
        "bank_recognized_named_pod_rows": sum(recognized_rows.values()),
        "bank_unknown_component_id_rows": sum(unknown_rows.values()),
        "bank_observed_component_id_count": len(all_observed),
        "bank_metric_app_opaque_kpi_count": len({point.entity.identifier for point in metrics
            if point.table == "bank/metric_app.csv"}),
        "trace_span_success_status_missing_rows": len(spans),
        "trace_span_duration_unit_undeclared_rows": len(spans),
        "log_service_full_rows_parsed": table_counts["log/log_service.csv"],
        "log_service_duplicate_log_id_rows": sum(value - 1 for value in log_id_counts.values()),
    })

    metric_policy_hash = sha256_file(metric_policy_path)
    kpi_policy_hash = sha256_file(kpi_policy_path)
    manifest_hash = sha256_file(manifest_path)
    data_parts = {key: value for key, value in file_hashes.items()}
    identity_doc = {"case_id": case_id, "files": data_parts,
                    "windows": {"reference": [reference.start_ms_utc, reference.end_ms_utc],
                                "alert": [alert_window.start_ms_utc, alert_window.end_ms_utc]},
                    "input_manifest_sha256": manifest_hash,
                    "metric_policy_sha256": metric_policy_hash,
                    "kpi_policy_sha256": kpi_policy_hash,
                    "catalog_sha256": catalog_hash}
    data_sha = hashlib.sha256(json.dumps(identity_doc, sort_keys=True, separators=(",", ":"),
                                         ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    file_hashes["config/bank/input_manifest.json"] = manifest_hash
    file_hashes["config/bank/metric_kind_policy.json"] = metric_policy_hash
    file_hashes["config/bank/kpi_policy.json"] = kpi_policy_hash
    file_bytes["config/bank/sample.json"] = sample_path.stat().st_size
    file_bytes["config/bank/input_manifest.json"] = manifest_path.stat().st_size
    file_bytes["config/bank/metric_kind_policy.json"] = metric_policy_path.stat().st_size
    file_bytes["config/bank/kpi_policy.json"] = kpi_policy_path.stat().st_size
    adapter_version = (f"bank-adapter/1;manifest={manifest_hash};metric_policy={metric_policy_hash};"
                       f"kpi_policy={kpi_policy_hash}")
    telemetry = Telemetry(
        case_id=case_id, reference_window=reference, alert_window=alert_window,
        metrics=tuple(metrics), spans=tuple(spans), deployment_catalog=catalog_entities,
        data_sha256=data_sha, file_sha256=file_hashes,
        duplicate_counts=dict(duplicates), quality_counts=dict(quality),
        adapter_version=adapter_version, file_bytes=file_bytes)
    provenance = {
        "input_manifest_sha256": manifest_hash,
        "sample_config_sha256": sha256_file(sample_path),
        "candidate_manifest_sha256": manifest.get("candidate_manifest_sha256"),
        "dev_manifest_sha256": manifest.get("dev_manifest_sha256"),
        "clip_manifest_sha256": manifest.get("clip_manifest_sha256"),
        "deployment_catalog_sha256": catalog_hash,
        "deployment_catalog_schema_sha256": manifest.get("deployment_catalog_schema_sha256"),
        "metric_policy_sha256": metric_policy_hash,
        "dev_kpi_policy_sha256": kpi_policy_hash,
        "bank_source_revision": BANK_SOURCE_REVISION,
        "table_rows": dict(table_counts),
        "table_bytes": dict(file_bytes),
        "upstream_declared_timestamp_units": dict(alert["timestamp_unit_in_source_files"]),
        "empirical_timestamp_units_by_source_file": {
            relative: doc["timestamp_unit"] for relative, doc in case_manifest["files"].items()},
        "empirical_timestamp_units_by_source_date": dict(alert["source_date_timestamp_units"]),
        "upstream_timestamp_unit_provenance": alert.get("upstream_timestamp_unit_provenance"),
        "source_parent_identity": "trace_id + span_id; parent_id resolved only within the same trace",
        "trace_success_semantics": "no source success field; success is missing",
        "trace_duration_semantics": "native duration preserved; absolute units undeclared; within-field comparison only",
        "topology_semantics": "pod IDs preserved exactly; no aliases or vertical relationships inferred",
        "component_identity_coverage": {
            modality: {
                "observed_ids": sorted(observed_ids[modality]),
                "recognized_official_candidate_ids": sorted(observed_ids[modality] & official_candidates),
                "unmapped_ids": sorted(observed_ids[modality] - official_candidates),
                "recognized_rows": recognized_rows[modality],
                "unmapped_rows": unknown_rows[modality],
            } for modality in ("metric_container", "trace_span", "log_service")},
    }
    return BankCase(telemetry=telemetry, alert=MappingProxyType(dict(alert)),
                    public_manifest_row=MappingProxyType(dict(public_row)),
                    deployment_catalog=MappingProxyType(dict(catalog)),
                    log_summary=MappingProxyType(log_summary),
                    provenance=MappingProxyType(provenance))
