"""Split-aware, single-incident public Bank task views for M5."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import os
import math
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import mean, median
from typing import Any, Mapping

from e4.paths import ROOT


CASE_ID = re.compile(r"^bank-[0-9]{8}T[0-9]{4}-utc8$")
TABLE_SCHEMAS: Mapping[str, tuple[str, ...]] = {
    "metric/metric_app.csv": ("timestamp", "rr", "sr", "cnt", "mrt", "tc"),
    "metric/metric_container.csv": ("timestamp", "cmdb_id", "kpi_name", "value"),
    "trace/trace_span.csv": ("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"),
    "log/log_service.csv": ("log_id", "timestamp", "cmdb_id", "log_name", "value"),
}
FORBIDDEN_KEYS = {"ground_truth", "scoring_points", "root_cause", "fault_component", "reason"}


class TaskViewError(ValueError):
    """Public task data is malformed, outside the selected split, or fails pins."""


def _linear_quantile(values: list[float], q: float) -> float:
    if not values or not 0.0 <= q <= 1.0:
        raise ValueError("linear quantile requires values and q in [0,1]")
    position = q * (len(values) - 1)
    lower = int(position)
    upper = min(len(values) - 1, lower + 1)
    fraction = position - lower
    return float(values[lower] + (values[upper] - values[lower]) * fraction)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _reject_labels(value: Any, where: str) -> None:
    if isinstance(value, dict):
        if FORBIDDEN_KEYS.intersection(str(key).casefold() for key in value):
            raise TaskViewError(f"label-bearing field in public {where}")
        for child in value.values():
            _reject_labels(child, where)
    elif isinstance(value, list):
        for child in value:
            _reject_labels(child, where)


def _select_app_context_kpi(files: Mapping[str, Path], alert: Mapping[str, Any],
                            policy: Mapping[str, Any], policy_hash: str) -> dict[str, Any]:
    """Use the frozen M4 support-first MRT selector on the same public windows."""
    minimum = int(policy["minimum_support_per_window"])
    allowed = set(policy.get("primary_metric_allowlist", ()))
    windows = {"reference": (int(alert["analysis_window_start_epoch_ms_utc"]),
                             int(alert["alert_window_start_epoch_ms_utc"])),
               "alert": (int(alert["alert_window_start_epoch_ms_utc"]),
                         int(alert["alert_window_end_epoch_ms_utc"]))}
    unit_map = alert.get("source_date_timestamp_units", {})
    buckets: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    raw_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for relative, path in files.items():
        if not relative.endswith("/metric/metric_app.csv"):
            continue
        source_date = Path(relative).parts[1]
        unit = unit_map.get(source_date, {}).get("metric/metric_app.csv")
        if unit not in {"seconds", "milliseconds"}:
            raise TaskViewError("metric_app timestamp unit is missing from selected public metadata")
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            for row in csv.DictReader(stream):
                try:
                    timestamp = Decimal(row["timestamp"])
                    timestamp_ms = timestamp * (1000 if unit == "seconds" else 1)
                    value = float(Decimal(row["mrt"]))
                    tc = row["tc"].strip()
                except (KeyError, InvalidOperation, ValueError, TypeError, OverflowError):
                    continue
                if (not timestamp_ms.is_finite() or timestamp_ms != timestamp_ms.to_integral_value() or
                        not math.isfinite(value) or not tc or tc.casefold() in {"none", "null"}):
                    continue
                timestamp_int = int(timestamp_ms)
                window = next((name for name, (start, end) in windows.items()
                               if start <= timestamp_int < end), None)
                if window is not None and "metric_app.mrt" in allowed:
                    buckets[(window, "mrt", tc, timestamp_int)].append(value)
                    raw_counts[("metric_app.mrt", tc, window)] += 1
    grouped: dict[tuple[str, str, str], list[tuple[int, float]]] = defaultdict(list)
    for (window, field, tc, timestamp), values in buckets.items():
        grouped[(window, field, tc)].append((timestamp, float(median(values))))
    before_tcs = {tc for window, field, tc in grouped if window == "reference" and field == "mrt"}
    after_tcs = {tc for window, field, tc in grouped if window == "alert" and field == "mrt"}
    candidates = []
    for tc in sorted(before_tcs & after_tcs, key=lambda value: (value.casefold(), value)):
        before = sorted(value for _, value in grouped[("reference", "mrt", tc)])
        after = sorted(value for _, value in grouped[("alert", "mrt", tc)])
        if min(len(before), len(after)) < minimum:
            continue
        candidates.append({"kpi_id": "metric_app.mrt", "field": "mrt", "tc_id": tc,
            "role": "app_kpi_context_only", "reference_support": len(before),
            "alert_support": len(after), "paired_support": min(len(before), len(after)),
            "raw_rows": {name: raw_counts[("metric_app.mrt", tc, name)]
                         for name in ("reference", "alert")},
            "duplicate_rows_collapsed": {
                name: raw_counts[("metric_app.mrt", tc, name)] - support
                for name, support in (("reference", len(before)), ("alert", len(after)))},
            "reference": {"median": float(median(before)), "mean": float(mean(before)),
                          "q90": _linear_quantile(before, 0.9)},
            "alert": {"median": float(median(after)), "mean": float(mean(after)),
                      "q90": _linear_quantile(after, 0.9)},
            "unit": "undeclared", "component_mapping": "none",
            "interpretation": "mean-response-time-style working interpretation from field name only"})
    candidates.sort(key=lambda item: (-item["paired_support"], item["tc_id"].casefold(),
                                      item["tc_id"], item["kpi_id"]))
    if not candidates:
        return {"kpi_id": None,
                "selection_reason": "no allowlisted app KPI/tc pair met frozen support; no resource fallback",
                "minimum_support_per_window": minimum, "policy_sha256": policy_hash,
                "source_policy": policy.get("selection_provenance")}
    return {**candidates[0], "selection_reason": policy.get("selection"),
            "minimum_support_per_window": minimum, "policy_sha256": policy_hash,
            "source_policy": policy.get("selection_provenance")}


@dataclass(frozen=True)
class BankCaseView:
    case_id: str
    split: str
    alert_text: str
    timezone: str
    alert_window_start: str
    alert_window_end: str
    alert: Mapping[str, Any]
    alert_sha256: str
    reference_window_utc_ms: tuple[int, int]
    alert_window_utc_ms: tuple[int, int]
    telemetry_dates: tuple[str, ...]
    source_date_timestamp_units: Mapping[str, Mapping[str, str]]
    telemetry_root: Path
    telemetry_files: Mapping[str, Path]
    telemetry_file_sha256: Mapping[str, str]
    telemetry_schema: Mapping[str, tuple[str, ...]]
    component_ids: tuple[str, ...]
    official_candidate_components: tuple[str, ...]
    kpi_context: Mapping[str, Any]
    input_manifest_sha256: str
    public_manifest_sha256: str
    staged_root: Path | None = None

    def as_public_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id, "split": self.split, "alert_text": self.alert_text,
            "timezone": self.timezone, "alert_window_start": self.alert_window_start,
            "alert_window_end": self.alert_window_end,
            "alert": dict(self.alert),
            "alert_sha256": self.alert_sha256,
            "reference_window_utc_ms": list(self.reference_window_utc_ms),
            "alert_window_utc_ms": list(self.alert_window_utc_ms),
            "telemetry_dates": list(self.telemetry_dates),
            "source_date_timestamp_units": {date: dict(units) for date, units in
                                             self.source_date_timestamp_units.items()},
            "telemetry_files": {key: str(value) for key, value in sorted(self.telemetry_files.items())},
            "telemetry_file_sha256": dict(sorted(self.telemetry_file_sha256.items())),
            "telemetry_schema": {key: list(value) for key, value in self.telemetry_schema.items()},
            "component_ids": list(self.component_ids),
            "official_candidate_components": list(self.official_candidate_components),
            "kpi_context": dict(self.kpi_context),
            "input_manifest_sha256": self.input_manifest_sha256,
            "public_manifest_sha256": self.public_manifest_sha256,
        }


def load_bank_case_view(case_id: str, *, split: str, root: Path = ROOT,
                        staged_root: Path | None = None) -> BankCaseView:
    """Load exactly one frozen split case and verify each public clip against its pins.

    This loader reads only the selected split's public manifest and public clips. It never
    opens an evaluator path. A scored invocation must be made only after the caller's freeze gate.
    """
    root = Path(root).resolve()
    if split not in {"dev", "scored"} or not isinstance(case_id, str) or not CASE_ID.fullmatch(case_id):
        raise TaskViewError("a valid split and frozen Bank case ID are required")
    sample = json.loads((root / "config/bank/sample.json").read_text(encoding="utf-8"))
    _reject_labels(sample, "sample configuration")
    key = "development_cases" if split == "dev" else "scored_cases"
    allowlisted = [row for row in sample.get(key, []) if row.get("incident_id") == case_id]
    if len(allowlisted) != 1:
        raise TaskViewError("case is not a unique member of the requested frozen split")
    public_manifest_path = root / "data/prepared/bank" / ("dev_incidents.jsonl" if split == "dev"
                                                              else "scored_incidents.jsonl")
    selected = []
    with public_manifest_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                row = json.loads(line)
                if row.get("incident_id") == case_id:
                    selected.append(row)
    if len(selected) != 1 or selected[0].get("split") != split:
        raise TaskViewError("public split manifest does not contain exactly one selected case")
    alert = selected[0]
    _reject_labels(alert, "incident manifest")
    if (alert.get("timezone") != "Asia/Shanghai" or not isinstance(alert.get("alert_text"), str) or
            any(type(alert.get(key)) is not int for key in
                ("analysis_window_start_epoch_ms_utc", "alert_window_start_epoch_ms_utc",
                 "alert_window_end_epoch_ms_utc"))):
        raise TaskViewError("selected public alert has malformed timezone, text, or UTC windows")
    if (alert["alert_window_start_epoch_ms_utc"] - alert["analysis_window_start_epoch_ms_utc"] != 1_800_000 or
            alert["alert_window_end_epoch_ms_utc"] - alert["alert_window_start_epoch_ms_utc"] != 1_800_000):
        raise TaskViewError("selected Bank reference/alert windows must be adjacent 30-minute intervals")
    for local_key, epoch_key in (("analysis_window_start_local", "analysis_window_start_epoch_ms_utc"),
                                 ("alert_window_start_local", "alert_window_start_epoch_ms_utc"),
                                 ("alert_window_end_local", "alert_window_end_epoch_ms_utc")):
        try:
            local_time = datetime.fromisoformat(alert[local_key])
        except (KeyError, TypeError, ValueError):
            raise TaskViewError(f"selected public alert has invalid {local_key}") from None
        if (local_time.utcoffset() != timedelta(hours=8) or
                int(local_time.timestamp() * 1000) != alert[epoch_key]):
            raise TaskViewError(f"selected public alert {local_key} disagrees with pinned UTC milliseconds")
    if alert != allowlisted[0]:
        # The prepared public row adds provenance fields; compare only the fixed task contract.
        for field in ("incident_id", "alert_text", "alert_window_start_local", "alert_window_end_local",
                      "telemetry_dates", "timezone"):
            if alert.get(field) != allowlisted[0].get(field):
                raise TaskViewError(f"public manifest differs from frozen sample field {field}")

    pin_doc = json.loads((root / "config/bank/input_manifest.json").read_text(encoding="utf-8"))
    if pin_doc.get("schema_version") != "e4-bank-input-manifest/1":
        raise TaskViewError("unsupported Bank input pin manifest")
    sample_path = root / "config/bank/sample.json"
    split_pin_key = "dev_manifest_sha256" if split == "dev" else "scored_manifest_sha256"
    required_pins = {
        "sample_config_sha256": (sample_path, _sha256(sample_path)),
        split_pin_key: (public_manifest_path, _sha256(public_manifest_path)),
        "clip_manifest_sha256": (root / "data/prepared/bank/public/clip_manifest.jsonl",
                                 _sha256(root / "data/prepared/bank/public/clip_manifest.jsonl")),
        "deployment_catalog_file_sha256": (root / "data/prepared/bank/public/deployment_catalog.json",
                                           _sha256(root / "data/prepared/bank/public/deployment_catalog.json")),
    }
    for pin_name, (_path, actual_hash) in required_pins.items():
        if pin_doc.get(pin_name) != actual_hash:
            raise TaskViewError(f"prepared Bank public input failed its {pin_name} pin")
    schema_path = root / "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py"
    if _sha256(schema_path) != pin_doc.get("deployment_catalog_schema_sha256"):
        raise TaskViewError("upstream Bank schema document differs from its frozen pin")
    case_pin = pin_doc.get("cases", {}).get(case_id)
    if not isinstance(case_pin, dict) or not isinstance(case_pin.get("files"), dict):
        raise TaskViewError("input manifest has no selected case file pins")
    expected_files = {f"source_dates/{date}/{table}"
                      for date in alert.get("telemetry_dates", []) for table in TABLE_SCHEMAS}
    if set(case_pin["files"]) != expected_files:
        raise TaskViewError("selected case files do not match its declared telemetry dates and tables")
    source_units = alert.get("source_date_timestamp_units")
    if (not isinstance(source_units, dict) or set(source_units) != set(alert.get("telemetry_dates", [])) or
            any(not isinstance(source_units.get(date), dict) or
                set(source_units[date]) != set(TABLE_SCHEMAS) for date in alert.get("telemetry_dates", []))):
        raise TaskViewError("selected case timestamp units do not cover every declared native table")
    expected_windows = {
        "reference": {"start_epoch_ms_utc": alert.get("analysis_window_start_epoch_ms_utc"),
                      "end_epoch_ms_utc": alert.get("alert_window_start_epoch_ms_utc")},
        "alert": {"start_epoch_ms_utc": alert.get("alert_window_start_epoch_ms_utc"),
                  "end_epoch_ms_utc": alert.get("alert_window_end_epoch_ms_utc")},
    }
    if case_pin.get("windows") != expected_windows:
        raise TaskViewError("selected case windows differ from input manifest pins")
    clip_manifest_path = root / "data/prepared/bank/public/clip_manifest.jsonl"
    clip_rows = []
    with clip_manifest_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                row = json.loads(line)
                if row.get("case_id") == case_id:
                    clip_rows.append(row)
    selected_clips = {row.get("relative_path"): row for row in clip_rows}
    if len(selected_clips) != len(clip_rows):
        raise TaskViewError("selected clip manifest contains duplicate relative paths")
    if set(selected_clips) != set(case_pin["files"]):
        raise TaskViewError("selected public clip list differs from the pinned case file set")
    source_dir = (root / "data/prepared/bank/public/cases" / case_id).resolve(strict=True)
    public_root = (root / "data/prepared/bank/public/cases").resolve(strict=True)
    if public_root not in source_dir.parents:
        raise TaskViewError("selected case path escapes public case root")
    alert_path = (source_dir / "alert.json").resolve(strict=True)
    if (source_dir not in alert_path.parents or not alert_path.is_file() or
            _sha256(alert_path) != case_pin.get("alert_sha256")):
        raise TaskViewError("selected public alert differs from its frozen case pin")
    pinned_alert = json.loads(alert_path.read_text(encoding="utf-8"))
    for field in ("incident_id", "alert_text", "timezone", "alert_window_start_local",
                  "alert_window_end_local", "analysis_window_start_local",
                  "analysis_window_start_epoch_ms_utc", "alert_window_start_epoch_ms_utc",
                  "alert_window_end_epoch_ms_utc", "timestamp_unit_in_source_files",
                  "upstream_timestamp_unit_provenance", "source_date_timestamp_units",
                  "telemetry_date", "telemetry_dates"):
        if field not in pinned_alert or alert.get(field) != pinned_alert[field]:
            raise TaskViewError(f"selected alert file and split manifest disagree on {field}")
    dest = source_dir
    if staged_root:
        raw_dest = Path(staged_root).absolute()
        if raw_dest.exists():
            raise TaskViewError("staged task-view destination must be fresh; existing paths are never deleted")
        run_dir_value = os.environ.get("E4_RUN_DIR")
        if not run_dir_value:
            raise TaskViewError("staged task views must be created under the model runner E4_RUN_DIR")
        run_dir = Path(run_dir_value).resolve()
        dest = raw_dest
        if run_dir not in dest.parents:
            raise TaskViewError("staged task-view destination must be a child of E4_RUN_DIR")
        for parent in (dest, *dest.parents):
            if parent.exists() and parent.is_symlink():
                raise TaskViewError("staged task-view destination cannot traverse symlinks")
            if parent == run_dir:
                break
        if dest == source_dir or source_dir in dest.parents or dest in source_dir.parents:
            raise TaskViewError("staged task view cannot overlap the public source case")
        dest.mkdir(parents=True, exist_ok=False)
    files: dict[str, Path] = {}
    file_hashes: dict[str, str] = {}
    components: set[str] = set()
    for relative, pin in sorted(case_pin["files"].items()):
        parts = Path(relative).parts
        native_table = "/".join(parts[-2:])
        if len(parts) != 4 or parts[0] != "source_dates" or native_table not in TABLE_SCHEMAS:
            raise TaskViewError(f"unsupported public table path: {relative}")
        source = (source_dir / relative).resolve(strict=True)
        if source_dir not in source.parents or not source.is_file():
            raise TaskViewError("selected clip path escapes case directory")
        if source.stat().st_size != pin.get("size_bytes") or _sha256(source) != pin.get("sha256"):
            raise TaskViewError("selected clip differs from its frozen source pin")
        clip = selected_clips[relative]
        if (clip.get("clip_sha256") != pin.get("sha256") or
                clip.get("clip_size_bytes") != pin.get("size_bytes") or
                clip.get("clip_rows") != pin.get("rows") or
                clip.get("header") != pin.get("header")):
            raise TaskViewError("public clip manifest disagrees with input pin")
        if clip.get("reference_window_utc_ms") != [expected_windows["reference"]["start_epoch_ms_utc"],
                                                     expected_windows["reference"]["end_epoch_ms_utc"]]:
            raise TaskViewError("public clip reference window differs from alert and input pins")
        if clip.get("alert_window_utc_ms") != [expected_windows["alert"]["start_epoch_ms_utc"],
                                                expected_windows["alert"]["end_epoch_ms_utc"]]:
            raise TaskViewError("public clip alert window differs from alert and input pins")
        if (clip.get("source_date") != parts[1] or clip.get("timestamp_field") != "timestamp" or
                clip.get("timestamp_source_unit") != source_units[parts[1]][native_table]):
            raise TaskViewError("public clip date/time-unit metadata differs from the pinned alert")
        with source.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != TABLE_SCHEMAS[native_table]:
                raise TaskViewError(f"public clip schema differs for {relative}")
            count = 0
            for row in reader:
                count += 1
                identifier = row.get("cmdb_id")
                if identifier and identifier.strip().casefold() not in {"none", "null"}:
                    components.add(identifier.strip())
        if count != pin.get("rows"):
            raise TaskViewError("selected clip row count differs from frozen pin")
        target = dest / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if staged_root:
            shutil.copyfile(source, target)
        else:
            target = source
        files[relative] = target
        file_hashes[relative] = pin["sha256"]

    if staged_root:
        (dest / "alert.json").write_bytes(alert_path.read_bytes())

    if not components:
        raise TaskViewError("selected case has no observed component identities")
    policy_path = root / "config/bank/kpi_policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    catalog_path = root / "data/prepared/bank/public/deployment_catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    source = catalog.get("source")
    candidates = catalog.get("official_candidate_components")
    if (catalog.get("schema_version") != "bank-source-catalog/1" or
            not isinstance(source, dict) or source.get("official_revision") != "c121a483a5a91dbae50708e5942c42a6609098b2" or
            source.get("official_repository") != "https://github.com/microsoft/OpenRCA" or
            source.get("prompt_sha256") != pin_doc.get("deployment_catalog_schema_sha256") or
            not isinstance(candidates, list) or any(type(item) is not str for item in candidates) or
            len({item.casefold() for item in candidates}) != len(candidates) or
            catalog.get("topology", {}).get("relationships") != []):
        raise TaskViewError("public Bank deployment catalog differs from its pinned source contract")
    kpi = {"fixed_primary_kpi_rule": policy.get("selection"),
           "primary_metric_allowlist": policy.get("primary_metric_allowlist"),
           "unit_policy": policy.get("unit_policy"),
           "minimum_support_per_window": policy.get("minimum_support_per_window"),
           "policy_sha256": _sha256(policy_path),
           "selected_app_context_kpi": _select_app_context_kpi(files, alert, policy,
                                                                _sha256(policy_path))}
    return BankCaseView(
        case_id=case_id, split=split, alert_text=alert["alert_text"], timezone=alert["timezone"],
        alert_window_start=alert["alert_window_start_local"], alert_window_end=alert["alert_window_end_local"],
        alert=alert,
        alert_sha256=case_pin["alert_sha256"],
        reference_window_utc_ms=(int(alert["analysis_window_start_epoch_ms_utc"]),
                                 int(alert["alert_window_start_epoch_ms_utc"])),
        alert_window_utc_ms=(int(alert["alert_window_start_epoch_ms_utc"]),
                             int(alert["alert_window_end_epoch_ms_utc"])),
        telemetry_dates=tuple(alert["telemetry_dates"]),
        source_date_timestamp_units=alert["source_date_timestamp_units"], telemetry_root=dest,
        telemetry_files=files, telemetry_file_sha256=file_hashes, telemetry_schema=TABLE_SCHEMAS,
        component_ids=tuple(sorted(components, key=lambda x: (x.casefold(), x))),
        official_candidate_components=tuple(candidates),
        kpi_context=kpi, input_manifest_sha256=_sha256(root / "config/bank/input_manifest.json"),
        public_manifest_sha256=_sha256(public_manifest_path), staged_root=dest if staged_root else None)
