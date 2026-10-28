"""Leak-resistant preparation of OpenRCA Telecom alert metadata."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from e4.paths import ROOT

LOCAL_ZONE = ZoneInfo("Asia/Shanghai")
WINDOW_SECONDS = 30 * 60
MONTHS = {name.lower(): i for i, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"), start=1)}
WINDOW_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4}).*?\b(?P<start>\d{1,2}:\d{2})"
    r"\s+(?:to|and)\s+(?P<end>\d{1,2}:\d{2})", re.IGNORECASE | re.DOTALL)
RECORD_FIELDS = ("level", "reason", "component", "timestamp", "datetime")
QUERY_FIELDS = ("task_index", "instruction", "scoring_points")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv_rows(path: Path, expected_fields: Iterable[str]) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != tuple(expected_fields):
            raise ValueError(f"{path}: expected header {tuple(expected_fields)}, got {reader.fieldnames}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path}: no data rows")
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f"{path}: malformed row width")
    return rows


def parse_alert_window(instruction: str) -> tuple[datetime, datetime]:
    """Extract the user-visible half-hour window, never its hidden scoring criteria."""
    match = WINDOW_RE.search(instruction)
    if not match:
        raise ValueError(f"could not extract date and half-hour window from query: {instruction!r}")
    month = MONTHS[match.group("month").lower()]
    day, year = int(match.group("day")), int(match.group("year"))
    start_clock = datetime.strptime(match.group("start"), "%H:%M").time()
    end_clock = datetime.strptime(match.group("end"), "%H:%M").time()
    start = datetime(year, month, day, start_clock.hour, start_clock.minute, tzinfo=LOCAL_ZONE)
    end = datetime(year, month, day, end_clock.hour, end_clock.minute, tzinfo=LOCAL_ZONE)
    if end <= start:
        end += timedelta(days=1)
    if (end - start).total_seconds() != WINDOW_SECONDS:
        raise ValueError(f"expected a 30-minute OpenRCA query window, got {start} to {end}")
    return start, end


def read_query_windows(path: Path) -> list[tuple[datetime, datetime]]:
    """Read only the instruction column; scoring_points is intentionally discarded."""
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if tuple(header or ()) != QUERY_FIELDS:
            raise ValueError(f"{path}: expected query columns {QUERY_FIELDS}, got {header}")
        instruction_idx = header.index("instruction")
        windows = []
        for line, row in enumerate(reader, start=2):
            if len(row) != len(header):
                raise ValueError(f"{path}:{line}: malformed row width")
            # row[scoring_points] is never accessed or returned.
            windows.append(parse_alert_window(row[instruction_idx]))
    return windows


def incident_id(start: datetime) -> str:
    return "telecom-" + start.strftime("%Y%m%dT%H%M") + "-utc8"


def prepare_metadata(raw_dir: Path, output_dir: Path, audit_dir: Path) -> dict[str, Any]:
    record_path, query_path = raw_dir / "record.csv", raw_dir / "query.csv"
    records = read_csv_rows(record_path, RECORD_FIELDS)
    windows = read_query_windows(query_path)
    if len(records) != len(windows):
        raise ValueError(f"record/query row count differs: {len(records)} vs {len(windows)}")

    grouped: dict[str, dict[str, Any]] = {}
    exact_rows: set[tuple[str, ...]] = set()
    duplicate_rows = 0
    timestamp_mismatch = 0
    for index, (record, (start, end)) in enumerate(zip(records, windows)):
        try:
            timestamp = int(record["timestamp"])
        except ValueError as exc:
            raise ValueError(f"record row {index + 2}: timestamp must be epoch seconds") from exc
        # Explicitly treat OpenRCA record.csv timestamps as seconds; telemetry files use ms.
        fault_time = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(LOCAL_ZONE)
        written_datetime = datetime.strptime(record["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_ZONE)
        if fault_time != written_datetime:
            raise ValueError(f"record row {index + 2}: timestamp seconds disagree with datetime in UTC+8")
        if not start <= fault_time < end:
            timestamp_mismatch += 1
        row_key = tuple(record[field].strip() for field in RECORD_FIELDS)
        if row_key in exact_rows:
            duplicate_rows += 1
            continue
        exact_rows.add(row_key)
        case_id = incident_id(start)
        case = grouped.setdefault(case_id, {
            "incident_id": case_id,
            "alert_window_start_local": start.isoformat(timespec="seconds"),
            "alert_window_end_local": end.isoformat(timespec="seconds"),
            "telemetry_date": start.strftime("%Y_%m_%d"),
            "timezone": "Asia/Shanghai",
            "alert_text": ("A failure was reported in the Telecom system during the interval "
                           f"{start.strftime('%Y-%m-%d %H:%M:%S')} to {end.strftime('%Y-%m-%d %H:%M:%S')} "
                           "(UTC+8). The affected component and underlying cause are unknown. "
                           "Diagnose the incident using the available telemetry."),
            "_gold": [], "_strata": set(), "_source_rows": [],
        })
        case["_gold"].append({
            "level": record["level"].strip(), "component": record["component"].strip(),
            "reason": record["reason"].strip(), "timestamp_epoch_seconds": timestamp,
            "timestamp_local": fault_time.isoformat(timespec="seconds"),
        })
        case["_strata"].add(record["reason"].strip())
        case["_source_rows"].append(index + 2)

    cases = sorted(grouped.values(), key=lambda row: row["incident_id"])
    if timestamp_mismatch:
        raise ValueError(f"{timestamp_mismatch} record onset(s) fall outside the supplied query windows")
    if not cases:
        raise ValueError("no distinct alert windows after deduplication")

    output_dir.mkdir(parents=True, exist_ok=True)
    evaluator_dir = output_dir / "evaluator"
    evaluator_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_dir / "candidate_alerts.jsonl", (public_case(case) for case in cases))
    write_jsonl(evaluator_dir / "candidate_ground_truth.jsonl", (gold_case(case) for case in cases))
    dates = sorted({case["telemetry_date"] for case in cases})
    required_dates_path = audit_dir / "required_telemetry_dates.json"
    audit_dir.mkdir(parents=True, exist_ok=True)
    required_dates_path.write_text(json.dumps({"dates": dates, "count": len(dates),
                                                "basis": "supplied query.csv instruction windows"}, indent=2) + "\n")
    reason_counts = Counter(gold["reason"] for case in cases for gold in case["_gold"])
    level_counts = Counter(gold["level"] for case in cases for gold in case["_gold"])
    report = {
        "source_files": {
            "record.csv": {"path": str(record_path), "sha256": sha256(record_path), "bytes": record_path.stat().st_size},
            "query.csv": {"path": str(query_path), "sha256": sha256(query_path), "bytes": query_path.stat().st_size},
        },
        "record_rows": len(records), "query_rows": len(windows),
        "exact_duplicate_record_rows_dropped": duplicate_rows,
        "physical_incident_definition": "one supplied 30-minute query alert window in Asia/Shanghai; exact duplicate record rows removed; multiple distinct ground-truth rows within a window are retained as a multi-cause gold set",
        "physical_incident_count": len(cases), "query_onset_window_mismatches": timestamp_mismatch,
        "record_epoch_unit": "seconds", "telemetry_epoch_unit": "milliseconds (per upstream Telecom schema)",
        "timezone": "Asia/Shanghai", "ground_truth_counts_by_reason": dict(sorted(reason_counts.items())),
        "ground_truth_counts_by_level": dict(sorted(level_counts.items())),
        "candidate_alerts": str(output_dir / "candidate_alerts.jsonl"),
        "candidate_gold_separate": str(evaluator_dir / "candidate_ground_truth.jsonl"),
        "required_telemetry_dates": str(required_dates_path),
        "leakage_controls": ["query.csv instruction used only to parse the supplied time interval",
                             "query.csv scoring_points column is not read or emitted",
                             "record.csv labels are written only to evaluator/ and the audit report",
                             "public alerts are normalized from the interval and omit count, component, reason, level, and exact onset"],
    }
    (audit_dir / "telecom_metadata_audit.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def public_case(case: dict[str, Any], split: str | None = None) -> dict[str, Any]:
    result = {key: value for key, value in case.items() if not key.startswith("_")}
    if split:
        result["split"] = split
    return result


def gold_case(case: dict[str, Any], split: str | None = None) -> dict[str, Any]:
    item = {"incident_id": case["incident_id"], "ground_truth": case["_gold"]}
    if split:
        item["split"] = split
    return item


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def jsonl_digest(rows: Iterable[dict[str, Any]]) -> str:
    content = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    return hashlib.sha256(content.encode()).hexdigest()


def load_candidates(public_path: Path, gold_path: Path) -> tuple[list[dict], dict[str, dict]]:
    public = [json.loads(line) for line in public_path.read_text().splitlines() if line.strip()]
    gold_rows = [json.loads(line) for line in gold_path.read_text().splitlines() if line.strip()]
    gold = {row["incident_id"]: row["ground_truth"] for row in gold_rows}
    if len(gold) != len(gold_rows) or {x["incident_id"] for x in public} != set(gold):
        raise ValueError("candidate public manifest and hidden gold IDs are inconsistent")
    return public, gold


def analysis_interval(case: dict[str, Any]) -> tuple[datetime, datetime]:
    alert_start = datetime.fromisoformat(case["alert_window_start_local"])
    alert_end = datetime.fromisoformat(case["alert_window_end_local"])
    return alert_start - timedelta(minutes=30), alert_end


def intervals_overlap(left: tuple[datetime, datetime], right: tuple[datetime, datetime]) -> bool:
    """Use half-open intervals so adjacent boundaries do not count as overlap."""
    return left[0] < right[1] and right[0] < left[1]


def sample_cases(public: list[dict], gold: dict[str, list[dict]], eligible_ids: set[str],
                 output_dir: Path, audit_dir: Path, seed: int = 42,
                 scored_per_reason: int = 2, dev_count: int = 2,
                 freeze_path: Path | None = None, overwrite_freeze: bool = False) -> dict[str, Any]:
    expected_strata: dict[str, list[str]] = defaultdict(list)
    for case_id, labels in gold.items():
        expected_strata[" + ".join(sorted({row["reason"] for row in labels}))].append(case_id)
    candidates = [case for case in public if case["incident_id"] in eligible_ids]
    by_id = {case["incident_id"]: case for case in candidates}
    if len(by_id) != len(candidates):
        raise ValueError("duplicate public incident IDs")
    strata: dict[str, list[str]] = defaultdict(list)
    for case in candidates:
        reasons = sorted({row["reason"] for row in gold[case["incident_id"]]})
        # Multi-cause windows form their own explicit stratum rather than choosing a label post hoc.
        stratum = " + ".join(reasons)
        strata[stratum].append(case["incident_id"])
    if set(strata) != set(expected_strata):
        missing = sorted(set(expected_strata) - set(strata))
        raise ValueError(f"coverage eligibility removed entire reason strata; refusing to change the sample composition: {missing}")
    expected_scored_count = len(expected_strata) * scored_per_reason
    if expected_scored_count != 10:
        raise ValueError(f"requested design yields {expected_scored_count} scored cases; expected exactly 10")
    if any(len(ids) < scored_per_reason for ids in strata.values()):
        insufficient = {reason: len(ids) for reason, ids in strata.items() if len(ids) < scored_per_reason}
        raise ValueError(f"eligible population cannot support requested stratification: {insufficient}")

    rng = random.Random(seed)
    scored_ids: list[str] = []
    for stratum in sorted(strata):
        scored_ids.extend(rng.sample(sorted(strata[stratum]), scored_per_reason))
    if len(scored_ids) != 10:
        raise AssertionError(f"sample design produced {len(scored_ids)} scored cases instead of 10")
    remainder = sorted(set(by_id) - set(scored_ids))
    scored_intervals = [analysis_interval(by_id[case_id]) for case_id in scored_ids]
    dev_candidates = [case_id for case_id in remainder
                      if not any(intervals_overlap(analysis_interval(by_id[case_id]), scored)
                                 for scored in scored_intervals)]
    if len(dev_candidates) < dev_count:
        raise ValueError(f"only {len(dev_candidates)} candidates remain after excluding telemetry-window overlap with scored cases; need {dev_count}")
    dev_ids = rng.sample(dev_candidates, dev_count)
    scored_ids.sort()
    dev_ids.sort()
    if set(scored_ids) & set(dev_ids):
        raise AssertionError("scored and development sets overlap")
    if any(intervals_overlap(analysis_interval(by_id[case_id]), scored)
           for case_id in dev_ids for scored in scored_intervals):
        raise AssertionError("development telemetry interval overlaps a scored interval")

    public_manifests = {
        "scored": [public_case(by_id[case_id], "scored") for case_id in scored_ids],
        "dev": [public_case(by_id[case_id], "dev") for case_id in dev_ids],
    }
    manifest_hashes = {split: jsonl_digest(rows) for split, rows in public_manifests.items()}
    frozen = None
    if freeze_path:
        source_config = json.loads((ROOT / "config/telecom/source.json").read_text())
        metadata_audit_path = audit_dir / "telecom_metadata_audit.json"
        eligibility_path = audit_dir / "metric_app_audit_and_eligibility.json"
        metadata_audit = json.loads(metadata_audit_path.read_text()) if metadata_audit_path.exists() else {}
        eligibility_audit = json.loads(eligibility_path.read_text()) if eligibility_path.exists() else {}
        frozen = {
        "dataset": "OpenRCA Telecom",
        "source": {"official_repository": source_config["official_repository"],
                   "official_repository_revision": source_config["official_repository_revision"],
                   "metadata_mirror": source_config["metadata_mirror"],
                   "metadata_file_sha256": {name: info["sha256"] for name, info in
                                             metadata_audit.get("source_files", {}).items()}},
        "selection": {"seed": seed, "candidate_incidents": len(public),
                      "eligible_incidents_after_metric_coverage": len(candidates),
                      "stratification": "ground-truth reason set; exactly 2 scored incidents per each of 5 strata",
                      "scored_count": len(scored_ids), "development_count": len(dev_ids),
                      "scored_ids": scored_ids, "development_ids": dev_ids,
                      "development_overlap_policy": "exclude any development [alert_start - 30m, alert_end) interval that overlaps a scored interval; half-open endpoint adjacency is allowed"},
        "metric_app_eligibility": {"baseline": "[alert_start - 30m, alert_start)",
                                   "alert": "[alert_start, alert_end)",
                                   "minimum_unique_timestamps_per_window": eligibility_audit.get("minimum_unique_timestamps_per_window", 24),
                                   "coverage_uses_unique_timestamps": True,
                                   "repeated_rows_preserved": True},
        "timezone": "Asia/Shanghai", "record_timestamp_unit": "seconds",
        "telemetry_timestamp_unit": "milliseconds",
        "alert_kpi": {"status": "No fixed KPI is supplied by the OpenRCA Telecom query text. The alert contains only the supplied 30-minute failure window; KPI selection is deferred to telemetry-backed development analysis.",
                       "alert_construction": "normalized from the supplied query instruction time window; scoring_points omitted"},
        "prepared_public_manifest_sha256": manifest_hashes,
        "public_manifest_paths": {"scored": "data/prepared/telecom/scored_incidents.jsonl",
                                  "dev": "data/prepared/telecom/dev_incidents.jsonl"},
        "scored_cases": [{key: row[key] for key in ("incident_id", "alert_window_start_local", "alert_window_end_local", "alert_text", "telemetry_date", "timezone")}
                         for row in public_manifests["scored"]],
        "development_cases": [{key: row[key] for key in ("incident_id", "alert_window_start_local", "alert_window_end_local", "alert_text", "telemetry_date", "timezone")}
                               for row in public_manifests["dev"]],
        }
    if freeze_path and freeze_path.exists() and not overwrite_freeze:
        existing = json.loads(freeze_path.read_text())
        if existing != frozen:
            raise RuntimeError(f"frozen sample differs from existing {freeze_path}; use explicit --overwrite-freeze only after review")
    elif freeze_path:
        freeze_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = freeze_path.with_suffix(freeze_path.suffix + ".tmp")
        temporary.write_text(json.dumps(frozen, indent=2) + "\n")
        temporary.replace(freeze_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    evaluator_dir = output_dir / "evaluator"
    evaluator_dir.mkdir(parents=True, exist_ok=True)
    for name, ids in (("scored", scored_ids), ("dev", dev_ids)):
        write_jsonl(output_dir / f"{name}_incidents.jsonl", public_manifests[name])
        write_jsonl(evaluator_dir / f"{name}_ground_truth.jsonl",
                    ({"incident_id": case_id, "ground_truth": gold[case_id], "split": name}
                     for case_id in ids))
    counts = {stratum: {"population": len(ids),
                        "scored": sum(case_id in scored_ids for case_id in ids),
                        "dev": sum(case_id in dev_ids for case_id in ids)}
              for stratum, ids in sorted(strata.items())}
    report = {"seed": seed, "stratification": "ground-truth reason set per physical alert window",
              "scored_per_reason_stratum": scored_per_reason, "development_count": dev_count,
              "development_overlap_policy": "exclude any development [alert_start - 30m, alert_end) interval that overlaps a scored interval; half-open endpoint adjacency is allowed",
              "dev_candidates_before_overlap_exclusion": len(remainder),
              "dev_candidates_after_overlap_exclusion": len(dev_candidates),
              "eligible_candidate_count": len(candidates), "scored_count": len(scored_ids),
              "development_count_actual": len(dev_ids), "strata": counts,
              "scored_ids": scored_ids, "development_ids": dev_ids,
              "scored_manifest": str(output_dir / "scored_incidents.jsonl"),
              "dev_manifest": str(output_dir / "dev_incidents.jsonl"),
              "public_manifest_sha256": manifest_hashes,
              "frozen_config": str(freeze_path) if freeze_path else None,
              "gold_separate": str(evaluator_dir)}
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / "telecom_sample_selection.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def numeric_median(rows: list[dict[str, str]], field: str) -> float | None:
    values = []
    for row in rows:
        try:
            value = float(row[field])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isfinite(value):
            values.append(value)
    if not values:
        return None
    values.sort()
    middle = len(values) // 2
    return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


def audit_metric_app(telemetry_root: Path, public_cases_path: Path, audit_dir: Path,
                     min_unique_timestamps: int = 24) -> dict[str, Any]:
    """Audit metric_app without deduplicating its repeated service/time keys."""
    cases = [json.loads(line) for line in public_cases_path.read_text().splitlines() if line.strip()]
    date_reports: dict[str, dict[str, Any]] = {}
    date_rows: dict[str, list[dict[str, str]]] = {}
    for path in sorted(telemetry_root.glob("*/metric/metric_app.csv")):
        date = path.parent.parent.name
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            fields = tuple(reader.fieldnames or ())
            required = ("serviceName", "startTime", "avg_time", "num", "succee_num", "succee_rate")
            if fields != required:
                raise ValueError(f"{path}: expected documented metric_app schema {required}, got {fields}")
            rows = list(reader)
        times: list[int] = []
        missing = Counter()
        key_counts = Counter()
        exact_counts = Counter()
        for row in rows:
            for field in fields:
                if not row[field].strip():
                    missing[field] += 1
            timestamp_ms = int(row["startTime"])
            times.append(timestamp_ms)
            key_counts[(row["serviceName"], row["startTime"])] += 1
            exact_counts[tuple(row[field] for field in fields)] += 1
            local_day = datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).astimezone(LOCAL_ZONE).strftime("%Y_%m_%d")
            if local_day != date:
                raise ValueError(f"{path}: timestamp {timestamp_ms} resolves to {local_day}, not directory {date}")
        if not rows:
            raise ValueError(f"{path}: empty metric_app file")
        unique_times = sorted(set(times))
        duplicate_keys = sum(count - 1 for count in key_counts.values() if count > 1)
        duplicate_exact = sum(count - 1 for count in exact_counts.values() if count > 1)
        date_rows[date] = rows
        date_reports[date] = {
            "path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size,
            "rows": len(rows), "unique_startTime_ms": len(unique_times),
            "duplicate_service_timestamp_keys": duplicate_keys,
            "duplicate_exact_rows": duplicate_exact,
            "missing_by_column": {field: missing[field] for field in fields},
            "timestamp_unit": "milliseconds", "timestamp_min_ms": min(times),
            "timestamp_max_ms": max(times), "timestamp_min_local": datetime.fromtimestamp(min(times) / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat(),
            "timestamp_max_local": datetime.fromtimestamp(max(times) / 1000, timezone.utc).astimezone(LOCAL_ZONE).isoformat(),
            "service_names": sorted({row["serviceName"] for row in rows}),
        }

    per_case = []
    eligible_ids = set()
    for case in cases:
        date = case["telemetry_date"]
        rows = date_rows.get(date, [])
        if not rows:
            per_case.append({"incident_id": case["incident_id"], "telemetry_date": date,
                             "eligible": False, "exclusion": "metric_app file unavailable"})
            continue
        start = datetime.fromisoformat(case["alert_window_start_local"])
        end = datetime.fromisoformat(case["alert_window_end_local"])
        baseline_start = start - timedelta(minutes=30)
        baseline_rows, alert_rows = [], []
        baseline_times, alert_times = set(), set()
        for row in rows:
            sample_time = datetime.fromtimestamp(int(row["startTime"]) / 1000, timezone.utc).astimezone(LOCAL_ZONE)
            if baseline_start <= sample_time < start:
                baseline_rows.append(row)
                baseline_times.add(int(row["startTime"]))
            if start <= sample_time < end:
                alert_rows.append(row)
                alert_times.add(int(row["startTime"]))
        eligible = (len(baseline_times) >= min_unique_timestamps and
                    len(alert_times) >= min_unique_timestamps)
        if eligible:
            eligible_ids.add(case["incident_id"])
        contrast = {}
        for field in ("avg_time", "succee_rate", "num", "succee_num"):
            before, during = numeric_median(baseline_rows, field), numeric_median(alert_rows, field)
            contrast[field] = {"baseline_median_raw_rows": before,
                               "alert_window_median_raw_rows": during,
                               "absolute_delta": (during - before) if before is not None and during is not None else None,
                               "relative_delta": ((during - before) / abs(before)) if before not in (None, 0) and during is not None else None}
        per_case.append({"incident_id": case["incident_id"], "telemetry_date": date,
                         "baseline_window_start_local": baseline_start.isoformat(timespec="seconds"),
                         "baseline_window_end_local": start.isoformat(timespec="seconds"),
                         "alert_window_start_local": case["alert_window_start_local"],
                         "alert_window_end_local": case["alert_window_end_local"],
                         "baseline_row_count": len(baseline_rows),
                         "baseline_unique_timestamps": len(baseline_times),
                         "alert_row_count": len(alert_rows), "alert_unique_timestamps": len(alert_times),
                         "eligible": eligible,
                         "exclusion": None if eligible else "fewer than minimum unique app timestamps in baseline or alert",
                         "kpi_contrasts": contrast})

    audit_dir.mkdir(parents=True, exist_ok=True)
    eligibility = {"source": "metric_app.csv only", "timestamp_field": "startTime",
                   "timestamp_unit": "milliseconds", "timezone": "Asia/Shanghai",
                   "baseline_window": "[alert_start - 30 min, alert_start)",
                   "alert_window": "[alert_start, alert_end)",
                   "minimum_unique_timestamps_per_window": min_unique_timestamps,
                   "expected_nominal_cadence": "about one minute; threshold is 80% of the 30 expected minute timestamps",
                   "aggregation_note": "All raw rows are retained. Repeated (serviceName,startTime) rows are counted and preserved; coverage uses unique timestamps, while the exploratory medians use all nonmissing raw rows and are not used to rank, filter, or choose incidents.",
                   "date_files": date_reports,
                   "eligible_incident_ids": sorted(eligible_ids),
                   "ineligible_incident_ids": sorted({x["incident_id"] for x in per_case if not x["eligible"]}),
                   "candidate_count": len(cases), "eligible_count": len(eligible_ids),
                   "per_incident": per_case}
    (audit_dir / "metric_app_audit_and_eligibility.json").write_text(json.dumps(eligibility, indent=2) + "\n")
    (audit_dir / "eligible_incident_ids.json").write_text(json.dumps(sorted(eligible_ids), indent=2) + "\n")
    return eligibility


def run_sample(public_path: Path, gold_path: Path, eligible_path: Path,
               output_dir: Path, audit_dir: Path, seed: int = 42,
               freeze_path: Path | None = None, overwrite_freeze: bool = False) -> dict[str, Any]:
    public, gold = load_candidates(public_path, gold_path)
    eligible = set(json.loads(eligible_path.read_text()))
    return sample_cases(public, gold, eligible, output_dir, audit_dir, seed=seed,
                        freeze_path=freeze_path, overwrite_freeze=overwrite_freeze)
