"""Leak-separated Bank metadata preparation and metric_app coverage sampling."""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import random
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
LOCAL_ZONE = ZoneInfo("Asia/Shanghai")
WINDOW_SECONDS = 30 * 60
SOURCE_CONFIG = ROOT / "config/openrca_bank_source.json"
RECORD_FIELDS = ("level", "component", "timestamp", "datetime", "reason")
QUERY_FIELDS = ("task_index", "instruction", "scoring_points")
MONTHS = {name.lower(): i for i, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"), start=1)}
DATE_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4})\b", re.IGNORECASE)
TIME_RANGE_RE = re.compile(
    r"\b(?:(?:within|during) the time range of|between(?: the time range of)?|from)\s+"
    r"(?P<start>\d{1,2}:\d{2})"
    r"\s+(?:to|and)\s+(?:(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4},?\s*(?:at\s*)?)?(?P<end>\d{1,2}:\d{2})\b", re.IGNORECASE)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_blob_sha1(path: Path) -> str:
    size = path.stat().st_size
    digest = hashlib.sha1()
    digest.update(f"blob {size}\0".encode("ascii"))
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def jsonl_digest(rows: Iterable[dict[str, Any]]) -> str:
    content = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":")) + "\n" for row in rows)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")) + "\n")


def read_csv(path: Path, expected_header: Iterable[str]) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != tuple(expected_header):
            raise ValueError(f"{path}: expected header {tuple(expected_header)}, got {reader.fieldnames}")
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f"{path}: malformed row width")
    return rows


def parse_query_window(instruction: str) -> tuple[datetime, datetime]:
    """Parse only the public Bank query window; scoring_points are never used."""
    date_match = DATE_RE.search(instruction)
    time_match = TIME_RANGE_RE.search(instruction)
    if not date_match or not time_match:
        raise ValueError("could not parse public Bank query date/time interval")
    date = datetime(int(date_match["year"]), MONTHS[date_match["month"].casefold()], int(date_match["day"]))
    start_clock = datetime.strptime(time_match["start"], "%H:%M").time()
    end_clock = datetime.strptime(time_match["end"], "%H:%M").time()
    start = date.replace(hour=start_clock.hour, minute=start_clock.minute, tzinfo=LOCAL_ZONE)
    end = date.replace(hour=end_clock.hour, minute=end_clock.minute, tzinfo=LOCAL_ZONE)
    if end <= start:
        end += timedelta(days=1)
    if (end - start).total_seconds() != WINDOW_SECONDS:
        raise ValueError("Bank query windows must be exactly 30 minutes")
    return start, end


def bank_case_id(start: datetime) -> str:
    return "bank-" + start.strftime("%Y%m%dT%H%M") + "-utc8"


def _epoch_ms(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def _case_public(case: dict[str, Any], split: str | None = None) -> dict[str, Any]:
    public = {key: value for key, value in case.items() if not key.startswith("_")}
    if split:
        public["split"] = split
    return public


def _case_gold(case: dict[str, Any], split: str | None = None) -> dict[str, Any]:
    output = {"incident_id": case["incident_id"], "ground_truth": case["_gold"]}
    if split:
        output["split"] = split
    return output


def prepare_metadata(raw_root: Path, prepared_root: Path, audit_root: Path) -> dict[str, Any]:
    source_config = json.loads(SOURCE_CONFIG.read_text())
    upstream_units = source_config["time_units"]
    empirical_by_date = source_config.get("empirical_timestamp_units_by_source_date", {}).get("dates", {})
    record_path = raw_root / "record.csv"
    query_path = raw_root / "query.csv"
    records = read_csv(record_path, RECORD_FIELDS)
    query_rows: list[tuple[str, tuple[datetime, datetime]]] = []
    with query_path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if tuple(header or ()) != QUERY_FIELDS:
            raise ValueError(f"{query_path}: expected query columns {QUERY_FIELDS}, got {header}")
        for line_no, row in enumerate(reader, start=2):
            if len(row) != len(QUERY_FIELDS):
                raise ValueError(f"{query_path}:{line_no}: malformed row width")
            task_index = row[0].strip()
            if not task_index:
                raise ValueError(f"{query_path}:{line_no}: task_index must be nonempty")
            # row[2] (scoring_points) is intentionally not inspected or copied.
            query_rows.append((task_index, parse_query_window(row[1])))
    if len(records) != len(query_rows):
        raise ValueError(f"record/query row count differs: {len(records)} vs {len(query_rows)}")
    task_indices = [item[0] for item in query_rows]

    grouped: dict[str, dict[str, Any]] = {}
    exact_record_rows: set[tuple[str, ...]] = set()
    duplicate_record_count = 0
    onset_outside_window = 0
    timestamp_datetime_mismatch = 0
    query_to_cases: list[tuple[int, str, datetime, datetime]] = []
    for index, (record, (task_index, (start, end))) in enumerate(zip(records, query_rows)):
        try:
            timestamp_value = Decimal(record["timestamp"])
            timestamp_s = int(timestamp_value)
            if timestamp_value != timestamp_s:
                raise ValueError("epoch seconds field is fractional")
            fault_local = datetime.fromtimestamp(timestamp_s, timezone.utc).astimezone(LOCAL_ZONE)
            written_local = datetime.strptime(record["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_ZONE)
        except (ValueError, InvalidOperation, OverflowError) as exc:
            raise ValueError(f"record row {index + 2}: invalid epoch seconds or local datetime") from exc
        if fault_local != written_local:
            timestamp_datetime_mismatch += 1
        if not (start <= fault_local < end):
            onset_outside_window += 1
        case_id = bank_case_id(start)
        query_to_cases.append((task_index, case_id, start, end))
        case = grouped.setdefault(case_id, {
            "incident_id": case_id,
            "alert_window_start_local": start.isoformat(timespec="seconds"),
            "alert_window_end_local": end.isoformat(timespec="seconds"),
            "analysis_window_start_local": (start - timedelta(minutes=30)).isoformat(timespec="seconds"),
            "analysis_window_end_local": end.isoformat(timespec="seconds"),
            "analysis_window_start_epoch_ms_utc": _epoch_ms(start - timedelta(minutes=30)),
            "alert_window_start_epoch_ms_utc": _epoch_ms(start),
            "alert_window_end_epoch_ms_utc": _epoch_ms(end),
            "timezone": "Asia/Shanghai",
            "telemetry_dates": sorted({(start - timedelta(minutes=30)).strftime("%Y_%m_%d"),
                                        (end - timedelta(milliseconds=1)).strftime("%Y_%m_%d")}),
            "telemetry_date": start.strftime("%Y_%m_%d"),
            "timestamp_unit_in_source_files": {relative: upstream_units[f"{relative}.timestamp"]
                                                for relative in TABLE_SPECS},
            "upstream_timestamp_unit_provenance": source_config["official_source"]["schema_document"],
            "source_date_timestamp_units": {
                date: dict(empirical_by_date[date]["units"])
                for date in sorted({(start - timedelta(minutes=30)).strftime("%Y_%m_%d"),
                                    (end - timedelta(milliseconds=1)).strftime("%Y_%m_%d")})
                if date in empirical_by_date},
            "source_date_timestamp_unit_evidence": {
                date: dict(empirical_by_date[date].get("evidence", {}))
                for date in sorted({(start - timedelta(minutes=30)).strftime("%Y_%m_%d"),
                                    (end - timedelta(milliseconds=1)).strftime("%Y_%m_%d")})
                if date in empirical_by_date},
            "alert_text": ("A single failure was reported in the Bank system during the interval "
                           f"{start.strftime('%Y-%m-%d %H:%M:%S')} to "
                           f"{end.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8). The exact cause and "
                           "affected component are unknown. Diagnose using available telemetry."),
            "_gold": [], "_source_task_indices": [], "_strata": set(), "_single_fault": False,
        })
        case["_source_task_indices"].append(task_index)
        key = tuple(record[field].strip() for field in RECORD_FIELDS)
        if key in exact_record_rows:
            duplicate_record_count += 1
            continue
        exact_record_rows.add(key)
        case["_gold"].append({
            "level": record["level"].strip(), "component": record["component"].strip(),
            "timestamp_epoch_seconds": timestamp_s,
            "timestamp_local": fault_local.isoformat(timespec="seconds"),
            "reason": record["reason"].strip(),
        })
        case["_strata"].add(record["reason"].strip())

    cases = sorted(grouped.values(), key=lambda case: case["incident_id"])
    # Only a single distinct record/cause per physical query window is eligible
    # for this one-component scoring pilot; all records remain in the private gold.
    for case in cases:
        case["_single_fault"] = len(case["_gold"]) == 1
    if timestamp_datetime_mismatch or onset_outside_window:
        raise ValueError("Bank record/query time audit failed: see private audit counters before sampling")
    overlaps = 0
    interval_rows = sorted((row[2], row[3], row[1]) for row in query_to_cases)
    for i, (left_start, left_end, left_id) in enumerate(interval_rows):
        for right_start, right_end, right_id in interval_rows[i + 1:]:
            if right_start >= left_end:
                break
            if left_id != right_id and left_start < right_end and right_start < left_end:
                overlaps += 1
    public_cases = [_case_public(case) for case in cases]
    candidate_gold = [_case_gold(case) for case in cases]
    prepared_root.mkdir(parents=True, exist_ok=True)
    write_jsonl(prepared_root / "candidate_incidents.jsonl", public_cases)
    write_jsonl(prepared_root / "evaluator/candidate_ground_truth.jsonl", candidate_gold)
    required_dates = sorted({date for case in cases for date in case["telemetry_dates"]})
    audit_root.mkdir(parents=True, exist_ok=True)
    (audit_root / "bank_required_telemetry_dates.json").write_text(
        json.dumps({"dates": required_dates, "count": len(required_dates),
                    "basis": "query.csv instruction windows plus 30-minute reference windows"}, indent=2) + "\n")
    report = {
        "record_rows": len(records), "query_rows": len(query_rows),
        "distinct_query_task_indices": len(set(task_indices)),
        "duplicate_query_task_index_rows": len(task_indices) - len(set(task_indices)),
        "physical_window_count": len(cases), "duplicate_query_window_pairs": len(query_rows) - len(cases),
        "exact_duplicate_record_rows": duplicate_record_count,
        "multi_record_physical_windows": sum(len(case["_gold"]) > 1 for case in cases),
        "single_record_physical_windows": sum(case["_single_fault"] for case in cases),
        "query_window_overlap_pairs": overlaps,
        "record_datetime_mismatches": timestamp_datetime_mismatch,
        "record_onsets_outside_public_query_window": onset_outside_window,
        "public_metadata_file_hashes": {name: {"sha256": sha256_file(raw_root / name),
                                                "size_bytes": (raw_root / name).stat().st_size,
                                                "git_blob_sha1": git_blob_sha1(raw_root / name)}
                                         for name in ("record.csv", "query.csv")},
        "candidate_public_path": str(prepared_root / "candidate_incidents.jsonl"),
        "candidate_gold_private_path": str(prepared_root / "evaluator/candidate_ground_truth.jsonl"),
        "required_telemetry_dates_path": str(audit_root / "bank_required_telemetry_dates.json"),
        "timezone": "Asia/Shanghai", "record_timestamp_unit": "seconds",
        "telemetry_source_units": json.loads(SOURCE_CONFIG.read_text())["time_units"],
        "public_alert_policy": "Only query-window times and generic Bank alert wording; task_index, scoring_points, record labels, exact onset and component are omitted.",
        "pilot_eligibility_policy": "A physical query window with exactly one distinct gold record may enter the single-component scored pilot; multi-record windows stay in private candidate gold and are excluded from that pilot.",
        "reference_note": "The preceding 30-minute reference interval is coverage-eligible, not guaranteed healthy.",
    }
    (audit_root / "bank_metadata_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def audit_metric_app(metric_root: Path, public_cases_path: Path, audit_root: Path,
                     *, coverage_fraction: float = 0.8) -> dict[str, Any]:
    """Audit all Bank app files and compute label-blind query-window coverage."""
    public = [json.loads(line) for line in public_cases_path.read_text().splitlines() if line.strip()]
    required_dates = sorted({date for case in public for date in case["telemetry_dates"]})
    date_rows: dict[str, list[dict[str, str]]] = {}
    date_summaries: dict[str, dict[str, Any]] = {}
    cadence_deltas = []
    all_metric_rows: list[dict[str, str]] = []
    expected_header = ("timestamp", "rr", "sr", "cnt", "mrt", "tc")
    for date in required_dates:
        path = metric_root / date / "metric" / "metric_app.csv"
        if not path.is_file():
            date_rows[date] = None
            date_summaries[date] = {"missing_source_file": True,
                                    "source_path": str(path),
                                    "rows": 0, "bytes": 0}
            continue
        rows = read_csv(path, expected_header)
        timestamps, per_service, missing = [], defaultdict(set), Counter()
        tc_time_keys, exact_rows = Counter(), Counter()
        for row in rows:
            try:
                timestamp = int(row["timestamp"])
            except ValueError as exc:
                raise ValueError(f"{path}: timestamp must be epoch seconds") from exc
            local = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(LOCAL_ZONE)
            nominal_date_mismatch = local.strftime("%Y_%m_%d") != date
            timestamps.append(timestamp)
            if row["tc"].strip():
                per_service[row["tc"].strip()].add(timestamp)
                tc_time_keys[(row["tc"].strip(), timestamp)] += 1
            else:
                missing["tc"] += 1
            for field in ("rr", "sr", "cnt", "mrt"):
                value = row[field].strip()
                if value == "":
                    missing[field] += 1
                else:
                    try:
                        number = float(value)
                    except ValueError:
                        missing[field] += 1
                    else:
                        if not math.isfinite(number):
                            missing[field] += 1
            exact_rows[tuple(row[field] for field in expected_header)] += 1
        unique_timestamps = sorted(set(timestamps))
        cadence_deltas.extend(right - left for left, right in zip(unique_timestamps, unique_timestamps[1:])
                              if right > left)
        date_rows[date] = rows
        all_metric_rows.extend(rows)
        counts = Counter(timestamps)
        date_summaries[date] = {
            "rows": len(rows), "bytes": path.stat().st_size, "sha256": sha256_file(path),
            "unique_timestamps": len(unique_timestamps),
            "repeated_global_timestamp_rows": sum(count - 1 for count in counts.values() if count > 1),
            "duplicate_tc_timestamp_key_rows": sum(count - 1 for count in tc_time_keys.values() if count > 1),
            "duplicate_exact_rows": sum(count - 1 for count in exact_rows.values() if count > 1),
            "rows_whose_local_date_differs_from_directory": sum(
                datetime.fromtimestamp(int(row["timestamp"]), timezone.utc).astimezone(LOCAL_ZONE).strftime("%Y_%m_%d") != date
                for row in rows),
            "timestamp_min_epoch_seconds": min(timestamps), "timestamp_max_epoch_seconds": max(timestamps),
            "missing_or_invalid_by_field": dict(sorted(missing.items())),
            "observed_app_entities": sorted(per_service),
        }
    if not cadence_deltas:
        raise ValueError("no distinct Bank metric_app timestamp intervals to estimate cadence")
    sorted_deltas = sorted(cadence_deltas)
    cadence_seconds = sorted_deltas[len(sorted_deltas) // 2]
    if cadence_seconds <= 0:
        raise ValueError("observed Bank metric_app cadence must be positive")
    minimum_unique = max(1, math.ceil((WINDOW_SECONDS / cadence_seconds) * coverage_fraction))
    per_case, eligible_ids = [], set()
    for case in public:
        alert_start = datetime.fromisoformat(case["alert_window_start_local"])
        alert_end = datetime.fromisoformat(case["alert_window_end_local"])
        ref_start = alert_start - timedelta(minutes=30)
        ref_epoch = (_epoch_ms(ref_start) // 1000, _epoch_ms(alert_start) // 1000)
        alert_epoch = (_epoch_ms(alert_start) // 1000, _epoch_ms(alert_end) // 1000)
        ref_timestamps, alert_timestamps = set(), set()
        ref_rows, alert_rows = [], []
        missing_dates = [date for date in case["telemetry_dates"] if date_rows.get(date) is None]
        for row in all_metric_rows:
            timestamp = int(row["timestamp"])
            if ref_epoch[0] <= timestamp < ref_epoch[1]:
                ref_timestamps.add(timestamp)
                ref_rows.append(row)
            if alert_epoch[0] <= timestamp < alert_epoch[1]:
                alert_timestamps.add(timestamp)
                alert_rows.append(row)
        # The named date directory can end a few minutes into the following
        # UTC+8 day; coverage follows actual timestamps across all downloaded
        # source files rather than assuming directory-date boundaries.
        eligible = (len(ref_timestamps) >= minimum_unique
                    and len(alert_timestamps) >= minimum_unique)
        if eligible:
            eligible_ids.add(case["incident_id"])
        window_rows = {"reference": ref_rows, "alert": alert_rows}
        window_status = {}
        for name, rows in window_rows.items():
            field_status = {}
            for field in ("rr", "sr", "cnt", "mrt"):
                finite = 0
                missing_or_invalid = 0
                for row in rows:
                    try:
                        number = float(row[field])
                    except (ValueError, TypeError):
                        missing_or_invalid += 1
                    else:
                        if math.isfinite(number):
                            finite += 1
                        else:
                            missing_or_invalid += 1
                field_status[field] = {"finite_values": finite,
                                       "missing_or_invalid_values": missing_or_invalid}
            field_status["tc"] = {"distinct_values": len({row["tc"].strip() for row in rows if row["tc"].strip()}),
                                  "missing_rows": sum(not row["tc"].strip() for row in rows)}
            key_counts = Counter((row["tc"].strip(), row["timestamp"]) for row in rows)
            exact_counts = Counter(tuple(row[field] for field in expected_header) for row in rows)
            window_status[name] = {"rows": len(rows), "fields": field_status,
                                   "duplicate_tc_timestamp_key_rows": sum(count - 1 for count in key_counts.values() if count > 1),
                                   "duplicate_exact_rows": sum(count - 1 for count in exact_counts.values() if count > 1)}
        per_case.append({"incident_id": case["incident_id"],
                         "reference_unique_timestamps": len(ref_timestamps),
                         "alert_unique_timestamps": len(alert_timestamps),
                         "window_data_audit": window_status,
                         "minimum_unique_timestamps_per_window": minimum_unique,
                         "missing_telemetry_dates": missing_dates,
                         "eligible_by_metric_app_coverage": eligible})
    audit_root.mkdir(parents=True, exist_ok=True)
    result = {
        "source_table": "metric_app.csv", "header": list(expected_header),
        "timestamp_field": "timestamp", "timestamp_unit": "epoch seconds",
        "timezone": "Asia/Shanghai", "expected_cadence_seconds_global_median": cadence_seconds,
        "coverage_fraction": coverage_fraction,
        "coverage_threshold": "unique timestamp count >= ceil(30min / global median positive timestamp delta * 0.8) in each half-open window",
        "baseline_window": "[alert_start - 30m, alert_start)", "alert_window": "[alert_start, alert_end)",
        "reference_health": "Coverage eligibility does not imply healthy behavior; reference may contain adjacent/ongoing faults.",
        "date_files": date_summaries, "candidate_count": len(public),
        "eligible_count": len(eligible_ids), "eligible_incident_ids": sorted(eligible_ids),
        "ineligible_incident_ids": sorted(set(case["incident_id"] for case in public) - eligible_ids),
        "per_incident_coverage": per_case,
    }
    (audit_root / "bank_metric_app_coverage.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (audit_root / "bank_metric_app_eligible_ids.json").write_text(json.dumps(sorted(eligible_ids), indent=2) + "\n")
    return result


def sample_bank_cases(public_path: Path, gold_path: Path, eligible_path: Path,
                      prepared_root: Path, audit_root: Path, *, seed: int = 42,
                      scored_count: int = 10, dev_count: int = 2,
                      freeze_path: Path | None = None,
                      overwrite_freeze: bool = False) -> dict[str, Any]:
    public = [json.loads(line) for line in public_path.read_text().splitlines() if line.strip()]
    gold_rows = [json.loads(line) for line in gold_path.read_text().splitlines() if line.strip()]
    gold = {row["incident_id"]: row["ground_truth"] for row in gold_rows}
    if len(gold) != len(gold_rows) or set(gold) != {row["incident_id"] for row in public}:
        raise ValueError("Bank public candidate manifest and private gold IDs differ")
    eligible_ids = set(json.loads(eligible_path.read_text()))
    by_id = {row["incident_id"]: row for row in public}
    eligible_cases = [case_id for case_id in sorted(eligible_ids)
                      if case_id in by_id and len(gold[case_id]) == 1]
    strata: dict[str, list[str]] = defaultdict(list)
    for case_id in eligible_cases:
        strata[gold[case_id][0]["reason"]].append(case_id)
    if len(eligible_cases) < scored_count + dev_count:
        raise ValueError(f"only {len(eligible_cases)} single-record coverage-eligible Bank windows; need {scored_count + dev_count}")
    supported = {reason: ids for reason, ids in strata.items() if ids}
    if len(supported) > scored_count:
        chosen_reasons = sorted(supported, key=lambda reason: (-len(supported[reason]), reason.casefold()))[:scored_count]
    else:
        chosen_reasons = sorted(supported, key=str.casefold)
    if len(chosen_reasons) == 0:
        raise ValueError("no single-record fault-reason strata have eligible Bank windows")
    quota = {reason: 1 for reason in chosen_reasons}
    seats_left = scored_count - len(chosen_reasons)
    chosen_total = sum(len(supported[reason]) for reason in chosen_reasons)
    for _ in range(seats_left):
        available = [reason for reason in chosen_reasons if quota[reason] < len(supported[reason])]
        if not available:
            raise ValueError("eligible Bank reason strata cannot fill ten scored windows")
        # Proportional deterministic allocation, with reason-string tie break.
        reason = min(available, key=lambda r: (
            -(scored_count * len(supported[r]) / chosen_total - quota[r]), r.casefold()))
        quota[reason] += 1
    rng = random.Random(seed)
    scored_ids = []
    for reason in chosen_reasons:
        if len(supported[reason]) < quota[reason]:
            raise ValueError("reason stratum lacks coverage-eligible cases for fixed quota")
        scored_ids.extend(rng.sample(sorted(supported[reason]), quota[reason]))
    if len(scored_ids) != scored_count:
        raise AssertionError("Bank sample did not produce the requested scored count")

    def analysis_interval(case_id: str) -> tuple[datetime, datetime]:
        alert_start = datetime.fromisoformat(by_id[case_id]["alert_window_start_local"])
        alert_end = datetime.fromisoformat(by_id[case_id]["alert_window_end_local"])
        return alert_start - timedelta(minutes=30), alert_end

    scored_intervals = [analysis_interval(case_id) for case_id in scored_ids]
    def overlaps(left, right):
        return left[0] < right[1] and right[0] < left[1]
    dev_candidates = [case_id for case_id in eligible_cases if case_id not in scored_ids
                      and not any(overlaps(analysis_interval(case_id), item) for item in scored_intervals)]
    if len(dev_candidates) < dev_count:
        raise ValueError(f"only {len(dev_candidates)} disjoint development candidates remain; need {dev_count}")
    dev_ids = rng.sample(sorted(dev_candidates), dev_count)
    scored_ids.sort()
    dev_ids.sort()
    scored_set, dev_set = set(scored_ids), set(dev_ids)
    if scored_set & dev_set:
        raise AssertionError("Bank scored/development incident IDs overlap")
    if any(overlaps(analysis_interval(case_id), analysis_interval(scored_id))
           for case_id in dev_ids for scored_id in scored_ids):
        raise AssertionError("Bank development/scored full telemetry intervals overlap")
    manifests = {"scored": [_case_public(by_id[case_id], "scored") for case_id in scored_ids],
                 "dev": [_case_public(by_id[case_id], "dev") for case_id in dev_ids]}
    hashes = {split: jsonl_digest(rows) for split, rows in manifests.items()}
    source_doc = json.loads(SOURCE_CONFIG.read_text())
    source_doc = {**source_doc,
                  "source_config_sha256": sha256_file(SOURCE_CONFIG),
                  "metadata_audit_sha256": sha256_file(audit_root / "bank_metadata_audit.json"),
                  "metric_app_coverage_audit_sha256": sha256_file(audit_root / "bank_metric_app_coverage.json"),
                  "metric_app_file_sha256_by_date": {
                      date: details["sha256"]
                      for date, details in json.loads((audit_root / "bank_metric_app_coverage.json").read_text())["date_files"].items()
                      if details.get("sha256")}}
    frozen = {
        "dataset": "OpenRCA Bank", "source": source_doc,
        "selection": {"seed": seed, "candidate_physical_windows": len(public),
                      "coverage_eligible_single_record_windows": len(eligible_cases),
                      "scored_count": scored_count, "development_count": dev_count,
                      "stratification_policy": "Seeded sampling within eligible single-record fault-reason strata. Include every supported reason when at most ten; otherwise choose ten highest-count reasons, tie-break by reason text. Start one scored slot per chosen reason; allocate remaining slots to the most under-allocated supported reason deterministically. Reason labels are private and never written to public manifests.",
                      "private_reason_stratum_count": len(chosen_reasons),
                      "scored_ids": scored_ids, "development_ids": dev_ids,
                      "split_disjointness": "development full input interval [alert_start-30m, alert_end) must not overlap any scored interval; half-open endpoints allowed"},
        "coverage": {"source_table": "metric_app.csv", "coverage_audit": "artifacts/data_audit/bank_metric_app_coverage.json",
                     "note": "Coverage eligibility is based on unique public metric_app timestamp buckets only and does not imply a healthy baseline."},
        "timezone": "Asia/Shanghai", "record_epoch_unit": "seconds",
        "upstream_declared_telemetry_time_units": source_doc["time_units"],
        "empirical_timestamp_units_by_source_date": source_doc.get("empirical_timestamp_units_by_source_date", {}),
        "prepared_public_manifest_sha256": hashes,
        "public_manifest_paths": {"scored": "data/prepared/bank/scored_incidents.jsonl",
                                  "dev": "data/prepared/bank/dev_incidents.jsonl"},
        "scored_cases": [{key: case[key] for key in ("incident_id", "alert_window_start_local",
                         "alert_window_end_local", "alert_text", "telemetry_dates", "timezone")}
                         for case in manifests["scored"]],
        "development_cases": [{key: case[key] for key in ("incident_id", "alert_window_start_local",
                            "alert_window_end_local", "alert_text", "telemetry_dates", "timezone")}
                              for case in manifests["dev"]],
    }
    if freeze_path and freeze_path.exists() and not overwrite_freeze:
        if json.loads(freeze_path.read_text()) != frozen:
            raise FileExistsError(f"frozen Bank sample differs from {freeze_path}; use explicit overwrite only after review")
    elif freeze_path:
        freeze_path.parent.mkdir(parents=True, exist_ok=True)
        temp = freeze_path.with_suffix(freeze_path.suffix + ".tmp")
        temp.write_text(json.dumps(frozen, indent=2, sort_keys=True) + "\n")
        temp.replace(freeze_path)
    prepared_root.mkdir(parents=True, exist_ok=True)
    write_jsonl(prepared_root / "scored_incidents.jsonl", manifests["scored"])
    write_jsonl(prepared_root / "dev_incidents.jsonl", manifests["dev"])
    evaluator = prepared_root / "evaluator"
    write_jsonl(evaluator / "scored_ground_truth.jsonl",
                ({"incident_id": case_id, "ground_truth": gold[case_id], "split": "scored"}
                 for case_id in scored_ids))
    write_jsonl(evaluator / "dev_ground_truth.jsonl",
                ({"incident_id": case_id, "ground_truth": gold[case_id], "split": "dev"}
                 for case_id in dev_ids))
    audit_root.mkdir(parents=True, exist_ok=True)
    report = {"seed": seed, "scored_count": len(scored_ids), "development_count": len(dev_ids),
              "eligible_single_record_count": len(eligible_cases), "private_stratum_count": len(chosen_reasons),
              "quota_count_distribution": sorted(quota.values()),
              "scored_ids": scored_ids, "development_ids": dev_ids,
              "all_development_intervals_disjoint_from_scored": True,
              "prepared_public_manifest_sha256": hashes,
              "frozen_config": str(freeze_path) if freeze_path else None,
              "public_splits": {"scored": str(prepared_root / "scored_incidents.jsonl"),
                                "dev": str(prepared_root / "dev_incidents.jsonl")},
              "private_evaluator": str(evaluator)}
    (audit_root / "bank_sample_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


TABLE_SPECS = {
    "metric/metric_app.csv": {"header": ("timestamp", "rr", "sr", "cnt", "mrt", "tc"),
                               "timestamp_field": "timestamp", "timestamp_unit": "seconds",
                               "id_fields": ("tc",), "numeric_fields": ("rr", "sr", "cnt", "mrt")},
    "metric/metric_container.csv": {"header": ("timestamp", "cmdb_id", "kpi_name", "value"),
                                    "timestamp_field": "timestamp", "timestamp_unit": "seconds",
                                    "id_fields": ("cmdb_id",), "numeric_fields": ("value",)},
    "trace/trace_span.csv": {"header": ("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"),
                             "timestamp_field": "timestamp", "timestamp_unit": "milliseconds",
                             "id_fields": ("cmdb_id",), "numeric_fields": ("duration",)},
    "log/log_service.csv": {"header": ("log_id", "timestamp", "cmdb_id", "log_name", "value"),
                            "timestamp_field": "timestamp", "timestamp_unit": "seconds",
                            "id_fields": ("cmdb_id",), "numeric_fields": ()},
}

BANK_OFFICIAL_CANDIDATE_COMPONENTS = (
    "apache01", "apache02", "Tomcat01", "Tomcat02", "Tomcat04", "Tomcat03",
    "MG01", "MG02", "IG01", "IG02", "Mysql01", "Mysql02", "Redis01", "Redis02",
)


def _source_integrity(path: Path, remote_record: dict[str, Any], remote_path: str) -> dict[str, Any]:
    actual_size = path.stat().st_size
    if actual_size != int(remote_record["bytes"]):
        raise ValueError(f"downloaded Bank source changed size after verification: {remote_path}")
    actual_sha = sha256_file(path)
    if actual_sha != remote_record["sha256"]:
        raise ValueError(f"downloaded Bank source SHA-256 changed: {remote_path}")
    if remote_record.get("hub_lfs_sha256"):
        if actual_sha != remote_record["hub_lfs_sha256"]:
            raise ValueError(f"Bank source LFS SHA-256 mismatch: {remote_path}")
        integrity = "hub_lfs_sha256"
    else:
        actual_git = git_blob_sha1(path)
        if actual_git != remote_record.get("hub_git_blob_sha1"):
            raise ValueError(f"Bank source Git blob SHA-1 mismatch: {remote_path}")
        integrity = "hub_git_blob_sha1"
    return {"size_bytes": actual_size, "sha256": actual_sha,
            "hub_git_blob_sha1": remote_record.get("hub_git_blob_sha1"),
            "hub_lfs_sha256": remote_record.get("hub_lfs_sha256"),
            "integrity": integrity}


def _parse_epoch_ms(raw: str, unit: str, *, path: str, row_number: int) -> int:
    token = raw.strip()
    if token.isdigit():
        integer_value = int(token)
        milliseconds = integer_value * 1000 if unit == "seconds" else integer_value
    else:
        try:
            value = Decimal(token)
        except InvalidOperation as exc:
            raise ValueError(f"{path}: invalid timestamp at row {row_number}") from exc
        milliseconds_decimal = value * 1000 if unit == "seconds" else value
        if milliseconds_decimal != milliseconds_decimal.to_integral_value():
            raise ValueError(f"{path}: timestamp has unsupported fractional {unit} at row {row_number}")
        integer_value = int(value)
        milliseconds = int(milliseconds_decimal)
    if unit == "seconds" and not 1_000_000_000 <= integer_value < 10_000_000_000:
        raise ValueError(f"{path}: timestamp magnitude contradicts the seconds unit at row {row_number}")
    if unit == "milliseconds" and not 1_000_000_000_000 <= integer_value < 10_000_000_000_000:
        raise ValueError(f"{path}: timestamp magnitude contradicts the milliseconds unit at row {row_number}")
    return milliseconds


def _spillover_from_source_day(timestamp_ms: int, day_start_ms: int,
                               day_end_ms: int) -> tuple[str | None, int]:
    """Return before/after spillover in UTC ms using precomputed day bounds."""
    if timestamp_ms < day_start_ms:
        return "before", day_start_ms - timestamp_ms
    if timestamp_ms >= day_end_ms:
        return "after", timestamp_ms - day_end_ms
    return None, 0


def _source_year_bounds_ms(source_date: str) -> tuple[int, int]:
    """Return UTC epoch-ms bounds for the local calendar year of a source date."""
    year = datetime.strptime(source_date, "%Y_%m_%d").year
    start = datetime(year, 1, 1, tzinfo=LOCAL_ZONE)
    end = datetime(year + 1, 1, 1, tzinfo=LOCAL_ZONE)
    return _epoch_ms(start), _epoch_ms(end)


def _finite_value(value: str) -> bool:
    if not value.strip():
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def build_public_catalog(source_config: Mapping[str, Any],
                         observed_entities: Mapping[str, set[str]],
                         prompt_sha256: str) -> dict[str, Any]:
    """Build the label-free deployment catalog consumed by Bank adapters."""
    return {"schema_version": "bank-source-catalog/1",
        "source": {"official_repository": source_config["official_source"]["repository"],
                   "official_revision": source_config["official_source"]["revision"],
                   "prompt_path": source_config["official_source"]["schema_document"],
                   "prompt_sha256": prompt_sha256,
                   "mirror_repository": source_config["working_mirror"]["repository"],
                   "mirror_revision": source_config["working_mirror"]["revision"]},
        "topology": {"known": "Bank components are pod-level; upstream describes horizontal pod traces only.",
                     "relationships": [],
                     "unknown": "No vertical pod/container/service alias map is documented. Preserve each source cmdb_id and tc value without merging."},
        "official_candidate_components": list(BANK_OFFICIAL_CANDIDATE_COMPONENTS),
        "entities": {identifier: {"source_fields": sorted(fields), "relationship": "unknown"}
                     for identifier, fields in sorted(observed_entities.items())}}


def finalize_staged_bank(raw_root: Path, prepared_root: Path, audit_root: Path, *,
                         sample_path: Path = ROOT / "config/bank_sample.json",
                         download_log_path: Path = ROOT / "artifacts/data_audit/bank_downloads.jsonl",
                         progress_path: Path,
                         expected_counts_path: Path | None = None) -> dict[str, Any]:
    """Recover publication from a complete scan and staged clips without rescanning CSV sources."""
    sample = json.loads(sample_path.read_text())
    source_config = json.loads(SOURCE_CONFIG.read_text())
    cases = {row["incident_id"]: row for split in ("scored", "dev")
             for row in (json.loads(line) for line in
                         (prepared_root / f"{split}_incidents.jsonl").read_text().splitlines() if line.strip())}
    frozen_ids = {row["incident_id"] for name in ("scored_cases", "development_cases")
                  for row in sample.get(name, [])}
    if frozen_ids and frozen_ids != set(cases):
        raise ValueError("prepared case IDs differ from the sample freeze")
    public_freezes = sample.get("prepared_public_manifest_sha256", {})
    for split in ("scored", "dev"):
        expected_hash = public_freezes.get(split)
        if not expected_hash or sha256_file(prepared_root / f"{split}_incidents.jsonl") != expected_hash:
            raise ValueError(f"{split} public manifest differs from the sample freeze")
    independent_counts = json.loads(expected_counts_path.read_text()) if expected_counts_path else None
    selected_dates = sorted({date for case in cases.values() for date in case["telemetry_dates"]})
    events = [json.loads(line) for line in progress_path.read_text().splitlines() if line.strip()]
    if not events or events[0].get("event") != "clip_start" or events[0].get("selected_dates") != selected_dates:
        raise ValueError("progress file does not describe these frozen selected dates")
    completed = {}
    for event in events:
        if event.get("event") != "source_file_complete":
            continue
        path = event.get("source_path")
        if path in completed:
            raise ValueError(f"duplicate completed progress record: {path}")
        audit = event.get("unit_validation", {})
        if (event.get("source_rows", 0) <= 0 or audit.get("rows_checked") != event.get("source_rows")
                or audit.get("homogeneous_unit_validation") != "passed_all_rows"
                or audit.get("unit_magnitude_mismatches") != 0
                or audit.get("rows_outside_source_year") != 0):
            raise ValueError(f"incomplete or invalid source scan evidence: {path}")
        completed[path] = event
    expected = {f"Bank/telemetry/{date}/{table}" for date in selected_dates for table in TABLE_SPECS}
    if set(completed) != expected:
        raise ValueError(f"completed source progress set differs: {len(completed)} of {len(expected)}")
    downloads = {}
    for line in download_log_path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            if row.get("integrity"):
                downloads[row["path"]] = row
    verified = {}
    for source_rel in sorted(expected):
        record = completed[source_rel]
        source_path = raw_root / "telemetry" / source_rel.removeprefix("Bank/telemetry/")
        remote = downloads.get(source_rel)
        if remote is None:
            raise ValueError(f"missing pinned download record for {source_rel}")
        integrity = _source_integrity(source_path, remote, source_rel)
        if integrity["size_bytes"] != record["source_size_bytes"]:
            raise ValueError(f"source size differs from complete progress evidence: {source_rel}")
        verified[source_rel] = {**integrity, "record": record}

    stage_root = prepared_root / ".bank_public_stage"
    stage_cases = stage_root / "cases"
    public_root = prepared_root / "public"
    already_published = public_root.is_dir() and (public_root / "cases").is_dir()
    if already_published:
        stage_root, stage_cases = public_root, public_root / "cases"
    elif not stage_cases.is_dir() or (public_root.exists() and any(public_root.iterdir())):
        raise FileExistsError("staged cases missing or public destination already exists")
    clip_records, cases_manifest = [], {}
    clip_counts: dict[str, dict[str, dict[str, dict[str, int]]]] = {}
    observed_entities: dict[str, set[str]] = defaultdict(set)
    expected_stage_paths = {f"{case_id}/source_dates/{date}/{table}"
                            for case_id, case in cases.items()
                            for date in case["telemetry_dates"] for table in TABLE_SPECS}
    actual_stage_paths = {str(path.relative_to(stage_cases)) for path in stage_cases.rglob("*.csv")}
    if actual_stage_paths != expected_stage_paths:
        raise ValueError(f"staged case CSV set differs: expected {len(expected_stage_paths)}, got {len(actual_stage_paths)}")
    if independent_counts is not None:
        independent_counts = independent_counts.get("counts", independent_counts)
        if not isinstance(independent_counts, dict):
            raise ValueError("expected count artifact must be a mapping")
    for case_id, case in cases.items():
        alert = stage_cases / case_id / "alert.json"
        if not alert.is_file() or json.loads(alert.read_text()) != case:
            raise ValueError(f"staged alert differs from frozen public case: {case_id}")
        files = {}
        case_date_counts = {}
        clip_counts[case_id] = {}
        for date in case["telemetry_dates"]:
            for relative, spec in TABLE_SPECS.items():
                rel_key = f"source_dates/{date}/{relative}"
                clip_path = stage_cases / case_id / rel_key
                source_rel = f"Bank/telemetry/{date}/{relative}"
                source = verified[source_rel]
                source_event = source["record"]
                unit = source_event["timestamp_unit"]
                empirical_unit = source_config["empirical_timestamp_units_by_source_date"]["dates"][date]["units"].get(relative)
                alert_unit = case["source_date_timestamp_units"].get(date, {}).get(relative)
                if (unit != source_event.get("unit_validation", {}).get("expected_unit")
                        or unit != empirical_unit or unit != alert_unit):
                    raise ValueError(f"source unit provenance disagrees for {source_rel}")
                rows = 0
                window_rows = Counter()
                with clip_path.open("r", newline="", encoding="utf-8-sig") as stream:
                    reader = csv.DictReader(stream)
                    if tuple(reader.fieldnames or ()) != spec["header"]:
                        raise ValueError(f"staged clip header mismatch: {clip_path}")
                    for row in reader:
                        if None in row or any(value is None for value in row.values()):
                            raise ValueError(f"malformed staged clip row: {clip_path}")
                        timestamp_ms = _parse_epoch_ms(row[spec["timestamp_field"]], unit,
                                                       path=str(clip_path), row_number=rows + 2)
                        if case["analysis_window_start_epoch_ms_utc"] <= timestamp_ms < case["alert_window_start_epoch_ms_utc"]:
                            window_rows["reference"] += 1
                        elif case["alert_window_start_epoch_ms_utc"] <= timestamp_ms < case["alert_window_end_epoch_ms_utc"]:
                            window_rows["alert"] += 1
                        else:
                            raise ValueError(f"staged row falls outside frozen case windows: {clip_path}")
                        rows += 1
                        for field in spec["id_fields"]:
                            if row[field].strip():
                                observed_entities[row[field].strip()].add(f"{relative}:{field}")
                case_date_counts[relative] = dict(window_rows)
                if independent_counts is not None and case_id in independent_counts and relative in independent_counts[case_id]:
                    wanted = independent_counts[case_id][relative]
                    actual = {window: window_rows.get(window, 0) for window in ("reference", "alert")}
                    if actual != wanted:
                        raise ValueError(f"independent source count mismatch for {case_id}/{relative}: {actual} != {wanted}")
                clip_counts[case_id][relative] = {window: window_rows.get(window, 0)
                                                  for window in ("reference", "alert")}
                digest, size = sha256_file(clip_path), clip_path.stat().st_size
                source_units = source_config["empirical_timestamp_units_by_source_date"]["dates"][date]
                audit = source_event["unit_validation"]
                file_info = {"sha256": digest, "rows": rows, "size_bytes": size,
                    "header": list(spec["header"]), "timestamp_unit": unit,
                    "full_source_file_unit_validation": audit,
                    "timestamp_field": spec["timestamp_field"], "source_path": source_rel,
                    "source_sha256": source["sha256"],
                    "source_git_blob_sha1": source.get("hub_git_blob_sha1"),
                    "source_lfs_sha256": source.get("hub_lfs_sha256"),
                    "source_size_bytes": source["size_bytes"], "source_rows": source_event["source_rows"],
                    "unit_evidence": source_units.get("evidence", {}).get(relative, "unknown"),
                    "spillover_before_day_max_ms": audit["max_spillover_before_day_ms"],
                    "spillover_after_day_max_ms": audit["max_spillover_after_day_ms"]}
                files[rel_key] = file_info
                clip_records.append({"case_id": case_id, "relative_path": rel_key,
                    "clip_path": f"data/prepared/bank/public/cases/{case_id}/{rel_key}",
                    "source_date": date, "source_path": source_rel,
                    "source_size_bytes": source["size_bytes"], "source_sha256": source["sha256"],
                    "source_git_blob_sha1": source.get("hub_git_blob_sha1"),
                    "source_lfs_sha256": source.get("hub_lfs_sha256"),
                    "source_rows": source_event["source_rows"], "source_rows_scanned": source_event["source_rows"],
                    "full_source_file_unit_validation": audit, "clip_rows": rows,
                    "clip_size_bytes": size, "clip_sha256": digest, "header": list(spec["header"]),
                    "timestamp_field": spec["timestamp_field"], "timestamp_source_unit": unit,
                    "timestamp_unit_evidence": file_info["unit_evidence"],
                    "reference_window_utc_ms": [case["analysis_window_start_epoch_ms_utc"], case["alert_window_start_epoch_ms_utc"]],
                    "alert_window_utc_ms": [case["alert_window_start_epoch_ms_utc"], case["alert_window_end_epoch_ms_utc"]]})
        cases_manifest[case_id] = {"alert_sha256": sha256_file(alert),
            "windows": {"reference": {"start_epoch_ms_utc": case["analysis_window_start_epoch_ms_utc"],
                "end_epoch_ms_utc": case["alert_window_start_epoch_ms_utc"]},
                "alert": {"start_epoch_ms_utc": case["alert_window_start_epoch_ms_utc"],
                "end_epoch_ms_utc": case["alert_window_end_epoch_ms_utc"]}}, "files": files}
    write_jsonl(stage_root / "clip_manifest.jsonl", clip_records)
    schema_path = ROOT / source_config["official_source"]["schema_document"]
    prompt_hash = sha256_file(schema_path)
    (stage_root / "deployment_catalog.json").write_text(json.dumps(
        build_public_catalog(source_config, observed_entities, prompt_hash), ensure_ascii=False,
        indent=2, sort_keys=True) + "\n")
    input_manifest = {"schema_version": "e4-bank-input-manifest/1",
        "sample_config_sha256": sha256_file(sample_path),
        "candidate_manifest_sha256": sha256_file(prepared_root / "candidate_incidents.jsonl"),
        "scored_manifest_sha256": sha256_file(prepared_root / "scored_incidents.jsonl"),
        "dev_manifest_sha256": sha256_file(prepared_root / "dev_incidents.jsonl"),
        "clip_manifest_sha256": sha256_file(stage_root / "clip_manifest.jsonl"),
        "deployment_catalog_file_sha256": sha256_file(stage_root / "deployment_catalog.json"),
        "deployment_catalog_schema_sha256": prompt_hash,
        "deployment_mapping_source_sha256": prompt_hash, "cases": cases_manifest}
    input_path = ROOT / "config/bank_input_manifest.json"
    if input_path.exists() and json.loads(input_path.read_text()) != input_manifest:
        raise FileExistsError(f"Bank input manifest differs from existing freeze: {input_path}")
    if not already_published:
        stage_root.rename(public_root)
    if not input_path.exists():
        temporary_manifest = input_path.with_name(input_path.name + ".recovery.tmp")
        temporary_manifest.write_text(json.dumps(input_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        temporary_manifest.replace(input_path)
    report = {"selected_case_count": len(cases), "selected_dates": selected_dates,
        "verified_source_file_count": len(verified),
        "verified_source_bytes": sum(item["size_bytes"] for item in verified.values()),
        "clip_manifest_sha256": input_manifest["clip_manifest_sha256"],
        "input_manifest_sha256": sha256_file(input_path), "recovered_from_progress": str(progress_path),
        "independent_counts_file": str(expected_counts_path) if expected_counts_path else None,
        "per_case_table_window_clip_rows": clip_counts,
        "count_audit_provenance": "read-only independent full-source CSV count, native per-file units, no labels"}
    audit_root.mkdir(parents=True, exist_ok=True)
    (audit_root / "bank_selected_clip_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def clip_selected_bank(raw_root: Path, prepared_root: Path, audit_root: Path, *,
                       sample_path: Path = ROOT / "config/bank_sample.json",
                       download_log_path: Path = ROOT / "artifacts/data_audit/bank_downloads.jsonl",
                       overwrite_public: bool = False) -> dict[str, Any]:
    """Publish per-source-date native CSV clips after full file/unit verification."""
    sample = json.loads(sample_path.read_text())
    source_config = json.loads(SOURCE_CONFIG.read_text())
    observed_units = source_config.get("empirical_timestamp_units_by_source_date", {}).get("dates", {})
    cases = {row["incident_id"]: row for split in ("scored", "dev")
             for row in (json.loads(line) for line in
                         (prepared_root / f"{split}_incidents.jsonl").read_text().splitlines() if line.strip())}
    download_records = {}
    for line in download_log_path.read_text().splitlines():
        if line.strip():
            item = json.loads(line)
            if item.get("integrity"):
                download_records[item["path"]] = item
    selected_dates = sorted({date for case in cases.values() for date in case["telemetry_dates"]})
    audit_root.mkdir(parents=True, exist_ok=True)
    progress_path = audit_root / f"bank_clip_progress_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"

    def progress(**item: Any) -> None:
        with progress_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
            stream.flush()

    progress(event="clip_start", selected_dates=selected_dates, selected_cases=len(cases),
             source_tables=len(TABLE_SPECS))
    public_root = prepared_root / "public"
    stage_root = prepared_root / ".bank_public_stage"
    if (public_root.exists() and any(public_root.iterdir()) or stage_root.exists()) and not overwrite_public:
        raise FileExistsError("Bank public clips/staging already exist; explicit --overwrite-public is required")
    if stage_root.exists():
        import shutil
        shutil.rmtree(stage_root)
    stage_cases_root = stage_root / "cases"
    stage_cases_root.mkdir(parents=True, exist_ok=True)
    for case_id, case in cases.items():
        case_directory = stage_cases_root / case_id
        case_directory.mkdir(parents=True)
        public_case_path = case_directory / "alert.json"
        public_case_path.write_text(json.dumps(case, ensure_ascii=False, indent=2,
                                               sort_keys=True) + "\n")
    case_stats: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    writers: dict[tuple[str, str, str], Any] = {}
    open_streams = []
    for case_id, case in cases.items():
        case_stats[case_id] = {}
        for date in case["telemetry_dates"]:
            case_stats[case_id][date] = {}
            for relative, spec in TABLE_SPECS.items():
                empirical_date = observed_units.get(date)
                if not empirical_date:
                    raise ValueError(f"no empirical date-specific unit declaration for selected source date {date}")
                unit = empirical_date.get("units", {}).get(relative)
                if unit not in {"seconds", "milliseconds"}:
                    raise ValueError(f"no valid per-file timestamp unit for {date}/{relative}")
                unit_evidence = empirical_date.get("evidence", {}).get(relative, "unknown")
                destination = stage_cases_root / case_id / "source_dates" / date / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                stream = destination.open("w", newline="", encoding="utf-8")
                writer = csv.DictWriter(stream, fieldnames=spec["header"], lineterminator="\n")
                writer.writeheader()
                open_streams.append(stream)
                writers[(case_id, date, relative)] = writer
                case_stats[case_id][date][relative] = {
                    "unit": unit, "unit_evidence": unit_evidence,
                    "rows": 0, "rows_by_window": Counter(), "missing_by_field": Counter(),
                    "numeric_by_window": defaultdict(lambda: defaultdict(lambda: Counter())),
                    "ids_by_field": defaultdict(set), "kpi_counts_by_window": defaultdict(Counter),
                    "trace_span_keys": set(), "trace_span_key_counts": Counter(),
                    "trace_parent_refs": [], "trace_parent_sentinel_counts": Counter(),
                    "min_timestamp_epoch_ms": None, "max_timestamp_epoch_ms": None,
                    "spillover_before_day_ms_max": 0, "spillover_after_day_ms_max": 0,
                }
    verified_sources: dict[str, dict[str, Any]] = {}
    source_rows_by_path: dict[str, int] = {}
    source_unit_audits: dict[str, dict[str, Any]] = {}
    try:
        for date in selected_dates:
            empirical_date = observed_units[date]
            local_day = datetime.strptime(date, "%Y_%m_%d").replace(tzinfo=LOCAL_ZONE)
            day_start_ms = _epoch_ms(local_day)
            day_end_ms = day_start_ms + 24 * 60 * 60 * 1000
            year_start_ms, year_end_ms = _source_year_bounds_ms(date)
            source_year = int(date[:4])
            cases_by_date = [case_id for case_id, case in cases.items() if date in case["telemetry_dates"]]
            for relative, spec in TABLE_SPECS.items():
                source_rel = f"Bank/telemetry/{date}/{relative}"
                download = download_records.get(source_rel)
                if download is None:
                    raise FileNotFoundError(f"no verified download log record for {source_rel}")
                source_path = raw_root / "telemetry" / date / relative
                source_integrity = _source_integrity(source_path, download, source_rel)
                source_unit = empirical_date["units"][relative]
                verified_sources[source_rel] = {**source_integrity, "source_path": str(source_path),
                                                "timestamp_unit": source_unit,
                                                "unit_evidence": empirical_date.get("evidence", {}).get(relative)}
                unit_audit = {"expected_unit": source_unit, "rows_checked": 0,
                              "unit_magnitude_mismatches": 0, "rows_outside_nominal_source_date": 0,
                              "source_year": source_year,
                              "source_year_start_epoch_ms_utc": year_start_ms,
                              "source_year_end_epoch_ms_utc": year_end_ms,
                              "rows_outside_source_year": 0,
                              "max_spillover_before_day_ms": 0, "max_spillover_after_day_ms": 0,
                              "spillover_policy": "same_source_year_is_required; other dates are counted, not rejected by a fixed day tolerance"}
                source_unit_audits[source_rel] = unit_audit
                scanned = 0
                with source_path.open("r", newline="", encoding="utf-8-sig") as stream:
                    reader = csv.DictReader(stream)
                    if tuple(reader.fieldnames or ()) != spec["header"]:
                        raise ValueError(f"{source_path}: header differs from frozen Bank schema")
                    for line_number, row in enumerate(reader, start=2):
                        scanned += 1
                        if scanned % 1_000_000 == 0:
                            progress(event="source_scan_progress", source_path=source_rel,
                                     rows_scanned=scanned, expected_bytes=source_path.stat().st_size)
                        timestamp_ms = _parse_epoch_ms(row[spec["timestamp_field"]], source_unit,
                                                       path=str(source_path), row_number=line_number)
                        unit_audit["rows_checked"] += 1
                        if not year_start_ms <= timestamp_ms < year_end_ms:
                            unit_audit["rows_outside_source_year"] += 1
                            raise ValueError(f"{source_path}: timestamp is outside the source-date calendar year at row {line_number}")
                        spill_direction, spill = _spillover_from_source_day(
                            timestamp_ms, day_start_ms, day_end_ms)
                        if spill_direction is not None:
                            unit_audit["rows_outside_nominal_source_date"] += 1
                            if spill_direction == "before":
                                unit_audit["max_spillover_before_day_ms"] = max(
                                    unit_audit["max_spillover_before_day_ms"], spill)
                                for case_id in cases_by_date:
                                    st = case_stats[case_id][date][relative]
                                    st["spillover_before_day_ms_max"] = max(st["spillover_before_day_ms_max"], spill)
                            else:
                                unit_audit["max_spillover_after_day_ms"] = max(
                                    unit_audit["max_spillover_after_day_ms"], spill)
                                for case_id in cases_by_date:
                                    st = case_stats[case_id][date][relative]
                                    st["spillover_after_day_ms_max"] = max(st["spillover_after_day_ms_max"], spill)
                            # Source files can contain valid same-year timestamps
                            # from adjacent dates. Only rows in frozen half-open
                            # case windows are copied to clips below.
                        for case_id in cases_by_date:
                            case = cases[case_id]
                            reference = (case["analysis_window_start_epoch_ms_utc"],
                                         case["alert_window_start_epoch_ms_utc"])
                            alert = (case["alert_window_start_epoch_ms_utc"],
                                     case["alert_window_end_epoch_ms_utc"])
                            if reference[0] <= timestamp_ms < reference[1]:
                                window_name = "reference"
                            elif alert[0] <= timestamp_ms < alert[1]:
                                window_name = "alert"
                            else:
                                continue
                            writers[(case_id, date, relative)].writerow(row)
                            stat = case_stats[case_id][date][relative]
                            stat["rows"] += 1
                            stat["rows_by_window"][window_name] += 1
                            stat["min_timestamp_epoch_ms"] = timestamp_ms if stat["min_timestamp_epoch_ms"] is None else min(stat["min_timestamp_epoch_ms"], timestamp_ms)
                            stat["max_timestamp_epoch_ms"] = timestamp_ms if stat["max_timestamp_epoch_ms"] is None else max(stat["max_timestamp_epoch_ms"], timestamp_ms)
                            for field, value in row.items():
                                if value is None or value.strip() == "":
                                    stat["missing_by_field"][field] += 1
                            for field in spec["id_fields"]:
                                if row[field].strip():
                                    stat["ids_by_field"][field].add(row[field].strip())
                            for field in spec["numeric_fields"]:
                                state = "finite" if _finite_value(row[field]) else (
                                    "missing" if not row[field].strip() else "invalid_or_nonfinite")
                                stat["numeric_by_window"][window_name][field][state] += 1
                            if relative == "metric/metric_container.csv":
                                stat["kpi_counts_by_window"][window_name][row["kpi_name"]] += 1
                            if relative == "trace/trace_span.csv":
                                key = (row["trace_id"].strip(), row["span_id"].strip())
                                stat["trace_span_keys"].add(key)
                                stat["trace_span_key_counts"][key] += 1
                                parent = row["parent_id"].strip()
                                if parent.casefold() in {"", "none", "null"}:
                                    stat["trace_parent_sentinel_counts"][parent.casefold() or "empty"] += 1
                                else:
                                    stat["trace_parent_refs"].append((key[0], parent, window_name))
                source_rows_by_path[source_rel] = scanned
                if unit_audit["rows_checked"] != scanned or scanned == 0:
                    raise ValueError(f"{source_path}: full-file timestamp unit validation row count failed")
                unit_audit["unit_magnitude_mismatches"] = 0
                unit_audit["homogeneous_unit_validation"] = "passed_all_rows"
                progress(event="source_file_complete", source_path=source_rel,
                         source_size_bytes=source_path.stat().st_size, source_rows=scanned,
                         timestamp_unit=source_unit,
                         unit_validation=unit_audit)
        for stream in open_streams:
            stream.close()
    except Exception:
        for stream in open_streams:
            try:
                stream.close()
            except OSError:
                pass
        raise

    clip_records, case_file_manifest = [], defaultdict(dict)
    case_audits: dict[str, Any] = {}
    observed_entities: dict[str, set[str]] = defaultdict(set)
    for case_id, case in cases.items():
        date_reports = {}
        for date in case["telemetry_dates"]:
            table_reports = {}
            for relative, spec in TABLE_SPECS.items():
                clip_path = stage_cases_root / case_id / "source_dates" / date / relative
                stat = case_stats[case_id][date][relative]
                clip_hash = sha256_file(clip_path)
                clip_size = clip_path.stat().st_size
                source_rel = f"Bank/telemetry/{date}/{relative}"
                source = verified_sources[source_rel]
                file_info = {"sha256": clip_hash, "rows": stat["rows"], "size_bytes": clip_size,
                             "header": list(spec["header"]), "timestamp_unit": stat["unit"],
                             "full_source_file_unit_validation": source_unit_audits[source_rel],
                             "timestamp_field": spec["timestamp_field"],
                             "source_path": source_rel, "source_sha256": source["sha256"],
                             "source_git_blob_sha1": source.get("hub_git_blob_sha1"),
                             "source_lfs_sha256": source.get("hub_lfs_sha256"),
                             "source_size_bytes": source["size_bytes"],
                             "source_rows": source_rows_by_path[source_rel],
                             "unit_evidence": stat["unit_evidence"],
                             "spillover_before_day_max_ms": stat["spillover_before_day_ms_max"],
                             "spillover_after_day_max_ms": stat["spillover_after_day_ms_max"]}
                rel_key = f"source_dates/{date}/{relative}"
                case_file_manifest[case_id][rel_key] = file_info
                clip_records.append({"case_id": case_id, "relative_path": rel_key,
                    "clip_path": f"data/prepared/bank/public/cases/{case_id}/{rel_key}",
                    "source_date": date, "source_path": source_rel,
                    "source_size_bytes": source["size_bytes"], "source_sha256": source["sha256"],
                    "source_git_blob_sha1": source.get("hub_git_blob_sha1"),
                    "source_lfs_sha256": source.get("hub_lfs_sha256"),
                    "source_rows": source_rows_by_path[source_rel],
                    "source_rows_scanned": source_rows_by_path[source_rel],
                    "full_source_file_unit_validation": source_unit_audits[source_rel],
                    "clip_rows": stat["rows"], "clip_size_bytes": clip_size,
                    "clip_sha256": clip_hash, "header": list(spec["header"]),
                    "timestamp_field": spec["timestamp_field"], "timestamp_source_unit": stat["unit"],
                    "timestamp_unit_evidence": stat["unit_evidence"],
                    "min_timestamp_epoch_ms": stat["min_timestamp_epoch_ms"],
                    "max_timestamp_epoch_ms": stat["max_timestamp_epoch_ms"],
                    "spillover_before_day_max_ms": stat["spillover_before_day_ms_max"],
                    "spillover_after_day_max_ms": stat["spillover_after_day_ms_max"],
                    "reference_window_utc_ms": [case["analysis_window_start_epoch_ms_utc"],
                                                case["alert_window_start_epoch_ms_utc"]],
                    "alert_window_utc_ms": [case["alert_window_start_epoch_ms_utc"],
                                             case["alert_window_end_epoch_ms_utc"]]})
                for field, values in stat["ids_by_field"].items():
                    for identifier in values:
                        observed_entities[identifier].add(f"{relative}:{field}")
                report = {"rows": stat["rows"], "size_bytes": clip_size, "sha256": clip_hash,
                    "timestamp_unit": stat["unit"], "unit_evidence": stat["unit_evidence"],
                    "min_timestamp_epoch_ms": stat["min_timestamp_epoch_ms"],
                    "max_timestamp_epoch_ms": stat["max_timestamp_epoch_ms"],
                    "spillover_before_day_max_ms": stat["spillover_before_day_ms_max"],
                    "spillover_after_day_max_ms": stat["spillover_after_day_ms_max"],
                    "rows_by_window": dict(stat["rows_by_window"]),
                    "missing_by_field": dict(stat["missing_by_field"]),
                    "numeric_status_by_window": {window: {field: dict(counts) for field, counts in per_field.items()}
                                                  for window, per_field in stat["numeric_by_window"].items()},
                    "id_cardinality_by_field": {field: len(values) for field, values in stat["ids_by_field"].items()},
                    "id_values_by_field": {field: sorted(values) for field, values in stat["ids_by_field"].items()},
                    "container_kpi_counts_by_window": {window: dict(counts) for window, counts in stat["kpi_counts_by_window"].items()}}
                if relative == "trace/trace_span.csv":
                    key_counts = stat["trace_span_key_counts"]
                    resolved = sum((trace_id, parent_id) in stat["trace_span_keys"]
                                   for trace_id, parent_id, _ in stat["trace_parent_refs"])
                    report["duplicate_trace_span_keys"] = sum(count - 1 for count in key_counts.values() if count > 1)
                    report["parent_link_audit"] = {"identity_key": "(trace_id,span_id)",
                        "parent_match_key": "(trace_id,parent_id)",
                        "parent_references_within_clip": len(stat["trace_parent_refs"]),
                        "same_trace_parent_matches_in_clip": resolved,
                        "unresolved_or_out_of_clip_parent_refs": len(stat["trace_parent_refs"]) - resolved,
                        "parent_sentinel_counts": dict(stat["trace_parent_sentinel_counts"]),
                        "interpretation": "Unresolved references may point outside the selected interval; source parent links are never repaired."}
                table_reports[relative] = report
            date_reports[date] = table_reports
        alert_file = stage_cases_root / case_id / "alert.json"
        case_audits[case_id] = {"alert_sha256": sha256_file(alert_file),
                                "source_date_tables": date_reports}

    clip_manifest = stage_root / "clip_manifest.jsonl"
    write_jsonl(clip_manifest, clip_records)
    source_config = json.loads(SOURCE_CONFIG.read_text())
    prompt_path = ROOT / source_config["official_source"]["schema_document"]
    prompt_hash = sha256_file(prompt_path)
    catalog = build_public_catalog(source_config, observed_entities, prompt_hash)
    catalog_path = stage_root / "deployment_catalog.json"
    catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    candidate_hash = sha256_file(prepared_root / "candidate_incidents.jsonl")
    scored_hash = sha256_file(prepared_root / "scored_incidents.jsonl")
    dev_hash = sha256_file(prepared_root / "dev_incidents.jsonl")
    cases_manifest = {}
    for case_id, case in cases.items():
        alert_path = stage_cases_root / case_id / "alert.json"
        cases_manifest[case_id] = {"alert_sha256": sha256_file(alert_path),
            "windows": {"reference": {"start_epoch_ms_utc": case["analysis_window_start_epoch_ms_utc"],
                                       "end_epoch_ms_utc": case["alert_window_start_epoch_ms_utc"]},
                        "alert": {"start_epoch_ms_utc": case["alert_window_start_epoch_ms_utc"],
                                  "end_epoch_ms_utc": case["alert_window_end_epoch_ms_utc"]}},
            "files": case_file_manifest[case_id]}
    input_manifest = {"schema_version": "e4-bank-input-manifest/1",
        "sample_config_sha256": sha256_file(sample_path),
        "candidate_manifest_sha256": candidate_hash,
        "scored_manifest_sha256": scored_hash, "dev_manifest_sha256": dev_hash,
        "clip_manifest_sha256": sha256_file(clip_manifest),
        "deployment_catalog_file_sha256": sha256_file(catalog_path),
        "deployment_catalog_schema_sha256": prompt_hash,
        "deployment_mapping_source_sha256": prompt_hash,
        "cases": cases_manifest}
    input_manifest_path = ROOT / "config/bank_input_manifest.json"
    if input_manifest_path.is_file() and json.loads(input_manifest_path.read_text()) != input_manifest:
        raise FileExistsError(f"Bank input manifest differs from the existing freeze: {input_manifest_path}")
    if public_root.exists() and any(public_root.iterdir()) and not overwrite_public:
        raise FileExistsError(f"Bank public inputs already exist: {public_root}")
    import shutil
    if public_root.exists():
        shutil.rmtree(public_root)
    # All three public artifacts are already complete under stage_root. Rename
    # the directory as one promotion so consumers cannot see a mixed tree.
    stage_root.rename(public_root)
    input_manifest_path.write_text(json.dumps(input_manifest, ensure_ascii=False,
                                              indent=2, sort_keys=True) + "\n")
    audit_root.mkdir(parents=True, exist_ok=True)
    report = {"source_revision": source_config["working_mirror"]["revision"],
        "selected_case_count": len(cases), "selected_dates": selected_dates,
        "verified_source_file_count": len(verified_sources),
        "verified_source_bytes": sum(info["size_bytes"] for info in verified_sources.values()),
        "clip_row_count": sum(case_stats[c][d][rel]["rows"] for c in case_stats for d in case_stats[c] for rel in TABLE_SPECS),
        "clip_manifest_sha256": sha256_file(public_root / "clip_manifest.jsonl"),
        "deployment_catalog_sha256": sha256_file(public_root / "deployment_catalog.json"),
        "input_manifest_sha256": sha256_file(input_manifest_path),
        "per_case_audit": case_audits,
        "policy_notes": ["Raw native CSV values and repeated rows are preserved in separate source-date clips.",
                         "Every source file is verified against a Hub LFS SHA-256 or Git-blob SHA-1 before clipping.",
                         "Each row is timestamp-converted using its explicit source-date unit; full-file unit homogeneity and source calendar year are checked.",
                         "Off-date rows are counted; valid same-year rows are copied only when they fall inside a frozen half-open case window.",
                         "Parent links are matched only by (trace_id,span_id) within each clipped case; unresolved refs are not repaired.",
                         "The reference window is not assumed healthy; the public catalog contains no inferred deployment aliases."]}
    audit_root.mkdir(parents=True, exist_ok=True)
    (audit_root / "bank_selected_clip_audit.json").write_text(json.dumps(report, ensure_ascii=False,
        indent=2, sort_keys=True) + "\n")
    return report
