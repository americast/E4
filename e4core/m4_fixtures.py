"""Independent neutral fixtures for M4 bootstrap and controller conformance."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .bank_adapter import BankCase
from .contracts import EntityRef, MetricPoint, Span, Telemetry, Window
from .runtime import build_case_runtime


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CASE_ID = "bank-20990101T0000-utc8"
FIXTURE_CAUSE_ID = "fixture_pod_alpha"
FIXTURE_CONTROL_ID = "fixture_pod_beta"
FIXTURE_KPI = "OSLinux-CPU_CPU_CPUCpuUtil"


def synthetic_bank_telemetry(*, scenario: str = "positive",
                             alpha_id: str = FIXTURE_CAUSE_ID,
                             beta_id: str = FIXTURE_CONTROL_ID) -> Telemetry:
    """Return a fully synthetic telemetry fixture with no real labels."""
    if scenario not in {"positive", "no_change", "high_stable", "missing"}:
        raise ValueError("unknown synthetic Bank fixture scenario")
    start = int(datetime(2099, 1, 1, 0, 0,
                         tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000)
    reference = Window("reference", start, start + 1_800_000)
    alert = Window("alert", start + 1_800_000, start + 3_600_000)
    metrics: list[MetricPoint] = []
    spans: list[Span] = []
    for entity_id in (alpha_id, beta_id):
        entity = EntityRef("component", entity_id)
        for window, window_start in ((reference, reference.start_ms_utc),
                                     (alert, alert.start_ms_utc)):
            for minute in range(30):
                timestamp = window_start + minute * 60_000
                metric_value = _synthetic_value(scenario, entity_id, window.name, modality="metric",
                                                is_control=entity_id == beta_id)
                metrics.append(MetricPoint("bank/metric_container.csv", timestamp, entity,
                    FIXTURE_KPI, "bank.metric_container", "value", metric_value,
                    str(metric_value), f"synthetic:{entity_id}:{window.name}:{minute}"))
                log_value = _synthetic_value(scenario, entity_id, window.name, modality="log",
                                             is_control=entity_id == beta_id)
                metrics.append(MetricPoint("bank/log/log_service.csv", timestamp, entity,
                    "log_volume_count", "bank.log_service", "synthetic-log", log_value,
                    str(log_value), f"synthetic-log:{entity_id}:{window.name}:{minute}"))
            for trace_index in range(12):
                duration = _synthetic_value(scenario, entity_id, window.name, modality="trace",
                                            is_control=entity_id == beta_id)
                timestamp = window_start + trace_index * 150_000
                spans.append(Span(trace_id=f"{entity_id}-{window.name}-{trace_index}",
                    span_id="span-0", parent_id=None, start_ms_utc=timestamp,
                    elapsed_raw=str(duration), success=None, success_status="missing",
                    success_raw="", call_type="unprovided", executing_component=entity,
                    logical_service=None, database_target=None,
                    parent_resolved_in_clip=False, parent_unresolved_in_clip=False))
    fixture_identity = json.dumps({"scenario": scenario, "alpha": alpha_id, "beta": beta_id,
        "metric_rows": [(row.entity.identifier, row.timestamp_ms_utc, row.value) for row in metrics],
        "trace_rows": [(row.trace_id, row.elapsed_raw) for row in spans],
        "log_text_policy": "native lexical synthetic fixtures v2"},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    data_digest = hashlib.sha256(fixture_identity).hexdigest()
    return Telemetry(case_id=FIXTURE_CASE_ID, reference_window=reference, alert_window=alert,
        metrics=tuple(metrics), spans=tuple(spans),
        deployment_catalog={alpha_id: {"role": "component"},
                            beta_id: {"role": "component"}},
        data_sha256=data_digest, file_sha256={},
        duplicate_counts={}, quality_counts={}, adapter_version="m4-synthetic-fixture/1")


def _synthetic_value(scenario: str, entity_id: str, window_name: str, *,
                     modality: str, is_control: bool = False) -> float | None:
    if scenario == "missing":
        return None
    if scenario == "high_stable":
        return 900.0 if is_control and modality == "metric" else 10.0
    if scenario == "no_change" or is_control or window_name == "reference":
        return {"metric": 10.0, "log": 1.0, "trace": 100.0}[modality]
    return {"metric": 90.0, "log": 3.0, "trace": 500.0}[modality]


def _synthetic_log_text(scenario: str, window_name: str, is_control: bool) -> str:
    """Fixed generic JVM vocabulary with positive, stable, and healthy text controls."""
    if scenario == "positive" and window_name == "alert" and not is_control:
        return "java.lang.OutOfMemoryError: Java heap space; GC overhead limit exceeded"
    if scenario == "high_stable" and is_control:
        return "java.lang.OutOfMemoryError: Java heap space; GC overhead limit exceeded"
    return "request completed successfully; health check ok"


def synthetic_case_runtime(telemetry: Telemetry | None = None):
    telemetry = telemetry or synthetic_bank_telemetry()
    return build_case_runtime({
        "analysis_window_start_epoch_ms_utc": telemetry.reference_window.start_ms_utc,
        "alert_window_start_epoch_ms_utc": telemetry.alert_window.start_ms_utc,
        "alert_window_end_epoch_ms_utc": telemetry.alert_window.end_ms_utc},
        schema_path=ROOT / "config/runtime_schema.json",
        config_path=ROOT / "config/core_runtime.json")


def fixture_expected_top_component(alpha_id: str = FIXTURE_CAUSE_ID) -> str:
    """Expected synthetic control oracle; never imported into prompts or production."""
    return alpha_id


def write_synthetic_public_case(root: Path, *, scenario: str = "positive",
                                alpha_id: str = FIXTURE_CAUSE_ID,
                                beta_id: str = FIXTURE_CONTROL_ID) -> BankCase:
    """Write tiny native Bank clips so bounded query operators can be tested end-to-end."""
    telemetry = synthetic_bank_telemetry(scenario=scenario, alpha_id=alpha_id, beta_id=beta_id)
    case_id = telemetry.case_id
    local_start = datetime(2099, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=8)))
    start_ms = telemetry.reference_window.start_ms_utc
    alert = {
        "incident_id": case_id,
        "alert_text": "Synthetic fixture interval; this is not a real incident or label.",
        "timezone": "Asia/Shanghai",
        "analysis_window_start_local": local_start.isoformat(),
        "alert_window_start_local": (local_start + timedelta(minutes=30)).isoformat(),
        "alert_window_end_local": (local_start + timedelta(minutes=60)).isoformat(),
        "analysis_window_start_epoch_ms_utc": start_ms,
        "alert_window_start_epoch_ms_utc": start_ms + 1_800_000,
        "alert_window_end_epoch_ms_utc": start_ms + 3_600_000,
        "telemetry_date": "2099_01_01", "telemetry_dates": ["2099_01_01"],
        "timestamp_unit_in_source_files": {
            "metric/metric_app.csv": "epoch seconds",
            "metric/metric_container.csv": "epoch seconds",
            "trace/trace_span.csv": "epoch milliseconds",
            "log/log_service.csv": "epoch seconds"},
        "upstream_timestamp_unit_provenance": "synthetic fixture; upstream labels retained for adapter compatibility",
        "source_date_timestamp_units": {"2099_01_01": {
            "metric/metric_app.csv": "seconds", "metric/metric_container.csv": "seconds",
            "trace/trace_span.csv": "milliseconds", "log/log_service.csv": "seconds"}},
    }
    case_dir = root / "data/prepared/bank/public/cases" / case_id
    date_dir = case_dir / "source_dates/2099_01_01"
    alert_path = case_dir / "alert.json"
    alert_path.parent.mkdir(parents=True, exist_ok=True)
    alert_path.write_text(json.dumps(alert, sort_keys=True), encoding="utf-8")
    file_hashes = {"alert.json": _sha256(alert_path)}
    file_bytes = {"alert.json": alert_path.stat().st_size}
    tables: dict[str, tuple[tuple[str, ...], list[dict[str, Any]]]] = {}
    for table in ("metric/metric_app.csv", "metric/metric_container.csv",
                  "trace/trace_span.csv", "log/log_service.csv"):
        tables[table] = (TABLE_HEADERS[table], [])

    for window_name, window_start in (("reference", start_ms), ("alert", start_ms + 1_800_000)):
        for entity_id in (alpha_id, beta_id):
            for minute in range(30):
                seconds = (window_start + minute * 60_000) // 1000
                is_control = entity_id == beta_id
                metric_value = _synthetic_value(scenario, entity_id, window_name,
                                                modality="metric", is_control=is_control)
                tables["metric/metric_container.csv"][1].append({
                    "timestamp": seconds, "cmdb_id": entity_id, "kpi_name": FIXTURE_KPI,
                    "value": "" if metric_value is None else str(metric_value)})
                app_value = _synthetic_value(scenario, entity_id, window_name,
                                             modality="metric", is_control=is_control)
                tables["metric/metric_app.csv"][1].append({"timestamp": seconds,
                    "rr": "", "sr": "", "cnt": "", "mrt": "" if app_value is None else str(app_value),
                    "tc": "synthetic_tc"})
                log_value = _synthetic_value(scenario, entity_id, window_name,
                                             modality="log", is_control=is_control)
                for index in range(int(log_value or 0)):
                    log_text = _synthetic_log_text(scenario, window_name, is_control)
                    tables["log/log_service.csv"][1].append({"log_id": f"{entity_id}-{window_name}-{minute}-{index}",
                        "timestamp": seconds, "cmdb_id": entity_id, "log_name": "synthetic_log",
                        "value": log_text})
            for trace_index in range(12):
                duration = _synthetic_value(scenario, entity_id, window_name,
                                            modality="trace", is_control=is_control)
                tables["trace/trace_span.csv"][1].append({
                    "timestamp": window_start + trace_index * 150_000,
                    "cmdb_id": entity_id, "parent_id": "", "span_id": "span-0",
                    "trace_id": f"{entity_id}-{window_name}-{trace_index}",
                    "duration": "" if duration is None else str(duration)})

    for table, (header, rows) in tables.items():
        path = date_dir / table
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        relative = f"source_dates/2099_01_01/{table}"
        file_hashes[relative] = _sha256(path)
        file_bytes[relative] = path.stat().st_size
    source_digest = hashlib.sha256(json.dumps(dict(sorted(file_hashes.items())), sort_keys=True,
        separators=(",", ":")).encode("utf-8")).hexdigest()
    telemetry = Telemetry(telemetry.case_id, telemetry.reference_window, telemetry.alert_window,
        telemetry.metrics, telemetry.spans, telemetry.deployment_catalog, source_digest,
        file_hashes, telemetry.duplicate_counts, telemetry.quality_counts,
        telemetry.adapter_version, file_bytes)
    provenance = {"component_identity_coverage": {}, "synthetic_fixture": True}
    return BankCase(telemetry=telemetry, alert=alert, public_manifest_row={"incident_id": case_id},
                    deployment_catalog={"official_candidate_components": [alpha_id, beta_id],
                                        "entities": {alpha_id: {}, beta_id: {}}},
                    log_summary={"summary_policy": "synthetic-only fixture"}, provenance=provenance)


TABLE_HEADERS = {
    "metric/metric_app.csv": ("timestamp", "rr", "sr", "cnt", "mrt", "tc"),
    "metric/metric_container.csv": ("timestamp", "cmdb_id", "kpi_name", "value"),
    "trace/trace_span.csv": ("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"),
    "log/log_service.csv": ("log_id", "timestamp", "cmdb_id", "log_name", "value"),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()

