"""Public-data prompt and deterministic gates for a generated Bank adapter."""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import tempfile
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from adapters.bank_adapter import BankAdapter
from e4.data.adapter import AdapterConformanceError
from e4.data.submissions import (
    AdapterInputRequired,
    AdapterSubmissionError,
    load_adapter_submission,
    parse_adapter_submission,
)
from e4.types import EntityRef, LogRecord, MetricRecord, SpanRecord, Window


_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_SAMPLE_CONFIG = _REPOSITORY_ROOT / "config" / "bank_sample.json"
_METRIC_APP = "metric/metric_app.csv"
_PROMPT_SAMPLE_LIMIT = 2
_PARITY_SAMPLE_LIMIT = 24
_LABEL_PATH_PARTS = ("ground_truth", "groundtruth", "fault_reason", "private_reason", "labels.json", "answer_key")


def _public_development_ids() -> list[str]:
    payload = json.loads(_SAMPLE_CONFIG.read_text(encoding="utf-8"))
    selection = payload.get("selection", {})
    ids = selection.get("development_ids")
    if not isinstance(ids, list) or not ids or any(not isinstance(item, str) for item in ids):
        raise ValueError("config/bank_sample.json does not define public development IDs")
    return ids


def _validate_case_selection(cases_dir: Path | str, development_ids: Sequence[str]) -> tuple[Path, list[str], list[Path]]:
    root = Path(cases_dir).resolve()
    if not isinstance(development_ids, Sequence) or isinstance(development_ids, (str, bytes)):
        raise TypeError("development_ids must be a sequence of case directory names")
    requested = list(development_ids)
    allowed = _public_development_ids()
    if len(requested) != len(set(requested)) or set(requested) != set(allowed):
        raise ValueError("Bank onboarding must use exactly config/bank_sample.json selection.development_ids")
    case_paths: list[Path] = []
    for case_id in allowed:
        if not case_id or Path(case_id).name != case_id:
            raise ValueError("Development case IDs must be simple directory names")
        case_path = (root / case_id).resolve()
        if case_path.parent != root or not (case_path / "alert.json").is_file():
            raise ValueError(f"Public development case {case_id!r} is unavailable under {root}")
        case_paths.append(case_path)
    return root, allowed, case_paths


def bank_adapter_config(case_dir: Path | str) -> dict[str, str]:
    """The sole runtime configuration value accepted by the generated adapter."""
    return {"case_dir": str(Path(case_dir).resolve())}


def _case_public_summary(case_path: Path, alias: str) -> dict[str, Any]:
    alert = json.loads((case_path / "alert.json").read_text(encoding="utf-8"))
    reference = BankAdapter(case_path)
    schema = reference.get_schema()
    tables = []
    for table in schema.tables:
        tables.append({
            "name": table.name,
            "record_type": table.record_type,
            "columns": [
                {
                    "name": column.name,
                    "data_type": column.data_type,
                    "nullable": column.nullable,
                    "unit": column.unit,
                }
                for column in sorted(table.columns, key=lambda item: item.name)
            ],
            "entity_roles": sorted(table.entity_roles),
        })

    source_dates = []
    for date in alert.get("telemetry_dates", []):
        date_root = case_path / "source_dates" / str(date)
        date_rows = []
        if date_root.is_dir():
            for csv_path in sorted(date_root.glob("*/*.csv")):
                relative = csv_path.relative_to(date_root).as_posix()
                with csv_path.open("r", encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    rows = []
                    for row in reader:
                        rows.append({
                            key: (value[:180] + "…" if isinstance(value, str) and len(value) > 180 else value)
                            for key, value in row.items()
                        })
                        if len(rows) >= _PROMPT_SAMPLE_LIMIT:
                            break
                    date_rows.append({
                        "path": relative,
                        "header": list(reader.fieldnames or ()),
                        "timestamp_unit_by_date": alert.get("source_date_timestamp_units", {}).get(str(date), {}).get(relative),
                        "timestamp_unit_global_fallback": alert.get("timestamp_unit_in_source_files", {}).get(relative),
                        "sample_rows": rows,
                    })
        source_dates.append({"date": str(date), "files": date_rows})

    allowed_alert_keys = (
        "alert_text",
        "analysis_window_start_epoch_ms_utc",
        "alert_window_start_epoch_ms_utc",
        "alert_window_end_epoch_ms_utc",
        "analysis_window_start_local",
        "analysis_window_end_local",
        "alert_window_start_local",
        "alert_window_end_local",
        "timezone",
        "telemetry_date",
        "telemetry_dates",
        "source_date_timestamp_units",
        "timestamp_unit_in_source_files",
        "source_date_timestamp_unit_evidence",
        "upstream_timestamp_unit_provenance",
    )
    raw_alert_metadata = {key: alert[key] for key in allowed_alert_keys if key in alert}
    return {
        "case_alias": alias,
        "raw_alert_metadata": raw_alert_metadata,
        "normalized_expected_schema": {"time_unit": schema.time_unit, "tables": tables},
        "raw_source_examples": source_dates,
    }


def build_bank_adapter_prompt(cases_dir: Path | str, development_ids: Sequence[str]) -> str:
    """Render adapter-authoring instructions from the two configured public development cases only."""
    _, _, case_paths = _validate_case_selection(cases_dir, development_ids)
    cases = [_case_public_summary(path, f"development_case_{index + 1}") for index, path in enumerate(case_paths)]
    raw_metadata = [
        {
            "alias": item["case_alias"],
            "raw_alert_json_fields": item["raw_alert_metadata"],
            "normalized_expected_schema": item["normalized_expected_schema"],
        }
        for item in cases
    ]
    sample_data = [
        {"alias": item["case_alias"], "source_files": item["raw_source_examples"]}
        for item in cases
    ]
    domain_brief = """OpenRCA Bank adapter task: diagnose the listed incident using telemetry and rank observed backend/service components in the top three. Preserve component identity as `EntityRef(role='service', identifier=cmdb_id)`. Application transaction codes (`tc`) are opaque KPI context, not rankable service identities; represent each nonempty `rr`, `sr`, `cnt`, and `mrt` projection under entity role `app_kpi`, keep `tc` in record details, and never map a transaction code to a component. Missing trace success remains `None`. The trace `duration` field has unknown physical units: preserve its raw numeric value in `SpanRecord.duration_ms` with no unit declaration and do not claim it is milliseconds. Metric units and the meanings of `rr`, `sr`, `cnt`, and `mrt` are undeclared. Preserve source-row linkage in `details['origin_row_id']` when one app input row emits multiple canonical metric records."""
    public_metadata_text = """The JSON below contains only filtered public `alert.json` keys from the configured development cases. Key names are raw source keys, not a derived `windows` object. `analysis_window_start_epoch_ms_utc` is the reference start, `alert_window_start_epoch_ms_utc` is reference end and alert start, and `alert_window_end_epoch_ms_utc` is alert end. These bounds are already UTC milliseconds; never apply the displayed timezone offset again. Use the exact logical table names and normalized column/kind/unit facts in `normalized_expected_schema`, omitting tables whose input file is absent from a runtime case. At runtime accept only a `case_dir` configuration and inspect that case's `alert.json` and `source_dates/<date>/{metric,trace,log}/` files. For timestamp conversion prefer `source_date_timestamp_units[date][relative_path]`; only use `timestamp_unit_in_source_files[relative_path]` when the date-specific entry is missing. The public source can contain seconds and milliseconds; normalize both correctly, do not hard-code either unit for every date. Filter records to the reference and alert half-open UTC millisecond windows. A file that exists but has no rows remains declared and streams an empty iterator. Do not use benchmark/evaluator code, other adapters, private labels, or incident identifiers.""" + "\n\n" + json.dumps(raw_metadata, indent=2, ensure_ascii=False, sort_keys=True)
    sample_data_text = json.dumps(sample_data, indent=2, ensure_ascii=False, sort_keys=True)
    from e4.data.agent_prompt import get_adapter_generation_prompt

    return get_adapter_generation_prompt(
        sample_data_text,
        domain_brief=domain_brief,
        public_metadata_text=public_metadata_text,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_manifest(case_paths: Sequence[Path], development_ids: Sequence[str]) -> dict[str, Any]:
    cases = []
    for case_id, case_path in zip(development_ids, case_paths):
        files = [case_path / "alert.json"]
        for date in json.loads(files[0].read_text(encoding="utf-8")).get("telemetry_dates", []):
            date_root = case_path / "source_dates" / str(date)
            if date_root.is_dir():
                files.extend(sorted(date_root.glob("*/*.csv")))
        cases.append({
            "development_id": case_id,
            "files": [
                {
                    "path": path.relative_to(case_path).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
                for path in sorted(set(files))
            ],
        })
    return {"development_ids": list(development_ids), "cases": cases}


def _validate_bank_source(source: str) -> None:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise AdapterSubmissionError("source", "adapter source has invalid Python syntax") from exc
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [node.module or ""]
        else:
            modules = []
        for module in modules:
            root = module.split(".", 1)[0]
            if root in {"adapters", "benchmarks"}:
                raise AdapterSubmissionError(
                    "source",
                    "generated source may not import existing adapters, evaluators, or benchmark modules",
                    {"module": module},
                )
        if isinstance(node, ast.Name) and node.id in {"BankAdapter", "GroundTruthOracle", "EvaluationSession"}:
            raise AdapterSubmissionError("source", "generated source may not delegate to benchmark adapter or label evaluators")
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value.casefold().replace("\\", "/")
            if any(part in value for part in _LABEL_PATH_PARTS):
                raise AdapterSubmissionError("source", "generated source may not reference private label files or fields")
    public_config = json.loads(_SAMPLE_CONFIG.read_text(encoding="utf-8"))
    scored_ids = public_config.get("selection", {}).get("scored_ids", [])
    if any(isinstance(case_id, str) and case_id in source for case_id in scored_ids):
        raise AdapterSubmissionError("source", "generated source may not hard-code scored case identifiers")


def _config_schema_is_case_dir(schema: Mapping[str, Any]) -> bool:
    draft = schema.get("$schema")
    properties = schema.get("properties")
    required = schema.get("required")
    if (
        (draft is not None and draft != "https://json-schema.org/draft/2020-12/schema")
        or schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
    ):
        return False
    if not isinstance(properties, Mapping) or set(properties) != {"case_dir"}:
        return False
    if not isinstance(required, list) or set(required) != {"case_dir"}:
        return False
    case_dir_schema = properties.get("case_dir", {})
    return isinstance(case_dir_schema, Mapping) and case_dir_schema.get("type") == "string"


def _schema_facts(schema: Any) -> dict[str, Any]:
    return {
        "time_unit": schema.time_unit,
        "tables": [
            {
                "name": table.name,
                "record_type": table.record_type,
                "entity_roles": sorted(table.entity_roles),
                "columns": [
                    {
                        "name": column.name,
                        "data_type": column.data_type,
                        "nullable": column.nullable,
                        "unit": column.unit,
                    }
                    for column in sorted(table.columns, key=lambda item: item.name)
                ],
            }
            for table in sorted(schema.tables, key=lambda item: item.name)
        ],
    }


def _entity_set(entities: Sequence[EntityRef]) -> set[tuple[str, str]]:
    return {(entity.role, entity.identifier) for entity in entities}


def _round_value(value: Any) -> Any:
    if type(value) is float:
        return round(value, 12)
    return value


def _record_fact(record: Any) -> dict[str, Any]:
    if isinstance(record, MetricRecord):
        return {
            "kind": "metric",
            "table": record.source_table,
            "timestamp_ms_utc": record.timestamp_ms_utc,
            "entity": [record.entity.role, record.entity.identifier],
            "metric_name": record.metric_name,
            "value": _round_value(record.value),
            "unit": record.unit,
        }
    if isinstance(record, SpanRecord):
        return {
            "kind": "span",
            "table": record.source_table,
            "timestamp_ms_utc": record.timestamp_ms_utc,
            "trace_id": record.trace_id,
            "span_id": record.span_id,
            "parent_id": record.parent_id,
            "duration": _round_value(record.duration_ms),
            "success": record.success,
            "call_type": record.call_type,
            "executing_component": None if record.executing_component is None else [record.executing_component.role, record.executing_component.identifier],
        }
    if isinstance(record, LogRecord):
        return {
            "kind": "log",
            "table": record.source_table,
            "timestamp_ms_utc": record.timestamp_ms_utc,
            "entity": None if record.entity is None else [record.entity.role, record.entity.identifier],
            "log_name": record.log_name,
            "value": record.value[:256] if isinstance(record.value, str) else record.value,
            "severity": record.severity,
        }
    raise TypeError(f"unsupported canonical record {type(record).__name__}")


def _stream_summary(adapter: Any, table_name: str, window: Window) -> dict[str, Any]:
    count = 0
    sample: list[dict[str, Any]] = []
    app_origins: dict[str, tuple[tuple[str, str] | None, int, set[str]]] = {}
    for record in adapter.stream_records(table_name, window):
        count += 1
        fact = _record_fact(record)
        if len(sample) < _PARITY_SAMPLE_LIMIT:
            sample.append(fact)
        if isinstance(record, MetricRecord) and record.entity.role == "app_kpi":
            origin_id = record.details.get("origin_row_id")
            if isinstance(origin_id, str) and len(app_origins) < _PARITY_SAMPLE_LIMIT:
                entity = (record.entity.role, record.entity.identifier)
                previous = app_origins.get(origin_id)
                if previous is None:
                    app_origins[origin_id] = (entity, record.timestamp_ms_utc, {record.metric_name})
                else:
                    previous_entity, previous_timestamp, metric_names = previous
                    if previous_entity != entity or previous_timestamp != record.timestamp_ms_utc:
                        raise ValueError("app_kpi origin_row_id maps to inconsistent entity or timestamp")
                    metric_names.add(record.metric_name)
    projections = sorted(tuple(sorted(metric_names)) for _, _, metric_names in app_origins.values())
    return {
        "count": count,
        "sample": sorted(sample, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"))),
        "app_projection_groups": projections,
    }


def _safe_failure(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, AdapterSubmissionError):
        return {"stage": exc.stage, "message": str(exc), "details": dict(exc.details)}
    if isinstance(exc, AdapterConformanceError):
        return {"stage": "conformance", "message": str(exc)}
    return {"stage": "validation", "message": f"{type(exc).__name__}: {str(exc)[:240]}"}


def _timestamp_unit_probes(
    submission: Mapping[str, Any],
    case_paths: Sequence[Path],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Exercise both timestamp scales using one public trace row and conflicting fallbacks."""
    sample_case = case_paths[0]
    alert = json.loads((sample_case / "alert.json").read_text(encoding="utf-8"))
    dates = alert.get("telemetry_dates", ())
    if not dates:
        return [], ["timestamp normalization probe requires a public telemetry date"]
    date = str(dates[0])
    trace_relative = "trace/trace_span.csv"
    metric_relative = "metric/metric_container.csv"
    trace_path = sample_case / "source_dates" / date / trace_relative
    try:
        with trace_path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            row = next(reader)
            fieldnames = list(reader.fieldnames or ())
        sample_value = int(float(row["timestamp"]))
        declared_unit = alert.get("source_date_timestamp_units", {}).get(date, {}).get(trace_relative)
        if declared_unit is None:
            declared_unit = alert.get("timestamp_unit_in_source_files", {}).get(trace_relative)
        normalized_ms = sample_value * 1000 if str(declared_unit).lower().replace("epoch ", "") in {"seconds", "second", "s"} else sample_value
    except Exception as exc:
        return [], [f"timestamp normalization probe could not read a public trace row ({type(exc).__name__})"]

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for unit in ("seconds", "milliseconds"):
        raw_timestamp = normalized_ms // 1000 if unit == "seconds" else normalized_ms
        probe_start = normalized_ms - 2000
        alert_start = normalized_ms - 1000
        alert_end = normalized_ms + 1000
        opposite = "milliseconds" if unit == "seconds" else "seconds"
        probe_date = "timestamp_unit_probe"
        probe_row = dict(row)
        probe_row["timestamp"] = str(raw_timestamp)

        with tempfile.TemporaryDirectory(prefix="e4_bank_unit_probe_") as directory:
            case_path = Path(directory)
            alert_doc = {
                "alert_text": "Synthetic public-row timestamp normalization probe.",
                "analysis_window_start_epoch_ms_utc": probe_start,
                "alert_window_start_epoch_ms_utc": alert_start,
                "alert_window_end_epoch_ms_utc": alert_end,
                "telemetry_date": probe_date,
                "telemetry_dates": [probe_date],
                "timezone": "UTC",
                "source_date_timestamp_units": {
                    probe_date: {trace_relative: unit, metric_relative: "seconds"},
                },
                "timestamp_unit_in_source_files": {
                    trace_relative: "epoch " + opposite,
                    metric_relative: "epoch milliseconds",
                },
            }
            (case_path / "alert.json").write_text(json.dumps(alert_doc), encoding="utf-8")
            trace_path = case_path / "source_dates" / probe_date / trace_relative
            trace_path.parent.mkdir(parents=True, exist_ok=True)
            with trace_path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(probe_row)

            metric_path = case_path / "source_dates" / probe_date / metric_relative
            metric_path.parent.mkdir(parents=True, exist_ok=True)
            with metric_path.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=("timestamp", "cmdb_id", "kpi_name", "value"))
                writer.writeheader()
                writer.writerow({
                    "timestamp": str(normalized_ms // 1000),
                    "cmdb_id": str(row.get("cmdb_id") or "probe-service"),
                    "kpi_name": "public_timestamp_probe",
                    "value": "1.0",
                })

            try:
                candidate = load_adapter_submission(
                    submission,
                    bank_adapter_config(case_path),
                    check_conformance=False,
                )
                reference = BankAdapter(case_path)
                ref_window, alert_window = reference.get_windows()
                candidate_windows = candidate.get_windows()
                windows_match = tuple(item.as_dict() for item in candidate_windows) == tuple(
                    item.as_dict() for item in (ref_window, alert_window)
                )
                table = "bank/trace_span.csv"
                expected = _stream_summary(reference, table, alert_window)
                observed = _stream_summary(candidate, table, alert_window)
                expected_metric = _stream_summary(reference, "bank/metric_container.csv", alert_window)
                observed_metric = _stream_summary(candidate, "bank/metric_container.csv", alert_window)
                matched = (
                    windows_match
                    and expected["count"] == 1
                    and observed["count"] == 1
                    and expected["sample"] == observed["sample"]
                    and expected_metric["count"] == 1
                    and observed_metric["count"] == 1
                    and expected_metric["sample"] == observed_metric["sample"]
                )
                results.append({
                    "unit": unit,
                    "date_specific_overrides_opposite_global_fallback": True,
                    "reference_count": expected["count"],
                    "candidate_count": observed["count"],
                    "windows_match": windows_match,
                    "records_match": expected["sample"] == observed["sample"],
                    "component_probe_match": expected_metric["sample"] == observed_metric["sample"],
                    "passed": matched,
                })
                if not matched:
                    errors.append(f"{unit} timestamp normalization probe failed")
            except Exception as exc:
                results.append({"unit": unit, "passed": False, "error_type": type(exc).__name__})
                errors.append(f"{unit} timestamp normalization probe failed ({type(exc).__name__})")
    return results, errors


def validate_bank_adapter_submission(
    response: str | Mapping[str, Any],
    cases_dir: Path | str,
    development_ids: Sequence[str],
) -> dict[str, Any]:
    """Validate a generated Bank adapter against public development cases only."""
    try:
        cases_root, dev_ids, case_paths = _validate_case_selection(cases_dir, development_ids)
        source_manifest = _source_manifest(case_paths, dev_ids)
    except Exception as exc:
        return {"status": "invalid", "stage": "inputs", "errors": [_safe_failure(exc)]}

    try:
        submission = parse_adapter_submission(response)
    except AdapterSubmissionError as exc:
        return {
            "status": "invalid",
            "stage": exc.stage,
            "errors": [_safe_failure(exc)],
            "source_manifest": source_manifest,
        }
    if submission["status"] == "needs_input":
        return {
            "status": "needs_input",
            "required_fields": submission["required_fields"],
            "source_manifest": source_manifest,
        }

    source = submission["source"]
    source_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()
    try:
        if not _config_schema_is_case_dir(submission["config_schema"]):
            raise AdapterSubmissionError(
                "config_schema",
                "Bank adapter config_schema must require exactly one string case_dir and reject extra properties",
            )
        _validate_bank_source(source)
    except AdapterSubmissionError as exc:
        return {
            "status": "invalid",
            "stage": exc.stage,
            "errors": [_safe_failure(exc)],
            "adapter_source_sha256": source_hash,
            "source_manifest": source_manifest,
        }

    case_reports = []
    first_schema = None
    for development_id, case_path in zip(dev_ids, case_paths):
        config = bank_adapter_config(case_path)
        try:
            candidate = load_adapter_submission(submission, config)
            reference = BankAdapter(case_path)
            candidate_schema = candidate.get_schema()
            reference_schema = reference.get_schema()
            candidate_windows = candidate.get_windows()
            reference_windows = reference.get_windows()
            windows_match = tuple(window.as_dict() for window in candidate_windows) == tuple(
                window.as_dict() for window in reference_windows
            )
            schema_matches = _schema_facts(candidate_schema) == _schema_facts(reference_schema)
            candidate_entities = _entity_set(candidate.extract_suspect_entities())
            reference_entities = _entity_set(reference.extract_suspect_entities())
            entities_match = candidate_entities == reference_entities
            if first_schema is None:
                first_schema = candidate_schema.as_dict()

            table_reports: dict[str, Any] = {}
            record_mismatches: list[str] = []
            common_tables = sorted(
                {table.name for table in candidate_schema.tables}
                & {table.name for table in reference_schema.tables}
            )
            for table_name in common_tables:
                table_reports[table_name] = {}
                for window in (reference_windows[0], reference_windows[1]):
                    expected = _stream_summary(reference, table_name, window)
                    observed = _stream_summary(candidate, table_name, window)
                    table_reports[table_name][window.name] = {
                        "reference_count": expected["count"],
                        "candidate_count": observed["count"],
                        "count_match": expected["count"] == observed["count"],
                        "sample_records_compared": min(len(expected["sample"]), len(observed["sample"])),
                    }
                    if expected["count"] != observed["count"]:
                        record_mismatches.append(f"{table_name}/{window.name}: record count differs")
                    if expected["sample"] != observed["sample"]:
                        record_mismatches.append(f"{table_name}/{window.name}: bounded canonical records differ")
                    if expected["app_projection_groups"] != observed["app_projection_groups"]:
                        record_mismatches.append(f"{table_name}/{window.name}: app KPI source-row projections differ")

            if not windows_match:
                record_mismatches.append("reference/alert windows differ from public case metadata")
            if not schema_matches:
                record_mismatches.append("normalized table/column/kind/unit schema differs from reference")
            if not entities_match:
                record_mismatches.append("observed rankable service identity set differs from reference")
            case_reports.append({
                "development_id": development_id,
                "windows_match": windows_match,
                "schema_match": schema_matches,
                "suspect_entities_match": entities_match,
                "tables": table_reports,
                "errors": record_mismatches,
            })
        except AdapterSubmissionError as exc:
            case_reports.append({
                "development_id": development_id,
                "errors": [_safe_failure(exc)],
            })
        except Exception as exc:
            case_reports.append({
                "development_id": development_id,
                "errors": [_safe_failure(exc)],
            })

    errors = [
        {"development_id": report["development_id"], "message": message}
        for report in case_reports
        for message in report.get("errors", [])
    ]
    timestamp_unit_probes, timestamp_unit_errors = _timestamp_unit_probes(submission, case_paths)
    errors.extend({"message": message} for message in timestamp_unit_errors)
    seconds_probe_passed = any(item.get("unit") == "seconds" and item.get("passed") for item in timestamp_unit_probes)
    milliseconds_probe_failed = any(item.get("unit") == "milliseconds" and not item.get("passed") for item in timestamp_unit_probes)
    case_failures = [
        message
        for case in case_reports
        for message in case.get("errors", [])
    ]
    if milliseconds_probe_failed and seconds_probe_passed:
        report_stage = "milliseconds_normalization"
    elif case_failures:
        first_case_error = case_failures[0]
        report_stage = first_case_error.get("stage", "parity") if isinstance(first_case_error, Mapping) else "parity"
    elif timestamp_unit_errors:
        report_stage = "timestamp_normalization"
    else:
        report_stage = None
    return {
        "status": "conforming" if not errors else "invalid",
        "stage": report_stage,
        "adapter_source_sha256": source_hash,
        "config_schema": submission["config_schema"],
        "schema": first_schema,
        "source_manifest": source_manifest,
        "cases": case_reports,
        "timestamp_unit_probes": timestamp_unit_probes,
        "errors": errors,
    }
