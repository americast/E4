"""Versioned public type and helper contract for authored Python operators.

The registry here is the metadata source consumed by the static checker and
worker namespace. Keep entries descriptive and algorithm-neutral: generated
code can query bounded typed records, then write its own analysis in Python.
"""
from __future__ import annotations

import contextlib
import csv
import dataclasses
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Mapping

if TYPE_CHECKING:
    from .contracts import Telemetry

PROFILE = "typed-operator-code-v1"
MAX_QUERY_ROWS = 10_000
MAX_GROUPS = 10_000
MAX_AGGREGATE_PAGE_ROWS = 1_000
MAX_ARTIFACT_BYTES = 128 * 1024 * 1024
MAX_ARTIFACT_ROWS = 300_000
MAX_SOURCE_BYTES = 8_192

RECORD_FIELDS = {
    "WindowView": {"name": "Literal['reference', 'alert']", "start_ms_utc": "int", "end_ms_utc": "int"},
    "TelemetryView": {
        "case_id": "str", "reference_window": "WindowView", "alert_window": "WindowView",
        "data_sha256": "str", "source_tables": "Sequence[str]", "point_count": "int",
    },
    "CoverageView": {
        "complete": "bool", "truncated": "bool", "offset": "int",
        "next_offset": "int | None", "total_matches": "int", "support": "int",
    },
    "MetricPointView": {
        "source_table": "str", "timestamp_ms_utc": "int", "entity_role": "str",
        "entity_identifier": "str", "origin_row_id": "str", "metric_key": "str", "source_field": "str | None",
        "metric_name": "str", "representation": "str", "bomc_id": "str | None",
        "item_id": "str | None", "value": "float | None",
    },
    "LogRecordView": {
        "source_table": "str", "timestamp_ms_utc": "int", "source_ref": "str", "log_id": "str | None",
        "entity_role": "str | None", "entity_identifier": "str | None", "log_name": "str | None",
        "value": "str | None",
    },
    "SpanView": {
        "source_table": "str", "timestamp_ms_utc": "int", "source_ref": "str", "trace_id": "str",
        "span_id": "str", "parent_id": "str | None", "duration_value": "float | None",
        "duration_status": "Literal['valid', 'missing', 'invalid', 'nonfinite']",
        "duration_unit": "Literal['undeclared']",
        "call_type": "str | None",
        "executing_component_role": "str | None",
        "executing_component_identifier": "str | None", "logical_service_role": "str | None",
        "logical_service_identifier": "str | None", "database_target_role": "str | None",
        "database_target_identifier": "str | None", "parent_resolved_in_clip": "bool",
        "parent_unresolved_in_clip": "bool",
    },
    "QueryResult": {
        "rows": "Sequence[MetricPointView] | Sequence[SpanView] | Sequence[LogRecordView] | Sequence[AggregateGroupView]",
        "window": "WindowView", "source_tables": "Sequence[str]", "coverage": "CoverageView",
    },
    "SummaryResult": {"value": "float | int | None", "support": "int",
        "missing_count": "int", "nonfinite_count": "int", "invalid_count": "int"},
    "AggregateGroupView": {"key": "tuple[str | int | float | bool | None, ...]",
        "group_fields": "Sequence[str]", "value": "float | int | str | None", "support": "int",
        "missing_count": "int", "nonfinite_count": "int", "invalid_count": "int"},
}

COLUMN_FIELDS = {
    "MetricPointView": ("source_table", "timestamp_ms_utc", "entity_role", "entity_identifier", "origin_row_id",
        "metric_key", "source_field", "metric_name", "representation", "bomc_id", "item_id", "value"),
    "SpanView": ("source_table", "timestamp_ms_utc", "source_ref", "trace_id", "span_id", "parent_id",
        "duration_value", "duration_status", "duration_unit", "call_type",
        "executing_component_role", "executing_component_identifier", "logical_service_role",
        "logical_service_identifier", "database_target_role", "database_target_identifier",
        "parent_resolved_in_clip", "parent_unresolved_in_clip"),
    "LogRecordView": ("source_table", "timestamp_ms_utc", "source_ref", "log_id", "entity_role",
        "entity_identifier", "log_name", "value"),
}
STATISTICS = ("count", "distinct", "sum", "mean", "min", "max", "quantile")
FIELD_TYPE_ALIASES = {
    "MetricField": tuple(COLUMN_FIELDS["MetricPointView"]),
    "SpanField": tuple(COLUMN_FIELDS["SpanView"]),
    "LogField": tuple(COLUMN_FIELDS["LogRecordView"]),
    "SourceField": ("rr", "sr", "cnt", "mrt", "value"),
    "Statistic": STATISTICS,
    "RecordType": ("metric", "span", "log"),
}

HELPERS = (
    {"name": "telemetry_view", "signature": "telemetry_view(value)",
     "parameters": {"value": "Telemetry"}, "returns": "TelemetryView"},
    {"name": "metric_points", "signature": "metric_points(telemetry, source_table, window, entity_role=None, entity_identifier=None, source_field=None, metric_key=None, bomc_id=None, item_id=None, limit=10000, offset=0)",
     "parameters": {"telemetry": "TelemetryView", "source_table": "Literal['bank/log/log_service.csv', 'bank/metric_app.csv', 'bank/metric_container.csv', 'metric/metric_app.csv', 'metric/metric_container.csv', 'metric/metric_middleware.csv', 'metric/metric_node.csv', 'metric/metric_service.csv']", "window": "WindowView",
         "entity_role": "str | None", "entity_identifier": "str | None", "source_field": "SourceField | None",
         "metric_key": "str | None",
         "bomc_id": "str | None", "item_id": "str | None", "limit": "int", "offset": "int"},
     "returns": "QueryResult[MetricPointView]"},
    {"name": "span_records", "signature": "span_records(telemetry, source_table, window, component_role=None, component_identifier=None, limit=10000, offset=0)",
     "parameters": {"telemetry": "TelemetryView", "source_table": "Literal['bank/trace_span.csv', 'trace/trace_span.csv']", "window": "WindowView",
         "component_role": "str | None",
         "component_identifier": "str | None", "limit": "int", "offset": "int"},
     "returns": "QueryResult[SpanView]"},
    {"name": "log_records", "signature": "log_records(telemetry, source_table, window, entity_identifier=None, log_name=None, limit=10000, offset=0)",
     "parameters": {"telemetry": "TelemetryView", "source_table": "Literal['bank/log/log_service.csv']", "window": "WindowView",
         "entity_identifier": "str | None", "log_name": "str | None", "limit": "int", "offset": "int"},
     "returns": "QueryResult[LogRecordView]"},
    {"name": "aggregate_records", "signature": "aggregate_records(telemetry, source_table, window, record_type, field, statistic, group_by=(), time_bucket_ms=None, quantile=None, limit=1000, offset=0)",
     "parameters": {"telemetry": "TelemetryView", "source_table": "Literal['bank/log/log_service.csv', 'bank/metric_app.csv', 'bank/metric_container.csv', 'bank/trace_span.csv', 'metric/metric_app.csv', 'metric/metric_container.csv', 'metric/metric_middleware.csv', 'metric/metric_node.csv', 'metric/metric_service.csv', 'trace/trace_span.csv']", "window": "WindowView", "record_type": "RecordType", "field": "MetricField | SpanField | LogField", "statistic": "Statistic", "group_by": "Sequence[MetricField | SpanField | LogField]", "time_bucket_ms": "int | None", "quantile": "float | None", "limit": "int", "offset": "int"},
     "returns": "QueryResult[AggregateGroupView]"},
    {"name": "group_by", "signature": "group_by(rows, fields)",
     "parameters": {"rows": "Sequence[MetricPointView] | Sequence[SpanView] | Sequence[LogRecordView]", "fields": "Sequence[MetricField | SpanField | LogField]"},
     "returns": "Mapping[tuple[str | int | float | bool | None, ...], Sequence[MetricPointView] | Sequence[SpanView] | Sequence[LogRecordView]]"},
    {"name": "summarize", "signature": "summarize(values, statistic, quantile=None)",
     "parameters": {"values": "Sequence[float | int | None]", "statistic": "Statistic", "quantile": "float | None"},
     "returns": "SummaryResult"},
    {"name": "top_k", "signature": "top_k(rows, field, k, descending=True)",
     "parameters": {"rows": "Sequence[MetricPointView] | Sequence[SpanView] | Sequence[LogRecordView]", "field": "MetricField | SpanField | LogField", "k": "int", "descending": "bool"},
     "returns": "Sequence[MetricPointView] | Sequence[SpanView] | Sequence[LogRecordView]"},
)

SOURCE_TABLES = {
    "bank/metric_app.csv", "bank/metric_container.csv", "bank/trace_span.csv",
    "bank/log/log_service.csv", "metric/metric_app.csv", "metric/metric_container.csv",
    "metric/metric_middleware.csv", "metric/metric_node.csv", "metric/metric_service.csv",
    "trace/trace_span.csv",
}
METRIC_SOURCE_TABLES = ("bank/log/log_service.csv", "bank/metric_app.csv", "bank/metric_container.csv",
    "metric/metric_app.csv", "metric/metric_container.csv", "metric/metric_middleware.csv",
    "metric/metric_node.csv", "metric/metric_service.csv")
SPAN_SOURCE_TABLES = ("bank/trace_span.csv", "trace/trace_span.csv")
LOG_SOURCE_TABLES = ("bank/log/log_service.csv",)


def operator_code_contract_hash() -> str:
    payload = {"profile": PROFILE, "record_fields": RECORD_FIELDS, "helpers": HELPERS,
        "source_tables": sorted(SOURCE_TABLES), "column_fields": {key: list(value) for key, value in COLUMN_FIELDS.items()},
        "field_type_aliases": {key: list(value) for key, value in FIELD_TYPE_ALIASES.items()},
        "statistics": list(STATISTICS), "query_limit": MAX_QUERY_ROWS,
        "source_limit_bytes": MAX_SOURCE_BYTES, "data_manifest_version": "e4-operator-code-data/2",
        "resource_limits": {"query_rows": MAX_QUERY_ROWS, "groups": MAX_GROUPS,
            "aggregate_page_rows": MAX_AGGREGATE_PAGE_ROWS, "artifact_bytes": MAX_ARTIFACT_BYTES,
            "artifact_rows": MAX_ARTIFACT_ROWS}}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def typed_operator_code_api() -> dict:
    """Return JSON-safe authoring metadata derived from the public registry."""
    return {
        "schema_version": "e4-typed-operator-code/1", "authoring_profile": PROFILE,
        "helpers": [dict(row) for row in HELPERS], "record_fields": RECORD_FIELDS,
        "annotation_types": ["Inputs", "Params", "Runtime", "Output", "Telemetry",
            "TelemetryView", "WindowView", "MetricPointView", "LogRecordView", "SpanView", "CoverageView",
            "QueryResult", "SummaryResult", "AggregateGroupView", "MetricField", "SpanField",
            "LogField", "SourceField", "Statistic", "RecordType", "SourceTable", "Sequence", "Mapping"],
        "source_tables": sorted(SOURCE_TABLES), "field_type_aliases": {key: list(value) for key, value in FIELD_TYPE_ALIASES.items()}, "query_limit": MAX_QUERY_ROWS,
        "source_limit_bytes": MAX_SOURCE_BYTES,
        "resource_limits": {"query_rows": MAX_QUERY_ROWS, "groups": MAX_GROUPS,
            "aggregate_page_rows": MAX_AGGREGATE_PAGE_ROWS, "artifact_bytes": MAX_ARTIFACT_BYTES,
            "artifact_rows": MAX_ARTIFACT_ROWS},
        "data_manifest_version": "e4-operator-code-data/2",
        "api_sha256": operator_code_contract_hash(),
        "semantics": {"coverage": "each result reports exact total matches, support, completeness, and pagination",
            "nulls": "missing metric values remain None; generic summaries omit None and report the included support through query coverage",
            "metric_fields": "source_field names the native value column (container 'value'; app 'rr/sr/cnt/mrt'); metric_key/metric_name retains the observed series label (container KPI name). Select an observed KPI by metric_key, never by source_field.",
            "logs": "Bank log rows are exposed only after source hash verification; absent values remain None and value text is not interpreted",
            "aggregation": "generic grouping and summaries only; authored source implements any novel statistic"},
    }


def typed_operator_code_guidance() -> str:
    return ("In typed-operator-code-v1, consume registered typed inputs directly. "
        "For a Telemetry port, create its read-only view with telemetry_view(inputs[port]); "
        "query typed metric, span, log, or full-window aggregate records using exact source table IDs and an explicit window. "
        "For native metrics, source_field is the value column (for example container 'value'); metric_key/metric_name is the observed series label such as a container KPI name. "
        "Results include coverage and pagination; inspect coverage.complete before treating absence as evidence. "
        "Bank log rows expose source log_id, component identity, log_name, timestamp, and nullable value. "
        "Use generic grouping, "
        "summaries, and ordinary Python loops/math/statistics to implement the operator's analysis. "
        "All scalar record fields and helper signatures are listed in sandbox.typed_operator_code_api.")


@dataclasses.dataclass(frozen=True)
class PreparedOperatorCodeInputs:
    wire_inputs: dict[str, Any]
    case_dir: Path | None
    trusted_context: Mapping[str, Any] | None = None


def _wire_value(value: Any, *, variadic: bool = False) -> Any:
    from .cache import canonical_json
    if variadic:
        if not isinstance(value, (list, tuple)):
            raise ValueError("variadic generated input must be a list or tuple")
        return [json.loads(canonical_json(item).decode("utf-8")) for item in value]
    return json.loads(canonical_json(value).decode("utf-8"))


def _telemetries(value: Any) -> list["Telemetry"]:
    from .contracts import Telemetry
    if isinstance(value, Telemetry):
        return [value]
    if isinstance(value, (list, tuple)):
        return [item for nested in value for item in _telemetries(nested)]
    return []


def _write_jsonl(path: Path, rows: Iterator[Mapping[str, Any]]) -> tuple[int, int, str]:
    digest = hashlib.sha256()
    count = size = 0
    with path.open("wb") as stream:
        for row in rows:
            data = (json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                allow_nan=False) + "\n").encode("utf-8")
            stream.write(data)
            digest.update(data)
            count += 1
            size += len(data)
    return count, size, digest.hexdigest()


def _duration_value(raw: str) -> tuple[float | None, str]:
    if raw == "":
        return None, "missing"
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None, "invalid"
    if not math.isfinite(value):
        return None, "nonfinite"
    return value, "valid"


def _opaque_row_ref(case_digest: str, source: str, row_key: str) -> str:
    return hashlib.sha256((case_digest + "\0" + source + "\0" + row_key).encode("utf-8")).hexdigest()


def _source_table_from_hash_path(relative: str) -> str | None:
    if relative.startswith("source_dates/"):
        candidate = "bank/" + relative.split("/", 2)[-1]
    else:
        candidate = relative
    return candidate if candidate in SOURCE_TABLES else None


def _public_log_rows(telemetry: "Telemetry", public_data_root: Path) -> Iterator[Mapping[str, Any]]:
    # Only hash-pinned files named by the already validated public Telemetry
    # contract are considered. Truth/evaluator inputs are never searched.
    root = public_data_root.resolve()
    expected_header = ("log_id", "timestamp", "cmdb_id", "log_name", "value")
    for relative, expected_sha in sorted(telemetry.file_sha256.items()):
        if not (relative.startswith("source_dates/") and relative.endswith("/log/log_service.csv")):
            continue
        candidates = (root / relative, root / "cases" / telemetry.case_id / relative)
        raw_source = next((candidate for candidate in candidates if candidate.is_file()), None)
        if raw_source is None:
            raise ValueError("hash-pinned public log source is unavailable under its fixed root")
        source = raw_source.resolve(strict=True)
        if root.resolve() not in source.parents or not source.is_file():
            raise ValueError("hash-pinned public log source escaped its fixed root")
        digest = hashlib.sha256()
        with source.open("rb") as raw:
            for chunk in iter(lambda: raw.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected_sha:
            raise ValueError("hash-pinned public log source digest mismatch")
        with source.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != expected_header:
                raise ValueError("hash-pinned public log source schema mismatch")
            for line_number, row in enumerate(reader, 2):
                if set(row) != set(expected_header):
                    raise ValueError("hash-pinned public log row shape mismatch")
                try:
                    millis = int(Decimal(row["timestamp"]) * 1000)
                except (InvalidOperation, ValueError) as exc:
                    raise ValueError("hash-pinned public log timestamp is invalid") from exc
                if not (telemetry.reference_window.contains(millis) or telemetry.alert_window.contains(millis)):
                    continue
                yield {"source_table": "bank/log/log_service.csv", "timestamp_ms_utc": millis,
                    "source_ref": _opaque_row_ref(telemetry.data_sha256, relative, str(line_number)),
                    "log_id": row["log_id"] or None, "entity_role": "component" if row["cmdb_id"] else None,
                    "entity_identifier": row["cmdb_id"] or None, "log_name": row["log_name"] or None,
                    "value": row["value"] if row["value"] != "" else None}


@contextlib.contextmanager
def prepare_operator_code_inputs(inputs: Mapping[str, Any], *,
        public_data_root: Path | None = None,
        input_ports: Any = None) -> Iterator[PreparedOperatorCodeInputs]:
    """Create an ephemeral public row store and an opaque typed handle."""
    from .contracts import Telemetry
    variadic_names = {row["name"] for row in input_ports or ()
        if isinstance(row, Mapping) and row.get("variadic") is True}
    telemetry_values = [item for value in inputs.values() for item in _telemetries(value)]
    if not telemetry_values:
        yield PreparedOperatorCodeInputs({name: _wire_value(value, variadic=name in variadic_names)
            for name, value in inputs.items()}, None)
        return
    telemetry = telemetry_values[0]
    identity = (telemetry.case_id, telemetry.data_sha256,
        telemetry.reference_window, telemetry.alert_window, tuple(sorted(telemetry.file_sha256.items())))
    if any((value.case_id, value.data_sha256, value.reference_window, value.alert_window,
            tuple(sorted(value.file_sha256.items()))) != identity for value in telemetry_values[1:]):
        raise ValueError("typed-operator-code-v1 Telemetry aliases must refer to the same case, windows, and source hashes")
    with tempfile.TemporaryDirectory(prefix="e4-operator-code-") as raw_dir:
        root = Path(raw_dir)
        bank = (any(point.table.startswith("bank/") for point in telemetry.metrics) or
            any(relative.startswith("source_dates/") for relative in telemetry.file_sha256))
        span_table = "bank/trace_span.csv" if bank else "trace/trace_span.csv"
        metric_count, metric_bytes, metric_sha = _write_jsonl(root / "metrics.jsonl", iter(
            {"source_table": point.table, "timestamp_ms_utc": point.timestamp_ms_utc,
             "entity_role": point.entity.role, "entity_identifier": point.entity.identifier,
             "origin_row_id": _opaque_row_ref(telemetry.data_sha256, point.table,
                point.row_key.rsplit(":", 1)[0] if point.table.endswith("metric_app.csv") and
                    point.metric_name in {"rr", "sr", "cnt", "mrt"} and point.row_key.endswith(":" + point.metric_name) else point.row_key),
             "metric_key": point.metric_name,
            "source_field": (None if point.table.endswith("/log_service.csv") and point.metric_name == "log_volume_count" else
                point.metric_name if point.table.endswith("metric_app.csv") else "value"),
             "metric_name": point.metric_name,
             "representation": ("derived_zero_filled_minute_count" if point.table.endswith("/log_service.csv") and point.metric_name == "log_volume_count" else "source_metric_point"),
             "bomc_id": point.bomc_id, "item_id": point.item_id,
             "value": point.value} for point in telemetry.metrics))
        def spans():
            for index, span in enumerate(telemetry.spans):
                def identity(entity):
                    return (None, None) if entity is None else (entity.role, entity.identifier)
                exec_role, exec_id = identity(span.executing_component)
                service_role, service_id = identity(span.logical_service)
                db_role, db_id = identity(span.database_target)
                yield {"source_table": span_table, "timestamp_ms_utc": span.start_ms_utc,
                    "source_ref": _opaque_row_ref(telemetry.data_sha256, span_table, str(index)),
                    "trace_id": span.trace_id, "span_id": span.span_id, "parent_id": span.parent_id,
                    "duration_value": _duration_value(span.elapsed_raw)[0],
                    "duration_status": _duration_value(span.elapsed_raw)[1],
                    "duration_unit": "undeclared",
                    "call_type": None if span.call_type == "unprovided" else span.call_type,
                    "executing_component_role": exec_role, "executing_component_identifier": exec_id,
                    "logical_service_role": service_role, "logical_service_identifier": service_id,
                    "database_target_role": db_role, "database_target_identifier": db_id,
                    "parent_resolved_in_clip": span.parent_resolved_in_clip,
                    "parent_unresolved_in_clip": span.parent_unresolved_in_clip}
        span_count, span_bytes, span_sha = _write_jsonl(root / "spans.jsonl", iter(spans()))
        public_root = (public_data_root if public_data_root is not None else
            Path(__file__).resolve().parents[1] / "data/prepared/bank/public")
        log_count, log_bytes, log_sha = _write_jsonl(root / "logs.jsonl",
            iter(_public_log_rows(telemetry, public_root)))
        file_meta = {"metrics.jsonl": {"rows": metric_count, "bytes": metric_bytes, "sha256": metric_sha},
            "spans.jsonl": {"rows": span_count, "bytes": span_bytes, "sha256": span_sha},
            "logs.jsonl": {"rows": log_count, "bytes": log_bytes, "sha256": log_sha}}
        has_public_log_source = any(relative.startswith("source_dates/") and
            relative.endswith("/log/log_service.csv") for relative in telemetry.file_sha256)
        metric_sources = sorted({point.table for point in telemetry.metrics} |
            {table for relative in telemetry.file_sha256
                if (table := _source_table_from_hash_path(relative)) is not None and
                    table.startswith(("bank/metric_", "metric/metric_"))})
        span_sources = sorted(({span_table} if telemetry.spans else set()) |
            {table for relative in telemetry.file_sha256
                if (table := _source_table_from_hash_path(relative)) is not None and table.endswith("trace_span.csv")})
        source_tables = sorted(set(metric_sources) | set(span_sources) |
            ({"bank/log/log_service.csv"} if has_public_log_source else set()))
        manifest = {"schema_version": "e4-operator-code-data/2", "case_id": telemetry.case_id,
            "data_sha256": telemetry.data_sha256, "reference_window": {
                "name": telemetry.reference_window.name, "start_ms_utc": telemetry.reference_window.start_ms_utc,
                "end_ms_utc": telemetry.reference_window.end_ms_utc}, "alert_window": {
                "name": telemetry.alert_window.name, "start_ms_utc": telemetry.alert_window.start_ms_utc,
                "end_ms_utc": telemetry.alert_window.end_ms_utc},
            "source_tables": source_tables, "files": file_meta,
            "record_sources": {"metric": metric_sources, "span": span_sources,
                "log": ["bank/log/log_service.csv"] if has_public_log_source else []},
            "metric_count": metric_count, "span_count": span_count, "log_count": log_count,
            "point_count": metric_count + span_count + log_count}
        manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        manifest_path = root / "operator_code_manifest.json"
        manifest_path.write_bytes(manifest_bytes)
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        handle = {"$type": "TelemetryHandle", "$fields": {
            "case_id": telemetry.case_id, "data_sha256": telemetry.data_sha256,
            "reference_window": manifest["reference_window"],
            "alert_window": manifest["alert_window"],
            "source_tables": manifest["source_tables"], "point_count": manifest["point_count"],
            "manifest_sha256": manifest_sha}}
        def convert(value, variadic=False):
            if isinstance(value, Telemetry):
                identity = (value.case_id, value.data_sha256, value.reference_window, value.alert_window,
                    tuple(sorted(value.file_sha256.items())))
                if identity != (telemetry.case_id, telemetry.data_sha256, telemetry.reference_window,
                        telemetry.alert_window, tuple(sorted(telemetry.file_sha256.items()))):
                    raise ValueError("Telemetry alias differs from the prepared public row store")
                return handle
            if variadic:
                if not isinstance(value, (list, tuple)):
                    raise ValueError("variadic generated input must be a list or tuple")
                return [convert(item) for item in value]
            if _telemetries(value):
                raise ValueError("Telemetry inputs must be a direct port value or an item in a variadic port")
            return _wire_value(value)
        wire: dict[str, Any] = {}
        for name, value in inputs.items():
            wire[name] = convert(value, variadic=name in variadic_names)
        yield PreparedOperatorCodeInputs(wire, root)
