"""Bounded, label-blind public Bank clip queries for M4-generated analyses."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

from .bank_adapter import BankCase, _timestamp_ms, sha256_file
from .bank_ops import bank_registry, combine_bank_scores
from .cache import canonical_json
from .contracts import Aggregate, Telemetry
from .executor import NodeContext, OperatorResult
from .m4_projection import register_m4_projection
from .m4_generated import register_generated_specs
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec, _jsonable


ROOT = Path(__file__).resolve().parents[1]
TABLE_SCHEMAS: Mapping[str, tuple[str, ...]] = {
    "metric/metric_app.csv": ("timestamp", "rr", "sr", "cnt", "mrt", "tc"),
    "metric/metric_container.csv": ("timestamp", "cmdb_id", "kpi_name", "value"),
    "trace/trace_span.csv": ("timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"),
    "log/log_service.csv": ("log_id", "timestamp", "cmdb_id", "log_name", "value"),
}
MAX_REQUESTS = 32
MAX_ROWS_PER_REQUEST = 256
MAX_ROWS_PER_CASE = 2048
MAX_BYTES_PER_REQUEST = 512 * 1024
MAX_BYTES_PER_CASE = 2 * 1024 * 1024
MAX_STRING_BYTES = 4096
MAX_OFFSET = 100_000
MAX_ANALYSIS_WEIGHT = 5.0
MAX_SOURCE_BYTES_PER_QUERY = 512 * 1024 * 1024


class BankQueryError(ValueError):
    """A public case query is invalid, over budget, or fails source integrity checks."""


def bank_m4_analysis_weights(*, root: Path = ROOT,
                             overrides: Mapping[str, Any] | None = None) -> dict[str, float]:
    """Return Bank-policy defaults with explicitly bounded M4 runtime overrides."""
    policy_path = root / "config/bank_dev_kpi_policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise BankQueryError("unsupported Bank KPI family-weight policy")
    family_defaults = policy.get("family_weights", {})
    aliases = {"metric": "bank_metric", "trace_latency": "bank_trace_latency",
               "log_volume": "bank_log_volume"}
    result = {alias: float(family_defaults.get(family, 1.0))
              for alias, family in aliases.items()}
    supplied = dict(overrides or {})
    unknown = set(supplied) - set(aliases)
    if unknown:
        raise BankQueryError(f"unsupported M4 analysis weight(s): {sorted(unknown)}")
    for name, value in supplied.items():
        if (type(value) not in (int, float) or not math.isfinite(float(value)) or
                float(value) < 0 or float(value) > MAX_ANALYSIS_WEIGHT):
            raise BankQueryError(f"M4 analysis weight {name} must be between zero and {MAX_ANALYSIS_WEIGHT}")
        result[name] = float(value)
    for name, value in result.items():
        if not math.isfinite(value) or value < 0 or value > MAX_ANALYSIS_WEIGHT:
            raise BankQueryError(f"frozen Bank analysis weight {name} is outside the M4 weight bound")
    return result


class M4QueryBudget:
    """Mutable total-read budget reusable across regenerated per-case registries."""
    def __init__(self, audit_dir: Path | None):
        self.audit_dir = audit_dir
        self.lock = threading.Lock()
        self.requests = 0
        self.rows = 0
        self.bytes = 0
        self.case_id: str | None = None

    def reserve(self, case_id: str, requested_limit: int) -> None:
        with self.lock:
            if self.case_id is None:
                self.case_id = case_id
            if self.case_id != case_id:
                raise BankQueryError("a query registry is scoped to one Bank case")
            if self.requests >= MAX_REQUESTS:
                raise BankQueryError("public query request budget exceeded")
            if type(requested_limit) is not int or not 1 <= requested_limit <= MAX_ROWS_PER_REQUEST:
                raise BankQueryError("public query limit must be between one and 256")
            if self.rows + requested_limit > MAX_ROWS_PER_CASE:
                raise BankQueryError("public query row budget exceeded")
            self.requests += 1

    def commit(self, case_id: str, rows: int, byte_count: int) -> None:
        with self.lock:
            if self.case_id != case_id:
                raise BankQueryError("query case scope changed during request")
            if type(rows) is not int or not 0 <= rows <= MAX_ROWS_PER_REQUEST:
                raise BankQueryError("public query returned more than its per-request row limit")
            if type(byte_count) is not int or not 0 <= byte_count <= MAX_BYTES_PER_REQUEST:
                raise BankQueryError("public query returned more than its per-request byte limit")
            if self.rows + rows > MAX_ROWS_PER_CASE:
                raise BankQueryError("public query row budget exceeded")
            if self.bytes + byte_count > MAX_BYTES_PER_CASE:
                raise BankQueryError("public query byte budget exceeded")
            self.rows += rows
            self.bytes += byte_count

    def audit(self, item: Mapping[str, Any]) -> None:
        if self.audit_dir is None:
            return
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        path = self.audit_dir / "public_query_calls.jsonl"
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(dict(item), ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False) + "\n")


def _params(table: str, window: str, entity: str | None, metric: str | None,
            offset: int, limit: int, fields: tuple[str, ...] | list[str] | None) -> dict[str, Any]:
    if table not in TABLE_SCHEMAS:
        raise BankQueryError(f"table must be one of {sorted(TABLE_SCHEMAS)}")
    if window not in {"reference", "alert"}:
        raise BankQueryError("window must be reference or alert")
    if type(offset) is not int or not 0 <= offset <= MAX_OFFSET:
        raise BankQueryError(f"offset must be an integer between zero and {MAX_OFFSET}")
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS_PER_REQUEST:
        raise BankQueryError(f"limit must be an integer between one and {MAX_ROWS_PER_REQUEST}")
    if entity is not None and (not isinstance(entity, str) or not entity or len(entity.encode()) > 256):
        raise BankQueryError("entity must be null or a non-empty observed identifier up to 256 bytes")
    if metric is not None and (not isinstance(metric, str) or not metric or len(metric.encode()) > 256):
        raise BankQueryError("metric must be null or a non-empty field/name up to 256 bytes")
    allowed_fields = set(TABLE_SCHEMAS[table]) | {"timestamp_ms_utc"}
    if fields is None:
        selected = list(TABLE_SCHEMAS[table]) + ["timestamp_ms_utc"]
    else:
        if not isinstance(fields, (tuple, list)) or not fields:
            raise BankQueryError("fields must be omitted or a non-empty list of source column names")
        if any(not isinstance(field, str) for field in fields):
            raise BankQueryError("fields must contain only strings")
        if len(fields) != len(set(fields)):
            raise BankQueryError("fields cannot contain duplicate column names")
        unknown = set(fields) - allowed_fields
        if unknown:
            raise BankQueryError(f"unsupported {table} field(s): {sorted(unknown)}")
        selected = list(fields)
        if "timestamp_ms_utc" not in selected:
            selected.append("timestamp_ms_utc")
    if metric is not None and table == "metric/metric_app.csv":
        if metric not in {"rr", "sr", "cnt", "mrt"}:
            raise BankQueryError("metric_app metric must name rr, sr, cnt, or mrt")
        if fields is None:
            selected = ["timestamp", metric, "tc", "timestamp_ms_utc"]
        elif set(selected) & ({"rr", "sr", "cnt", "mrt"} - {metric}):
            raise BankQueryError("metric_app fields cannot expose other metrics when one metric is selected")
    if metric is not None and table == "trace/trace_span.csv" and metric != "duration":
        raise BankQueryError("trace metric must be duration")
    return {"table": table, "window": window, "entity": entity, "metric": metric,
            "offset": offset, "limit": limit, "fields": selected}


def _case_files(telemetry: Telemetry, root: Path, table: str) -> tuple[Path, list[tuple[str, Path, str, str]]]:
    public_root = (root / "data/prepared/bank/public").resolve(strict=True)
    cases_root = (public_root / "cases").resolve(strict=True)
    case_id_path = Path(telemetry.case_id)
    if (not telemetry.case_id or case_id_path.name != telemetry.case_id or
            telemetry.case_id in {".", ".."}):
        raise BankQueryError("case ID is not a single public case directory name")
    case_dir = (cases_root / telemetry.case_id).resolve(strict=True)
    if cases_root not in case_dir.parents:
        raise BankQueryError("case directory escapes prepared public inputs")
    alert_path = (case_dir / "alert.json").resolve(strict=True)
    if case_dir not in alert_path.parents or not alert_path.is_file():
        raise BankQueryError("case alert metadata escapes prepared public case clips")
    if not alert_path.is_file() or sha256_file(alert_path) != telemetry.file_sha256.get("alert.json"):
        raise BankQueryError("hash-verified case alert metadata is unavailable")
    alert = json.loads(alert_path.read_text(encoding="utf-8"))
    if alert.get("incident_id") != telemetry.case_id:
        raise BankQueryError("case alert metadata identity mismatch")
    table_files = []
    dates = alert.get("telemetry_dates")
    units_by_date = alert.get("source_date_timestamp_units")
    if not isinstance(dates, list) or not isinstance(units_by_date, Mapping):
        raise BankQueryError("case alert metadata lacks pinned telemetry date/unit maps")
    for date in sorted(dates):
        relative = f"source_dates/{date}/{table}"
        expected_hash = telemetry.file_sha256.get(relative)
        unit = units_by_date.get(date, {}).get(table)
        if not expected_hash or unit not in {"seconds", "milliseconds"}:
            raise BankQueryError(f"case source metadata is incomplete for {relative}")
        path = (case_dir / relative).resolve(strict=True)
        if case_dir not in path.parents or not path.is_file():
            raise BankQueryError("case source path escapes prepared case clips")
        table_files.append((relative, path, expected_hash, unit))
    return alert_path, table_files


def query_bank_public(telemetry: Telemetry, *, root: Path = ROOT,
                      table: str, window: str, entity: str | None = None,
                      metric: str | None = None, offset: int = 0, limit: int = 64,
                      fields: tuple[str, ...] | list[str] | None = None) -> Aggregate:
    """Read one bounded native-column page from exactly one frozen case window.

    Raw log text is read only from that case's prepared clips and verified again
    against the hash-checked telemetry manifest. Other tables use the same
    immutable clip paths so the returned headers and raw values remain native.
    """
    request = _params(table, window, entity, metric, offset, limit, fields)
    selected = request["fields"]
    window_value = telemetry.reference_window if window == "reference" else telemetry.alert_window
    alert_path, files = _case_files(telemetry, root.resolve(strict=True), table)
    total_source_bytes = sum(path.stat().st_size for _relative, path, _sha, _unit in files)
    if total_source_bytes > MAX_SOURCE_BYTES_PER_QUERY:
        raise BankQueryError(f"public query source clips exceed {MAX_SOURCE_BYTES_PER_QUERY} bytes")
    source_hashes = {"alert.json": telemetry.file_sha256["alert.json"]}
    units: dict[str, str] = {}
    matching_count = 0
    page: list[dict[str, Any]] = []
    total_source_rows = 0
    max_field_chars = 4 * 1024 * 1024
    csv.field_size_limit(max_field_chars)
    for relative, path, expected_hash, unit in files:
        source_hashes[relative] = expected_hash
        units[relative] = unit
        if path.stat().st_size > 512 * 1024 * 1024:
            raise BankQueryError(f"prepared case source exceeds bounded query read size: {relative}")
        if sha256_file(path) != expected_hash:
            raise BankQueryError(f"prepared case source hash mismatch: {relative}")
        file_rows = 0
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != TABLE_SCHEMAS[table]:
                raise BankQueryError(f"native source header mismatch: {relative}")
            for row_number, row in enumerate(reader, start=2):
                file_rows += 1
                if None in row or any(value is None for value in row.values()):
                    raise BankQueryError(f"malformed native row in {relative} at line {row_number}")
                timestamp_ms = _timestamp_ms(row["timestamp"], unit, path, row_number)
                if not window_value.contains(timestamp_ms):
                    continue
                entity_field = "tc" if table == "metric/metric_app.csv" else "cmdb_id"
                if entity is not None and row.get(entity_field) != entity:
                    continue
                if metric is not None:
                    metric_field = {"metric/metric_app.csv": None,
                        "metric/metric_container.csv": "kpi_name",
                        "trace/trace_span.csv": "duration",
                        "log/log_service.csv": "log_name"}[table]
                    if table == "metric/metric_app.csv" or table == "trace/trace_span.csv":
                        pass
                    elif row.get(metric_field) != metric:
                        continue
                matching_count += 1
                if offset <= matching_count - 1 < offset + limit:
                    record = {field: row[field] if field != "timestamp_ms_utc" else timestamp_ms
                              for field in selected}
                    for field, value in record.items():
                        if isinstance(value, str) and len(value.encode("utf-8")) > MAX_STRING_BYTES:
                            raise BankQueryError(f"selected field {field} exceeds {MAX_STRING_BYTES} UTF-8 bytes")
                    page.append(record)
        total_source_rows += file_rows
        if sha256_file(path) != expected_hash:
            raise BankQueryError(f"prepared case source changed during query: {relative}")
    has_more = matching_count > offset + len(page)
    row_payload_bytes = len(json.dumps(_jsonable(page), ensure_ascii=False, sort_keys=True,
                                       separators=(",", ":"), allow_nan=False).encode("utf-8"))
    source_bytes_hashed = 2 * total_source_bytes
    output_metadata = {
        "public_query": {
            **request,
            "rows_data": page,
            "source_header": list(TABLE_SCHEMAS[table]),
            "canonical_extra_fields": ["timestamp_ms_utc"],
            "coverage": {"total_matching_rows": matching_count,
                         "returned_rows": len(page), "source_files": len(files),
                         "source_rows_scanned": total_source_rows,
                         "source_bytes_scanned": total_source_bytes,
                         "source_bytes_hashed": source_bytes_hashed,
                         "case_metadata_bytes_hashed": alert_path.stat().st_size,
                         "returned_bytes": row_payload_bytes,
                         "bytes_processed_definition": "CSV source scan bytes plus both SHA-256 source-file passes",
                         "omitted_before_rows": min(offset, matching_count),
                         "remaining_after_rows": max(0, matching_count - offset - len(page)),
                         "truncated": has_more,
                         "page_complete": not has_more,
                         "complete": offset == 0 and len(page) == matching_count},
            "next_offset": offset + len(page) if has_more else None,
            "source_hashes": dict(sorted(source_hashes.items())),
            "timestamp_units": dict(sorted(units.items())),
            "timestamp_field": "timestamp_ms_utc is normalized UTC milliseconds; source timestamp is preserved verbatim",
            "value_semantics": "native source columns only; no inferred metric unit or component alias",
        }}
    result = Aggregate(window_value, (), (f"bank/{table}",), output_metadata)
    encoded = canonical_json(result)
    if len(encoded) > MAX_BYTES_PER_REQUEST:
        raise BankQueryError(f"serialized public query page exceeds {MAX_BYTES_PER_REQUEST} bytes")
    return result


def _query_operator(registry: OperatorRegistry, *, root: Path,
                    audit_dir: Path | None, expected_case: BankCase | None,
                    shared_budget: M4QueryBudget | None = None) -> None:
    budget = shared_budget or M4QueryBudget(audit_dir)
    if audit_dir is not None and budget.audit_dir is None:
        budget.audit_dir = audit_dir
    params = {
        "table": ParameterSpec("table", "string", required=True,
                               choices=tuple(TABLE_SCHEMAS)),
        "window": ParameterSpec("window", "string", required=True,
                                choices=("reference", "alert")),
        "entity": ParameterSpec("entity", "string", nullable=True),
        "metric": ParameterSpec("metric", "string", nullable=True),
        "offset": ParameterSpec("offset", "integer", default=0, has_default=True),
        "limit": ParameterSpec("limit", "positive_integer", default=64, has_default=True),
        "fields": ParameterSpec("fields", "string_list", default=(), has_default=True),
    }

    def implementation(inputs: Mapping[str, Any], values: Mapping[str, Any], context: NodeContext) -> OperatorResult:
        telemetry = inputs.get("data")
        if not isinstance(telemetry, Telemetry):
            raise BankQueryError("bank.public.query requires hash-checked Telemetry")
        resolved_limit = int(values["limit"])
        audit = {"case_id": telemetry.case_id, "node_id": context.node_id,
                 "table": values["table"], "window": values["window"],
                 "offset": values["offset"], "limit": resolved_limit,
                 "status": "failed"}
        try:
            if expected_case is None and not telemetry.case_id.startswith(("synthetic-", "fixture-", "test-")):
                raise BankQueryError("a case-free M4 registry can query synthetic fixtures only")
            if expected_case is not None and (telemetry.case_id != expected_case.telemetry.case_id or
                    telemetry.data_sha256 != expected_case.telemetry.data_sha256):
                raise BankQueryError("query telemetry does not match the registry's public case")
            budget.reserve(telemetry.case_id, resolved_limit)
            fields_value = values.get("fields")
            query_fields = tuple(fields_value) if fields_value else None
            result = query_bank_public(telemetry, root=root, table=values["table"],
                window=values["window"], entity=values.get("entity"),
                metric=values.get("metric"), offset=int(values["offset"]),
                limit=resolved_limit, fields=query_fields)
            query_doc = result.metadata["public_query"]
            row_count = len(query_doc["rows_data"])
            byte_count = len(canonical_json(result))
            budget.commit(telemetry.case_id, row_count, byte_count)
            audit.update({"status": "success", "rows": row_count,
                          "returned_bytes": query_doc["coverage"]["returned_bytes"],
                          "serialized_query_bytes": byte_count,
                          "source_bytes_scanned": query_doc["coverage"]["source_bytes_scanned"],
                          "source_bytes_hashed": query_doc["coverage"]["source_bytes_hashed"],
                          "truncated": query_doc["coverage"]["truncated"],
                          "next_offset": query_doc["next_offset"],
                          "source_hashes": query_doc["source_hashes"]})
            return OperatorResult(result, row_count,
                query_doc["coverage"]["source_bytes_scanned"] +
                query_doc["coverage"]["source_bytes_hashed"])
        except Exception as exc:
            audit["error"] = {"type": type(exc).__name__, "message": str(exc)[:1000]}
            raise
        finally:
            budget.audit(_jsonable(audit))

    registry.register(OperatorSpec("bank.public.query", "1",
        (PortSpec("data", "Telemetry"),), "Aggregate", parameters=params,
        family="bank_public_query", implementation=implementation, cacheable=False,
        description="Bounded, hash-checked native-column page from one frozen case window; no cross-case or label access."))


def _register_m4_combiner(registry: OperatorRegistry, base: OperatorSpec,
                          base_signature: Mapping[str, Any], *, root: Path) -> None:
    family_aliases = {"metric": "bank_metric", "trace_latency": "bank_trace_latency",
                      "log_volume": "bank_log_volume"}
    defaults = bank_m4_analysis_weights(root=root)
    policy_path = root / "config/bank_dev_kpi_policy.json"
    version_content = {"base_signature": base_signature,
        "family_aliases": family_aliases, "default_analysis_weights": defaults,
        "bank_kpi_policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest(),
        "m4_data_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "weight_cap": MAX_ANALYSIS_WEIGHT}
    version_hash = hashlib.sha256(json.dumps(version_content, ensure_ascii=False,
        sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()[:16]
    version = f"1+m4.{version_hash}"

    def implementation(inputs: Mapping[str, Any], params: Mapping[str, Any],
                       context: NodeContext) -> Any:
        runtime_overrides = context.runtime_values.get("analysis_weights")
        aliases = bank_m4_analysis_weights(root=root, overrides=runtime_overrides)
        family_weights = {family: aliases[alias] for alias, family in family_aliases.items()}
        return combine_bank_scores(inputs, params, context, policy_path=policy_path,
                                   weights_override=family_weights)

    registry.register(replace(base, version=version, implementation=implementation,
        description=(base.description + " M4 wrapper resolves the frozen Bank family weights "
                     "from runtime.analysis_weights; unknown generated families use weight 1.")))


def generated_sandbox_limits(root: Path = ROOT) -> dict[str, Any]:
    """Return the frozen M4-only worker overrides; generic sandbox defaults stay independent."""
    policy = json.loads((root / "config/m4_runtime.json").read_text(encoding="utf-8"))
    limits = policy.get("query_and_generation", {})
    return {"input_bytes": int(limits["maximum_operator_input_bytes"]),
            "cpu_seconds": int(limits["generated_operator_cpu_seconds"]),
            "wall_seconds": float(limits["generated_operator_wall_seconds"]),
            "memory_bytes": int(limits["generated_operator_memory_bytes"])}


def make_m4_registry(case: BankCase | None, generated_specs: list[dict[str, Any]], *,
                     root: Path = ROOT, audit_dir: Path | None = None,
                     shared_query_budget: M4QueryBudget | None = None,
                     metric_policy_path: Path | None = None) -> OperatorRegistry:
    """Build the current Bank registry plus trusted query and generated operators.

    An explicit historical metric-policy path creates a legacy-only registry for
    replay; the default contains only current policy-bound operators.
    """
    root = root.resolve(strict=True)
    base_registry = bank_registry(root=root, metric_policy_path=metric_policy_path)
    signatures = base_registry.signatures()
    base_combiner_signature = next((row for row in signatures
                                    if row["name"] == "bank.answers.combine"), None)
    if base_combiner_signature is None:
        raise BankQueryError("Bank base registry has no answer combiner")
    base_combiner = base_registry.require("bank.answers.combine", base_combiner_signature["version"])
    registry = OperatorRegistry()
    for estimator_name, estimator in base_registry.estimators.items():
        registry.register_estimator(estimator_name, estimator)
    for signature in signatures:
        if signature["name"] == "bank.answers.combine":
            continue
        registry.register(base_registry.require(signature["name"], signature["version"]))
    _query_operator(registry, root=root, audit_dir=audit_dir, expected_case=case,
                    shared_budget=shared_query_budget)
    _register_m4_combiner(registry, base_combiner, base_combiner_signature, root=root)
    register_m4_projection(registry)
    sandbox_limits = generated_sandbox_limits(root) if generated_specs else None
    register_generated_specs(registry, generated_specs, audit_dir=audit_dir,
                             sandbox_limits=sandbox_limits)
    return registry
