"""Deterministic serial DAG execution with inspectable provenance and content caching."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping

from .cache import CacheStore, digest_value, implementation_fingerprint
from .contracts import (Aggregate, CandidateSet, Case, ExecutionResult, NodeRecord,
                        RankedAnswer, RuntimeSnapshot, ScoredCandidates,
                        Telemetry, TelemetryWindows, Window)

# Raw data types: hashed from their source hashes rather than serialized, and never cached.
RAW_DATA_TYPES = ("Telemetry", "Case", "TelemetryWindows")


def _data_digest(value: Any) -> str:
    """A digest for any value. Raw data is identified by its telemetry's source hashes and its
    windows' bounds, so the full record set is never serialized just to hash it."""
    def window_identity(window):
        return [window.window_id, window.telemetry.data_sha256, window.start_ms, window.end_ms]
    if isinstance(value, Telemetry):
        return value.data_sha256
    if isinstance(value, Case):
        return digest_value({"case_id": value.case_id, "anomaly": value.anomaly,
                             "components": [[c.role, c.identifier] for c in value.components],
                             "windows": [window_identity(w) for w in value.windows]})
    if isinstance(value, TelemetryWindows):
        return digest_value([window_identity(w) for w in value.windows])
    return digest_value(value)
from .dag import DAGValidationError, VerifiedDAG, verify_dag
from .registry import OperatorRegistry
from .runtime import RuntimeContext, RuntimeResolutionError, _jsonable, _freeze, resolve_parameters


class ExecutionFailure(RuntimeError):
    def __init__(self, message: str, records: tuple[NodeRecord, ...], error: Mapping[str, Any]):
        super().__init__(message)
        self.records = records
        self.error = dict(error)

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.error.get("code", "execution_error"),
                "message": str(self), "node_id": self.error.get("node_id"),
                "records": [node_record_to_dict(item) for item in self.records]}


@dataclass(frozen=True, slots=True)
class OperatorResult:
    value: Any
    rows_processed: int = 0
    bytes_processed: int = 0

    def __post_init__(self) -> None:
        if self.rows_processed < 0 or self.bytes_processed < 0:
            raise ValueError("operator resource counters cannot be negative")


@dataclass(frozen=True, slots=True)
class NodeContext:
    case_id: str
    node_id: str
    operator: str
    runtime_values: Mapping[str, Any]
    runtime_provenance: Mapping[str, str]
    reference_window: Window | None
    alert_window: Window | None
    data_sha256: str


class SerialExecutor:
    def __init__(self, registry: OperatorRegistry, cache: CacheStore | None = None,
                 *, dag_schema_version: str = "e4-dag/1"):
        self.registry = registry
        default_cache_root = (Path(os.environ["E4_RUN_DIR"]) / "core_cache"
                              if os.environ.get("E4_RUN_DIR") else Path("artifacts/core_cache"))
        self.cache = cache or CacheStore(default_cache_root, enabled=False)
        self.dag_schema_version = dag_schema_version

    def execute_document(self, document: Mapping[str, Any], runtime: RuntimeContext,
                         external_loader: Callable[[], Mapping[str, Any]],
                         *, cache_enabled: bool = True) -> ExecutionResult:
        """Verify the graph before calling the loader for potentially large case data."""
        max_nodes = int(runtime.snapshot.values.get("max_nodes", 40))
        max_depth = int(runtime.snapshot.values.get("dag_depth", 12))
        verified = verify_dag(document, self.registry, runtime,
                              max_nodes=max_nodes, max_depth=max_depth)
        external = external_loader()
        return self.execute_verified(verified, runtime, external,
                                     cache_enabled=cache_enabled)

    def execute_verified(self, dag: VerifiedDAG, runtime: RuntimeContext,
                         external_values: Mapping[str, Any], *,
                         cache_enabled: bool = True,
                         case_id: str | None = None,
                         data_sha256: str | None = None,
                         file_sha256: Mapping[str, str] | None = None) -> ExecutionResult:
        values: dict[str, Any] = {}
        digests: dict[str, str] = {}
        records: list[NodeRecord] = []
        hits = 0
        node_by_id = {node.node_id: node for node in dag.nodes}
        unknown_external = set(external_values) - set(node_by_id)
        if unknown_external:
            raise ExecutionFailure("external values refer to nodes outside the verified DAG", (),
                {"code": "unknown_external_node", "node_id": None,
                 "node_ids": sorted(unknown_external)})
        invalid_external = [node_id for node_id in external_values if not node_by_id[node_id].operator.external_input]
        if invalid_external:
            raise ExecutionFailure("external injection is restricted to declared external source operators", (),
                {"code": "external_override_forbidden", "node_id": invalid_external[0],
                 "node_ids": sorted(invalid_external)})
        telemetry = next((value for value in external_values.values() if isinstance(value, Telemetry)), None)
        external_fingerprints = {key: _data_digest(value) for key, value in external_values.items()}
        if telemetry:
            case_id = case_id or telemetry.case_id
            file_sha256 = file_sha256 or telemetry.file_sha256
        case = next((value for value in external_values.values() if isinstance(value, Case)), None)
        if case:
            case_id = case_id or case.case_id
        case_id = case_id or "synthetic"
        if not data_sha256:
            data_sha256 = (telemetry.data_sha256 if telemetry and len(external_fingerprints) == 1
                           else digest_value(external_fingerprints))
        file_sha256 = file_sha256 or {}
        reference = runtime.snapshot.values.get("reference_window")
        alert = runtime.snapshot.values.get("alert_window")
        if telemetry:
            if ((reference is not None and reference != telemetry.reference_window) or
                    (alert is not None and alert != telemetry.alert_window)):
                raise ExecutionFailure("runtime windows differ from hash-verified case windows", (),
                                       {"code": "runtime_window_mismatch", "node_id": None})
            reference, alert = telemetry.reference_window, telemetry.alert_window
        context_values = runtime.snapshot.values
        runtime_json = {"schema_version": runtime.snapshot.schema_version,
                        "values": _jsonable(context_values),
                        "provenance": dict(runtime.snapshot.provenance),
                        "measured_graph_depth": dag.max_depth,
                        "measured_node_count": dag.measured_node_count}
        source_fingerprint = implementation_fingerprint()
        runtime_json["implementation_fingerprint"] = source_fingerprint
        effective_cache = CacheStore(self.cache.root,
                                     enabled=self.cache.enabled and cache_enabled)
        for node in dag.nodes:
            spec = node.operator
            started_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            started = time.perf_counter()
            input_values: dict[str, Any] = {}
            input_digests: dict[str, Any] = {}
            for port, refs in node.inputs.items():
                if not refs:
                    input_values[port] = None
                    input_digests[port] = []
                elif len(refs) == 1 and not next(item for item in spec.inputs if item.name == port).variadic:
                    input_values[port] = values[refs[0]]
                    input_digests[port] = [digests[refs[0]]]
                else:
                    input_values[port] = tuple(values[ref] for ref in refs)
                    input_digests[port] = [digests[ref] for ref in refs]
            if node.node_id in external_values:
                input_digests["$external"] = external_fingerprints[node.node_id]
            resolved = None
            cache_key = ""
            cache_status = "failure_not_cached"
            try:
                needs_estimator = any(
                    parameter.estimator and _parameter_requests_auto(name, parameter, node, runtime)
                    for name, parameter in spec.parameters.items())
                resolved = resolve_parameters(spec.parameters, node.params, node.bindings,
                                              runtime, self.registry.estimators,
                                              reference_data=_reference_only(telemetry) if needs_estimator else None)
                cache_key = effective_cache.key(
                    case_id=case_id, data_sha256=data_sha256, file_sha256=file_sha256,
                    windows={"reference": reference, "alert": alert},
                    schema_version=dag.schema_version, adapter_version=(telemetry.adapter_version if telemetry else "synthetic/1"),
                    runtime_snapshot=runtime_json, operator=spec.name, operator_version=spec.version,
                    parameters=resolved.values, parameter_provenance=resolved.provenance,
                    input_digests=input_digests, implementation_hash=source_fingerprint)
                can_cache_output = spec.cacheable and spec.output_type not in RAW_DATA_TYPES
                cached, cached_digest, cache_status = effective_cache.get(cache_key, spec.output_type) if can_cache_output else (None, None, "not_cacheable")
                if cache_status == "hit":
                    output, rows_processed, bytes_processed = cached, 0, 0
                    output_digest = cached_digest or digest_value(output)
                    hits += 1
                else:
                    if node.node_id in external_values:
                        raw_result = external_values[node.node_id]
                    elif spec.implementation is not None:
                        node_context = NodeContext(case_id, node.node_id, spec.qualified_name,
                                                  runtime.snapshot.values, runtime.snapshot.provenance,
                                                  reference, alert, data_sha256)
                        raw_result = spec.implementation(MappingProxyType(input_values),
                                                         resolved.values, node_context)
                    else:
                        raise RuntimeResolutionError("missing_operator_implementation",
                                                     f"operator {spec.qualified_name} has no implementation or external value",
                                                     node.node_id)
                    if isinstance(raw_result, OperatorResult):
                        output, rows_processed, bytes_processed = raw_result.value, raw_result.rows_processed, raw_result.bytes_processed
                    else:
                        output, rows_processed, bytes_processed = raw_result, _row_count(raw_result), 0
                    _validate_output(spec.output_type, output, node.node_id)
                    output_digest = _data_digest(output)
                    if can_cache_output:
                        stored = effective_cache.put(cache_key, spec.output_type, output)
                        if stored == "disabled":
                            cache_status = "disabled"
                        elif stored == "not_cacheable":
                            cache_status = "not_cacheable"
                        else:
                            cache_status = "miss_stored" if cache_status == "miss" else cache_status
                    else:
                        cache_status = "not_cacheable" if spec.cacheable is False or spec.output_type in RAW_DATA_TYPES else cache_status
                values[node.node_id] = output
                digests[node.node_id] = output_digest
                records.append(NodeRecord(
                    node_id=node.node_id, operator=spec.name, version=spec.version,
                    status="success", input_digests=MappingProxyType(input_digests),
                    resolved_params=resolved.values, parameter_provenance=resolved.provenance,
                    runtime_snapshot=MappingProxyType(runtime_json), started_at=started_iso,
                    elapsed_ms=(time.perf_counter() - started) * 1000,
                    rows_processed=rows_processed, bytes_processed=bytes_processed,
                    cache_key=cache_key, cache_status=cache_status,
                    output_digest=output_digest,
                    implementation_fingerprint=source_fingerprint))
            except Exception as exc:
                elapsed = (time.perf_counter() - started) * 1000
                structured = (exc.as_dict() if hasattr(exc, "as_dict") else
                              {"code": "operator_exception", "message": str(exc),
                               "exception_type": type(exc).__name__})
                structured["node_id"] = node.node_id
                records.append(NodeRecord(
                    node_id=node.node_id, operator=spec.name, version=spec.version,
                    status="failed", input_digests=MappingProxyType(input_digests),
                    resolved_params=resolved.values if resolved else {},
                    parameter_provenance=resolved.provenance if resolved else {},
                    runtime_snapshot=MappingProxyType(runtime_json), started_at=started_iso,
                    elapsed_ms=elapsed, rows_processed=0, bytes_processed=0,
                    cache_key=cache_key, cache_status="failure_not_cached", output_digest=None,
                    implementation_fingerprint=source_fingerprint,
                    error=MappingProxyType(structured)))
                raise ExecutionFailure(f"node {node.node_id} failed: {exc}", tuple(records), structured) from exc
        answer = values[dag.return_node]
        if not isinstance(answer, RankedAnswer):
            raise ExecutionFailure("verified return did not produce RankedAnswer", tuple(records),
                                   {"code": "return_type", "node_id": dag.return_node})
        limit = int(runtime.snapshot.values.get("lead_count", 5))
        if len(answer.leads) > limit:
            raise ExecutionFailure(f"ranked answer exceeds runtime lead_count {limit}", tuple(records),
                                   {"code": "lead_count", "node_id": dag.return_node})
        return ExecutionResult(answer, tuple(records), hits, case_id, source_fingerprint)


def _validate_output(output_type: str, value: Any, node_id: str) -> None:
    expected = {"Telemetry": Telemetry, "Case": Case, "TelemetryWindows": TelemetryWindows,
                "Aggregate": Aggregate, "CandidateSet": CandidateSet,
                "ScoredCandidates": ScoredCandidates, "RankedAnswer": RankedAnswer}.get(output_type)
    if expected is None or not isinstance(value, expected):
        raise RuntimeResolutionError("operator_output_type",
                                     f"node {node_id} must return {output_type}", node_id)


def _row_count(value: Any) -> int:
    if isinstance(value, Telemetry):
        return len(value.metrics) + len(value.spans) + len(value.logs)
    if isinstance(value, Aggregate):
        return sum(item.support for item in value.rows)
    if isinstance(value, CandidateSet):
        return len(value.candidates)
    if isinstance(value, ScoredCandidates):
        return len(value.candidates)
    if isinstance(value, RankedAnswer):
        return len(value.leads)
    return 0


def _reference_only(telemetry: Telemetry | None) -> Any:
    if telemetry is None:
        return None
    # A bounded view object prevents estimators from seeing alert-window samples.
    return MappingProxyType({
        "window": telemetry.reference_window,
        "metrics": tuple(point for point in telemetry.metrics
                          if telemetry.reference_window.contains(point.timestamp_ms_utc)),
        "spans": tuple(span for span in telemetry.spans
                        if telemetry.reference_window.contains(span.start_ms_utc)),
        "logs": tuple(record for record in telemetry.logs
                       if telemetry.reference_window.contains(record.timestamp_ms_utc)),
    })


def _parameter_requests_auto(name, parameter, node, runtime) -> bool:
    if name in node.params:
        return node.params[name] == "auto"
    field = node.bindings.get(name)
    if field is not None and field.startswith("runtime."):
        return runtime.snapshot.values.get(field.removeprefix("runtime.")) == "auto"
    if parameter.runtime_field:
        return runtime.snapshot.values.get(parameter.runtime_field) == "auto"
    return False


def node_record_to_dict(record: NodeRecord) -> dict[str, Any]:
    return {"node_id": record.node_id, "operator": record.operator, "version": record.version,
            "status": record.status, "input_digests": _jsonable(record.input_digests),
            "resolved_params": _jsonable(record.resolved_params),
            "parameter_provenance": dict(record.parameter_provenance),
            "runtime_snapshot": _jsonable(record.runtime_snapshot),
            "started_at": record.started_at, "elapsed_ms": record.elapsed_ms,
            "rows_processed": record.rows_processed, "bytes_processed": record.bytes_processed,
            "cache_key": record.cache_key, "cache_status": record.cache_status,
            "output_digest": record.output_digest,
            "implementation_fingerprint": record.implementation_fingerprint,
            "error": _jsonable(record.error) if record.error else None}


def execution_result_to_dict(result: ExecutionResult) -> dict[str, Any]:
    from .cache import _encode
    return {"case_id": result.case_id, "answer": _encode(result.answer),
            "records": [node_record_to_dict(item) for item in result.records],
            "cache_hits": result.cache_hits,
            "implementation_fingerprint": result.implementation_fingerprint}
