"""Content-addressed JSON cache for immutable core contract values."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import platform
import tempfile
import types
from dataclasses import MISSING, fields, is_dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Union, get_args, get_origin, get_type_hints

from e4.contracts import (Aggregate, AggregateRow, Candidate, CandidateSet, Case, EntityRef,
                          Evidence, ExecutionResult, LogRecord, MetricPoint, NodeRecord,
                          RankedAnswer, RuntimeSnapshot, ScoredCandidate, ScoredCandidates, Span,
                          Telemetry, TelemetryWindow, TelemetryWindows, Window, TelemetryWindowRef)
from e4.paths import ROOT


_CONTRACT_TYPES = {item.__name__: item for item in
                   (Aggregate, AggregateRow, Candidate, CandidateSet, Case, EntityRef, Evidence,
                    ExecutionResult, LogRecord, MetricPoint, NodeRecord, RankedAnswer,
                    RuntimeSnapshot, ScoredCandidate, ScoredCandidates, Span, Telemetry,
                    TelemetryWindow, TelemetryWindows, Window, TelemetryWindowRef)}


def deserialize_contract_json(value: Any, expected_type: str) -> Any:
    """Decode a worker's JSON-serialized core value through strict contracts.

    Generated code cannot instantiate trusted objects directly. A typed value
    must carry the cache wire envelope and pass dataclass invariants here.
    """
    expected = _CONTRACT_TYPES.get(expected_type)
    if expected is None:
        raise ValueError(f"unsupported core contract type {expected_type}")
    if (not isinstance(value, dict) or value.get("$type") != expected_type
            or set(value) != {"$type", "$fields"}
            or not isinstance(value.get("$fields"), dict)
            or not _fields_match(value["$fields"], expected)):
        raise ValueError(f"malformed serialized {expected_type} contract")
    _validate_wire_envelopes(value)
    decoded = _decode(value)
    if not isinstance(decoded, expected):
        raise TypeError(f"decoded value is not {expected_type}")
    _validate_annotation(decoded, expected)
    return decoded


def _fields_match(given: Mapping[str, Any], contract: type) -> bool:
    """Every field without a default is present, and nothing else is; defaulted fields may be
    omitted, so envelopes written before a defaulted field was added still decode."""
    names = {field.name for field in fields(contract)}
    required = {field.name for field in fields(contract)
                if field.default is MISSING and field.default_factory is MISSING}
    return required <= set(given) <= names


def _validate_wire_envelopes(value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            _validate_wire_envelopes(item)
        return
    if not isinstance(value, dict):
        return
    if "$type" in value:
        type_name = value.get("$type")
        contract = _CONTRACT_TYPES.get(type_name) if isinstance(type_name, str) else None
        if (contract is None or set(value) != {"$type", "$fields"}
                or not isinstance(value.get("$fields"), dict)
                or not _fields_match(value["$fields"], contract)):
            raise ValueError("malformed nested core contract envelope")
        for item in value["$fields"].values():
            _validate_wire_envelopes(item)
    elif "$map" in value:
        if set(value) != {"$map"} or not isinstance(value["$map"], dict):
            raise ValueError("malformed serialized mapping envelope")
        _validate_wire_envelopes(value["$map"])
    elif "$tuple" in value:
        if set(value) != {"$tuple"} or not isinstance(value["$tuple"], list):
            raise ValueError("malformed serialized tuple envelope")
        _validate_wire_envelopes(value["$tuple"])
    else:
        for item in value.values():
            _validate_wire_envelopes(item)


def _validate_annotation(value: Any, annotation: Any) -> None:
    """Check decoded values recursively against trusted dataclass annotations."""
    if annotation is Any:
        _validate_json_value(value)
        return
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (Union, types.UnionType):
        for option in args:
            try:
                _validate_annotation(value, option)
                return
            except (TypeError, ValueError):
                pass
        raise TypeError(f"value does not match union {annotation}")
    is_mapping = origin is Mapping
    if origin is not None and not is_mapping:
        try:
            is_mapping = issubclass(origin, Mapping)
        except TypeError:
            is_mapping = False
    if is_mapping:
        if not isinstance(value, Mapping):
            raise TypeError(f"expected mapping for {annotation}")
        key_type, value_type = args or (Any, Any)
        for key, item in value.items():
            _validate_annotation(key, key_type)
            _validate_annotation(item, value_type)
        return
    if origin is tuple or annotation is tuple:
        if not isinstance(value, tuple):
            raise TypeError(f"expected tuple for {annotation}")
        if args and args[-1] is not Ellipsis:
            if len(value) != len(args):
                raise TypeError(f"tuple length mismatch for {annotation}")
            for item, item_type in zip(value, args):
                _validate_annotation(item, item_type)
        elif args:
            for item in value:
                _validate_annotation(item, args[0])
        return
    if origin is list or annotation is list:
        if type(value) is not list:
            raise TypeError(f"expected list for {annotation}")
        if args:
            for item in value:
                _validate_annotation(item, args[0])
        return
    if origin is dict or annotation is dict:
        if not isinstance(value, Mapping):
            raise TypeError(f"expected mapping for {annotation}")
        key_type, value_type = args or (Any, Any)
        for key, item in value.items():
            _validate_annotation(key, key_type)
            _validate_annotation(item, value_type)
        return
    if annotation is float:
        if type(value) not in (int, float) or not math.isfinite(float(value)):
            raise TypeError("expected finite non-boolean number")
        return
    if annotation is int:
        if type(value) is not int:
            raise TypeError("expected non-boolean integer")
        return
    if annotation is bool:
        if type(value) is not bool:
            raise TypeError("expected boolean")
        return
    if is_dataclass(annotation):
        if not isinstance(value, annotation):
            raise TypeError(f"expected {annotation.__name__}")
        hints = get_type_hints(annotation)
        for field in fields(annotation):
            _validate_annotation(getattr(value, field.name), hints[field.name])
        return
    if annotation in (str,):
        if type(value) is not annotation:
            raise TypeError(f"expected {annotation.__name__}")
        return
    if isinstance(annotation, type) and not isinstance(value, annotation):
        raise TypeError(f"expected {annotation.__name__}")


def _validate_json_value(value: Any) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise TypeError("non-finite JSON value")
        return
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise TypeError("JSON object keys must be strings")
        for item in value.values():
            _validate_json_value(item)
        return
    if isinstance(value, (tuple, list)):
        for item in value:
            _validate_json_value(item)
        return
    raise TypeError(f"unsupported JSON value {type(value).__name__}")


def canonical_json(value: Any) -> bytes:
    return json.dumps(_encode(value), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest_value(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def implementation_fingerprint() -> str:
    """Hash all local core modules plus the reused trusted metadata parser."""
    root = ROOT
    files = sorted((root / "src/e4").rglob("*.py")) + [
        root / "src/e4/prep/telecom.py",
        root / "config/runtime/schema.json",
        root / "config/runtime/values.json",
        root / "config/records/decisions.json",
        root / "config/telecom/metric_kind_policy.json",
        root / "config/telecom/kpi_policy.json",
        root / "pyproject.toml",
        root / "requirements.lock",
    ]
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    for package in ("numpy", "scipy", "scikit-learn", "joblib", "threadpoolctl", "cloudpickle"):
        try:
            version = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            version = "missing"
        digest.update(f"{package}=={version}\0".encode())
    digest.update(f"python=={platform.python_version()}\0".encode())
    return digest.hexdigest()


def _encode(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {"$type": value.__class__.__name__,
                "$fields": {field.name: _encode(getattr(value, field.name)) for field in fields(value)}}
    if isinstance(value, Mapping):
        return {"$map": {str(key): _encode(item) for key, item in value.items()}}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(item) for item in value]}
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and not __import__("math").isfinite(value):
            raise ValueError("cache values cannot contain nonfinite numbers")
        return value
    raise TypeError(f"unsupported cache value type: {type(value).__name__}")


def _decode(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode(item) for item in value]
    if isinstance(value, dict):
        if "$map" in value:
            return MappingProxyType({key: _decode(item) for key, item in value["$map"].items()})
        if "$tuple" in value:
            return tuple(_decode(item) for item in value["$tuple"])
        if "$type" in value:
            class_ = _CONTRACT_TYPES.get(value["$type"])
            if class_ is None:
                raise ValueError(f"unknown cached contract type {value['$type']}")
            return class_(**{key: _decode(item) for key, item in value["$fields"].items()})
        return {key: _decode(item) for key, item in value.items()}
    return value


class CacheStore:
    def __init__(self, root: Path, enabled: bool = True):
        self.root = root
        self.enabled = enabled

    def key(self, *, case_id: str, data_sha256: str, file_sha256: Mapping[str, str],
            windows: Mapping[str, Any], schema_version: str, adapter_version: str,
            runtime_snapshot: Mapping[str, Any], operator: str, operator_version: str,
            parameters: Mapping[str, Any], parameter_provenance: Mapping[str, str],
            input_digests: Mapping[str, Any], implementation_hash: str) -> str:
        payload = {"case_id": case_id, "data_sha256": data_sha256,
                   "file_sha256": file_sha256, "windows": windows,
                   "schema_version": schema_version, "adapter_version": adapter_version,
                   "runtime_snapshot": runtime_snapshot, "operator": operator,
                   "operator_version": operator_version, "parameters": parameters,
                   "parameter_provenance": parameter_provenance,
                   "input_digests": input_digests,
                   "implementation_fingerprint": implementation_hash}
        return hashlib.sha256(canonical_json(payload)).hexdigest()

    def get(self, key: str, expected_type: str) -> tuple[Any | None, str | None, str]:
        if not self.enabled:
            return None, None, "disabled"
        path = self.root / f"{key}.json"
        if not path.is_file():
            return None, None, "miss"
        try:
            raw = path.read_bytes()
            envelope = json.loads(raw)
            payload_bytes = json.dumps(envelope["payload"], sort_keys=True, separators=(",", ":"),
                                       ensure_ascii=False, allow_nan=False).encode("utf-8")
            if envelope.get("key") != key or envelope.get("output_type") != expected_type:
                raise ValueError("cache key or type mismatch")
            if hashlib.sha256(payload_bytes).hexdigest() != envelope.get("payload_sha256"):
                raise ValueError("cache payload checksum mismatch")
            value = _decode(envelope["payload"])
            if not _is_output_type(value, expected_type):
                raise ValueError("decoded cache value type mismatch")
            return value, hashlib.sha256(payload_bytes).hexdigest(), "hit"
        except Exception:
            return None, None, "corrupt_recomputed"

    def put(self, key: str, output_type: str, value: Any) -> str:
        if not self.enabled:
            return "disabled"
        if isinstance(value, Telemetry):
            return "not_cacheable"
        payload = _encode(value)
        payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                   ensure_ascii=False, allow_nan=False).encode("utf-8")
        envelope = {"key": key, "output_type": output_type,
                    "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
                    "payload": payload}
        self.root.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{key}.", suffix=".tmp", dir=self.root)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(envelope, stream, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False)
                stream.write("\n")
            os.replace(temporary_name, self.root / f"{key}.json")
        finally:
            Path(temporary_name).unlink(missing_ok=True)
        return hashlib.sha256(payload_bytes).hexdigest()


def _is_output_type(value: Any, output_type: str) -> bool:
    types = {"Telemetry": Telemetry, "Aggregate": Aggregate, "CandidateSet": CandidateSet,
             "ScoredCandidates": ScoredCandidates, "RankedAnswer": RankedAnswer}
    expected = types.get(output_type)
    return expected is not None and isinstance(value, expected)
