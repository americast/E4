"""Typed immutable runtime context and explicit argument resolution rules."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping

from .contracts import RuntimeSnapshot, Window


class RuntimeResolutionError(ValueError):
    def __init__(self, code: str, message: str, field: str | None = None):
        super().__init__(message)
        self.code = code
        self.field = field

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "field": self.field}


@dataclass(frozen=True, slots=True)
class RuntimeField:
    name: str
    value_type: str
    meaning: str
    scope: str
    required: bool = False
    nullable: bool = False
    default: Any = None
    has_default: bool = False
    provider: str | None = None
    writable: bool = True

    def __post_init__(self) -> None:
        if self.has_default:
            object.__setattr__(self, "default", _freeze(self.default))


class RuntimeSchema:
    def __init__(self, fields: Mapping[str, RuntimeField], version: str):
        self.fields = MappingProxyType(dict(fields))
        self.version = version

    @classmethod
    def from_json(cls, path: Path) -> "RuntimeSchema":
        raw = json.loads(path.read_text())
        fields = {}
        for name, value in raw["fields"].items():
            fields[name] = RuntimeField(
                name=name, value_type=value["type"], meaning=value["meaning"],
                scope=value.get("scope", "run"), required=value.get("required", False),
                nullable=value.get("nullable", False), default=value.get("default"),
                has_default="default" in value, provider=value.get("provider"),
                writable=value.get("writable", True))
        return cls(fields, raw["version"])

    def validate_value(self, name: str, value: Any) -> None:
        spec = self.fields.get(name)
        if spec is None:
            raise RuntimeResolutionError("unknown_runtime_field", f"unknown runtime field {name}", name)
        if value is None:
            if spec.nullable:
                return
            raise RuntimeResolutionError("null_not_allowed", f"runtime.{name} does not accept null", name)
        valid = {
            "string": lambda x: isinstance(x, str),
            "boolean": lambda x: type(x) is bool,
            "integer": lambda x: type(x) is int,
            "positive_integer": lambda x: type(x) is int and x > 0,
            "number": lambda x: type(x) in (int, float) and math.isfinite(float(x)),
            "number_map": lambda x: isinstance(x, Mapping) and all(
                isinstance(key, str) and type(item) in (int, float) and math.isfinite(float(item)) and float(item) >= 0
                for key, item in x.items()),
            "window": lambda x: isinstance(x, Window),
            "string_list": lambda x: isinstance(x, (list, tuple)) and all(isinstance(v, str) for v in x),
        }.get(spec.value_type)
        if valid is None:
            raise RuntimeResolutionError("schema_type_unsupported", f"unsupported runtime type {spec.value_type}", name)
        if not valid(value):
            raise RuntimeResolutionError("runtime_type_mismatch",
                                         f"runtime.{name} must have type {spec.value_type}", name)


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    snapshot: RuntimeSnapshot
    schema: RuntimeSchema

    @classmethod
    def build(cls, schema: RuntimeSchema, supplied: Mapping[str, Any],
              providers: Mapping[str, Any] | None = None) -> "RuntimeContext":
        providers = providers or {}
        unknown = set(supplied) - set(schema.fields)
        unknown_providers = set(providers) - set(schema.fields)
        if unknown:
            name = sorted(unknown)[0]
            raise RuntimeResolutionError("unknown_runtime_field", f"unknown runtime field {name}", name)
        if unknown_providers:
            name = sorted(unknown_providers)[0]
            raise RuntimeResolutionError("unknown_runtime_field", f"unknown runtime provider field {name}", name)
        values: dict[str, Any] = {}
        provenance: dict[str, str] = {}
        for name, spec in schema.fields.items():
            if name in supplied:
                if not spec.writable:
                    raise RuntimeResolutionError("runtime_field_read_only", f"runtime.{name} is read-only", name)
                value, source = supplied[name], "explicit_runtime"
            elif name in providers:
                value, source = providers[name], f"provider:{spec.provider or 'case_context'}"
            elif spec.has_default:
                value, source = spec.default, "schema_default"
            elif spec.required:
                raise RuntimeResolutionError("missing_required_runtime_field",
                                             f"required runtime.{name} has no value, provider, or default", name)
            else:
                continue
            schema.validate_value(name, value)
            _validate_runtime_semantics(name, value)
            values[name] = _freeze(value)
            provenance[name] = source
        return cls(RuntimeSnapshot(MappingProxyType(values), MappingProxyType(provenance), schema.version), schema)

    def as_dict(self) -> dict[str, Any]:
        return {"schema_version": self.snapshot.schema_version,
                "values": {key: _jsonable(value) for key, value in self.snapshot.values.items()},
                "provenance": dict(self.snapshot.provenance)}


def build_case_runtime(case_metadata: Mapping[str, Any], *,
                       schema_path: Path | None = None,
                       config_path: Path | None = None,
                       overrides: Mapping[str, Any] | None = None) -> RuntimeContext:
    """Build runtime from pinned defaults and public case windows only."""
    root = Path(__file__).resolve().parents[1]
    schema = RuntimeSchema.from_json(schema_path or root / "config/runtime_schema.json")
    supplied = json.loads((config_path or root / "config/core_runtime.json").read_text())
    supplied.update(overrides or {})
    reference = Window("reference", int(case_metadata["analysis_window_start_epoch_ms_utc"]),
                       int(case_metadata["alert_window_start_epoch_ms_utc"]))
    alert = Window("alert", int(case_metadata["alert_window_start_epoch_ms_utc"]),
                   int(case_metadata["alert_window_end_epoch_ms_utc"]))
    return RuntimeContext.build(schema, supplied,
                                {"reference_window": reference, "alert_window": alert})


@dataclass(frozen=True, slots=True)
class ParameterSpec:
    name: str
    value_type: str
    required: bool = False
    nullable: bool = False
    default: Any = None
    has_default: bool = False
    runtime_field: str | None = None
    estimator: str | None = None
    description: str = ""
    choices: tuple[Any, ...] = ()
    constraints: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "choices", tuple(self.choices))
        if not isinstance(self.constraints, Mapping):
            raise ValueError("parameter constraints must be a mapping")
        constraints = dict(self.constraints)
        allowed_constraints = {"minimum", "maximum", "exclusive_minimum", "exclusive_maximum"}
        unknown = set(constraints) - allowed_constraints
        if unknown:
            raise ValueError(f"unsupported parameter constraint key(s): {sorted(unknown)}")
        for key in ("minimum", "maximum"):
            if key in constraints:
                bound = constraints[key]
                if type(bound) not in (int, float) or (type(bound) is float and not math.isfinite(bound)):
                    raise ValueError(f"parameter constraint {key} must be a finite number")
        for key in ("exclusive_minimum", "exclusive_maximum"):
            if key in constraints and type(constraints[key]) is not bool:
                raise ValueError(f"parameter constraint {key} must be a boolean")
        if ("exclusive_minimum" in constraints and "minimum" not in constraints) or (
                "exclusive_maximum" in constraints and "maximum" not in constraints):
            raise ValueError("exclusive parameter constraints require their corresponding bound")
        if "minimum" in constraints and "maximum" in constraints:
            low, high = constraints["minimum"], constraints["maximum"]
            if low > high or (low == high and (constraints.get("exclusive_minimum") or
                                                constraints.get("exclusive_maximum"))):
                raise ValueError("parameter constraint interval is empty")
        object.__setattr__(self, "constraints", _freeze(constraints))
        if self.has_default:
            object.__setattr__(self, "default", _freeze(self.default))


@dataclass(frozen=True, slots=True)
class ResolvedParameters:
    values: Mapping[str, Any]
    provenance: Mapping[str, str]


def resolve_parameters(specs: Mapping[str, ParameterSpec], explicit: Mapping[str, Any],
                       bindings: Mapping[str, str], runtime: RuntimeContext,
                       estimators: Mapping[str, Callable[[Any], Any]] | None = None,
                       reference_data: Any = None) -> ResolvedParameters:
    """Resolve explicit > binding > default, with `auto` limited to declared estimators."""
    estimators = estimators or {}
    unknown = (set(explicit) | set(bindings)) - set(specs)
    if unknown:
        name = sorted(unknown)[0]
        raise RuntimeResolutionError("unknown_parameter", f"unsupported parameter {name}", name)
    for name, field in bindings.items():
        if not isinstance(field, str) or not field.startswith("runtime."):
            raise RuntimeResolutionError("invalid_runtime_binding",
                                         f"binding for {name} must name runtime.<field>", name)
        field_name = field.removeprefix("runtime.")
        if field_name not in runtime.schema.fields:
            raise RuntimeResolutionError("unknown_runtime_field",
                                         f"binding for {name} refers to undeclared runtime.{field_name}",
                                         field_name)
    for name, spec in specs.items():
        if spec.runtime_field and spec.runtime_field not in runtime.schema.fields:
            raise RuntimeResolutionError("unknown_runtime_field",
                                         f"parameter {name} refers to undeclared runtime.{spec.runtime_field}",
                                         spec.runtime_field)
    values, provenance = {}, {}
    for name, spec in specs.items():
        if name in explicit:
            value, source = explicit[name], "explicit_argument"
        elif name in bindings:
            field = bindings[name]
            if not isinstance(field, str) or not field.startswith("runtime."):
                raise RuntimeResolutionError("invalid_runtime_binding",
                                             f"binding for {name} must name runtime.<field>", name)
            field_name = field.removeprefix("runtime.")
            if field_name not in runtime.snapshot.values:
                raise RuntimeResolutionError("missing_required_runtime_field",
                                             f"binding {name} requires unavailable runtime.{field_name}", field_name)
            value, source = runtime.snapshot.values[field_name], field
        elif spec.runtime_field:
            field_name = spec.runtime_field
            if field_name not in runtime.snapshot.values:
                if spec.has_default:
                    value, source = spec.default, "static_default_after_missing_runtime"
                else:
                    raise RuntimeResolutionError("missing_required_runtime_field",
                                                 f"parameter {name} requires runtime.{field_name}", field_name)
            else:
                value, source = runtime.snapshot.values[field_name], f"runtime.{field_name}"
        elif spec.has_default:
            value, source = spec.default, "static_default"
        elif spec.required:
            raise RuntimeResolutionError("missing_required_parameter",
                                         f"required parameter {name} is unresolved", name)
        else:
            continue
        if value == "auto":
            if not spec.estimator:
                raise RuntimeResolutionError("auto_not_supported",
                                             f"parameter {name} has no declared estimator", name)
            estimate = estimators.get(spec.estimator)
            if estimate is None:
                raise RuntimeResolutionError("estimator_unavailable",
                                             f"estimator {spec.estimator} is not registered", name)
            value = estimate(reference_data)
            source += f" -> auto:{spec.estimator}:reference_window"
        _validate_parameter(spec, value)
        values[name], provenance[name] = _freeze(value), source
    return ResolvedParameters(MappingProxyType(values), MappingProxyType(provenance))


def _validate_parameter(spec: ParameterSpec, value: Any) -> None:
    if value is None:
        if spec.nullable:
            return
        raise RuntimeResolutionError("null_not_allowed", f"parameter {spec.name} does not accept null", spec.name)
    valid = {
        "string": lambda x: isinstance(x, str),
        "boolean": lambda x: type(x) is bool,
        "integer": lambda x: type(x) is int,
        "positive_integer": lambda x: type(x) is int and x > 0,
        "number": lambda x: type(x) in (int, float) and math.isfinite(float(x)),
        "number_map": lambda x: isinstance(x, Mapping) and all(
            isinstance(key, str) and type(item) in (int, float) and math.isfinite(float(item)) and float(item) >= 0
            for key, item in x.items()),
        "string_list": lambda x: isinstance(x, (list, tuple)) and all(isinstance(v, str) for v in x),
    }.get(spec.value_type)
    if valid is None:
        raise RuntimeResolutionError("schema_type_unsupported", f"unsupported parameter type {spec.value_type}", spec.name)
    if not valid(value):
        raise RuntimeResolutionError("parameter_type_mismatch",
                                     f"parameter {spec.name} must have type {spec.value_type}", spec.name)
    if spec.choices and value not in spec.choices:
        raise RuntimeResolutionError("parameter_value_not_allowed",
                                     f"parameter {spec.name} must be one of {spec.choices}", spec.name)
    constraints = spec.constraints
    if type(value) in (int, float) and constraints:
        minimum = constraints.get("minimum")
        maximum = constraints.get("maximum")
        if minimum is not None and (value < minimum or
                (constraints.get("exclusive_minimum") is True and value == minimum)):
            raise RuntimeResolutionError("parameter_value_below_minimum",
                f"parameter {spec.name} must be {'greater than' if constraints.get('exclusive_minimum') else 'at least'} {minimum}", spec.name)
        if maximum is not None and (value > maximum or
                (constraints.get("exclusive_maximum") is True and value == maximum)):
            raise RuntimeResolutionError("parameter_value_above_maximum",
                f"parameter {spec.name} must be {'less than' if constraints.get('exclusive_maximum') else 'at most'} {maximum}", spec.name)


def _validate_runtime_semantics(name: str, value: Any) -> None:
    bounds = {
        "flow_min_r2": (0.0, 1.0, False),
        "flow_min_contribution_fraction": (0.0, 1.0, False),
        "trace_propagation_weight": (0.0, 1.0, False),
        "deployment_host_weight": (0.0, 1.0, False),
        "maximum_change_p_value": (0.0, 1.0, False),
        "tail_quantile": (0.0, 1.0, True),
        "permutation_p_score_floor": (0.0, 1.0, True),
        "relative_scale_floor": (0.0, None, False),
        "absolute_scale_floor": (0.0, None, True),
        "flow_alpha": (0.0, None, True),
        "minimum_normalized_effect": (0.0, None, False),
    }
    if name in bounds:
        minimum, maximum, exclusive_min = bounds[name]
        numeric = float(value)
        if (numeric < minimum or (exclusive_min and numeric == minimum)
                or (name == "tail_quantile" and numeric >= 1.0)
                or (maximum is not None and numeric > maximum)):
            raise RuntimeResolutionError("runtime_value_out_of_range",
                                         f"runtime.{name} is outside its declared numeric range", name)


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, Window):
        return {"name": value.name, "start_ms_utc": value.start_ms_utc,
                "end_ms_utc": value.end_ms_utc}
    if isinstance(value, Mapping):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value
