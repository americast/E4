"""Operator specifications, ports, parameters, and central operator registry."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence


def _freeze(val: Any) -> Any:
    if isinstance(val, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in val.items()})
    if isinstance(val, (list, tuple)):
        return tuple(_freeze(item) for item in val)
    return val


def _jsonable(val: Any) -> Any:
    if isinstance(val, Mapping):
        return {str(k): _jsonable(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_jsonable(item) for item in val]
    return val


@dataclass(frozen=True, slots=True)
class PortSpec:
    name: str
    value_type: str
    required: bool = True
    variadic: bool = False
    minimum_items: int = 1
    maximum_items: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.value_type,
            "required": self.required,
            "variadic": self.variadic,
            "minimum_items": self.minimum_items,
            "maximum_items": self.maximum_items,
        }


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
            raise ValueError("Parameter constraints must be a mapping")
        constraints = dict(self.constraints)
        for key in ("minimum", "maximum"):
            if key in constraints:
                bound = constraints[key]
                if type(bound) not in (int, float) or not math.isfinite(bound):
                    raise ValueError(f"Constraint {key} must be a finite number")
        object.__setattr__(self, "constraints", _freeze(constraints))
        if self.has_default:
            object.__setattr__(self, "default", _freeze(self.default))

    def as_dict(self) -> dict[str, Any]:
        res: dict[str, Any] = {
            "type": self.value_type,
            "required": self.required,
            "nullable": self.nullable,
            "has_default": self.has_default,
            "default": _jsonable(self.default) if self.has_default else None,
            "choices": list(self.choices),
            "runtime_field": self.runtime_field,
            "estimator": self.estimator,
            "description": self.description,
        }
        if self.constraints:
            res["constraints"] = _jsonable(self.constraints)
        return res


OperatorFunction = Callable[[Mapping[str, Any], Mapping[str, Any], Any], Any]


@dataclass(frozen=True, slots=True)
class OperatorSpec:
    name: str
    version: str
    inputs: tuple[PortSpec, ...]
    output_type: str
    parameters: Mapping[str, ParameterSpec] = field(default_factory=dict)
    family: str = "utility"
    cacheable: bool = True
    estimator_names: tuple[str, ...] = ()
    implementation: OperatorFunction | None = None
    description: str = ""
    external_input: bool = False
    authoring_profile: str | None = None
    source: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", tuple(self.inputs))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        object.__setattr__(self, "estimator_names", tuple(self.estimator_names))
        if self.external_input and (self.inputs or self.implementation is not None):
            raise ValueError("External input operators must have no DAG inputs or implementation")

    @property
    def qualified_name(self) -> str:
        return f"{self.name}@{self.version}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "inputs": [p.as_dict() for p in self.inputs],
            "output_type": self.output_type,
            "family": self.family,
            "external_input": self.external_input,
            "parameters": {k: v.as_dict() for k, v in self.parameters.items()},
            "description": self.description,
            "authoring_profile": self.authoring_profile,
        }


class OperatorRegistry:
    """Registry holding all registered baseline and evolved operators."""

    def __init__(self) -> None:
        self._operators: dict[tuple[str, str], OperatorSpec] = {}
        self._estimators: dict[str, Callable[[Any], Any]] = {}

    def register(self, operator: OperatorSpec) -> None:
        key = (operator.name, operator.version)
        if key in self._operators:
            raise ValueError(f"Duplicate operator registration: {operator.qualified_name}")
        port_names = [p.name for p in operator.inputs]
        if len(set(port_names)) != len(port_names):
            raise ValueError(f"Duplicate input port for {operator.qualified_name}")
        self._operators[key] = operator

    def get(self, name: str, version: str) -> OperatorSpec | None:
        return self._operators.get((name, version))

    def require(self, name: str, version: str) -> OperatorSpec:
        op = self.get(name, version)
        if op is None:
            raise KeyError(f"Unknown operator: {name}@{version}")
        return op

    def has(self, name: str, version: str) -> bool:
        return (name, version) in self._operators

    def register_estimator(self, name: str, estimator: Callable[[Any], Any]) -> None:
        if not name or name in self._estimators:
            raise ValueError(f"Duplicate or invalid estimator name: {name!r}")
        self._estimators[name] = estimator

    def signatures(self) -> list[dict[str, Any]]:
        return [
            op.as_dict()
            for op in sorted(self._operators.values(), key=lambda o: (o.name, o.version))
        ]

    def all_operators(self) -> list[OperatorSpec]:
        return list(self._operators.values())

    def clone(self) -> OperatorRegistry:
        new_reg = OperatorRegistry()
        for op in self._operators.values():
            new_reg.register(op)
        for name, est in self._estimators.items():
            new_reg.register_estimator(name, est)
        return new_reg
