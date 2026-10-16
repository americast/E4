"""Versioned operator signatures; executable implementations are registered separately."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Mapping

from .runtime import ParameterSpec


@dataclass(frozen=True, slots=True)
class PortSpec:
    name: str
    value_type: str
    required: bool = True
    variadic: bool = False
    minimum_items: int = 1
    maximum_items: int | None = None


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", tuple(self.inputs))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        object.__setattr__(self, "estimator_names", tuple(self.estimator_names))
        if self.external_input and (self.inputs or self.implementation is not None):
            raise ValueError("external input operators must have no DAG inputs or implementation")

    @property
    def qualified_name(self) -> str:
        return f"{self.name}@{self.version}"


class OperatorRegistry:
    def __init__(self) -> None:
        self._operators: dict[tuple[str, str], OperatorSpec] = {}
        self._estimators: dict[str, Callable[[Any], Any]] = {}

    def register(self, operator: OperatorSpec) -> None:
        key = (operator.name, operator.version)
        if key in self._operators:
            raise ValueError(f"duplicate operator registration: {operator.qualified_name}")
        if len({port.name for port in operator.inputs}) != len(operator.inputs):
            raise ValueError(f"duplicate input port for {operator.qualified_name}")
        self._operators[key] = operator

    def get(self, name: str, version: str) -> OperatorSpec | None:
        return self._operators.get((name, version))

    def require(self, name: str, version: str) -> OperatorSpec:
        operator = self.get(name, version)
        if operator is None:
            raise KeyError(f"unknown operator/version: {name}@{version}")
        return operator

    def register_estimator(self, name: str, estimator: Callable[[Any], Any]) -> None:
        if not name or name in self._estimators:
            raise ValueError(f"duplicate or empty estimator name {name!r}")
        self._estimators[name] = estimator

    @property
    def estimators(self) -> Mapping[str, Callable[[Any], Any]]:
        return dict(self._estimators)

    def signatures(self) -> list[dict[str, Any]]:
        return [{"name": item.name, "version": item.version,
                 "inputs": [{"name": port.name, "type": port.value_type,
                             "required": port.required, "variadic": port.variadic,
                             "minimum_items": port.minimum_items,
                             "maximum_items": port.maximum_items}
                            for port in item.inputs],
                 "output_type": item.output_type, "family": item.family,
                 "external_input": item.external_input,
                 "parameters": {name: {"type": spec.value_type,
                                        "required": spec.required,
                                        "nullable": spec.nullable,
                                        "has_default": spec.has_default,
                                        "default": spec.default if spec.has_default else None,
                                        "choices": list(spec.choices),
                                        "runtime_field": spec.runtime_field,
                                        "estimator": spec.estimator,
                                        "description": spec.description}
                                for name, spec in item.parameters.items()},
                 "description": item.description}
                for item in sorted(self._operators.values(), key=lambda op: (op.name, op.version))]
