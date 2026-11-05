"""Domain-agnostic DAG grammar, structural verifier, and topological executor."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence

from .registry import OperatorRegistry, OperatorSpec
from .types import ExecutionResult, NodeRecord


class DAGValidationError(ValueError):
    """Raised when a candidate DAG fails structural or semantic validation."""

    def __init__(
        self,
        code: str,
        message: str,
        node_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.node_id = node_id
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "node_id": self.node_id,
            "details": self.details,
        }


@dataclass(frozen=True, slots=True)
class VerifiedNode:
    node_id: str
    operator: OperatorSpec
    inputs: Mapping[str, tuple[str, ...]]
    params: Mapping[str, Any]
    bindings: Mapping[str, str]
    depth: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "inputs",
            MappingProxyType({k: tuple(v) for k, v in self.inputs.items()}),
        )
        object.__setattr__(self, "params", MappingProxyType(dict(self.params)))
        object.__setattr__(self, "bindings", MappingProxyType(dict(self.bindings)))


@dataclass(frozen=True, slots=True)
class VerifiedDAG:
    nodes: tuple[VerifiedNode, ...]
    return_node: str
    max_depth: int
    measured_node_count: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "e4-dag/1",
            "nodes": [
                {
                    "id": node.node_id,
                    "operator": node.operator.name,
                    "version": node.operator.version,
                    "inputs": {k: list(v) for k, v in node.inputs.items()},
                    "params": dict(node.params),
                    "bindings": dict(node.bindings),
                    "depth": node.depth,
                }
                for node in self.nodes
            ],
            "return_node": self.return_node,
            "max_depth": self.max_depth,
            "node_count": self.measured_node_count,
        }


def _validate_parameter_value(
    spec: Any, val: Any, node_id: str, param_name: str
) -> None:
    if val is None:
        if spec.nullable:
            return
        raise DAGValidationError(
            "null_parameter_forbidden",
            f"Parameter '{param_name}' does not allow null",
            node_id,
            {"parameter": param_name},
        )
    if spec.choices and val not in spec.choices:
        raise DAGValidationError(
            "parameter_value_not_allowed",
            f"Value {val!r} for '{param_name}' not in allowed choices: {spec.choices}",
            node_id,
            {"parameter": param_name, "choices": list(spec.choices), "value": val},
        )
    type_checks = {
        "string": lambda v: isinstance(v, str),
        "boolean": lambda v: type(v) is bool,
        "integer": lambda v: type(v) is int,
        "positive_integer": lambda v: type(v) is int and v > 0,
        "number": lambda v: type(v) in (int, float),
        "string_list": lambda v: isinstance(v, (list, tuple))
        and all(isinstance(x, str) for x in v),
    }
    check = type_checks.get(spec.value_type)
    if check and not check(val):
        raise DAGValidationError(
            "parameter_type_mismatch",
            f"Parameter '{param_name}' expected type {spec.value_type}, got {type(val).__name__}",
            node_id,
            {"parameter": param_name, "expected": spec.value_type},
        )


def _resolve_runtime_field(runtime: Any, field_name: str) -> Any:
    if runtime is None:
        return None
    if isinstance(runtime, Mapping) and field_name in runtime:
        return runtime[field_name]
    if hasattr(runtime, "snapshot") and hasattr(runtime.snapshot, "values") and field_name in runtime.snapshot.values:
        return runtime.snapshot.values[field_name]
    if hasattr(runtime, field_name):
        return getattr(runtime, field_name)
    return None


def verify_dag(
    document: Mapping[str, Any],
    registry: OperatorRegistry,
    runtime: Any = None,
    max_nodes: int = 40,
    max_depth: int = 12,
    external_inputs: Sequence[str] = ("telemetry", "data"),
) -> VerifiedDAG:
    """Validate graph topology, types, acyclicity, and parameter bindings."""
    external_set = set(external_inputs)
    if not isinstance(document, Mapping):
        raise DAGValidationError("document_must_be_mapping", "DAG document must be a JSON mapping")

    raw_nodes = document.get("nodes")
    return_node = document.get("return_node")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise DAGValidationError("missing_nodes", "DAG document must define non-empty 'nodes' list")
    if not isinstance(return_node, str) or not return_node:
        raise DAGValidationError("missing_return_node", "DAG document must define 'return_node'")

    if len(raw_nodes) > max_nodes:
        raise DAGValidationError(
            "too_many_nodes",
            f"DAG contains {len(raw_nodes)} nodes, exceeding maximum of {max_nodes}",
            details={"node_count": len(raw_nodes), "max_nodes": max_nodes},
        )

    # Check unique node IDs and operators
    node_ids: set[str] = set()
    node_docs: dict[str, Mapping[str, Any]] = {}
    operators: dict[str, OperatorSpec] = {}

    for node in raw_nodes:
        if not isinstance(node, Mapping):
            raise DAGValidationError("node_must_be_mapping", "Node entries must be mappings")
        nid = node.get("id")
        op_name = node.get("operator")
        op_version = node.get("version", "1")
        if not isinstance(nid, str) or not nid:
            raise DAGValidationError("invalid_node_id", "Node must have a valid string 'id'")
        if nid in node_ids:
            raise DAGValidationError("duplicate_node_id", f"Duplicate node ID '{nid}'", nid)
        node_ids.add(nid)
        node_docs[nid] = node

        op = registry.get(op_name, op_version)
        if op is None:
            raise DAGValidationError(
                "unknown_operator",
                f"Operator '{op_name}@{op_version}' not found in registry",
                nid,
                {"operator": op_name, "version": op_version},
            )
        operators[nid] = op

    if return_node not in node_ids:
        raise DAGValidationError(
            "unknown_return_node",
            f"Return node '{return_node}' not present in nodes",
            return_node,
        )

    # Build adjacency list and in-degrees for topological sort
    adj: dict[str, list[str]] = {nid: [] for nid in node_ids}
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    normalized_inputs: dict[str, dict[str, tuple[str, ...]]] = {nid: {} for nid in node_ids}

    for nid, node in node_docs.items():
        op = operators[nid]
        raw_inputs = node.get("inputs", {})
        if not isinstance(raw_inputs, Mapping):
            raise DAGValidationError(
                "inputs_must_be_mapping",
                f"Node '{nid}' inputs must be a mapping",
                nid,
            )

        # Validate ports
        port_by_name = {p.name: p for p in op.inputs}
        for port_name, targets in raw_inputs.items():
            if port_name not in port_by_name:
                raise DAGValidationError(
                    "unknown_input_port",
                    f"Port '{port_name}' not defined on operator '{op.name}'",
                    nid,
                    {"port": port_name},
                )
            port_spec = port_by_name[port_name]
            if isinstance(targets, str):
                target_list = [targets]
            elif isinstance(targets, (list, tuple)):
                target_list = list(targets)
            else:
                raise DAGValidationError(
                    "invalid_port_target",
                    f"Target for port '{port_name}' must be string or list of strings",
                    nid,
                )

            for src in target_list:
                if src in external_set:
                    # Permitted external input source
                    continue
                if src not in node_ids:
                    raise DAGValidationError(
                        "unknown_source_node",
                        f"Port '{port_name}' references non-existent node '{src}'",
                        nid,
                        {"source_node": src},
                    )
                # Check output type compatibility
                src_op = operators[src]
                if src_op.output_type != port_spec.value_type and port_spec.value_type != "Any":
                    raise DAGValidationError(
                        "type_mismatch",
                        f"Port '{port_name}' expects {port_spec.value_type}, node '{src}' produces {src_op.output_type}",
                        nid,
                        {"expected": port_spec.value_type, "actual": src_op.output_type},
                    )
                # Check input requirements satisfiability
                if op.input_requirements and port_name in op.input_requirements:
                    reqs = op.input_requirements[port_name]
                    if isinstance(reqs, Mapping):
                        req_output_type = reqs.get("output_type")
                        if req_output_type and req_output_type != src_op.output_type and req_output_type != "Any":
                            raise DAGValidationError(
                                "input_requirement_mismatch",
                                f"Port '{port_name}' requires output_type '{req_output_type}', but '{src}' produces '{src_op.output_type}'",
                                nid,
                                {"expected": req_output_type, "actual": src_op.output_type},
                            )
                adj[src].append(nid)
                in_degree[nid] += 1

            normalized_inputs[nid][port_name] = tuple(target_list)

        # Check required ports
        for p in op.inputs:
            if p.required and p.name not in normalized_inputs[nid]:
                raise DAGValidationError(
                    "missing_required_port",
                    f"Node '{nid}' missing required input port '{p.name}'",
                    nid,
                    {"port": p.name},
                )

    # Topological sort (Kahn's algorithm)
    queue = [nid for nid in node_ids if in_degree[nid] == 0]
    sorted_nodes: list[str] = []
    depths: dict[str, int] = {nid: 1 for nid in node_ids}

    while queue:
        curr = queue.pop(0)
        sorted_nodes.append(curr)
        curr_depth = depths[curr]
        for neighbor in adj[curr]:
            in_degree[neighbor] -= 1
            if curr_depth + 1 > depths[neighbor]:
                depths[neighbor] = curr_depth + 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_nodes) != len(node_ids):
        cycle_nodes = [nid for nid, deg in in_degree.items() if deg > 0]
        raise DAGValidationError(
            "cycle_detected",
            f"Cycle detected in DAG among nodes: {cycle_nodes}",
            details={"cycle_nodes": cycle_nodes},
        )

    max_measured_depth = max(depths.values()) if depths else 0
    if max_measured_depth > max_depth:
        raise DAGValidationError(
            "dag_depth_exceeded",
            f"DAG depth {max_measured_depth} exceeds limit {max_depth}",
            details={"depth": max_measured_depth, "max_depth": max_depth},
        )

    # Validate parameters
    verified_nodes: list[VerifiedNode] = []
    for nid in sorted_nodes:
        node = node_docs[nid]
        op = operators[nid]
        explicit_params = dict(node.get("params", {}))
        bindings = dict(node.get("bindings", {}))

        # Check parameters against spec
        for pname, pspec in op.parameters.items():
            if pname in explicit_params:
                _validate_parameter_value(pspec, explicit_params[pname], nid, pname)
            elif pname in bindings:
                bval = bindings[pname]
                if bval.startswith("runtime.") and runtime is not None:
                    rfield = bval.removeprefix("runtime.")
                    rval = _resolve_runtime_field(runtime, rfield)
                    if rval is not None:
                        _validate_parameter_value(pspec, rval, nid, pname)
            elif pspec.has_default:
                explicit_params[pname] = pspec.default
            elif pspec.required:
                raise DAGValidationError(
                    "missing_required_parameter",
                    f"Node '{nid}' missing required parameter '{pname}'",
                    nid,
                    {"parameter": pname},
                )

        verified_nodes.append(
            VerifiedNode(
                node_id=nid,
                operator=op,
                inputs=normalized_inputs[nid],
                params=explicit_params,
                bindings=bindings,
                depth=depths[nid],
            )
        )

    return VerifiedDAG(
        nodes=tuple(verified_nodes),
        return_node=return_node,
        max_depth=max_measured_depth,
        measured_node_count=len(verified_nodes),
    )


class SerialExecutor:
    """Topological serial executor for verified DAGs."""

    def __init__(self, registry: OperatorRegistry):
        self.registry = registry

    def execute_verified(
        self,
        dag: VerifiedDAG,
        runtime: Any,
        external_values: Mapping[str, Any] | None = None,
        runner: Any = None,
    ) -> ExecutionResult:
        external_values = external_values or {}
        values: dict[str, Any] = dict(external_values)
        records: list[NodeRecord] = []
        start_time = time.monotonic()

        for node in dag.nodes:
            nid = node.node_id
            if nid in values:
                # Already provided externally (e.g. initial telemetry input)
                continue

            op = node.operator

            # Gather inputs
            node_inputs: dict[str, Any] = {}
            for port in op.inputs:
                sources = node.inputs.get(port.name, ())
                if port.variadic:
                    node_inputs[port.name] = tuple(values[src] for src in sources)
                elif sources:
                    node_inputs[port.name] = values[sources[0]]
                else:
                    node_inputs[port.name] = None

            # Resolve parameters
            node_params: dict[str, Any] = dict(node.params)
            for pname, bval in node.bindings.items():
                if pname not in node_params and bval.startswith("runtime.") and runtime is not None:
                    rfield = bval.removeprefix("runtime.")
                    rval = _resolve_runtime_field(runtime, rfield)
                    if rval is not None:
                        node_params[pname] = rval

            t0 = time.monotonic()
            if op.implementation is not None:
                val = op.implementation(node_inputs, node_params, runtime)
            elif runner is not None:
                val = runner.run(op, node_inputs, node_params, runtime)
            else:
                raise RuntimeError(
                    f"Operator '{op.qualified_name}' has neither implementation nor runner"
                )

            elapsed_node_ms = int((time.monotonic() - t0) * 1000)
            values[nid] = val
            records.append(
                NodeRecord(
                    node_id=nid,
                    operator=op.qualified_name,
                    inputs={k: str(type(v).__name__) for k, v in node_inputs.items()},
                    params=node_params,
                    elapsed_ms=elapsed_node_ms,
                    output_type=op.output_type,
                )
            )

        total_elapsed_ms = int((time.monotonic() - start_time) * 1000)
        return ExecutionResult(
            return_node=dag.return_node,
            return_value=values[dag.return_node],
            records=tuple(records),
            elapsed_ms=total_elapsed_ms,
        )
