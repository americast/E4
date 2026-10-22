"""Ordered JSON DAG grammar, structural verifier, and structured errors."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .registry import OperatorRegistry, OperatorSpec
from .runtime import RuntimeContext, RuntimeResolutionError, _freeze, _validate_parameter


class DAGValidationError(ValueError):
    def __init__(self, code: str, message: str, node_id: str | None = None,
                 details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.node_id = node_id
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "node_id": self.node_id,
                "details": self.details}


@dataclass(frozen=True, slots=True)
class VerifiedNode:
    node_id: str
    operator: OperatorSpec
    inputs: Mapping[str, tuple[str, ...]]
    params: Mapping[str, Any]
    bindings: Mapping[str, str]
    depth: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", MappingProxyType({key: tuple(value) for key, value in self.inputs.items()}))
        object.__setattr__(self, "params", MappingProxyType({key: _freeze(value) for key, value in self.params.items()}))
        object.__setattr__(self, "bindings", MappingProxyType(dict(self.bindings)))


@dataclass(frozen=True, slots=True)
class VerifiedDAG:
    nodes: tuple[VerifiedNode, ...]
    return_node: str
    max_depth: int
    measured_node_count: int
    schema_version: str = "e4-dag/1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))

    def as_dict(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version,
                "nodes": [{"id": node.node_id,
                           "operator": node.operator.name,
                           "version": node.operator.version,
                           "inputs": {name: list(refs) for name, refs in node.inputs.items()},
                           "params": dict(node.params), "bindings": dict(node.bindings),
                           "depth": node.depth}
                          for node in self.nodes],
                "return": {"node": self.return_node},
                "measured_graph_depth": self.max_depth,
                "measured_node_count": self.measured_node_count}


def verify_dag(document: Mapping[str, Any], registry: OperatorRegistry,
               runtime: RuntimeContext | None = None,
               max_nodes: int = 40, max_depth: int = 12) -> VerifiedDAG:
    if set(document) != {"schema_version", "nodes", "return"}:
        raise DAGValidationError("document_fields", "DAG must contain schema_version, nodes, and return")
    if document.get("schema_version") != "e4-dag/1":
        raise DAGValidationError("schema_version", "unsupported DAG schema version")
    rows = document.get("nodes")
    if not isinstance(rows, list) or not rows:
        raise DAGValidationError("nodes_required", "DAG nodes must be a nonempty ordered array")
    if len(rows) > max_nodes:
        raise DAGValidationError("node_limit", f"DAG has {len(rows)} nodes; configured maximum is {max_nodes}")
    returned = document.get("return")
    if not isinstance(returned, dict) or set(returned) != {"node"} or not isinstance(returned["node"], str):
        raise DAGValidationError("return_shape", "return must be exactly {\"node\": <id>}")

    node_by_id: dict[str, VerifiedNode] = {}
    type_by_id: dict[str, str] = {}
    consumers: dict[str, list[tuple[str, str, OperatorSpec]]] = {}
    candidate_direct_scorers: dict[str, list[str]] = {}
    score_ids: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != {"id", "operator", "version", "inputs", "params", "bindings"}:
            raise DAGValidationError("node_shape", "each node must have id/operator/version/inputs/params/bindings")
        node_id, op_name, version = row["id"], row["operator"], row["version"]
        if not all(isinstance(value, str) and value for value in (node_id, op_name, version)):
            raise DAGValidationError("node_identity", "node id/operator/version must be nonempty strings")
        if node_id in node_by_id:
            raise DAGValidationError("duplicate_node_id", f"duplicate node id {node_id}", node_id)
        operator = registry.get(op_name, version)
        if operator is None:
            raise DAGValidationError("unknown_operator", f"unknown operator {op_name}@{version}", node_id)
        raw_inputs = row["inputs"]
        params = row["params"]
        bindings = row["bindings"]
        if not isinstance(raw_inputs, dict) or not isinstance(params, dict) or not isinstance(bindings, dict):
            values = {"inputs": raw_inputs, "params": params, "bindings": bindings}
            bad_types = {name: ("null" if value is None else "array" if isinstance(value, list) else
                                "boolean" if type(value) is bool else "number" if type(value) in (int, float) else
                                "string" if isinstance(value, str) else type(value).__name__)
                         for name, value in values.items() if not isinstance(value, dict)}
            details = ", ".join(f"{name}={kind}" for name, kind in sorted(bad_types.items()))
            raise DAGValidationError("node_maps",
                f"node {node_id} inputs, params, and bindings must each be JSON objects; got {details}",
                node_id, {"invalid_field_types": bad_types})
        supported_ports = {port.name: port for port in operator.inputs}
        if set(raw_inputs) - set(supported_ports):
            raise DAGValidationError("unknown_input", f"unsupported input port(s): {sorted(set(raw_inputs) - set(supported_ports))}", node_id)
        normalized_inputs: dict[str, tuple[str, ...]] = {}
        depth = 1
        for port in operator.inputs:
            raw_ref = raw_inputs.get(port.name)
            if raw_ref is None:
                if port.required:
                    raise DAGValidationError("missing_input", f"required input {port.name} is missing", node_id)
                normalized_inputs[port.name] = ()
                continue
            if port.variadic:
                if not isinstance(raw_ref, list):
                    raise DAGValidationError("input_arity", f"variadic input {port.name} must be an array", node_id)
                refs = tuple(raw_ref)
                if len(refs) < port.minimum_items or (port.maximum_items is not None and len(refs) > port.maximum_items):
                    raise DAGValidationError("input_arity", f"variadic input {port.name} has invalid item count", node_id)
            else:
                if not isinstance(raw_ref, str):
                    raise DAGValidationError("input_arity", f"input {port.name} must reference one node", node_id)
                refs = (raw_ref,)
            if any(not isinstance(ref, str) for ref in refs):
                raise DAGValidationError("input_reference", f"input {port.name} references must be strings", node_id)
            for ref in refs:
                if ref not in node_by_id:
                    code = "forward_reference" if any(isinstance(item, dict) and item.get("id") == ref for item in rows[index + 1:]) else "unknown_reference"
                    reason = "forward reference to later node" if code == "forward_reference" else "unavailable node"
                    raise DAGValidationError(code, f"node {node_id} has {reason} {ref}", node_id,
                                             {"reference": ref})
                actual_type = type_by_id[ref]
                if actual_type == "RankedAnswer":
                    raise DAGValidationError("illegal_type_edge", "a RankedAnswer cannot feed another DAG node", node_id)
                if port.value_type == "RankedAnswer":
                    raise DAGValidationError("illegal_type_edge", "RankedAnswer is a terminal output type", node_id)
                if actual_type != port.value_type:
                    raise DAGValidationError("input_type_mismatch",
                                             f"{node_id}.{port.name} expects {port.value_type}, got {actual_type}",
                                             node_id, {"reference": ref})
                legal_inputs = {
                    "Aggregate": {"Telemetry", "Aggregate"},
                    "CandidateSet": {"Telemetry", "Aggregate", "CandidateSet"},
                    "ScoredCandidates": {"Telemetry", "Aggregate", "CandidateSet", "ScoredCandidates"},
                    "RankedAnswer": {"ScoredCandidates"},
                }
                if actual_type not in legal_inputs.get(operator.output_type, set()):
                    raise DAGValidationError("illegal_type_edge",
                                             f"{actual_type} cannot feed output type {operator.output_type}",
                                             node_id, {"reference": ref})
                depth = max(depth, node_by_id[ref].depth + 1)
                consumers.setdefault(ref, []).append((node_id, port.name, operator))
                if actual_type == "CandidateSet" and operator.output_type == "ScoredCandidates":
                    candidate_direct_scorers.setdefault(ref, []).append(node_id)
            normalized_inputs[port.name] = refs
        if set(params) - set(operator.parameters):
            raise DAGValidationError("unknown_parameter", f"unsupported parameters: {sorted(set(params) - set(operator.parameters))}", node_id)
        if set(bindings) - set(operator.parameters):
            raise DAGValidationError("unknown_parameter_binding", f"unsupported parameter bindings: {sorted(set(bindings) - set(operator.parameters))}", node_id)
        if set(params) & set(bindings):
            # Explicit values override bindings. Keep both in the document so precedence is inspectable.
            pass
        for name, field in bindings.items():
            if not isinstance(field, str) or not field.startswith("runtime."):
                raise DAGValidationError("invalid_runtime_binding", f"binding {name} must be runtime.<field>", node_id)
            field_name = field.removeprefix("runtime.")
            if runtime is not None and field_name not in runtime.schema.fields:
                raise DAGValidationError("unknown_runtime_field",
                                         f"binding {name} refers to undeclared {field}", node_id,
                                         {"field": field})
            if runtime is not None and name not in params and field_name not in runtime.snapshot.values:
                raise DAGValidationError("missing_required_runtime_field",
                                         f"binding {name} requires unavailable {field}", node_id,
                                         {"field": field})
            if runtime is not None and name not in params and field_name in runtime.snapshot.values:
                try:
                    _validate_parameter(operator.parameters[name], runtime.snapshot.values[field_name])
                except RuntimeResolutionError as exc:
                    raise DAGValidationError("binding_type_mismatch",
                                             f"{field} is incompatible with parameter {name}: {exc}",
                                             node_id, {"field": field, "parameter": name,
                                                      "expected_type": operator.parameters[name].value_type}) from exc
        for name, value in params.items():
            if value == "auto":
                if not operator.parameters[name].estimator:
                    raise DAGValidationError("auto_not_supported", f"parameter {name} does not declare auto", node_id)
            else:
                try:
                    _validate_parameter(operator.parameters[name], value)
                except RuntimeResolutionError as exc:
                    raise DAGValidationError(exc.code, str(exc), node_id, {"parameter": name}) from exc
        # Ensure omitted required values can be resolved before loading case telemetry.
        if runtime is not None:
            for name, spec in operator.parameters.items():
                if spec.runtime_field and spec.runtime_field not in runtime.schema.fields:
                    raise DAGValidationError("unknown_runtime_field",
                                             f"parameter {name} refers to undeclared runtime.{spec.runtime_field}",
                                             node_id, {"field": spec.runtime_field})
                if name in params or name in bindings:
                    continue
                if spec.runtime_field:
                    if spec.runtime_field in runtime.snapshot.values:
                        try:
                            _validate_parameter(spec, runtime.snapshot.values[spec.runtime_field])
                        except RuntimeResolutionError as exc:
                            raise DAGValidationError("binding_type_mismatch",
                                f"runtime.{spec.runtime_field} is incompatible with parameter {name}: {exc}",
                                node_id, {"field": f"runtime.{spec.runtime_field}",
                                          "parameter": name, "expected_type": spec.value_type}) from exc
                    elif spec.has_default:
                        try:
                            _validate_parameter(spec, spec.default)
                        except RuntimeResolutionError as exc:
                            raise DAGValidationError("default_type_mismatch",
                                f"default for parameter {name} is invalid: {exc}", node_id,
                                {"parameter": name}) from exc
                    elif spec.required:
                        raise DAGValidationError("missing_required_runtime_field",
                                                 f"parameter {name} requires runtime.{spec.runtime_field}", node_id)
                if spec.required and not spec.runtime_field and not spec.has_default:
                    raise DAGValidationError("missing_required_parameter",
                                             f"required parameter {name} is unresolved", node_id)
        node = VerifiedNode(node_id, operator, normalized_inputs, dict(params), dict(bindings), depth)
        node_by_id[node_id] = node
        type_by_id[node_id] = operator.output_type
        if operator.output_type == "ScoredCandidates":
            score_ids.append(node_id)

    return_node = returned["node"]
    if return_node not in node_by_id:
        raise DAGValidationError("unknown_return", f"return references unknown node {return_node}")
    if type_by_id[return_node] != "RankedAnswer":
        raise DAGValidationError("return_type", "the sole return must be a RankedAnswer node", return_node)
    if node_by_id[return_node].depth > max_depth:
        raise DAGValidationError("depth_limit",
                                 f"measured DAG path depth {node_by_id[return_node].depth} exceeds configured limit {max_depth}",
                                 return_node, {"measured_graph_depth": node_by_id[return_node].depth,
                                               "configured_dag_depth": max_depth})
    if any(node_id != return_node and not consumers.get(node_id) for node_id in node_by_id):
        unused = [node_id for node_id in node_by_id if node_id != return_node and not consumers.get(node_id)]
        raise DAGValidationError("unused_binding", f"node outputs are unused: {unused}", details={"node_ids": unused})
    candidate_nodes = [node_id for node_id, value_type in type_by_id.items() if value_type == "CandidateSet"]
    for candidate_id in candidate_nodes:
        if not candidate_direct_scorers.get(candidate_id):
            raise DAGValidationError("candidate_not_scored",
                                     f"CandidateSet {candidate_id} is not directly consumed by a scoring operation",
                                     candidate_id)
    rank_node = node_by_id[return_node]
    terminal_score_ids = {score_id for score_id in score_ids
                          if not any(type_by_id.get(child) == "ScoredCandidates"
                                     for child, _, _ in consumers.get(score_id, []))}
    returned_scores = {ref for refs in rank_node.inputs.values() for ref in refs
                       if type_by_id.get(ref) == "ScoredCandidates"}
    if not terminal_score_ids or terminal_score_ids != returned_scores:
        missing = sorted(terminal_score_ids - returned_scores)
        unexpected = sorted(returned_scores - terminal_score_ids)
        raise DAGValidationError("scored_sink_not_returned",
                                 "the sole RankedAnswer must combine every terminal scored output",
                                 return_node, {"missing_scored_nodes": missing,
                                               "unexpected_scored_nodes": unexpected,
                                               "rank_inputs": sorted(returned_scores)})
    return VerifiedDAG(tuple(node_by_id[row["id"]] for row in rows), return_node,
                       max(node.depth for node in node_by_id.values()), len(node_by_id))
