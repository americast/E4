"""Closed-catalog operator-DAG macros and deterministic expansion."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from pathlib import Path
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import (ParameterSpec, RuntimeContext, RuntimeResolutionError,
                      _validate_parameter, resolve_parameters)


PROFILE = "operator-dag-v1"
PROGRAM_SCHEMA = "e4-operator-dag/1"
EXPANSION_SCHEMA = "e4-operator-dag-expansion/1"
_COMPOSITE_NAME = re.compile(r"^evolved\.[a-z][a-z0-9_]{0,62}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_FAMILY = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PORT_NAME = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_PARAM_NAME = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_NODE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_INPUT_TYPES = {"Telemetry", "Aggregate", "CandidateSet", "ScoredCandidates"}
_OUTPUT_TYPES = {"Aggregate", "CandidateSet", "ScoredCandidates"}
_NODE_FIELDS = {"id", "operator", "version", "inputs", "params", "bindings"}
_LEGAL_INPUTS = {
    "Aggregate": {"Telemetry", "Aggregate"},
    "CandidateSet": {"Telemetry", "Aggregate", "CandidateSet"},
    "ScoredCandidates": {"Telemetry", "Aggregate", "CandidateSet", "ScoredCandidates"},
}
_AGGREGATE_PRODUCERS = {
    "bank.metrics.aggregate": "bank_metric_window_summaries",
    "bank.traces.aggregate": "bank_trace_duration_summaries",
    "bank.public.query": "bank_public_raw_page",
    "bank.public.project": "bank_projected_scalar_summaries",
}
_AGGREGATE_CONSUMERS = {
    "bank.metrics.contrast": {"reference": "bank_metric_window_summaries",
                              "alert": "bank_metric_window_summaries"},
    "bank.logs.volume_contrast": {"reference": "bank_metric_window_summaries",
                                  "alert": "bank_metric_window_summaries"},
    "bank.traces.latency_contrast": {"reference": "bank_trace_duration_summaries",
                                     "alert": "bank_trace_duration_summaries"},
}


def _check_aggregate_consumer(consumer_name: str, node_id: str,
                             port_refs: Mapping[str, Sequence[str]],
                             producer_capabilities: Mapping[str, Mapping[str, str]]) -> None:
    for port_name, expected_capability in _AGGREGATE_CONSUMERS.get(consumer_name, {}).items():
        for producer_id in port_refs.get(port_name, ()):
            actual = producer_capabilities.get(producer_id)
            if actual is not None and actual["capability"] != expected_capability:
                raise CompositeSpecError("aggregate_capability_mismatch",
                    f"{consumer_name}.{port_name} requires {expected_capability}, but "
                    f"{actual['producer']}@{actual['version']} provides {actual['capability']}",
                    node_id=node_id, details={"consumer": consumer_name, "consumer_node_id": node_id,
                        "port": port_name, "expected_capability": expected_capability,
                        "actual_producer": f"{actual['producer']}@{actual['version']}",
                        "actual_capability": actual["capability"], "producer_node_id": producer_id})


class CompositeSpecError(ValueError):
    def __init__(self, code: str, message: str, *, node_id: str | None = None,
                 details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.node_id = node_id
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "node_id": self.node_id,
                "details": self.details}


@dataclass(frozen=True, slots=True)
class CompositeExpansion:
    dag: dict[str, Any]
    logical_to_expanded: dict[str, dict[str, Any]]
    call_outputs: dict[str, dict[str, Any]]
    node_origins: dict[str, dict[str, Any]]
    report: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"dag": self.dag, "logical_to_expanded": self.logical_to_expanded,
                "call_outputs": self.call_outputs, "node_origins": self.node_origins,
                "report": self.report}


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if type(value) in (tuple, list):
        return [_plain(item) for item in value]
    return value


def _same_json(left: Any, right: Any) -> bool:
    try:
        return _canonical(_plain(left)) == _canonical(_plain(right))
    except (TypeError, ValueError):
        return False


def _resolve_macro_parameters(operator: OperatorSpec, params: Mapping[str, Any],
                              bindings: Mapping[str, str], runtime: RuntimeContext | None,
                              registry: OperatorRegistry) -> dict[str, Any]:
    if runtime is not None:
        try:
            resolved = resolve_parameters(operator.parameters, params, bindings, runtime,
                estimators=registry.estimators)
            return _plain(resolved.values)
        except RuntimeResolutionError as exc:
            raise CompositeSpecError(exc.code, str(exc)) from exc
    if bindings:
        raise CompositeSpecError("runtime_context_required", "runtime-bound composite arguments require RuntimeContext")
    result = {}
    for name, spec in operator.parameters.items():
        if name in params:
            value = params[name]
        elif spec.runtime_field:
            raise CompositeSpecError("runtime_context_required",
                f"parameter {name} requires runtime.{spec.runtime_field}")
        elif spec.has_default:
            value = _plain(spec.default)
        elif spec.required:
            raise CompositeSpecError("missing_macro_parameter", f"required composite parameter {name} is unresolved")
        else:
            continue
        if value == "auto":
            raise CompositeSpecError("estimator_context_required", f"parameter {name} requires reference data for auto")
        try:
            _validate_parameter(spec, value)
        except RuntimeResolutionError as exc:
            raise CompositeSpecError(exc.code, f"parameter {name}: {exc}") from exc
        if spec.choices and value not in spec.choices:
            raise CompositeSpecError("parameter_choice", f"parameter {name} is outside its declared choices")
        result[name] = _plain(value)
    return result


def _composite_node_id(path: Sequence[Mapping[str, str]], local_id: str) -> str:
    digest = hashlib.sha256(_canonical([*path, {"local_node": local_id}])).hexdigest()[:24]
    safe_local = re.sub(r"[^A-Za-z0-9_.-]", "_", local_id)[:24] or "node"
    return f"m_{digest}_{safe_local}"


def _nested_call_key(path: Sequence[Mapping[str, str]]) -> str:
    if len(path) == 1:
        return path[0]["node_id"]
    return "nested_" + hashlib.sha256(_canonical(list(path))).hexdigest()[:24]


def expand_composite_dag(logical_dag: Mapping[str, Any],
                         composite_specs: Sequence[Mapping[str, Any]],
                         registry: OperatorRegistry, runtime: RuntimeContext | None = None, *,
                         max_nodes: int = 40, max_depth: int = 12) -> CompositeExpansion:
    """Inline active registered composites; output remains an ordinary e4-dag/1 document."""
    from .dag import DAGValidationError, verify_dag

    started = time.monotonic()
    if not isinstance(logical_dag, Mapping) or set(logical_dag) != {"schema_version", "nodes", "return"}:
        raise CompositeSpecError("logical_dag_shape", "logical DAG must use exact e4-dag/1 document fields")
    composites = _spec_index(composite_specs)
    if not composites and any(isinstance(row, Mapping) and row.get("operator", "").startswith("evolved.")
                              for row in logical_dag.get("nodes", []) if isinstance(row, Mapping)):
        # Legacy generated operators may also be named evolved.*. Only a
        # registered normalized composite profile is expanded below.
        pass
    _check_composite_cycles(composites)
    try:
        verify_dag(logical_dag, registry, runtime, max_nodes=max_nodes, max_depth=max_depth)
    except DAGValidationError as exc:
        raise CompositeSpecError(exc.code, str(exc), node_id=exc.node_id,
                                 details=exc.details) from exc

    rows_out: list[dict[str, Any]] = []
    occupied = {row["id"] for row in logical_dag["nodes"]}
    logical_to_expanded: dict[str, dict[str, Any]] = {}
    call_outputs: dict[str, dict[str, Any]] = {}
    node_origins: dict[str, dict[str, Any]] = {}
    closure_counts: dict[str, dict[str, Any]] = {}
    top_outputs: dict[str, str] = {}
    primitive_counts: Counter[str] = Counter()
    active: list[tuple[str, str]] = []

    def map_logical_ref(ref: Any) -> Any:
        if isinstance(ref, str):
            return top_outputs.get(ref, ref)
        if type(ref) is list:
            return [top_outputs.get(item, item) if isinstance(item, str) else item for item in ref]
        return ref

    def resolve_body_inputs(node: Mapping[str, Any], macro: Mapping[str, Any],
                            outer_refs: Mapping[str, Any], local_outputs: Mapping[str, str],
                            operator: OperatorSpec, logical_path: Sequence[Mapping[str, str]]) -> dict[str, Any]:
        output = {}
        ports = {port.name: port for port in operator.inputs}
        for port_name, port in ports.items():
            value = node["inputs"].get(port_name)
            if value is None:
                continue
            refs = value if type(value) is list else [value]
            flattened: list[str] = []
            absent = False
            for ref in refs:
                if isinstance(ref, str) and ref.startswith("$input."):
                    outer_name = ref[len("$input."):]
                    mapped = outer_refs.get(outer_name)
                    if mapped is None:
                        absent = True
                        continue
                    if isinstance(mapped, list):
                        flattened.extend(mapped)
                    else:
                        flattened.append(mapped)
                else:
                    mapped = local_outputs.get(ref)
                    if mapped is None:
                        raise CompositeSpecError("expanded_reference", f"unmapped local reference {ref}")
                    flattened.append(mapped)
            if absent and port.required:
                raise CompositeSpecError("missing_macro_input", f"required {port_name} input is absent")
            if port.variadic:
                if len(flattened) < port.minimum_items or (port.maximum_items is not None and len(flattened) > port.maximum_items):
                    raise CompositeSpecError("expanded_input_arity", f"{node['id']}.{port_name} has invalid expanded arity")
                output[port_name] = flattened
            else:
                if len(flattened) != 1:
                    if absent and not port.required:
                        continue
                    raise CompositeSpecError("expanded_input_arity", f"{node['id']}.{port_name} requires one expanded reference")
                output[port_name] = flattened[0]
        return output

    def expand_call(spec: Mapping[str, Any], call_node_id: str, raw_params: Mapping[str, Any],
                    raw_bindings: Mapping[str, str], outer_refs: Mapping[str, Any],
                    call_path: list[dict[str, str]], depth: int) -> tuple[str, list[str], Counter[str]]:
        key = (spec["name"], spec["version"])
        if key in active:
            raise CompositeSpecError("composite_cycle", f"recursive composite call {key[0]}@{key[1]}")
        if depth > max_depth:
            raise CompositeSpecError("expanded_depth_limit", "composite nesting exceeds the configured depth")
        operator = registry.get(*key)
        if operator is None:
            raise CompositeSpecError("unknown_composite_signature", f"{key[0]}@{key[1]} is not registered")
        params = _resolve_macro_parameters(operator, raw_params, raw_bindings, runtime, registry)
        current_path = [*call_path, {"node_id": call_node_id, "operator": key[0], "version": key[1]}]
        call_key = _nested_call_key(current_path)
        active.append(key)
        local_outputs: dict[str, str] = {}
        emitted: list[str] = []
        counts: Counter[str] = Counter()
        for row in spec["program"]["nodes"]:
            inner_op = _operator_spec(registry, composites, row["operator"], row["version"])
            mapped_inputs = resolve_body_inputs(row, spec, outer_refs, local_outputs, inner_op, current_path)
            bound_params = {}
            for name, value in row["params"].items():
                if isinstance(value, Mapping) and set(value) == {"parameter"}:
                    pname = value["parameter"]
                    if pname not in params:
                        target = inner_op.parameters[name]
                        if target.required and not target.has_default and not target.runtime_field:
                            raise CompositeSpecError("unresolved_macro_parameter",
                                f"{call_key} has no resolved parameter {pname}")
                        continue
                    bound_params[name] = _plain(params[pname])
                else:
                    bound_params[name] = _plain(value)
            nested = composites.get((row["operator"], row["version"]))
            if nested is not None:
                child_id = row["id"]
                child_path = current_path
                child_output, child_ids, child_counts = expand_call(nested, child_id,
                    bound_params, row["bindings"], mapped_inputs, child_path, depth + 1)
                local_outputs[row["id"]] = child_output
                emitted.extend(child_ids)
                counts.update(child_counts)
                continue
            expanded_id = _composite_node_id(current_path, row["id"])
            if expanded_id in occupied:
                raise CompositeSpecError("expanded_id_collision", f"expanded node id collides with {expanded_id}", node_id=expanded_id)
            occupied.add(expanded_id)
            expanded_row = {"id": expanded_id, "operator": row["operator"], "version": row["version"],
                "inputs": mapped_inputs, "params": bound_params, "bindings": dict(row["bindings"])}
            rows_out.append(expanded_row)
            local_outputs[row["id"]] = expanded_id
            emitted.append(expanded_id)
            op_key = f"{row['operator']}@{row['version']}"
            counts[op_key] += 1
            primitive_counts[op_key] += 1
            node_origins[expanded_id] = {"logical_call_id": current_path[0]["node_id"],
                "macro_path": [dict(item) for item in current_path],
                "primitive_operator": row["operator"], "primitive_version": row["version"],
                "primitive_key": op_key}
            if len(rows_out) > max_nodes:
                raise CompositeSpecError("expanded_node_limit",
                    f"expanded graph exceeds {max_nodes} primitive nodes")
        output_id = local_outputs.get(spec["program"]["return"]["node"])
        if output_id is None:
            raise CompositeSpecError("expanded_return", f"{call_key} return node was not expanded")
        # Preserve the logical call's identity at the graph boundary. For a
        # top-level call this is its original DAG id; nested calls receive a
        # deterministic scoped id so their parent's local refs remain unique.
        output_alias = call_node_id if not call_path else _composite_node_id(call_path, call_node_id)
        if output_alias != output_id:
            if output_alias in occupied and not (not call_path and output_alias == call_node_id):
                raise CompositeSpecError("expanded_id_collision",
                    f"macro output id collides with {output_alias}", node_id=output_alias)
            output_row = next((item for item in rows_out if item["id"] == output_id), None)
            if output_row is None:
                raise CompositeSpecError("expanded_return", f"expanded return node {output_id} is unavailable")
            origin = node_origins.pop(output_id)
            output_row["id"] = output_alias
            node_origins[output_alias] = origin
            emitted = [output_alias if item == output_id else item for item in emitted]
            # A parent macro can rename the same leaf again. Keep nested call
            # provenance referentially consistent with the final flat graph.
            for call in call_outputs.values():
                if call.get("output_node_id") == output_id:
                    call["output_node_id"] = output_alias
                call["expanded_node_ids"] = [output_alias if item == output_id else item
                    for item in call.get("expanded_node_ids", ())]
            occupied.add(output_alias)
            output_id = output_alias
        call_outputs[call_key] = {"composite": f"{key[0]}@{key[1]}",
            "logical_node_id": current_path[0]["node_id"], "call_path": [dict(item) for item in current_path],
            "output_node_id": output_id, "expanded_node_ids": list(emitted),
            "primitive_operator_counts": dict(sorted(counts.items()))}
        closure_counts[call_key] = {"composite": f"{key[0]}@{key[1]}",
            "primitive_node_count": len(emitted), "primitive_operator_counts": dict(sorted(counts.items()))}
        active.pop()
        return output_id, emitted, counts

    for row in logical_dag["nodes"]:
        op = registry.get(row["operator"], row["version"])
        composite = composites.get((row["operator"], row["version"]))
        if op is not None and not op.external_input and composite is None and op.implementation is None:
            raise CompositeSpecError("composite_program_unavailable",
                f"{op.name}@{op.version} has no active composite definition or primitive implementation",
                node_id=row["id"])
        if composite is None:
            mapped = {name: map_logical_ref(value) for name, value in row["inputs"].items()}
            out_row = {"id": row["id"], "operator": row["operator"], "version": row["version"],
                "inputs": mapped, "params": _plain(row["params"]), "bindings": _plain(row["bindings"])}
            if row["id"] in {item["id"] for item in rows_out}:
                raise CompositeSpecError("logical_id_collision", f"duplicate logical id {row['id']}")
            rows_out.append(out_row)
            top_outputs[row["id"]] = row["id"]
            logical_to_expanded[row["id"]] = {"expanded_node_ids": [row["id"]],
                "output_node_id": row["id"]}
            if op is not None:
                op_key = f"{op.name}@{op.version}"
                primitive_counts[op_key] += 1
                node_origins[row["id"]] = {"logical_call_id": row["id"], "macro_path": [],
                    "primitive_operator": op.name, "primitive_version": op.version,
                    "primitive_key": op_key}
            continue
        op = _operator_spec(registry, composites, row["operator"], row["version"])
        mapped_outer = {name: map_logical_ref(value) for name, value in row["inputs"].items()}
        output_id, expanded_ids, _counts = expand_call(composite, row["id"], row["params"],
            row["bindings"], mapped_outer, [], 1)
        top_outputs[row["id"]] = output_id
        logical_to_expanded[row["id"]] = {"expanded_node_ids": list(expanded_ids),
            "output_node_id": output_id}

    returned = logical_dag["return"]["node"]
    expanded_return = top_outputs.get(returned)
    if expanded_return is None:
        raise CompositeSpecError("logical_return", f"logical return node {returned} was not expanded")
    expanded_dag = {"schema_version": "e4-dag/1", "nodes": rows_out,
                    "return": {"node": expanded_return}}
    # Validate catalog-sensitive primitives after all macro arguments have
    # resolved to concrete values. This also covers ordinary stage-2 nodes,
    # which may not be wrapped by a composite at all.
    expansion_effects: dict[str, set[str]] = {}
    window_roles: dict[str, str | None] = {}
    aggregate_capabilities: dict[str, dict[str, str]] = {}
    for row in rows_out:
        operator = registry.get(row["operator"], row["version"])
        if operator is None:
            raise CompositeSpecError("unknown_expanded_operator",
                f"expanded operator {row['operator']}@{row['version']} is not registered", node_id=row["id"])
        input_refs = [ref for value in row["inputs"].values()
                      for ref in (value if type(value) is list else [value])]
        input_effect_sets = [expansion_effects.get(ref, set()) for ref in input_refs if ref is not None]
        current_effects = set().union(*input_effect_sets) if input_effect_sets else set()
        resolved: dict[str, Any] | None = None
        if row["operator"] in {"bank.public.query", "bank.public.project"}:
            resolved = _resolve_macro_parameters(operator, row["params"], row["bindings"], runtime, registry)
            if row["operator"] == "bank.public.query":
                _validate_query_catalog(row["id"], resolved, {}, operator.parameters, {})
                current_effects.add("public_query")
            else:
                _validate_project_catalog(row["id"], resolved, {}, operator.parameters, {}, input_effect_sets)
                current_effects.add("projected")
        expansion_effects[row["id"]] = current_effects

        role: str | None = None
        if row["operator"] in {"bank.metrics.aggregate", "bank.traces.aggregate", "bank.public.query"}:
            if resolved is None:
                resolved = _resolve_macro_parameters(operator, row["params"], row["bindings"], runtime, registry)
            value = resolved.get("window")
            if value in {"reference", "alert"}:
                role = value
        elif row["operator"] == "bank.public.project":
            data_ref = row["inputs"].get("data")
            if isinstance(data_ref, str):
                role = window_roles.get(data_ref)
        else:
            aggregate_refs = [ref for port_name, refs in row["inputs"].items()
                if any(port.name == port_name and port.value_type == "Aggregate" for port in operator.inputs)
                for ref in (refs if type(refs) is list else [refs])]
            known_roles = [window_roles.get(ref) for ref in aggregate_refs]
            if known_roles and all(item is not None for item in known_roles):
                unique_roles = set(known_roles)
                role = next(iter(unique_roles)) if len(unique_roles) == 1 else "mixed"
        if row["operator"] in {"bank.metrics.contrast", "bank.traces.latency_contrast",
                               "bank.logs.volume_contrast"}:
            for port_name, expected in (("reference", "reference"), ("alert", "alert")):
                ref = row["inputs"].get(port_name)
                if isinstance(ref, str):
                    actual = window_roles.get(ref)
                    if actual is not None and actual != expected:
                        raise CompositeSpecError("aggregate_window_role",
                            f"{row['operator']} requires {expected} Aggregate on {port_name}, got {actual}",
                            node_id=row["id"], details={"port": port_name, "expected": expected, "actual": actual})
        port_refs = {port_name: (value if type(value) is list else [value])
            for port_name, value in row["inputs"].items()}
        _check_aggregate_consumer(row["operator"], row["id"], port_refs, aggregate_capabilities)
        producer_capability = _AGGREGATE_PRODUCERS.get(row["operator"])
        if producer_capability is not None:
            aggregate_capabilities[row["id"]] = {"capability": producer_capability,
                "producer": row["operator"], "version": row["version"]}
        window_roles[row["id"]] = role
    try:
        verified = verify_dag(expanded_dag, registry, runtime, max_nodes=max_nodes, max_depth=max_depth)
    except DAGValidationError as exc:
        raise CompositeSpecError(exc.code, str(exc), node_id=exc.node_id, details=exc.details) from exc
    report = {"schema_version": EXPANSION_SCHEMA,
        "logical_node_count": len(logical_dag["nodes"]),
        "expanded_node_count": len(rows_out),
        "primitive_node_count": len(rows_out),
        "macro_call_count": len(call_outputs),
        "primitive_operator_counts": dict(sorted(primitive_counts.items())),
        "macro_closure_counts": closure_counts,
        "measured_expanded_depth": verified.max_depth,
        "configured_max_nodes": max_nodes, "configured_max_depth": max_depth,
        "elapsed_ms": int((time.monotonic() - started) * 1000)}
    return CompositeExpansion(expanded_dag, logical_to_expanded, call_outputs, node_origins, report)


def composite_spec_schema() -> dict[str, Any]:
    """Published closed-DAG profile; the body contains operator calls only."""
    return {
        "schema_version": "e4-operator-dag-spec/1",
        "authoring_profile": PROFILE,
        "required_fields": ["name", "version", "description", "family", "input_ports",
            "output_type", "parameters", "authoring_profile", "program"],
        "additional_fields": False,
        "fields": {
            "name": {"type": "string", "pattern": "^evolved\\.[a-z][a-z0-9_]{0,62}$"},
            "version": {"type": "string", "max_length": 64, "meaning": "declared version token; normalized version is content-addressed"},
            "description": {"type": "string", "min_length": 1, "max_length": 1024},
            "family": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,63}$"},
            "input_ports": {"type": "array", "min_items": 1, "max_items": 8,
                "item_fields": {"name": "unique lowercase identifier", "type": sorted(_INPUT_TYPES),
                    "required": "boolean; default true", "variadic": "boolean; default false",
                    "minimum_items": "integer 0..64; default 1", "maximum_items": "integer 1..64 or null; default null"},
                "additional_item_fields": False},
            "output_type": {"enum": sorted(_OUTPUT_TYPES)},
            "parameters": {"type": "object", "max_properties": 24,
                "entry_fields": {"type": ["string", "boolean", "integer", "positive_integer", "number", "string_list"],
                    "required": "boolean; default false", "nullable": "boolean; default false",
                    "default": "finite JSON value, optional", "choices": "finite JSON array; default []",
                    "runtime_field": "declared runtime field name or null", "description": "string <=512 characters"},
                "additional_entry_fields": False},
            "authoring_profile": {"const": PROFILE},
            "program": {"type": "object", "schema_version": PROGRAM_SCHEMA,
                "required_fields": ["schema_version", "nodes", "return"], "additional_fields": False,
                "nodes": {"type": "ordered array", "min_items": 1, "max_items": 40,
                    "node_fields": sorted(_NODE_FIELDS), "additional_node_fields": False,
                    "id": {"type": "string", "pattern": _NODE_ID.pattern},
                    "operator": "exact registered name", "version": "exact registered version",
                    "inputs": "object keyed by the selected operator's input ports",
                    "params": "object keyed by the selected operator's parameters",
                    "bindings": "object keyed by parameters; values are runtime.<declared_field>"},
                "return": {"required_fields": ["node"], "additional_fields": False,
                    "node": "local program node id"}},
        },
        "program": {
            "schema_version": PROGRAM_SCHEMA,
            "fields": ["schema_version", "nodes", "return"],
            "node_fields": sorted(_NODE_FIELDS),
            "input_references": "local node id or $input.<declared_port>; arrays only for variadic ports",
            "parameter_values": "finite JSON literal or exactly {parameter: <declared_macro_parameter>}",
            "runtime_bindings": "map a node parameter to exactly runtime.<declared_field>, matching the existing e4-dag verifier",
            "catalog": "exact registered operator name and version; composite macros expand recursively to registered primitives",
            "expressions": "not supported; no Python, lambdas, conditions, code, or new primitive definitions",
        },
        "limits": {"expanded_nodes": 40, "expanded_depth": 12},
        "metadata": ["program_sha256", "signature_sha256", "dependencies", "program_effects"],
    }


def trusted_aggregate_semantics(registry: OperatorRegistry | None = None) -> dict[str, Any]:
    """Single prompt/compiler contract for known Bank Aggregate producers and consumers."""
    signatures = {}
    if registry is not None:
        rows = registry.signatures()
        signatures = {name: sorted(f"{row['name']}@{row['version']}" for row in rows
            if row["name"] == name)
            for name in set(_AGGREGATE_PRODUCERS) | set(_AGGREGATE_CONSUMERS)}
    active_names = ({name for name, versions in signatures.items() if versions} if registry is not None else
                    set(_AGGREGATE_PRODUCERS) | set(_AGGREGATE_CONSUMERS))
    return {"schema_version": "e4-aggregate-semantic-contract/1",
        "producers": {name: details for name, details in {
            "bank.metrics.aggregate": {"capability": "bank_metric_window_summaries",
                "provides": "full-window AggregateRows with median/tail_quantile statistics, bucket_samples, metric_kind/ranking metadata; includes derived log_volume_count rows when present"},
            "bank.traces.aggregate": {"capability": "bank_trace_duration_summaries",
                "provides": "AggregateRows with per_trace_median/per_trace_mean/per_trace_tail_quantile and trace_values keyed by trace_id"},
            "bank.public.query": {"capability": "bank_public_raw_page",
                "provides": "bounded native rows in Aggregate.metadata.public_query.rows_data; Aggregate.rows is empty"},
            "bank.public.project": {"capability": "bank_projected_scalar_summaries",
                "provides": "paged existing summary rows with non-scalar details omitted, including bucket_samples and trace_values"},
        }.items() if name in active_names},
        "consumers": {name: details for name, details in {
            "bank.metrics.contrast": {"reference": "bank_metric_window_summaries", "alert": "bank_metric_window_summaries"},
            "bank.logs.volume_contrast": {"reference": "bank_metric_window_summaries",
                "alert": "bank_metric_window_summaries", "row_requirements": "median/tail_quantile log_volume_count rows with bucket_samples from bank/log/log_service.csv"},
            "bank.traces.latency_contrast": {"reference": "bank_trace_duration_summaries",
                "alert": "bank_trace_duration_summaries"},
        }.items() if name in active_names},
        "compatibility": "A known producer mismatch is rejected before execution. Unknown or symbolic Aggregate inputs remain unclassified until their producer is available.",
        "projection": "bank.public.project cannot consume public-query pages or a prior projection; it omits non-scalar sample buffers required by Bank contrast operators.",
        "registered_signatures": signatures}


def _json_value(value: Any, where: str, depth: int = 0, seen: set[int] | None = None) -> None:
    if depth > 32:
        raise CompositeSpecError("json_depth", f"{where} exceeds JSON nesting limit")
    if seen is None:
        seen = set()
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise CompositeSpecError("nonfinite_json", f"{where} must contain finite numbers")
        return
    if type(value) in (list, dict):
        ident = id(value)
        if ident in seen:
            raise CompositeSpecError("cyclic_json", f"{where} contains a cycle")
        seen.add(ident)
        if type(value) is list:
            for index, item in enumerate(value):
                _json_value(item, f"{where}[{index}]", depth + 1, seen)
        else:
            if any(type(key) is not str for key in value):
                raise CompositeSpecError("json_key", f"{where} object keys must be strings")
            for key, item in value.items():
                _json_value(item, f"{where}.{key}", depth + 1, seen)
        seen.remove(ident)
        return
    raise CompositeSpecError("not_json", f"{where} must be finite JSON data")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _spec_index(specs: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
    result = {}
    for spec in specs:
        if not isinstance(spec, Mapping):
            continue
        name, version = spec.get("name"), spec.get("version")
        if isinstance(name, str) and isinstance(version, str):
            key = (name, version)
            if key in result:
                raise CompositeSpecError("duplicate_composite", f"duplicate composite {name}@{version}")
            result[key] = spec
    return result


def _normalize_ports(value: Any) -> list[dict[str, Any]]:
    if type(value) is not list or not value or len(value) > 8:
        raise CompositeSpecError("input_ports", "input_ports must be an array of 1 to 8 ports")
    ports, names = [], set()
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) - {
                "name", "type", "required", "variadic", "minimum_items", "maximum_items"} or not {"name", "type"} <= set(raw):
            raise CompositeSpecError("input_port_shape", "each input port requires only name/type and published bounds")
        name, value_type = raw["name"], raw["type"]
        if not isinstance(name, str) or not _PORT_NAME.fullmatch(name) or name in names:
            raise CompositeSpecError("input_port_name", f"invalid or duplicate input port name {name!r}")
        if value_type not in _INPUT_TYPES:
            raise CompositeSpecError("input_port_type", f"unsupported composite input type {value_type!r}")
        required, variadic = raw.get("required", True), raw.get("variadic", False)
        minimum, maximum = raw.get("minimum_items", 1), raw.get("maximum_items")
        if type(required) is not bool or type(variadic) is not bool:
            raise CompositeSpecError("input_port_flags", "required and variadic must be booleans")
        if type(minimum) is not int or minimum < 0 or minimum > 64:
            raise CompositeSpecError("input_port_minimum", "minimum_items must be between 0 and 64")
        if maximum is not None and (type(maximum) is not int or maximum < max(1, minimum) or maximum > 64):
            raise CompositeSpecError("input_port_maximum", "maximum_items is outside the permitted range")
        if not variadic and (minimum != 1 or maximum is not None):
            raise CompositeSpecError("input_port_bounds", "non-variadic ports cannot declare item bounds")
        if not required and variadic and minimum != 0:
            raise CompositeSpecError("input_port_bounds", "optional variadic ports require minimum_items=0")
        ports.append({"name": name, "type": value_type, "required": required, "variadic": variadic,
                      "minimum_items": minimum, "maximum_items": maximum})
        names.add(name)
    return ports


def _normalize_parameters(value: Any, runtime: RuntimeContext | None) -> dict[str, dict[str, Any]]:
    allowed_fields = {"type", "required", "nullable", "default", "choices", "runtime_field", "description"}
    types = {"string", "boolean", "integer", "positive_integer", "number", "string_list"}
    if not isinstance(value, Mapping) or len(value) > 24:
        raise CompositeSpecError("parameters", "parameters must be an object with at most 24 entries")
    normalized = {}
    for name, raw in sorted(value.items()):
        if not isinstance(name, str) or not _PARAM_NAME.fullmatch(name):
            raise CompositeSpecError("parameter_name", f"invalid parameter name {name!r}")
        if not isinstance(raw, Mapping) or "type" not in raw or set(raw) - allowed_fields:
            raise CompositeSpecError("parameter_shape", f"parameter {name} has an invalid shape")
        kind = raw["type"]
        required, nullable = raw.get("required", False), raw.get("nullable", False)
        if kind not in types or type(required) is not bool or type(nullable) is not bool:
            raise CompositeSpecError("parameter_type", f"parameter {name} has invalid type/flags")
        choices = raw.get("choices", [])
        if type(choices) is not list:
            raise CompositeSpecError("parameter_choices", f"parameter {name}.choices must be a list")
        for choice in choices:
            _json_value(choice, f"parameter {name} choice")
        has_default = "default" in raw
        default = raw.get("default")
        if has_default:
            _json_value(default, f"parameter {name} default")
        field = raw.get("runtime_field")
        if field is not None and (not isinstance(field, str) or not _PORT_NAME.fullmatch(field)):
            raise CompositeSpecError("runtime_field", f"parameter {name}.runtime_field is invalid")
        description = raw.get("description", "")
        if not isinstance(description, str) or len(description) > 512:
            raise CompositeSpecError("parameter_description", f"parameter {name}.description is too long")
        spec = ParameterSpec(name, kind, required, nullable, default, has_default,
            field, None, description, tuple(choices))
        for label, candidate in (("choice", item) for item in choices):
            try:
                _validate_parameter(spec, candidate)
            except RuntimeResolutionError as exc:
                raise CompositeSpecError("parameter_choice", f"parameter {name} has invalid {label}: {exc}") from exc
        if has_default:
            try:
                _validate_parameter(spec, default)
            except RuntimeResolutionError as exc:
                raise CompositeSpecError("parameter_default", f"parameter {name} has invalid default: {exc}") from exc
            if choices and default not in choices:
                raise CompositeSpecError("parameter_default_choice", f"parameter {name} default is outside choices")
        if field and runtime is not None and field not in runtime.schema.fields:
            raise CompositeSpecError("unknown_runtime_field", f"parameter {name} names unknown runtime.{field}")
        normalized[name] = {"type": kind, "required": required, "nullable": nullable,
            "default": default, "has_default": has_default, "choices": list(choices),
            "runtime_field": field, "description": description}
    return normalized


def _parameter_specs(normalized: Mapping[str, Mapping[str, Any]]) -> dict[str, ParameterSpec]:
    return {name: ParameterSpec(name, row["type"], row["required"], row["nullable"],
        row["default"], row["has_default"], row["runtime_field"], None,
        row["description"], tuple(row["choices"])) for name, row in normalized.items()}


def _operator_spec(registry: OperatorRegistry, composites: Mapping[tuple[str, str], Mapping[str, Any]],
                   name: str, version: str) -> OperatorSpec:
    operator = registry.get(name, version)
    if operator is None:
        raise CompositeSpecError("unknown_operator", f"operator {name}@{version} is not registered")
    if operator.external_input:
        raise CompositeSpecError("external_input_forbidden", "composite programs cannot declare external input nodes")
    composite = composites.get((name, version))
    if composite is None and operator.implementation is None:
        raise CompositeSpecError("composite_program_unavailable",
            f"{name}@{version} has no active composite definition or primitive implementation")
    if composite is not None:
        norm_parameters = composite.get("parameters", {})
        parameter_match = len(operator.parameters) == len(norm_parameters)
        if parameter_match:
            for pname, pspec in operator.parameters.items():
                nrow = norm_parameters.get(pname)
                if not isinstance(nrow, Mapping) or (
                        pspec.value_type != nrow.get("type") or pspec.required != nrow.get("required") or
                        pspec.nullable != nrow.get("nullable") or pspec.has_default != nrow.get("has_default") or
                        (pspec.has_default and not _same_json(pspec.default, nrow.get("default"))) or
                        pspec.runtime_field != nrow.get("runtime_field") or
                        not _same_json(list(pspec.choices), nrow.get("choices")) or
                        pspec.description != nrow.get("description")):
                    parameter_match = False
                    break
        if (operator.output_type != composite["output_type"] or
                tuple((p.name, p.value_type, p.required, p.variadic, p.minimum_items, p.maximum_items)
                      for p in operator.inputs) != tuple(
                    (p["name"], p["type"], p["required"], p["variadic"], p["minimum_items"], p["maximum_items"])
                    for p in composite["input_ports"]) or not parameter_match):
            raise CompositeSpecError("composite_signature_mismatch", f"registered signature disagrees with {name}@{version}")
    return operator


def _parameter_ref_type(value: Mapping[str, Any], macro_params: Mapping[str, ParameterSpec],
                        target: ParameterSpec, node_id: str) -> None:
    if set(value) != {"parameter"} or not isinstance(value.get("parameter"), str):
        raise CompositeSpecError("parameter_reference", "parameter references must be exactly {parameter: name}", node_id=node_id)
    name = value["parameter"]
    source = macro_params.get(name)
    if source is None:
        raise CompositeSpecError("unknown_macro_parameter", f"node references undeclared macro parameter {name}", node_id=node_id)
    compatible = source.value_type == target.value_type or (
        source.value_type == "integer" and target.value_type == "number") or (
        source.value_type == "positive_integer" and target.value_type in {"integer", "number"})
    if not compatible or (source.nullable and not target.nullable):
        raise CompositeSpecError("parameter_reference_type",
            f"macro parameter {name} cannot bind {target.value_type} parameter {target.name}", node_id=node_id)
    if (not source.required and not source.has_default and not source.runtime_field and
            target.required and not target.has_default and not target.runtime_field):
        raise CompositeSpecError("optional_parameter_reference",
            f"optional macro parameter {name} cannot satisfy required node parameter {target.name}", node_id=node_id)
    if target.choices and (not source.choices or any(
            not any(choice == allowed for allowed in target.choices) for choice in source.choices)):
        raise CompositeSpecError("parameter_reference_choices",
            f"macro parameter {name} does not guarantee choices required by {target.name}", node_id=node_id)


def _possible_parameter_values(value: Any, macro_params: Mapping[str, ParameterSpec],
                               spec: ParameterSpec, *, missing: bool = False) -> tuple[Any, ...] | None:
    if missing:
        if spec.has_default:
            return (spec.default,)
        if spec.runtime_field or spec.required:
            return None
        return (None,)
    if isinstance(value, Mapping) and set(value) == {"parameter"}:
        source = macro_params.get(value.get("parameter"))
        if source is None:
            return None
        if source.choices:
            return tuple(source.choices)
        return None
    if isinstance(value, str) and value.startswith("runtime."):
        return None
    return (value,)


def _runtime_schema(runtime: RuntimeContext | None):
    if runtime is not None:
        return runtime.schema
    from .runtime import RuntimeSchema
    return RuntimeSchema.from_json(Path(__file__).resolve().parents[1] / "config" / "runtime_schema.json")


def _program_input_ref(raw_ref: Any, port: Mapping[str, Any], node_id: str,
                       port_types: Mapping[str, str], port_variadic: Mapping[str, bool],
                       node_types: Mapping[str, str], *, outer: bool) -> tuple[list[str], set[str]]:
    if port["variadic"]:
        if type(raw_ref) is not list:
            raise CompositeSpecError("input_arity", f"{node_id}.{port['name']} must be a reference array", node_id=node_id)
        refs = raw_ref
    else:
        if not isinstance(raw_ref, str):
            raise CompositeSpecError("input_arity", f"{node_id}.{port['name']} must contain one reference", node_id=node_id)
        refs = [raw_ref]
    local_refs: set[str] = set()
    result: list[str] = []
    for ref in refs:
        if not isinstance(ref, str) or not ref:
            raise CompositeSpecError("input_reference", f"{node_id}.{port['name']} references must be nonempty strings", node_id=node_id)
        if ref.startswith("$input."):
            if not outer or not _PORT_NAME.fullmatch(ref[len("$input."):]):
                raise CompositeSpecError("input_reference", f"{node_id}.{port['name']} has an invalid outer-port reference", node_id=node_id)
            outer_port = ref[len("$input."):]
            if outer_port not in port_types:
                raise CompositeSpecError("unknown_macro_input", f"{node_id} references undeclared macro input {outer_port}", node_id=node_id)
            if port_variadic[outer_port] and not port["variadic"]:
                raise CompositeSpecError("input_arity", f"variadic $input.{outer_port} must feed a variadic node port", node_id=node_id)
            actual_type = port_types[outer_port]
        else:
            if ref not in node_types:
                raise CompositeSpecError("forward_or_unknown_reference", f"{node_id} references unavailable local node {ref}", node_id=node_id,
                    details={"reference": ref})
            actual_type = node_types[ref]
            local_refs.add(ref)
        if actual_type != port["type"]:
            raise CompositeSpecError("input_type_mismatch",
                f"{node_id}.{port['name']} expects {port['type']}, got {actual_type}", node_id=node_id)
        result.append(ref)
    return result, local_refs


def _check_composite_cycles(index: Mapping[tuple[str, str], Mapping[str, Any]]) -> None:
    state: dict[tuple[str, str], int] = {}

    def visit(key: tuple[str, str], path: list[tuple[str, str]]) -> None:
        status = state.get(key, 0)
        if status == 1:
            cycle = path[path.index(key):] + [key] if key in path else path + [key]
            raise CompositeSpecError("composite_cycle", "composite dependency cycle: " +
                " -> ".join(f"{name}@{version}" for name, version in cycle))
        if status == 2:
            return
        state[key] = 1
        spec = index[key]
        for dep in spec.get("dependencies", ()):
            dep_key = tuple(dep.split("@", 1)) if isinstance(dep, str) and "@" in dep else None
            if dep_key in index:
                visit(dep_key, path + [key])
        state[key] = 2

    for key in index:
        visit(key, [])


def normalize_composite_spec(raw: Mapping[str, Any], registry: OperatorRegistry,
                             runtime: RuntimeContext | None = None, *,
                             composite_specs: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Validate a closed operator-call macro and bind its identity to canonical program bytes."""
    if not isinstance(raw, Mapping):
        raise CompositeSpecError("spec_shape", "composite spec must be an object")
    allowed = {"name", "version", "description", "family", "input_ports", "output_type",
               "parameters", "authoring_profile", "program"}
    if set(raw) != allowed:
        raise CompositeSpecError("spec_fields", f"composite spec fields must be exactly {sorted(allowed)}")
    name, declared_version = raw["name"], raw["version"]
    if not isinstance(name, str) or not _COMPOSITE_NAME.fullmatch(name):
        raise CompositeSpecError("spec_name", "composite name must use evolved.<lowercase_name>")
    if not isinstance(declared_version, str) or not _VERSION.fullmatch(declared_version):
        raise CompositeSpecError("spec_version", "composite version must be a short declared token")
    if raw.get("authoring_profile") != PROFILE:
        raise CompositeSpecError("authoring_profile", f"composite specs must select {PROFILE}")
    description, family = raw["description"], raw["family"]
    if not isinstance(description, str) or not description.strip() or len(description) > 1024:
        raise CompositeSpecError("description", "composite description must contain 1 to 1024 characters")
    if not isinstance(family, str) or not _FAMILY.fullmatch(family):
        raise CompositeSpecError("family", "composite family must be a lowercase identifier")
    ports = _normalize_ports(raw["input_ports"])
    output_type = raw["output_type"]
    if output_type not in _OUTPUT_TYPES:
        raise CompositeSpecError("output_type", f"composite output_type must be one of {sorted(_OUTPUT_TYPES)}")
    parameters = _normalize_parameters(raw["parameters"], runtime)
    macro_parameters = _parameter_specs(parameters)
    program = raw["program"]
    _json_value(program, "program")
    if not isinstance(program, Mapping) or set(program) != {"schema_version", "nodes", "return"}:
        raise CompositeSpecError("program_shape", "program must contain schema_version, nodes, and return")
    if program.get("schema_version") != PROGRAM_SCHEMA:
        raise CompositeSpecError("program_schema", f"program schema_version must be {PROGRAM_SCHEMA}")
    raw_rows = program.get("nodes")
    if type(raw_rows) is not list or not raw_rows or len(raw_rows) > 40:
        raise CompositeSpecError("program_nodes", "program nodes must be an ordered array of 1 to 40 calls")
    returned = program.get("return")
    if not isinstance(returned, Mapping) or set(returned) != {"node"} or not isinstance(returned.get("node"), str):
        raise CompositeSpecError("program_return", "program return must be exactly {node: local_node_id}")

    existing_index = _spec_index(composite_specs)
    # Composite definitions in the registry are placeholders; this side map
    # carries only normalized macro metadata and never executable code.
    node_types: dict[str, str] = {}
    node_effects: dict[str, set[str]] = {}
    node_capabilities: dict[str, dict[str, str]] = {}
    dependencies: set[str] = set()
    local_dependencies: dict[str, set[str]] = {}
    normalized_rows = []
    outer_types = {port["name"]: port["type"] for port in ports}
    outer_variadic = {port["name"]: port["variadic"] for port in ports}
    outer_port_specs = {port["name"]: port for port in ports}
    used_outer: set[str] = set()
    schema = _runtime_schema(runtime)
    for row in raw_rows:
        if not isinstance(row, Mapping) or set(row) != _NODE_FIELDS:
            raise CompositeSpecError("program_node_shape", "program nodes must use exact e4-dag node fields")
        node_id, op_name, version = row["id"], row["operator"], row["version"]
        if not all(isinstance(item, str) for item in (node_id, op_name, version)) or not _NODE_ID.fullmatch(node_id):
            raise CompositeSpecError("program_node_identity", "program node id/operator/version is invalid")
        if node_id in node_types:
            raise CompositeSpecError("duplicate_program_node", f"duplicate program node id {node_id}", node_id=node_id)
        operator = _operator_spec(registry, existing_index, op_name, version)
        if operator.output_type == "RankedAnswer" or operator.output_type not in _OUTPUT_TYPES:
            raise CompositeSpecError("program_operator_output", f"{op_name}@{version} cannot be used in a composite", node_id=node_id)
        raw_inputs, raw_params, raw_bindings = row["inputs"], row["params"], row["bindings"]
        if not isinstance(raw_inputs, Mapping) or not isinstance(raw_params, Mapping) or not isinstance(raw_bindings, Mapping):
            raise CompositeSpecError("program_node_maps", f"{node_id} inputs/params/bindings must be objects", node_id=node_id)
        operator_ports = {port.name: {"name": port.name, "type": port.value_type,
            "required": port.required, "variadic": port.variadic,
            "minimum_items": port.minimum_items, "maximum_items": port.maximum_items}
            for port in operator.inputs}
        if set(raw_inputs) - set(operator_ports):
            raise CompositeSpecError("unknown_program_input", f"{node_id} has unknown input ports", node_id=node_id)
        normalized_refs: dict[str, list[str]] = {}
        refs_by_port: dict[str, set[str]] = {}
        for port_name, port in operator_ports.items():
            raw_ref = raw_inputs.get(port_name)
            if raw_ref is None:
                if port["required"]:
                    raise CompositeSpecError("missing_program_input", f"{node_id}.{port_name} is required", node_id=node_id)
                normalized_refs[port_name], refs_by_port[port_name] = [], set()
                continue
            refs, local_refs = _program_input_ref(raw_ref, port, node_id, outer_types,
                outer_variadic, node_types, outer=True)
            if port["variadic"]:
                minimum_count, maximum_count = 0, 0
                for ref in refs:
                    if ref.startswith("$input."):
                        source = outer_port_specs[ref[len("$input."):]]
                        if not source["required"] and port["required"]:
                            raise CompositeSpecError("optional_input_required",
                                f"optional macro input {source['name']} cannot feed required {node_id}.{port_name}",
                                node_id=node_id)
                        minimum_count += source["minimum_items"] if source["variadic"] else 1
                        if source["variadic"] and source["maximum_items"] is None:
                            maximum_count = None
                        elif maximum_count is not None:
                            maximum_count += source["maximum_items"] if source["variadic"] else 1
                    else:
                        minimum_count += 1
                        if maximum_count is not None:
                            maximum_count += 1
                if minimum_count < port["minimum_items"]:
                    raise CompositeSpecError("macro_input_minimum",
                        f"{node_id}.{port_name} cannot guarantee its minimum variadic arity", node_id=node_id)
                if port["maximum_items"] is not None and (
                        maximum_count is None or maximum_count > port["maximum_items"]):
                    raise CompositeSpecError("macro_input_maximum",
                        f"{node_id}.{port_name} may exceed its maximum variadic arity", node_id=node_id)
            else:
                for ref in refs:
                    if ref.startswith("$input."):
                        source = outer_port_specs[ref[len("$input."):]]
                        if not source["required"] and port["required"]:
                            raise CompositeSpecError("optional_input_required",
                                f"optional macro input {source['name']} cannot feed required {node_id}.{port_name}",
                                node_id=node_id)
            for ref in refs:
                if ref.startswith("$input."):
                    used_outer.add(ref[len("$input."):])
                    actual_type = outer_types[ref[len("$input."):]]
                else:
                    actual_type = node_types[ref]
                if actual_type not in _LEGAL_INPUTS[operator.output_type]:
                    raise CompositeSpecError("illegal_program_edge",
                        f"{actual_type} cannot feed an operator returning {operator.output_type}", node_id=node_id)
            normalized_refs[port_name] = refs
            refs_by_port[port_name] = local_refs

        node_param_specs = operator.parameters
        if set(raw_params) - set(node_param_specs):
            raise CompositeSpecError("unknown_program_parameter", f"{node_id} has unknown parameters", node_id=node_id)
        if set(raw_bindings) - set(node_param_specs):
            raise CompositeSpecError("unknown_program_binding", f"{node_id} has unknown parameter bindings", node_id=node_id)
        normalized_node_params: dict[str, Any] = {}
        for param_name, value in raw_params.items():
            target = node_param_specs[param_name]
            if isinstance(value, Mapping):
                if set(value) != {"parameter"}:
                    raise CompositeSpecError("program_parameter_expression",
                        f"{node_id}.{param_name} permits only exact macro parameter references", node_id=node_id)
                _parameter_ref_type(value, macro_parameters, target, node_id)
                normalized_node_params[param_name] = {"parameter": value["parameter"]}
            else:
                _json_value(value, f"{node_id}.{param_name}")
                try:
                    _validate_parameter(target, value)
                except RuntimeResolutionError as exc:
                    raise CompositeSpecError("program_parameter_value",
                        f"{node_id}.{param_name}: {exc}", node_id=node_id) from exc
                normalized_node_params[param_name] = value
        normalized_bindings = {}
        for param_name, binding in raw_bindings.items():
            if not isinstance(binding, str) or not binding.startswith("runtime."):
                raise CompositeSpecError("program_runtime_binding",
                    f"{node_id}.{param_name} must bind to runtime.<field>", node_id=node_id)
            field_name = binding.removeprefix("runtime.")
            runtime_field = schema.fields.get(field_name)
            if runtime_field is None:
                raise CompositeSpecError("unknown_runtime_field",
                    f"{node_id}.{param_name} binds unknown runtime.{field_name}", node_id=node_id)
            if not runtime_field.writable:
                raise CompositeSpecError("readonly_runtime_binding",
                    f"{node_id}.{param_name} cannot bind read-only runtime.{field_name}", node_id=node_id)
            if not _runtime_type_compatible(runtime_field.value_type, node_param_specs[param_name].value_type):
                raise CompositeSpecError("runtime_binding_type",
                    f"runtime.{field_name} is incompatible with {node_id}.{param_name}", node_id=node_id)
            normalized_bindings[param_name] = f"runtime.{field_name}"
        for param_name, param_spec in node_param_specs.items():
            if param_name not in normalized_node_params and param_name not in normalized_bindings:
                if param_spec.required and not param_spec.has_default and not param_spec.runtime_field:
                    raise CompositeSpecError("missing_program_parameter",
                        f"{node_id}.{param_name} is required", node_id=node_id)
            if param_spec.runtime_field and param_spec.runtime_field not in schema.fields:
                raise CompositeSpecError("unknown_runtime_field",
                    f"{node_id}.{param_name} names unknown runtime.{param_spec.runtime_field}", node_id=node_id)

        macro = existing_index.get((op_name, version))
        current_effects = set()
        if macro is not None:
            dependencies.add(f"{op_name}@{version}")
            current_effects |= set(macro.get("program_effects", ()))
        else:
            for refs in refs_by_port.values():
                for ref in refs:
                    if ref in node_effects:
                        current_effects |= node_effects[ref]
            if op_name == "bank.public.query":
                _validate_query_catalog(node_id, normalized_node_params, normalized_bindings,
                    node_param_specs, macro_parameters)
                current_effects.add("public_query")
            if op_name == "bank.public.project":
                input_effect_sets = [node_effects.get(ref, set()) for refs in refs_by_port.values() for ref in refs]
                _validate_project_catalog(node_id, normalized_node_params, normalized_bindings,
                    node_param_specs, macro_parameters, input_effect_sets)
                current_effects.add("projected")
        _check_aggregate_consumer(op_name, node_id,
            {port_name: tuple(refs) for port_name, refs in refs_by_port.items()}, node_capabilities)
        capability = _AGGREGATE_PRODUCERS.get(op_name)
        if capability is not None:
            node_capabilities[node_id] = {"capability": capability,
                "producer": op_name, "version": version}
        normalized_row = {"id": node_id, "operator": op_name, "version": version,
            "inputs": {}, "params": normalized_node_params, "bindings": normalized_bindings}
        # Omit absent optional ports; explicit empty variadic lists stay visible.
        for key, refs in normalized_refs.items():
            port = operator_ports[key]
            if not refs and not port["required"]:
                if key in raw_inputs:
                    normalized_row["inputs"][key] = []
                continue
            normalized_row["inputs"][key] = refs if port["variadic"] else refs[0]
        normalized_rows.append(normalized_row)
        node_types[node_id] = operator.output_type
        node_effects[node_id] = current_effects
        local_dependencies[node_id] = set().union(*refs_by_port.values()) if refs_by_port else set()

    return_id = returned["node"]
    if return_id not in node_types:
        raise CompositeSpecError("unknown_program_return", f"program returns unknown node {return_id}")
    if node_types[return_id] != output_type:
        raise CompositeSpecError("program_output_type",
            f"program returns {node_types[return_id]}, spec declares {output_type}", node_id=return_id)
    reachable = set()
    stack = [return_id]
    while stack:
        node_id = stack.pop()
        if node_id in reachable:
            continue
        reachable.add(node_id)
        stack.extend(local_dependencies.get(node_id, ()))
    unused = sorted(set(node_types) - reachable)
    if unused:
        raise CompositeSpecError("unused_program_nodes", f"program nodes do not contribute to return: {unused}",
            details={"node_ids": unused})

    normalized_program = {"schema_version": PROGRAM_SCHEMA, "nodes": normalized_rows,
                          "return": {"node": return_id}}
    program_sha = hashlib.sha256(_canonical(normalized_program)).hexdigest()
    # A composite's identity is content-addressed over all public semantics,
    # including the exact canonical call graph.
    signature = {"name": name, "declared_version": declared_version,
        "description": description.strip(), "family": family, "input_ports": ports,
        "output_type": output_type, "parameters": parameters, "authoring_profile": PROFILE,
        "program_sha256": program_sha}
    signature_sha = hashlib.sha256(_canonical(signature)).hexdigest()
    version_hash = hashlib.sha256(_canonical({"signature": signature,
        "program_sha256": program_sha})).hexdigest()
    normalized = {**signature, "version": f"{declared_version}+{version_hash[:16]}",
        "program": normalized_program, "program_sha256": program_sha,
        "signature_sha256": signature_sha, "dependencies": sorted(dependencies),
        "program_effects": sorted(node_effects.get(return_id, set()))}
    _check_composite_cycles({**existing_index, (name, normalized["version"]): normalized})
    return normalized


def _runtime_type_compatible(runtime_kind: str, parameter_kind: str) -> bool:
    if runtime_kind == parameter_kind:
        return True
    return (runtime_kind in {"integer", "positive_integer"} and parameter_kind == "number") or (
        runtime_kind == "positive_integer" and parameter_kind == "integer")


def _static_values(parameter: str, params: Mapping[str, Any], bindings: Mapping[str, str],
                   specs: Mapping[str, ParameterSpec], macro_params: Mapping[str, ParameterSpec],
                   node_id: str) -> tuple[Any, ...] | None:
    spec = specs[parameter]
    if parameter in bindings:
        return None
    if parameter in params:
        values = _possible_parameter_values(params[parameter], macro_params, spec)
    else:
        values = _possible_parameter_values(None, macro_params, spec, missing=True)
    return values


def _validate_query_catalog(node_id: str, params: Mapping[str, Any], bindings: Mapping[str, str],
                            specs: Mapping[str, ParameterSpec], macro_params: Mapping[str, ParameterSpec]) -> None:
    from .m4_data import MAX_OFFSET, MAX_ROWS_PER_REQUEST, TABLE_SCHEMAS

    table_values = _static_values("table", params, bindings, specs, macro_params, node_id)
    window_values = _static_values("window", params, bindings, specs, macro_params, node_id)
    fields_values = _static_values("fields", params, bindings, specs, macro_params, node_id)
    metric_values = _static_values("metric", params, bindings, specs, macro_params, node_id)
    entity_values = _static_values("entity", params, bindings, specs, macro_params, node_id)
    offset_values = _static_values("offset", params, bindings, specs, macro_params, node_id)
    limit_values = _static_values("limit", params, bindings, specs, macro_params, node_id)
    if table_values is None:
        return
    if not table_values or any(type(item) is not str or item not in TABLE_SCHEMAS for item in table_values):
        raise CompositeSpecError("query_table_unknown", f"{node_id}.table must select a registered public table", node_id=node_id)
    if window_values is not None and (not window_values or any(item not in {"reference", "alert"} for item in window_values)):
        raise CompositeSpecError("query_window_unknown", f"{node_id}.window must select reference or alert", node_id=node_id)
    if offset_values is not None and any(type(item) is not int or not 0 <= item <= MAX_OFFSET for item in offset_values):
        raise CompositeSpecError("query_offset_range", f"{node_id}.offset exceeds the public query bounds", node_id=node_id)
    if limit_values is not None and any(type(item) is not int or not 1 <= item <= MAX_ROWS_PER_REQUEST for item in limit_values):
        raise CompositeSpecError("query_limit_range", f"{node_id}.limit exceeds the public query bounds", node_id=node_id)
    if entity_values is not None and any(value is not None and (
            type(value) is not str or not value or len(value.encode("utf-8")) > 256) for value in entity_values):
        raise CompositeSpecError("query_entity_invalid", f"{node_id}.entity must be null or nonempty text <=256 bytes", node_id=node_id)
    if metric_values is not None and any(value is not None and (
            type(value) is not str or not value or len(value.encode("utf-8")) > 256) for value in metric_values):
        raise CompositeSpecError("query_metric_invalid", f"{node_id}.metric must be null or nonempty text <=256 bytes", node_id=node_id)
    normalized_fields = []
    for fields in fields_values or ():
        if fields in (None, (), []):
            normalized_fields.append(None)
            continue
        if type(fields) not in (list, tuple) or not fields or any(type(item) is not str for item in fields):
            raise CompositeSpecError("query_fields_shape", f"{node_id}.fields must be omitted or a nonempty string list", node_id=node_id)
        if len(fields) != len(set(fields)):
            raise CompositeSpecError("query_fields_duplicate", f"{node_id}.fields contains duplicates", node_id=node_id)
        normalized_fields.append(tuple(fields))
    for table in table_values:
        known_fields = set(TABLE_SCHEMAS[table]) | {"timestamp_ms_utc"}
        for fields in normalized_fields:
            if fields is not None:
                unknown = set(fields) - known_fields
                if unknown:
                    raise CompositeSpecError("query_field_unknown",
                        f"{node_id}.fields names columns absent from {table}: {sorted(unknown)}", node_id=node_id,
                        details={"table": table, "unknown_fields": sorted(unknown)})
        for metric in metric_values or ():
            if table == "metric/metric_app.csv" and metric is not None:
                if metric not in {"rr", "sr", "cnt", "mrt"}:
                    raise CompositeSpecError("query_metric_unknown", f"{node_id}.metric is invalid for metric_app.csv", node_id=node_id)
                for fields in normalized_fields:
                    if fields is not None and set(fields) & ({"rr", "sr", "cnt", "mrt"} - {metric}):
                        raise CompositeSpecError("query_metric_field_conflict",
                            f"{node_id}.fields exposes metrics other than selected {metric}", node_id=node_id)
            if table == "trace/trace_span.csv" and metric is not None and metric != "duration":
                raise CompositeSpecError("query_metric_unknown", f"{node_id}.metric must be duration for trace spans", node_id=node_id)


def _validate_project_catalog(node_id: str, params: Mapping[str, Any], bindings: Mapping[str, str],
                              specs: Mapping[str, ParameterSpec], macro_params: Mapping[str, ParameterSpec],
                              input_effects: Sequence[set[str]]) -> None:
    from .m4_data import MAX_OFFSET
    from .m4_projection import MAX_PAGE_ROWS

    source_fields = _static_values("source_fields", params, bindings, specs, macro_params, node_id)
    entities = _static_values("entities", params, bindings, specs, macro_params, node_id)
    statistics = _static_values("statistics", params, bindings, specs, macro_params, node_id)
    offset_values = _static_values("offset", params, bindings, specs, macro_params, node_id)
    limit_values = _static_values("limit", params, bindings, specs, macro_params, node_id)
    for values in source_fields or ():
        if values in (None, (), []):
            continue
        if (type(values) not in (list, tuple) or len(values) > 256 or
                any(type(item) is not str or not item.strip() or len(item.encode("utf-8")) > 4096
                    for item in values)):
            raise CompositeSpecError("project_fields_shape", f"{node_id}.source_fields must be a string list", node_id=node_id)
        if len(set(values)) != len(values):
            raise CompositeSpecError("project_fields_duplicate", f"{node_id}.source_fields contains duplicates", node_id=node_id)
    for parameter, values, fold in (("entities", entities, True), ("statistics", statistics, False)):
        for items in values or ():
            if items in (None, (), []):
                continue
            if (type(items) not in (list, tuple) or len(items) > 256 or
                    any(type(item) is not str or not item.strip() or len(item.encode("utf-8")) > 4096
                        for item in items)):
                raise CompositeSpecError("project_filter_shape", f"{node_id}.{parameter} must be a bounded string list", node_id=node_id)
            canonical = [item.strip().casefold() if fold else item.strip() for item in items]
            if len(set(canonical)) != len(canonical):
                raise CompositeSpecError("project_filter_duplicate", f"{node_id}.{parameter} contains duplicates", node_id=node_id)
    if offset_values is not None and any(type(value) is not int or not 0 <= value <= MAX_OFFSET for value in offset_values):
        raise CompositeSpecError("project_offset_range", f"{node_id}.offset exceeds projection bounds", node_id=node_id)
    if limit_values is not None and any(type(value) is not int or not 1 <= value <= MAX_PAGE_ROWS for value in limit_values):
        raise CompositeSpecError("project_limit_range", f"{node_id}.limit exceeds projection bounds", node_id=node_id)
    if any(effect & {"public_query", "projected"} for effect in input_effects):
        raise CompositeSpecError("project_reprojection", f"{node_id} cannot project a public-query page or a prior projection",
            node_id=node_id)


