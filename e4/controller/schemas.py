"""Authoring contract schema, validator, and batched error collection."""

from __future__ import annotations

import ast
import re
from typing import Any, Mapping, Sequence

EVOLVED_NAME_PATTERN = re.compile(r"^evolved\.[a-z][a-z0-9_]{0,62}$")
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
VALID_OUTPUT_TYPES = frozenset({"Aggregate", "CandidateSet", "ScoredCandidates"})
VALID_PORT_TYPES = frozenset({"Telemetry", "Aggregate", "CandidateSet", "ScoredCandidates"})
REQUIRED_SPEC_FIELDS = frozenset({
    "name", "version", "authoring_profile", "description",
    "family", "input_ports", "output_type", "parameters",
    "input_requirements", "source",
})
MAX_PROPOSED_OPERATORS = 3


def validate_operator_proposal(spec: Mapping[str, Any]) -> list[str]:
    """Validate a single operator proposal and return all detected errors."""
    errors: list[str] = []

    # Check top-level required fields
    missing_fields = REQUIRED_SPEC_FIELDS - set(spec.keys())
    if missing_fields:
        errors.append(f"Missing required field(s): {sorted(missing_fields)}")

    name = spec.get("name")
    if not isinstance(name, str) or not EVOLVED_NAME_PATTERN.fullmatch(name):
        errors.append(
            f"Invalid operator name {name!r}: must match regex '^evolved\\.[a-z][a-z0-9_]{{0,62}}$' (e.g., 'evolved.metric_ratio')"
        )

    version = spec.get("version")
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        errors.append(f"Invalid version {version!r}: must be a short identifier token like '1'")

    profile = spec.get("authoring_profile")
    if profile != "typed-operator-code-v1":
        errors.append(f"Invalid authoring_profile {profile!r}: must be 'typed-operator-code-v1'")

    output_type = spec.get("output_type")
    if output_type not in VALID_OUTPUT_TYPES:
        errors.append(f"Invalid output_type {output_type!r}: must be one of {sorted(VALID_OUTPUT_TYPES)}")

    input_ports = spec.get("input_ports")
    if not isinstance(input_ports, list) or not input_ports:
        errors.append("'input_ports' must be a non-empty list of port specifications")
    else:
        for idx, port in enumerate(input_ports):
            if not isinstance(port, Mapping):
                errors.append(f"input_ports[{idx}] must be a mapping")
                continue
            pname = port.get("name")
            ptype = port.get("type")
            if not pname or not isinstance(pname, str):
                errors.append(f"input_ports[{idx}] missing valid 'name'")
            if ptype not in VALID_PORT_TYPES:
                errors.append(f"input_ports[{idx}] invalid type {ptype!r}: must be in {sorted(VALID_PORT_TYPES)}")

    input_reqs = spec.get("input_requirements")
    if not isinstance(input_reqs, Mapping):
        errors.append("'input_requirements' must be a mapping defining requirements for every input port")
    elif isinstance(input_ports, list):
        for port in input_ports:
            if isinstance(port, Mapping) and "name" in port:
                pname = port["name"]
                if pname not in input_reqs:
                    errors.append(f"input_requirements missing declaration for port '{pname}'")
                elif not isinstance(input_reqs[pname], Mapping):
                    errors.append(
                        f"input_requirements for port '{pname}' must be a mapping contract, got {type(input_reqs[pname]).__name__}"
                    )

    source = spec.get("source")
    if not isinstance(source, str) or not source.strip():
        errors.append("'source' must be non-empty Python code")

    return errors


def canonicalize_ast_source(source: str) -> str:
    try:
        tree = ast.parse(source)
        return ast.dump(tree, annotate_fields=False, include_attributes=False)
    except Exception:
        return " ".join(source.split())


def validate_proposal_bundle(bundle: Mapping[str, Any], existing_library: Any = None) -> list[str]:
    """Validate full EVOLVE output bundle and collect all errors across all operators."""
    errors: list[str] = []

    if not isinstance(bundle, Mapping):
        return ["Response is not a valid JSON object"]

    proposed_ops = bundle.get("proposed_operators")
    if not isinstance(proposed_ops, list) or not proposed_ops:
        errors.append("'proposed_operators' must be a non-empty list of operator proposals")
        return errors
    if len(proposed_ops) > MAX_PROPOSED_OPERATORS:
        return [
            f"'proposed_operators' contains {len(proposed_ops)} entries; "
            f"the maximum for one EVOLVE proposal is {MAX_PROPOSED_OPERATORS}"
        ]

    gap_record = bundle.get("gap_record")
    if not isinstance(gap_record, Mapping):
        errors.append("Missing or invalid 'gap_record' object")
    else:
        gap_desc = gap_record.get("gap_description")
        if not isinstance(gap_desc, str) or not gap_desc.strip():
            errors.append("gap_record.gap_description must be a non-empty string explaining the analysis gap")
        prop_names = gap_record.get("proposed_operator_names")
        if not isinstance(prop_names, (list, tuple)) or not prop_names:
            errors.append("gap_record.proposed_operator_names must be a non-empty sequence of proposed operator names")
        else:
            op_names_in_bundle = [
                op.get("name") for op in proposed_ops if isinstance(op, Mapping) and "name" in op
            ]
            for p_name in prop_names:
                if p_name not in op_names_in_bundle:
                    errors.append(f"gap_record references '{p_name}', but operator is not found in proposed_operators")

    # Extract existing sources from active library/registry if provided
    existing_sources: dict[str, str] = {}
    if existing_library is not None:
        ops_iterable = []
        if hasattr(existing_library, "all_operators"):
            ops_iterable = existing_library.all_operators()
        elif hasattr(existing_library, "registry") and hasattr(existing_library.registry, "all_operators"):
            ops_iterable = existing_library.registry.all_operators()
        for existing_op in ops_iterable:
            if getattr(existing_op, "source", None):
                existing_sources[canonicalize_ast_source(existing_op.source)] = getattr(
                    existing_op, "qualified_name", getattr(existing_op, "name", "existing")
                )

    seen_sources: dict[str, str] = {}
    for idx, op in enumerate(proposed_ops):
        op_name = op.get("name", f"operator_{idx}") if isinstance(op, Mapping) else f"operator_{idx}"
        if isinstance(op, Mapping):
            src = op.get("source", "").strip()
            if src:
                ast_canon = canonicalize_ast_source(src)
                if ast_canon in seen_sources:
                    errors.append(
                        f"[{op_name}] Structural duplicate body: identical implementation to '{seen_sources[ast_canon]}'"
                    )
                elif ast_canon in existing_sources:
                    errors.append(
                        f"[{op_name}] Duplicate implementation: identical source to existing registered operator '{existing_sources[ast_canon]}'"
                    )
                else:
                    seen_sources[ast_canon] = op_name
        op_errors = validate_operator_proposal(op)
        for err in op_errors:
            errors.append(f"[{op_name}] {err}")

    return errors
