"""Authoring contract schema, validator, and batched error collection."""

from __future__ import annotations

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


def validate_proposal_bundle(bundle: Mapping[str, Any]) -> list[str]:
    """Validate full EVOLVE output bundle and collect all errors across all operators."""
    errors: list[str] = []

    if not isinstance(bundle, Mapping):
        return ["Response is not a valid JSON object"]

    gap_record = bundle.get("gap_record")
    if not isinstance(gap_record, Mapping):
        errors.append("Missing or invalid 'gap_record' object")

    proposed_ops = bundle.get("proposed_operators")
    if not isinstance(proposed_ops, list) or not proposed_ops:
        errors.append("'proposed_operators' must be a non-empty list of operator proposals")
        return errors

    seen_sources: dict[str, str] = {}
    for idx, op in enumerate(proposed_ops):
        op_name = op.get("name", f"operator_{idx}") if isinstance(op, Mapping) else f"operator_{idx}"
        if isinstance(op, Mapping):
            src = op.get("source", "").strip()
            norm_src = " ".join(src.split())
            if norm_src:
                if norm_src in seen_sources:
                    errors.append(
                        f"[{op_name}] Structural duplicate body: identical implementation to '{seen_sources[norm_src]}'"
                    )
                else:
                    seen_sources[norm_src] = op_name
        op_errors = validate_operator_proposal(op)
        for err in op_errors:
            errors.append(f"[{op_name}] {err}")

    return errors
