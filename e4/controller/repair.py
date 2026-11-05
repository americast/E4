"""Bounded static repair loop preserving rejected source code."""

from __future__ import annotations

import json
from typing import Any, Callable, Mapping, Sequence

from ..prompts.templates import format_repair_prompt
from ..registry import OperatorSpec, ParameterSpec, PortSpec
from ..sandbox.ast_validator import SecurityValidationError, validate_source
from ..sandbox.typechecker import TypeCheckError, check_operator_types
from .schemas import validate_proposal_bundle


def _parse_bundle(raw: Any) -> tuple[dict[str, Any] | None, str | None]:
    if isinstance(raw, Mapping):
        return dict(raw), None
    if not isinstance(raw, str):
        return None, f"Expected JSON string or mapping, got {type(raw).__name__}"
    clean = raw.strip()
    try:
        return json.loads(clean), None
    except Exception:
        pass
    if "```" in clean:
        parts = clean.split("```")
        for i in range(1, len(parts), 2):
            block = parts[i].strip()
            if block.startswith("json"):
                block = block[4:].strip()
            try:
                return json.loads(block), None
            except Exception:
                continue
    first_brace = clean.find("{")
    last_brace = clean.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        try:
            return json.loads(clean[first_brace : last_brace + 1]), None
        except Exception:
            pass
    return None, f"Failed to parse JSON response: {clean[:100]}..."


class StaticRepairController:
    """Manages bounded repair attempts for operator proposals with batched error reporting."""

    def __init__(self, max_repairs: int = 2):
        self.max_repairs = max_repairs

    def validate_and_typecheck(
        self,
        bundle: Mapping[str, Any],
        dataset_schema: Any = None,
    ) -> tuple[list[str], list[OperatorSpec]]:
        """Validate syntax, schema, AST security, and strict Mypy types across all proposals."""
        errors: list[str] = []
        parsed_specs: list[OperatorSpec] = []

        # 1. Structural schema validation
        schema_errors = validate_proposal_bundle(bundle)
        if schema_errors:
            return schema_errors, []

        proposed = bundle.get("proposed_operators", [])
        for op_doc in proposed:
            name = op_doc.get("name", "unknown")
            source = op_doc.get("source", "")

            # 2. AST security check
            try:
                validate_source(source)
            except SecurityValidationError as exc:
                errors.append(f"[{name}] AST security error at line {exc.line}, col {exc.column}: {exc}")
                continue
            except Exception as exc:
                errors.append(f"[{name}] AST validation error: {exc}")
                continue

            # 3. Strict Mypy type-checking
            try:
                check_operator_types(op_doc, dataset_schema)
            except TypeCheckError as exc:
                errors.append(f"[{name}] Typecheck failure: {exc}")
                continue
            except Exception as exc:
                errors.append(f"[{name}] Typecheck error: {exc}")
                continue

            # Parse into OperatorSpec
            ports = [
                PortSpec(
                    name=p["name"],
                    value_type=p["type"],
                    required=p.get("required", True),
                    variadic=p.get("variadic", False),
                )
                for p in op_doc.get("input_ports", [])
            ]
            params = {
                k: ParameterSpec(
                    name=k,
                    value_type=v.get("type", "string"),
                    required=v.get("required", False),
                    nullable=v.get("nullable", False),
                    default=v.get("default"),
                    has_default=v.get("has_default", "default" in v),
                    runtime_field=v.get("runtime_field"),
                    estimator=v.get("estimator"),
                    description=v.get("description", ""),
                    choices=tuple(v.get("choices", ())),
                    constraints=v.get("constraints", {}),
                )
                for k, v in op_doc.get("parameters", {}).items()
            }
            parsed_specs.append(
                OperatorSpec(
                    name=name,
                    version=str(op_doc.get("version", "1")),
                    inputs=tuple(ports),
                    output_type=op_doc.get("output_type", "Aggregate"),
                    parameters=params,
                    family=op_doc.get("family", "evolved"),
                    authoring_profile="typed-operator-code-v1",
                    source=source,
                    description=op_doc.get("description", ""),
                    input_requirements=dict(op_doc.get("input_requirements", {})),
                )
            )

        return errors, parsed_specs

    def attempt_repairs(
        self,
        initial_bundle: str | Mapping[str, Any],
        query_model_fn: Callable[[str], str],
        dataset_schema: Any = None,
    ) -> tuple[bool, list[OperatorSpec], list[str], int]:
        """Run repair loop up to max_repairs, accurately counting model calls."""
        current_raw: Any = initial_bundle
        repairs_used = 0
        all_errors: list[str] = []

        while repairs_used <= self.max_repairs:
            bundle_dict, parse_err = _parse_bundle(current_raw)
            if parse_err is not None:
                errors = [parse_err]
                specs: list[OperatorSpec] = []
            else:
                errors, specs = self.validate_and_typecheck(bundle_dict or {}, dataset_schema)

            if not errors and specs:
                return True, specs, [], repairs_used

            all_errors = errors
            if repairs_used >= self.max_repairs:
                break

            # Collect sources to preserve in repair prompt
            rejected_sources: dict[str, str] = {}
            if isinstance(bundle_dict, Mapping):
                for op in bundle_dict.get("proposed_operators", []):
                    if isinstance(op, Mapping) and "name" in op and "source" in op:
                        rejected_sources[op["name"]] = str(op["source"])

            repair_prompt = format_repair_prompt(errors, rejected_sources)
            repairs_used += 1
            try:
                current_raw = query_model_fn(repair_prompt)
            except Exception as exc:
                all_errors.append(f"Repair model call failed: {exc}")
                break

        return False, [], all_errors, repairs_used
