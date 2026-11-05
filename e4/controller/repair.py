"""Bounded static repair loop preserving rejected source code."""

from __future__ import annotations

import json
from typing import Any, Callable, Mapping, Sequence

from ..prompts.templates import format_repair_prompt
from ..registry import OperatorSpec, ParameterSpec, PortSpec
from ..sandbox.ast_validator import SecurityValidationError, validate_source
from ..sandbox.typechecker import TypeCheckError, check_operator_types
from .schemas import validate_proposal_bundle


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
                    default=v.get("default"),
                    has_default="default" in v,
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
                )
            )

        return errors, parsed_specs

    def attempt_repairs(
        self,
        initial_bundle: Mapping[str, Any],
        query_model_fn: Callable[[str], str],
        dataset_schema: Any = None,
    ) -> tuple[bool, list[OperatorSpec], list[str], int]:
        """Run repair loop up to max_repairs, preserving rejected source in feedback."""
        current_bundle = initial_bundle
        attempt = 0
        all_errors: list[str] = []

        while attempt <= self.max_repairs:
            errors, specs = self.validate_and_typecheck(current_bundle, dataset_schema)
            if not errors and specs:
                return True, specs, [], attempt

            all_errors = errors
            attempt += 1
            if attempt > self.max_repairs:
                break

            # Collect sources to preserve in repair prompt
            rejected_sources: dict[str, str] = {}
            if isinstance(current_bundle, Mapping):
                for op in current_bundle.get("proposed_operators", []):
                    if isinstance(op, Mapping) and "name" in op and "source" in op:
                        rejected_sources[op["name"]] = str(op["source"])

            repair_prompt = format_repair_prompt(errors, rejected_sources)
            try:
                raw_response = query_model_fn(repair_prompt)
                clean = raw_response.strip()
                parsed = None
                try:
                    parsed = json.loads(clean)
                except Exception:
                    pass
                if parsed is None and "```" in clean:
                    parts = clean.split("```")
                    for i in range(1, len(parts), 2):
                        block = parts[i].strip()
                        if block.startswith("json"):
                            block = block[4:].strip()
                        try:
                            parsed = json.loads(block)
                            break
                        except Exception:
                            continue
                if parsed is None:
                    first_brace = clean.find("{")
                    last_brace = clean.rfind("}")
                    if first_brace != -1 and last_brace > first_brace:
                        parsed = json.loads(clean[first_brace : last_brace + 1])
                if parsed is None:
                    parsed = json.loads(clean)
                current_bundle = parsed
            except Exception as exc:
                all_errors.append(f"Repair response parsing failed: {exc}")

        return False, [], all_errors, attempt
