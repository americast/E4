"""Offline rendering of adapter-first and mode-aware library prompts."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from importlib import resources
from typing import Any

from .authoring import authoring_contract_reference

_RESOURCE_NAMES = {
    "initialization_system.md",
    "domain_brief_template.md",
    "bank_domain_brief.md",
    "bank_domain_priors.md",
    "adapter_system.md",
    "bootstrap_system.md",
    "clean_slate_system.md",
}


def load_initialization_resource(name: str) -> str:
    """Read a bundled, human-reviewable prompt resource by its fixed filename."""
    if name not in _RESOURCE_NAMES:
        raise ValueError(f"Unknown initialization prompt resource: {name!r}")
    return resources.files("e4.prompts").joinpath("resources", name).read_text(encoding="utf-8")


def _json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _prompt_simplify(value: Any) -> Any:
    """Drop only redundant optional metadata while preserving effective contracts."""
    if isinstance(value, Mapping):
        has_explicit_default = value.get("has_default") is True
        simplified: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"nullable", "has_default"} and item is False:
                continue
            if key in {"runtime_field", "estimator", "maximum_items"} and item is None:
                continue
            if key == "default" and item is None and not has_explicit_default:
                continue
            if key in {"choices", "constraints"} and item in ([], {}):
                continue
            simplified[key] = _prompt_simplify(item)
        return simplified
    if isinstance(value, (list, tuple)):
        return [_prompt_simplify(item) for item in value]
    return value


def _compact_availability(value: Any) -> Any:
    """Keep the full catalog out of prompts; include only bounded status facts."""
    if not isinstance(value, Mapping):
        return value
    result: dict[str, Any] = {"counts": value.get("counts", {})}
    for key in ("operators", "playbooks"):
        entries = value.get(key, ())
        result[key] = [
            {
                "name": item.get("name"),
                "availability": item.get("availability"),
                "reason": item.get("availability_reason", item.get("gap_reason", "")),
            }
            for item in entries
            if isinstance(item, Mapping)
        ]
    result["active_operator_names"] = value.get("active_operator_names", [])
    result["active_playbook_names"] = value.get("active_playbook_names", [])
    result["formula_disclosure"] = value.get("formula_disclosure", "Exact formulas and DAGs are not published.")
    return result


def _compact_reference_examples(
    examples: Any, active_inventory: Any, mode: str
) -> list[dict[str, Any]]:
    """Avoid repeating active operator contracts; retain bounded reference DAGs."""
    active_versions = {
        (item.get("name"), item.get("version"))
        for item in active_inventory.get("operators", [])
        if isinstance(active_inventory, Mapping)
        and isinstance(item, Mapping)
    }
    compact: list[dict[str, Any]] = []
    playbook_count = 0
    for item in examples if isinstance(examples, (list, tuple)) else ():
        if not isinstance(item, Mapping):
            continue
        kind = item.get("kind")
        name = item.get("name")
        if kind == "operator":
            versions = [version for active_name, version in active_versions if active_name == name]
            if mode == "paper" and versions:
                purpose = item.get("semantics") or item.get("description", "")
                compact.append({
                    "kind": "active operator example reference",
                    "purpose": str(purpose)[:240],
                    "qualified_name": f"{name}@{versions[0]}",
                    "see": "active callable inventory for exact ports, parameters, and requirements",
                })
            else:
                compact.append({
                    "kind": "reference-only operator contract",
                    "name": name,
                    "purpose": item.get("description", ""),
                    "semantics": item.get("semantics", ""),
                    "inputs": item.get("inputs", []),
                    "output_type": item.get("output_type"),
                    "parameters": item.get("parameters", {}),
                })
        elif kind == "playbook" and playbook_count < 2:
            compact.append({
                "kind": "reference-only playbook DAG example",
                "name": name,
                "description": item.get("description", ""),
                "dag": item.get("dag", {}),
            })
            playbook_count += 1
    return _prompt_simplify(compact)


def _paper_context(schema: Any, mode: str) -> tuple[Any, Any, Any]:
    """Load deterministic active inventory and reference-only samples on demand."""
    # Deliberately lazy: importing paper implementations can import the numerical
    # runtime. Prompt export without a schema must not initialize that catalog.
    from e4.onboarding import paper_availability, paper_reference_examples, prepare_library

    references = paper_reference_examples()
    if schema is None:
        pending = {
            "status": "pending_schema",
            "message": "No validated adapter schema supplied; active compatibility and inventory are not asserted.",
        }
        if mode == "clean_slate":
            empty_library = prepare_library(None, mode=mode)
            return empty_library.as_dict(), pending, references
        return pending, pending, references

    normalized_schema = schema
    from e4.onboarding import normalize_dataset_schema

    normalized_schema = normalize_dataset_schema(schema)
    library = prepare_library(normalized_schema, mode=mode)
    availability = _compact_availability(paper_availability(normalized_schema))
    return library.as_dict(), availability, references


def render_initialization_prompt(
    domain_brief: str | None = None,
    *,
    include_bank: bool = False,
    include_domain_priors: bool = False,
    clean_slate: bool = False,
    schema: Mapping[str, Any] | Any | None = None,
) -> str:
    """Render default paper bootstrap or clean-slate generation without a model call."""
    if include_domain_priors and not include_bank:
        raise ValueError("include_domain_priors requires include_bank=True")

    mode = "clean_slate" if clean_slate else "paper"
    system = load_initialization_resource("initialization_system.md")

    if include_bank:
        domain = load_initialization_resource("bank_domain_brief.md")
        if domain_brief:
            domain += "\n\n# Additional user-supplied domain brief\n\n" + domain_brief.strip() + "\n"
        domain_variant = "Bank essentials" + (" + evaluation-informed priors" if include_domain_priors else "")
    else:
        domain = domain_brief if domain_brief is not None else load_initialization_resource(
            "domain_brief_template.md"
        )
        domain_variant = "user-supplied" if domain_brief is not None else "fillable generic"

    inventory, availability, references = _paper_context(schema, mode)
    inventory = _prompt_simplify(inventory)
    availability = _prompt_simplify(availability)
    references = _compact_reference_examples(references, inventory, mode)
    schema_payload = schema.as_dict() if hasattr(schema, "as_dict") else schema
    schema_text = _json_text(schema_payload) if schema is not None else "PENDING: provide validated adapter schema JSON before claiming active compatibility."
    prior_text = (
        load_initialization_resource("bank_domain_priors.md")
        if include_domain_priors
        else "No additional domain analytical priors supplied."
    )
    prompt_name = "clean_slate_system.md" if clean_slate else "bootstrap_system.md"
    mode_template = load_initialization_resource(prompt_name)
    substitutions = {
        "DATASET_SCHEMA": schema_text,
        "ACTIVE_INVENTORY": _json_text(inventory),
        "AVAILABILITY": _json_text(availability),
        "REFERENCE_EXAMPLES": _json_text(references),
        "DOMAIN_BRIEF": domain,
        "DOMAIN_PRIORS": prior_text,
        "AUTHORING_REFERENCE": authoring_contract_reference(),
    }
    rendered = re.sub(
        r"\{(DATASET_SCHEMA|ACTIVE_INVENTORY|AVAILABILITY|REFERENCE_EXAMPLES|DOMAIN_BRIEF|DOMAIN_PRIORS|AUTHORING_REFERENCE)\}",
        lambda match: substitutions[match.group(1)],
        mode_template,
    )
    label = "CLEAN-SLATE GENERATION" if clean_slate else "PRE-RUN BOOTSTRAP (PAPER DEFAULT)"
    inventory_note = (
        "Executable paper inventory excluded; automatic bootstrap is skipped."
        if clean_slate
        else "Shipped compatible paper library remains active; prompt proposes additions only."
    )
    return (
        "# E4 Initialization Prompt Review Export\n\n"
        "This is a deterministic prompt-only export. No model is called and no library is generated.\n\n"
        f"Stage: **{label}**\n\nMode: `{mode}` — {inventory_note}\n\n"
        f"Domain variant: **{domain_variant}**\n\n"
        "## Shared generic instructions\n\n"
        + system.strip()
        + "\n\n## Mode-specific task prompt\n\n"
        + rendered.strip()
        + "\n"
    )
