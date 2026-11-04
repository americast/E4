"""Three-stage M4 Bank controller with bounded prompts and binary-only escalation."""

from __future__ import annotations

import ast
import contextvars
import hashlib
import json
import math
import os
import re
import shutil
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable, Mapping, Sequence

from .bank_adapter import BankCase, load_bank_case, sha256_file
from .bank_ops import bank_registry
from .benchmark import REPAIR_LIMIT, StaticTaskError, parse_strict_json_object
from .cache import CacheStore, canonical_json, implementation_fingerprint
from .contracts import (Aggregate, Candidate, CandidateSet, EntityRef, RankedAnswer,
                        ScoredCandidate, ScoredCandidates)
from .dag import DAGValidationError, VerifiedDAG, verify_dag
from .executor import (ExecutionFailure, NodeContext, SerialExecutor,
                       execution_result_to_dict)
from .m4_data import (M4QueryBudget, bank_m4_analysis_weights,
                      generated_sandbox_limits, make_m4_registry)
from .m4_generated import (generated_contract_fields_guidance,
                           register_generated_specs, validate_generated_spec)
from .m4_composite import (CompositeSpecError, composite_spec_schema,
                           normalize_composite_spec, trusted_aggregate_semantics)
from .m4_capabilities import (InputCapabilityError, check_typed_input_capabilities,
                              input_producer_capability_schema)
from .m4_library import (LibrarySnapshot, admit_after_accepted,
                         assert_library_registry_compatible,
                         canonicalize_playbook_graph, generated_specs_from_snapshot,
                         playbook_graph_hash, _playbook_operator_keys, record_library_invocation,
                         snapshot_library, stage3_authoring_profile)
from .model_client import LocalModelClient
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec
from .runtime import RuntimeContext, build_case_runtime


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_M4_POLICY = ROOT / "config/m4_runtime.json"
MAX_CASE_DIGEST_BYTES = 32 * 1024
MAX_DIGEST_SERIES = 8
MAX_PROMPT_HISTORY = 3
_SIGNATURE_DISPLAY_DEFAULTS = {
    "operator": {"external_input": False},
    "input": {"required": True, "variadic": False, "minimum_items": 1,
              "maximum_items": None},
    "parameter": {"required": False, "nullable": False, "has_default": False,
                  "default": None, "choices": [], "runtime_field": None,
                  "estimator": None, "description": ""},
}
PAPER_STAGE2_RESPONSE_SCHEMA = "e4-paper-stage2-adaptation/1"
PAPER_ADAPTATION_RECORD_SCHEMA = "e4-paper-adaptation-record/1"
_PAPER_V2_AUTHORING_EXAMPLE = {
    "name": "evolved.window_summary_example", "version": "v1",
    "description": "Illustrates complete V2 references, required parameters and trusted evidence assembly.",
    "family": "metric", "output_type": "ScoredCandidates", "parameters": {},
    "authoring_profile": "typed-operator-ir-v2",
    "program": {"schema_version": "e4-typed-operator-ir/2", "nodes": [
        {"id": "components", "kernel": "series.component_metrics", "version": "1",
            "inputs": {"reference": "$input.reference", "alert": "$input.alert",
                "candidates": "$input.candidates"}, "params": {}, "bindings": {}},
        {"id": "reduced", "kernel": "series.reduce", "version": "1",
            "inputs": {"series": "components"}, "params": {"statistic": "median"},
            "bindings": {"block_minutes": "runtime.block_minutes",
                "minimum_blocks": "runtime.minimum_independent_blocks"}},
        {"id": "stat", "kernel": "numbers.window_compare", "version": "1",
            "inputs": {"values": "reduced"}, "params": {"operation": "relative_change"},
            "bindings": {}},
        {"id": "scores", "kernel": "numbers.reduce_component", "version": "1",
            "inputs": {"values": "stat"}, "params": {"method": "max"}, "bindings": {}},
        {"id": "evidence", "kernel": "evidence.from_scores", "version": "1",
            "inputs": {"scores": "scores"}, "params": {}, "bindings": {}},
        {"id": "result", "kernel": "scores.assemble", "version": "1",
            "inputs": {"candidates": "$input.candidates", "scores": "scores",
                "evidence": "evidence"}, "params": {}, "bindings": {}}],
        "return": {"node": "result"}}}
_GAP_CAPABILITY_FIELDS = (
    "operator_name", "operator_version", "gap_hypothesis", "computational_difference",
    "intended_contribution", "coverage_requirement", "validation_expectation",
    "closest_existing_operators", "required_inputs")
M4_SYSTEM_PROMPT = (
    "Analyze the supplied public Bank telemetry using only the registered E4 contracts. "
    "Return only the requested JSON artifact. You have no external tools or private-label access."
)
M4_EVOLUTION_SYSTEM_PROMPTS = {
    "compose": M4_SYSTEM_PROMPT + "\n\nStage 2 composition: diagnose the actual incident and retain up to five leads; success requires the fault component in the first three. Distinguish faults from symptoms. Inspect registered descriptions and prior analyses. Compose a distinct analysis from registered capabilities only; no generated source in this stage. Native public_query rows_data are plain CSV dictionaries. Preserve typed outputs and compute evidence eligibility explicitly. Scorers in one analysis family combine by taking the maximum raw score per component before family-level normalization, so additional evidence may leave the combined score and ordering unchanged.",
    "evolve": M4_SYSTEM_PROMPT + "\n\nStage 3 evolution authors a genuinely new typed numerical analysis in the private typed-operator-ir-v1 language. Do not diagnose or return a serving DAG, runtime overrides, predicted answer, or ranking. Ground every proposal in a specific failure or limitation visible in prior analysis: state what statistic/relationship is missing and how the new computation differs from prior computations. Return exactly a structured gap record and up to three new atomic operators. The language composes only the private fixed low-level typed kernels provided in this evolve prompt. It cannot call registered high-level operators or macros, add primitives, change source data, select case/entity IDs, or author evidence/eligibility. Every operator uses authoring_profile='typed-operator-ir-v1'; provide the exact typed signature and program. No Python, source strings, expressions, imports, or arbitrary data operations. Inspect prior logical and expanded plans and prior IR bodies. Do not resubmit the same canonical IR body under a new name/version. If no useful extension addresses a specific observed limitation, return exactly capabilities:[] and operators:[] rather than a duplicate. Proposed name/version pairs map one-to-one to gap records.",
    "stage3_serve": M4_SYSTEM_PROMPT + "\n\nStage 3 serving phase: diagnose the actual incident and retain up to five leads; success requires the fault component in the first three. Distinguish faults from symptoms. Use the validated gap record and registered operator signatures. Use every proposed typed IR operator as an ordinary atomic node in the serving DAG; do not reauthor or expand its private kernel body. Existing stored macros may remain available by their registered signatures. Do not revise the gap record or define more operators. Correlation returns signed rho in [-1,1], and only strictly positive final component scores can emit ranked evidence; minimum support does not establish significance or causality. Reference-median centering does not make association a change detector. Scorers in one analysis family combine by maximum raw score per component before family-level normalization, so added evidence can leave scores and rankings unchanged. Do not rename families, inflate scores, or force a different ranking. A failed diagnosis does not prove every analysis is insufficient. A bounded query page no-match is not full-window absence; paginate within published budgets or report the coverage limitation/abstain. Preserve strict answer evidence eligibility, support, and source attribution.",
}
PAPER_M4_SYSTEM_PROMPTS = {
    "compose": M4_SYSTEM_PROMPT + "\n\nPaper-starter profile: compose a typed DAG from the registered active capabilities. Normalize eligible nonnegative scores within each scorer before combining; exact host-validated computation/data fingerprints count once; combine by a trusted semantic group using its bounded explicit weight. Do not trust model-authored family labels to create a group. Generated IR scorers share one capped generated metric group.",
    # Evolution uses its own phase instructions in the system role as well as
    # the user prompt. Keep this phase-specific prompt for paper runs; the
    # generic base prompt is too weak to communicate the gap/spec-only output.
    "evolve": M4_SYSTEM_PROMPT + "\n\nPaper Stage 3 evolution uses `typed-operator-ir-v2`: author only genuinely new atomic operators and a capability gap. Do not diagnose or output a serving DAG, runtime overrides, answer, or ranking. Use only the closed V2 kernel catalog and trusted capability report; host derives fixed input ports and requirements. `starter_input` is required only for reachable log kernels. V1 bodies are provided for novelty comparison/replay, but new proposals must use V2. No Python, source strings, imports, unsupported fields, case/entity literals, labels, or invented thresholds. Ground the gap in a public limitation and explain the distinct supported statistic/source/window. If no useful extension exists, return an empty proposal with capability/operator arrays; never rename or duplicate a prior body.",
    "stage3_serve": M4_SYSTEM_PROMPT + "\n\nServe: use active typed registry and validated gap; preserve answer/evidence contracts. Normalize each scorer's eligible nonnegative scores by maximum; deduplicate exact validated computation/data fingerprints; average per trusted semantic group with bounded weights. All generated IR scorers share one capped group; host-pinned program digests identify validated computations; family labels add no votes. Association is noncausal; unsupported stays unknown.",
}
PAPER_CODE_M4_SYSTEM_PROMPTS = {
    "compose": PAPER_M4_SYSTEM_PROMPTS["compose"],
    "evolve": M4_SYSTEM_PROMPT + "\n\nPaper Stage 3 EVOLVE authors new Python operators using typed-operator-code-v1. Propose genuine source functions over declared typed inputs and the bounded neutral-data API, with new typed signatures/parameters/output and per-port data requirements. Do not return a serving DAG, diagnosis, ranking, or answer. Do not restrict work to prewritten kernels or high-level operator compositions. Ground the gap in an observed prior limitation and cite exact plan references. The host checks typing, source policy, novelty structure, and actual upstream data capabilities. A canonical AST duplicate check is structural only, not a proof of mathematical novelty. Every new scorer is host-pinned to its source/computation and shares one capped generated-analysis consensus group; family labels cannot create votes.",
    "stage3_serve": M4_SYSTEM_PROMPT + "\n\nPaper Stage 3 SERVE composes only registered operators into a typed DAG. Newly authored Python operators are validated registered atoms; never author source or add high-level composites here. Preserve trusted evidence/support/source semantics. Normalize, deduplicate and combine using host-stamped computation identities and the single capped generated-analysis group.",
}
_ACTIVE_M4_SYSTEM_PROMPT: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "active_m4_system_prompt", default=None)


class M4ControllerError(RuntimeError):
    def __init__(self, code: str, message: str, *, static: bool = False,
                 details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code, self.static = code, static
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        value = {"code": self.code, "message": str(self), "static": self.static}
        if self.details:
            value["details"] = self.details
        return value


class M4ModelClient(LocalModelClient):
    """Use M4-specific system wording without changing the frozen model payload."""

    def __init__(self, profile: Mapping[str, Any], *, base_url: str | None = None,
                 request_timeout: float | None = None, context_tokens: int | None = None,
                 harness_profile: str = "legacy-m4"):
        super().__init__(profile, base_url=base_url, request_timeout=request_timeout,
                         context_tokens=context_tokens)
        self.harness_profile = harness_profile

    def make_payload(self, prompt: str, *, max_tokens: int | None = None) -> dict[str, Any]:
        payload = super().make_payload(prompt, max_tokens=max_tokens)
        payload["messages"][0]["content"] = _ACTIVE_M4_SYSTEM_PROMPT.get() or M4_SYSTEM_PROMPT
        return payload

    def complete_for_stage(self, stage: str, prompt: str, *, task_id: str,
                           attempt: int, max_tokens: int | None = None) -> Any:
        token = _ACTIVE_M4_SYSTEM_PROMPT.set(self.system_prompt_for_stage(
            stage, harness_profile=self.harness_profile))
        try:
            return super().complete(prompt, task_id=task_id, attempt=attempt,
                                     max_tokens=max_tokens)
        finally:
            _ACTIVE_M4_SYSTEM_PROMPT.reset(token)

    @staticmethod
    def system_prompt_for_stage(stage: str, *, harness_profile: str = "legacy-m4") -> str:
        if stage == "synthesize":
            stage = "stage3_serve"
        if harness_profile == "reconstructed-paper-starter-v1-typed-operator-code-v1":
            return PAPER_CODE_M4_SYSTEM_PROMPTS.get(stage, M4_SYSTEM_PROMPT)
        if harness_profile == "reconstructed-paper-starter-v1":
            return PAPER_M4_SYSTEM_PROMPTS.get(stage, M4_SYSTEM_PROMPT)
        return M4_EVOLUTION_SYSTEM_PROMPTS.get(stage, M4_SYSTEM_PROMPT)

    def payload_for_stage(self, stage: str, prompt: str) -> dict[str, Any]:
        token = _ACTIVE_M4_SYSTEM_PROMPT.set(self.system_prompt_for_stage(
            stage, harness_profile=self.harness_profile))
        try:
            return self.make_payload(prompt)
        finally:
            _ACTIVE_M4_SYSTEM_PROMPT.reset(token)


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _compact_registered_signatures(signatures: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Omit only catalog metadata equal to the accompanying explicit defaults."""
    operators = json.loads(json.dumps([_plain(row) for row in signatures],
                                      ensure_ascii=False, allow_nan=False))
    for operator in operators:
        for key, default in _SIGNATURE_DISPLAY_DEFAULTS["operator"].items():
            if operator.get(key) == default:
                operator.pop(key)
        for port in operator.get("inputs", []):
            for key, default in _SIGNATURE_DISPLAY_DEFAULTS["input"].items():
                if port.get(key) == default:
                    port.pop(key)
        for parameter in operator.get("parameters", {}).values():
            for key, default in _SIGNATURE_DISPLAY_DEFAULTS["parameter"].items():
                if parameter.get(key) == default:
                    parameter.pop(key)
    return {"schema": "e4-registered-signatures-defaults/1",
        "omitted_field_rule": "An omitted operator, input-port, or parameter field is exactly the corresponding value in defaults.",
        "defaults": _plain(_SIGNATURE_DISPLAY_DEFAULTS), "operators": operators}


def _paper_registered_signature_catalog(signatures: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Losslessly intern repeated port, parameter, and requirement contracts."""
    compact = _compact_registered_signatures(signatures)
    input_by_json: dict[str, str] = {}
    parameter_by_json: dict[str, str] = {}
    requirement_by_json: dict[str, str] = {}
    input_contracts: dict[str, Any] = {}
    parameter_contracts: dict[str, Any] = {}
    requirement_contracts: dict[str, Any] = {}

    def intern(value: Any, key_to_ref: dict[str, str], table: dict[str, Any], prefix: str) -> str:
        encoded = json.dumps(_plain(value), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False)
        ref = key_to_ref.get(encoded)
        if ref is None:
            ref = f"{prefix}{len(key_to_ref) + 1}"
            key_to_ref[encoded] = ref
            table[ref] = json.loads(encoded)
        return ref

    operators = []
    for raw in compact["operators"]:
        row = json.loads(json.dumps(raw, ensure_ascii=False, allow_nan=False))
        ports = row.pop("inputs", [])
        parameters = row.pop("parameters", {})
        requirements = row.pop("input_requirements", None)
        row["inputs"] = [{"name": port["name"], "contract_ref": intern(
            {key: value for key, value in port.items() if key != "name"},
            input_by_json, input_contracts, "i")}
            for port in ports]
        row["parameters"] = {name: intern(value, parameter_by_json,
            parameter_contracts, "p")
            for name, value in parameters.items()}
        if isinstance(requirements, Mapping):
            row["input_requirements_ref"] = intern(requirements,
                requirement_by_json, requirement_contracts, "r")
        operators.append(row)
    return {"schema": "e4-paper-signature-contract-refs/2",
        "omitted_field_rule": compact["omitted_field_rule"],
        "defaults": compact["defaults"],
        "contract_reference_rule": "Each input contract_ref resolves in input_contracts; each parameter value resolves in parameter_contracts; each input_requirements_ref resolves in input_requirement_contracts. Operator input names and parameter names remain exact. Defaults apply only as stated in defaults.",
        "input_contracts": input_contracts,
        "parameter_contracts": parameter_contracts,
        "input_requirement_contracts": requirement_contracts,
        "operators": operators}


def _expand_paper_registered_signature_catalog(catalog: Mapping[str, Any]) -> dict[str, Any]:
    """Restore the exact compact signature form from the prompt's contract refs."""
    if (catalog.get("schema") != "e4-paper-signature-contract-refs/2" or
            catalog.get("defaults") != _SIGNATURE_DISPLAY_DEFAULTS or
            not isinstance(catalog.get("operators"), list) or
            not isinstance(catalog.get("input_contracts"), Mapping) or
            not isinstance(catalog.get("parameter_contracts"), Mapping) or
            not isinstance(catalog.get("input_requirement_contracts"), Mapping)):
        raise ValueError("unsupported paper signature reference catalog")
    operators = json.loads(json.dumps(catalog["operators"], ensure_ascii=False, allow_nan=False))
    for operator in operators:
        input_rows = operator.get("inputs", [])
        expanded_inputs = []
        for row in input_rows:
            if (not isinstance(row, Mapping) or not isinstance(row.get("name"), str) or
                    not isinstance(row.get("contract_ref"), str) or
                    row["contract_ref"] not in catalog["input_contracts"]):
                raise ValueError("malformed input contract reference")
            expanded_inputs.append({"name": row["name"],
                **_plain(catalog["input_contracts"][row["contract_ref"]])})
        operator["inputs"] = expanded_inputs
        requirements_ref = operator.pop("input_requirements_ref", None)
        if requirements_ref is not None:
            if (not isinstance(requirements_ref, str) or
                    requirements_ref not in catalog["input_requirement_contracts"]):
                raise ValueError("malformed input-requirements contract reference")
            operator["input_requirements"] = _plain(
                catalog["input_requirement_contracts"][requirements_ref])
        expanded_parameters = {}
        for name, ref in operator.get("parameters", {}).items():
            if not isinstance(ref, str) or ref not in catalog["parameter_contracts"]:
                raise ValueError("malformed parameter contract reference")
            expanded_parameters[name] = _plain(catalog["parameter_contracts"][ref])
        operator["parameters"] = expanded_parameters
    return {"schema": "e4-registered-signatures-defaults/1",
        "omitted_field_rule": catalog["omitted_field_rule"],
        "defaults": _plain(catalog["defaults"]), "operators": operators}


def _front_code_paper_metric_fields(digest: Mapping[str, Any]) -> dict[str, Any]:
    """Losslessly front-code the long ordered public metric-name list."""
    result = json.loads(json.dumps(_plain(digest), ensure_ascii=False, allow_nan=False))
    fields = result.get("available_metric_fields")
    if not isinstance(fields, list) or any(not isinstance(value, str) for value in fields):
        return result
    encoded = []
    previous = ""
    for value in fields:
        prefix = 0
        max_prefix = min(len(previous), len(value))
        while prefix < max_prefix and previous[prefix] == value[prefix]:
            prefix += 1
        encoded.append([prefix, value[prefix:]])
        previous = value
    result["available_metric_fields"] = {
        "encoding": "e4-front-codepoint-prefix/1",
        "values": encoded,
    }
    return result


def _expand_front_coded_paper_metric_fields(value: Any) -> list[str]:
    if (not isinstance(value, Mapping) or
            value.get("encoding") != "e4-front-codepoint-prefix/1" or
            not isinstance(value.get("values"), list)):
        raise ValueError("unsupported front-coded metric field list")
    output = []
    previous = ""
    for row in value["values"]:
        if (not isinstance(row, list) or len(row) != 2 or type(row[0]) is not int or
                row[0] < 0 or row[0] > len(previous) or not isinstance(row[1], str)):
            raise ValueError("malformed front-coded metric field entry")
        previous = previous[:row[0]] + row[1]
        output.append(previous)
    return output


def _paper_evolve_operator_catalog(signatures: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Display only registered details relevant to IR evolution.

    Operator identities, versions, outputs, families, descriptions, and signed
    input capability requirements remain available. DAG port/parameter detail
    is omitted because this phase authors typed IR kernels, not a serving DAG;
    the full registry remains available to the serving phase and validator.
    """
    operators = []
    input_requirement_by_json: dict[str, str] = {}
    input_requirement_contracts: dict[str, Any] = {}
    for row in signatures:
        display = {key: _plain(row[key]) for key in (
            "name", "version", "output_type", "family", "description", "external_input")
            if key in row}
        if isinstance(row.get("input_requirements"), Mapping):
            encoded = json.dumps(_plain(row["input_requirements"]), ensure_ascii=False,
                sort_keys=True, separators=(",", ":"), allow_nan=False)
            ref = input_requirement_by_json.get(encoded)
            if ref is None:
                ref = f"r{len(input_requirement_by_json) + 1}"
                input_requirement_by_json[encoded] = ref
                input_requirement_contracts[ref] = json.loads(encoded)
            display["input_requirements_ref"] = ref
        display["omitted_serving_dag_fields"] = [field for field in ("inputs", "parameters")
            if field in row]
        operators.append(display)
    return {"schema": "e4-paper-evolve-operator-catalog/2",
        "purpose": "IR evolution creates typed numerical operators, not a serving DAG; full input-port and parameter contracts remain registered and are supplied during compose/serve.",
        "input_requirement_reference_rule": "Resolve each input_requirements_ref in input_requirement_contracts; requirements are exact signed contracts.",
        "input_requirement_contracts": input_requirement_contracts,
        "operators": operators}


def _paper_compose_port_contracts(signatures: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Repeat only frequent paper scorer ports as readable named contracts."""
    wanted = {"paper.backend.mean_welch", "paper.backend.tail_permutation",
        "paper.bank.log_pattern_contrast", "bank.metrics.contrast",
        "bank.metrics.aggregate", "bank.traces.aggregate"}
    result = []
    for row in signatures:
        if row.get("name") not in wanted:
            continue
        required_parameters = {}
        for name, parameter in row.get("parameters", {}).items():
            if parameter.get("required") and not parameter.get("has_default"):
                required_parameters[name] = parameter.get("choices", parameter.get("type"))
        contract = {"name": row["name"], "inputs": {
            port["name"]: port["type"] for port in row.get("inputs", [])}}
        if required_parameters:
            contract["required_params"] = required_parameters
        result.append(contract)
    return result


def _compact_paper_prior_ir_bodies(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Losslessly encode prior IR bodies with shared object field names once."""
    operator_fields = ["name", "version", "family", "authoring_profile", "program_sha256"]
    program_fields = ["schema_version", "nodes", "return"]
    node_fields = ["id", "kernel", "version", "inputs", "params", "bindings"]
    encoded_rows = []
    for index, row in enumerate(rows):
        if (not isinstance(row, Mapping) or any(key not in row for key in operator_fields) or
                not isinstance(row.get("program"), Mapping)):
            raise M4ControllerError("paper_prior_ir_schema",
                f"prior IR body {index} has missing typed metadata", static=True)
        program = row["program"]
        if set(program) != set(program_fields) or not isinstance(program.get("nodes"), list):
            raise M4ControllerError("paper_prior_ir_schema",
                f"prior IR body {index} has an unsupported program shape", static=True)
        nodes = []
        for node_index, node in enumerate(program["nodes"]):
            if not isinstance(node, Mapping) or set(node) != set(node_fields):
                raise M4ControllerError("paper_prior_ir_schema",
                    f"prior IR body {index} node {node_index} has an unsupported field set", static=True)
            nodes.append([_plain(node[key]) for key in node_fields])
        if (not isinstance(program.get("return"), Mapping) or
                set(program["return"]) != {"node"}):
            raise M4ControllerError("paper_prior_ir_schema",
                f"prior IR body {index} return shape is invalid", static=True)
        metadata = [_plain(row[key]) for key in operator_fields]
        compact_program = [program["schema_version"], nodes, _plain(program["return"])]
        encoded_rows.append([*metadata, compact_program])
    return {"schema_version": "e4-paper-prior-ir-columnar/1",
        "operator_row_fields": operator_fields, "program_field_order": program_fields,
        "node_field_order": node_fields,
        "decode_rule": "Each operator row has operator_row_fields followed by program=[schema_version,node_rows,return]. Each node row follows node_field_order; nested inputs, params, and bindings are unchanged JSON objects. No operator body, node, parameter, binding, or return is omitted.",
        "operators": encoded_rows}


def _expand_paper_prior_ir_bodies(compact: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Restore the prior IR body list from its lossless prompt columnar form."""
    if (compact.get("schema_version") != "e4-paper-prior-ir-columnar/1" or
            compact.get("operator_row_fields") != ["name", "version", "family",
                "authoring_profile", "program_sha256"] or
            compact.get("program_field_order") != ["schema_version", "nodes", "return"] or
            compact.get("node_field_order") != ["id", "kernel", "version", "inputs", "params", "bindings"] or
            not isinstance(compact.get("operators"), list)):
        raise ValueError("unsupported columnar prior IR schema")
    operators = []
    for row in compact["operators"]:
        if not isinstance(row, list) or len(row) != 6 or not isinstance(row[5], list) or len(row[5]) != 3:
            raise ValueError("malformed columnar prior IR operator row")
        program_schema, node_rows, return_value = row[5]
        if not isinstance(node_rows, list):
            raise ValueError("malformed columnar prior IR node rows")
        nodes = []
        for node in node_rows:
            if not isinstance(node, list) or len(node) != 6:
                raise ValueError("malformed columnar prior IR node row")
            nodes.append(dict(zip(compact["node_field_order"], _plain(node))))
        operators.append({**dict(zip(compact["operator_row_fields"], _plain(row[:5]))),
            "program": {"schema_version": program_schema, "nodes": nodes,
                "return": _plain(return_value)}})
    return operators


def _intern_paper_ir_metric_choices(schema: Mapping[str, Any]) -> dict[str, Any]:
    """Replace repeated private-kernel selector enums with their one shared catalog."""
    result = json.loads(json.dumps(_plain(schema), ensure_ascii=False, allow_nan=False))
    kernels = result.get("kernels", {})
    for identity, parameter, catalog_ref in (
            ("series.select_field@1", "source_field", "paper.bank_component_metric_fields"),
            ("series.select_metric_rule@1", "selector_ref", "metric_selector_catalog")):
        selector = kernels.get(identity, {}) if isinstance(kernels, Mapping) else {}
        descriptor = selector.get("params", {}).get(parameter) if isinstance(selector, Mapping) else None
        if isinstance(descriptor, dict) and "choices" in descriptor:
            descriptor.pop("choices", None)
            descriptor["choices_ref"] = catalog_ref
    if "approved_component_metric_fields" in result:
        result["approved_component_metric_fields"] = {
            "choices_ref": "paper.bank_component_metric_fields",
            "meaning": "same exact frozen component metric allowlist as this paper profile's parameter catalog"}
    component = result.get("component_metrics")
    if isinstance(component, dict) and "approved_source_fields" in component:
        component["approved_source_fields"] = {
            "choices_ref": "paper.bank_component_metric_fields",
            "meaning": "same exact frozen component metric allowlist as the parameter catalog"}
    return result


def _intern_paper_ir_runtime_context(context: Mapping[str, Any]) -> dict[str, Any]:
    """Point V2's trusted selector lists at their single plain parameter catalog."""
    from .paper_starter import paper_parameter_catalog
    result = json.loads(json.dumps(_plain(context), ensure_ascii=False, allow_nan=False))
    enums = paper_parameter_catalog()["enums"]
    for name, ref in (("component_metric_allowlist", "paper.bank_component_metric_fields"),
                      ("primary_metric_allowlist", "paper.app_kpi_fields")):
        values = result.get(name)
        if isinstance(values, list) and values == enums.get(ref, {}).get("values"):
            result[name] = {"choices_ref": ref,
                "meaning": "same exact enum as in the shared paper parameter catalog"}
    return result


def _paper_evolve_typed_input_capabilities(registry: OperatorRegistry) -> dict[str, Any]:
    """Avoid a second full exact metric enum in the IR-authoring phase."""
    result = _typed_ir_input_capabilities(registry)
    component = result.get("component_metrics")
    if isinstance(component, dict) and "approved_source_fields" in component:
        component["approved_source_fields"] = {
            "selector_catalog_ref": "metric_selector_catalog",
            "meaning": "V2 field selection uses only selector_ref values from the private IR schema catalog; the host resolves exact observed fields.",
            "source_table": component.get("table")}
    return result


def _paper_runtime_prompt_values(runtime: RuntimeContext) -> dict[str, Any]:
    """Keep exact runtime values and replace per-field provenance with one digest."""
    snapshot = runtime.as_dict()
    provenance = snapshot.get("provenance", {})
    return {"schema_version": snapshot.get("schema_version"),
        "values": _plain(snapshot.get("values", {})),
        "provenance_map_sha256": _canonical_hash(provenance),
        "provenance_reference": "Full runtime field provenance is retained in the host audit/runtime snapshot."}


def _paper_compact_case_digest_for_prompt(digest: Mapping[str, Any], *, stage: str) -> dict[str, Any]:
    """Bound descriptive host metadata while preserving its aggregate digests."""
    from .m4_prompt_digest import compact_paper_case_digest
    projected = compact_paper_case_digest(digest, stage=stage)
    kpi_context = projected.get("primary_app_kpi_context")
    if isinstance(kpi_context, dict) and "capability_summary_ref" not in kpi_context:
        # Selection rationale/provenance is recoverable from the pinned policy
        # hash; retain the selected opaque ID and all support/numeric summaries.
        kpi_context.pop("selection_reason", None)
        kpi_context.pop("source_policy", None)
        kpi_context["interpretation"] = "field-name working interpretation only"
        if "policy_sha256" in kpi_context:
            kpi_context["selection_policy_ref"] = "primary_app_kpi_policy_sha256"
    inventory = projected.get("available_metric_fields")
    if isinstance(inventory, dict) and inventory.get("status") in {"available", "available_empty"}:
        count = inventory.get("observed_unique_field_count")
        if type(count) is int and count >= 0:
            inventory["examples"] = []
            inventory["represented_field_count"] = 0
            inventory["omitted_field_count"] = count
            inventory["coverage_fraction"] = 1.0 if count == 0 else 0.0
            inventory["omission_reason"] = (
                "No descriptive examples are included. Exact legal field choices, stable family refs, and their case support appear in trusted selector catalogs.")
    file_hashes = projected.pop("file_sha256", None)
    file_bytes = projected.pop("file_bytes", None)
    if isinstance(file_hashes, Mapping) or isinstance(file_bytes, Mapping):
        hashes = _plain(file_hashes) if isinstance(file_hashes, Mapping) else {}
        byte_counts = _plain(file_bytes) if isinstance(file_bytes, Mapping) else {}
        projected["source_file_inventory"] = {
            "file_hash_count": len(hashes),
            "file_hashes_sha256": _canonical_hash(dict(sorted(hashes.items()))),
            "file_bytes_entry_count": len(byte_counts),
            "file_byte_counts_sha256": _canonical_hash(dict(sorted(byte_counts.items()))),
            "total_source_bytes": sum(value for value in byte_counts.values()
                if type(value) is int and value >= 0),
            "per_file_maps_omitted": True,
            "host_manifest_reference": "source_hashes and source sizes remain in the trusted host audit manifest"}
    return projected


def _paper_parameter_catalog_for_prompt(policy: Mapping[str, Any]) -> dict[str, Any]:
    """Attach exact case support to prompt-only selector enum entries.

    The emitted parameter remains the exact string in each entry's ``value``;
    case support lives in a deduplicated profile table and never changes the
    immutable host-side ParameterSpec choices.
    """
    from .m4_prompt_support import intern_case_support_profiles
    from .paper_starter import paper_parameter_catalog

    catalog = json.loads(json.dumps(paper_parameter_catalog(), ensure_ascii=False,
        sort_keys=True, allow_nan=False))
    report = policy.get("_paper_ir_capability_report")
    if not isinstance(report, Mapping):
        return catalog
    selector_rows = report.get("metric_selector_catalog", [])
    resolution_rows = report.get("metric_selector_resolutions", [])
    if not isinstance(selector_rows, list) or not isinstance(resolution_rows, list):
        return catalog
    selector_by_ref = {row["selector_ref"]: row for row in selector_rows
        if isinstance(row, Mapping) and isinstance(row.get("selector_ref"), str)}
    resolution_by_ref = {row["selector_ref"]: row for row in resolution_rows
        if isinstance(row, Mapping) and isinstance(row.get("selector_ref"), str)}
    summary = _paper_v2_capability_prompt_summary(report)
    summary_profiles = summary.get("selector_semantic_profiles", {})
    summary_statuses = summary.get("selector_status_by_ref", {})
    field_support: dict[str, dict[str, Any]] = {}
    rule_refs: dict[str, Mapping[str, Any]] = {}
    for selector_ref, selector in selector_by_ref.items():
        if selector.get("source_table") != "bank/metric_container.csv":
            continue
        resolution = resolution_by_ref.get(selector_ref, {})
        if selector.get("selector_kind") == "pattern":
            rule_refs[selector_ref] = resolution
        for match in resolution.get("matches", []) if isinstance(resolution, Mapping) else []:
            if not isinstance(match, Mapping) or not isinstance(match.get("field_name"), str):
                continue
            status_row = summary_statuses.get(selector_ref)
            profile_refs = (status_row[2] if isinstance(status_row, list) and len(status_row) == 5 else [])
            field_support[match["field_name"]] = {"status": "available",
                "semantic_profile_refs": list(profile_refs),
                "window_support": _plain(match.get("window_support", {}))}

    enums = catalog.get("enums", {})
    support_catalogs: dict[str, Any] = {}

    exact_enum = enums.get("paper.bank_component_metric_fields")
    if isinstance(exact_enum, dict) and isinstance(exact_enum.get("values"), list):
        entries = []
        for value in exact_enum["values"]:
            if value == "":
                support = {"status": "available" if field_support else "unavailable",
                    "selection_semantics": "Analyze each supported allowlisted component metric separately."}
            else:
                support = field_support.get(value, {"status": "unavailable",
                    "reason": "no exact observed rankable field match in this case"})
            entries.append({"value": value, "case_support": support})
        projected, profile_catalog = intern_case_support_profiles(entries,
            ref_key="value", support_key="case_support")
        exact_enum["values"] = projected
        exact_enum["value_field"] = "value"
        exact_enum["parameter_value_type"] = "string"
        exact_enum["case_support_profile_catalog_ref"] = "case_support_profiles.paper.bank_component_metric_fields"
        support_catalogs["paper.bank_component_metric_fields"] = {
            "schema_version": profile_catalog["schema_version"],
            "support_profile_ref_field": "support_profile_ref",
            "profile_payload_field": "case_support",
            "row_order_ref": "paper.bank_component_metric_fields.values",
            "profiles": profile_catalog["profiles"]}

    rule_enum = enums.get("paper.bank_component_metric_rule_refs")
    if isinstance(rule_enum, dict) and isinstance(rule_enum.get("values"), list):
        descriptions = rule_enum.pop("descriptions", {})
        entries = []
        for value in rule_enum["values"]:
            if value == "":
                support = {"status": "available" if field_support else "unavailable",
                    "match_count": len(field_support),
                    "selection_semantics": "Empty means keep the exact-field selection behavior."}
                description = rule_enum.get("empty_value_semantics", "empty selector")
            else:
                row = rule_refs.get(value)
                status_row = summary_statuses.get(value)
                if row is None:
                    support = {"status": "unavailable", "match_count": 0,
                        "semantic_profiles": [], "support_histograms": {}}
                elif isinstance(status_row, list) and len(status_row) == 5:
                    support = {"status": status_row[0], "match_count": status_row[1],
                        "semantic_profile_refs": list(status_row[2]),
                        "reference_support_histogram": _plain(status_row[3]),
                        "alert_support_histogram": _plain(status_row[4])}
                else:
                    support = {"status": row.get("status", "unavailable"),
                        "match_count": row.get("match_count", 0)}
                description = descriptions.get(value, value)
            entries.append({"value": value, "description": description,
                "case_support": support})
        projected, profile_catalog = intern_case_support_profiles(entries,
            ref_key="value", support_key="case_support")
        rule_enum["values"] = projected
        rule_enum["value_field"] = "value"
        rule_enum["parameter_value_type"] = "string"
        rule_enum["case_support_profile_catalog_ref"] = "case_support_profiles.paper.bank_component_metric_rule_refs"
        support_catalogs["paper.bank_component_metric_rule_refs"] = {
            "schema_version": profile_catalog["schema_version"],
            "support_profile_ref_field": "support_profile_ref",
            "profile_payload_field": "case_support",
            "row_order_ref": "paper.bank_component_metric_rule_refs.values",
            "profiles": profile_catalog["profiles"]}

    if support_catalogs:
        catalog["case_support_profiles"] = support_catalogs
        catalog["case_semantic_profiles"] = _paper_semantic_profile_prompt_table(summary_profiles)
        catalog["prompt_value_semantics"] = (
            "For object entries in selector enums, emit only the exact string in the declared value_field. "
            "A case_support_profile_ref resolves in case_support_profiles; semantic_profile_refs resolve in "
            "case_semantic_profiles. Reconstruct a semantic profile from defaults plus overrides, then keep exactly present_fields. "
            "These support references are descriptive and never submitted as parameters.")
        catalog["case_support_provenance"] = {key: report.get("provenance", {}).get(key)
            for key in ("bank_metric_policy_sha256", "telemetry_data_sha256",
                "telemetry_source_hashes_sha256")
            if isinstance(report.get("provenance", {}).get(key), str)}
        catalog["case_support_provenance"].update({
            "exact_field_match_count": summary.get("selector_field_match_count"),
            "unique_exact_field_count": summary.get("selector_unique_field_count"),
            "field_selector_mapping_sha256": summary.get("selector_field_mapping_sha256")})
    return catalog


def _paper_semantic_profile_prompt_table(profiles: Mapping[str, Any]) -> dict[str, Any]:
    """Losslessly factor repeated semantic labels out of per-case profile rows."""
    rows = {str(ref): _plain(value) for ref, value in profiles.items()
        if isinstance(ref, str) and isinstance(value, Mapping)}
    if not rows:
        return {"schema_version": "e4-paper-semantic-profile-table/1",
            "fields": [], "defaults": {}, "profiles": []}
    fields = sorted({key for value in rows.values() for key in value})
    defaults: dict[str, Any] = {}
    for key in fields:
        values = [value[key] for value in rows.values() if key in value]
        if len(values) == len(rows) and all(value == values[0] for value in values[1:]):
            defaults[key] = values[0]
    compressed = []
    for ref, value in sorted(rows.items()):
        compressed.append({"profile_ref": ref,
            "present_fields": sorted(value),
            "overrides": {key: value[key] for key in sorted(value)
                if key not in defaults or value[key] != defaults[key]}})
    return {"schema_version": "e4-paper-semantic-profile-table/1",
        "fields": fields, "defaults": defaults,
        "decode_rule": "For each row, start with defaults, apply overrides, then retain exactly present_fields. No semantic field is inferred from a selector name.",
        "profiles": compressed}


def _expand_paper_semantic_profile_prompt_table(table: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if table.get("schema_version") != "e4-paper-semantic-profile-table/1":
        raise ValueError("unsupported semantic profile prompt table")
    fields, defaults, rows = table.get("fields"), table.get("defaults"), table.get("profiles")
    if (not isinstance(fields, list) or not isinstance(defaults, Mapping) or
            not isinstance(rows, list)):
        raise ValueError("malformed semantic profile prompt table")
    output = {}
    for row in rows:
        if (not isinstance(row, Mapping) or not isinstance(row.get("profile_ref"), str) or
                not isinstance(row.get("present_fields"), list) or
                not isinstance(row.get("overrides"), Mapping)):
            raise ValueError("malformed semantic profile row")
        values = dict(defaults)
        values.update(_plain(row["overrides"]))
        if any(key not in fields for key in row["present_fields"]):
            raise ValueError("semantic profile row references an unknown field")
        output[row["profile_ref"]] = {key: values[key] for key in row["present_fields"]}
    return output


def _without_paper_raw_query_capability(schema: Mapping[str, Any]) -> dict[str, Any]:
    """Hide the legacy raw-row query producer from opt-in paper prompts."""
    value = json.loads(json.dumps(_plain(schema), ensure_ascii=False, allow_nan=False))
    producers = value.get("producers")
    if isinstance(producers, list):
        value["producers"] = [row for row in producers
            if not isinstance(row, Mapping) or row.get("name") != "bank.public.query"]
    elif isinstance(producers, Mapping):
        value["producers"] = {key: row for key, row in producers.items()
            if key != "bank.public.query"}
    signatures = value.get("registered_signatures")
    if isinstance(signatures, Mapping):
        value["registered_signatures"] = {key: rows for key, rows in signatures.items()
            if key != "bank.public.query"}
    value["disabled_producers"] = {"bank.public.query":
        "Raw-row pages are not a paper-starter profile capability."}
    return value


def _typed_ir_input_capabilities(registry: OperatorRegistry) -> dict[str, Any]:
    """Expose only the trusted full-window buffers consumed by typed IR."""
    from .m4_operator_ir import trusted_runtime_context

    signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
    producer = next((f"{name}@{version}" for name, version in signatures
        if name == "bank.metrics.aggregate"), None)
    trusted = trusted_runtime_context()
    return {"schema_version": "e4-typed-operator-ir-inputs/1",
        "producer": producer,
        "coverage": "reference and alert bank.metrics.aggregate inputs cover the complete adjacent windows",
        "row_requirements": {"statistic": "median", "support_unit": "timestamp",
            "component_details": ["bucket_samples", "source_field", "metric_kind",
                "ranking_eligible", "unit"],
            "metadata": ["source_hashes", "case_data_sha256"]},
        "component_metrics": {"table": "metric/metric_container.csv",
            "role": "component", "columns": ["timestamp", "cmdb_id", "kpi_name", "value"],
            "approved_source_fields": trusted["component_metric_allowlist"]},
        "primary_kpi": {"table": "metric/metric_app.csv", "role": "app_kpi",
            "columns": ["timestamp", "mrt", "tc"],
            "approved_source_fields": trusted["primary_metric_allowlist"],
            "minimum_timestamps_per_window": trusted["primary_kpi_minimum_support"],
            "identity": "tc is opaque and is never mapped to a component"},
        "pairing": "reference and alert must share exact source hashes and case_data_sha256; their windows are adjacent",
        "excluded_inputs": "A bank.public.query page is not full-window input: Aggregate.rows is empty and native rows live in metadata.public_query.rows_data. bank.public.project removes bucket_samples.",
        "support_note": "Rows from each selected source_field retain native units; distinct native fields cannot be combined unless the score is dimensionless."}


def _paper_prompt_input_capabilities(registry: OperatorRegistry,
        case_capabilities: Mapping[str, Any]) -> dict[str, Any]:
    """Deduplicate producer semantics already present in exact signatures/catalogs."""
    result = _typed_ir_input_capabilities(registry)
    component = result.get("component_metrics")
    if isinstance(component, dict):
        component["approved_source_fields"] = {
            "choices_ref": "paper.bank_component_metric_fields",
            "rule_choices_ref": "paper.bank_component_metric_rule_refs",
            "meaning": "exact strings and case support are declared in the shared paper parameter catalog"}
    result["prompt_projection"] = {
        "schema_version": "e4-paper-input-capabilities-prompt/1",
        "semantics": "Operator names, versions, port/parameter contracts, and input requirements are supplied once in REGISTERED OPERATOR SIGNATURES. This section retains the shared source-row, coverage, identity, and provenance contracts.",
        "query_capability": "unsupported in this paper profile; raw-row pages are not a full-window provider"}
    result["trace_span_input"] = {"table": "trace/trace_span.csv",
        "columns": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
        "duration_unit": "undeclared native unit",
        "root_rule": "only an explicit null parent_id is a root; unresolved parents, duplicate IDs, and cycles do not imply roots",
        "dependency_capability": case_capabilities.get("trace_dependencies", "unknown"),
        "root_duration_capability": case_capabilities.get("trace_root_duration", "unknown")}
    result["native_log_input"] = {"table": "log/log_service.csv",
        "columns": ["log_id", "timestamp", "cmdb_id", "log_name", "value"],
        "scan": "hash-pinned full-window local aggregation",
        "model_visible": "bounded normalized templates, counts, window denominators, support, and source digests",
        "raw_log_text": "not sent to the model",
        "capability": case_capabilities.get("native_log_patterns", "unknown")}
    return result


def _compact_evolution_case_digest(digest: Mapping[str, Any]) -> dict[str, Any]:
    """Keep case identity and coverage while omitting diagnosis-sized examples from evolve."""
    retained = {
        "case_id", "timezone", "reference_window_utc_ms", "alert_window_utc_ms",
        "source_timestamp_units_by_date", "exact_observed_component_ids",
        "available_metric_fields", "primary_app_kpi_context",
        "primary_app_kpi_policy_sha256", "metric_digest_selection_policy",
        "component_identity_coverage", "quality_counts", "log_summary",
        "digest_sha256", "digest_bytes", "metric_series_limit",
        "app_kpi_context_series_limit", "trace_series_limit", "log_volume_series_limit",
    }
    # Keep every producer-reported omission count and limit, including fields
    # introduced by later digest versions, while dropping the bulky examples.
    result = {key: _plain(value) for key, value in digest.items()
        if key in retained or key.endswith("_omitted") or key.endswith("_limit")}
    dropped = sorted(key for key in digest if key not in result)
    result["omitted_for_evolution"] = dropped
    result["evolution_digest_semantics"] = (
        "Raw case examples and detailed series are omitted because prior selection/compose attempts "
        "already examined this case. Author a deployment-generic analysis from prior failure evidence; "
        "the later serving phase receives the full current case digest.")
    return result


def _expand_registered_signatures(compact: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Losslessly restore `_compact_registered_signatures` output for tests/tools."""
    if (compact.get("schema") != "e4-registered-signatures-defaults/1" or
            compact.get("defaults") != _SIGNATURE_DISPLAY_DEFAULTS or
            not isinstance(compact.get("operators"), list)):
        raise ValueError("unsupported compact registered-signature schema")
    operators = json.loads(json.dumps(compact["operators"], ensure_ascii=False, allow_nan=False))
    for operator in operators:
        for key, default in _SIGNATURE_DISPLAY_DEFAULTS["operator"].items():
            operator.setdefault(key, _plain(default))
        for port in operator.get("inputs", []):
            for key, default in _SIGNATURE_DISPLAY_DEFAULTS["input"].items():
                port.setdefault(key, _plain(default))
        for parameter in operator.get("parameters", {}).values():
            for key, default in _SIGNATURE_DISPLAY_DEFAULTS["parameter"].items():
                parameter.setdefault(key, _plain(default))
    return operators


def _compact_runtime_history(history: list[Mapping[str, Any]],
                             current_values: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Encode prior runtime values as an exact delta from current allowlisted values."""
    rows = json.loads(json.dumps([_plain(row) for row in history[-MAX_PROMPT_HISTORY:]],
                                 ensure_ascii=False, allow_nan=False))
    for row in rows:
        for key in [key for key in row if key.startswith("_")]:
            row.pop(key, None)
    base = _plain(current_values)
    base_hash = _canonical_hash(base)
    for row in rows:
        old = row.pop("runtime_values", None)
        if isinstance(old, Mapping):
            delta = {key: value for key, value in old.items()
                     if key not in base or value != base[key]}
            removed = sorted(set(base) - set(old))
            row["runtime_values_delta"] = {"base_sha256": base_hash,
                "set": delta, "removed": removed}
    return rows


def _expand_runtime_values_delta(delta: Mapping[str, Any],
                                 current_values: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct prior values; reject applying a delta to a different current base."""
    base = _plain(current_values)
    if delta.get("base_sha256") != _canonical_hash(base):
        raise ValueError("runtime history delta base does not match current runtime")
    result = dict(base)
    for key in delta.get("removed", []):
        result.pop(key, None)
    result.update(_plain(delta.get("set", {})))
    return result


def _implementation_fingerprint() -> str:
    pinned = os.environ.get("E4_M4_IMPLEMENTATION_FINGERPRINT")
    if pinned is None:
        return implementation_fingerprint()
    if not re.fullmatch(r"[0-9a-f]{64}", pinned):
        raise M4ControllerError("source_fingerprint_env", "trusted source fingerprint must be a lowercase SHA-256")
    return pinned


def _write_json(path: Path, value: Any, *, exclusive: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "x" if exclusive else "w"
    with path.open(mode, encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True,
                                allow_nan=False) + "\n")


def _append_jsonl(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":"), allow_nan=False) + "\n")
        stream.flush()


def _policy(root: Path = ROOT, policy_path: Path | None = None) -> dict[str, Any]:
    path = policy_path or root / "config/m4_runtime.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") != "e4-m4-runtime-policy/1":
        raise M4ControllerError("m4_policy_version", "unsupported M4 runtime policy")
    serving = value.get("serving", {})
    protected = value.get("protected_runtime_values", {})
    query = value.get("query_and_generation", {})
    query_frozen = {"maximum_public_query_requests_per_case": 32,
        "maximum_public_query_rows_per_request": 256,
        "maximum_public_query_rows_per_case": 2048,
        "maximum_public_query_bytes_per_request": 512 * 1024,
        "maximum_public_query_bytes_per_case": 2 * 1024 * 1024,
        "maximum_public_query_string_bytes": 4096,
        "maximum_public_query_offset": 100_000,
        "maximum_source_bytes_per_query": 512 * 1024 * 1024,
        "maximum_projected_summary_rows_per_request": 256,
        "maximum_projected_summary_offset": 100_000,
        "maximum_projected_string_bytes": 4096,
        "maximum_projected_output_bytes": 512 * 1024,
        "maximum_operator_input_bytes": 16 * 1024 * 1024,
        "generated_operator_cpu_seconds": 10,
        "generated_operator_wall_seconds": 15,
        "generated_operator_memory_bytes": 512 * 1024 * 1024}
    if (serving.get("stages") != ["select", "compose", "synthesize"] or
            value.get("policy_name") != "bank-m4-controller-input16m-cpu10-wall15-v3" or
            serving.get("maximum_diagnostic_attempts") != 3 or
            serving.get("maximum_new_operators") != 3 or
            serving.get("static_repair_limit_per_attempt") != 2 or
            serving.get("evolution_protocol_version") != "e4-separated-evolve-serve/1" or
            serving.get("maximum_stage3_static_repairs") != 2 or
            serving.get("maximum_model_calls") != 10 or
            serving.get("maximum_aggregate_generated_tokens") != 73728 or
            serving.get("maximum_nodes") != 40 or
            serving.get("maximum_depth") != 12 or
            serving.get("maximum_leads") != 5 or serving.get("acceptance_top_k") != 3 or
            value.get("bootstrap", {}).get("static_repair_limit") != 2 or
            value.get("bootstrap", {}).get("maximum_new_operators") != 2 or
            value.get("bootstrap", {}).get("maximum_new_playbooks") != 4 or
            value.get("library", {}).get("total_operator_cap") != 19 or
            protected.get("max_nodes") != 40 or protected.get("dag_depth") != 12 or
            protected.get("lead_count") != 5 or protected.get("acceptance_top_k") != 3 or
            protected.get("seed") != 42 or protected.get("permutation_resamples") != 499 or
            any(query.get(key) != expected for key, expected in query_frozen.items())):
        raise M4ControllerError("m4_policy_frozen_values", "M4 stage or resource budgets differ from the authorized frozen policy")
    return value


def typed_contract_wire_schema() -> dict[str, Any]:
    """Exact cache-wire fields required by M4 generated typed outputs."""
    return {"envelope": "exact object {$type: contract_name, $fields: object with every listed field}",
        "nested_collections": {"tuple": "{'$tuple':[items...]}",
                               "mapping": "{'$map':{string_key: value,...}}"},
        "finite_json": "numbers must be finite; object details contain JSON values only",
        "contract_fields": generated_contract_fields_guidance(),
        "evidence_requirements": {
            "allowed_native_sources": ["bank/metric_app.csv", "bank/metric_container.csv",
                "bank/trace_span.csv", "bank/log/log_service.csv",
                "bank/metric/metric_app.csv", "bank/metric/metric_container.csv",
                "bank/trace/trace_span.csv", "bank/log/log_service.csv"],
            "support": "every positive-ranked lead requires at least one evidence item with support > 0",
            "eligibility": "every positive-ranked lead requires details.ranking_eligible == true only after the operator's declared gates pass",
            "attribution": "evidence.contributing_entities must include the exact component EntityRef being scored",
            "statistics": "do not invent p-values, effects, supports, or statistical-test claims; include them only when computed from the supplied inputs with declared methods",
            "bank_category": "category must be null; no Bank fault-category classifier is supported"},
        "contracts": {
            "EntityRef": {"fields": {"role": "string; Bank component outputs use 'component'",
                                       "identifier": "exact observed component ID"}},
            "Window": {"fields": {"name": "string: reference or alert",
                                    "start_ms_utc": "integer", "end_ms_utc": "integer"}},
            "AggregateRow": {"fields": {"entity": "EntityRef envelope", "metric_key": "string",
                "window": "Window envelope", "statistic": "string", "value": "finite number or null",
                "support": "nonnegative integer", "source_tables": "tuple of strings",
                "contributing_entities": "tuple of EntityRef envelopes", "details": "mapping envelope of finite JSON data"}},
            "Aggregate": {"fields": {"window": "Window envelope", "rows": "tuple of AggregateRow envelopes",
                "source_tables": "tuple of strings", "metadata": "mapping envelope of finite JSON data"}},
            "Candidate": {"fields": {"entity": "EntityRef envelope", "rationale": "string"}},
            "CandidateSet": {"fields": {"candidates": "tuple of Candidate envelopes", "source": "string"}},
            "Evidence": {"fields": {"source": "string", "window": "Window envelope", "statistic": "string",
                "value": "finite number, string, boolean, or null", "contributing_entities": "tuple of EntityRef envelopes",
                "support": "nonnegative integer", "details": "mapping envelope of finite JSON data"}},
            "ScoredCandidate": {"fields": {"entity": "EntityRef envelope", "score": "finite number",
                "category": "null for Bank M4; no Bank category classifier is supported",
                "evidence": "tuple of Evidence envelopes", "analysis_family": "string or null"}},
            "ScoredCandidates": {"fields": {"candidates": "tuple of ScoredCandidate envelopes",
                "scorer": "string", "status": "ready or abstained",
                "status_details": "mapping envelope of finite JSON data"}}}}


def data_operator_contract_schema(*, profile: str = "legacy-m4") -> dict[str, Any]:
    """Document the exact metadata layouts returned by native query and summary projection."""
    if profile == "reconstructed-paper-starter-v1":
        from .paper_starter import consensus_weight_catalog, paper_catalog, paper_parameter_catalog
        return {"profile": profile, "reconstruction": paper_catalog(),
            "consensus_weight_groups": consensus_weight_catalog(),
            "parameter_contracts": paper_parameter_catalog(),
            "paper_input": {"type": "PaperStarterInput",
                "optional_rich_input_semantics": "None means unsupported; empty tuple means supported but observed-empty",
                "native_log_text": "fully scanned locally from hash-pinned source clips; only normalized bounded template summaries cross into operators; no raw lines are returned",
                "terminal": "paper.answers.weighted_consensus@1",
                "canonical_counts": {"diagnostics": 19, "candidate_generators": 7,
                    "scorers": 10, "summarizers": 2, "playbooks": 11}}}
    return {
        "bank.public.query": {
            "output": "Aggregate",
            "aggregate_rows": "always an empty tuple; native rows are in metadata.public_query.rows_data",
            "public_query": {
                "request_fields": {"table": "native table path", "window": "reference|alert",
                    "entity": "exact filter or null", "metric": "exact filter or null",
                    "offset": "nonnegative integer", "limit": "integer 1..256",
                    "fields": "selected native columns plus timestamp_ms_utc"},
                "rows_data": "list of native CSV row objects; selected source-column values remain strings, timestamp_ms_utc is integer UTC milliseconds; selected fields only",
                "coverage_fields": ["total_matching_rows", "returned_rows", "source_files",
                    "source_rows_scanned", "source_bytes_scanned", "source_bytes_hashed",
                    "case_metadata_bytes_hashed", "returned_bytes", "bytes_processed_definition",
                    "omitted_before_rows", "remaining_after_rows", "truncated", "page_complete", "complete"],
                "page_fields": {"next_offset": "integer for next page or null",
                    "source_header": "native table header list",
                    "canonical_extra_fields": ["timestamp_ms_utc"],
                    "source_hashes": "mapping of exact prepared relative paths, including alert.json, to SHA-256 hex",
                    "timestamp_units": "mapping of exact prepared relative paths to empirically pinned seconds|milliseconds",
                    "timestamp_field": "timestamp_ms_utc is normalized; source timestamp remains verbatim",
                    "value_semantics": "native values only; no inferred metric units or component aliases"},
                "table_columns": {
                    "metric/metric_app.csv": ["timestamp", "rr", "sr", "cnt", "mrt", "tc"],
                    "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
                    "trace/trace_span.csv": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
                    "log/log_service.csv": ["log_id", "timestamp", "cmdb_id", "log_name", "value"]}},
            "limits": {"maximum_rows_per_request": 256, "maximum_request_bytes": 512 * 1024,
                "maximum_offset": 100_000, "string_limit_utf8_bytes": 4096}},
        "bank.public.project": {
            "input_output": "Aggregate to Aggregate; input must be an existing full-window summary Aggregate, not a native query page or prior projection",
            "output": {"rows": "tuple of selected AggregateRows; details preserve scalar values and add projection_omitted_detail_keys; each row preserves full-window value, support, metric/entity/window/source/contributors",
                "metadata": "preserves scalar source metadata; adds projection and projection_omitted_metadata_keys",
                "metadata_projection_fields": ["source_aggregate_digest", "source_row_count",
                    "total_matching_row_count", "selected_row_count", "omitted_before", "remaining_after",
                    "complete", "offset", "limit", "next_offset", "source_fields_filter",
                    "entities_filter", "statistics_filter", "omitted_metadata_keys",
                    "omitted_detail_keys", "projection_policy", "source_serialized_bytes",
                    "projection_serialized_bytes"]},
            "selection": "filters exact source_fields and statistics, entity identifier trim+casefold only; offset/limit pages rows without recomputing or sampling statistics",
            "limits": {"maximum_rows_per_request": 256, "maximum_output_bytes": 512 * 1024,
                "maximum_offset": 100_000, "string_limit_utf8_bytes": 4096}}
    }


def generated_spec_shape_example() -> dict[str, Any]:
    """Return a neutral shape-only example, not an analytical operator proposal."""
    return {
        "name": "evolved.example_operator",
        "version": "1",
        "description": "Shape-only pass-through example; replace with data-driven logic.",
        "family": "example",
        "input_ports": [{"name": "data", "type": "Aggregate", "required": True,
                         "variadic": False, "minimum_items": 1, "maximum_items": None}],
        "output_type": "Aggregate",
        "parameters": {"scale": {"type": "number", "required": False,
            "nullable": False, "default": 1.0, "choices": [], "runtime_field": None,
            "description": "Illustrative declared parameter."}},
        "source": "def operator(inputs, params, runtime):\n    # Shape only; replace with data-driven logic.\n    return inputs['data']",
    }


def _dag_json_shape_guidance(*, terminal_operator: str = "bank.answers.combine") -> str:
    """Explain the DAG wire shape independently of any analytical graph choice."""
    empty_node_maps = {"inputs": {}, "params": {}, "bindings": {}}
    edge_example = {"inputs": {"scalar_port": "prior_node",
                               "variadic_port": ["prior_node", "another_prior_node"]},
                    "params": {}, "bindings": {}}
    return (
        "DAG JSON SHAPE (syntax illustration; replace every placeholder with registered nodes and exact signatures). "
        "Every node must include inputs, params, and bindings as JSON objects, even when empty: use {}. "
        "For inputs, map each port name to a prior node ID string when the signature says required/non-variadic; "
        "map a variadic port to an array of prior node ID strings (including [] only when its minimum is zero). "
        "Never use an array for a scalar port or an object for a node reference. Params map declared parameter names "
        "to JSON values; bindings map declared parameter names to runtime.field strings. Nodes must appear after "
        f"their input nodes. The terminal return must be the trusted registered {terminal_operator} node, whose "
        "scores input is variadic and receives one or more ScoredCandidates node IDs, and whose output is RankedAnswer. "
        "Node map example: " + json.dumps(empty_node_maps, ensure_ascii=False, separators=(",", ":")) +
        "; input reference example: " + json.dumps(edge_example, ensure_ascii=False, separators=(",", ":"))
    )


def build_m4_runtime(case: Mapping[str, Any], *,
                     overrides: Mapping[str, Any] | None = None,
                     root: Path = ROOT,
                     policy_path: Path | None = None) -> RuntimeContext:
    """Build a typed case runtime while enforcing frozen protected values and override bounds."""
    policy = _policy(root, policy_path)
    serving = policy["serving"]
    protected = policy["protected_runtime_values"]
    core = json.loads((root / "config/core_runtime.json").read_text(encoding="utf-8"))
    for key, value in protected.items():
        if core.get(key) != value:
            raise M4ControllerError("protected_runtime_mismatch",
                                    f"core runtime {key} differs from frozen M4 policy")
    if not isinstance(overrides, (Mapping, type(None))):
        raise M4ControllerError("runtime_override_schema", "runtime_overrides must be an object", static=True)
    supplied = dict(overrides or {})
    if set(supplied) & set(protected):
        raise M4ControllerError("protected_runtime_override",
            f"protected runtime values cannot be changed: {sorted(set(supplied) & set(protected))}", static=True)
    allowed = policy["model_runtime_overrides"]
    unknown = set(supplied) - set(allowed)
    if unknown:
        raise M4ControllerError("unknown_runtime_override",
                                 f"undeclared runtime override(s): {sorted(unknown)}", static=True)

    runtime_values = dict(core)
    runtime_values.update(protected)
    weight_defaults = bank_m4_analysis_weights(root=root)
    runtime_values["analysis_weights"] = weight_defaults
    for name, value in supplied.items():
        spec = allowed[name]
        value_type = spec["type"]
        if value_type == "number_map":
            if not isinstance(value, Mapping):
                raise M4ControllerError("runtime_override_type", f"{name} must be a JSON object", static=True)
            keys = set(value)
            invalid_keys = keys - set(spec["allowed_keys"])
            if invalid_keys:
                raise M4ControllerError("runtime_override_key",
                    f"unsupported {name} key(s): {sorted(invalid_keys)}; allowed keys are "
                    f"{sorted(spec['allowed_keys'])}", static=True,
                    details={"allowed_keys": sorted(spec["allowed_keys"])})
            if any(type(item) not in (int, float) or not math.isfinite(float(item)) or
                   float(item) < float(spec["minimum"]) or float(item) > float(spec["maximum"])
                   for item in value.values()):
                raise M4ControllerError("runtime_override_range",
                    f"{name} values must be finite and between {spec['minimum']} and {spec['maximum']}",
                    static=True)
            runtime_values[name] = bank_m4_analysis_weights(root=root, overrides=value)
            continue
        if value_type == "positive_integer":
            valid_type = type(value) is int and value > 0
        elif value_type == "number":
            valid_type = type(value) in (int, float) and math.isfinite(float(value))
        else:
            raise M4ControllerError("m4_policy_schema", f"unsupported override type {value_type}")
        if not valid_type:
            raise M4ControllerError("runtime_override_type", f"invalid value type for {name}", static=True)
        if "choices" in spec and value not in spec["choices"]:
            raise M4ControllerError("runtime_override_choice",
                f"{name} must be one of {spec['choices']}", static=True)
        if "minimum" in spec and float(value) < float(spec["minimum"]):
            raise M4ControllerError("runtime_override_range", f"{name} is below its frozen minimum", static=True)
        if "maximum" in spec and float(value) > float(spec["maximum"]):
            raise M4ControllerError("runtime_override_range", f"{name} exceeds its frozen maximum", static=True)
        runtime_values[name] = value
    block_minutes = int(runtime_values["block_minutes"])
    if runtime_values["minimum_independent_blocks"] > 30 // block_minutes:
        raise M4ControllerError("runtime_block_support",
            "minimum_independent_blocks exceeds the number of blocks in a 30-minute window", static=True)
    try:
        return build_case_runtime(case, schema_path=root / "config/runtime_schema.json",
                                  config_path=root / "config/core_runtime.json",
                                  overrides=runtime_values)
    except Exception as exc:
        raise M4ControllerError("runtime_context_invalid", str(exc), static=True) from exc


def public_case_digest(case: BankCase, *, root: Path = ROOT,
                       max_series: int = MAX_DIGEST_SERIES,
                       max_bytes: int = MAX_CASE_DIGEST_BYTES) -> dict[str, Any]:
    """Build a bounded, deterministic case digest from public telemetry only."""
    if type(max_series) is not int or max_series <= 0 or type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("digest limits must be positive integers")
    telemetry = case.telemetry
    metric_policy = json.loads((root / "config/bank_metric_kind_policy.json").read_text(encoding="utf-8"))
    metric_semantics = metric_policy.get("source_semantics", {})
    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    metric_groups: dict[tuple[str, str, str], dict[str, dict[int, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    for point in telemetry.metrics:
        if point.value is None or not math.isfinite(point.value):
            continue
        name = next((key for key, window in windows.items()
                     if window.contains(point.timestamp_ms_utc)), None)
        if name is None:
            continue
        metric_groups[(point.entity.role, point.entity.identifier,
                       f"{point.table}|{point.metric_name}|{point.item_id or ''}")][name][
                           point.timestamp_ms_utc].append(float(point.value))
    component_series = []
    app_series = []
    log_series = []
    for (role, entity, metric), by_window in sorted(metric_groups.items(),
            key=lambda item: (item[0][0], item[0][1].casefold(), item[0][1], item[0][2])):
        table, field_name, item_id = (metric.split("|", 2) + [""])[:3]
        if table.endswith("metric_app.csv"):
            declared_semantics = metric_semantics.get("metric_app", {}).get("fields", {}).get(field_name, {})
        elif table.endswith("metric_container.csv"):
            declared_semantics = metric_semantics.get("metric_container", {}).get("ranking_allowlist", {}).get(
                field_name, {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False})
        elif table.endswith("log_service.csv"):
            declared_semantics = metric_semantics.get("log_service", {})
        else:
            declared_semantics = {"kind": "unknown", "unit": "undeclared", "ranking_eligible": False}
        row = {"role": role, "entity": entity, "metric": metric,
               "field_semantics": {key: declared_semantics.get(key) for key in
                   ("kind", "unit", "ranking_eligible", "provenance", "working_interpretation")
                   if key in declared_semantics}, "windows": {}}
        for window_name in ("reference", "alert"):
            timestamp_groups = by_window.get(window_name, {})
            timestamp_medians = [float(median(values)) for _timestamp, values in sorted(timestamp_groups.items())
                                 if values]
            values = sorted(timestamp_medians)
            raw_rows = sum(len(items) for items in timestamp_groups.values())
            if values:
                row["windows"][window_name] = {"support": len(values),
                    "raw_rows": raw_rows,
                    "duplicate_rows_collapsed": max(0, raw_rows - len(values)),
                    "median": float(median(values)), "mean": float(mean(values)),
                    "q90": _quantile(values, 0.9)}
            else:
                row["windows"][window_name] = {"support": 0, "raw_rows": raw_rows,
                    "duplicate_rows_collapsed": 0, "median": None,
                    "mean": None, "q90": None}
        ref_center = row["windows"]["reference"]["median"]
        alt_center = row["windows"]["alert"]["median"]
        if ref_center is not None and alt_center is not None:
            row["descriptive_selection_change_ratio"] = float(abs(alt_center - ref_center) /
                max(abs(ref_center), float(metric_policy.get("source_semantics", {}).get(
                    "normalization_floor", 1e-9))))
        else:
            row["descriptive_selection_change_ratio"] = None
        if role == "app_kpi":
            app_series.append(row)
        elif "log_service.csv" in metric:
            log_series.append(row)
        else:
            component_series.append(row)

    # This ratio controls only which public series enter the bounded digest;
    # it is descriptive input selection, not inferential effect or causality.
    # Exact identifiers provide stable label-blind tie-breakers.
    component_series.sort(key=lambda row: (
        -(row["descriptive_selection_change_ratio"] if row["descriptive_selection_change_ratio"] is not None else -1.0),
        row["entity"].casefold(), row["entity"], row["metric"]))
    app_series.sort(key=lambda row: (
        -min(row["windows"]["reference"]["support"], row["windows"]["alert"]["support"]),
        row["entity"].casefold(), row["entity"], row["metric"]))
    log_series.sort(key=lambda row: (
        -(row["descriptive_selection_change_ratio"] if row["descriptive_selection_change_ratio"] is not None else -1.0),
        row["entity"].casefold(), row["entity"], row["metric"]))
    kept_series = component_series[:max_series]
    kept_app_series = app_series[:max_series]
    kept_log_series = log_series[:max_series]
    app_kpi_context = _select_primary_app_kpi_context(case, root=root)

    trace_groups: dict[tuple[str, str], dict[str, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    for span in telemetry.spans:
        if span.executing_component is None or not span.trace_id or not span.span_id:
            continue
        window_name = next((key for key, window in windows.items()
                            if window.contains(span.start_ms_utc)), None)
        if window_name is None:
            continue
        try:
            value = float(span.elapsed_raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value >= 0:
            trace_groups[(span.executing_component.identifier,
                          "bank/trace_span.csv|duration")][window_name].setdefault(span.trace_id, []).append(value)
    traces = []
    for (entity, metric), by_window in sorted(trace_groups.items(),
            key=lambda item: (item[0][0].casefold(), item[0][0], item[0][1])):
        row = {"entity": entity, "metric": metric, "success_field": None,
               "duration_unit": "undeclared", "windows": {}}
        for window_name in ("reference", "alert"):
            values = [float(median(group)) for _trace, group in sorted(by_window[window_name].items()) if group]
            values.sort()
            row["windows"][window_name] = {"independent_trace_support": len(values),
                "median": float(median(values)) if values else None,
                "mean": float(mean(values)) if values else None,
                "q90": _quantile(values, 0.9) if values else None}
        traces.append(row)
    output = {
        "case_id": telemetry.case_id,
        "timezone": case.alert.get("timezone"),
        "reference_window_utc_ms": [telemetry.reference_window.start_ms_utc,
                                     telemetry.reference_window.end_ms_utc],
        "alert_window_utc_ms": [telemetry.alert_window.start_ms_utc,
                                 telemetry.alert_window.end_ms_utc],
        "source_timestamp_units_by_date": _plain(case.alert.get("source_date_timestamp_units", {})),
        "file_sha256": dict(sorted(telemetry.file_sha256.items())),
        "file_bytes": dict(sorted(telemetry.file_bytes.items())),
        "exact_observed_component_ids": sorted(telemetry.deployment_catalog,
            key=lambda item: (item.casefold(), item)),
        "available_metric_fields": sorted({point.metric_name for point in telemetry.metrics}),
        "primary_app_kpi_context": app_kpi_context,
        "primary_app_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "metric_digest_selection_policy": "component series ordered by absolute median change divided by max(abs(reference median), frozen normalization floor); descriptive display selection only, not an inferential effect or cause score; ties use casefolded then exact entity and metric ID",
        "component_identity_coverage": _plain(case.provenance.get("component_identity_coverage", {})),
        "quality_counts": dict(sorted(telemetry.quality_counts.items())),
        "log_summary": _plain(case.log_summary),
        "metric_series": kept_series,
        "metric_series_omitted": max(0, len(component_series) - len(kept_series)),
        "app_kpi_context_series": kept_app_series,
        "app_kpi_context_series_omitted": max(0, len(app_series) - len(kept_app_series)),
        "log_volume_series": kept_log_series,
        "log_volume_series_omitted": max(0, len(log_series) - len(kept_log_series)),
        "trace_series": traces[:max_series],
        "trace_series_omitted": max(0, len(traces) - max_series),
        "log_text_excerpts": [],
        "log_text_omission_reason": "raw text is queried through bounded bank.public.query pages and is not in the initial case digest",
    }
    encoded = json.dumps(output, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > max_bytes:
        output["component_identity_coverage"] = {"truncated": True}
        output["quality_counts"] = {}
        output["log_summary"] = {"truncated": True}
        output["metric_series"] = kept_series[:max(1, max_series // 2)]
        output["metric_series_omitted"] = max(0, len(component_series) - len(output["metric_series"]))
        output["app_kpi_context_series"] = kept_app_series[:max(1, max_series // 2)]
        output["app_kpi_context_series_omitted"] = max(0, len(app_series) - len(output["app_kpi_context_series"]))
        output["log_volume_series"] = kept_log_series[:max(1, max_series // 2)]
        output["log_volume_series_omitted"] = max(0, len(log_series) - len(output["log_volume_series"]))
        output["trace_series"] = traces[:max(1, max_series // 2)]
        output["trace_series_omitted"] = max(0, len(traces) - len(output["trace_series"]))
        encoded = json.dumps(output, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > max_bytes:
        raise M4ControllerError("case_digest_limit", "bounded public case digest exceeds its frozen byte limit")
    output["digest_sha256"] = hashlib.sha256(encoded).hexdigest()
    output["digest_bytes"] = len(encoded)
    output["metric_series_limit"] = max_series
    output["trace_series_limit"] = max_series
    return output


def _quantile(values: list[float], q: float) -> float:
    if not values:
        raise ValueError("quantile requires nonempty values")
    position = q * (len(values) - 1)
    lower = int(position)
    upper = min(len(values) - 1, lower + 1)
    fraction = position - lower
    return float(values[lower] + fraction * (values[upper] - values[lower]))


def _select_primary_app_kpi_context(case: BankCase, *, root: Path) -> dict[str, Any]:
    """Apply the frozen Bank support-first app-mrt context rule without fallback."""
    policy_path = root / "config/bank_dev_kpi_policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise M4ControllerError("bank_kpi_policy_version", "unsupported frozen Bank KPI context policy")
    minimum = int(policy["minimum_support_per_window"])
    allowed = set(policy.get("primary_metric_allowlist", ()))
    windows = {"reference": case.telemetry.reference_window,
               "alert": case.telemetry.alert_window}
    groups: dict[tuple[str, str, str], dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    raw_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for point in case.telemetry.metrics:
        if point.table != "bank/metric_app.csv" or point.value is None or not math.isfinite(point.value):
            continue
        field = f"metric_app.{point.metric_name}"
        if field not in allowed or point.entity.role != "app_kpi":
            continue
        window_name = next((name for name, window in windows.items()
                            if window.contains(point.timestamp_ms_utc)), None)
        if window_name is None:
            continue
        key = (field, point.entity.identifier, window_name)
        groups[key][point.timestamp_ms_utc].append(float(point.value))
        raw_counts[key] += 1
    candidates = []
    identities = sorted({(field, tc) for field, tc, window in groups if window == "reference"} &
                        {(field, tc) for field, tc, window in groups if window == "alert"},
                        key=lambda item: (-min(len(groups[(item[0], item[1], name)])
                            for name in ("reference", "alert")), item[1].casefold(), item[1], item[0]))
    for field, tc in identities:
        values_by_window = {}
        for name in ("reference", "alert"):
            per_timestamp = [float(median(values)) for _, values in
                             sorted(groups[(field, tc, name)].items()) if values]
            values_by_window[name] = sorted(per_timestamp)
        support = {name: len(values_by_window[name]) for name in values_by_window}
        if min(support.values()) < minimum:
            continue
        candidates.append({"kpi_id": field, "field": field.split(".", 1)[1],
            "tc_id": tc, "role": "app_kpi_context_only", "reference_support": support["reference"],
            "alert_support": support["alert"], "paired_support": min(support.values()),
            "raw_rows": {name: raw_counts[(field, tc, name)] for name in support},
            "duplicate_rows_collapsed": {name: raw_counts[(field, tc, name)] - support[name]
                                         for name in support},
            "reference": {"median": float(median(values_by_window["reference"])),
                "mean": float(mean(values_by_window["reference"])),
                "q90": _quantile(values_by_window["reference"], 0.9)},
            "alert": {"median": float(median(values_by_window["alert"])),
                "mean": float(mean(values_by_window["alert"])),
                "q90": _quantile(values_by_window["alert"], 0.9)},
            "unit": "undeclared", "component_mapping": "none",
            "interpretation": "mean-response-time-style working interpretation from field name only"})
    if not candidates:
        return {"kpi_id": None, "selection_reason": "no allowlisted app KPI/tc pair met frozen support; no resource fallback",
            "minimum_support_per_window": minimum, "policy_sha256": sha256_file(policy_path),
            "source_policy": policy.get("selection_provenance")}
    candidates.sort(key=lambda item: (-item["paired_support"], item["tc_id"].casefold(),
                                      item["tc_id"], item["kpi_id"]))
    return {**candidates[0], "selection_reason": policy.get("selection"),
        "minimum_support_per_window": minimum, "policy_sha256": sha256_file(policy_path),
        "source_policy": policy.get("selection_provenance")}


def _topology(document: Mapping[str, Any]) -> dict[str, Any]:
    return {"nodes": [{"id": row["id"], "operator": row["operator"],
                        "version": row["version"], "inputs": _plain(row["inputs"])}
                       for row in document["nodes"]],
            "return": _plain(document["return"])}


def _materialize_seeds_for_registry(seed_library: Mapping[str, Any],
                                    registry: OperatorRegistry) -> dict[str, Any]:
    versions: dict[str, str] = {}
    for row in registry.signatures():
        if row["name"] == "bank.answers.combine":
            if row["version"].startswith("1+m4."):
                versions[row["name"]] = row["version"]
        elif row["name"] not in versions:
            versions[row["name"]] = row["version"]
    examples = seed_library.get("examples")
    if not isinstance(examples, Mapping):
        raise M4ControllerError("seed_schema", "Bank seed library examples must be an object")
    materialized = {}
    for name, doc in examples.items():
        value = json.loads(json.dumps(doc, ensure_ascii=False, allow_nan=False))
        for node in value.get("nodes", []):
            operator_name = node.get("operator")
            if operator_name not in versions:
                raise M4ControllerError("seed_operator_missing", f"no active registry version for {operator_name}")
            node["version"] = versions[operator_name]
        materialized[str(name)] = value
    return materialized


def _validate_graph(document: Mapping[str, Any], registry: OperatorRegistry,
                    runtime: RuntimeContext, *, stage: str,
                    new_operator_keys: set[str] | None = None,
                    profile: str = "legacy-m4") -> VerifiedDAG:
    try:
        verified = verify_dag(document, registry,
            runtime=runtime,
            max_nodes=int(runtime.snapshot.values["max_nodes"]),
            max_depth=int(runtime.snapshot.values["dag_depth"]))
    except DAGValidationError as exc:
        raise M4ControllerError("dag_static_invalid", str(exc), static=True) from exc
    external_nodes = [node for node in verified.nodes if node.operator.external_input]
    if len(external_nodes) != 1 or external_nodes[0].operator.name != "case.telemetry":
        raise M4ControllerError("dag_external_input", "M4 DAG must contain exactly one case.telemetry input", static=True)
    if profile == "reconstructed-paper-starter-v1" and any(
            node.operator.name == "bank.public.query" for node in verified.nodes):
        raise M4ControllerError("paper_raw_query_forbidden",
            "paper starter profile does not expose native raw-row query operators", static=True)
    returned = next(node for node in verified.nodes if node.node_id == verified.return_node)
    if profile == "legacy-m4" and registry.get("paper.answers.weighted_consensus", "1") is not None:
        profile = "reconstructed-paper-starter-v1"
    if profile == "reconstructed-paper-starter-v1":
        terminal_valid = (returned.operator.name == "paper.answers.weighted_consensus" and
            returned.operator.version == "1" and returned.operator.output_type == "RankedAnswer")
        terminal_error = "paper starter DAG must return the registered paper weighted-consensus terminal"
    else:
        terminal_valid = (returned.operator.name == "bank.answers.combine" and
            returned.operator.version.startswith("1+m4.") and returned.operator.output_type == "RankedAnswer")
        terminal_error = "M4 DAG must return the versioned Bank M4 RankedAnswer combiner"
    if not terminal_valid:
        raise M4ControllerError("dag_return_type", terminal_error, static=True)
    if "limit" in returned.params and returned.params["limit"] > int(runtime.snapshot.values["lead_count"]):
        raise M4ControllerError("dag_lead_limit", "M4 DAG cannot return more leads than protected runtime.lead_count", static=True)
    if stage in {"synthesize", "stage3_serve"} and new_operator_keys is not None:
        reachable = new_operator_keys or set()
        used = {f"{node.operator.name}@{node.operator.version}" for node in verified.nodes}
        if not reachable or not reachable.issubset(used):
            raise M4ControllerError("stage3_new_operator_unreachable",
                "Stage 3 DAG must reach every newly proposed operator", static=True)
    return verified


def _apply_stage1_selection(selection: Mapping[str, Any], snapshot: LibrarySnapshot,
                            registry: OperatorRegistry) -> tuple[dict[str, Any], str]:
    expected = {"playbook", "runtime_overrides", "node_parameters", "node_bindings"}
    if not isinstance(selection, Mapping) or set(selection) != expected:
        raise M4ControllerError("stage1_schema", f"Stage 1 response must contain exactly {sorted(expected)}", static=True)
    name = selection["playbook"]
    if not isinstance(name, str) or name not in snapshot.playbooks:
        raise M4ControllerError("stage1_playbook_unknown", "Stage 1 must select one current library playbook", static=True)
    playbook = snapshot.playbooks[name]
    document = _plain(playbook["document"])
    before_topology = _topology(document)
    if not isinstance(selection["node_parameters"], Mapping) or not isinstance(selection["node_bindings"], Mapping):
        raise M4ControllerError("stage1_parameter_schema", "node_parameters/node_bindings must be objects", static=True)
    for node_id in set(selection["node_parameters"]) & set(selection["node_bindings"]):
        if not isinstance(selection["node_parameters"][node_id], Mapping) or not isinstance(selection["node_bindings"][node_id], Mapping):
            continue
        conflict = set(selection["node_parameters"][node_id]) & set(selection["node_bindings"][node_id])
        if conflict:
            raise M4ControllerError("stage1_parameter_binding_conflict",
                f"a node parameter cannot be overridden explicitly and by binding at once: {node_id}:{sorted(conflict)}",
                static=True)
    by_id = {node["id"]: node for node in document["nodes"]}
    for field_name in ("node_parameters", "node_bindings"):
        values = selection[field_name]
        unknown_nodes = set(values) - set(by_id)
        if unknown_nodes:
            raise M4ControllerError("stage1_node_unknown", f"unknown stored node IDs: {sorted(unknown_nodes)}", static=True)
        for node_id, mapping in values.items():
            if not isinstance(mapping, Mapping):
                raise M4ControllerError("stage1_parameter_schema", f"{field_name}.{node_id} must be an object", static=True)
            node = by_id[node_id]
            operator = registry.get(node["operator"], node["version"])
            if operator is None:
                raise M4ControllerError("stage1_operator_unknown", "stored playbook operator is unavailable", static=True)
            for key, value in mapping.items():
                if key not in operator.parameters:
                    raise M4ControllerError("stage1_parameter_unknown", f"undeclared parameter {key} on node {node_id}", static=True)
                if field_name == "node_parameters":
                    node["params"][key] = value
                    node["bindings"].pop(key, None)
                else:
                    if not isinstance(value, str) or not value.startswith("runtime."):
                        raise M4ControllerError("stage1_binding_schema", "runtime bindings must use runtime.<field>", static=True)
                    node["bindings"][key] = value
                    node["params"].pop(key, None)
    if _topology(document) != before_topology:
        raise M4ControllerError("stage1_topology_changed", "Stage 1 cannot add or change nodes, edges, operators, versions, or return node", static=True)
    return document, name


def _stage_prompt(stage: str, *, digest: Mapping[str, Any], registry: OperatorRegistry,
                  snapshot: LibrarySnapshot, runtime: RuntimeContext,
                  history: list[Mapping[str, Any]], policy: Mapping[str, Any],
                  query_budget: M4QueryBudget, include_case_digest: bool = True,
                  include_history: bool = True,
                  gap_record: Mapping[str, Any] | None = None,
                  composite_operator_specs: list[Mapping[str, Any]] | None = None) -> str:
    from e4infra.bank_prep import BANK_OFFICIAL_CANDIDATE_COMPONENTS

    profile = snapshot.document.get("policy", {}).get("capacity_policy", "legacy-m4")
    paper_profile = profile == "reconstructed-paper-starter-v1"
    paper_code_profile = (paper_profile and
        stage3_authoring_profile(snapshot) == "typed-operator-code-v1")
    terminal_operator = "paper.answers.weighted_consensus" if paper_profile else "bank.answers.combine"
    v2_selector_status_rows: list[dict[str, Any]] = []
    v2_selector_policy_sha256: str | None = None
    v2_selector_case_sha256: str | None = None
    query_policy = policy["query_and_generation"]
    if composite_operator_specs is None:
        composite_operator_specs = [spec for spec in generated_specs_from_snapshot(snapshot)
            if spec.get("authoring_profile") == "operator-dag-v1"]
    composite_catalog = [{"name": spec["name"], "version": spec["version"],
        "description": spec["description"], "family": spec["family"],
        "input_ports": _plain(spec["input_ports"]), "output_type": spec["output_type"],
        "parameters": _plain(spec["parameters"]), "program_sha256": spec["program_sha256"],
        "dependencies": _plain(spec.get("dependencies", [])),
        **({"input_requirements": _plain(spec["input_requirements"])}
            if isinstance(spec.get("input_requirements"), Mapping) else {}),
        "program": _plain(spec["program"])} for spec in composite_operator_specs]
    query_usage = {"requests_used": query_budget.requests,
        "requests_remaining": max(0, int(query_policy["maximum_public_query_requests_per_case"]) - query_budget.requests),
        "rows_returned": query_budget.rows,
        "rows_remaining": max(0, int(query_policy["maximum_public_query_rows_per_case"]) - query_budget.rows),
        "serialized_bytes_returned": query_budget.bytes,
        "bytes_remaining": max(0, int(query_policy["maximum_public_query_bytes_per_case"]) - query_budget.bytes)}
    evolution_stage = stage in {"compose", "synthesize", "evolve", "stage3_serve"}
    override_guidance = ("Only named, bounded runtime overrides listed in the policy may be changed; use {} when none apply."
        if evolution_stage else "Use supplied runtime overrides only; do not invent threshold")
    common = (
        "You are running one Bank case in the frozen M4 experiment. Do not infer unsupported "
        "trace success, root/database roles, aliases, categories, or raw log severity. Bank duration units "
        "are undeclared. Exact observed IDs are case-local; never alias docker IDs to named pods. The app tc "
        "field is opaque and not a component. " + override_guidance +
        (" " if evolution_stage else " constants. ") +
        "The initial case summary is a bounded digest; raw rows can be inspected only with the " +
        "registered bank.public.query operator, whose offsets and total per-case request/row/byte budgets "
        "are enforced outside your process. When an Aggregate's cached details exceed the M4 serving worker's 16 MiB "
        "input limit, pass it through bank.public.project with declared source_fields/entities/statistics, "
        "offset 0..100000, and limit 1..256. Projection preserves existing full-window scalar summaries, support, "
        "units and identity; it drops non-scalar row details/metadata with explicit omitted-key and coverage records, "
        "does not recompute statistics, and is limited to 512 KiB/4096-byte strings. Never omit fields silently. "
        "Native query pages are already bounded and cannot be projected again. Use fresh bank.public.query pages "
        "when an algorithm needs raw row samples or raw log text. No label or prior gold-evaluation data is available."
    )
    if paper_profile:
        from .paper_starter import consensus_weight_catalog
        paper_override_guidance = ("Evolution does not set runtime overrides."
            if stage == "evolve" else override_guidance)
        common = (
            "Validated reconstructed-paper-starter-v1: the 19 canonical families and 11 playbooks are explicit "
            "reconstructions. Use active registered capabilities only; unsupported is not zero. Bank lacks client/session "
            "outcomes, URL/request fields, verified aliases, trace success/call type; trace duration units are undeclared. "
            "Trace parents are local and do not prove complete tracing. Logs expose only normalized bounded templates "
            "and support. Do not infer fault categories, aliases, or causality. "
            + paper_override_guidance)
        if paper_code_profile:
            common = common.replace("Logs expose only normalized bounded templates and support.",
                "Bank log rows are exposed as typed public records with verified source provenance; raw metadata envelopes are unavailable.")
            common += (" New source operators read declared typed inputs. Telemetry, when present, is available only "
                "through the published query helpers; the host supplies an opaque handle and verified bounded public row store. Inspect result coverage; "
                "do not treat incomplete queries as absence or infer unprovided trace success/log severity.")
        common += ("\n\nTRUSTED CONSENSUS WEIGHT CATALOG\n" +
            json.dumps(consensus_weight_catalog(), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        case_capabilities = policy.get("_paper_case_capabilities", {})
        common += ("\n\nCASE CAPABILITY STATUS\n" +
            json.dumps(case_capabilities, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\nUnsupported capabilities require abstention when used by a conditional algorithm.")
        v2_capability_prompt = policy.get("_paper_ir_capability_prompt_summary")
        if (stage in {"compose", "evolve", "stage3_serve"} and
                isinstance(v2_capability_prompt, Mapping)):
            displayed_capability_summary = _plain(v2_capability_prompt)
            if paper_profile and stage == "evolve":
                statuses = displayed_capability_summary.pop("selector_status_by_ref", {})
                if not isinstance(statuses, Mapping):
                    raise M4ControllerError("paper_selector_status_schema",
                        "V2 selector statuses must be a ref map in the trusted capability summary", static=True)
                row_schema = displayed_capability_summary.pop("selector_status_schema", [])
                if row_schema != ["status", "match_count", "semantic_profile_refs",
                        "reference_support_histogram", "alert_support_histogram"]:
                    raise M4ControllerError("paper_selector_status_schema",
                        "V2 selector status row schema is unsupported", static=True)
                v2_selector_status_rows = []
                for selector_ref, values in sorted(statuses.items()):
                    if (not isinstance(selector_ref, str) or not isinstance(values, list) or
                            len(values) != 5):
                        raise M4ControllerError("paper_selector_status_schema",
                            "V2 selector status values are malformed", static=True)
                    v2_selector_status_rows.append({"selector_ref": selector_ref,
                        "status": values[0], "match_count": values[1],
                        "semantic_profile_refs": values[2],
                        "reference_support_histogram": values[3],
                        "alert_support_histogram": values[4]})
                v2_selector_semantic_profiles = displayed_capability_summary.pop(
                    "selector_semantic_profiles", {})
                v2_selector_policy_sha256 = displayed_capability_summary.pop(
                    "selector_policy_sha256", None)
                v2_selector_case_sha256 = displayed_capability_summary.pop(
                    "selector_case_data_sha256", None)
                v2_selector_match_count = displayed_capability_summary.pop(
                    "selector_field_match_count", 0)
                v2_selector_unique_field_count = displayed_capability_summary.pop(
                    "selector_unique_field_count", 0)
                v2_selector_mapping_sha256 = displayed_capability_summary.pop(
                    "selector_field_mapping_sha256", None)
                displayed_capability_summary.pop("selector_field_mapping_omission", None)
                displayed_capability_summary["selector_status_and_semantics_ref"] = (
                    "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog[*].support_profile_ref "
                    "-> selector_case_resolution_profiles.profiles[*].case_resolution; "
                    "semantic profiles are in PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.selector_semantic_profiles")
                provenance = displayed_capability_summary.get("provenance")
                if isinstance(provenance, dict):
                    for key in ("metric_policy_sha256", "bank_metric_policy_sha256",
                                "primary_kpi_policy_sha256"):
                        provenance.pop(key, None)
                    displayed_capability_summary["policy_provenance_ref"] = (
                        "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.provenance")
            elif paper_profile:
                for computation in displayed_capability_summary.get("computations", []):
                    if (isinstance(computation, dict) and
                            computation.get("computation") == "native_log_pattern_composition"):
                        count = computation.get("pattern_feature_count", 0)
                        computation.pop("pattern_features_by_support", None)
                        computation["pattern_features_omitted_from_prompt"] = count
                        computation["pattern_example_omission_reason"] = (
                            "This high-level DAG has no pattern-hash selector; the registered scorer scans the full window internally. "
                            "Exact hash examples remain available to V2 IR evolution and the full host report.")
                for key in ("selector_status_by_ref", "selector_semantic_profiles",
                            "selector_status_schema", "selector_field_match_count",
                            "selector_unique_field_count", "selector_field_mapping_sha256",
                            "selector_field_mapping_omission", "selector_policy_sha256",
                            "selector_case_data_sha256"):
                    displayed_capability_summary.pop(key, None)
                displayed_capability_summary["selector_case_support_ref"] = (
                    "PAPER PARAMETER ENUMS AND CONSTRAINTS.case_support_profiles")
                provenance = displayed_capability_summary.get("provenance")
                if isinstance(provenance, dict):
                    for key in ("metric_policy_sha256", "bank_metric_policy_sha256",
                                "primary_kpi_policy_sha256", "telemetry_data_sha256",
                                "telemetry_source_hashes_sha256"):
                        provenance.pop(key, None)
                    displayed_capability_summary["case_support_provenance_ref"] = (
                        "PAPER PARAMETER ENUMS AND CONSTRAINTS.case_support_provenance")
            common += ("\n\nTRUSTED TYPED-IR V2 DATA/SUPPORT CAPABILITY SUMMARY\n" +
                json.dumps(displayed_capability_summary, ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")) +
                "\nHost summary: data presence, computability, and ranking are distinct; unknown ranking is no claim. "
                "V2 kernels still emit runtime support/abstention.")
    registered_signatures = registry.signatures()
    if paper_profile:
        # The frozen utility signature remains available for snapshot integrity,
        # but raw rows are not a serving capability in this profile.
        registered_signatures = [row for row in registered_signatures
            if row["name"] != "bank.public.query"]
    # Legacy source operators may carry a signed static input contract without
    # opting into either typed-record source profile. Publish that normalized
    # metadata beside its exact registered identity so callers can wire a
    # compatible producer; it does not alter the operator implementation.
    requirements_by_key = {(str(spec.get("name")), str(spec.get("version"))):
        _plain(spec["input_requirements"])
        for spec in generated_specs_from_snapshot(snapshot)
        if isinstance(spec.get("input_requirements"), Mapping)}
    registered_signatures = [{**row, **({"input_requirements": requirements_by_key[(row["name"], row["version"])]}
        if (row["name"], row["version"]) in requirements_by_key else {})}
        for row in registered_signatures]
    if paper_profile:
        from .paper_starter import intern_paper_parameter_choices
        registered_signatures = intern_paper_parameter_choices(registered_signatures)
        if stage != "evolve":
            parameter_catalog = _paper_parameter_catalog_for_prompt(policy)
            common += ("\n\nPAPER PARAMETER ENUMS AND CONSTRAINTS (choices_ref values are declared once here)\n" +
                json.dumps(parameter_catalog, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
                "\nOnly the exact string in a selector enum entry's `value` field is a parameter value; case-support fields are not emitted.")
    if evolution_stage:
        if paper_profile and stage == "evolve" and not paper_code_profile:
            from .m4_prompt_catalog import pack_registered_signature_catalog
            registered_signature_section = (
                "\n\nREGISTERED HIGH-LEVEL OPERATOR CAPABILITIES (lossless columnar catalog; "
                "operator row values follow operator_table.column_order; shared_fields apply to each row, and input requirements resolve through input_requirement_contract_table; "
                "IR evolution does not create a serving DAG)\n" +
                json.dumps(pack_registered_signature_catalog(
                    _paper_evolve_operator_catalog(registered_signatures)),
                    ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        elif paper_profile:
            from .m4_prompt_catalog import pack_registered_signature_catalog
            registered_signature_section = (
                "\n\nREGISTERED OPERATOR SIGNATURES (columnar host contract view; operator rows follow operator_table.column_order; "
                "input and parameter pairs resolve through the named contract tables; choices resolve through choice_lists; "
                "input-requirement refs resolve through input_requirement_contract_table. Optional parameter-description prose is omitted; "
                "parameter_description_omission carries one count/hash and points to the full host catalog. Emit exact selector values.)\n" +
                json.dumps(pack_registered_signature_catalog(
                    _paper_registered_signature_catalog(registered_signatures),
                    omit_parameter_descriptions=True),
                    ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        else:
            registered_signature_section = (
            "\n\nREGISTERED OPERATOR SIGNATURES (lossless defaults encoding; expand omitted catalog fields from listed defaults)\n" +
            json.dumps(_plain(_compact_registered_signatures(registered_signatures)),
                ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    else:
        if paper_profile:
            from .m4_prompt_catalog import pack_registered_signature_catalog
            registered_signature_section = (
                "\n\nREGISTERED OPERATOR SIGNATURES (columnar host contract view; operator rows follow operator_table.column_order; "
                "input and parameter pairs resolve through the named contract tables; choices resolve through choice_lists; "
                "input-requirement refs resolve through input_requirement_contract_table. Optional parameter-description prose is omitted; "
                "parameter_description_omission carries one count/hash and points to the full host catalog. Emit exact selector values.)\n" +
                json.dumps(pack_registered_signature_catalog(
                    _paper_registered_signature_catalog(registered_signatures),
                    omit_parameter_descriptions=True),
                    ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        else:
            registered_signature_section = (
                "\n\nREGISTERED OPERATOR SIGNATURES\n" +
                json.dumps(_plain(registered_signatures), ensure_ascii=False, sort_keys=True,
                    separators=(",", ":")))
    if paper_profile and stage in {"compose", "stage3_serve"}:
        readable = _paper_compose_port_contracts(registered_signatures)
        registered_signature_section += (
            "\nREADABLE NAMED PORTS (iN refs are metadata, never ports; required_params are ordinary params). High-level path: telemetry -> candidates/data and starter_input/data -> scorer(data=starter_input,candidates=candidates).\n" +
            json.dumps(readable, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if not paper_profile:
        common += (
        "\n\nOFFICIAL STATIC BANK CANDIDATE IDENTIFIERS (published schema vocabulary, not observations)\n" +
        json.dumps(list(BANK_OFFICIAL_CANDIDATE_COMPONENTS), ensure_ascii=False,
                   separators=(",", ":")) +
        "\nThese fixed source identifiers are public reference information only. They do not imply that an ID "
        "was observed in this case, do not provide aliases or mappings, and do not change the case-local observed "
        "component list or evidence requirements. Only rank exact observed component IDs supported by the selected case."
        )
    if include_case_digest:
        if paper_profile:
            prompt_digest = _paper_compact_case_digest_for_prompt(digest, stage=stage)
        else:
            prompt_digest = (_compact_evolution_case_digest(digest) if stage == "evolve" else digest)
        common += "\n\nCURRENT PUBLIC CASE DIGEST\n" + json.dumps(prompt_digest, ensure_ascii=False,
            sort_keys=True, separators=(",", ":"))
        if paper_profile:
            field_summary = prompt_digest.get("available_metric_fields", {})
            selector_ref = field_summary.get("selector_catalog_ref") if isinstance(field_summary, Mapping) else None
            related_refs = field_summary.get("related_selector_catalog_refs", []) if isinstance(field_summary, Mapping) else []
            common += ("\nThe observed metric-field inventory above is summarized by exact unique count, SHA-256, "
                "deterministic examples, and represented/omitted counts. Examples are descriptive only and do not "
                "authorize a selector. Use the dedicated legal selector catalog references " +
                json.dumps([selector_ref, *related_refs], ensure_ascii=False, separators=(",", ":")) +
                "; the host performs exact case-local matching and reports selector status/support.")
    elif digest:
        # Static repair does not need the repeated example block, but retain a
        # content-addressed pointer so the omission is explicit and auditable.
        common += ("\n\nCURRENT PUBLIC CASE DIGEST OMITTED FROM STATIC REPAIR PROMPT; "
            "the complete digest remains in the host audit. SHA-256 of canonical full digest: "
            + _canonical_hash(digest))
    if paper_profile:
        common += ("\n\nCURRENT ALLOWLISTED RUNTIME VALUES\n" +
            json.dumps(_paper_runtime_prompt_values(runtime), ensure_ascii=False,
                sort_keys=True, separators=(",", ":")))
        if stage != "evolve":
            common += "\n\nRUNTIME OVERRIDE POLICY\n" + json.dumps(policy["model_runtime_overrides"],
                ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        common += registered_signature_section
    else:
        common += (
            "\n\nPUBLIC QUERY AND GENERATED WORKER LIMITS\n" +
            json.dumps({"limits": query_policy, "current_case_usage": query_usage},
                       ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\n\nCURRENT ALLOWLISTED RUNTIME VALUES\n" + json.dumps(runtime.as_dict(), ensure_ascii=False,
                sort_keys=True, separators=(",", ":")) +
            "\n\nRUNTIME OVERRIDE POLICY\n" + json.dumps(policy["model_runtime_overrides"],
                ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            registered_signature_section)
    if include_history:
        if evolution_stage:
            compact_history = _compact_runtime_history(history, runtime.as_dict()["values"])
            history_header = ("\n\nPRIOR ATTEMPT HISTORY (diagnostic outputs and binary accepted flags only; "
                "runtime_values_delta sets/removes fields from the current runtime values using the matching base_sha256)\n")
            if paper_profile:
                history_header = ("\n\nPRIOR ATTEMPT HISTORY (diagnostic outputs and binary accepted flags only; "
                    "runtime_values_delta sets/removes fields from the current runtime values using the matching base_sha256. "
                    "Each history_ref is stable within this prompt. If public_analysis_evidence has ranked_answer_ref, "
                    "its complete bounded ranked-lead/evidence excerpt is in ranked_answer on that same history_ref.)\n")
        else:
            compact_history = [{key: value for key, value in row.items()
                if key != "plan_summary" and not key.startswith("_")} for row in history[-MAX_PROMPT_HISTORY:]]
            history_header = "\n\nPRIOR ATTEMPT HISTORY (diagnostic outputs and binary accepted flags only)\n"
        compact_history = [{key: value for key, value in row.items() if key != "plan_summary" and not key.startswith("_")}
                           for row in compact_history]
        if paper_profile:
            for index, row in enumerate(compact_history, start=1):
                row["history_ref"] = f"h{index}"
                evidence = row.get("public_analysis_evidence")
                if isinstance(evidence, dict):
                    ranked_leads = evidence.pop("ranked_leads", None)
                    if isinstance(ranked_leads, list):
                        evidence["ranked_answer_ref"] = "ranked_answer in this same history_ref"
        if paper_profile and stage in {"compose", "stage3_serve"}:
            # The following public-evidence section carries these exact fields
            # with plan references, so emit them only there. Keep the bounded
            # ranked-answer excerpt because it contains evidence details not
            # repeated by the coverage projection.
            for index, row in enumerate(compact_history, start=1):
                row["history_ref"] = f"h{index}"
                for key in ("public_analysis_evidence", "adaptation_record",
                            "adaptation_record_origin", "static_errors"):
                    row.pop(key, None)
        common += (history_header +
            json.dumps(compact_history, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False))
    if stage in {"compose", "synthesize", "evolve", "stage3_serve"}:
        plans = [({**row["plan_summary"], "stage": row.get("stage")}
            if paper_profile and isinstance(row.get("stage"), str) else row["plan_summary"])
            for row in history if isinstance(row.get("plan_summary"), Mapping)]
        common += "\n\nPRIOR ANALYSIS PLANS (bounded structural summaries; compare their computations, not names)\n" + \
            json.dumps(_bounded_plan_history(plans), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if paper_profile and stage in {"compose", "stage3_serve"}:
            adaptation_context = _paper_adaptation_evidence_context(history)
            common += ("\n\nPRIOR PUBLIC ANALYSIS EVIDENCE (coverage, abstentions, rankings and effective scorer groups; "
                "not ground truth)\n" +
                json.dumps(adaptation_context, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        if paper_profile:
            common += ("\n\nDATA COVERAGE GUIDANCE\nUse only declared available fields and full-window trusted summaries. "
                "Unsupported or partial support requires abstention only for the affected computation or input; "
                "independent supported analyses may proceed.")
        else:
            common += ("\n\nDATA COVERAGE GUIDANCE\nPrefer registered operators that compute supported full-window aggregates for their selected windows. "
            "Case-digest selection and raw-query page limits bound the exposed digest or returned page; they do not cap the source scan performed by a registered full-window aggregator. "
            "bank.public.project selects rows and details from an existing aggregate and does not recompute statistics. "
            "bank.public.query returns a bounded raw-row page, so a no-match on a truncated or partial page does not establish absence across the full window.")
        if stage == "evolve" and not paper_profile:
            common += ("\n\nTYPED OPERATOR IR LANGUAGE\n" +
                "Stage 3 operators are atomic typed programs interpreted by a fixed source-free worker. A program can only compose the published private kernels. It cannot call a registered high-level operator or macro. Existing Aggregate producers retain timestamped median bucket_samples and trusted source/case hashes; new analyses may use only full-window approved component metrics plus paired app mrt context. The worker derives component identities, primary KPI selection, evidence, and eligibility from those inputs and frozen policies.")
        elif stage == "synthesize":
            common += ("\n\nCLOSED COMPOSITE LANGUAGE\n"
            "New operators are finite JSON DAG compositions over the registered catalog. This remains a stored macro profile for backcompat and reuse; it is not accepted as a newly synthesized Stage 3 operator. Preserve the trusted Bank output, evidence, support, and source-attribution contracts.")
        elif stage == "stage3_serve":
            common += ("\n\nSTAGE 3 SERVING CONTRACT\n" +
                ("Use registered signatures; proposed IR operators are atomic calls and stored macros remain usable. Preserve Bank output, evidence, support, and source attribution."
                 if paper_profile else
                 "Use registered operator signatures. Newly synthesized typed IR operators are atomic serving operators; their private implementation is not part of this call. Older stored macros remain reusable as registered operators. Preserve trusted Bank output, evidence, support, and source-attribution contracts."))
        else:
            common += ("\n\nDAG DATA WIRING REMINDER\n"
                "Use registered input and output types exactly. Native public_query metadata.rows_data contains plain CSV row dictionaries, "
                "not AggregateRow envelopes. No generated source is written in this stage.")
    if stage == "select":
        active_keys = {f"{row['name']}@{row['version']}" for row in registry.signatures()}
        allowed_paper_books = set(policy.get("_paper_case_available_books", ()))
        plays = [{"name": name, "graph_sha256": item["graph_sha256"],
                  "document": _plain(item["document"])}
                 for name, item in sorted(snapshot.playbooks.items())
                 if _playbook_operator_keys(item["document"]) <= active_keys and
                    (not paper_profile or name in allowed_paper_books)]
        return (common + "\n\nSTAGE 1: select and instantiate one stored playbook. Return exactly "
            "{playbook:string,runtime_overrides:object,node_parameters:object,node_bindings:object}. "
            "You may choose explicit values only for parameters declared by the stored nodes, or bind a "
            "declared parameter to an existing runtime field. For one parameter choose either an explicit value or a binding, not both; "
            "an explicit override replaces a stored binding, and a binding replaces a stored explicit value. The node IDs, operators, versions, input edges, "
            "variadic positions, and return node must remain exactly as stored. No new node or operator. "
            "Use empty objects when no override is needed.\n\nCURRENT STORED PLAYBOOKS\n" +
            json.dumps(plays, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    if stage == "compose":
        adaptation_guidance = (" Use a soft preference for a meaningfully different computation that uses supported public inputs, "
            "but a different modality is not required and a ranking change is not required. Do not force a new "
            "family, boost a score, or claim a fault from the adaptation record. Return one paper Stage 2 "
            "adaptation record using schema_version='e4-paper-stage2-adaptation/1': adaptation_record has exactly "
            "schema_version='e4-paper-adaptation-record/1', tested_plan_refs (copy one to three dag_sha256 strings verbatim "
            "from PRIOR ANALYSIS PLANS; include each selected digest once, never the stage or whole plan object), public_observations (one to four bounded facts from the supplied "
            "public outputs/coverage), unexplained_limit, hypothesis_change, statistic, source_scope, "
            "window_relationship, and typed_evidence_outputs (one to eight node IDs whose registered output type "
            "is ScoredCandidates). State what the earlier supported plan tested, what remains unexplained, and how "
            "the new statistic/source/window relation differs. Do not include truth labels, guessed root IDs, or "
            "unsupported modalities. Exact expanded-DAG repeats are rejected; changing node IDs alone does not count. "
            "If no other modality is available, consider a distinct supported statistic/source/window relation; "
            "do not force a different final ranking.") if paper_profile else ""
        schema = ("{schema_version:'e4-paper-stage2-adaptation/1',adaptation_record:{schema_version:'e4-paper-adaptation-record/1',tested_plan_refs:[...],public_observations:[...],unexplained_limit:string,hypothesis_change:string,statistic:string,source_scope:string,window_relationship:string,typed_evidence_outputs:[node_id,...]},dag:{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}},runtime_overrides:object}. "
            if paper_profile else
            "{dag:{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}},runtime_overrides:object}. ")
        return (common + "\n\nREGISTERED COMPOSITE PROGRAMS\n" +
            json.dumps(composite_catalog, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            ("\n\nTRUSTED PAPER INPUT CONTRACTS\n" +
                json.dumps(_paper_prompt_input_capabilities(registry,
                    policy.get("_paper_case_capabilities", {})), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")) if paper_profile else
             "\n\nTRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS\n" +
                json.dumps(trusted_aggregate_semantics(registry), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":"))) +
            "\n\nSTAGE 2: compose a distinct analysis from the current registered operator catalog only. Inspect prior plans, registered descriptions, and composite programs. Return exactly " + schema +
            "Use one case.telemetry input and return the trusted " + terminal_operator + " combiner. "
            "Keep the fully expanded graph within max_nodes/dag_depth from protected runtime. The controller expands every composite before execution." +
            adaptation_guidance + "\n\n" + _dag_json_shape_guidance(terminal_operator=terminal_operator))
    if stage == "evolve":
        if paper_code_profile:
            from .m4_generated import typed_operator_code_contract_schema
            source_specs = generated_specs_from_snapshot(snapshot)
            prior_bodies = []
            for old in source_specs:
                body = old.get("source") if isinstance(old.get("source"), str) else old.get("program")
                if not isinstance(body, (str, Mapping)):
                    continue
                prior_bodies.append({"name": old["name"], "version": old["version"],
                    "authoring_profile": old.get("authoring_profile"),
                    "source_sha256": old.get("source_sha256"),
                    "program_sha256": old.get("program_sha256"), "body": _plain(body)})
            prior_body_text = json.dumps(prior_bodies, ensure_ascii=False,
                sort_keys=True, separators=(",", ":"), allow_nan=False)
            prior_plans = _bounded_plan_history([{**row["plan_summary"], "stage": row["stage"]}
                for row in history if isinstance(row.get("plan_summary"), Mapping)
                and isinstance(row.get("stage"), str)])
            schema = typed_operator_code_contract_schema()
            return (common +
                "\n\nPRIOR ANALYSIS PLANS (copy exact {stage, dag_sha256} references into the gap record)\n" +
                json.dumps(prior_plans, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
                "\n\nRETAINED OPERATOR BODIES (complete lossless body for every retained generated source/program; comparison only; the model client tokenizes the complete rendered message before dispatch and rejects requests above its context budget without truncation)\n" +
                prior_body_text +
                "\n\nTRUSTED REGISTERED OPERATOR CONTRACTS\n" + registered_signature_section +
                "\n\nTRUSTED PRODUCER CAPABILITY CONTRACTS\n" +
                json.dumps(input_producer_capability_schema(registry,
                    operator_code_profile=True), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")) +
                "\n\nTYPED PYTHON OPERATOR AUTHORING CONTRACT\n" +
                json.dumps(schema, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
                "\n\nSTAGE 3 EVOLVE: author genuinely new Python `operator(inputs, params, runtime)` functions using only the published typed records and bounded neutral-data API. The source implements its own statistic/relationship; the API offers generic typed queries/grouping/summaries, not fixed analysis kernels. Return exactly {gap_record:{schema_version:'e4-capability-gaps/1',prior_plans:[...],capabilities:[...]},operators:[...]}. Each operator object follows the complete typed Python spec schema, including exact input_ports, params, output_type, authoring_profile='typed-operator-code-v1', per-port input_requirements, and source. For ScoredCandidates use the published typed output constructors and generic worker scorer `typed-operator-code-v1`; the host stamps registered identity. Propose at most three operators. Each capability maps one-to-one to its operator and describes the observed gap and computational difference. Cite one or more exact prior plan references when available. Inspect every retained source/program body above and do not repeat an identical canonical Python AST under a new name; this structural novelty check is not mathematical semantic equivalence. Do not output a DAG, runtime overrides, diagnosis, answer, or ranking. No filesystem/network IO or dynamic execution; imports must be statically bound to the allowlisted modules. Do not decode raw metadata/wire formats, embed case/entity literals, or refer to host-private paths. A typed input declaration must be satisfiable by actual registered upstream producers; no impossible or invented capability claims. If no useful source extension exists, return capabilities:[] and operators:[]; never force a duplicate.\n")
        from .m4_operator_ir import (trusted_runtime_context,
            typed_operator_ir_schema)
        if paper_profile:
            from .m4_operator_ir import (model_facing_operator_ir_v2_schema,
                model_facing_trusted_runtime_context_v2, trusted_runtime_context_v2)
            ir_schema = model_facing_operator_ir_v2_schema()
            if v2_selector_status_rows:
                selector_catalog = ir_schema.get("metric_selector_catalog")
                if not isinstance(selector_catalog, list):
                    raise M4ControllerError("paper_selector_catalog_schema",
                        "V2 model-facing schema has no selector catalog", static=True)
                statuses_by_ref = {row["selector_ref"]: row for row in v2_selector_status_rows}
                catalog_refs = {row.get("selector_ref") for row in selector_catalog
                    if isinstance(row, Mapping)}
                if catalog_refs != set(statuses_by_ref):
                    raise M4ControllerError("paper_selector_catalog_mismatch",
                        "V2 schema choices and case selector status catalog must match exactly", static=True,
                        details={"schema_selector_count": len(catalog_refs),
                            "case_selector_count": len(statuses_by_ref)})
                schema_metric_policy_sha = ir_schema.get("provenance", {}).get(
                    "component_metric_policy_sha256")
                summary_case_sha = displayed_capability_summary.get(
                    "provenance", {}).get("telemetry_data_sha256")
                if (v2_selector_policy_sha256 is not None and
                        schema_metric_policy_sha != v2_selector_policy_sha256):
                    raise M4ControllerError("paper_selector_policy_provenance",
                        "V2 selector policy digest differs from the model-facing schema pin", static=True)
                if (v2_selector_case_sha256 is not None and
                        summary_case_sha != v2_selector_case_sha256):
                    raise M4ControllerError("paper_selector_case_provenance",
                        "V2 selector case digest differs from the trusted capability summary", static=True)
                from .m4_prompt_support import intern_case_support_profiles
                catalog_with_resolution = []
                for row in selector_catalog:
                    catalog_with_resolution.append({**row, "case_resolution": {
                        key: _plain(statuses_by_ref[row["selector_ref"]][key])
                        for key in ("status", "match_count", "semantic_profile_refs",
                            "reference_support_histogram", "alert_support_histogram")}})
                projected_catalog, resolution_profiles = intern_case_support_profiles(
                    catalog_with_resolution, ref_key="selector_ref", support_key="case_resolution")
                ir_schema["metric_selector_catalog"] = projected_catalog
                ir_schema["selector_case_resolution_profiles"] = {
                    "schema_version": resolution_profiles["schema_version"],
                    "support_profile_ref_field": "support_profile_ref",
                    "profile_payload_field": "case_resolution",
                    "profiles": resolution_profiles["profiles"]}
                ir_schema["selector_case_resolution_reference_rule"] = (
                    "Each metric_selector_catalog row's support_profile_ref resolves in selector_case_resolution_profiles.profiles; "
                    "the case_resolution object contains exact status, match count, semantic profile refs, and per-window support histograms.")
                ir_schema["selector_semantic_profiles"] = _paper_semantic_profile_prompt_table(
                    v2_selector_semantic_profiles)
                ir_schema["selector_resolution_provenance"] = {
                    "bank_metric_policy_sha256_ref": "provenance.component_metric_policy_sha256",
                    "case_data_sha256_ref": "TRUSTED TYPED-IR V2 DATA/SUPPORT CAPABILITY SUMMARY.provenance.telemetry_data_sha256",
                    "exact_field_match_count": v2_selector_match_count,
                    "unique_exact_field_count": v2_selector_unique_field_count,
                    "field_selector_mapping_sha256": v2_selector_mapping_sha256}
            trusted_ir_context = model_facing_trusted_runtime_context_v2(
                trusted_runtime_context_v2())
            trusted_ir_context["metric_selector_resolutions_ref"] = (
                "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog[*].support_profile_ref "
                "-> selector_case_resolution_profiles.profiles[*].case_resolution")
        else:
            ir_schema = typed_operator_ir_schema()
            trusted_ir_context = trusted_runtime_context()
        if paper_profile:
            ir_node_syntax = (
                "IR NODE SYNTAX: bare V2 kernel name and catalog version; follow schema authoring_rules for refs/params/bindings/windows. "
                "Select native fields only by exact `selector_ref` from `metric_selector_catalog` (the paper.bank_component_metric_fields enum is for registered operators). "
                "Host matching is exact; never invent, prefix-match, index, or choose among multiple matches. "
                "Normalize eligible nonnegative scores per scorer, dedupe exact validated fingerprints, then average trusted groups by bounded weights; generated IR shares one capped group and authored family labels add no votes.\n\n")
        else:
            # Keep the legacy V1 prompt wording and examples stable.
            ir_node_syntax = (
                'IR NODE SYNTAX: `kernel` is the bare kernel name without `@version`; set `version` to the catalog version '
                '(for example `kernel:\'series.primary_kpi\',version:\'1\'`; catalog key `series.primary_kpi@1` is only its identity). '
                'Put ordinary parameters in `params`. For every parameter with a `runtime_field`, omit it from `params` and bind that exact field in `bindings`. '
                'Example primary KPI node: {"id":"kpi","kernel":"series.primary_kpi","version":"1","inputs":{"reference":"$input.reference","alert":"$input.alert"},"params":{},"bindings":{}}. '
                'Example correlation node: {"id":"corr","kernel":"series.correlate","version":"1","inputs":{"pairs":"aligned"},"params":{"method":"spearman"},"bindings":{"minimum_pairs":"runtime.minimum_support"}}.\n\n')
        prior_ir = [{"name": spec["name"], "version": spec["version"],
            "family": spec["family"], "authoring_profile": spec.get("authoring_profile"),
            "program_sha256": spec["program_sha256"],
            "program": _plain(spec["program"])}
            for spec in generated_specs_from_snapshot(snapshot)
            if spec.get("authoring_profile") in {"typed-operator-ir-v1", "typed-operator-ir-v2"}]
        prior_plans = _bounded_plan_history([{**row["plan_summary"], "stage": row["stage"]}
            for row in history if isinstance(row.get("plan_summary"), Mapping)
            and isinstance(row.get("stage"), str)])
        if paper_profile:
            # This points to the identical complete bounded plans already
            # rendered above and avoids serializing every DAG twice.
            prior_plans_reference_text = (
                "Use the exact {stage, dag_sha256} records in PRIOR ANALYSIS PLANS above for gap_record.prior_plans. "
                "That section is the complete plan-reference set for this call; each full node/input/parameter/binding row appears there once.")
        else:
            prior_plans_reference_text = json.dumps(prior_plans, ensure_ascii=False,
                sort_keys=True, separators=(",", ":"))
        if paper_profile:
            prior_ir_text = ("\n\nPRIOR ATOMIC IR BODIES (lossless columnar form; use decode_rule and node_field_order exactly)\n" +
                json.dumps(_compact_paper_prior_ir_bodies(prior_ir), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")) +
                "\nThis representation is comparison-only and is not an authoring format. Every new program must use the normal V2 node-object syntax declared in the schema below.")
        else:
            prior_ir_text = ("\n\nPRIOR ATOMIC IR BODIES (for comparison; do not resubmit a canonical body)\n" +
                json.dumps(prior_ir, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        if paper_profile:
            evolve_instructions = (
                "\n\nSTAGE 3 EVOLVE: return exactly {\"gap_record\":{\"schema_version\":\"e4-capability-gaps/1\",\"prior_plans\":[...],\"capabilities\":[...]},\"operators\":[...]}; these are sibling fields. Set exactly one bounded capability string per operator describing the missing computation and how it differs from prior analysis. Follow `spec_fields` and `fixed_host_abi`; do not emit outer `inputs`, `input_ports`, or `input_requirements`. Cite up to three exact prior plan references from the available history.\n\n")
        else:
            evolve_instructions = (
                "\n\nSTAGE 3 EVOLVE PHASE: identify a specific failure-grounded computational gap. This call does not diagnose the incident. Return exactly one top-level object with exactly two sibling fields: {\"gap_record\":{\"schema_version\":\"e4-capability-gaps/1\",\"prior_plans\":[...],\"capabilities\":[...]},\"operators\":[...]}. `operators` MUST be a sibling of `gap_record`, never a field inside it. Each operator spec must contain exactly the model-authored fields listed in `spec_fields`. The host supplies the fixed signature shown under `fixed_host_abi`; do not emit outer-spec `inputs`, `input_ports`, or `input_requirements`. Include one capability per proposed operator. State the missing computation/input/coverage and how the actual statistic differs from prior analysis. Do not assert diagnostic correctness. Do not repeat a canonical prior IR body under a new name. If no useful extension addresses a specific observed limitation, return capabilities:[] and operators:[]; do not force a duplicate. Include up to three real prior plan references and at least one when available. Closest-operator references must identify exact registered names/versions. Use bounded strings of at most 768 characters and at most eight required_inputs. The serving call uses each proposed new operator as an ordinary atomic registered operator. Do not return a serving DAG, runtime_overrides, answer, or ranking.\n\n")
        fresh_v2_example = ""
        if paper_profile:
            has_prior_v2 = any(spec.get("authoring_profile") == "typed-operator-ir-v2"
                for spec in prior_ir)
            if not has_prior_v2:
                example_refs = [{"stage": row["stage"], "dag_sha256": row["dag_sha256"]}
                    for row in prior_plans if isinstance(row.get("stage"), str)
                    and isinstance(row.get("dag_sha256"), str)][-3:]
                response_example = {"gap_record": {
                    "schema_version": "e4-capability-gaps/1",
                    "prior_plans": example_refs,
                    "capabilities": ["Describe one missing computation and how it differs from prior analysis."]},
                    "operators": [_PAPER_V2_AUTHORING_EXAMPLE]}
                fresh_v2_example = (
                    "\n\nCOMPLETE V2 STAGE-3 RESPONSE EXAMPLE (valid illustration only; author a genuinely new useful program and do not copy this computation): " +
                    json.dumps(response_example, ensure_ascii=False,
                        sort_keys=True, separators=(",", ":")))
            else:
                fresh_v2_example = (
                    "\n\nGAP DRAFT SHAPE: `capabilities` is an array of bounded strings, exactly one per proposed operator; "
                    "each string maps by order. Copy exact prior plan references from PRIOR ANALYSIS PLANS. "
                    "Use the complete V2 operator schema and the lossless prior IR bodies above for comparison.")
        return (common + prior_ir_text +
            evolve_instructions + ir_node_syntax + fresh_v2_example + "\n\nPRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG\n" +
            json.dumps(ir_schema, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\n\nTRUSTED TYPED-IR RUNTIME CONTEXT\n" +
            json.dumps(_plain(trusted_ir_context), ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\n\nPRIOR SERVED PLANS AVAILABLE FOR REFERENCES\n" +
            (prior_plans_reference_text if paper_profile else prior_plans_reference_text) +
            "\n\nTRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS AND TRUSTED INPUT PRODUCER CAPABILITIES\n" +
            json.dumps(_without_paper_raw_query_capability(
                           _paper_evolve_typed_input_capabilities(registry)) if paper_profile else
                       _typed_ir_input_capabilities(registry), ensure_ascii=False,
                       sort_keys=True, separators=(",", ":")))
    if stage == "stage3_serve":
        no_extension = (isinstance(gap_record, Mapping) and
                        gap_record.get("capabilities") == [])
        if paper_code_profile:
            use_guidance = ("No useful extension was validated. Diagnose using the current registered catalog and prior plans; you may use any registered operators or composites and must not invent or reference a proposed operator. The stage-3 serving call still gets its ordinary diagnosis opportunity." if no_extension else
                            "Use the validated gap record and every proposed new operator as registered atomic calls in the serving DAG.")
        else:
            # Preserve the legacy V1/V2 serving prompt exactly for old snapshots.
            use_guidance = ("No useful extension was validated. Diagnose using the current registered catalog and prior plans; you may use any registered operators or composites and must not invent or reference a proposed composite. The stage-3 serving call still gets its ordinary diagnosis opportunity." if no_extension else
                            "Use the validated gap record and every proposed composite in the logical DAG.")
        coverage_sentence = ("If query coverage is partial, no-match is not full-window absence."
            if not paper_profile else
            "If typed input coverage is partial or a capability is unsupported, no-match is not evidence of full-window absence.")
        if paper_profile:
            serve_instructions = ("STAGE 3 SERVE: diagnose root cause; retain at most five leads with the fault in the first three and distinguish fault from symptom. " +
                use_guidance + (" Use registered signatures; proposed Python operators are atomic calls, and existing registered operators/macros remain usable. Do not author source/specs or revise the gap. "
                    if paper_code_profile else
                    " Use registered signatures; proposed IR operators are atomic calls, stored macros remain usable. Do not author specs or revise the gap. ") +
                coverage_sentence + " Preserve evidence eligibility, support, and source attribution. Return exactly {dag:{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}},runtime_overrides:object}; node maps are objects and the terminal is the trusted " + terminal_operator + " combiner.")
        else:
            serve_instructions = ("STAGE 3 SERVE PHASE: diagnose the incident root cause and retain up to five leads; success requires the fault component in the first three. Distinguish faults from symptoms. " +
                use_guidance + " Use registered signatures. Each newly synthesized typed IR operator is one atomic registered call; do not redefine or expand it. Existing stored macros may also be used. Do not return new specs or change the gap record. " +
                coverage_sentence + " Preserve typed evidence, support, and source attribution. Return exactly {dag:{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}},runtime_overrides:object}. Every node has object maps for inputs, params, and bindings. The terminal return must be the trusted " + terminal_operator + " combiner.")
        return (common + "\n\nVALIDATED CAPABILITY GAP RECORD (immutable input; use it as supplied)\n" +
            json.dumps(_plain(gap_record or {}), ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            "\n\nREGISTERED COMPOSITE PROGRAMS\n" +
            json.dumps(composite_catalog, ensure_ascii=False, sort_keys=True, separators=(",", ":")) +
            ("\n\nTRUSTED PAPER INPUT CONTRACTS\n" +
                json.dumps(_paper_prompt_input_capabilities(registry,
                    policy.get("_paper_case_capabilities", {})), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")) if paper_profile else
             "\n\nTRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS\n" +
                json.dumps(trusted_aggregate_semantics(registry), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")) +
             "\n\nTRUSTED INPUT PRODUCER CAPABILITIES\n" +
                json.dumps(input_producer_capability_schema(registry), ensure_ascii=False,
                    sort_keys=True, separators=(",", ":"))) +
            "\n\n" + serve_instructions + "\n\n" +
            _dag_json_shape_guidance(terminal_operator=terminal_operator) + "\n\nANSWER OUTPUT REQUIREMENTS\n"
            "Every positive-ranked lead requires supported source-tagged evidence with explicit eligibility and exact component attribution; category must remain null. "
            "Use registered producer semantics; a bounded query page does not establish full-window absence.")
    if stage == "synthesize":
        return _stage_prompt("stage3_serve", digest=digest, registry=registry,
            snapshot=snapshot, runtime=runtime, history=history, policy=policy,
            query_budget=query_budget, include_case_digest=include_case_digest,
            include_history=include_history, gap_record=gap_record,
            composite_operator_specs=composite_operator_specs)
    raise ValueError(f"unknown M4 stage {stage}")


def _safe_repair_artifact(previous: str) -> str:
    try:
        previous_doc, _ = parse_strict_json_object(previous)

        def omit_executable_fields(value: Any) -> Any:
            if isinstance(value, Mapping):
                return {key: ("[rejected executable source omitted]" if key in {"source", "python_source"}
                    else omit_executable_fields(child)) for key, child in value.items()}
            if isinstance(value, list):
                return [omit_executable_fields(child) for child in value]
            return value

        safe_previous = json.dumps(omit_executable_fields(previous_doc), sort_keys=True,
            ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except Exception:
        safe_previous = "[unparseable previous response omitted]"
    return safe_previous


def _static_repair_prompt(base_prompt: str, previous: str, error: Mapping[str, Any]) -> str:
    diagnostic = {key: error[key] for key in ("code", "message", "node_id", "details") if key in error}
    safe_previous = _safe_repair_artifact(previous)
    return (base_prompt + "\n\nSTATIC CONTRACT ERROR ONLY. Repair the JSON schema, declared operator contract, "
        "or typed DAG topology. No execution, ranking, or feedback details are provided.\n" +
        json.dumps(diagnostic, sort_keys=True, ensure_ascii=False, separators=(",", ":")) +
        "\n\nPrevious artifact (executable source fields are omitted):\n" + safe_previous +
        "\n\nReturn the corrected JSON object only.")


def _normalize_overrides(response: Mapping[str, Any], case: BankCase,
                         root: Path, policy: Mapping[str, Any]) -> RuntimeContext:
    overrides = response.get("runtime_overrides")
    if not isinstance(overrides, Mapping):
        raise M4ControllerError("runtime_override_schema", "runtime_overrides must be an object", static=True)
    return build_m4_runtime(case.alert, overrides=overrides, root=root)


def _call_feedback(feedback: Any, case_id: str, answer: RankedAnswer) -> dict[str, bool]:
    callback = feedback.evaluate if hasattr(feedback, "evaluate") else feedback
    if not callable(callback):
        raise M4ControllerError("feedback_unavailable", "trusted binary feedback callback is required")
    result = callback(case_id, answer)
    if type(result) is not dict or set(result) != {"accepted"} or type(result["accepted"]) is not bool:
        raise M4ControllerError("feedback_contract", "feedback callback must return exactly {'accepted': bool}")
    return result


def validate_bank_answer(answer: RankedAnswer, case: BankCase, *, maximum_leads: int = 5) -> None:
    """Enforce exact case-local observed component membership and eligible evidence."""
    if not isinstance(answer, RankedAnswer) or len(answer.leads) > maximum_leads:
        raise M4ControllerError("answer_contract", "ranked output violates the frozen answer shape")
    observed = {identifier.strip().casefold(): identifier
                for identifier, metadata in case.telemetry.deployment_catalog.items()
                if isinstance(metadata, Mapping) and metadata.get("role", "component") == "component"}
    seen: set[str] = set()
    prior_score = math.inf
    allowed_sources = {
        "bank/metric_app.csv", "bank/metric_container.csv", "bank/trace_span.csv",
        "bank/log/log_service.csv", "bank/metric/metric_app.csv",
        "bank/metric/metric_container.csv", "bank/trace/trace_span.csv",
        "bank/log/log_service.csv",
    }
    for index, candidate in enumerate(answer.leads):
        identifier = candidate.entity.identifier.strip()
        canonical = identifier.casefold()
        if candidate.entity.role != "component" or canonical not in observed:
            raise M4ControllerError("answer_unobserved_component",
                f"ranked lead {index + 1} is not an exact observed Bank component")
        if canonical in seen:
            raise M4ControllerError("answer_duplicate_component", "ranked leads repeat a canonical component")
        seen.add(canonical)
        if candidate.score > prior_score:
            raise M4ControllerError("answer_order", "ranked leads are not in nonincreasing score order")
        prior_score = candidate.score
        eligible = [item for item in candidate.evidence
                    if item.details.get("ranking_eligible") is True and item.support > 0
                    and item.source in allowed_sources
                    and any(entity.role == "component" and
                            entity.identifier.strip().casefold() == canonical
                            for entity in item.contributing_entities)]
        if not eligible:
            raise M4ControllerError("answer_missing_evidence",
                f"ranked lead {index + 1} has no supported, source-tagged eligible evidence")


def _model_cost(calls: list[Mapping[str, Any]]) -> dict[str, Any]:
    prompt_tokens = 0
    completion_tokens = 0
    prompt_known = True
    completion_known = True
    for row in calls:
        usage = row.get("usage", {})
        prompt = usage.get("prompt_tokens", usage.get("input_tokens")) if isinstance(usage, Mapping) else None
        completion = usage.get("completion_tokens", usage.get("output_tokens")) if isinstance(usage, Mapping) else None
        if type(prompt) is int:
            prompt_tokens += prompt
        else:
            prompt_known = False
        if type(completion) is int:
            completion_tokens += completion
        else:
            completion_known = False
    phases: dict[str, dict[str, Any]] = {}
    charged_tokens = 0
    for row in calls:
        phase = str(row.get("m4_phase", "unphased"))
        item = phases.setdefault(phase, {"model_call_count": 0, "prompt_tokens": 0,
            "completion_tokens": 0, "completion_tokens_known": True,
            "charged_generated_tokens": 0, "elapsed_seconds": 0.0})
        usage = row.get("usage", {})
        prompt = usage.get("prompt_tokens", usage.get("input_tokens")) if isinstance(usage, Mapping) else None
        completion = usage.get("completion_tokens", usage.get("output_tokens")) if isinstance(usage, Mapping) else None
        item["model_call_count"] += 1
        if type(prompt) is int:
            item["prompt_tokens"] += prompt
        if type(completion) is int:
            item["completion_tokens"] += completion
            charge = completion
        else:
            item["completion_tokens_known"] = False
            charge = row.get("m4_requested_generation_tokens")
        if type(charge) is not int:
            charge = row.get("m4_requested_generation_tokens", 0)
        item["charged_generated_tokens"] += int(charge or 0)
        charged_tokens += int(charge or 0)
        item["elapsed_seconds"] += float(row.get("elapsed_seconds", 0.0))
    return {"model_call_count": len(calls),
        "prompt_tokens": prompt_tokens if prompt_known else None,
        "completion_tokens": completion_tokens if completion_known else None,
        "model_elapsed_seconds": sum(float(row.get("elapsed_seconds", 0.0)) for row in calls),
        "generation_elapsed_seconds": sum(float(row.get("generation_elapsed_seconds", 0.0)) for row in calls),
        "static_repair_calls": sum(1 for row in calls if row.get("m4_static_repair") is True),
        "charged_generated_tokens": charged_tokens,
        "phase_cost": phases}


def _generation_allowance(calls: list[Mapping[str, Any]], *, model_call_limit: int,
                          token_budget: int, per_call_limit: int) -> int:
    """Clamp the next generation request against hard case-wide call/token ceilings."""
    if len(calls) >= model_call_limit:
        return 0
    charged = 0
    for row in calls:
        usage = row.get("usage", {})
        actual = (usage.get("completion_tokens", usage.get("output_tokens"))
                  if isinstance(usage, Mapping) else None)
        charge = actual if type(actual) is int and actual >= 0 else row.get("m4_requested_generation_tokens")
        if type(charge) is not int or charge < 0:
            charge = per_call_limit
        charged += charge
    return max(0, min(per_call_limit, token_budget - charged))


def _bounded_answer_history(answer: RankedAnswer, *, lead_limit: int = 5,
                            evidence_per_lead: int = 1, max_bytes: int = 8192) -> dict[str, Any]:
    """Keep useful escalation context bounded while retaining a digest of full output."""
    raw = canonical_json(answer)
    leads = []
    for rank, candidate in enumerate(answer.leads[:lead_limit], start=1):
        evidence = []
        eligible_first = sorted(enumerate(candidate.evidence), key=lambda pair: (
            pair[1].details.get("ranking_eligible") is not True, pair[0]))
        for _index, item in eligible_first[:evidence_per_lead]:
            details = item.details
            evidence.append({"source": item.source, "window": item.window.name,
                "statistic": item.statistic, "value": item.value, "support": item.support,
                "details": {key: details[key] for key in (
                    "metric_key", "metric_kind", "normalized_effect", "p_value",
                    "direction", "unit", "support_unit", "ranking_eligible", "summary_statistic")
                    if key in details}})
        leads.append({"rank": rank, "component_id": candidate.entity.identifier,
            "score": candidate.score, "category": candidate.category,
            "analysis_family": candidate.analysis_family,
            "evidence_count": len(candidate.evidence), "evidence_excerpt": evidence,
            "evidence_omitted": max(0, len(candidate.evidence) - len(evidence)),
            "eligible_evidence_count": sum(item.details.get("ranking_eligible") is True
                                            for item in candidate.evidence)})
    output = {"lead_count": len(answer.leads), "leads": leads,
        "leads_omitted": max(0, len(answer.leads) - len(leads)),
        "full_answer_sha256": hashlib.sha256(raw).hexdigest(),
        "full_answer_bytes": len(raw), "history_is_truncated": True}
    while len(json.dumps(output, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")) > max_bytes and leads:
        removed = leads.pop()
        output["leads_omitted"] += 1
        output["leads"] = leads
        if leads:
            last = leads[-1]
            if last["evidence_excerpt"]:
                last["evidence_omitted"] += len(last["evidence_excerpt"])
                last["evidence_excerpt"] = []
    return output


def _bounded_plan_history(plans: list[Mapping[str, Any]], *, max_bytes: int = 6144) -> list[dict[str, Any]]:
    """Compact deterministic structural summaries; source and data rows are never included."""
    if max_bytes < 272:
        raise ValueError("plan history byte cap must be at least 272")
    rows = json.loads(json.dumps([_plain(plan) for plan in plans[-2:]], ensure_ascii=False,
                                 allow_nan=False))
    def size() -> int:
        return len(json.dumps(rows, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode("utf-8"))
    while rows and size() > max_bytes:
        last = rows[-1]
        nodes = last.get("nodes")
        if isinstance(nodes, list) and nodes:
            nodes.pop()
            last["nodes_omitted"] = int(last.get("nodes_omitted", 0)) + 1
        elif len(rows) > 1:
            rows.pop(0)
        else:
            if "runtime_overrides" in last:
                last.pop("runtime_overrides")
                last["runtime_overrides_omitted"] = True
            if "nodes" in last:
                last["nodes_omitted"] = int(last.get("nodes_omitted", 0)) + len(last.pop("nodes"))
            if "return" in last:
                last.pop("return")
                last["return_omitted"] = True
            last["summary_truncated"] = True
            if size() > max_bytes:
                dag_hash = last.get("dag_sha256", "")
                summary_hash = last.get("full_summary_sha256", "")
                stage_name = last.get("stage")
                node_omissions = last.get("nodes_omitted", 0)
                last.clear()
                last.update({"dag_sha256": dag_hash, "full_summary_sha256": summary_hash,
                    "nodes_omitted": node_omissions, "runtime_overrides_omitted": True,
                    "return_omitted": True, "summary_truncated": True})
                if stage_name is not None:
                    last["stage"] = stage_name
            if size() > max_bytes:
                raise ValueError("plan history byte cap is too small for omission summary")
            break
    return rows


def _plan_summary(document: Mapping[str, Any], registry: OperatorRegistry,
                  runtime: RuntimeContext, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
    nodes = []
    for node in document.get("nodes", []):
        spec = signatures.get((node.get("operator"), node.get("version")), {})
        nodes.append({"id": node.get("id"), "operator": node.get("operator"),
            "version": node.get("version"),
            "inputs": _plain(node.get("inputs", {})), "params": _plain(node.get("params", {})),
            "bindings": _plain(node.get("bindings", {}))})
    plan = {"dag_sha256": _canonical_hash(document), "nodes": nodes,
        "return": _plain(document.get("return", {})),
        "runtime_overrides": _plain(overrides or {})}
    plan["full_summary_sha256"] = _canonical_hash(plan)
    return plan


def _paper_public_analysis_evidence(answer: RankedAnswer | None, execution: Mapping[str, Any],
                                    capability_checks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Expose bounded public execution coverage/rank/group evidence, never evaluator truth."""
    leads = []
    groups: dict[str, dict[str, int]] = {}
    if answer is not None:
        for rank, candidate in enumerate(answer.leads[:5], start=1):
            family = candidate.analysis_family or "unspecified"
            row = groups.setdefault(family, {"lead_count": 0, "eligible_evidence_count": 0})
            row["lead_count"] += 1
            eligible = sum(item.details.get("ranking_eligible") is True for item in candidate.evidence)
            row["eligible_evidence_count"] += eligible
            leads.append({"rank": rank, "component_id": candidate.entity.identifier,
                "score": candidate.score, "scorer_group": family,
                "eligible_evidence_count": eligible,
                "evidence_count": len(candidate.evidence)})
    coverage_by_operator: dict[str, dict[str, Any]] = {}
    for record in execution.get("records", []) if isinstance(execution, Mapping) else []:
        if not isinstance(record, Mapping):
            continue
        error = record.get("error")
        key = f"{record.get('operator')}@{record.get('version')}"
        summary = coverage_by_operator.setdefault(key, {"success_nodes": 0,
            "failed_nodes": 0, "rows_processed_total": 0, "error_codes": []})
        summary["success_nodes" if record.get("status") == "success" else "failed_nodes"] += 1
        rows_processed = record.get("rows_processed")
        if type(rows_processed) is int and rows_processed >= 0:
            summary["rows_processed_total"] += rows_processed
        if isinstance(error, Mapping) and isinstance(error.get("code"), str):
            summary["error_codes"].append(error["code"])
    for summary in coverage_by_operator.values():
        summary["error_codes"] = sorted(set(summary["error_codes"]))
    checks = [{key: _plain(check[key]) for key in ("node_id", "port", "status", "code", "support")
        if key in check} for check in capability_checks]
    return {"schema_version": "e4-paper-public-analysis-evidence/1",
        "ranked_leads": leads,
        "effective_scorer_groups": groups,
        "operator_coverage_by_operator": coverage_by_operator,
        "input_capability_checks": checks}


def _paper_adaptation_evidence_context(history: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Summarize only prior public plan refs, rankings, coverage and scorer groups."""
    output = []
    for index, row in enumerate(history[-MAX_PROMPT_HISTORY:], start=1):
        if not isinstance(row, Mapping):
            continue
        plan = row.get("plan_summary")
        evidence = row.get("public_analysis_evidence")
        if not isinstance(plan, Mapping) and not isinstance(evidence, Mapping):
            continue
        item: dict[str, Any] = {"history_ref": f"h{index}",
            "stage": row.get("stage"), "status": row.get("status")}
        if isinstance(plan, Mapping) and isinstance(plan.get("dag_sha256"), str):
            item["plan_ref"] = plan["dag_sha256"]
        if isinstance(evidence, Mapping):
            public_view = _plain(evidence)
            ranked_leads = public_view.pop("ranked_leads", None)
            item["public_analysis_evidence"] = public_view
            if isinstance(ranked_leads, list):
                item["ranked_answer_ref"] = {"history_ref": item["history_ref"],
                    "field": "ranked_answer",
                    "semantics": "the complete bounded ranked-answer excerpt appears once in PRIOR ATTEMPT HISTORY"}
        adaptation_record = row.get("adaptation_record")
        if isinstance(adaptation_record, Mapping):
            item["adaptation_record"] = _plain(adaptation_record)
            if isinstance(row.get("adaptation_record_origin"), str):
                item["adaptation_record_origin"] = row["adaptation_record_origin"]
        static_errors = row.get("static_errors")
        if isinstance(static_errors, list):
            item["static_errors"] = [{key: _plain(error[key]) for key in ("code", "message")
                if key in error} for error in static_errors if isinstance(error, Mapping)]
        output.append(item)
    return output


def _paper_v2_capability_prompt_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    """Project the full host report into a compact, exact-support prompt view."""
    full_host_report_sha256 = _canonical_hash(report)
    def histogram(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in rows:
            value = row.get(key)
            label = "null" if value is None else str(value)
            counts[label] = counts.get(label, 0) + 1
        return dict(sorted(counts.items()))

    primary = report.get("primary_kpi", {})
    primary_detail = primary.get("details", {}) if isinstance(primary, Mapping) else {}
    primary_summary = {key: _plain(primary[key]) for key in (
        "computation", "data_state", "computability_state", "ranking_state",
        "selected_identity", "selection_reason", "reasons", "upstream_abstention")
        if key in primary}
    if isinstance(primary_detail, Mapping):
        primary_summary["selection_support"] = {key: _plain(primary_detail[key]) for key in (
            "minimum_timestamps_per_window", "meets_primary_kpi_policy",
            "best_observed_support_per_window", "observed_kpi_candidate_count",
            "omitted_candidate_count", "selection_policy_sha256")
            if key in primary_detail}

    computation_summaries = []
    for computation in report.get("computations", []):
        if not isinstance(computation, Mapping):
            continue
        details = computation.get("details", {})
        if not isinstance(details, Mapping):
            details = {}
        summary = {key: _plain(computation[key]) for key in (
            "computation", "data_state", "computability_state", "ranking_state",
            "required_inputs", "selected_identity", "selection_reason", "reasons",
            "upstream_abstention") if key in computation}
        if "series_support" in details:
            rows = [row for row in details.get("series_support", []) if isinstance(row, Mapping)]
            if computation.get("computation") == "component_univariate_window_change":
                support_keys = ("reference_unique_timestamps", "alert_unique_timestamps",
                    "reference_independent_blocks", "alert_independent_blocks")
                computable_key = "computable_arithmetic_change"
            else:
                support_keys = ("reference_exact_shared_pairs", "alert_exact_shared_pairs",
                    "reference_pair_blocks", "alert_pair_blocks")
                computable_key = "computable_by_declared_minimum_pairs"
            summary["support_series_count"] = len(rows)
            summary["support_histograms"] = {key: histogram(rows, key)
                for key in support_keys if any(key in row for row in rows)}
            summary["constant_series_count"] = {
                "reference": sum(row.get("reference_constant") is True for row in rows),
                "alert": sum(row.get("alert_constant") is True for row in rows)}
            summary["computable_series_count"] = sum(row.get(computable_key) is True for row in rows)
            summary["omitted_series_count"] = details.get("omitted_series_count", 0)
            for key in ("minimum_independent_blocks", "minimum_exact_pairs_per_window",
                        "primary_kpi_policy_selected", "primary_kpi_minimum_timestamps_per_window",
                        "rankability_state_reason", "ranking_state_reason", "metric_policy_sha256",
                        "selection_policy_sha256"):
                if key in details:
                    summary[key] = _plain(details[key])
        elif computation.get("computation") == "native_log_pattern_composition":
            denominators: dict[str, dict[str, int]] = {}
            for row in details.get("full_window_denominator_records", []):
                if not isinstance(row, Mapping):
                    continue
                stream = f"{row.get('window')}|{row.get('log_name')}"
                item = denominators.setdefault(stream, {"component_streams": 0,
                    "window_record_count": 0, "source_rows_scanned": 0})
                item["component_streams"] += 1
                item["window_record_count"] += int(row.get("row_count", 0) or 0)
                item["source_rows_scanned"] += int(row.get("source_rows_scanned", 0) or 0)
            patterns = [row for row in details.get("pattern_features", []) if isinstance(row, Mapping)]
            patterns.sort(key=lambda row: (-int(row.get("row_support", 0) or 0),
                str(row.get("pattern_sha256", "")), str(row.get("component_id", ""))))
            summary["window_denominators_by_stream"] = denominators
            summary["pattern_feature_count"] = len(patterns)
            summary["pattern_features_omitted_from_prompt"] = max(0, len(patterns) - 8)
            summary["pattern_features_by_support"] = [{key: _plain(row[key]) for key in (
                "component_id", "log_name", "window", "pattern_sha256", "normalized_tokens",
                "row_support", "block_row_count", "block_start_ms_utc", "block_duration_ms") if key in row}
                for row in patterns[:8]]
            for key in ("analysis_block_duration_ms", "derived_count_semantics",
                        "summary_count", "pattern_summary_count", "omitted_summary_count"):
                if key in details:
                    summary[key] = _plain(details[key])
            derived_semantics = summary.get("derived_count_semantics")
            if isinstance(derived_semantics, Mapping):
                summary["derived_count_semantics"] = {
                    key: _plain(derived_semantics[key]) for key in (
                        "field_name", "kind", "unit", "unit_status", "direction",
                        "transform", "classification_status", "ranking_eligible",
                        "reset_policy", "source_table", "selector_ref")
                    if key in derived_semantics}
                summary["derived_count_semantics_ref"] = "full_host_report.derived_count_semantics"
        computation_summaries.append(summary)
    result = {"schema_version": "e4-typed-ir-v2-capability-prompt-summary/2",
        "support_summary_semantics": "Metric histograms aggregate the reported series for each computation; component-selector statuses are keyed by exact stable selector_ref. KPI and log inputs use separate kernels. Exact field matches and per-series rows remain in the full host-side report.",
        "primary_kpi": primary_summary,
        "computations": computation_summaries,
        "provenance": _plain(report.get("provenance", {})),
        "full_host_report_sha256": full_host_report_sha256}
    resolutions = report.get("metric_selector_resolutions")
    if isinstance(resolutions, list):
        resolution_items = [(row.get("selector_ref"), row) for row in resolutions
            if isinstance(row, Mapping) and isinstance(row.get("selector_ref"), str)]
    elif isinstance(resolutions, Mapping):
        resolution_items = list(resolutions.items())
    else:
        resolution_items = []
    if resolution_items:
        selector_status_by_ref: dict[str, list[Any]] = {}
        semantic_profile_by_json: dict[str, str] = {}
        semantic_profiles: dict[str, dict[str, Any]] = {}
        field_mapping_rows = []
        policy_hashes: set[str] = set()
        case_hashes: set[str] = set()
        for selector_ref, resolution in sorted(resolution_items, key=lambda item: str(item[0])):
            if not isinstance(selector_ref, str) or not isinstance(resolution, Mapping):
                continue
            if resolution.get("source_table") != "bank/metric_container.csv":
                # V2's selector kernel operates on ComponentSeries. KPI and
                # log inputs have their own closed kernels and are not legal
                # selector_ref inputs to this parameter.
                continue
            if resolution.get("selector_ref", selector_ref) != selector_ref:
                raise M4ControllerError("paper_capability_selector_ref",
                    "selector resolution key does not match its trusted selector_ref", static=True)
            semantic_profile_refs: set[str] = set()
            reference_support: dict[str, int] = {}
            alert_support: dict[str, int] = {}
            for match in resolution.get("matches", []):
                if not isinstance(match, Mapping) or not isinstance(match.get("field_name"), str):
                    continue
                semantic = {key: _plain(match[key]) for key in (
                    "kind", "unit", "unit_status", "direction", "transform",
                    "reset_policy", "classification_status", "derived_from") if key in match}
                support = match.get("window_support")
                semantic["meaning"] = "Trusted source semantics; selector_ref identifies the exact field or versioned family rule."
                semantic_json = json.dumps(semantic, sort_keys=True, ensure_ascii=False,
                    separators=(",", ":"), allow_nan=False)
                profile_ref = semantic_profile_by_json.get(semantic_json)
                if profile_ref is None:
                    profile_ref = f"m{len(semantic_profile_by_json) + 1}"
                    semantic_profile_by_json[semantic_json] = profile_ref
                    semantic_profiles[profile_ref] = semantic
                semantic_profile_refs.add(profile_ref)
                field_mapping_rows.append({"selector_ref": selector_ref,
                    "field_name": match["field_name"]})
                if isinstance(support, Mapping):
                    for window in ("reference", "alert"):
                        count = support.get(window)
                        if type(count) is int and count >= 0:
                            bins = reference_support if window == "reference" else alert_support
                            label = str(count)
                            bins[label] = bins.get(label, 0) + 1
            selector_status_by_ref[selector_ref] = [resolution.get("status"),
                resolution.get("match_count"), sorted(semantic_profile_refs),
                [[int(count), n] for count, n in sorted(reference_support.items(),
                    key=lambda item: int(item[0]))],
                [[int(count), n] for count, n in sorted(alert_support.items(),
                    key=lambda item: int(item[0]))]]
            if isinstance(resolution.get("policy_sha256"), str):
                policy_hashes.add(resolution["policy_sha256"])
            if isinstance(resolution.get("case_data_sha256"), str):
                case_hashes.add(resolution["case_data_sha256"])
        if len(policy_hashes) > 1 or len(case_hashes) > 1:
            raise M4ControllerError("paper_capability_selector_provenance",
                "selector resolutions must share one metric policy and case-data digest", static=True)
        unique_fields = sorted({row["field_name"] for row in field_mapping_rows})
        result["selector_semantic_profiles"] = semantic_profiles
        result["selector_status_schema"] = ["status", "match_count", "semantic_profile_refs",
            "reference_support_histogram", "alert_support_histogram"]
        result["selector_status_by_ref"] = selector_status_by_ref
        result["selector_field_match_count"] = len(field_mapping_rows)
        result["selector_unique_field_count"] = len(unique_fields)
        result["selector_field_mapping_sha256"] = _canonical_hash(field_mapping_rows)
        result["selector_field_mapping_omission"] = (
            "Exact field-to-selector mappings remain in the full host-side report; this prompt uses stable selector refs, deduplicated source-semantic profiles, and exact support histograms.")
        if policy_hashes:
            result["selector_policy_sha256"] = next(iter(policy_hashes))
        if case_hashes:
            result["selector_case_data_sha256"] = next(iter(case_hashes))
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(encoded) > 24000:
        raise M4ControllerError("paper_capability_prompt_budget",
            "compact typed-IR capability summary exceeds its declared 24KB prompt allowance", static=True,
            details={"bytes": len(encoded)})
    return result


def _validate_paper_adaptation_record(record: Any, history: list[Mapping[str, Any]],
                                      document: Mapping[str, Any],
                                      registry: OperatorRegistry) -> dict[str, Any]:
    expected = {"schema_version", "tested_plan_refs", "public_observations",
        "unexplained_limit", "hypothesis_change", "statistic", "source_scope",
        "window_relationship", "typed_evidence_outputs"}
    if (not isinstance(record, Mapping) or set(record) != expected or
            record.get("schema_version") != PAPER_ADAPTATION_RECORD_SCHEMA):
        raise M4ControllerError("paper_adaptation_schema",
            "paper Stage 2 requires an e4-paper-adaptation-record/1 with exactly the declared fields", static=True)
    if not isinstance(document, Mapping) or not isinstance(document.get("nodes"), list):
        raise M4ControllerError("paper_adaptation_dag", "adaptation evidence outputs require a typed DAG node list", static=True)
    available_refs = {str(row["plan_summary"]["dag_sha256"])
        for row in history if isinstance(row, Mapping) and
        isinstance(row.get("plan_summary"), Mapping) and
        isinstance(row["plan_summary"].get("dag_sha256"), str)}
    refs = record.get("tested_plan_refs")
    if (not isinstance(refs, list) or len(refs) > 3 or
            any(not isinstance(ref, str) for ref in refs) or
            len(set(refs)) != len(refs) or any(ref not in available_refs for ref in refs) or
            (available_refs and not refs)):
        raise M4ControllerError("paper_adaptation_prior_refs",
            ("adaptation tested_plan_refs must contain one to three distinct exact dag_sha256 values from PRIOR ANALYSIS PLANS; "
             if available_refs else "adaptation tested_plan_refs must be empty because PRIOR ANALYSIS PLANS has no digests; ") +
            f"available values: {sorted(available_refs)}", static=True,
            details={"available_dag_sha256": sorted(available_refs)})
    observations = record.get("public_observations")
    if (not isinstance(observations, list) or not 1 <= len(observations) <= 4 or
            any(not isinstance(value, str) or not value.strip() or len(value) > 384
                for value in observations)):
        raise M4ControllerError("paper_adaptation_observations",
            "public_observations must contain one to four bounded observations", static=True)
    for name, limit in (("unexplained_limit", 512), ("hypothesis_change", 512),
                        ("statistic", 384), ("source_scope", 384),
                        ("window_relationship", 384)):
        value = record.get(name)
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            raise M4ControllerError("paper_adaptation_field",
                f"adaptation {name} must be nonempty and at most {limit} characters", static=True)
    evidence_nodes = record.get("typed_evidence_outputs")
    if (not isinstance(evidence_nodes, list) or not 1 <= len(evidence_nodes) <= 8 or
            any(not isinstance(node_id, str) or not node_id for node_id in evidence_nodes) or
            len(set(evidence_nodes)) != len(evidence_nodes)):
        raise M4ControllerError("paper_adaptation_evidence_outputs",
            "typed_evidence_outputs must contain one to eight unique DAG node IDs", static=True)
    rows = {row.get("id"): row for row in document.get("nodes", [])
        if isinstance(row, Mapping) and isinstance(row.get("id"), str)}
    signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
    for node_id in evidence_nodes:
        node = rows.get(node_id)
        signature = signatures.get((node.get("operator"), node.get("version"))) if node else None
        if signature is None or signature.get("output_type") != "ScoredCandidates":
            raise M4ControllerError("paper_adaptation_evidence_type",
                "typed_evidence_outputs must identify registered ScoredCandidates outputs", static=True,
                details={"node_id": node_id})
    normalized = _plain(record)
    size = len(json.dumps(normalized, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8"))
    if size > 4096:
        raise M4ControllerError("paper_adaptation_size",
            "adaptation record exceeds the 4096-byte static output contract", static=True)
    return normalized


def _reject_exact_prior_paper_plan(document: Mapping[str, Any],
                                   history: list[Mapping[str, Any]]) -> None:
    current = playbook_graph_hash(document)
    matches = []
    for row in history:
        prior = row.get("_expanded_plan") if isinstance(row, Mapping) else None
        prior_dag = prior.get("dag") if isinstance(prior, Mapping) else None
        if not isinstance(prior_dag, Mapping):
            continue
        try:
            prior_hash = playbook_graph_hash(prior_dag)
        except Exception:
            continue
        if prior_hash == current:
            plan = row.get("plan_summary", {})
            matches.append({"stage": row.get("stage"),
                "dag_sha256": plan.get("dag_sha256") if isinstance(plan, Mapping) else None})
    if matches:
        raise M4ControllerError("paper_stage2_semantic_duplicate",
            "paper Stage 2 exact expanded DAG repeats a prior analyzed computation; revise the supported statistic/source/window relationship",
            static=True, details={"expanded_dag_sha256": current, "prior_matches": matches})


def _expanded_expression_hashes(document: Mapping[str, Any], registry: OperatorRegistry,
                               runtime_values: Mapping[str, Any], *,
                               case_data_sha256: str) -> dict[str, str | None]:
    """Hash exact, deterministic node expressions while ignoring graph node IDs.

    This proves only exact computational repetition for supported operators. Unknown,
    non-cacheable, or estimator-dependent expressions remain inconclusive.
    """
    trusted_pure = {"bank.metrics.aggregate", "bank.traces.aggregate",
        "bank.candidates.generate", "bank.metrics.contrast",
        "bank.traces.latency_contrast", "bank.logs.volume_contrast",
        "bank.public.project", "bank.answers.combine"}
    effective_input_ports = {"bank.candidates.generate": {"data"}}
    rows = {str(row.get("id")): row for row in document.get("nodes", [])
            if isinstance(row, Mapping) and isinstance(row.get("id"), str)}
    signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
    memo: dict[str, str | None] = {}
    active: set[str] = set()

    def visit(node_id: str) -> str | None:
        if node_id in memo:
            return memo[node_id]
        row = rows.get(node_id)
        if row is None or node_id in active:
            return None
        signature = signatures.get((row.get("operator"), row.get("version")))
        if signature is None:
            return None
        if signature["name"] == "case.telemetry":
            # All compared plans are from this same case execution. Its source is fixed.
            digest = _canonical_hash({"external_input": "case.telemetry",
                "case_data_sha256": case_data_sha256})
            memo[node_id] = digest
            return digest
        if signature["name"] not in trusted_pure:
            memo[node_id] = None
            return None
        operator_spec = registry.get(row.get("operator"), row.get("version"))
        if operator_spec is None or not operator_spec.cacheable:
            memo[node_id] = None
            return None
        active.add(node_id)
        params = dict(_plain(row.get("params", {})))
        bindings = row.get("bindings", {})
        if not isinstance(bindings, Mapping):
            active.remove(node_id)
            memo[node_id] = None
            return None
        effective: dict[str, Any] = {}
        inconclusive = False
        for name, spec in signature.get("parameters", {}).items():
            if name in params:
                value = params[name]
            elif name in bindings:
                field = str(bindings[name]).removeprefix("runtime.")
                if field not in runtime_values:
                    inconclusive = True
                    break
                value = runtime_values[field]
            elif spec.get("runtime_field"):
                field = spec["runtime_field"]
                if field not in runtime_values:
                    if spec.get("has_default"):
                        value = spec.get("default")
                    else:
                        inconclusive = True
                        break
                else:
                    value = runtime_values[field]
            elif spec.get("has_default"):
                value = spec.get("default")
            elif spec.get("required"):
                inconclusive = True
                break
            else:
                continue
            if value == "auto":
                inconclusive = True
                break
            effective[name] = _plain(value)
        input_expressions: dict[str, Any] = {}
        if not inconclusive:
            consumed_ports = effective_input_ports.get(row["operator"])
            for port_name, refs in row.get("inputs", {}).items():
                if consumed_ports is not None and port_name not in consumed_ports:
                    continue
                sequence = refs if type(refs) is list else [refs]
                children = [visit(ref) for ref in sequence if isinstance(ref, str)]
                if len(children) != len(sequence) or any(child is None for child in children):
                    inconclusive = True
                    break
                input_expressions[str(port_name)] = children if type(refs) is list else children[0]
        active.remove(node_id)
        if inconclusive:
            memo[node_id] = None
            return None
        memo[node_id] = _canonical_hash({
            "operator": row["operator"], "version": row["version"],
            "inputs": input_expressions, "effective_params": effective,
            # Runtime access is available in NodeContext to registered code. Keeping
            # the full context prevents false exact matches when its unused fields differ.
            "runtime_values": _plain(runtime_values),
        })
        return memo[node_id]

    for node_id in rows:
        visit(node_id)
    return memo


def _snapshot_source_pins(snapshot: LibrarySnapshot, root: Path, policy_path: Path, *,
                          metric_policy_path: Path | None = None) -> None:
    provenance = snapshot.document.get("provenance", {})
    if not isinstance(provenance, Mapping):
        raise M4ControllerError("library_provenance", "library snapshot lacks source provenance")
    metric_policy_path = metric_policy_path or root / "config/bank_metric_kind_policy.json"
    expected = {"implementation_fingerprint": _implementation_fingerprint(),
        "m4_runtime_policy_sha256": sha256_file(policy_path),
        "bank_metric_policy_sha256": sha256_file(metric_policy_path),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json")}
    if snapshot.document.get("policy", {}).get("capacity_policy") == "reconstructed-paper-starter-v1":
        expected.update({
            "paper_starter_catalog_sha256": sha256_file(root / "config/paper_starter_catalog.json"),
            "paper_starter_playbooks_sha256": sha256_file(root / "config/paper_starter_playbooks.json"),
            "paper_starter_runtime_sha256": sha256_file(root / "config/paper_starter_runtime.json"),
            "paper_bank_adaptations_sha256": sha256_file(root / "config/paper_bank_adaptations.json"),
        })
        selected_authoring = stage3_authoring_profile(snapshot)
        if selected_authoring == "typed-operator-code-v1":
            authoring_path = root / "config/paper_starter_authoring.json"
            try:
                authoring_config = json.loads(authoring_path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise M4ControllerError("library_source_mismatch",
                    "paper-starter authoring config is required by the persisted code-profile snapshot") from exc
            if (authoring_config.get("schema_version") != "e4-paper-starter-authoring/1" or
                    authoring_config.get("stage3_authoring_profile") != selected_authoring):
                raise M4ControllerError("library_authoring_profile_mismatch",
                    "authoring config differs from the profile pinned in the snapshot policy")
            expected["paper_starter_authoring_sha256"] = sha256_file(authoring_path)
    for key, value in expected.items():
        if provenance.get(key) != value:
            raise M4ControllerError("library_source_mismatch",
                f"bootstrap library {key} does not match the active frozen source")


def _stage1_doc(selection: Mapping[str, Any], snapshot: LibrarySnapshot,
                registry: OperatorRegistry, runtime: RuntimeContext) -> tuple[dict[str, Any], str]:
    doc, name = _apply_stage1_selection(selection, snapshot, registry)
    return doc, name


def _candidate_spec_mapping(specs: list[Mapping[str, Any]], registry: OperatorRegistry,
                            runtime: RuntimeContext,
                            active_composites: list[Mapping[str, Any]],
                            active_ir_specs: list[Mapping[str, Any]] = (), *,
                            allowed_authoring_profiles: Sequence[str] = ("typed-operator-ir-v1",)
                            ) -> tuple[list[dict[str, Any]], dict[tuple[str, str], str]]:
    normalized: list[dict[str, Any]] = []
    mapping: dict[tuple[str, str], str] = {}
    names = set()
    body_hashes = {str(spec.get("program_sha256")) for spec in active_ir_specs
        if isinstance(spec.get("program_sha256"), str)}
    source_body_hashes = set()
    for prior in active_ir_specs:
        source = prior.get("source")
        if isinstance(source, str):
            try:
                tree = ast.parse(source, mode="exec")
            except SyntaxError:
                continue
            source_body_hashes.add(hashlib.sha256(
                ast.dump(tree, annotate_fields=True, include_attributes=False).encode("utf-8")).hexdigest())
    for raw in specs:
        try:
            if (not isinstance(raw, Mapping) or
                    raw.get("authoring_profile") not in set(allowed_authoring_profiles)):
                if set(allowed_authoring_profiles) == {"typed-operator-ir-v2"}:
                    message = ("new V2 operator spec requires authoring_profile="
                        "'typed-operator-ir-v2'; program.schema_version is separately "
                        "'e4-typed-operator-ir/2'")
                else:
                    message = (f"Stage 3 new operators must use one of "
                        f"{sorted(set(allowed_authoring_profiles))}; closed macros are adaptation only")
                raise M4ControllerError("stage3_operator_profile",
                    message, static=True,
                    details={"required_authoring_profile": "typed-operator-ir-v2",
                        "required_program_schema_version": "e4-typed-operator-ir/2"}
                    if set(allowed_authoring_profiles) == {"typed-operator-ir-v2"} else {})
            spec = validate_generated_spec(raw)
        except Exception as exc:
            code = getattr(exc, "code", "composite_spec_static")
            raise M4ControllerError(code, str(exc), static=True,
                details=getattr(exc, "details", {})) from exc
        key = (spec["name"], spec["declared_version"])
        if key in mapping or spec["name"] in names:
            raise M4ControllerError("generated_spec_duplicate", "Stage 3 specs must have unique names and declared versions", static=True)
        if (isinstance(spec.get("program_sha256"), str) and
                spec["program_sha256"] in body_hashes):
            raise M4ControllerError("stage3_ir_duplicate_body",
                "Stage 3 IR body exactly matches a retained synthesized operator", static=True,
                details={"operator": spec["name"], "program_sha256": spec["program_sha256"]})
        mapping[key] = spec["version"]
        names.add(spec["name"])
        if isinstance(spec.get("program_sha256"), str):
            body_hashes.add(spec["program_sha256"])
        if isinstance(spec.get("source"), str):
            tree = ast.parse(spec["source"], mode="exec")
            source_body_sha = hashlib.sha256(
                ast.dump(tree, annotate_fields=True, include_attributes=False).encode("utf-8")).hexdigest()
            if source_body_sha in source_body_hashes:
                raise M4ControllerError("stage3_source_duplicate_body",
                    "Stage 3 Python source has the same canonical AST as a retained authored body; this structural check does not establish mathematical equivalence",
                    static=True, details={"operator": spec["name"], "canonical_ast_sha256": source_body_sha})
            source_body_hashes.add(source_body_sha)
        normalized.append(spec)
    return normalized, mapping


def _split_composite_specs(specs: list[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    composites = [dict(spec) for spec in specs
        if spec.get("authoring_profile") == "operator-dag-v1"]
    legacy = [dict(spec) for spec in specs
        if spec.get("authoring_profile") != "operator-dag-v1"]
    return legacy, composites


def _logical_registry(primitive_registry: OperatorRegistry,
                      composite_specs: list[Mapping[str, Any]]) -> OperatorRegistry:
    """Add signature-only macro placeholders to a fresh planning registry."""
    logical = OperatorRegistry()
    for row in primitive_registry.signatures():
        logical.register(primitive_registry.require(row["name"], row["version"]))
    for name, estimator in primitive_registry.estimators.items():
        logical.register_estimator(name, estimator)
    for spec in composite_specs:
        ports = tuple(PortSpec(row["name"], row["type"], row["required"],
            row["variadic"], row["minimum_items"], row["maximum_items"])
            for row in spec["input_ports"])
        parameters = {name: ParameterSpec(name=name, value_type=row["type"],
            required=row["required"], nullable=row["nullable"], default=row["default"],
            has_default=row["has_default"], runtime_field=row["runtime_field"],
            description=row["description"], choices=tuple(row["choices"]))
            for name, row in spec["parameters"].items()}
        logical.register(OperatorSpec(name=spec["name"], version=spec["version"],
            inputs=ports, output_type=spec["output_type"], parameters=parameters,
            family=spec["family"], cacheable=False, implementation=None,
            description=spec["description"]))
    return logical


def _expand_composite_document(document: Mapping[str, Any],
        composite_specs: list[Mapping[str, Any]], primitive_registry: OperatorRegistry,
        runtime: RuntimeContext) -> Any | None:
    from .m4_composite import expand_composite_dag
    return expand_composite_dag(document, composite_specs, primitive_registry, runtime,
        max_nodes=int(runtime.snapshot.values["max_nodes"]),
        max_depth=int(runtime.snapshot.values["dag_depth"]))


def _expansion_document(expansion: Any) -> dict[str, Any]:
    return {"schema_version": "e4-operator-dag-expansion/1",
        "report": _plain(expansion.report),
        "logical_to_expanded": _plain(expansion.logical_to_expanded),
        "call_outputs": _plain(expansion.call_outputs),
        "node_origins": _plain(expansion.node_origins)}


def _composite_program_example(registry: OperatorRegistry) -> dict[str, Any]:
    aggregate = next((row for row in registry.signatures()
        if row["name"] == "bank.metrics.aggregate"), None)
    if aggregate is None:
        return {}
    return {"name": "evolved.example_window_aggregate", "version": "example",
        "description": "Syntax example only; reuse the registered metric-window aggregator.",
        "family": "bank_aggregation_pipeline",
        "input_ports": [{"name": "data", "type": "Telemetry"}],
        "output_type": "Aggregate",
        "parameters": {"window": {"type": "string", "required": True,
            "choices": ["reference", "alert"]}},
        "authoring_profile": "operator-dag-v1",
        "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
            "id": "summary", "operator": "bank.metrics.aggregate",
            "version": aggregate["version"], "inputs": {"data": "$input.data"},
            "params": {"window": {"parameter": "window"}}, "bindings": {}}],
            "return": {"node": "summary"}}}


def _normalize_gap_record_draft(gap_record: Any, specs: list[Mapping[str, Any]],
                               registry: OperatorRegistry,
                               history: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Normalize only unambiguous shorthand into the canonical auditable gap record.

    The model may cite plan hashes without stage labels and may state one gap string
    per proposed operator. All operator identities, ports, related signatures, and
    host validation obligations are derived from the normalized IR and registry.
    """
    if not isinstance(gap_record, Mapping):
        raise M4ControllerError("stage3_gap_schema",
            "gap_record must be an object with schema_version, prior_plans, and capabilities", static=True)
    allowed = {"schema_version", "prior_plans", "capabilities", "gap"}
    if set(gap_record) - allowed or not {"schema_version", "prior_plans"}.issubset(gap_record):
        raise M4ControllerError("stage3_gap_schema",
            "gap_record must contain only schema_version, prior_plans, and either capabilities or the single-operator gap alias", static=True)
    if "gap" in gap_record and "capabilities" in gap_record:
        raise M4ControllerError("stage3_gap_schema",
            "use capabilities or gap, not both; gap is allowed only for one proposed operator", static=True)

    available: dict[str, list[str]] = {}
    for row in history:
        plan = row.get("plan_summary")
        stage = row.get("stage")
        digest = plan.get("dag_sha256") if isinstance(plan, Mapping) else None
        if isinstance(stage, str) and isinstance(digest, str):
            available.setdefault(digest, []).append(stage)
    raw_plans = gap_record.get("prior_plans")
    if not isinstance(raw_plans, list):
        raise M4ControllerError("stage3_gap_prior_schema",
            "prior_plans must be a list of {stage,dag_sha256} objects or unique recorded DAG hashes", static=True)
    prior_plans: list[dict[str, str]] = []
    for reference in raw_plans:
        if isinstance(reference, str):
            stages = available.get(reference, [])
            if not re.fullmatch(r"[0-9a-f]{64}", reference) or len(stages) != 1:
                code = "stage3_gap_prior_ambiguous" if len(stages) > 1 else "stage3_gap_prior_unknown"
                raise M4ControllerError(code,
                    "a hash-only prior plan must uniquely match one recorded stage and DAG hash", static=True)
            prior_plans.append({"stage": stages[0], "dag_sha256": reference})
        elif isinstance(reference, Mapping):
            prior_plans.append(_plain(reference))
        else:
            raise M4ControllerError("stage3_gap_prior_schema",
                "each prior plan must be a {stage,dag_sha256} object or a uniquely recorded DAG hash", static=True)
    if any(not isinstance(row, Mapping) or not isinstance(row.get("stage"), str) or
           not isinstance(row.get("dag_sha256"), str) for row in prior_plans):
        raise M4ControllerError("stage3_gap_prior_schema",
            "prior plan references require stage and lowercase DAG SHA-256", static=True)
    canonical_plan_ids = [(row["stage"], row["dag_sha256"]) for row in prior_plans]
    if len(set(canonical_plan_ids)) != len(canonical_plan_ids):
        raise M4ControllerError("stage3_gap_prior_duplicate",
            "prior_plans must reference each recorded plan at most once", static=True)

    if "gap" in gap_record:
        gap_text = gap_record.get("gap")
        if len(specs) != 1 or not isinstance(gap_text, str) or not gap_text.strip():
            raise M4ControllerError("stage3_gap_capability_schema",
                "gap is a shorthand for exactly one proposed operator; provide capabilities[] for multiple operators", static=True)
        capability_texts = [gap_text]
    else:
        raw_capabilities = gap_record.get("capabilities")
        if not isinstance(raw_capabilities, list):
            raise M4ControllerError("stage3_gap_capability_schema",
                "capabilities must be an array; use one short gap string per operator or a full capability object", static=True)
        if all(isinstance(item, str) for item in raw_capabilities):
            if len(raw_capabilities) != len(specs):
                raise M4ControllerError("stage3_gap_count",
                    "provide exactly one capability string per proposed operator", static=True)
            capability_texts = raw_capabilities
        elif any(isinstance(item, str) for item in raw_capabilities):
            raise M4ControllerError("stage3_gap_capability_schema",
                "do not mix capability strings and objects; use all strings or all full objects", static=True)
        else:
            required_fields = set(_GAP_CAPABILITY_FIELDS)
            malformed = [(index, item) for index, item in enumerate(raw_capabilities)
                         if isinstance(item, Mapping) and set(item) != required_fields]
            if malformed:
                index, item = malformed[0]
                actual_fields = set(item)
                missing = sorted(required_fields - actual_fields)
                extra = sorted(actual_fields - required_fields)
                raise M4ControllerError("stage3_gap_capability_schema",
                    "capabilities must be bounded strings, one per operator; capability objects are accepted only in the full canonical form. "
                    "Expected draft: capabilities:[\"one gap hypothesis per operator\"]. "
                    f"Capability object {index} is missing fields {missing} and has extra fields {extra}; "
                    f"a full object requires exactly {list(_GAP_CAPABILITY_FIELDS)}.", static=True,
                    details={"expected_draft_type": "array of bounded strings, one per ordered operator",
                        "required_canonical_object_fields": list(_GAP_CAPABILITY_FIELDS),
                        "missing_fields": missing, "extra_fields": extra})
            capability_texts = []

    if capability_texts:
        signatures = registry.signatures()
        specs_by_identity = {(str(spec.get("name")), str(spec.get("declared_version"))): spec
                             for spec in specs}
        canonical_capabilities: list[dict[str, Any]] = []
        for spec, gap_text in zip(specs, capability_texts):
            if not isinstance(gap_text, str) or not gap_text.strip() or len(gap_text) > 768:
                raise M4ControllerError("stage3_gap_text",
                    "each capability string must be nonempty text of at most 768 characters", static=True)
            identity = (str(spec.get("name")), str(spec.get("declared_version")))
            ports = spec.get("input_ports", [])
            if not isinstance(ports, list):
                raise M4ControllerError("stage3_gap_inputs",
                    "normalized operator input ports are unavailable for capability normalization", static=True)
            output_type = spec.get("output_type")
            same_output = [row for row in signatures
                if row.get("output_type") == output_type and
                (row.get("name"), row.get("version")) not in {
                    (str(item.get("name")), str(item.get("version")))
                    for item in specs_by_identity.values()}]
            family_matches = [row for row in same_output if row.get("family") == spec.get("family")]
            closest_rows = sorted(family_matches or same_output,
                key=lambda row: (str(row.get("name", "")), str(row.get("version", ""))))[:4]
            if not closest_rows:
                raise M4ControllerError("stage3_gap_closest",
                    "no registered operator has the proposed output type; provide a full capability object and review the output contract", static=True)
            canonical_capabilities.append({
                "operator_name": identity[0],
                "operator_version": identity[1],
                "gap_hypothesis": gap_text.strip(),
                "computational_difference": str(spec.get("description", "")).strip(),
                "intended_contribution": (
                    "Expose the proposed typed operator output to the registered scoring pipeline; "
                    "the controller still validates support, evidence, and admission."),
                "coverage_requirement": (
                    "Satisfy the operator's host-validated input requirements and abstain when "
                    "available typed support is insufficient."),
                "validation_expectation": (
                    ("Pass static Python typing/source checks, bounded execution, typed output, "
                     "host-pinned source/data provenance, support, and admission checks; no diagnostic success is assumed."
                     if spec.get("authoring_profile") == "typed-operator-code-v1" else
                     "Pass static IR checks, bounded execution, typed output, provenance, support, "
                     "and case admission checks; no diagnostic success is assumed.")),
                "closest_existing_operators": [
                    {"name": str(row["name"]), "version": str(row["version"])}
                    for row in closest_rows],
                "required_inputs": [str(port["name"]) for port in ports
                    if isinstance(port, Mapping) and isinstance(port.get("name"), str)]})
        canonical_gap = {"schema_version": gap_record["schema_version"],
            "prior_plans": prior_plans, "capabilities": canonical_capabilities}
    else:
        raw_capabilities = gap_record.get("capabilities")
        canonical_gap = {"schema_version": gap_record["schema_version"],
            "prior_plans": prior_plans, "capabilities": _plain(raw_capabilities)}
    return canonical_gap


def _validate_gap_record(gap_record: Any, specs: list[Mapping[str, Any]],
                         registry: OperatorRegistry,
                         history: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Validate the bounded, auditable references in a stage-3 capability hypothesis."""
    if not isinstance(gap_record, Mapping) or set(gap_record) != {
            "schema_version", "prior_plans", "capabilities"}:
        raise M4ControllerError("stage3_gap_schema", "gap_record has the wrong object shape", static=True)
    if gap_record.get("schema_version") != "e4-capability-gaps/1":
        raise M4ControllerError("stage3_gap_version", "unsupported capability gap record version", static=True)
    prior_plans = gap_record.get("prior_plans")
    capabilities = gap_record.get("capabilities")
    if (not isinstance(prior_plans, list) or len(prior_plans) > 3 or
            not isinstance(capabilities, list) or len(capabilities) > 3 or
            len(capabilities) != len(specs)):
        raise M4ControllerError("stage3_gap_count", "gap record must reference up to three prior plans and map 0-3 capabilities one-to-one to operators", static=True)
    available_plans = {
        (row.get("stage"), row.get("plan_summary", {}).get("dag_sha256"))
        for row in history if isinstance(row.get("plan_summary"), Mapping)
        and isinstance(row.get("stage"), str)
        and isinstance(row["plan_summary"].get("dag_sha256"), str)
    }
    if available_plans and not prior_plans:
        raise M4ControllerError("stage3_gap_prior_missing", "gap record must cite at least one available prior plan", static=True)
    if not specs and capabilities:
        raise M4ControllerError("stage3_gap_empty_mapping", "an empty extension must have capabilities:[]", static=True)
    if specs and not capabilities:
        raise M4ControllerError("stage3_gap_empty_mapping", "proposed operators require matching capability records", static=True)
    seen_plans: set[tuple[str, str]] = set()
    for reference in prior_plans:
        if (not isinstance(reference, Mapping) or set(reference) != {"stage", "dag_sha256"} or
                not isinstance(reference.get("stage"), str) or
                not re.fullmatch(r"[0-9a-f]{64}", str(reference.get("dag_sha256", "")))):
            raise M4ControllerError("stage3_gap_prior_schema", "prior plan references require stage and lowercase DAG SHA-256", static=True)
        pair = (reference["stage"], reference["dag_sha256"])
        if pair not in available_plans or pair in seen_plans:
            raise M4ControllerError("stage3_gap_prior_unknown", "prior plan reference does not match unique recorded history", static=True)
        seen_plans.add(pair)
    registered = {(signature["name"], signature["version"])
        for signature in registry.signatures()}
    expected = {(str(spec["name"]), str(spec["declared_version"])) for spec in specs}
    spec_by_identity = {(str(spec["name"]), str(spec["declared_version"])): spec
                       for spec in specs}
    seen: set[tuple[str, str]] = set()
    string_fields = ("gap_hypothesis", "computational_difference", "intended_contribution",
                     "coverage_requirement", "validation_expectation")
    for capability in capabilities:
        required = {"operator_name", "operator_version", *string_fields,
                    "closest_existing_operators", "required_inputs"}
        if not isinstance(capability, Mapping) or set(capability) != required:
            raise M4ControllerError("stage3_gap_capability_schema", "capability record has missing or extra fields", static=True)
        operator_name = capability.get("operator_name")
        operator_version = capability.get("operator_version")
        if (not isinstance(operator_name, str) or not operator_name.strip() or len(operator_name) > 128 or
                not isinstance(operator_version, str) or not operator_version.strip() or
                len(operator_version) > 64):
            raise M4ControllerError("stage3_gap_operator_mapping",
                "capability operator name/version must be nonempty bounded strings", static=True)
        identity = (operator_name, operator_version)
        if identity not in expected or identity in seen:
            raise M4ControllerError("stage3_gap_operator_mapping", "capabilities must map one-to-one to proposed operator names and declared versions", static=True)
        seen.add(identity)
        for field in string_fields:
            value = capability.get(field)
            if not isinstance(value, str) or not value.strip() or len(value) > 768:
                raise M4ControllerError("stage3_gap_text", f"{field} must be nonempty text of at most 768 characters", static=True)
        closest = capability.get("closest_existing_operators")
        if not isinstance(closest, list) or not 1 <= len(closest) <= 4:
            raise M4ControllerError("stage3_gap_closest", "each capability needs 1-4 closest registered operators", static=True)
        seen_closest: set[tuple[str, str]] = set()
        for row in closest:
            if (not isinstance(row, Mapping) or set(row) != {"name", "version"} or
                    not isinstance(row.get("name"), str) or not isinstance(row.get("version"), str)):
                raise M4ControllerError("stage3_gap_closest_schema", "closest operator refs require name and version", static=True)
            pair = (row["name"], row["version"])
            if pair not in registered or pair in seen_closest:
                raise M4ControllerError("stage3_gap_closest_unknown", "closest operator ref must identify a unique registered signature", static=True)
            seen_closest.add(pair)
        required_inputs = capability.get("required_inputs")
        if (not isinstance(required_inputs, list) or len(required_inputs) > 8 or
                any(not isinstance(item, str) or not item.strip() or len(item) > 256
                    for item in required_inputs)):
            raise M4ControllerError("stage3_gap_inputs", "required_inputs must contain at most eight nonempty strings of at most 256 characters", static=True)
        declared_ports = {str(port["name"]) for port in spec_by_identity[identity]["input_ports"]}
        if len(set(required_inputs)) != len(required_inputs) or set(required_inputs) != declared_ports:
            raise M4ControllerError("stage3_gap_input_refs", "required_inputs must name every declared input port exactly once", static=True)
    if seen != expected:
        raise M4ControllerError("stage3_gap_operator_mapping", "every proposed operator requires exactly one capability record", static=True)
    return _plain(gap_record)


def reject_identifier_literals(source: str, identifiers: set[str] | list[str] | tuple[str, ...]) -> None:
    """Reject generated code that embeds an exact case/entity answer as a string constant."""
    forbidden = {item.strip().casefold() for item in identifiers
                 if isinstance(item, str) and item.strip()}
    if not forbidden:
        return
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise M4ControllerError("generated_source_syntax", str(exc), static=True) from exc
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value.strip().casefold() in forbidden:
                raise M4ControllerError("generated_source_identifier_literal",
                    "generated source cannot embed an incident ID or exact observed component ID; use inputs/params",
                    static=True)


def _reject_program_identifier_literals(composite: Mapping[str, Any],
                                        identifiers: set[str] | list[str] | tuple[str, ...]) -> None:
    forbidden = {item.strip().casefold() for item in identifiers
        if isinstance(item, str) and item.strip()}
    def visit_parameter_values(values: Any, node_id: str | None) -> None:
        stack = [values]
        while stack:
            current = stack.pop()
            if isinstance(current, str) and current.strip().casefold() in forbidden:
                code_profile = composite.get("authoring_profile") == "typed-operator-code-v1"
                raise M4ControllerError("operator_parameter_identifier_literal" if code_profile else
                        "composite_identifier_literal",
                    ("authored parameter defaults/choices cannot embed an incident or exact observed component ID"
                     if code_profile else
                     "composite parameters cannot embed an incident or exact observed component ID"),
                    static=True, details={"operator": composite.get("name"),
                        "node_id": node_id})
            if isinstance(current, Mapping):
                stack.extend(current.values())
            elif isinstance(current, (list, tuple)):
                stack.extend(current)

    program = composite.get("program", {})
    for node in program.get("nodes", []) if isinstance(program, Mapping) else []:
        if isinstance(node, Mapping):
            visit_parameter_values(node.get("params", {}), node.get("id"))
    for name, parameter in composite.get("parameters", {}).items():
        if not isinstance(parameter, Mapping):
            continue
        constants = []
        if parameter.get("has_default") is True or "default" in parameter:
            constants.append(parameter.get("default"))
        constants.extend(parameter.get("choices", ()))
        visit_parameter_values(constants, f"parameter:{name}")
    source = composite.get("source")
    if isinstance(source, str):
        try:
            tree = ast.parse(source, mode="exec")
        except SyntaxError:
            return
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value.strip().casefold() in forbidden:
                    raise M4ControllerError("source_identifier_literal",
                        "authored source cannot embed an incident or exact observed component ID",
                        static=True, details={"operator": composite.get("name"),
                            "line": getattr(node, "lineno", None)})


def _resolve_generated_versions(document: Any, mapping: Mapping[tuple[str, str], str]) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise M4ControllerError("dag_schema", "dag must be a JSON object", static=True)
    result = json.loads(json.dumps(document, ensure_ascii=False, allow_nan=False))
    if not isinstance(result.get("nodes"), list):
        raise M4ControllerError("dag_schema", "dag.nodes must be an array", static=True)
    for node in result["nodes"]:
        if not isinstance(node, dict):
            raise M4ControllerError("dag_schema", "each DAG node must be an object", static=True)
        operator_name, version = node.get("operator"), node.get("version")
        if (not isinstance(operator_name, str) or not operator_name or
                not isinstance(version, str) or not version):
            raise M4ControllerError("node_identity",
                "DAG node operator and version must be nonempty strings", static=True)
        key = (operator_name, version)
        if key in mapping:
            node["version"] = mapping[key]
    return result


def _verify_and_execute(document: Mapping[str, Any], registry: OperatorRegistry,
                        runtime: RuntimeContext, telemetry: Any, run_dir: Path,
                        *, stage: str, new_operator_keys: set[str] | None = None,
                        profile: str = "legacy-m4") -> tuple[VerifiedDAG, Any]:
    verified = _validate_graph(document, registry, runtime, stage=stage,
                               new_operator_keys=new_operator_keys, profile=profile)
    source_id = next(node.node_id for node in verified.nodes if node.operator.external_input)
    executor = SerialExecutor(registry, CacheStore(run_dir / "cache", enabled=True))
    try:
        result = executor.execute_verified(verified, runtime, {source_id: telemetry},
            case_id=telemetry.case_id, data_sha256=telemetry.data_sha256,
            file_sha256=telemetry.file_sha256, cache_enabled=True)
    except ExecutionFailure as exc:
        raise M4ControllerError("runtime_execution_failed", str(exc)) from exc
    return verified, result


def run_diagnostic(case_id: str, *, library_root: Path, run_dir: Path,
                   feedback: Any, profile_path: Path,
                   client: LocalModelClient | None = None,
                   root: Path = ROOT, policy_path: Path | None = None,
                   split: str = "dev") -> dict[str, Any]:
    """Run one three-stage Bank diagnosis; default remains frozen development-only."""
    started = time.perf_counter()
    root = root.resolve(strict=True)
    policy_path = policy_path or root / "config/m4_runtime.json"
    policy = _policy(root, policy_path)
    if run_dir.exists():
        raise FileExistsError(f"M4 run directory must be fresh: {run_dir}")
    run_dir.mkdir(parents=True)
    for folder in ("prompts", "attempts", "dags", "operators", "audit"):
        (run_dir / folder).mkdir()
    calls_path = run_dir / "model_calls.jsonl"
    calls_path.touch(exist_ok=False)
    status_path = run_dir / "controller_status.json"
    _write_json(status_path, {"phase": "public_input_load", "status": "running"})
    snapshot: LibrarySnapshot | None = None
    paper_starter_input = None
    paper_profile = False
    calls: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    used_operator_keys: set[str] = set()
    used_playbooks: set[str] = set()
    last_snapshot: LibrarySnapshot | None = None
    accepted = False
    stage3_entered = False
    stage3_diagnostics_written = False
    final_answer: RankedAnswer | None = None
    stage_rows: list[dict[str, Any]] = []
    try:
        sample = json.loads((root / "config/bank_sample.json").read_text(encoding="utf-8"))
        if split not in {"dev", "scored"}:
            raise M4ControllerError("case_split", "diagnosis split must be dev or scored")
        case_key = "development_cases" if split == "dev" else "scored_cases"
        allowed_cases = {row["incident_id"] for row in sample.get(case_key, [])}
        if case_id not in allowed_cases:
            raise M4ControllerError("case_not_in_split", f"controller case is not in the frozen {split} allowlist")
        case = load_bank_case(case_id, split, root=root)
        digest = public_case_digest(case, root=root)

        source_snapshot = snapshot_library(library_root)
        local_library = run_dir / "library"
        shutil.copytree(source_snapshot.root, local_library)
        snapshot = snapshot_library(local_library)
        last_snapshot = snapshot
        _snapshot_source_pins(snapshot, root, policy_path)
        paper_profile = snapshot.document.get("policy", {}).get("capacity_policy") == "reconstructed-paper-starter-v1"
        paper_authoring_profile = (stage3_authoring_profile(snapshot) if paper_profile else None)
        paper_code_profile = paper_profile and paper_authoring_profile == "typed-operator-code-v1"
        specs = generated_specs_from_snapshot(snapshot)
        legacy_specs, active_composite_specs = _split_composite_specs(specs)
        query_budget = M4QueryBudget(run_dir / "audit")
        if paper_profile:
            from .paper_starter import (available_bank_profile_playbooks,
                bank_profile_playbooks, make_paper_starter_input, make_paper_starter_registry)
            paper_starter_input = make_paper_starter_input(case.telemetry, root=root)
            primitive_active_keys = {f"{entry['name']}@{entry['version']}"
                for entry in snapshot.document["operators"]
                if entry.get("kind") == "builtin" or
                   entry.get("signature", {}).get("authoring_profile") != "operator-dag-v1"}
            primitive_registry = make_paper_starter_registry(bank_only=True,
                starter_input=paper_starter_input, generated_specs=legacy_specs,
                audit_dir=run_dir / "audit", active_operator_keys=primitive_active_keys, root=root)
            case_allowed_books = {item["name"] for item in available_bank_profile_playbooks(
                [dict(item) for item in snapshot.playbooks.values()], paper_starter_input)}
            policy = {**policy, "_paper_case_available_books": sorted(case_allowed_books),
                "_paper_case_capabilities": dict(paper_starter_input.capabilities)}
        else:
            primitive_registry = make_m4_registry(case, legacy_specs, root=root,
                audit_dir=run_dir / "audit", shared_query_budget=query_budget)
        registry = _logical_registry(primitive_registry, active_composite_specs)
        assert_library_registry_compatible(snapshot, primitive_registry)
        profile_path = profile_path.resolve(strict=True)
        if not profile_path.is_relative_to(root):
            raise M4ControllerError("profile_path", "M4 model profile must remain inside project root")
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        model_harness_profile = ("reconstructed-paper-starter-v1-typed-operator-code-v1"
            if paper_code_profile else "reconstructed-paper-starter-v1" if paper_profile else "legacy-m4")
        model = client or M4ModelClient(profile,
            base_url=os.environ.get("E4_BASE_URL"),
            request_timeout=float(policy["serving"]["model_timeout_seconds"]),
            harness_profile=model_harness_profile)
        if isinstance(model, M4ModelClient):
            model.harness_profile = model_harness_profile
        paper_ir_capability_report_path: Path | None = None
        paper_ir_capability_report_sha256: str | None = None
        paper_ir_capability_summary_sha256: str | None = None
        if paper_profile and not paper_code_profile:
            from .m4_operator_ir import operator_ir_capability_report
            capability_runtime = build_m4_runtime(case.alert, root=root, policy_path=policy_path)
            paper_ir_capability_report = operator_ir_capability_report(
                paper_starter_input, registry,
                runtime_values=capability_runtime.as_dict()["values"], root=root)
            paper_ir_capability_report_path = run_dir / "typed_ir_capability_report.json"
            _write_json(paper_ir_capability_report_path, paper_ir_capability_report)
            paper_ir_capability_report_sha256 = sha256_file(paper_ir_capability_report_path)
            policy = {**policy,
                "_paper_ir_capability_report": paper_ir_capability_report,
                "_paper_ir_capability_prompt_summary":
                    _paper_v2_capability_prompt_summary(paper_ir_capability_report)}
            paper_ir_capability_summary_sha256 = _canonical_hash(
                policy["_paper_ir_capability_prompt_summary"])
        if paper_code_profile:
            policy = {**policy, "_paper_stage3_authoring_profile": paper_authoring_profile}
        run_doc = {"schema_version": "e4-m4-diagnostic-run/1", "case_id": case_id,
            "split": split, "profile_path": str(profile_path),
            "profile_sha256": sha256_file(profile_path), "profile": profile,
            "model_alias": profile.get("model_alias"),
            "implementation_fingerprint": _implementation_fingerprint(),
            "m4_runtime_policy_sha256": sha256_file(policy_path),
            "bootstrap_snapshot_id": snapshot.snapshot_id,
            "input_manifest_sha256": case.provenance.get("input_manifest_sha256"),
            "public_digest_sha256": digest["digest_sha256"],
            "public_digest_bytes": digest["digest_bytes"],
            "case_data_sha256": case.telemetry.data_sha256,
            "metric_policy_sha256": case.provenance.get("metric_policy_sha256"),
            "kpi_policy_sha256": case.provenance.get("dev_kpi_policy_sha256"),
            "source_hashes": dict(case.telemetry.file_sha256),
            "static_repairs_per_attempt": policy["serving"]["static_repair_limit_per_attempt"],
            "maximum_diagnostic_attempts": policy["serving"]["maximum_diagnostic_attempts"],
            "evolution_protocol_version": policy["serving"]["evolution_protocol_version"],
            "evolve_and_serve_shared_static_repairs": policy["serving"]["maximum_stage3_static_repairs"],
            "maximum_model_calls": policy["serving"]["maximum_model_calls"],
            "maximum_aggregate_generated_tokens": policy["serving"]["maximum_aggregate_generated_tokens"],
            "typed_ir_capability_report_path": (str(paper_ir_capability_report_path.relative_to(run_dir))
                if paper_ir_capability_report_path is not None else None),
            "typed_ir_capability_report_sha256": paper_ir_capability_report_sha256,
            "typed_ir_capability_prompt_summary_sha256": paper_ir_capability_summary_sha256,
            "acceptance_contract": policy["serving"]["acceptance_contract"],
            "started_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "diagnostic_manifest.json", run_doc)
        _write_json(run_dir / "public_case_digest.json", digest)
        if model is None:
            raise M4ControllerError("model_unavailable", "M4 model client is unavailable")

        def write_stage3_diagnostics(*, gap_record: Mapping[str, Any] | None,
                new_specs: list[Mapping[str, Any]], dag_document: Mapping[str, Any] | None,
                verified: VerifiedDAG | None, execution: Any | None,
                failure: ExecutionFailure | None,
                preverification_error: Mapping[str, Any] | None,
                runtime: RuntimeContext, active_registry: OperatorRegistry,
                composite_expansion: Any | None = None) -> None:
            nonlocal stage3_diagnostics_written
            from .m4_evolution_diagnostics import build_evolution_contribution_diagnostics
            artifact = build_evolution_contribution_diagnostics(
                gap_record=gap_record, new_specs=new_specs, dag_document=dag_document,
                verified=verified, execution=execution, failure=failure,
                cache=CacheStore(run_dir / "cache", enabled=True), runtime=runtime,
                registry=active_registry, run_dir=run_dir,
                preverification_error=preverification_error, case_id=case_id,
                composite_expansion=composite_expansion)
            _write_json(run_dir / "evolution_contribution_diagnostics.json", artifact,
                        exclusive=False)
            stage3_diagnostics_written = True

        stage_names = policy["serving"]["stages"]
        for stage_index, stage in enumerate(stage_names, start=1):
            if time.perf_counter() - started > float(policy["serving"]["case_wall_seconds"]):
                stage_rows.append({"stage": stage, "status": "failed_budget",
                                   "error": {"code": "case_wall_budget", "message": "case wall-time budget exhausted"}})
                break
            runtime = build_m4_runtime(case.alert, root=root, policy_path=policy_path)
            stage_name = f"stage{stage_index}_{stage}"
            stage_dir = run_dir / "attempts" / stage_name
            stage_dir.mkdir()
            _write_json(status_path, {"phase": stage_name, "status": "running"}, exclusive=False)
            admission_checks_path = stage_dir / "admission_checks.jsonl"
            evolved_gap: dict[str, Any] | None = None
            normalized_new_specs: list[dict[str, Any]] = []
            version_map: dict[tuple[str, str], str] = {}
            candidate_registry: OperatorRegistry | None = None
            candidate_primitive_registry: OperatorRegistry | None = None
            composite_expansion: Any | None = None
            logical_document: dict[str, Any] | None = None
            expanded_document: dict[str, Any] | None = None
            evolve_repairs = 0
            if stage == "synthesize":
                stage3_entered = True
                admission_checks_path.touch(exist_ok=False)
                write_stage3_diagnostics(gap_record={}, new_specs=[], dag_document=None,
                    verified=None, execution=None, failure=None,
                    preverification_error={"code": "evolution_in_progress",
                        "message": "stage-3 evolution phase has not produced a validated gap yet"},
                    runtime=runtime, active_registry=registry)
                evolve_errors: list[dict[str, Any]] = []
                composite_validation_checks: list[dict[str, Any]] = []
                evolve_call_id: str | None = None
                evolve_prompt = _stage_prompt("evolve", digest=digest, registry=registry,
                    snapshot=snapshot, runtime=runtime, history=history, policy=policy,
                    query_budget=query_budget)
                evolve_previous = ""
                evolved_ok = False
                while not evolved_ok:
                    if time.perf_counter() - started > float(policy["serving"]["case_wall_seconds"]):
                        evolve_errors.append({"code": "case_wall_budget", "message": "case wall-time budget exhausted"})
                        break
                    request_cap = int(getattr(model, "max_generation_tokens", profile["max_generation_tokens"]))
                    allowance = _generation_allowance(calls,
                        model_call_limit=int(policy["serving"]["maximum_model_calls"]),
                        token_budget=int(policy["serving"]["maximum_aggregate_generated_tokens"]),
                        per_call_limit=request_cap)
                    if allowance <= 0 or (not isinstance(model, M4ModelClient) and allowance < request_cap):
                        evolve_errors.append({"code": "generated_token_budget", "message": "aggregate generated-token or model-call budget exhausted"})
                        break
                    call_attempt = evolve_repairs + 1
                    prompt_path = run_dir / "prompts" / f"stage3_evolve_call{call_attempt}.txt"
                    prompt_path.write_text(evolve_prompt, encoding="utf-8")
                    if isinstance(model, M4ModelClient):
                        call = model.complete_for_stage("evolve", evolve_prompt,
                            task_id=f"m4_{case_id}_stage3_evolve", attempt=call_attempt,
                            max_tokens=allowance)
                    else:
                        call = model.complete(evolve_prompt,
                            task_id=f"m4_{case_id}_stage3_evolve", attempt=call_attempt)
                    call_doc = call.as_dict()
                    call_doc.update({"m4_stage": stage_name, "m4_phase": "evolve",
                        "m4_static_repair": evolve_repairs > 0,
                        "m4_requested_generation_tokens": allowance,
                        "prompt_file": str(prompt_path.relative_to(run_dir))})
                    calls.append(call_doc)
                    _append_jsonl(calls_path, call_doc)
                    evolve_call_id = call.call_id
                    if call.error or call.response_metadata.get("finish_reason") in {"length", "max_tokens"}:
                        error = call.error or {"code": "model_output_truncated", "message": "evolution response ended at token limit"}
                        evolve_errors.append(error)
                        break
                    evolve_previous = call.content or ""
                    try:
                        evolution_response, _ = parse_strict_json_object(evolve_previous)
                        if set(evolution_response) != {"gap_record", "operators"}:
                            raise M4ControllerError("stage3_evolve_schema", "evolve response must contain exactly gap_record and operators", static=True)
                        raw_specs = evolution_response["operators"]
                        if not isinstance(raw_specs, list) or len(raw_specs) > int(policy["serving"].get("maximum_new_operators", 3)):
                            raise M4ControllerError("stage3_operator_budget", "Stage 3 operator proposals exceed the frozen limit", static=True)
                        normalized_new_specs, version_map = _candidate_spec_mapping(
                            raw_specs, registry, runtime, active_composite_specs,
                            specs,
                            allowed_authoring_profiles=(
                                (paper_authoring_profile,) if paper_profile else
                                ("typed-operator-ir-v1",)))
                        for ir_spec in normalized_new_specs:
                            identity_hash = ir_spec.get("program_sha256") or ir_spec.get("source_sha256")
                            composite_validation_checks.append({"operator": ir_spec["name"],
                                "version": ir_spec["version"],
                                "program_sha256": ir_spec.get("program_sha256"),
                                "source_sha256": ir_spec.get("source_sha256"), "status": "passed"})
                            _append_jsonl(admission_checks_path, {
                                "phase": "evolve", "call_attempt": call_attempt,
                                "call_id": evolve_call_id, "operator": ir_spec["name"],
                                "source_sha256": identity_hash,
                                "check_kind": ("typed_python_static" if ir_spec.get("authoring_profile") == "typed-operator-code-v1"
                                    else "closed_program_static"), "status": "passed",
                                "code": None, "line": None, "column": None,
                                "elapsed_ms": 0, "cache_hit": False,
                                "checker_context_sha256": None})
                        normalized_gap_draft = _normalize_gap_record_draft(
                            evolution_response["gap_record"], normalized_new_specs, registry, history)
                        evolved_gap = _validate_gap_record(normalized_gap_draft,
                            normalized_new_specs, registry, history)
                        entity_literals = set(case.telemetry.deployment_catalog) | {case_id}
                        for composite in normalized_new_specs:
                            _reject_program_identifier_literals(composite, entity_literals)
                        new_keys = {f"{spec['name']}@{spec['version']}" for spec in normalized_new_specs}
                        existing_keys = {f"{spec['name']}@{spec['version']}" for spec in specs}
                        if new_keys & existing_keys:
                            raise M4ControllerError("stage3_operator_collision", "Stage 3 proposed an already-retained operator version", static=True)
                        if paper_profile:
                            from .paper_starter import make_paper_starter_registry
                            candidate_active_keys = set(primitive_active_keys) | {
                                f"{spec['name']}@{spec['version']}" for spec in normalized_new_specs}
                            candidate_primitive_registry = make_paper_starter_registry(bank_only=True,
                                starter_input=paper_starter_input,
                                generated_specs=[*legacy_specs, *normalized_new_specs],
                                audit_dir=run_dir / "audit", active_operator_keys=candidate_active_keys,
                                root=root)
                        else:
                            candidate_primitive_registry = make_m4_registry(case, legacy_specs,
                                root=root, audit_dir=run_dir / "audit", shared_query_budget=query_budget)
                        if normalized_new_specs and not paper_profile:
                            register_generated_specs(candidate_primitive_registry, normalized_new_specs,
                                audit_dir=run_dir / "audit", sandbox_limits=generated_sandbox_limits(root))
                        candidate_registry = _logical_registry(candidate_primitive_registry,
                            active_composite_specs)
                        evolved_ok = True
                    except (StaticTaskError, DAGValidationError, M4ControllerError, ValueError) as exc:
                        error = exc.as_dict() if hasattr(exc, "as_dict") else {
                            "code": getattr(exc, "code", "static_validation_failed"), "message": str(exc)}
                        if not getattr(exc, "static", True):
                            raise
                        evolve_errors.append(error)
                        if evolve_repairs >= int(policy["serving"]["maximum_stage3_static_repairs"]):
                            break
                        evolve_repairs += 1
                        repair_base = _stage_prompt("evolve", digest=digest, registry=registry,
                            snapshot=snapshot, runtime=runtime, history=history, policy=policy,
                            query_budget=query_budget, include_case_digest=False, include_history=True)
                        evolve_prompt = _static_repair_prompt(repair_base, evolve_previous, error)
                _write_json(stage_dir / "evolution_record.json", {
                    "schema_version": "e4-stage3-evolution-record/1",
                    "status": ("no_extension" if evolved_ok and not normalized_new_specs
                        else "validated" if evolved_ok else "failed"),
                    "gap_record": evolved_gap, "operators": [
                        {key: value for key, value in spec.items() if key != "source"}
                        for spec in normalized_new_specs],
                    "composite_validation_checks": composite_validation_checks,
                    "static_repairs": evolve_repairs, "static_errors": evolve_errors})
                if evolved_ok:
                    for composite in normalized_new_specs:
                        safe_name = composite["name"].replace(".", "_")
                        if isinstance(composite.get("source"), str):
                            source_path = run_dir / "operators" / (
                                f"{safe_name}_{composite['source_sha256'][:16]}.py")
                            source_path.parent.mkdir(parents=True, exist_ok=True)
                            source_path.write_text(composite["source"], encoding="utf-8")
                        else:
                            program_path = run_dir / "operators" / (
                                f"{safe_name}_{composite['program_sha256'][:16]}.json")
                            _write_json(program_path, composite)
                if not evolved_ok:
                    write_stage3_diagnostics(gap_record=evolved_gap or {},
                        new_specs=normalized_new_specs, dag_document=None, verified=None,
                        execution=None, failure=None,
                        preverification_error=evolve_errors[-1] if evolve_errors else {
                            "code": "evolution_failed", "message": "evolution phase did not validate"},
                        runtime=runtime, active_registry=registry)
                    failed_row = {"stage": stage_name, "status": "evolution_failed",
                        "static_repairs": evolve_repairs, "static_errors": evolve_errors,
                        "evolve_status": "failed", "evolve_static_repairs": evolve_repairs,
                        "stage3_serve_static_repairs": 0,
                        "static_repair_budget": int(policy["serving"]["maximum_stage3_static_repairs"])}
                    stage_rows.append(failed_row)
                    _write_json(stage_dir / "stage_record.json", failed_row)
                    history.append({"stage": stage_name, "status": "evolution_failed", "accepted": None})
                    continue
            serving_phase = "stage3_serve" if stage == "synthesize" else stage
            serving_registry = candidate_registry if stage == "synthesize" else registry
            base_prompt = _stage_prompt(serving_phase, digest=digest, registry=serving_registry,
                snapshot=snapshot, runtime=runtime, history=history, policy=policy,
                query_budget=query_budget, gap_record=evolved_gap,
                composite_operator_specs=[spec for spec in [*specs, *normalized_new_specs]
                    if spec.get("authoring_profile") == "operator-dag-v1"])
            prompt = base_prompt
            repair_count = evolve_repairs
            phase_repair_count = 0
            previous = ""
            static_errors: list[dict[str, Any]] = []
            document: dict[str, Any] | None = None
            verified: VerifiedDAG | None = None
            current_plan_summary: dict[str, Any] | None = None
            runtime_for_stage = runtime
            diagnostic_dag_doc: Mapping[str, Any] | None = None
            input_capability_checks: list[dict[str, Any]] = []
            current_adaptation_record: dict[str, Any] | None = None
            while True:
                # A failed repair must never leave a previously verified candidate eligible to run.
                document = None
                verified = None
                playbook_name = None
                current_adaptation_record = None
                if time.perf_counter() - started > float(policy["serving"]["case_wall_seconds"]):
                    stage_rows.append({"stage": stage_name, "status": "failed_budget",
                        "static_repairs": repair_count,
                        "static_errors": static_errors,
                        "error": {"code": "case_wall_budget", "message": "case wall-time budget exhausted"}})
                    break
                call_attempt = phase_repair_count + 1
                phase_name = "stage3_serve" if stage == "synthesize" else stage_name
                prompt_path = run_dir / "prompts" / f"{phase_name}_call{call_attempt}.txt"
                prompt_path.write_text(prompt, encoding="utf-8")
                request_cap = int(getattr(model, "max_generation_tokens", profile["max_generation_tokens"]))
                allowance = _generation_allowance(calls,
                    model_call_limit=int(policy["serving"]["maximum_model_calls"]),
                    token_budget=int(policy["serving"]["maximum_aggregate_generated_tokens"]),
                    per_call_limit=request_cap)
                if allowance <= 0 or (not isinstance(model, M4ModelClient) and allowance < request_cap):
                    stage_rows.append({"stage": stage_name, "status": "failed_budget",
                        "static_repairs": repair_count, "static_errors": static_errors,
                        "error": {"code": "generated_token_budget", "message": "aggregate generated-token or model-call budget exhausted"}})
                    break
                if isinstance(model, M4ModelClient):
                    call = model.complete_for_stage(serving_phase, prompt,
                        task_id=f"m4_{case_id}_{phase_name}", attempt=call_attempt,
                        max_tokens=allowance)
                else:
                    call = model.complete(prompt, task_id=f"m4_{case_id}_{phase_name}",
                                          attempt=call_attempt)
                call_doc = call.as_dict()
                call_doc["m4_stage"] = stage_name
                call_doc["m4_phase"] = serving_phase
                call_doc["m4_static_repair"] = repair_count > 0
                call_doc["m4_requested_generation_tokens"] = allowance
                call_doc["prompt_file"] = str(prompt_path.relative_to(run_dir))
                calls.append(call_doc)
                _append_jsonl(calls_path, call_doc)
                if call.error:
                    stage_rows.append({"stage": stage_name, "status": "model_call_failed",
                        "static_repairs": repair_count, "static_errors": static_errors,
                        "call_id": call.call_id,
                        "error": call.error})
                    break
                if call.response_metadata.get("finish_reason") in {"length", "max_tokens"}:
                    stage_rows.append({"stage": stage_name, "status": "truncated",
                        "static_repairs": repair_count, "static_errors": static_errors,
                        "call_id": call.call_id,
                        "error": {"code": "model_output_truncated", "message": "response ended at token limit"}})
                    break
                previous = call.content or ""
                try:
                    response, _fenced = parse_strict_json_object(previous)
                    if stage == "synthesize" and isinstance(response.get("dag"), Mapping):
                        diagnostic_dag_doc = response["dag"]
                    if stage == "select":
                        if set(response) != {"playbook", "runtime_overrides", "node_parameters", "node_bindings"}:
                            raise M4ControllerError("stage1_schema", "Stage 1 response keys differ from the frozen schema", static=True)
                        runtime_for_stage = _normalize_overrides(response, case, root, policy)
                        document, playbook_name = _stage1_doc(response, snapshot, registry, runtime_for_stage)
                        if paper_profile and playbook_name not in set(policy.get("_paper_case_available_books", ())):
                            raise M4ControllerError("paper_book_unsupported",
                                "Stage 1 selected a playbook whose required case capability is unavailable", static=True)
                        logical_verified = _validate_graph(document, registry, runtime_for_stage, stage=stage)
                        composite_expansion = _expand_composite_document(document,
                            active_composite_specs, registry, runtime_for_stage)
                        expanded_document = composite_expansion.dag if composite_expansion else document
                        verified = (_validate_graph(expanded_document, primitive_registry,
                            runtime_for_stage, stage=stage) if composite_expansion else logical_verified)
                        active_typed_specs = [spec for spec in specs
                            if spec.get("authoring_profile") in {"typed-records-v1", "typed-operator-code-v1", "typed-operator-ir-v1", "typed-operator-ir-v2"}
                            or isinstance(spec.get("input_requirements"), Mapping)]
                        try:
                            input_capability_checks = check_typed_input_capabilities(
                                verified, active_typed_specs)
                        except InputCapabilityError as exc:
                            raise M4ControllerError(exc.code, str(exc), static=True,
                                details=exc.details) from exc
                    elif stage == "compose":
                        if paper_profile:
                            expected_stage2 = {"schema_version", "adaptation_record", "dag", "runtime_overrides"}
                            if (set(response) != expected_stage2 or
                                    response.get("schema_version") != PAPER_STAGE2_RESPONSE_SCHEMA):
                                raise M4ControllerError("paper_stage2_schema",
                                    "paper Stage 2 requires e4-paper-stage2-adaptation/1; legacy M4 keeps the separate v1 response schema",
                                    static=True)
                            document = response["dag"]
                            current_adaptation_record = _validate_paper_adaptation_record(
                                response["adaptation_record"], history, document, registry)
                        else:
                            if set(response) != {"dag", "runtime_overrides"}:
                                raise M4ControllerError("stage2_schema", "Stage 2 response keys differ from the frozen schema", static=True)
                            document = response["dag"]
                        runtime_for_stage = _normalize_overrides(response, case, root, policy)
                        logical_verified = _validate_graph(document, registry, runtime_for_stage, stage=stage)
                        composite_expansion = _expand_composite_document(document,
                            active_composite_specs, registry, runtime_for_stage)
                        expanded_document = composite_expansion.dag if composite_expansion else document
                        verified = (_validate_graph(expanded_document, primitive_registry,
                            runtime_for_stage, stage=stage) if composite_expansion else logical_verified)
                        if paper_profile:
                            _reject_exact_prior_paper_plan(expanded_document, history)
                        active_typed_specs = [spec for spec in specs
                            if spec.get("authoring_profile") in {"typed-records-v1", "typed-operator-code-v1", "typed-operator-ir-v1", "typed-operator-ir-v2"}
                            or isinstance(spec.get("input_requirements"), Mapping)]
                        try:
                            input_capability_checks = check_typed_input_capabilities(
                                verified, active_typed_specs)
                        except InputCapabilityError as exc:
                            raise M4ControllerError(exc.code, str(exc), static=True,
                                details=exc.details) from exc
                        playbook_name = None
                    else:
                        if set(response) != {"dag", "runtime_overrides"}:
                            raise M4ControllerError("stage3_serve_schema", "Stage 3 serving response must contain exactly dag and runtime_overrides", static=True)
                        if candidate_registry is None or evolved_gap is None:
                            raise M4ControllerError("stage3_evolution_missing", "Stage 3 serving requires a validated evolution result", static=True)
                        runtime_for_stage = _normalize_overrides(response, case, root, policy)
                        new_keys = {f"{spec['name']}@{spec['version']}" for spec in normalized_new_specs}
                        resolved_doc = _resolve_generated_versions(response["dag"], version_map)
                        candidate_document = resolved_doc
                        _validate_graph(candidate_document, candidate_registry,
                            runtime_for_stage, stage="stage3_serve",
                            new_operator_keys=new_keys if new_keys else None)
                        all_composites = active_composite_specs
                        composite_expansion = _expand_composite_document(candidate_document,
                            all_composites, candidate_registry, runtime_for_stage)
                        expanded_document = (composite_expansion.dag if composite_expansion
                            else candidate_document)
                        candidate_verified = _validate_graph(expanded_document,
                            candidate_primitive_registry, runtime_for_stage,
                            stage="stage3_serve")
                        previous_expressions: dict[str, list[dict[str, Any]]] = defaultdict(list)
                        for prior in history:
                            prior_plan = prior.get("_expanded_plan")
                            if not isinstance(prior_plan, Mapping):
                                continue
                            prior_values = prior_plan.get("runtime_values")
                            prior_dag = prior_plan.get("dag")
                            prior_data_sha256 = prior_plan.get("case_data_sha256")
                            if (not isinstance(prior_values, Mapping) or not isinstance(prior_dag, Mapping) or
                                    not isinstance(prior_data_sha256, str)):
                                continue
                            for prior_node, expression_hash in _expanded_expression_hashes(
                                    prior_dag, candidate_primitive_registry, prior_values,
                                    case_data_sha256=prior_data_sha256).items():
                                if expression_hash is not None:
                                    row = next((item for item in prior_dag.get("nodes", [])
                                        if isinstance(item, Mapping) and item.get("id") == prior_node), {})
                                    previous_expressions[expression_hash].append({
                                        "stage": prior.get("stage"), "operator": row.get("operator"),
                                        "version": row.get("version")})
                        current_values = runtime_for_stage.as_dict()["values"]
                        current_expressions = _expanded_expression_hashes(
                            expanded_document, candidate_primitive_registry, current_values,
                            case_data_sha256=case.telemetry.data_sha256)
                        if normalized_new_specs and composite_expansion is not None:
                            for composite_call in composite_expansion.call_outputs.values() if composite_expansion else ():
                                composite_key = composite_call.get("composite")
                                if composite_key not in new_keys:
                                    continue
                                output_node = composite_call.get("output_node_id")
                                expression_hash = current_expressions.get(output_node)
                                if expression_hash is not None and expression_hash in previous_expressions:
                                    raise M4ControllerError("stage3_computational_duplicate",
                                        "expanded composite output exactly repeats a prior analyzed computation",
                                        static=True, details={"composite": composite_key,
                                            "output_node_id": output_node,
                                            "expression_sha256": expression_hash,
                                            "prior_matches": previous_expressions[expression_hash]})
                        elif not normalized_new_specs:
                            returned_expression = current_expressions.get(expanded_document.get("return", {}).get("node"))
                            if returned_expression is not None and returned_expression in previous_expressions:
                                raise M4ControllerError("stage3_computational_duplicate",
                                    "current-catalog serving DAG exactly repeats a prior analyzed answer computation",
                                    static=True, details={"expression_sha256": returned_expression,
                                        "prior_matches": previous_expressions[returned_expression]})
                        capability_started = time.perf_counter()
                        active_typed_specs = [spec for spec in [*specs, *normalized_new_specs]
                            if spec.get("authoring_profile") in {"typed-records-v1", "typed-operator-code-v1", "typed-operator-ir-v1", "typed-operator-ir-v2"}
                            or isinstance(spec.get("input_requirements"), Mapping)]
                        try:
                            input_capability_checks = check_typed_input_capabilities(
                                candidate_verified, active_typed_specs)
                        except InputCapabilityError as exc:
                            capability_elapsed = (time.perf_counter() - capability_started) * 1000
                            failing_node_id = exc.details.get("node_id")
                            failing_node = next((node for node in candidate_verified.nodes
                                if node.node_id == failing_node_id), None)
                            failing_spec = next((item for item in active_typed_specs
                                if failing_node is not None and item["name"] == failing_node.operator.name), None)
                            failed_check = {"node_id": failing_node_id,
                                "port": exc.details.get("port"), "status": "failed",
                                "code": exc.code, "message": str(exc),
                                "details": exc.details, "elapsed_ms": capability_elapsed}
                            input_capability_checks.append(failed_check)
                            _append_jsonl(admission_checks_path, {
                                "phase": "stage3_serve", "call_attempt": call_attempt,
                                "call_id": call.call_id,
                                "operator": failing_spec["name"] if failing_spec else None,
                                "source_sha256": failing_spec.get("source_sha256") if failing_spec else None,
                                "check_kind": "input_capability", "status": "failed",
                                "code": exc.code, "line": None, "column": None,
                                "elapsed_ms": capability_elapsed, "cache_hit": False,
                                "checker_context_sha256": None,
                                "details": exc.details})
                            raise M4ControllerError(exc.code, str(exc), static=True,
                                details={**exc.details, "elapsed_ms": capability_elapsed}) from exc
                        capability_elapsed = (time.perf_counter() - capability_started) * 1000
                        for cap_check in input_capability_checks:
                            cap_check["elapsed_ms"] = capability_elapsed
                            source_spec = next((item for item in active_typed_specs
                                if item["name"] == next((node.operator.name
                                    for node in candidate_verified.nodes
                                    if node.node_id == cap_check.get("node_id")), None)), None)
                            _append_jsonl(admission_checks_path, {
                                "phase": "stage3_serve", "call_attempt": call_attempt,
                                "call_id": call.call_id,
                                "operator": source_spec["name"] if source_spec else None,
                                "source_sha256": source_spec.get("source_sha256") if source_spec else None,
                                "check_kind": "input_capability", "status": cap_check["status"],
                                "code": None, "line": None, "column": None,
                                "elapsed_ms": capability_elapsed, "cache_hit": False,
                                "checker_context_sha256": None})
                        response["normalized_operators"] = normalized_new_specs
                        response["gap_record"] = evolved_gap
                        response["input_capability_checks"] = input_capability_checks
                        response["declared_to_resolved_versions"] = [
                            {"name": name, "declared_version": declared, "resolved_version": resolved}
                            for (name, declared), resolved in sorted(version_map.items())]
                        registry = candidate_registry
                        document, verified = candidate_document, candidate_verified
                        playbook_name = None
                    assert document is not None and verified is not None
                    break
                except (StaticTaskError, DAGValidationError, M4ControllerError, ValueError) as exc:
                    document = None
                    verified = None
                    playbook_name = None
                    error = exc.as_dict() if hasattr(exc, "as_dict") else {
                        "code": getattr(exc, "code", "static_validation_failed"), "message": str(exc)}
                    if not getattr(exc, "static", True):
                        raise
                    static_errors.append(error)
                    repair_limit = (int(policy["serving"]["maximum_stage3_static_repairs"])
                        if stage == "synthesize" else int(policy["serving"]["static_repair_limit_per_attempt"]))
                    if repair_count >= repair_limit:
                        stage_rows.append({"stage": stage_name, "status": "static_validation_failed",
                            "static_repairs": repair_count, "static_errors": static_errors,
                            "call_id": call.call_id,
                            **({"adaptation_record": _plain(current_adaptation_record)}
                                if current_adaptation_record is not None else {})})
                        break
                    repair_count += 1
                    phase_repair_count += 1
                    repair_context = _stage_prompt(serving_phase, digest=digest, registry=serving_registry,
                        snapshot=snapshot, runtime=runtime_for_stage, history=history, policy=policy,
                        query_budget=query_budget, include_case_digest=False, include_history=True,
                        gap_record=evolved_gap,
                        composite_operator_specs=[spec for spec in [*specs, *normalized_new_specs]
                            if spec.get("authoring_profile") == "operator-dag-v1"])
                    prompt = _static_repair_prompt(repair_context, previous, error)

            if document is None or verified is None:
                if stage == "synthesize":
                    phase_error = (static_errors[-1] if static_errors else
                        stage_rows[-1].get("error", {"code": stage_rows[-1].get("status", "serve_failed"),
                            "message": "serving phase did not produce a verified DAG"}))
                    write_stage3_diagnostics(gap_record=evolved_gap or {},
                        new_specs=normalized_new_specs, dag_document=diagnostic_dag_doc,
                        verified=None, execution=None, failure=None,
                        preverification_error=phase_error, runtime=runtime_for_stage,
                        active_registry=serving_registry,
                        composite_expansion=composite_expansion)
                    stage_rows[-1].update({"evolve_status": ("no_extension" if not normalized_new_specs else "validated"),
                        "evolve_static_repairs": evolve_repairs,
                        "stage3_serve_static_repairs": phase_repair_count,
                        "static_repair_budget": int(policy["serving"]["maximum_stage3_static_repairs"])})
                _write_json(stage_dir / "stage_record.json", stage_rows[-1], exclusive=False)
                history.append({"stage": stage_name, "status": stage_rows[-1].get("status", "failed"),
                                "accepted": None,
                                **({"adaptation_record": _plain(current_adaptation_record)}
                                    if current_adaptation_record is not None else {}),
                                **({"static_errors": _plain(static_errors)} if static_errors else {})})
                continue

            current_plan_summary = _plan_summary(document, registry, runtime_for_stage,
                                                 response.get("runtime_overrides", {}))

            dag_path = run_dir / "dags" / f"{stage_name}.json"
            _write_json(dag_path, document)
            execution_registry = (candidate_primitive_registry if stage == "synthesize"
                else primitive_registry)
            if execution_registry is None:
                raise M4ControllerError("composite_registry_missing",
                    "expanded graph has no primitive execution registry", static=False)
            expanded_dag_path = None
            expansion_path = None
            if composite_expansion is not None:
                expanded_dag_path = run_dir / "dags" / f"{stage_name}_expanded.json"
                _write_json(expanded_dag_path, expanded_document or document)
                expansion_path = stage_dir / "composite_expansion.json"
                _write_json(expansion_path, _expansion_document(composite_expansion))
            runtime_path = stage_dir / "runtime.json"
            _write_json(runtime_path, runtime_for_stage.as_dict())
            try:
                execution = SerialExecutor(execution_registry, CacheStore(run_dir / "cache",
                    enabled=True)).execute_verified(verified, runtime_for_stage,
                    {next(node.node_id for node in verified.nodes if node.operator.external_input): case.telemetry},
                    case_id=case_id, data_sha256=case.telemetry.data_sha256,
                    file_sha256=case.telemetry.file_sha256, cache_enabled=True)
            except ExecutionFailure as exc:
                if stage == "synthesize":
                    write_stage3_diagnostics(gap_record=evolved_gap or {},
                        new_specs=normalized_new_specs, dag_document=document,
                        verified=verified, execution=None, failure=exc,
                        preverification_error=None, runtime=runtime_for_stage,
                        active_registry=registry,
                        composite_expansion=composite_expansion)
                used_operator_keys.update(f"{record.operator}@{record.version}" for record in exc.records
                    if record.status in {"success", "failed"})
                failure_execution = exc.as_dict()
                public_analysis = (_paper_public_analysis_evidence(None, failure_execution,
                    input_capability_checks) if paper_profile else None)
                result_record = {"stage": stage_name, "status": "runtime_execution_failed",
                    "static_repairs": repair_count, "static_errors": static_errors,
                    "evolve_status": (("no_extension" if not normalized_new_specs else "validated")
                        if stage == "synthesize" else None),
                    "evolve_static_repairs": evolve_repairs if stage == "synthesize" else 0,
                    "stage3_serve_static_repairs": phase_repair_count if stage == "synthesize" else 0,
                    "input_capability_checks": input_capability_checks if stage == "synthesize" else [],
                    "dag_sha256": sha256_file(dag_path),
                    "expanded_dag_sha256": sha256_file(expanded_dag_path) if expanded_dag_path else sha256_file(dag_path),
                    "composite_expansion_file": str(expansion_path.relative_to(run_dir)) if expansion_path else None,
                    "composite_expansion_report": _plain(composite_expansion.report) if composite_expansion else None,
                    "runtime": runtime_for_stage.as_dict(),
                    "execution": failure_execution,
                    **({"adaptation_record": _plain(current_adaptation_record),
                        "public_analysis_evidence": public_analysis} if paper_profile else {})}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                history.append({"stage": stage_name, "status": "execution_failed",
                                "accepted": None, "runtime": runtime_for_stage.as_dict(),
                                "plan_summary": current_plan_summary,
                                **({"adaptation_record": _plain(current_adaptation_record),
                                    "public_analysis_evidence": public_analysis} if paper_profile else {})})
                continue

            used_operator_keys.update(f"{node.operator.name}@{node.operator.version}" for node in verified.nodes)
            if composite_expansion is not None:
                used_operator_keys.update(row["composite"]
                    for row in composite_expansion.call_outputs.values()
                    if isinstance(row, Mapping) and isinstance(row.get("composite"), str))
            if playbook_name is not None:
                used_playbooks.add(playbook_name)
            execution_path = stage_dir / "execution.json"
            execution_doc = execution_result_to_dict(execution)
            public_analysis = (_paper_public_analysis_evidence(execution.answer, execution_doc,
                input_capability_checks) if paper_profile else None)
            _write_json(execution_path, execution_doc)
            if stage == "synthesize":
                write_stage3_diagnostics(gap_record=evolved_gap or {},
                    new_specs=normalized_new_specs, dag_document=document,
                    verified=verified, execution=execution, failure=None,
                    preverification_error=None, runtime=runtime_for_stage,
                    active_registry=registry,
                    composite_expansion=composite_expansion)
            if not isinstance(execution.answer, RankedAnswer):
                result_record = {"stage": stage_name, "status": "wrong_answer_type",
                    "static_repairs": repair_count, "dag_sha256": sha256_file(dag_path),
                    "execution": execution_doc,
                    **({"adaptation_record": _plain(current_adaptation_record),
                        "public_analysis_evidence": public_analysis} if paper_profile else {})}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                history.append({"stage": stage_name, "status": "invalid_answer", "accepted": None,
                    "plan_summary": current_plan_summary, "_expanded_plan": {
                        "dag": _plain(expanded_document or document),
                        "runtime_values": runtime_for_stage.as_dict()["values"],
                        "case_data_sha256": case.telemetry.data_sha256},
                    **({"adaptation_record": _plain(current_adaptation_record),
                        "public_analysis_evidence": public_analysis} if paper_profile else {})})
                continue
            try:
                validate_bank_answer(execution.answer, case,
                    maximum_leads=int(policy["serving"]["maximum_leads"]))
            except M4ControllerError as exc:
                result_record = {"stage": stage_name, "status": "invalid_answer_output",
                    "static_repairs": repair_count, "dag_sha256": sha256_file(dag_path),
                    "execution": execution_doc, "error": exc.as_dict(),
                    **({"adaptation_record": _plain(current_adaptation_record),
                        "public_analysis_evidence": public_analysis} if paper_profile else {})}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                history.append({"stage": stage_name, "status": "invalid_answer_output",
                    "accepted": None, "error_code": exc.code,
                    "plan_summary": current_plan_summary, "_expanded_plan": {
                        "dag": _plain(expanded_document or document),
                        "runtime_values": runtime_for_stage.as_dict()["values"],
                        "case_data_sha256": case.telemetry.data_sha256},
                    **({"adaptation_record": _plain(current_adaptation_record),
                        "public_analysis_evidence": public_analysis} if paper_profile else {})})
                continue
            try:
                feedback_result = _call_feedback(feedback, case_id, execution.answer)
            except Exception as exc:
                result_record = {"stage": stage_name, "status": "feedback_failed",
                    "static_repairs": repair_count, "dag_sha256": sha256_file(dag_path),
                    "execution": execution_doc,
                    "error": {"code": getattr(exc, "code", "feedback_failed"), "message": str(exc)},
                    **({"adaptation_record": _plain(current_adaptation_record),
                        "public_analysis_evidence": public_analysis} if paper_profile else {})}
                stage_rows.append(result_record)
                _write_json(stage_dir / "stage_record.json", result_record)
                raise M4ControllerError("feedback_failed", "trusted evaluator did not return a valid binary response") from exc
            accepted = feedback_result["accepted"]
            final_answer = execution.answer
            result_record = {"stage": stage_name, "status": "accepted" if accepted else "rejected",
                "accepted": accepted, "static_repairs": repair_count, "static_errors": static_errors,
                "evolve_status": (("no_extension" if not normalized_new_specs else "validated")
                    if stage == "synthesize" else None),
                "evolve_static_repairs": evolve_repairs if stage == "synthesize" else 0,
                "stage3_serve_static_repairs": phase_repair_count if stage == "synthesize" else 0,
                "input_capability_checks": input_capability_checks if stage == "synthesize" else [],
                "static_repair_budget": (int(policy["serving"]["maximum_stage3_static_repairs"])
                    if stage == "synthesize" else None),
                "evolution_protocol_version": (policy["serving"]["evolution_protocol_version"]
                    if stage == "synthesize" else None),
                "dag_sha256": sha256_file(dag_path),
                "expanded_dag_sha256": sha256_file(expanded_dag_path) if expanded_dag_path else sha256_file(dag_path),
                "composite_expansion_file": str(expansion_path.relative_to(run_dir)) if expansion_path else None,
                "composite_expansion_report": _plain(composite_expansion.report) if composite_expansion else None,
                "playbook_name": playbook_name,
                "runtime": runtime_for_stage.as_dict(), "execution": execution_doc,
                "generated_operator_keys": (sorted(new_keys) if stage == "synthesize" else []),
                "query_budget": {"requests": query_budget.requests, "rows": query_budget.rows,
                                 "bytes": query_budget.bytes},
                **({"adaptation_record": _plain(current_adaptation_record),
                    "public_analysis_evidence": public_analysis} if paper_profile else {})}
            stage_rows.append(result_record)
            _write_json(stage_dir / "stage_record.json", result_record)
            history.append({"stage": stage_name, "status": "executed", "accepted": accepted,
                "ranked_answer": _bounded_answer_history(execution.answer),
                "runtime_values": runtime_for_stage.as_dict()["values"],
                **({"adaptation_record": _plain(current_adaptation_record),
                    "public_analysis_evidence": public_analysis} if paper_profile else {}),
                "plan_summary": current_plan_summary, "_expanded_plan": {
                    "dag": _plain(expanded_document or document),
                    "runtime_values": runtime_for_stage.as_dict()["values"],
                    "case_data_sha256": case.telemetry.data_sha256}})

            if accepted:
                if stage == "synthesize":
                    graph_hash = playbook_graph_hash(document)
                    generated_playbook = {"name": f"accepted_m4_{graph_hash[:12]}",
                        "document": document,
                        "provenance": {"source": "accepted_stage3", "model_alias": profile.get("model_alias"),
                            "stage": stage_name, "verified_graph_sha256": graph_hash}}
                    reachable = {f"{node.operator.name}@{node.operator.version}" for node in verified.nodes}
                    if composite_expansion is not None:
                        reachable.update(row["composite"]
                            for row in composite_expansion.call_outputs.values()
                            if isinstance(row, Mapping) and isinstance(row.get("composite"), str))
                    admitted = admit_after_accepted(snapshot, response["normalized_operators"],
                        [generated_playbook], {"model_alias": profile.get("model_alias"),
                            "run_manifest_sha256": _canonical_hash(run_doc),
                            "implementation_fingerprint": _implementation_fingerprint()},
                        accepted=True, verified_reachable_operator_keys=reachable,
                        invocation_case_id=case_id, used_operator_keys=used_operator_keys,
                        used_playbook_names=used_playbooks | {generated_playbook["name"]},
                        verified_playbook_graph_hashes={graph_hash})
                    snapshot = admitted
                    last_snapshot = admitted
                else:
                    snapshot = record_library_invocation(snapshot,
                        invocation_case_id=case_id, used_operator_keys=used_operator_keys,
                        used_playbook_names=used_playbooks)
                    last_snapshot = snapshot
                break

        if not accepted and snapshot is not None:
            known = set(snapshot.operators)
            snapshot = record_library_invocation(snapshot, invocation_case_id=case_id,
                used_operator_keys=used_operator_keys & known,
                used_playbook_names=used_playbooks & set(snapshot.playbooks))
            last_snapshot = snapshot

        complete = {"schema_version": "e4-m4-diagnostic-run/1", "case_id": case_id,
            "status": "accepted" if accepted else "exhausted_or_failed",
            "accepted": accepted,
            "accepted_stage": next((row["stage"] for row in stage_rows if row.get("accepted") is True), None),
            "stages": stage_rows, "model_calls": _model_cost(calls),
            "query_budget": {"requests": query_budget.requests, "rows": query_budget.rows,
                             "bytes": query_budget.bytes},
            "library_before_snapshot_id": source_snapshot.snapshot_id,
            "library_after_snapshot_id": last_snapshot.snapshot_id if last_snapshot else None,
            "implementation_fingerprint": _implementation_fingerprint(),
            "wall_seconds": time.perf_counter() - started,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "diagnostic_result.json", complete)
        _write_json(status_path, {"phase": "complete", "status": complete["status"]}, exclusive=False)
        return {"result": complete, "snapshot": last_snapshot,
                "run_dir": str(run_dir), "answer": final_answer}
    except Exception as exc:
        error = {"code": getattr(exc, "code", "m4_controller_failed"), "message": str(exc)}
        if stage3_entered and not stage3_diagnostics_written and "write_stage3_diagnostics" in locals():
            try:
                write_stage3_diagnostics(gap_record=locals().get("evolved_gap") or {},
                    new_specs=locals().get("normalized_new_specs", []),
                    dag_document=locals().get("diagnostic_dag_doc"),
                    verified=locals().get("verified"), execution=None,
                    failure=None, preverification_error=error,
                    runtime=locals().get("runtime_for_stage") or locals().get("runtime"),
                    active_registry=locals().get("registry"),
                    composite_expansion=locals().get("composite_expansion"))
            except Exception as diagnostic_error:
                _write_json(run_dir / "evolution_contribution_diagnostics.json", {
                    "schema_version": "e4-evolution-contribution-diagnostics/1",
                    "status": "diagnostics_failed",
                    "error": {"type": type(diagnostic_error).__name__,
                              "message": str(diagnostic_error)[:500]},
                    "controller_error": error}, exclusive=False)
        failure = {"schema_version": "e4-m4-diagnostic-run/1", "case_id": case_id,
            "status": "failed", "accepted": False, "stages": stage_rows,
            "model_calls": _model_cost(calls), "error": error,
            "library_snapshot_id": last_snapshot.snapshot_id if last_snapshot else None,
            "wall_seconds": time.perf_counter() - started,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        if not (run_dir / "diagnostic_result.json").exists():
            _write_json(run_dir / "diagnostic_result.json", failure)
        _write_json(status_path, {"phase": "failed", "status": "failed", "error": error}, exclusive=False)
        raise
