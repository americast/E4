"""Validated registration for versioned, sandboxed M4 generated operators."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from .cache import canonical_json, deserialize_contract_json
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec, RuntimeResolutionError, _jsonable, _validate_parameter
from .sandbox import DEFAULT_LIMITS, run_generated_operator
from .sandbox_worker import (ALLOWED_MODULES, HELPER_API, HELPER_NAMES, SAFE_BUILTIN_NAMES,
                             helper_api_schema, generated_helper_guidance,
                             TYPED_RECORD_PROFILE, typed_record_helper_schema,
                             typed_record_helper_guidance,
                             validate_source as validate_sandbox_source)
from .operator_code_api import (PROFILE as TYPED_OPERATOR_CODE_PROFILE,
    MAX_SOURCE_BYTES as TYPED_OPERATOR_CODE_MAX_SOURCE_BYTES,
    operator_code_contract_hash, typed_operator_code_api, typed_operator_code_guidance)


def generated_contract_fields_guidance() -> str:
    """Shared concise guidance derived from the worker's typed helper registry."""
    return generated_helper_guidance()


def _published_typed_helper_api() -> dict[str, Any]:
    """Publish exact helper signatures once, projected from the worker API registry."""
    schema = helper_api_schema()
    return {"schema_version": schema["schema_version"],
        "helpers": [{key: row[key] for key in ("name", "signature", "returns")}
                    for row in HELPER_API],
        "wire_semantics": schema.get("wire_semantics", schema.get("wire_encoding", {}))}


GENERATED_NAME = re.compile(r"^evolved\.[a-z][a-z0-9_]{0,62}$")
VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
PORT_TYPES = {"Aggregate", "CandidateSet", "PaperStarterInput", "ScoredCandidates"}
TYPED_OPERATOR_CODE_PORT_TYPES = {"Aggregate", "CandidateSet", "ScoredCandidates", "Telemetry"}
OUTPUT_TYPES = {"Aggregate", "CandidateSet", "ScoredCandidates"}
PARAMETER_TYPES = {"string", "boolean", "integer", "positive_integer", "number", "string_list"}
SPEC_FIELDS = {"name", "version", "description", "family", "input_ports", "output_type",
               "parameters", "source"}
SPEC_OPTIONAL_FIELDS = {"authoring_profile", "input_requirements"}
TYPED_RECORDS_PROFILE = TYPED_RECORD_PROFILE
PARAM_FIELDS = {"type", "required", "nullable", "default", "choices", "runtime_field", "description"}
PORT_FIELDS = {"name", "type", "required", "variadic", "minimum_items", "maximum_items"}
MAX_SOURCE_BYTES = 65_536


class GeneratedSpecError(ValueError):
    """A proposed generated operator violates its published contract."""


def generated_spec_contract_schema() -> dict[str, Any]:
    """Return the JSON-ready public schema for a generated operator proposal.

    This describes the validator's existing grammar. Keep changes here aligned
    with ``validate_generated_spec``; prompt callers serialize this value as-is.
    """
    m4_policy = json.loads((Path(__file__).resolve().parents[1] /
        "config/m4_runtime.json").read_text(encoding="utf-8"))
    m4_limits = m4_policy["query_and_generation"]
    return {
        "schema_version": "e4-m4-generated-operator-spec/1",
        "top_level": {
            "required_fields": sorted(SPEC_FIELDS),
            "additional_fields": False,
            "fields": {
                "name": {"type": "string", "pattern": GENERATED_NAME.pattern,
                         "description": "Must use evolved.<lowercase_name>."},
                "version": {"type": "string", "pattern": VERSION.pattern,
                            "max_length": 64, "description": "Declared token; normalized version is source-bound."},
                "description": {"type": "string", "min_length": 1, "max_length": 1024},
                "family": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,63}$"},
                "input_ports": {"type": "array", "min_items": 1, "max_items": 8,
                                "items": "input_port"},
                "output_type": {"type": "string", "enum": sorted(OUTPUT_TYPES)},
                "parameters": {"type": "object", "max_properties": 24,
                               "property_name_pattern": "^[a-z][a-z0-9_]{0,47}$",
                               "additional_properties": "parameter"},
                "source": {"type": "string", "min_utf8_bytes": 1,
                           "max_utf8_bytes": MAX_SOURCE_BYTES,
                           "constraints": [
                               "Defines exactly one synchronous sandbox-valid operator(inputs, params, runtime).",
                               "Numeric constants other than 0 and 1 must be declared parameters or runtime fields.",
                           ]},
            },
        },
        "input_ports": {
            "container": "JSON array/list; mappings/objects are not accepted",
            "min_items": 1,
            "max_items": 8,
            "required_fields": ["name", "type"],
            "allowed_fields": sorted(PORT_FIELDS),
            "additional_fields": False,
            "fields": {
                "name": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,31}$", "unique": True},
                "type": {"type": "string", "enum": sorted(PORT_TYPES)},
                "required": {"type": "boolean", "default": True},
                "variadic": {"type": "boolean", "default": False},
                "minimum_items": {"type": "integer", "minimum": 0, "maximum": 64, "default": 1},
                "maximum_items": {"type": ["integer", "null"], "minimum": 1, "maximum": 64,
                                  "default": None},
            },
            "constraints": [
                "Non-variadic ports must use minimum_items=1 and maximum_items=null.",
                "For variadic ports, maximum_items must be null or at least max(1, minimum_items), and at most 64.",
                "An optional variadic port (required=false) must set minimum_items=0.",
            ],
        },
        "parameters": {
            "container": "JSON object/map",
            "max_properties": 24,
            "property_name_pattern": "^[a-z][a-z0-9_]{0,47}$",
            "entry_required_fields": ["type"],
            "entry_allowed_fields": sorted(PARAM_FIELDS),
            "additional_entry_fields": False,
            "entry_fields": {
                "type": {"type": "string", "enum": sorted(PARAMETER_TYPES)},
                "required": {"type": "boolean", "default": False},
                "nullable": {"type": "boolean", "default": False},
                "default": {"type": "JSON value", "default": "absent; when present, validated against type and choices"},
                "choices": {"type": "array of finite JSON values", "default": [],
                            "constraints": "Each choice must validate as the declared parameter type."},
                "runtime_field": {"type": ["string", "null"], "default": None,
                                  "pattern": "^[a-z][a-z0-9_]{0,63}$"},
                "description": {"type": "string", "default": "", "max_length": 512},
            },
            "value_semantics": {
                "string": "string",
                "boolean": "boolean (true/false; not integer 0/1)",
                "integer": "integer (booleans excluded)",
                "positive_integer": "integer greater than zero (booleans excluded)",
                "number": "finite integer or floating-point number (booleans excluded)",
                "string_list": "JSON array/list of strings",
                "null": "accepted only when nullable=true",
                "default_choice": "When choices is non-empty, a declared default must be one of its choices.",
            },
        },
        "output_types": sorted(OUTPUT_TYPES),
        "sandbox": {
            "forbidden_ast_constructs": ["ClassDef", "AsyncFunctionDef", "Lambda", "Global", "Nonlocal",
                "With", "AsyncWith", "Try", "Raise", "Delete", "Yield", "YieldFrom", "Await"],
            "forbidden_names": ["open", "eval", "exec", "compile", "globals", "locals", "getattr",
                "setattr", "delattr", "vars", "input", "breakpoint", "memoryview", "names beginning __"],
            "forbidden_attribute_access": "attribute names beginning with underscore",
            "allowed_builtins": sorted(SAFE_BUILTIN_NAMES),
            "allowed_modules": sorted(ALLOWED_MODULES),
            "provided_helpers": sorted(HELPER_NAMES),
            "typed_helper_api": _published_typed_helper_api(),
            "resource_limits": {
                "stdout_bytes": DEFAULT_LIMITS["stdout_bytes"],
                "stderr_bytes": DEFAULT_LIMITS["stderr_bytes"],
                "protocol_bytes": DEFAULT_LIMITS["protocol_bytes"],
                "standalone_input_bytes": DEFAULT_LIMITS["input_bytes"],
                "m4_serving_input_bytes": m4_limits["maximum_operator_input_bytes"],
                "m4_serving_wall_seconds": m4_limits["generated_operator_wall_seconds"],
                "m4_serving_memory_bytes": m4_limits["generated_operator_memory_bytes"],
                "m4_serving_cpu_seconds": m4_limits["generated_operator_cpu_seconds"],
                "input_metering": "UTF-8 bytes of the complete compact JSON request, including source, inputs, parameters, runtime, and limits",
            },
            "diagnostics": "static source errors identify the AST construct or name and its line and column; errors contain no case data or gold",
        },
    }


def typed_profile_contract_schema() -> dict[str, Any]:
    """Prompt schema for newly authored operators; omits legacy wire-envelope internals."""
    schema = generated_spec_contract_schema()
    optional_fields = {
        "authoring_profile": {"type": "string", "enum": [TYPED_RECORD_PROFILE],
            "meaning": "Selects the immutable typed-record authoring API; required for newly generated Stage 3 operators."},
        "input_requirements": {"type": "object", "required_for": TYPED_RECORD_PROFILE,
            "keyed_by": "every declared input port exactly once", "additional_fields": False,
            "port_fields": {"source_columns": "mapping of canonical table paths to arrays of published native column names",
                "statistics": "upstream statistics the source consumes; use raw for unaggregated rows",
                "group_by": "grouping dimensions the source provides",
                "support_unit": "required support unit string or null",
                "coverage": {"enum": ["any", "full_window", "page"],
                    "meaning": "page permits a bounded source page with explicit coverage metadata and never means complete-window coverage"}},
            "meaning": "Compared to trusted input producers before execution. This declaration does not prove arbitrary code honors it."}}
    return {"schema_version": schema["schema_version"],
        "required_fields": schema["top_level"]["required_fields"],
        "additional_fields": False,
        "optional_fields": optional_fields,
        "name": schema["top_level"]["fields"]["name"],
        "version": schema["top_level"]["fields"]["version"],
        "description": schema["top_level"]["fields"]["description"],
        "family": schema["top_level"]["fields"]["family"],
        "input_ports": schema["input_ports"],
        "output_types": schema["output_types"],
        "parameters": schema["parameters"],
        "source": schema["top_level"]["fields"]["source"],
        "sandbox": {key: schema["sandbox"][key] for key in (
            "forbidden_ast_constructs", "forbidden_names", "forbidden_attribute_access",
            "allowed_builtins", "allowed_modules", "resource_limits")},
        "authoring_profile": TYPED_RECORD_PROFILE,
        "typed_record_helper_api": typed_record_helper_schema(),
        "output_constraints": {
            "category": "null; Bank has no supported generated fault-category classifier",
            "evidence": "make_evidence requires explicit source, typed Window, typed contributors, support, and caller-computed ranking_eligible",
            "eligibility": "host validation remains authoritative; matching or a positive score alone does not establish eligibility",
            "allowed_sources": ["bank/metric_app.csv", "bank/metric_container.csv",
                "bank/trace_span.csv", "bank/log/log_service.csv",
                "bank/metric/metric_app.csv", "bank/metric/metric_container.csv",
                "bank/trace/trace_span.csv", "bank/log/log_service.csv"]}}


def typed_operator_code_contract_schema() -> dict[str, Any]:
    """Prompt schema for new Python operators over typed, bounded data APIs."""
    schema = typed_profile_contract_schema()
    schema["authoring_profile"] = TYPED_OPERATOR_CODE_PROFILE
    schema["optional_fields"]["authoring_profile"] = {
        "type": "string", "enum": [TYPED_OPERATOR_CODE_PROFILE],
        "meaning": "Selects statically checked Python operator authoring; required for new Stage 3 operators."}
    schema["optional_fields"]["input_requirements"]["required_for"] = TYPED_OPERATOR_CODE_PROFILE
    schema["optional_fields"]["input_requirements"]["port_fields"]["group_by_by_table"] = (
        "optional mapping from each required source table to its own nonempty grouping dimensions; use instead of flat group_by when tables expose different groups")
    schema["input_ports"]["fields"]["type"]["enum"] = sorted(TYPED_OPERATOR_CODE_PORT_TYPES)
    schema["source"]["max_utf8_bytes"] = TYPED_OPERATOR_CODE_MAX_SOURCE_BYTES
    schema["source"]["constraints"] = [
        "Defines exactly one synchronous operator(inputs, params, runtime) function.",
        "Obvious statistical decision cutoffs must use declared params/runtime fields; ordinary math constants and structural 0/1 checks are allowed."]
    output_api = typed_record_helper_schema()
    schema["typed_output_record_api"] = {
        "schema_version": output_api["schema_version"],
        "helpers": [row for row in output_api["helpers"] if row["name"].startswith("make_")],
        "record_fields": output_api["record_fields"],
        "semantics": "Use typed constructors for Aggregate, CandidateSet, and ScoredCandidates outputs; raw wire envelope construction and metadata unwrapping are not authoring APIs."}
    schema["typed_operator_code_api"] = typed_operator_code_api()
    schema["typed_operator_code_guidance"] = typed_operator_code_guidance()
    schema["output_constraints"]["source_provenance"] = (
        "The host pins validated source and computation identity from the registered spec; authored family labels do not create consensus groups.")
    return schema


def generated_sandbox_capability_text() -> str:
    """Format prompt guidance from the same lists installed in the worker."""
    return ("Allowed builtins are " + ", ".join(sorted(SAFE_BUILTIN_NAMES)) +
            "; allowed import modules are " + ", ".join(sorted(ALLOWED_MODULES)) +
            ". Imports must explicitly bind names such as math. Legacy typed helper signatures are in sandbox.typed_helper_api; immutable record helpers require authoring_profile='typed-records-v1' and are listed in sandbox.typed_record_helper_api")


def _json_value(value: Any, label: str) -> Any:
    try:
        json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise GeneratedSpecError(f"{label} must be finite JSON data: {exc}") from exc
    return value


def _check_declared_thresholds(source: str) -> None:
    """Conservatively require nontrivial numeric constants to use params/runtime."""
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise GeneratedSpecError(f"generated source has invalid syntax: {exc}") from exc
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and type(node.value) in (int, float)
                and node.value not in (0, 1)):
            raise GeneratedSpecError(
                "numeric constants other than 0 and 1 must be declared as operator parameters or runtime fields")


_STAT_DECISION_NAME = re.compile(
    r"(?:p[_-]?value|alpha|cutoff|threshold|support|significance|effect|minimum|"
    r"min[_-]?(?:support|samples?|blocks?|rows?)|sample[_-]?count|block[_-]?count|resamples?)",
    re.IGNORECASE)


def _check_code_statistical_cutoffs(source: str) -> None:
    """Reject simple authored statistical decision cutoffs; this is a narrow AST heuristic."""
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise GeneratedSpecError(f"generated source has invalid syntax: {exc}") from exc
    numeric_aliases: dict[str, int | float] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if isinstance(value, ast.Constant) and type(value.value) in (int, float):
                targets = (node.targets if isinstance(node, ast.Assign) else [node.target])
                for target in targets:
                    if isinstance(target, ast.Name):
                        numeric_aliases[target.id] = value.value
                        if _STAT_DECISION_NAME.search(target.id) and value.value not in (0, 1):
                            raise GeneratedSpecError(
                                "statistical decision cutoffs must come from declared params/runtime fields; ordinary math constants are allowed")
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if isinstance(value, ast.Name) and value.id in numeric_aliases:
                targets = (node.targets if isinstance(node, ast.Assign) else [node.target])
                for target in targets:
                    if isinstance(target, ast.Name):
                        numeric_aliases[target.id] = numeric_aliases[value.id]
                        if (_STAT_DECISION_NAME.search(target.id) and
                                numeric_aliases[target.id] not in (0, 1)):
                            raise GeneratedSpecError(
                                "statistical decision cutoffs must come from declared params/runtime fields; ordinary math constants are allowed")

    def name_of(node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Subscript):
            key = node.slice
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                return key.value
        return None

    def hardcoded_value(node: ast.AST) -> bool:
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            return node.value not in (0, 1)
        if isinstance(node, ast.Name) and node.id in numeric_aliases:
            return numeric_aliases[node.id] not in (0, 1)
        return False

    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        operands = [node.left, *node.comparators]
        for left, right in zip(operands, operands[1:]):
            left_name, right_name = name_of(left), name_of(right)
            if (_STAT_DECISION_NAME.search(left_name or "") and hardcoded_value(right) or
                    _STAT_DECISION_NAME.search(right_name or "") and hardcoded_value(left)):
                raise GeneratedSpecError(
                    "statistical decision cutoffs must come from declared params/runtime fields; ordinary math constants are allowed")


def validate_generated_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize the JSON contract for one generated operator.

    The source is never evaluated here. Syntax/signature/process isolation are
    enforced again by the generated-operator worker at execution time.
    """
    if (isinstance(spec, Mapping) and spec.get("authoring_profile") in
            {"typed-operator-ir-v1", "typed-operator-ir-v2"}):
        from .m4_operator_ir import normalize_operator_ir_spec, OperatorIRError
        compact_raw_fields = {"name", "version", "description", "family", "output_type",
            "parameters", "authoring_profile", "program"}
        legacy_raw_fields = compact_raw_fields | {"input_ports", "input_requirements"}
        normalized_fields = legacy_raw_fields | {"declared_version", "program_sha256", "source_sha256",
            "signature_sha256", "contract_hash", "kernel_abi_sha256", "interpreter_sha256", "policy_pins"}
        is_normalized = "declared_version" in spec
        if ((is_normalized and set(spec) != normalized_fields) or
                (not is_normalized and set(spec) not in (compact_raw_fields, legacy_raw_fields))):
            raise GeneratedSpecError("typed operator IR contains missing or unsupported fields")
        try:
            raw = {key: spec[key] for key in ("name", "description", "family", "input_ports",
                "output_type", "parameters", "authoring_profile", "input_requirements", "program")
                if key in spec}
            raw["version"] = spec.get("declared_version", spec.get("version"))
            canonical = normalize_operator_ir_spec(raw)
        except (KeyError, OperatorIRError, TypeError, ValueError) as exc:
            raise GeneratedSpecError(f"typed operator IR is invalid: {exc}") from exc
        if is_normalized:
            for field in ("program_sha256", "source_sha256", "signature_sha256",
                          "contract_hash", "kernel_abi_sha256", "interpreter_sha256", "policy_pins"):
                if spec[field] != canonical.get(field):
                    raise GeneratedSpecError(f"typed operator IR {field} does not match its contents")
            if (spec["version"] != canonical["version"] or
                    spec["declared_version"] != canonical["declared_version"]):
                raise GeneratedSpecError("typed operator IR version identity does not match its contents")
        return canonical
    normalized_fields = {"declared_version", "source_sha256", "signature_sha256", "contract_hash"}
    allowed_fields = SPEC_FIELDS | SPEC_OPTIONAL_FIELDS
    if (isinstance(spec, Mapping) and normalized_fields <= set(spec) and
            set(spec) <= allowed_fields | normalized_fields):
        declared = dict(spec)
        declared["version"] = declared.pop("declared_version")
        supplied_hashes = {name: declared.pop(name) for name in normalized_fields - {"declared_version"}}
        declared_parameters = {}
        for name, value in declared["parameters"].items():
            item = dict(value)
            has_default = item.pop("has_default", False)
            if not has_default:
                item.pop("default", None)
            declared_parameters[name] = item
        declared["parameters"] = declared_parameters
        canonical = validate_generated_spec(declared)
        if any(canonical[name] != value for name, value in supplied_hashes.items()):
            raise GeneratedSpecError("normalized generated spec hashes do not match its contents")
        return canonical
    if (not isinstance(spec, Mapping) or set(spec) - allowed_fields or
            not SPEC_FIELDS <= set(spec)):
        raise GeneratedSpecError(
            f"generated spec requires {sorted(SPEC_FIELDS)} and allows optional {sorted(SPEC_OPTIONAL_FIELDS)} only")
    authoring_profile = spec.get("authoring_profile")
    if authoring_profile is not None and authoring_profile not in {
            TYPED_RECORDS_PROFILE, TYPED_OPERATOR_CODE_PROFILE}:
        raise GeneratedSpecError(f"unsupported generated authoring_profile {authoring_profile!r}")
    input_requirements = spec.get("input_requirements")
    if authoring_profile in {TYPED_RECORDS_PROFILE, TYPED_OPERATOR_CODE_PROFILE} and input_requirements is None:
        raise GeneratedSpecError(f"{authoring_profile} requires input_requirements for every declared port")
    name, declared_version, description, family = (spec.get(key) for key in
        ("name", "version", "description", "family"))
    if not isinstance(name, str) or not GENERATED_NAME.fullmatch(name):
        raise GeneratedSpecError("generated operator names must use evolved.<lowercase_name>")
    if not isinstance(declared_version, str) or not VERSION.fullmatch(declared_version):
        raise GeneratedSpecError("generated operator version must be a short non-empty token")
    if not isinstance(description, str) or not description.strip() or len(description) > 1024:
        raise GeneratedSpecError("generated operator description must contain 1 to 1024 characters")
    if not isinstance(family, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", family):
        raise GeneratedSpecError("generated operator family must be a lowercase identifier")
    output_type = spec.get("output_type")
    if output_type not in OUTPUT_TYPES:
        raise GeneratedSpecError(f"generated output type must be one of {sorted(OUTPUT_TYPES)}")

    raw_ports = spec.get("input_ports")
    if not isinstance(raw_ports, list) or not raw_ports or len(raw_ports) > 8:
        raise GeneratedSpecError(
            'input_ports must be a JSON array of 1 to 8 objects with name/type '
            '(for example, [{"name":"data","type":"Aggregate"}]); mappings are not accepted')
    ports: list[dict[str, Any]] = []
    names: set[str] = set()
    for raw in raw_ports:
        if not isinstance(raw, Mapping) or set(raw) - PORT_FIELDS or not {"name", "type"} <= set(raw):
            raise GeneratedSpecError("each input port requires name/type and only published port fields")
        port_name, port_type = raw["name"], raw["type"]
        if not isinstance(port_name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", port_name):
            raise GeneratedSpecError("input port names must be lowercase identifiers")
        if port_name in names:
            raise GeneratedSpecError(f"duplicate input port {port_name}")
        names.add(port_name)
        if (port_type not in PORT_TYPES and
                not (port_type == "Telemetry" and authoring_profile == TYPED_OPERATOR_CODE_PROFILE)):
            raise GeneratedSpecError(f"unsupported generated input type {port_type!r}")
        if authoring_profile == TYPED_OPERATOR_CODE_PROFILE and port_type not in TYPED_OPERATOR_CODE_PORT_TYPES:
            raise GeneratedSpecError(f"typed-operator-code-v1 does not support input type {port_type!r}")
        required = raw.get("required", True)
        variadic = raw.get("variadic", False)
        minimum = raw.get("minimum_items", 1)
        maximum = raw.get("maximum_items")
        if type(required) is not bool or type(variadic) is not bool:
            raise GeneratedSpecError("input port required/variadic flags must be boolean")
        if type(minimum) is not int or minimum < 0 or minimum > 64:
            raise GeneratedSpecError("input port minimum_items must be between zero and 64")
        if maximum is not None and (type(maximum) is not int or maximum < max(1, minimum) or maximum > 64):
            raise GeneratedSpecError("input port maximum_items must be between minimum_items and 64")
        if not variadic and (minimum != 1 or maximum is not None):
            raise GeneratedSpecError("non-variadic input ports cannot declare item bounds")
        if not required and variadic and minimum != 0:
            raise GeneratedSpecError("optional variadic input ports must set minimum_items to zero")
        ports.append({"name": port_name, "type": port_type, "required": required,
                      "variadic": variadic, "minimum_items": minimum,
                      "maximum_items": maximum})

    normalized_requirements: dict[str, dict[str, Any]] | None = None
    if input_requirements is not None:
        try:
            from .m4_capabilities import validate_input_requirements
            normalized_requirements = validate_input_requirements(input_requirements, ports,
                allow_table_specific=authoring_profile == TYPED_OPERATOR_CODE_PROFILE,
                allow_bank_aliases=authoring_profile == TYPED_OPERATOR_CODE_PROFILE)
        except Exception as exc:
            raise GeneratedSpecError(f"invalid input_requirements: {exc}") from exc
    if authoring_profile == TYPED_OPERATOR_CODE_PROFILE:
        if not normalized_requirements or not any(
                requirement["source_columns"] or requirement["statistics"] or
                requirement["group_by"] or requirement.get("group_by_by_table") or
                requirement["support_unit"] is not None or requirement["coverage"] != "any"
                for requirement in normalized_requirements.values()):
            raise GeneratedSpecError("typed-operator-code-v1 must declare at least one meaningful per-port data requirement")

    raw_parameters = spec.get("parameters")
    if not isinstance(raw_parameters, Mapping) or len(raw_parameters) > 24:
        raise GeneratedSpecError("parameters must be an object with at most 24 entries")
    parameters: dict[str, dict[str, Any]] = {}
    for param_name, raw in sorted(raw_parameters.items()):
        if (not isinstance(param_name, str) or
                not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", param_name)):
            raise GeneratedSpecError("parameter names must be lowercase identifiers")
        if not isinstance(raw, Mapping) or set(raw) - PARAM_FIELDS or "type" not in raw:
            raise GeneratedSpecError(f"parameter {param_name} requires a type and published fields only")
        value_type = raw["type"]
        if value_type not in PARAMETER_TYPES:
            raise GeneratedSpecError(f"unsupported parameter type {value_type!r}")
        required = raw.get("required", False)
        nullable = raw.get("nullable", False)
        if type(required) is not bool or type(nullable) is not bool:
            raise GeneratedSpecError("parameter required/nullable flags must be boolean")
        has_default = "default" in raw
        default = _json_value(raw["default"], f"parameter {param_name} default") if has_default else None
        choices = raw.get("choices", [])
        if not isinstance(choices, list):
            raise GeneratedSpecError(f"parameter {param_name} choices must be an array")
        for value in choices:
            _json_value(value, f"parameter {param_name} choice")
        runtime_field = raw.get("runtime_field")
        if runtime_field is not None and (not isinstance(runtime_field, str) or
                                          not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", runtime_field)):
            raise GeneratedSpecError(f"parameter {param_name} runtime_field must name a declared field")
        param_description = raw.get("description", "")
        if not isinstance(param_description, str) or len(param_description) > 512:
            raise GeneratedSpecError(f"parameter {param_name} description is too long")
        parameters[param_name] = {"type": value_type, "required": required,
            "nullable": nullable, "default": default, "has_default": has_default,
            "choices": list(choices), "runtime_field": runtime_field,
            "description": param_description}
        type_spec = ParameterSpec(name=param_name, value_type=value_type,
                                  nullable=nullable, choices=())
        for choice in choices:
            try:
                _validate_parameter(type_spec, choice)
            except RuntimeResolutionError as exc:
                raise GeneratedSpecError(f"parameter {param_name} has a choice with the wrong type: {exc}") from exc
        if has_default:
            try:
                _validate_parameter(type_spec, default)
            except RuntimeResolutionError as exc:
                raise GeneratedSpecError(f"parameter {param_name} has an invalid default: {exc}") from exc
            if choices and default not in choices:
                raise GeneratedSpecError(f"parameter {param_name} default is outside its choices")

    source = spec.get("source")
    source_limit = (TYPED_OPERATOR_CODE_MAX_SOURCE_BYTES
        if authoring_profile == TYPED_OPERATOR_CODE_PROFILE else MAX_SOURCE_BYTES)
    if not isinstance(source, str) or not source.strip() or len(source.encode("utf-8")) > source_limit:
        raise GeneratedSpecError(f"generated source must contain 1 to {source_limit} UTF-8 bytes")
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise GeneratedSpecError(f"generated source has invalid syntax: {exc}") from exc
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "operator"]
    if len(functions) != 1:
        raise GeneratedSpecError("generated source must define exactly one operator function")
    try:
        validate_sandbox_source(source, authoring_profile=authoring_profile)
    except Exception as exc:
        raise GeneratedSpecError(f"generated source violates the sandbox syntax contract: {exc}") from exc
    if authoring_profile == TYPED_OPERATOR_CODE_PROFILE:
        _check_code_statistical_cutoffs(source)
    else:
        _check_declared_thresholds(source)

    signature = {"name": name, "declared_version": declared_version,
        "description": description.strip(), "family": family, "input_ports": ports,
        "output_type": output_type, "parameters": parameters}
    if authoring_profile is not None:
        signature["authoring_profile"] = authoring_profile
    if normalized_requirements is not None:
        signature["input_requirements"] = normalized_requirements
    source_sha = hashlib.sha256(source.encode("utf-8")).hexdigest()
    signature_sha = hashlib.sha256(canonical_json(signature)).hexdigest()
    if authoring_profile == TYPED_OPERATOR_CODE_PROFILE:
        source_identity = hashlib.sha256(ast.dump(tree, annotate_fields=True,
            include_attributes=False).encode("utf-8")).hexdigest()
        version_sha = hashlib.sha256(canonical_json({"signature": signature,
            "source_ast_sha256": source_identity,
            "typed_data_api_sha256": operator_code_contract_hash()})).hexdigest()
    else:
        version_sha = hashlib.sha256(canonical_json({"signature": signature,
            "source_sha256": source_sha})).hexdigest()
    normalized = {**signature, "version": f"{declared_version}+{version_sha[:16]}",
        "declared_version": declared_version, "source": source,
        "source_sha256": source_sha, "signature_sha256": signature_sha,
        "contract_hash": version_sha}
    if authoring_profile == TYPED_OPERATOR_CODE_PROFILE:
        from .m4_typecheck import check_generated_spec_types
        try:
            check_generated_spec_types(normalized)
        except Exception as exc:
            raise GeneratedSpecError(f"generated operator failed typed Python static checking: {exc}") from exc
    return normalized


def register_generated_specs(registry: OperatorRegistry, specs: list[Mapping[str, Any]], *,
                             audit_dir: Path | None = None,
                             sandbox_limits: Mapping[str, Any] | None = None,
                             public_data_root: Path | None = None) -> list[dict[str, Any]]:
    """Validate and register dynamic implementations that execute only in bwrap."""
    normalized_specs = [validate_generated_spec(spec) for spec in specs]
    if audit_dir is not None:
        audit_dir.mkdir(parents=True, exist_ok=True)
    for spec in normalized_specs:
        ports = tuple(PortSpec(item["name"], item["type"], item["required"],
                               item["variadic"], item["minimum_items"],
                               item["maximum_items"])
                     for item in spec["input_ports"])
        parameters = {name: ParameterSpec(name=name, value_type=item["type"],
            required=item["required"], nullable=item["nullable"], default=item["default"],
            has_default=item["has_default"], runtime_field=item["runtime_field"],
            description=item["description"], choices=tuple(item["choices"]))
            for name, item in spec["parameters"].items()}

        def implementation(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any,
                           *, _spec: dict[str, Any] = spec) -> Any:
            if _spec.get("authoring_profile") in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
                from .m4_operator_ir import (compact_operator_ir_inputs,
                    compact_operator_ir_inputs_v2)
                if _spec["authoring_profile"] == "typed-operator-ir-v1":
                    inputs = compact_operator_ir_inputs(inputs)
                else:
                    inputs = compact_operator_ir_inputs_v2(inputs, _spec["program"])
            wire_params = _jsonable(params)
            runtime_json = _jsonable(context.runtime_values)
            if _spec.get("authoring_profile") == TYPED_OPERATOR_CODE_PROFILE:
                from .operator_code_api import prepare_operator_code_inputs
                with prepare_operator_code_inputs(inputs,
                        public_data_root=public_data_root,
                        input_ports=_spec["input_ports"]) as prepared:
                    result = run_generated_operator(_spec["source"], prepared.wire_inputs,
                        wire_params, runtime_json, _spec["output_type"], limits=sandbox_limits,
                        case_dir=prepared.case_dir, authoring_profile=TYPED_OPERATOR_CODE_PROFILE,
                        typecheck_spec=_spec, trusted_context=prepared.trusted_context)
            else:
                ports = {port["name"]: port for port in _spec["input_ports"]}
                wire_inputs = {}
                for name, value in inputs.items():
                    if ports[name]["variadic"]:
                        wire_inputs[name] = [json.loads(canonical_json(item).decode("utf-8"))
                                             for item in value]
                    else:
                        wire_inputs[name] = json.loads(canonical_json(value).decode("utf-8"))
                run_kwargs = {}
                if _spec.get("authoring_profile") == TYPED_RECORDS_PROFILE:
                    run_kwargs = {"authoring_profile": TYPED_RECORDS_PROFILE,
                                  "typecheck_spec": _spec}
                if _spec.get("authoring_profile") in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
                    from .m4_operator_ir import trusted_runtime_context, trusted_runtime_context_v2
                    profile = _spec["authoring_profile"]
                    trusted_context = (trusted_runtime_context() if profile == "typed-operator-ir-v1"
                        else trusted_runtime_context_v2(inputs=inputs))
                    result = run_generated_operator(None, wire_inputs, wire_params, runtime_json,
                        _spec["output_type"], limits=sandbox_limits,
                        authoring_profile=profile, typecheck_spec=_spec,
                        ir_program=_spec["program"], trusted_context=trusted_context)
                else:
                    result = run_generated_operator(_spec["source"], wire_inputs, wire_params,
                        runtime_json, _spec["output_type"], limits=sandbox_limits, **run_kwargs)
            audit_row = {"operator": _spec["name"], "version": _spec["version"],
                "source_sha256": _spec["source_sha256"], "ok": result.get("ok"),
                "isolation": result.get("isolation"), "stdout": result.get("stdout", ""),
                "stderr": result.get("stderr", ""), "error": result.get("error")}
            if audit_dir is not None:
                path = audit_dir / "generated_operator_calls.jsonl"
                with path.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(audit_row, ensure_ascii=False, sort_keys=True,
                                            allow_nan=False) + "\n")
            if not result.get("ok"):
                error = result.get("error", {})
                raise RuntimeError(f"generated operator failed: {error.get('code')}: {error.get('message')}")
            value = result["result"]
            if _spec["output_type"] in OUTPUT_TYPES:
                value = deserialize_contract_json(value, _spec["output_type"])
                if (_spec["output_type"] == "ScoredCandidates" and
                        any(candidate.category is not None for candidate in value.candidates)):
                    raise ValueError("Bank M4 generated scorers must leave category null; no Bank category contract is enabled")
                return value
            return value

        registry.register(OperatorSpec(name=spec["name"], version=spec["version"],
            inputs=ports, output_type=spec["output_type"], parameters=parameters,
            family=spec["family"], cacheable=True, implementation=implementation,
            description=spec["description"]))
    return normalized_specs


def assert_snapshot_registry_compatible(snapshot: Any, registry: OperatorRegistry) -> None:
    """Fail if a snapshot's pinned builtin signatures differ from this registry."""
    document = getattr(snapshot, "document", snapshot)
    if not isinstance(document, Mapping) or not isinstance(document.get("operators"), (list, tuple)):
        raise ValueError("library snapshot has no operator catalog")

    def thaw(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): thaw(child) for key, child in value.items()}
        if isinstance(value, (list, tuple)):
            return [thaw(child) for child in value]
        return value

    def signature_hash(signature: Any) -> str:
        raw = json.dumps(thaw(signature), ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    pinned = {entry.get("name") + "@" + entry.get("version"): entry
              for entry in document["operators"]
              if isinstance(entry, Mapping) and entry.get("kind") == "builtin" and
              isinstance(entry.get("name"), str) and isinstance(entry.get("version"), str)}
    current_rows = registry.signatures()
    current = {entry["name"] + "@" + entry["version"]: entry for entry in current_rows
               if not entry["name"].startswith("evolved.")}
    if set(pinned) != set(current):
        raise ValueError("library snapshot builtin operator set differs from the current registry")
    for identity, entry in pinned.items():
        signature = entry.get("signature")
        expected_hash = entry.get("signature_sha256")
        current_signature = current[identity]
        digest = signature_hash(current_signature)
        if thaw(signature) != thaw(current_signature) or expected_hash != digest:
            raise ValueError(f"library snapshot builtin signature mismatch: {identity}")
    policy = document.get("policy", {})
    policy_versions = policy.get("builtin_policy_versions", {}) if isinstance(policy, Mapping) else {}
    if policy_versions:
        actual_versions = {identity: row["version"] for identity, row in current.items()}
        if thaw(policy_versions) != actual_versions:
            raise ValueError("library snapshot builtin policy versions differ from the current registry")
