"""Validated registration for versioned, sandboxed M4 generated operators."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from typing import Any, Mapping

from .cache import canonical_json, deserialize_contract_json
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec, RuntimeResolutionError, _jsonable, _validate_parameter
from .sandbox import run_generated_operator
from .sandbox_worker import validate_source as validate_sandbox_source


GENERATED_NAME = re.compile(r"^evolved\.[a-z][a-z0-9_]{0,62}$")
VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
PORT_TYPES = {"Aggregate", "CandidateSet", "ScoredCandidates"}
OUTPUT_TYPES = {"Aggregate", "CandidateSet", "ScoredCandidates"}
PARAMETER_TYPES = {"string", "boolean", "integer", "positive_integer", "number", "string_list"}
SPEC_FIELDS = {"name", "version", "description", "family", "input_ports", "output_type",
               "parameters", "source"}
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
    }


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


def validate_generated_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize the JSON contract for one generated operator.

    The source is never evaluated here. Syntax/signature/process isolation are
    enforced again by the generated-operator worker at execution time.
    """
    normalized_fields = {"declared_version", "source_sha256", "signature_sha256", "contract_hash"}
    if isinstance(spec, Mapping) and set(spec) == SPEC_FIELDS | normalized_fields:
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
    if not isinstance(spec, Mapping) or set(spec) != SPEC_FIELDS:
        raise GeneratedSpecError(f"generated spec must contain exactly {sorted(SPEC_FIELDS)}")
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
        if port_type not in PORT_TYPES:
            raise GeneratedSpecError(f"unsupported generated input type {port_type!r}")
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
    if not isinstance(source, str) or not source.strip() or len(source.encode("utf-8")) > MAX_SOURCE_BYTES:
        raise GeneratedSpecError("generated source must contain 1 to 65536 UTF-8 bytes")
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise GeneratedSpecError(f"generated source has invalid syntax: {exc}") from exc
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "operator"]
    if len(functions) != 1:
        raise GeneratedSpecError("generated source must define exactly one operator function")
    try:
        validate_sandbox_source(source)
    except Exception as exc:
        raise GeneratedSpecError(f"generated source violates the sandbox syntax contract: {exc}") from exc
    _check_declared_thresholds(source)

    signature = {"name": name, "declared_version": declared_version,
        "description": description.strip(), "family": family, "input_ports": ports,
        "output_type": output_type, "parameters": parameters}
    source_sha = hashlib.sha256(source.encode("utf-8")).hexdigest()
    signature_sha = hashlib.sha256(canonical_json(signature)).hexdigest()
    version_sha = hashlib.sha256(canonical_json({"signature": signature, "source_sha256": source_sha})).hexdigest()
    normalized = {**signature, "version": f"{declared_version}+{version_sha[:16]}",
        "declared_version": declared_version, "source": source,
        "source_sha256": source_sha, "signature_sha256": signature_sha,
        "contract_hash": version_sha}
    return normalized


def register_generated_specs(registry: OperatorRegistry, specs: list[Mapping[str, Any]], *,
                             audit_dir: Path | None = None,
                             sandbox_limits: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
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
            ports = {port["name"]: port for port in _spec["input_ports"]}
            wire_inputs = {}
            for name, value in inputs.items():
                if ports[name]["variadic"]:
                    wire_inputs[name] = [json.loads(canonical_json(item).decode("utf-8"))
                                         for item in value]
                else:
                    wire_inputs[name] = json.loads(canonical_json(value).decode("utf-8"))
            wire_params = _jsonable(params)
            runtime_json = _jsonable(context.runtime_values)
            result = run_generated_operator(_spec["source"], wire_inputs, wire_params,
                runtime_json, _spec["output_type"], limits=sandbox_limits)
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
