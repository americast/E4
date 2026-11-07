"""Deterministic parsing and trusted loading for structured adapter submissions."""

from __future__ import annotations

import ast
import json
import math
import sys
import uuid
from importlib import resources
from types import ModuleType
from collections.abc import Mapping
from typing import Any

from .adapter import AdapterConformanceError, BaseDatasetAdapter, check_adapter_conformance


_RESOURCE_PACKAGE = "e4.data"
_SCHEMA_RESOURCE = "adapter_response.schema.json"
_TEMPLATE_RESOURCE = "adapter_template.py.txt"
_REQUIRED_FIELDS = {
    "__init__": ("self", "config"),
    "get_schema": ("self",),
    "get_windows": ("self",),
    "extract_suspect_entities": ("self",),
    "stream_records": ("self", "table_name", "window"),
}
_TYPE_ALIASES = {
    "typing.Mapping": "Mapping",
    "collections.abc.Mapping": "Mapping",
    "typing.Iterator": "Iterator",
    "collections.abc.Iterator": "Iterator",
    "e4.types.DatasetSchema": "DatasetSchema",
    "e4.types.EntityRef": "EntityRef",
    "e4.types.TelemetryRecord": "TelemetryRecord",
    "e4.types.Window": "Window",
    "e4.data.adapter.BaseDatasetAdapter": "BaseDatasetAdapter",
}
_REQUIRED_TYPES = {
    "__init__": ("Mapping[str,object]", "None"),
    "get_schema": (None, "DatasetSchema"),
    "get_windows": (None, "tuple[Window,Window]"),
    "extract_suspect_entities": (None, "list[EntityRef]"),
    "stream_records": (("table_name", "str"), ("window", "Window"), "Iterator[TelemetryRecord]"),
}
_INPUT_TYPES = {"string", "number", "integer", "boolean", "object", "array"}


class AdapterSubmissionError(ValueError):
    """A structured submission failure with a stable stage and safe details."""

    def __init__(self, stage: str, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.stage = stage
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"stage": self.stage, "message": str(self), "details": self.details}


class AdapterInputRequired(AdapterSubmissionError):
    """The response explicitly asks for missing, typed user input."""

    def __init__(self, required_fields: list[dict[str, str]]):
        self.required_fields = [dict(item) for item in required_fields]
        super().__init__(
            "needs_input",
            "adapter authoring requires additional user input",
            {"required_fields": self.required_fields},
        )


def adapter_submission_schema() -> dict[str, Any]:
    """Read the offline JSON Schema for adapter response envelopes."""
    return json.loads(resources.files(_RESOURCE_PACKAGE).joinpath("resources", _SCHEMA_RESOURCE).read_text(encoding="utf-8"))


def adapter_code_template() -> str:
    """Read the fixed-interface trusted adapter source template."""
    return resources.files(_RESOURCE_PACKAGE).joinpath("resources", _TEMPLATE_RESOURCE).read_text(encoding="utf-8")


def _plain_json(value: Any, path: str = "$", *, allow_mapping: bool = True) -> Any:
    if value is None or type(value) in {str, bool, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"non-finite number at {path}")
        return value
    if isinstance(value, Mapping) and allow_mapping:
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"non-string object key at {path}")
            result[key] = _plain_json(item, f"{path}.{key}")
        return result
    if isinstance(value, list):
        return [_plain_json(item, f"{path}[{index}]") for index, item in enumerate(value)]
    raise ValueError(f"non-JSON value at {path}")


def _reject_json_constant(token: str) -> Any:
    raise ValueError(f"non-standard JSON constant {token}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _config_schema_validator(config_schema: Mapping[str, Any]) -> tuple[dict[str, Any], Any]:
    try:
        schema_doc = _plain_json(config_schema)
    except (TypeError, ValueError) as exc:
        raise AdapterSubmissionError("config_schema", "config schema must contain plain JSON values", {"error": str(exc)[:200]}) from exc
    if not isinstance(schema_doc, dict) or schema_doc.get("type") != "object":
        raise AdapterSubmissionError("config_schema", "config schema root type must be object")
    dialect = schema_doc.get("$schema")
    if dialect is not None and dialect != "https://json-schema.org/draft/2020-12/schema":
        raise AdapterSubmissionError("config_schema", "config schema dialect must be Draft 2020-12")
    if (
        not isinstance(schema_doc.get("properties"), dict)
        or not isinstance(schema_doc.get("required"), list)
        or schema_doc.get("additionalProperties") is not False
    ):
        raise AdapterSubmissionError(
            "config_schema",
            "config schema root must declare properties, required, and additionalProperties: false",
        )
    if _contains_ref(schema_doc):
        raise AdapterSubmissionError("config_schema", "config schema references are disabled; inline all definitions")
    try:
        from jsonschema import Draft202012Validator
    except ImportError as exc:
        raise AdapterSubmissionError("config_schema", "jsonschema dependency is required for config validation") from exc
    try:
        Draft202012Validator.check_schema(schema_doc)
        return schema_doc, Draft202012Validator(schema_doc)
    except Exception as exc:
        raise AdapterSubmissionError("config_schema", "invalid Draft 2020-12 config schema", {"error_type": type(exc).__name__}) from exc


def parse_adapter_submission(response: str | Mapping[str, Any]) -> dict[str, Any]:
    """Parse one exact ready/needs_input envelope; reject prose and duplicate keys."""
    try:
        if isinstance(response, str):
            parsed = json.loads(
                response,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            )
        elif isinstance(response, Mapping):
            parsed = _plain_json(response)
        else:
            raise ValueError("response must be JSON text or a mapping")
        parsed = _plain_json(parsed)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise AdapterSubmissionError("envelope", "response is not strict JSON", {"error": str(exc)[:240]}) from exc
    if not isinstance(parsed, dict):
        raise AdapterSubmissionError("envelope", "response root must be a JSON object")
    try:
        from jsonschema import Draft202012Validator
        envelope_validator = Draft202012Validator(adapter_submission_schema())
        envelope_error = next(iter(envelope_validator.iter_errors(parsed)), None)
    except Exception as exc:
        raise AdapterSubmissionError("envelope", "installed adapter response schema could not be loaded", {"error_type": type(exc).__name__}) from exc
    if envelope_error is not None:
        raise AdapterSubmissionError(
            "envelope",
            "response does not match the adapter submission schema",
            {"path": [str(item) for item in envelope_error.absolute_path], "validator": envelope_error.validator},
        )
    status = parsed.get("status")
    if status == "ready":
        _config_schema_validator(parsed["config_schema"])
        _validate_adapter_source(parsed["source"])
        return parsed
    if status == "needs_input":
        fields = parsed["required_fields"]
        seen: set[str] = set()
        normalized: list[dict[str, str]] = []
        for index, field in enumerate(fields):
            name, field_type = field["name"], field["type"]
            if not isinstance(name, str) or not name.strip() or name in seen:
                raise AdapterSubmissionError("envelope", "required field names must be nonempty and unique", {"index": index})
            if not isinstance(field_type, str) or field_type not in _INPUT_TYPES:
                raise AdapterSubmissionError("envelope", "required field type is not a supported JSON type", {"index": index})
            item = {"name": name, "type": field_type}
            if "reason" in field:
                item["reason"] = field["reason"]
            normalized.append(item)
            seen.add(name)
        parsed["required_fields"] = normalized
        return parsed
    raise AdapterSubmissionError("envelope", "status must be exactly 'ready' or 'needs_input'")


def _contains_ref(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(key in {"$ref", "$dynamicRef", "$recursiveRef"} or _contains_ref(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_ref(item) for item in value)
    return False


def validate_adapter_config(config_schema: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    """Check a Draft 2020-12 object schema and validate plain JSON config offline."""
    _, validator = _config_schema_validator(config_schema)
    try:
        config_doc = _plain_json(config)
    except (TypeError, ValueError) as exc:
        raise AdapterSubmissionError("config", "config must contain plain finite JSON values", {"error": str(exc)[:200]}) from exc
    if not isinstance(config_doc, dict):
        raise AdapterSubmissionError("config", "config must be a JSON object")
    error = next(iter(validator.iter_errors(config_doc)), None)
    if error is not None:
        path = [str(item) for item in error.absolute_path]
        raise AdapterSubmissionError(
            "config",
            "config does not satisfy its schema" + (f" at {'.'.join(path)}" if path else " at root"),
            {"path": path, "validator": error.validator},
        )
    return config_doc


def _annotation_text(node: ast.expr | None) -> str | None:
    if node is None:
        return None
    value = ast.unparse(node).replace(" ", "")
    for source, target in _TYPE_ALIASES.items():
        value = value.replace(source, target)
    return value


def _args(function: ast.FunctionDef) -> tuple[list[ast.arg], bool]:
    arguments = function.args
    return (
        [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs],
        arguments.vararg is not None or arguments.kwarg is not None or bool(arguments.defaults) or any(arguments.kw_defaults),
    )


def _method_stub(function: ast.FunctionDef) -> bool:
    body = list(function.body)
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
        body.pop(0)
    if len(body) == 1 and (isinstance(body[0], ast.Pass) or isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and body[0].value.value is Ellipsis):
        return True
    for node in ast.walk(function):
        if isinstance(node, ast.Raise) and (
            isinstance(node.exc, ast.Name) and node.exc.id == "NotImplementedError"
            or isinstance(node.exc, ast.Call) and isinstance(node.exc.func, ast.Name) and node.exc.func.id == "NotImplementedError"
        ):
            return True
    return "E4_ADAPTER_IMPLEMENTATION_REQUIRED" in ast.unparse(function)


def _validate_adapter_source(source: str) -> ast.Module:
    if "E4_ADAPTER_IMPLEMENTATION_REQUIRED" in source:
        raise AdapterSubmissionError("source", "adapter template placeholders must be implemented")
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise AdapterSubmissionError("source", "adapter source has invalid Python syntax", {"line": exc.lineno, "column": exc.offset}) from exc

    imports_base = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "e4.data.adapter"
        and any(alias.name == "BaseDatasetAdapter" and alias.asname is None for alias in node.names)
        for node in tree.body
    )
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "DatasetAdapter"]
    factories = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "create_adapter"]
    if not imports_base or len(classes) != 1 or len(factories) != 1:
        raise AdapterSubmissionError("source", "module must import BaseDatasetAdapter and define DatasetAdapter plus create_adapter exactly once")
    cls = classes[0]
    if not any(isinstance(base, ast.Name) and base.id == "BaseDatasetAdapter" for base in cls.bases):
        raise AdapterSubmissionError("source", "DatasetAdapter must subclass BaseDatasetAdapter")

    methods = [node for node in cls.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    method_by_name: dict[str, ast.FunctionDef] = {}
    for method in methods:
        if method.name in _REQUIRED_FIELDS:
            if isinstance(method, ast.AsyncFunctionDef) or method.name in method_by_name:
                raise AdapterSubmissionError("source", f"{method.name} must be one synchronous method")
            method_by_name[method.name] = method
    if set(method_by_name) != set(_REQUIRED_FIELDS):
        raise AdapterSubmissionError("source", "DatasetAdapter is missing one or more fixed interface methods")
    for name, function in method_by_name.items():
        expected_names = list(_REQUIRED_FIELDS[name])
        args, has_defaults = _args(function)
        if [argument.arg for argument in args] != expected_names or has_defaults or function.decorator_list:
            raise AdapterSubmissionError("source", f"{name} signature does not match the fixed adapter interface")
        if function.args.kwonlyargs or function.args.posonlyargs:
            raise AdapterSubmissionError("source", f"{name} must use the declared positional interface")
        if name == "stream_records":
            expected_parameters = {"table_name": "str", "window": "Window"}
            if any(_annotation_text(arg.annotation) != expected_parameters[arg.arg] for arg in args[1:]):
                raise AdapterSubmissionError("source", "stream_records parameter annotations do not match the adapter interface")
            expected_return = "Iterator[TelemetryRecord]"
        else:
            parameters, expected_return = _REQUIRED_TYPES[name]
            expected_parameters = {} if parameters is None else {"config": parameters}
            if any(_annotation_text(arg.annotation) != expected_parameters.get(arg.arg) for arg in args[1:]):
                raise AdapterSubmissionError("source", f"{name} parameter annotations do not match the adapter interface")
        if _annotation_text(function.returns) != expected_return:
            raise AdapterSubmissionError("source", f"{name} return annotation must be {expected_return}")
        if _method_stub(function):
            raise AdapterSubmissionError("source", f"{name} is still an unimplemented template placeholder")

    factory = factories[0]
    factory_args, factory_defaults = _args(factory)
    if (
        [argument.arg for argument in factory_args] != ["config"]
        or factory_defaults
        or factory.decorator_list
        or factory.args.kwonlyargs
        or factory.args.posonlyargs
        or _annotation_text(factory_args[0].annotation) != "Mapping[str,object]"
        or _annotation_text(factory.returns) != "BaseDatasetAdapter"
    ):
        raise AdapterSubmissionError("source", "create_adapter signature must be (config: Mapping[str, object]) -> BaseDatasetAdapter")
    factory_body = list(factory.body)
    if factory_body and isinstance(factory_body[0], ast.Expr) and isinstance(factory_body[0].value, ast.Constant) and isinstance(factory_body[0].value.value, str):
        factory_body.pop(0)
    valid_factory = (
        len(factory_body) == 1
        and isinstance(factory_body[0], ast.Return)
        and isinstance(factory_body[0].value, ast.Call)
        and isinstance(factory_body[0].value.func, ast.Name)
        and factory_body[0].value.func.id == "DatasetAdapter"
        and len(factory_body[0].value.args) == 1
        and isinstance(factory_body[0].value.args[0], ast.Name)
        and factory_body[0].value.args[0].id == "config"
        and not factory_body[0].value.keywords
    )
    if not valid_factory:
        raise AdapterSubmissionError("source", "create_adapter body must only return DatasetAdapter(config)")
    return tree


def load_adapter_submission(
    response: str | Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    check_conformance: bool = True,
) -> BaseDatasetAdapter:
    """Validate and run one trusted integration module; this is not a code sandbox."""
    submission = parse_adapter_submission(response)
    if submission["status"] == "needs_input":
        raise AdapterInputRequired(submission["required_fields"])
    validated_config = validate_adapter_config(submission["config_schema"], config)

    module_name = f"_e4_submitted_adapter_{uuid.uuid4().hex}"
    module = ModuleType(module_name)
    module.__file__ = "<trusted-e4-adapter-submission>"
    sys.modules[module_name] = module
    loaded = False
    try:
        code = compile(submission["source"], module.__file__, "exec")
        try:
            exec(code, module.__dict__)
        except Exception as exc:
            raise AdapterSubmissionError("source", f"adapter module import/execution failed ({type(exc).__name__})", {"error_type": type(exc).__name__}) from exc
        try:
            adapter = module.create_adapter(validated_config)
        except Exception as exc:
            raise AdapterSubmissionError("factory", f"create_adapter failed ({type(exc).__name__})", {"error_type": type(exc).__name__}) from exc
        if not isinstance(adapter, BaseDatasetAdapter):
            raise AdapterSubmissionError("factory", "create_adapter must return a BaseDatasetAdapter", {"returned_type": type(adapter).__name__})
        if check_conformance:
            try:
                check_adapter_conformance(adapter)
            except AdapterConformanceError as exc:
                raise AdapterSubmissionError("conformance", f"adapter conformance failed ({type(exc).__name__})", {"error_type": type(exc).__name__, "reason": str(exc)[:240]}) from exc
            except Exception as exc:
                raise AdapterSubmissionError("conformance", f"adapter conformance failed ({type(exc).__name__})", {"error_type": type(exc).__name__}) from exc
        loaded = True
        return adapter
    finally:
        if not loaded:
            sys.modules.pop(module_name, None)
