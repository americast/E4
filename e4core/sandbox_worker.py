"""Private child process entry point for generated operators.

This file is mounted read-only into a bubblewrap namespace by sandbox.py.
It deliberately accepts one JSON request on stdin and writes one bounded
JSON response on stdout.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import resource
import sys
from collections.abc import Mapping
from types import MappingProxyType
from pathlib import Path


class LimitedText(io.TextIOBase):
    def __init__(self, limit: int):
        self.limit = limit
        self.parts: list[str] = []
        self.size = 0

    def write(self, value: str) -> int:
        value = str(value)
        encoded = value.encode("utf-8", "replace")
        if self.size + len(encoded) > self.limit:
            allowed = max(0, self.limit - self.size)
            value = encoded[:allowed].decode("utf-8", "ignore")
            self.parts.append(value)
            self.size = self.limit
            raise OutputLimit("generated output limit exceeded")
        self.parts.append(value)
        self.size += len(encoded)
        return len(value)

    def getvalue(self) -> str:
        return "".join(self.parts)


class OutputLimit(Exception):
    pass


def validate_source(source: str) -> None:
    import ast

    if not isinstance(source, str) or not source.strip() or len(source.encode("utf-8")) > 65536:
        raise ValueError("source must be non-empty and at most 64 KiB")
    tree = ast.parse(source, mode="exec")
    funcs = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and node.name == "operator"]
    if len(funcs) != 1 or isinstance(funcs[0], ast.AsyncFunctionDef):
        raise ValueError("source must define exactly one synchronous operator function")
    args = funcs[0].args
    if (len(args.args) != 3 or args.posonlyargs or args.vararg or args.kwarg or
            args.kwonlyargs or args.defaults or args.kw_defaults or
            [arg.arg for arg in args.args] != ["inputs", "params", "runtime"]):
        raise ValueError("operator signature must be operator(inputs, params, runtime)")
    forbidden_nodes = (ast.ClassDef, ast.AsyncFunctionDef, ast.Lambda, ast.Global,
                       ast.Nonlocal, ast.With, ast.AsyncWith, ast.Try, ast.Raise,
                       ast.Delete, ast.Yield, ast.YieldFrom, ast.Await)
    allowed_imports = {"math", "statistics", "itertools", "collections"}
    for node in ast.walk(tree):
        if isinstance(node, forbidden_nodes):
            raise ValueError("source uses a restricted Python construct")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise ValueError("private attribute access is restricted")
        if isinstance(node, ast.Name) and (node.id.startswith("__") or node.id in {
                "open", "eval", "exec", "compile", "globals", "locals", "getattr",
                "setattr", "delattr", "vars", "input", "breakpoint", "memoryview"}):
            raise ValueError("source uses a restricted name")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name.split(".")[0] for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
            if any(name not in allowed_imports for name in names) or getattr(node, "level", 0):
                raise ValueError("source imports a restricted module")


def _json_value(value, depth=0, seen=None):
    if depth > 40:
        raise ValueError("result nesting exceeds 40 levels")
    if seen is None:
        seen = set()
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("result contains a non-finite number")
        return value
    if type(value) in (list, tuple):
        ident = id(value)
        if ident in seen:
            raise ValueError("result contains a cycle")
        seen.add(ident)
        result = [_json_value(item, depth + 1, seen) for item in value]
        seen.remove(ident)
        return result
    if isinstance(value, Mapping):
        ident = id(value)
        if ident in seen:
            raise ValueError("result contains a cycle")
        seen.add(ident)
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("result object keys must be strings")
            result[key] = _json_value(item, depth + 1, seen)
        seen.remove(ident)
        return result
    raise ValueError("operator result must contain JSON-compatible values")


def _readonly(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: _readonly(item) for key, item in value.items()})
    if type(value) in (list, tuple):
        return tuple(_readonly(item) for item in value)
    return value


def _decode_wire_value(value, depth=0):
    """Decode cache map/tuple envelopes while preserving typed core envelopes."""
    if depth > 40:
        raise ValueError("wire value nesting exceeds 40 levels")
    if type(value) in (list, tuple):
        return [_decode_wire_value(item, depth + 1) for item in value]
    if not isinstance(value, Mapping):
        return value
    if set(value) == {"$map"}:
        if not isinstance(value["$map"], Mapping):
            raise ValueError("malformed mapping wire envelope")
        return {key: _decode_wire_value(item, depth + 1)
                for key, item in value["$map"].items()}
    if set(value) == {"$tuple"}:
        if type(value["$tuple"]) not in (list, tuple):
            raise ValueError("malformed tuple wire envelope")
        return [_decode_wire_value(item, depth + 1) for item in value["$tuple"]]
    if "$type" in value:
        if (set(value) != {"$type", "$fields"} or type(value["$type"]) is not str or
                not isinstance(value["$fields"], Mapping)):
            raise ValueError("malformed typed wire envelope")
        return {"$type": value["$type"], "$fields": _decode_wire_value(value["$fields"], depth + 1)}
    return {key: _decode_wire_value(item, depth + 1) for key, item in value.items()}


def contract_fields(value, expected_type):
    """Read a typed input's public fields as plain read-only worker mappings."""
    if (not isinstance(value, Mapping) or set(value) != {"$type", "$fields"} or
            value.get("$type") != expected_type or not isinstance(value.get("$fields"), Mapping)):
        raise ValueError(f"expected {expected_type} cache wire envelope")
    return _readonly(_decode_wire_value(value["$fields"]))


class RuntimeView:
    """Read-only view of flat declared runtime fields."""

    __slots__ = ("_values",)

    def __init__(self, values):
        object.__setattr__(self, "_values", MappingProxyType(
            {key: _readonly(value) for key, value in values.items()}))

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self._values[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __getitem__(self, name):
        return self._values[name]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __setattr__(self, name, value):
        raise TypeError("runtime view is read-only")


def main():
    request = json.loads(sys.stdin.read())
    limits = request["limits"]
    resource.setrlimit(resource.RLIMIT_CPU, (limits["cpu_seconds"], limits["cpu_seconds"]))
    resource.setrlimit(resource.RLIMIT_AS, (limits["memory_bytes"], limits["memory_bytes"]))
    resource.setrlimit(resource.RLIMIT_FSIZE, (limits["file_bytes"], limits["file_bytes"]))
    resource.setrlimit(resource.RLIMIT_NOFILE, (limits["open_files"], limits["open_files"]))

    validate_source(request["source"])
    import ast
    safe_builtins = {
        "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
        "enumerate": enumerate, "float": float, "int": int, "len": len,
        "list": list, "max": max, "min": min, "range": range,
        "round": round, "set": set, "sorted": sorted, "str": str,
        "sum": sum, "tuple": tuple, "zip": zip, "print": print,
        "Exception": Exception, "ValueError": ValueError,
    }
    allowed_modules = {"math", "statistics", "itertools", "collections"}
    import importlib

    def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level or name not in allowed_modules:
            raise ImportError("module is not available in generated operators")
        return importlib.import_module(name)

    safe_builtins["__import__"] = safe_import
    def load_json_file(path: Path):
        maximum = 2 * 1024 * 1024
        if path.stat().st_size > maximum:
            raise ValueError("mounted JSON file exceeds 2 MiB")
        with path.open("rb") as stream:
            raw = stream.read(maximum + 1)
        if len(raw) > maximum:
            raise ValueError("mounted JSON file exceeds 2 MiB")
        def reject_constant(token):
            raise ValueError(f"non-finite JSON constant {token}")
        return json.loads(raw, parse_constant=reject_constant)

    def read_case_json(relative_path):
        if not isinstance(relative_path, str) or not relative_path or "\\" in relative_path:
            raise ValueError("case path must be a relative POSIX path")
        relative = Path(relative_path)
        if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
            raise ValueError("case path must stay within the mounted case")
        root = Path("/case").resolve(strict=True)
        target = (root / relative).resolve(strict=True)
        if not target.is_relative_to(root) or not target.is_file():
            raise ValueError("case path is outside the mounted case or is not a file")
        return load_json_file(target)

    def read_catalog_json():
        path = Path("/catalog.json")
        if not path.is_file():
            raise ValueError("deployment catalog was not mounted")
        return load_json_file(path)

    namespace = {"__builtins__": safe_builtins,
                 "read_case_json": read_case_json,
                 "read_catalog_json": read_catalog_json,
                 "contract_fields": contract_fields}
    source = request["source"]
    tree = ast.parse(source, mode="exec")
    code = compile(tree, "<generated-operator>", "exec", dont_inherit=True, optimize=2)
    out = LimitedText(limits["stdout_bytes"])
    err = LimitedText(limits["stderr_bytes"])
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            exec(code, namespace, namespace)
            inputs = request["inputs"]
            params = request["params"]
            runtime = RuntimeView(request["runtime"])
            # Nested mappings are recreated in the worker and not shared with the caller.
            result = namespace["operator"](inputs, params, runtime)
            result = _json_value(result)
        response = {"ok": True, "result": result, "stdout": out.getvalue(), "stderr": err.getvalue()}
    except OutputLimit as exc:
        response = {"ok": False, "result": None,
                    "error": {"code": "output_limit", "message": str(exc)},
                    "stdout": out.getvalue(), "stderr": err.getvalue()}
    except BaseException as exc:
        response = {"ok": False, "result": None,
                    "error": {"code": "resource_limit" if isinstance(exc, MemoryError) else "operator_error",
                              "message": str(exc)[:1000],
                              "type": type(exc).__name__},
                    "stdout": out.getvalue(), "stderr": err.getvalue()}
    sys.__stdout__.write(json.dumps(response, allow_nan=False, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except BaseException as exc:
        sys.__stdout__.write(json.dumps({"ok": False, "result": None,
                                        "error": {"code": "worker_error",
                                                  "message": str(exc)[:1000],
                                                  "type": type(exc).__name__}},
                                       allow_nan=False, separators=(",", ":")))
