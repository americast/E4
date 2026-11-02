"""Fail-closed static checker for the opt-in typed-record generated profile.

Generated source is parsed and type checked, never imported or executed here.
The checker writes only private temporary files and uses the pinned Mypy CLI.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import resource
import subprocess
import sys
import tempfile
import tokenize
import time
from pathlib import Path
from typing import Any, Mapping


PROFILE = "typed-records-v1"
MYPY_VERSION = "1.19.1"
MAX_SOURCE_BYTES = 65536
MAX_OUTPUT_BYTES = 16384
CHECK_TIMEOUT_SECONDS = 8
CHECK_CPU_SECONDS = 6
CHECK_MEMORY_BYTES = 1024 * 1024 * 1024


class TypedAuthoringError(ValueError):
    def __init__(self, code: str, message: str, line: int | None = None,
                 column: int | None = None, mypy_code: str | None = None):
        super().__init__(message)
        self.code = code
        self.line = line
        self.column = column
        self.mypy_code = mypy_code


_RESULT_CACHE: dict[str, dict[str, Any] | tuple[str, str, int | None, int | None, str | None]] = {}
_CACHE_LIMIT = 128


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _type_for(value_type: str, nullable: bool = False) -> str:
    types = {"string": "str", "integer": "int", "number": "float",
             "positive_integer": "int", "string_list": "Sequence[str]",
             "boolean": "bool", "object": "JSONMapping", "array": "Sequence[JSONValue]",
             "json": "JSONValue"}
    if value_type not in types:
        raise TypedAuthoringError("typed_schema_error", f"unsupported declared type {value_type!r}")
    result = types[value_type]
    return f"{result} | None" if nullable and result != "JSONValue" else result


def _parameter_type(row: Mapping[str, Any]) -> str:
    choices = row.get("choices", [])
    if isinstance(choices, list) and choices and all(
            value is None or type(value) in (str, int, bool) for value in choices):
        literals = ", ".join(repr(value) for value in choices)
        result = f"Literal[{literals}]"
        if row.get("nullable") and None not in choices:
            result += " | None"
        return result
    return _type_for(str(row.get("type")), bool(row.get("nullable")))


def _wire_type(output_type: str) -> str:
    return {"Telemetry": "TelemetryWire", "Aggregate": "AggregateWire",
            "CandidateSet": "CandidateSetWire", "ScoredCandidates": "ScoredCandidatesWire",
            "RankedAnswer": "RankedAnswerWire"}.get(output_type, "JSONValue")


def _input_type(port: Mapping[str, Any]) -> str:
    if port.get("type") == "Telemetry":
        raise TypedAuthoringError("typed_input_not_supported",
            "typed-records-v1 currently requires structured Aggregate, CandidateSet, or ScoredCandidates input ports")
    base = {"Aggregate": "AggregateWire", "CandidateSet": "CandidateSetWire",
            "ScoredCandidates": "ScoredCandidatesWire"}.get(str(port.get("type")))
    if base is None:
        raise TypedAuthoringError("typed_input_not_supported",
            f"typed-records-v1 does not support input type {port.get('type')!r}")
    if port.get("variadic"):
        return f"Sequence[{base}]"
    return base


def _checker_stub(spec: Mapping[str, Any], runtime_schema: Mapping[str, Any]) -> str:
    from .sandbox_worker import (_TYPED_OPTIONAL_PARAMETERS, _TYPED_PARAMETER_TYPES,
        _TYPED_RETURN_TYPES, typed_record_helper_schema)
    ports = spec.get("input_ports")
    params = spec.get("parameters")
    if not isinstance(ports, list) or not isinstance(params, Mapping):
        raise TypedAuthoringError("typed_schema_error", "typed checker requires normalized input_ports and parameters")
    lines = [
        "from typing import Mapping, Sequence, TypedDict, Protocol, Literal, overload",
        "from typing_extensions import NotRequired, ReadOnly, Required",
        "JSONValue = str | int | float | bool | None | list['JSONValue'] | tuple['JSONValue', ...] | Mapping[str, 'JSONValue']",
        "JSONMapping = Mapping[str, JSONValue]",
        "Category = Literal['CPU fault', 'db close', 'db connection limit', 'network delay', 'network loss'] | None",
        "RuntimeWindow = TypedDict('RuntimeWindow', {'name': ReadOnly[Literal['reference', 'alert']], 'start_ms_utc': ReadOnly[int], 'end_ms_utc': ReadOnly[int]})",
        "WindowWire = TypedDict('WindowWire', {'$type': Literal['Window'], '$fields': Mapping[str, JSONValue]})",
        "EntityRefWire = TypedDict('EntityRefWire', {'$type': Literal['EntityRef'], '$fields': Mapping[str, JSONValue]})",
        "EvidenceWire = TypedDict('EvidenceWire', {'$type': Literal['Evidence'], '$fields': Mapping[str, JSONValue]})",
        "CandidateWire = TypedDict('CandidateWire', {'$type': Literal['Candidate'], '$fields': Mapping[str, JSONValue]})",
        "CandidateSetWire = TypedDict('CandidateSetWire', {'$type': Literal['CandidateSet'], '$fields': Mapping[str, JSONValue]})",
        "AggregateRowWire = TypedDict('AggregateRowWire', {'$type': Literal['AggregateRow'], '$fields': Mapping[str, JSONValue]})",
        "AggregateWire = TypedDict('AggregateWire', {'$type': Literal['Aggregate'], '$fields': Mapping[str, JSONValue]})",
        "ScoredCandidateWire = TypedDict('ScoredCandidateWire', {'$type': Literal['ScoredCandidate'], '$fields': Mapping[str, JSONValue]})",
        "ScoredCandidatesWire = TypedDict('ScoredCandidatesWire', {'$type': Literal['ScoredCandidates'], '$fields': Mapping[str, JSONValue]})",
        "TelemetryWire = TypedDict('TelemetryWire', {'$type': Literal['Telemetry'], '$fields': Mapping[str, JSONValue]})",
        "RankedAnswerWire = TypedDict('RankedAnswerWire', {'$type': Literal['RankedAnswer'], '$fields': Mapping[str, JSONValue]})",
        "class WindowView(Protocol):\n    @property\n    def name(self) -> Literal['reference', 'alert']: ...\n    @property\n    def start_ms_utc(self) -> int: ...\n    @property\n    def end_ms_utc(self) -> int: ...",
        "class EntityRefView(Protocol):\n    @property\n    def role(self) -> str: ...\n    @property\n    def identifier(self) -> str: ...",
        "class EvidenceView(Protocol):\n    @property\n    def source(self) -> str: ...\n    @property\n    def window(self) -> WindowView: ...\n    @property\n    def statistic(self) -> str: ...\n    @property\n    def value(self) -> float | str | bool | None: ...\n    @property\n    def contributing_entities(self) -> Sequence[EntityRefView]: ...\n    @property\n    def support(self) -> int: ...\n    @property\n    def details(self) -> JSONMapping: ...",
        "class CandidateView(Protocol):\n    @property\n    def entity(self) -> EntityRefView: ...\n    @property\n    def rationale(self) -> str: ...",
        "class CandidateSetView(Protocol):\n    @property\n    def candidates(self) -> Sequence[CandidateView]: ...\n    @property\n    def source(self) -> str: ...",
        "class AggregateRowView(Protocol):\n    @property\n    def entity(self) -> EntityRefView: ...\n    @property\n    def metric_key(self) -> str: ...\n    @property\n    def window(self) -> WindowView: ...\n    @property\n    def statistic(self) -> str: ...\n    @property\n    def value(self) -> float | None: ...\n    @property\n    def support(self) -> int: ...\n    @property\n    def source_tables(self) -> Sequence[str]: ...\n    @property\n    def contributing_entities(self) -> Sequence[EntityRefView]: ...\n    @property\n    def details(self) -> JSONMapping: ...",
        "class AggregateView(Protocol):\n    @property\n    def window(self) -> WindowView: ...\n    @property\n    def rows(self) -> Sequence[AggregateRowView]: ...\n    @property\n    def source_tables(self) -> Sequence[str]: ...\n    @property\n    def metadata(self) -> JSONMapping: ...",
        "class ScoredCandidateView(Protocol):\n    @property\n    def entity(self) -> EntityRefView: ...\n    @property\n    def score(self) -> float: ...\n    @property\n    def category(self) -> Category: ...\n    @property\n    def evidence(self) -> Sequence[EvidenceView]: ...\n    @property\n    def analysis_family(self) -> str | None: ...",
        "class ScoredCandidatesView(Protocol):\n    @property\n    def candidates(self) -> Sequence[ScoredCandidateView]: ...\n    @property\n    def scorer(self) -> str: ...\n    @property\n    def status(self) -> Literal['ready', 'abstained']: ...\n    @property\n    def status_details(self) -> JSONMapping: ...",
        "class Runtime(Protocol):",
    ]
    runtime_fields = runtime_schema.get("fields", {})
    if not isinstance(runtime_fields, Mapping):
        raise TypedAuthoringError("typed_schema_error", "runtime schema fields must be an object")
    for name, row in sorted(runtime_fields.items()):
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", name) or not isinstance(row, Mapping):
            continue
        if row.get("scope") == "execution_output" or row.get("writable") is False:
            continue
        field_kind = row.get("type")
        ftype = {"positive_integer": "int", "integer": "int", "number": "float",
                 "number_map": "Mapping[str, float]", "window": "RuntimeWindow"}.get(field_kind)
        if ftype:
            lines.append(f"    @property\n    def {name}(self) -> {ftype}: ...")
    if not any(line.startswith("    ") for line in lines[-len(runtime_fields):] if runtime_fields):
        lines.append("    pass")
    lines += ["Window = WindowWire | WindowView", "EntityRef = EntityRefWire | EntityRefView",
        "Evidence = EvidenceWire | EvidenceView", "Candidate = CandidateWire | CandidateView",
        "CandidateSet = CandidateSetWire | CandidateSetView",
        "AggregateRow = AggregateRowWire | AggregateRowView", "Aggregate = AggregateWire | AggregateView",
        "ScoredCandidate = ScoredCandidateWire | ScoredCandidateView",
        "ScoredCandidates = ScoredCandidatesWire | ScoredCandidatesView"]
    input_fields = []
    for port in ports:
        if not isinstance(port, Mapping) or not isinstance(port.get("name"), str):
            raise TypedAuthoringError("typed_schema_error", "malformed normalized input port")
        typ = _input_type(port)
        key = json.dumps(port["name"])
        if port.get("required", True):
            input_fields.append(f"{key}: {typ}")
        else:
            input_fields.append(f"{key}: NotRequired[{typ}]")
    lines.append(f"Inputs = TypedDict('Inputs', {{{', '.join(input_fields)}}})")
    parameter_fields = []
    for name, row in sorted(params.items()):
        if not isinstance(row, Mapping):
            raise TypedAuthoringError("typed_schema_error", f"malformed parameter {name!r}")
        param_type = _parameter_type(row)
        required = bool(row.get("required"))
        type_expr = f"Required[{param_type}]" if required else f"NotRequired[{param_type}]"
        parameter_fields.append(f"{json.dumps(str(name))}: {type_expr}")
    lines.append(f"Params = TypedDict('Params', {{{', '.join(parameter_fields)}}})")
    lines.append(f"Output = {_wire_type(str(spec.get('output_type')))}")
    constructor_outputs = {"make_window": "WindowWire", "make_entity_ref": "EntityRefWire",
        "make_evidence": "EvidenceWire", "make_candidate": "CandidateWire",
        "make_candidate_set": "CandidateSetWire", "make_aggregate_row": "AggregateRowWire",
        "make_aggregate": "AggregateWire", "make_scored_candidate": "ScoredCandidateWire",
        "make_scored_candidates": "ScoredCandidatesWire"}
    def checker_type(value: str) -> str:
        return value

    for row in typed_record_helper_schema()["helpers"]:
        name = row["name"]
        args = []
        optional = set(_TYPED_OPTIONAL_PARAMETERS.get(name, ()))
        for arg, arg_type in _TYPED_PARAMETER_TYPES[name].items():
            suffix = " = ..." if arg in optional else ""
            args.append(f"{arg}: {checker_type(arg_type)}{suffix}")
        return_type = checker_type(_TYPED_RETURN_TYPES[name])
        if name in constructor_outputs:
            return_type = constructor_outputs[name]
        elif name == "aggregate_view":
            return_type = "AggregateView"
        elif name == "candidate_views":
            return_type = "tuple[CandidateView, ...]"
        elif name == "scored_views":
            return_type = "tuple[ScoredCandidateView, ...]"
        elif name == "native_query_rows":
            return_type = "tuple[JSONMapping, ...]"
        lines.append(f"def {name}({', '.join(args)}) -> {return_type}: ...")
    return "\n".join(lines) + "\n"


def _annotate_operator(source: str) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise TypedAuthoringError("typed_syntax_error", str(exc), exc.lineno, exc.offset) from exc
    funcs = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "operator"]
    if len(funcs) != 1:
        raise TypedAuthoringError("typed_source_error", "source must define one top-level operator")
    node = funcs[0]
    if len(node.args.args) != 3 or [a.arg for a in node.args.args] != ["inputs", "params", "runtime"]:
        raise TypedAuthoringError("typed_source_error", "operator must take inputs, params, runtime")
    # Replace any model-provided top-level annotations. Only this checker copy
    # is transformed; worker execution always receives the original source.
    byte_source = source.encode("utf-8")
    byte_lines = byte_source.splitlines(keepends=True)
    offsets = [0]
    for line in byte_lines:
        offsets.append(offsets[-1] + len(line))
    inserts: list[tuple[int, int, bytes]] = []
    aliases = ("__E4Inputs", "__E4Params", "__E4Runtime")
    for arg, alias in zip(node.args.args, aliases):
        arg_start = offsets[arg.lineno - 1] + arg.col_offset
        arg_end = offsets[arg.end_lineno - 1] + arg.end_col_offset
        if arg.annotation is None:
            inserts.append((arg_end, arg_end, f": {alias}".encode()))
        else:
            ann_start = offsets[arg.annotation.lineno - 1] + arg.annotation.col_offset
            ann_end = offsets[arg.annotation.end_lineno - 1] + arg.annotation.end_col_offset
            inserts.append((ann_start, ann_end, alias.encode()))
    # Add return annotation before the function header's final colon.
    tokens = tokenize.tokenize(iter(source.encode("utf-8").splitlines(keepends=True)).__next__)
    depth = 0
    in_header = False
    close_offset = None
    for token in tokens:
        if token.start[0] == node.lineno and token.type == tokenize.NAME and token.string == "operator":
            in_header = True
        if in_header and token.type == tokenize.OP:
            if token.string == "(": depth += 1
            elif token.string == ")":
                depth -= 1
                if depth == 0:
                    row = token.end[0] - 1
                    col = token.end[1]
                    close_offset = offsets[row] + len(byte_lines[row].decode("utf-8")[:col].encode("utf-8"))
                    break
    if close_offset is None:
        raise TypedAuthoringError("typed_source_error", "could not locate operator signature")
    if node.returns is not None:
        ret_start = offsets[node.returns.lineno - 1] + node.returns.col_offset
        ret_end = offsets[node.returns.end_lineno - 1] + node.returns.end_col_offset
        inserts.append((ret_start, ret_end, b"__E4Output"))
    else:
        inserts.append((close_offset, close_offset, b" -> __E4Output"))
    transformed = byte_source
    for lo, hi, data in sorted(inserts, reverse=True):
        transformed = transformed[:lo] + data + transformed[hi:]
    prefix = ("from typing import Mapping, Sequence, Literal, Optional\n"
              "from _e4_typed_profile_api import Inputs as __E4Inputs, Params as __E4Params, "
              "Runtime as __E4Runtime, Output as __E4Output, Inputs, Params, Runtime, Output, "
              "JSONValue, JSONMapping, AggregateView, AggregateRowView, "
              "CandidateView, CandidateSetView, EntityRefView, EvidenceView, WindowView, "
              "ScoredCandidateView, ScoredCandidatesView, RuntimeWindow, Category, aggregate_view, candidate_views, "
              "Window, EntityRef, Evidence, Candidate, CandidateSet, AggregateRow, Aggregate, "
              "ScoredCandidate, ScoredCandidates, scored_views, make_window, make_entity_ref, make_evidence, make_candidate, "
              "make_candidate_set, make_aggregate_row, make_aggregate, make_scored_candidate, "
              "make_scored_candidates, entity_identifier, entity_id, native_query_rows\n").encode()
    return (prefix + transformed).decode("utf-8")


def _reject_directives(source: str) -> None:
    try:
        for token in tokenize.generate_tokens(iter(source.splitlines(keepends=True)).__next__):
            if token.type == tokenize.COMMENT and re.search(r"\b(type:\s*ignore|mypy:)", token.string):
                raise TypedAuthoringError("typed_directive_forbidden",
                    "mypy/type-ignore directives are not allowed", token.start[0], token.start[1] + 1)
    except (tokenize.TokenError, IndentationError) as exc:
        raise TypedAuthoringError("typed_syntax_error", str(exc)) from exc


def _limit_child():
    resource.setrlimit(resource.RLIMIT_CPU, (CHECK_CPU_SECONDS, CHECK_CPU_SECONDS))
    resource.setrlimit(resource.RLIMIT_AS, (CHECK_MEMORY_BYTES, CHECK_MEMORY_BYTES))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_OUTPUT_BYTES, MAX_OUTPUT_BYTES))
    resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))


def check_generated_spec_types(spec: Mapping[str, Any], runtime_schema: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Run pinned Mypy on a checker-only, spec-annotated copy of a typed spec."""
    if not isinstance(spec, Mapping) or spec.get("authoring_profile") != PROFILE:
        raise TypedAuthoringError("typed_profile_required", f"spec must select {PROFILE}")
    source = spec.get("source")
    if not isinstance(source, str) or not source.strip() or len(source.encode("utf-8")) > MAX_SOURCE_BYTES:
        raise TypedAuthoringError("typed_source_error", "source must be non-empty and at most 64 KiB")
    if runtime_schema is None:
        schema_path = Path(__file__).resolve().parents[1] / "config" / "runtime_schema.json"
        try:
            runtime_schema = json.loads(schema_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise TypedAuthoringError("typed_checker_unavailable", f"runtime schema unavailable: {exc}") from exc
    from .sandbox_worker import TYPED_RECORD_TYPE_NAMES, validate_source
    try:
        validate_source(source, PROFILE)
        _reject_directives(source)
    except TypedAuthoringError:
        raise
    except Exception as exc:
        raise TypedAuthoringError("typed_source_error", str(exc)) from exc
    try:
        import mypy.version
        actual_version = mypy.version.__version__
    except Exception as exc:
        raise TypedAuthoringError("typed_checker_unavailable", "pinned mypy 1.19.1 is unavailable") from exc
    if actual_version != MYPY_VERSION:
        raise TypedAuthoringError("typed_checker_version_mismatch",
            f"expected mypy {MYPY_VERSION}, found {actual_version}")
    stub = _checker_stub(spec, runtime_schema)
    try:
        checked_source = _annotate_operator(source)
    except TypedAuthoringError:
        raise
    try:
        ast.parse(checked_source, mode="exec")
    except SyntaxError as exc:
        raise TypedAuthoringError("typed_checker_unavailable",
            f"trusted checker transformation produced invalid Python: {exc}", exc.lineno, exc.offset) from exc
    # The checker imports these names only from our trusted stub. Rejecting
    # arbitrary references to private names is still enforced by worker AST validation.
    stub_hash = hashlib.sha256(stub.encode()).hexdigest()
    runtime_hash = hashlib.sha256(_canonical(runtime_schema)).hexdigest()
    cache_key = hashlib.sha256(_canonical({"source": source, "spec": {k: v for k, v in spec.items() if k != "source"},
        "runtime": runtime_hash, "stub": stub_hash, "mypy": actual_version,
        "flags": ["--strict", "--disallow-any-expr", "--disallow-any-explicit",
                  "--disallow-any-unimported", "--no-incremental", "--no-site-packages",
                  "--show-column-numbers"]})).hexdigest()
    cached = _RESULT_CACHE.get(cache_key)
    if cached is not None:
        if isinstance(cached, tuple):
            code, message, line, column, mypy_code = cached
            raise TypedAuthoringError(code, message, line, column, mypy_code)
        result = dict(cached)
        result["cache_hit"] = True
        result["elapsed_ms"] = 0
        return result
    start = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(prefix="e4-mypy-") as temp:
            root = Path(temp)
            (root / "_e4_typed_profile_api.pyi").write_text(stub, encoding="utf-8")
            (root / "candidate.py").write_text(checked_source, encoding="utf-8")
            (root / "mypy.ini").write_text("[mypy]\nshow_error_codes = True\n", encoding="utf-8")
            env = {"PATH": os.defpath, "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                   "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
                   "MYPYPATH": temp, "MYPY_CACHE_DIR": str(root / "cache")}
            cmd = [sys.executable, "-m", "mypy", "--strict", "--disallow-any-expr",
                   "--disallow-any-explicit", "--disallow-any-unimported",
                   "--warn-unused-ignores", "--no-incremental", "--no-site-packages",
                   f"--config-file={root / 'mypy.ini'}", "--python-version=3.10", "--show-error-codes",
                   "--show-column-numbers",
                   "--no-error-summary", "candidate.py"]
            with (root / "stdout.log").open("wb") as stdout_file, (root / "stderr.log").open("wb") as stderr_file:
                proc = subprocess.run(cmd, cwd=temp, env=env, stdout=stdout_file,
                                      stderr=stderr_file, timeout=CHECK_TIMEOUT_SECONDS,
                                      preexec_fn=_limit_child, check=False)
            stdout = (root / "stdout.log").read_bytes()[:MAX_OUTPUT_BYTES].decode("utf-8", "replace")
            stderr = (root / "stderr.log").read_bytes()[:MAX_OUTPUT_BYTES].decode("utf-8", "replace")
            output = (stdout + stderr)[:MAX_OUTPUT_BYTES]
            if proc.returncode != 0:
                if proc.returncode < 0 or proc.returncode in {137, 152, 153}:
                    raise TypedAuthoringError("typed_checker_resource_limit",
                        f"mypy exceeded its bounded checker resource policy (exit {proc.returncode})")
                if (re.search(r"_e4_typed_profile_api\.pyi:\d+: error:", output) or
                        "INTERNAL ERROR" in output or "Traceback" in output or
                        "usage: mypy" in output):
                    raise TypedAuthoringError("typed_checker_unavailable",
                        "pinned mypy or its trusted type stubs failed; refusing typed execution: " + output.strip())
                matches = list(re.finditer(
                    r"candidate\.py:(\d+):(\d+): error: (.*?)(?: \[([^]]+)\])?$", output, re.M))
                if matches:
                    first = matches[0]
                    line = max(1, int(first.group(1)) - 2)
                    column = int(first.group(2))
                    mypy_code = first.group(4) or "type_error"
                    message = "\n".join(
                        f"line {max(1, int(item.group(1)) - 2)}, column {item.group(2)}: "
                        f"{item.group(3).strip()} [{item.group(4) or 'type_error'}]"
                        for item in matches)
                    code = mypy_code
                else:
                    raise TypedAuthoringError("typed_checker_unavailable",
                        "pinned mypy produced no source diagnostics; refusing typed execution: " +
                        (output.strip() or f"mypy exited with status {proc.returncode}"))
                error = TypedAuthoringError("typed_static_type_error", f"{code}: {message}",
                                            line, column, mypy_code)
                _RESULT_CACHE[cache_key] = (error.code, str(error), error.line, error.column,
                                            error.mypy_code)
                return _raise(error)
    except subprocess.TimeoutExpired as exc:
        raise TypedAuthoringError("typed_checker_timeout", "mypy exceeded its bounded checker time") from exc
    except TypedAuthoringError:
        raise
    except Exception as exc:
        raise TypedAuthoringError("typed_checker_unavailable", f"mypy failed closed: {type(exc).__name__}: {exc}") from exc
    result = {"status": "passed", "mypy_version": actual_version,
              "elapsed_ms": int((time.monotonic() - start) * 1000),
              "cache_hit": False, "context_sha256": cache_key}
    if len(_RESULT_CACHE) >= _CACHE_LIMIT:
        _RESULT_CACHE.pop(next(iter(_RESULT_CACHE)))
    _RESULT_CACHE[cache_key] = dict(result)
    return result


def _raise(error: TypedAuthoringError):
    raise error
