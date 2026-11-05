"""Fail-closed AST security validator for model-authored Python operators."""

from __future__ import annotations

import ast
import io
import re
import symtable
import tokenize
from typing import Sequence

MAX_SOURCE_BYTES = 8192

SAFE_BUILTINS = frozenset({
    "abs", "all", "any", "bool", "dict", "enumerate", "float", "int",
    "isinstance", "len", "list", "max", "min", "range", "round", "set",
    "sorted", "str", "sum", "tuple", "zip", "print", "Exception", "ValueError",
})

ALLOWED_MODULES = frozenset({"math", "statistics", "itertools", "collections"})

TYPED_RECORD_CONSTRUCTORS = frozenset({
    "make_window", "make_entity_ref", "make_evidence", "make_candidate",
    "make_candidate_set", "make_aggregate_row", "make_aggregate",
    "make_scored_candidate", "make_scored_candidates", "entity_identifier", "entity_id",
})

TYPED_HELPER_FUNCTIONS = frozenset({
    "telemetry_view", "metric_points", "span_records", "log_records",
    "aggregate_records", "group_by", "summarize", "top_k",
    "aggregate_view", "candidate_views", "scored_views",
})

ANNOTATION_ONLY_NAMES = frozenset({
    "Inputs", "Params", "Runtime", "Output", "Any", "Callable",
    "Telemetry", "TelemetryHandle", "TelemetryView", "MetricPointView",
    "SpanView", "LogRecordView", "CoverageView", "QueryResult", "SummaryResult",
    "AggregateGroupView", "WindowView", "EntityRefView", "EvidenceView",
    "CandidateView", "CandidateSetView", "AggregateRowView", "AggregateView",
    "ScoredCandidateView", "ScoredCandidatesView", "MetricField", "SpanField",
    "LogField", "SourceField", "Statistic", "RecordType", "SourceTable",
    "Scalar", "Sequence", "Mapping", "Literal", "Optional", "Union",
})


class SecurityValidationError(ValueError):
    """Raised when model-authored code violates AST security constraints."""

    def __init__(self, message: str, line: int | None = None, column: int | None = None):
        super().__init__(message)
        self.line = line
        self.column = column


def validate_source(source: str, max_bytes: int = MAX_SOURCE_BYTES) -> None:
    """Validate that authored Python source conforms to safety and interface rules."""
    if not isinstance(source, str) or not source.strip():
        raise SecurityValidationError("Source code must be a non-empty string")

    encoded = source.encode("utf-8")
    if len(encoded) > max_bytes:
        raise SecurityValidationError(
            f"Source size ({len(encoded)} bytes) exceeds limit of {max_bytes} bytes"
        )

    # Reject typechecker suppression directives (# type: ignore, # mypy:)
    try:
        tokens = tokenize.tokenize(io.BytesIO(encoded).readline)
        for tok in tokens:
            if tok.type == tokenize.COMMENT:
                comment_text = tok.string
                if re.search(r"#\s*(type\s*:\s*ignore|mypy\s*:)", comment_text, re.I):
                    raise SecurityValidationError(
                        "Type-checker suppression directives ('type: ignore' or 'mypy:') are forbidden",
                        line=tok.start[0],
                        column=tok.start[1],
                    )
    except tokenize.TokenError:
        pass

    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise SecurityValidationError(
            f"Python syntax error: {exc.msg}", line=exc.lineno, column=exc.offset
        ) from exc

    # Must contain exactly one synchronous operator function
    funcs = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "operator"
    ]
    if len(funcs) != 1 or isinstance(funcs[0], ast.AsyncFunctionDef):
        raise SecurityValidationError("Source must define exactly one synchronous 'operator' function")

    fn = funcs[0]
    args = fn.args
    if (
        len(args.args) != 3
        or args.posonlyargs
        or args.vararg
        or args.kwarg
        or args.kwonlyargs
        or args.defaults
        or args.kw_defaults
        or [a.arg for a in args.args] != ["inputs", "params", "runtime"]
    ):
        raise SecurityValidationError(
            "Operator signature must be exactly: def operator(inputs, params, runtime):"
        )

    # Walk AST and check for forbidden nodes
    forbidden_nodes = (
        ast.ClassDef, ast.AsyncFunctionDef, ast.Lambda, ast.Global,
        ast.Nonlocal, ast.With, ast.AsyncWith, ast.Try, ast.Raise,
        ast.Delete, ast.Yield, ast.YieldFrom, ast.Await,
    )

    for node in ast.walk(tree):
        if isinstance(node, forbidden_nodes):
            raise SecurityValidationError(
                f"Source uses forbidden language construct '{type(node).__name__}'",
                line=getattr(node, "lineno", None),
                column=getattr(node, "col_offset", None),
            )
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise SecurityValidationError(
                f"Private attribute access '{node.attr}' is forbidden",
                line=getattr(node, "lineno", None),
                column=getattr(node, "col_offset", None),
            )
        if isinstance(node, ast.Name) and (
            node.id.startswith("__")
            or node.id in {
                "open", "eval", "exec", "compile", "globals", "locals", "getattr",
                "setattr", "delattr", "vars", "input", "breakpoint", "memoryview",
            }
        ):
            raise SecurityValidationError(
                f"Use of dangerous built-in '{node.id}' is forbidden",
                line=getattr(node, "lineno", None),
                column=getattr(node, "col_offset", None),
            )
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = (
                [alias.name.split(".")[0] for alias in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
            )
            if any(name not in ALLOWED_MODULES for name in names) or getattr(node, "level", 0):
                raise SecurityValidationError(
                    f"Importing unauthorized module: only {sorted(ALLOWED_MODULES)} are allowed",
                    line=getattr(node, "lineno", None),
                    column=getattr(node, "col_offset", None),
                )

    # Check annotation-only names are not used as runtime values
    annotation_nodes = set()
    for parent in ast.walk(tree):
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for a in parent.args.args:
                if a.annotation:
                    annotation_nodes.update(id(n) for n in ast.walk(a.annotation))
            if parent.returns:
                annotation_nodes.update(id(n) for n in ast.walk(parent.returns))
        elif isinstance(parent, ast.AnnAssign):
            annotation_nodes.update(id(n) for n in ast.walk(parent.annotation))

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and node.id in ANNOTATION_ONLY_NAMES
            and id(node) not in annotation_nodes
        ):
            raise SecurityValidationError(
                f"'{node.id}' is a type annotation only and cannot be used as a runtime value",
                line=getattr(node, "lineno", None),
                column=getattr(node, "col_offset", None),
            )

    # Symbol table check for unbound global names
    try:
        symbols = symtable.symtable(source, "<authored-operator>", "exec")
    except Exception as exc:
        raise SecurityValidationError(f"Symbol table analysis failed: {exc}") from exc

    module_bindings = {
        s.get_name() for s in symbols.get_symbols()
        if s.is_assigned() or s.is_imported()
    }
    available = (
        set(SAFE_BUILTINS)
        | set(TYPED_RECORD_CONSTRUCTORS)
        | set(TYPED_HELPER_FUNCTIONS)
        | set(ANNOTATION_ONLY_NAMES)
        | module_bindings
    )

    pending = [symbols]
    while pending:
        table = pending.pop()
        pending.extend(table.get_children())
        for s in table.get_symbols():
            if s.is_referenced() and s.is_global() and s.get_name() not in available:
                name = s.get_name()
                occ = next(
                    (n for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id == name),
                    None,
                )
                lineno = getattr(occ, "lineno", None)
                col = getattr(occ, "col_offset", None)
                raise SecurityValidationError(
                    f"Unbound identifier '{name}': please import it or define it locally",
                    line=lineno,
                    column=col,
                )
