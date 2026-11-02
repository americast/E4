"""Private child process entry point for generated operators.

This file is mounted read-only into a bubblewrap namespace by sandbox.py.
It deliberately accepts one JSON request on stdin and writes one bounded
JSON response on stdout.
"""

from __future__ import annotations

import contextlib
import base64
import hashlib
import io
import json
import math
import resource
import sys
import zlib
from collections.abc import Mapping, Sequence
from typing import Literal, Optional
from types import MappingProxyType
from pathlib import Path


# This is the single public execution capability contract. Keep the runtime
# globals below and the spec schema derived from these names.
SAFE_BUILTINS = {
    "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
    "enumerate": enumerate, "float": float, "int": int, "isinstance": isinstance,
    "len": len, "list": list, "max": max, "min": min, "range": range,
    "round": round, "set": set, "sorted": sorted, "str": str, "sum": sum,
    "tuple": tuple, "zip": zip, "print": print, "Exception": Exception,
    "ValueError": ValueError,
}
SAFE_BUILTIN_NAMES = frozenset(SAFE_BUILTINS)
ALLOWED_MODULES = frozenset({"math", "statistics", "itertools", "collections"})
HELPER_API = (
    {"name": "read_case_json", "signature": "read_case_json(relative_path)",
     "required_parameters": ["relative_path"], "returns": "bounded mounted-case JSON",
     "description": "Read one explicitly mounted public case JSON file; existing mount/path rules apply."},
    {"name": "read_catalog_json", "signature": "read_catalog_json()",
     "required_parameters": [], "returns": "bounded mounted catalog JSON",
     "description": "Read the explicitly mounted public deployment catalog; existing mount rules apply."},
    {"name": "contract_fields", "signature": "contract_fields(value, expected_type)",
     "required_parameters": ["value", "expected_type"], "returns": "read-only fields mapping",
     "description": "Unwrap only the requested typed cache envelope; nested typed contracts remain envelopes and need their own call."},
    {"name": "make_window", "signature": "make_window(name, start_ms_utc, end_ms_utc)",
     "required_parameters": ["name", "start_ms_utc", "end_ms_utc"], "returns": "Window cache envelope",
     "description": "Construct a Window envelope from its exact primitive fields."},
    {"name": "make_entity_ref", "signature": "make_entity_ref(role, identifier)",
     "required_parameters": ["role", "identifier"], "returns": "EntityRef cache envelope",
     "description": "Construct an EntityRef envelope; identifier and role are supplied explicitly."},
    {"name": "make_evidence", "signature": "make_evidence(source, window, statistic, value, contributing_entities, support, ranking_eligible, details)",
     "required_parameters": ["source", "window", "statistic", "value", "contributing_entities", "support", "ranking_eligible", "details"],
     "returns": "Evidence cache envelope",
     "description": "Construct Evidence. Eligibility is mandatory and caller-computed; true requires positive support and nonempty typed source attribution. Details receives the same explicit eligibility flag; contradictions are rejected."},
    {"name": "make_candidate", "signature": "make_candidate(entity, rationale='')",
     "required_parameters": ["entity"], "returns": "Candidate cache envelope",
     "description": "Construct Candidate from a typed EntityRef envelope."},
    {"name": "make_candidate_set", "signature": "make_candidate_set(candidates, source)",
     "required_parameters": ["candidates", "source"], "returns": "CandidateSet cache envelope",
     "description": "Construct CandidateSet from typed Candidate envelopes."},
    {"name": "make_aggregate_row", "signature": "make_aggregate_row(entity, metric_key, window, statistic, value, support, source_tables, contributing_entities=(), details=None)",
     "required_parameters": ["entity", "metric_key", "window", "statistic", "value", "support", "source_tables"],
     "returns": "AggregateRow cache envelope",
     "description": "Construct AggregateRow using typed EntityRef/Window envelopes and explicit source/support fields."},
    {"name": "make_aggregate", "signature": "make_aggregate(window, rows, source_tables, metadata=None)",
     "required_parameters": ["window", "rows", "source_tables"], "returns": "Aggregate cache envelope",
     "description": "Construct Aggregate from typed Window and AggregateRow envelopes; every row window must match."},
    {"name": "make_scored_candidate", "signature": "make_scored_candidate(entity, score, category, evidence, analysis_family=None)",
     "required_parameters": ["entity", "score", "category", "evidence"], "returns": "ScoredCandidate cache envelope",
     "description": "Construct ScoredCandidate from typed EntityRef and Evidence envelopes. Existing host category and answer validation remain in force."},
    {"name": "make_scored_candidates", "signature": "make_scored_candidates(candidates, scorer, status='ready', status_details=None)",
     "required_parameters": ["candidates", "scorer"], "returns": "ScoredCandidates cache envelope",
     "description": "Construct ScoredCandidates from typed ScoredCandidate envelopes; host validation remains authoritative."},
    {"name": "entity_identifier", "signature": "entity_identifier(entity_ref)",
     "required_parameters": ["entity_ref"], "returns": "string identifier",
     "description": "Read identifier from an EntityRef envelope only; plain field dictionaries are rejected."},
    {"name": "entity_id", "signature": "entity_id(entity_ref)",
     "required_parameters": ["entity_ref"], "returns": "string identifier",
     "description": "Alias for entity_identifier; requires an EntityRef envelope."},
    {"name": "aggregate_rows", "signature": "aggregate_rows(aggregate)",
     "required_parameters": ["aggregate"], "returns": "tuple of AggregateRow envelopes",
     "description": "Return typed rows from Aggregate.rows; call contract_fields on each row before field access."},
    {"name": "candidate_items", "signature": "candidate_items(candidate_set)",
     "required_parameters": ["candidate_set"], "returns": "tuple of Candidate envelopes",
     "description": "Return typed Candidate items from CandidateSet; call contract_fields on each before field access."},
    {"name": "native_query_rows", "signature": "native_query_rows(aggregate)",
     "required_parameters": ["aggregate"], "returns": "tuple of read-only plain CSV row mappings",
     "description": "Return metadata.public_query.rows_data only. These are flat native CSV dictionaries, not AggregateRow envelopes."},
)
HELPER_NAMES = frozenset(row["name"] for row in HELPER_API)
_CONTRACT_FIELD_NAMES = {
    "Window": ("name", "start_ms_utc", "end_ms_utc"),
    "EntityRef": ("role", "identifier"),
    "Evidence": ("source", "window", "statistic", "value", "contributing_entities", "support", "details"),
    "Candidate": ("entity", "rationale"),
    "CandidateSet": ("candidates", "source"),
    "AggregateRow": ("entity", "metric_key", "window", "statistic", "value", "support",
                     "source_tables", "contributing_entities", "details"),
    "Aggregate": ("window", "rows", "source_tables", "metadata"),
    "ScoredCandidate": ("entity", "score", "category", "evidence", "analysis_family"),
    "ScoredCandidates": ("candidates", "scorer", "status", "status_details"),
}
TYPED_RECORD_HELPER_API = (
    {"name": "aggregate_view", "signature": "aggregate_view(aggregate)",
     "input_type": "Aggregate", "return_type": "AggregateView",
     "description": "Create an immutable Aggregate view; rows, windows, entities, and row details are typed read-only views."},
    {"name": "candidate_views", "signature": "candidate_views(candidate_set)",
     "input_type": "CandidateSet", "return_type": "tuple[CandidateView, ...]",
     "description": "Read CandidateSet.candidates as immutable Candidate views with typed EntityRef fields."},
    {"name": "scored_views", "signature": "scored_views(scored_candidates)",
     "input_type": "ScoredCandidates", "return_type": "tuple[ScoredCandidateView, ...]",
     "description": "Read ScoredCandidates.candidates as immutable ScoredCandidate views with typed evidence."},
)
TYPED_RECORD_HELPER_NAMES = frozenset(row["name"] for row in TYPED_RECORD_HELPER_API)
_TYPED_PUBLIC_HELPER_NAMES = frozenset({
    "aggregate_view", "candidate_views", "scored_views", "make_window", "make_entity_ref",
    "make_evidence", "make_candidate", "make_candidate_set", "make_aggregate_row",
    "make_aggregate", "make_scored_candidate", "make_scored_candidates", "entity_identifier",
    "entity_id", "native_query_rows",
})
_TYPED_PARAMETER_TYPES = {
    "aggregate_view": {"aggregate": "Aggregate"},
    "candidate_views": {"candidate_set": "CandidateSet"},
    "scored_views": {"scored_candidates": "ScoredCandidates"},
    "make_window": {"name": "Literal['reference', 'alert']", "start_ms_utc": "int", "end_ms_utc": "int"},
    "make_entity_ref": {"role": "str", "identifier": "str"},
    "make_evidence": {"source": "str", "window": "Window", "statistic": "str",
        "value": "float | str | bool | None", "contributing_entities": "Sequence[EntityRef]",
        "support": "int", "ranking_eligible": "bool", "details": "JSONMapping"},
    "make_candidate": {"entity": "EntityRef", "rationale": "str"},
    "make_candidate_set": {"candidates": "Sequence[Candidate]", "source": "str"},
    "make_aggregate_row": {"entity": "EntityRef", "metric_key": "str", "window": "Window",
        "statistic": "str", "value": "float | None", "support": "int",
        "source_tables": "Sequence[str]", "contributing_entities": "Sequence[EntityRef]",
        "details": "JSONMapping | None"},
    "make_aggregate": {"window": "Window", "rows": "Sequence[AggregateRow]",
        "source_tables": "Sequence[str]", "metadata": "JSONMapping | None"},
    "make_scored_candidate": {"entity": "EntityRef", "score": "float", "category": "None",
        "evidence": "Sequence[Evidence]", "analysis_family": "str | None"},
    "make_scored_candidates": {"candidates": "Sequence[ScoredCandidate]", "scorer": "str",
        "status": "Literal['ready', 'abstained']", "status_details": "JSONMapping | None"},
    "entity_identifier": {"entity_ref": "EntityRef"},
    "entity_id": {"entity_ref": "EntityRef"},
    "native_query_rows": {"aggregate": "Aggregate"},
}
_TYPED_RETURN_TYPES = {
    "aggregate_view": "AggregateView", "candidate_views": "Sequence[CandidateView]",
    "scored_views": "Sequence[ScoredCandidateView]", "make_window": "Window",
    "make_entity_ref": "EntityRef", "make_evidence": "Evidence", "make_candidate": "Candidate",
    "make_candidate_set": "CandidateSet", "make_aggregate_row": "AggregateRow",
    "make_aggregate": "Aggregate", "make_scored_candidate": "ScoredCandidate",
    "make_scored_candidates": "ScoredCandidates", "entity_identifier": "str", "entity_id": "str",
    "native_query_rows": "Sequence[JSONMapping]",
}
_TYPED_OPTIONAL_PARAMETERS = {
    "make_candidate": ("rationale",),
    "make_aggregate_row": ("contributing_entities", "details"),
    "make_aggregate": ("metadata",),
    "make_scored_candidate": ("analysis_family",),
    "make_scored_candidates": ("status", "status_details"),
}
TYPED_RECORD_TYPE_NAMES = frozenset({
    "AggregateView", "AggregateRowView", "CandidateView", "CandidateSetView",
    "EntityRefView", "EvidenceView", "WindowView", "ScoredCandidateView",
    "ScoredCandidatesView", "Runtime", "Inputs", "Params", "Output",
    "JSONValue", "JSONMapping", "RuntimeWindow", "Mapping", "Sequence", "Literal", "Optional",
    "Category", "Window", "EntityRef", "Evidence", "Candidate", "CandidateSet",
    "AggregateRow", "Aggregate", "ScoredCandidate", "ScoredCandidates",
})
TYPED_ANNOTATION_ONLY_NAMES = frozenset({"Runtime", "Inputs", "Params", "Output", "JSONValue", "JSONMapping",
    "RuntimeWindow", "Window", "EntityRef", "Evidence", "Candidate", "CandidateSet",
    "AggregateRow", "Aggregate", "ScoredCandidate", "ScoredCandidates", "Category", "Literal", "Optional"})
_RESERVED_WIRE_KEYS = frozenset({"$type", "$fields", "$map", "$tuple"})
_MAX_HELPER_DEPTH = 40
TYPED_RECORD_PROFILE = "typed-records-v1"


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


def validate_source(source: str, authoring_profile: str | None = None) -> None:
    import ast
    import symtable

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
    allowed_imports = ALLOWED_MODULES
    for node in ast.walk(tree):
        if isinstance(node, forbidden_nodes):
            location = f" at line {getattr(node, 'lineno', '?')}, column {getattr(node, 'col_offset', '?')}"
            raise ValueError(f"source uses forbidden AST construct {type(node).__name__}{location}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise ValueError("private attribute access is restricted")
        if authoring_profile == TYPED_RECORD_PROFILE:
            if (isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) and
                    node.slice.value in _RESERVED_WIRE_KEYS):
                raise ValueError("typed-records-v1 source must use typed helpers, not wire-format keys")
            if isinstance(node, ast.Dict) and any(
                    isinstance(key, ast.Constant) and key.value in _RESERVED_WIRE_KEYS
                    for key in node.keys):
                raise ValueError("typed-records-v1 source must use typed constructors, not wire-format keys")
        if isinstance(node, ast.Name) and (node.id.startswith("__") or node.id in {
                "open", "eval", "exec", "compile", "globals", "locals", "getattr",
                "setattr", "delattr", "vars", "input", "breakpoint", "memoryview"}):
            location = f" at line {getattr(node, 'lineno', '?')}, column {getattr(node, 'col_offset', '?')}"
            raise ValueError(f"source uses forbidden name {node.id!r}{location}")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name.split(".")[0] for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
            if any(name not in allowed_imports for name in names) or getattr(node, "level", 0):
                raise ValueError("source imports a restricted module")

    # Catch unbound global references before a generated operator consumes a
    # runtime diagnostic/repair. Symbol tables handle function locals,
    # parameters, imports, and nested scopes without mistaking attributes for
    # names. The whitelist is the same contract used by the worker globals.
    symbols = symtable.symtable(source, "<generated-operator>", "exec")
    module_bindings = {symbol.get_name() for symbol in symbols.get_symbols()
                       if symbol.is_assigned() or symbol.is_imported()}
    available = set(SAFE_BUILTIN_NAMES) | set(HELPER_NAMES) | module_bindings
    if authoring_profile == TYPED_RECORD_PROFILE:
        available -= {"contract_fields", "aggregate_rows", "candidate_items"}
        annotation_names = set()
        for parent in ast.walk(tree):
            annotations = []
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
                annotations.extend(arg.annotation for arg in (
                    list(parent.args.posonlyargs) + list(parent.args.args) +
                    list(parent.args.kwonlyargs) + ([parent.args.vararg] if parent.args.vararg else []) +
                    ([parent.args.kwarg] if parent.args.kwarg else [])) if arg.annotation is not None)
                if parent.returns is not None:
                    annotations.append(parent.returns)
            elif isinstance(parent, ast.AnnAssign):
                annotations.append(parent.annotation)
            for annotation in annotations:
                annotation_names.update(id(item) for item in ast.walk(annotation))
        for node in ast.walk(tree):
            if (isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and
                    node.id in TYPED_ANNOTATION_ONLY_NAMES and id(node) not in annotation_names):
                raise ValueError(f"{node.id} is available only as a type annotation, not a runtime value")
    if authoring_profile == TYPED_RECORD_PROFILE:
        available |= set(TYPED_RECORD_HELPER_NAMES) | set(TYPED_RECORD_TYPE_NAMES)
    elif authoring_profile is not None:
        raise ValueError(f"unsupported authoring_profile {authoring_profile!r}")
    pending = [symbols]
    while pending:
        table = pending.pop()
        pending.extend(table.get_children())
        for symbol in table.get_symbols():
            if symbol.is_referenced() and symbol.is_global() and symbol.get_name() not in available:
                name = symbol.get_name()
                occurrence = next((node for node in ast.walk(tree)
                                   if isinstance(node, ast.Name) and node.id == name
                                   and isinstance(node.ctx, ast.Load)), None)
                location = (f" at line {occurrence.lineno}, column {occurrence.col_offset}"
                            if occurrence is not None else "")
                raise ValueError(f"unbound name {name!r}{location}; import it or use a declared input/parameter/runtime field")


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
        if isinstance(value, MappingProxyType):
            return value
        return MappingProxyType({key: _readonly(item) for key, item in value.items()})
    if type(value) in (list, tuple):
        return tuple(_readonly(item) for item in value)
    return value


class _RecordView:
    """Trusted immutable fixed-field view over one validated nested contract."""
    __slots__ = ("_record_type", "_values")

    def __init__(self, record_type, values):
        object.__setattr__(self, "_record_type", record_type)
        object.__setattr__(self, "_values", MappingProxyType(dict(values)))

    def __getattr__(self, name):
        record_type = object.__getattribute__(self, "_record_type")
        if name not in _CONTRACT_FIELD_NAMES[record_type]:
            raise AttributeError(f"{record_type}View has no field {name!r}")
        return object.__getattribute__(self, "_values")[name]

    def __setattr__(self, name, value):
        raise TypeError("typed record views are read-only")


class WindowView(_RecordView):
    __slots__ = ()


class EntityRefView(_RecordView):
    __slots__ = ()


class EvidenceView(_RecordView):
    __slots__ = ()


class CandidateView(_RecordView):
    __slots__ = ()


class CandidateSetView(_RecordView):
    __slots__ = ()


class AggregateRowView(_RecordView):
    __slots__ = ()


class AggregateView(_RecordView):
    __slots__ = ()


class ScoredCandidateView(_RecordView):
    __slots__ = ()


class ScoredCandidatesView(_RecordView):
    __slots__ = ()


_RECORD_VIEW_TYPES = {record_type: view_type for record_type, view_type in (
    ("Window", WindowView), ("EntityRef", EntityRefView), ("Evidence", EvidenceView),
    ("Candidate", CandidateView), ("CandidateSet", CandidateSetView),
    ("AggregateRow", AggregateRowView), ("Aggregate", AggregateView),
    ("ScoredCandidate", ScoredCandidateView), ("ScoredCandidates", ScoredCandidatesView))}


def _view_value(value, path, depth=0, seen=None):
    if depth > _MAX_HELPER_DEPTH:
        raise ValueError(f"{path}: nesting exceeds {_MAX_HELPER_DEPTH} levels")
    if seen is None:
        seen = set()
    if isinstance(value, Mapping):
        if "$type" in value:
            record_type = value.get("$type")
            if record_type not in _RECORD_VIEW_TYPES:
                raise ValueError(f"{path}: unsupported typed record {record_type!r}")
            return _record_view(value, record_type, path, depth, seen)
        if set(value) == {"$map"}:
            inner = value["$map"]
            if not isinstance(inner, Mapping):
                raise ValueError(f"{path}: malformed $map envelope")
            value = inner
        elif set(value) == {"$tuple"}:
            inner = value["$tuple"]
            if type(inner) not in (list, tuple):
                raise ValueError(f"{path}: malformed $tuple envelope")
            return _view_value(inner, path, depth + 1, seen)
        ident = id(value)
        if ident in seen:
            raise ValueError(f"{path}: cyclic mapping")
        seen.add(ident)
        rows = {key: _view_value(item, f"{path}.{key}", depth + 1, seen)
                for key, item in value.items()}
        seen.remove(ident)
        return MappingProxyType(rows)
    if type(value) in (list, tuple):
        ident = id(value)
        if ident in seen:
            raise ValueError(f"{path}: cyclic sequence")
        seen.add(ident)
        result = tuple(_view_value(item, f"{path}[{index}]", depth + 1, seen)
                       for index, item in enumerate(value))
        seen.remove(ident)
        return result
    return value


def _record_view(value, record_type, path, depth=0, seen=None):
    fields = _typed_fields(value, record_type, path)
    converted = {name: _view_value(item, f"{path}.{name}", depth + 1, seen)
                 for name, item in fields.items()}
    return _RECORD_VIEW_TYPES[record_type](record_type, converted)


def aggregate_view(aggregate):
    """Read an Aggregate wire envelope as an immutable record view."""
    return _record_view(aggregate, "Aggregate", "Aggregate")


def candidate_views(candidate_set):
    """Read CandidateSet.items as immutable Candidate views."""
    fields = _typed_fields(candidate_set, "CandidateSet", "CandidateSet")
    values = _sequence_contents(fields["candidates"], "CandidateSet.candidates")
    return tuple(_record_view(item, "Candidate", f"CandidateSet.candidates[{index}]")
                 for index, item in enumerate(values))


def scored_views(scored_candidates):
    """Read ScoredCandidates.items as immutable ScoredCandidate views."""
    fields = _typed_fields(scored_candidates, "ScoredCandidates", "ScoredCandidates")
    values = _sequence_contents(fields["candidates"], "ScoredCandidates.candidates")
    return tuple(_record_view(item, "ScoredCandidate", f"ScoredCandidates.candidates[{index}]")
                 for index, item in enumerate(values))


TYPED_RECORD_HELPER_FUNCTIONS = {
    "aggregate_view": aggregate_view,
    "candidate_views": candidate_views,
    "scored_views": scored_views,
}


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


def helper_api_schema():
    """Return deterministic helper signatures, required fields, and wire semantics."""
    output_types = {"make_window": "Window", "make_entity_ref": "EntityRef",
        "make_evidence": "Evidence", "make_candidate": "Candidate",
        "make_candidate_set": "CandidateSet", "make_aggregate_row": "AggregateRow",
        "make_aggregate": "Aggregate", "make_scored_candidate": "ScoredCandidate",
        "make_scored_candidates": "ScoredCandidates"}
    helpers = []
    for item in HELPER_API:
        row = dict(item)
        contract = output_types.get(row["name"])
        if contract is not None:
            row["output_contract"] = contract
            row["output_fields"] = list(_CONTRACT_FIELD_NAMES[contract])
        helpers.append(row)
    return {"schema_version": "e4-generated-typed-helpers/1",
        "helpers": helpers,
        "typed_contract_fields": {name: list(fields) for name, fields in _CONTRACT_FIELD_NAMES.items()},
        "wire_semantics": {
            "typed_contract": "exact {$type, $fields} envelope with every host contract field",
            "mapping": "plain nested mappings encode as {$map: {...}}",
            "sequence": "plain nested lists/tuples encode as {$tuple: [...]} ",
            "nested_contracts": "constructors accept a typed envelope; plain field mappings are not substitutes",
            "native_query_rows": "flat read-only CSV mappings from Aggregate.metadata.public_query.rows_data; not AggregateRows"},
        "neutral_example": (
            "fields = contract_fields(inputs['data'], 'Aggregate'); window = fields['window']; "
            "entity = make_entity_ref('component', 'observed-id'); "
            "row = make_aggregate_row(entity, 'source/table.csv|field', window, 'summary', None, 0, "
            "['source/table.csv'], details={'note': 'shape example'}); "
            "return make_aggregate(window, [row], ['source/table.csv'])")}


def generated_helper_guidance():
    """Short prompt guidance derived from the same worker helper registry."""
    return ("Typed helpers construct strict {$type,$fields} envelopes; nested maps and sequences use "
        "{$map:{...}} and {$tuple:[...]}. contract_fields unwraps only its requested type; nested contracts "
        "need their own call. `aggregate_rows` and `candidate_items` return typed envelopes, while "
        "`native_query_rows` returns plain CSV dictionaries. `make_evidence` requires explicit, caller-computed "
        "ranking_eligible, support, and typed contributors; true requires positive support and nonempty attribution. "
        "Neutral shape example: " + helper_api_schema()["neutral_example"])


def typed_record_helper_schema():
    """The additive immutable-view API selected only by typed-records-v1."""
    legacy_rows = {row["name"]: row for row in HELPER_API}
    view_rows = {row["name"]: row for row in TYPED_RECORD_HELPER_API}
    rows = []
    for name in sorted(_TYPED_PUBLIC_HELPER_NAMES):
        base = view_rows.get(name, legacy_rows.get(name))
        if base is None:
            continue
        required = list(base.get("required_parameters", ()))
        optional = list(_TYPED_OPTIONAL_PARAMETERS.get(name, ()))
        if name in view_rows:
            required = list(_TYPED_PARAMETER_TYPES[name])
        rows.append({"name": name, "signature": base["signature"],
            "parameters": dict(_TYPED_PARAMETER_TYPES[name]),
            "required_parameters": required,
            "optional_parameters": optional,
            "returns": _TYPED_RETURN_TYPES[name],
            "description": (base.get("description", "") if name in view_rows else
                "Construct or inspect declared typed records using the listed fields."),
        })
    field_types = {
        "WindowView": {"name": "Literal['reference', 'alert']", "start_ms_utc": "int", "end_ms_utc": "int"},
        "EntityRefView": {"role": "str", "identifier": "str"},
        "EvidenceView": {"source": "str", "window": "WindowView", "statistic": "str",
            "value": "float | str | bool | None", "contributing_entities": "Sequence[EntityRefView]",
            "support": "int", "details": "JSONMapping"},
        "CandidateView": {"entity": "EntityRefView", "rationale": "str"},
        "CandidateSetView": {"candidates": "Sequence[CandidateView]", "source": "str"},
        "AggregateRowView": {"entity": "EntityRefView", "metric_key": "str", "window": "WindowView",
            "statistic": "str", "value": "float | None", "support": "int",
            "source_tables": "Sequence[str]", "contributing_entities": "Sequence[EntityRefView]",
            "details": "JSONMapping"},
        "AggregateView": {"window": "WindowView", "rows": "Sequence[AggregateRowView]",
            "source_tables": "Sequence[str]", "metadata": "JSONMapping"},
        "ScoredCandidateView": {"entity": "EntityRefView", "score": "float", "category": "Category",
            "evidence": "Sequence[EvidenceView]", "analysis_family": "str | None"},
        "ScoredCandidatesView": {"candidates": "Sequence[ScoredCandidateView]", "scorer": "str",
            "status": "Literal['ready', 'abstained']", "status_details": "JSONMapping"},
    }
    return {"schema_version": "e4-typed-records/1", "authoring_profile": TYPED_RECORD_PROFILE,
        "helpers": rows, "record_fields": field_types,
        "annotation_types": ["Inputs", "Params", "Runtime", "Output", "JSONValue", "JSONMapping",
            "Window", "EntityRef", "Evidence", "Candidate", "CandidateSet", "AggregateRow",
            "Aggregate", "ScoredCandidate", "ScoredCandidates", "Category"],
        "semantics": {"views": "immutable, fixed-field record objects; unknown attributes raise",
            "input_ports": "use the matching explicit view helper for declared typed inputs",
            "constructor_outputs": "constructors return values validated against the declared output contract",
            "details": "read-only Mapping[str, JSONValue]; nested collections are immutable",
            "source_and_eligibility": "make_evidence still requires caller-supplied source, support, contributors and explicit eligibility; host answer validators remain authoritative"}}


def typed_record_helper_guidance():
    """Concise profile-specific guidance; never added to the legacy namespace."""
    return ("In typed-records-v1, use aggregate_view(inputs[port]), candidate_views(inputs[port]), "
        "or scored_views(inputs[port]) to obtain immutable fixed-field record views. Fields are explicit; "
        "unknown attributes are type errors. Constructors accept typed records and return values checked against "
        "the declared output type. Nested metadata/details are read-only. Evidence eligibility must be computed "
        "explicitly by the operator. Helper signatures and record fields come from sandbox.typed_record_helper_api.")


def _typed_fields(value, expected_type, path):
    if isinstance(value, _RecordView):
        record_type = object.__getattribute__(value, "_record_type")
        if record_type != expected_type:
            raise ValueError(f"{path}: expected {expected_type}View, got {record_type}View")
        return object.__getattribute__(value, "_values")
    if not isinstance(value, Mapping):
        raise ValueError(f"{path}: expected typed {expected_type} envelope, got {type(value).__name__}")
    if set(value) != {"$type", "$fields"} or value.get("$type") != expected_type:
        raise ValueError(f"{path}: expected {expected_type} {{$type,$fields}} envelope")
    fields = value.get("$fields")
    expected_fields = set(_CONTRACT_FIELD_NAMES[expected_type])
    if not isinstance(fields, Mapping) or set(fields) != expected_fields:
        missing = sorted(expected_fields - set(fields)) if isinstance(fields, Mapping) else sorted(expected_fields)
        extra = sorted(set(fields) - expected_fields) if isinstance(fields, Mapping) else []
        raise ValueError(f"{path}: {expected_type} fields mismatch; missing={missing}, extra={extra}")
    return fields


def _require_string(value, path, *, nonempty=False):
    if type(value) is not str or (nonempty and not value):
        qualifier = "non-empty " if nonempty else ""
        raise ValueError(f"{path}: expected {qualifier}string")
    return value


def _require_support(value, path):
    if type(value) is not int or value < 0:
        raise ValueError(f"{path}: support must be a nonnegative integer (booleans excluded)")
    return value


def _require_number(value, path):
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ValueError(f"{path}: expected a finite number (booleans excluded)")
    return value


def _mapping_contents(value, path):
    if isinstance(value, Mapping) and set(value) == {"$map"}:
        value = value["$map"]
    if not isinstance(value, Mapping):
        raise ValueError(f"{path}: expected a mapping")
    if any(type(key) is not str for key in value):
        raise ValueError(f"{path}: mapping keys must be strings")
    return value


def _sequence_contents(value, path):
    if isinstance(value, Mapping) and set(value) == {"$tuple"}:
        value = value["$tuple"]
    if type(value) not in (list, tuple):
        raise ValueError(f"{path}: expected a list or tuple")
    return value


def _decode_plain_container(value, path, depth=0, seen=None):
    """Normalize only map/tuple wrappers; typed envelopes are not plain details."""
    if depth > _MAX_HELPER_DEPTH:
        raise ValueError(f"{path}: nesting exceeds {_MAX_HELPER_DEPTH} levels")
    if seen is None:
        seen = set()
    if type(value) in (list, tuple):
        ident = id(value)
        if ident in seen:
            raise ValueError(f"{path}: cyclic sequence")
        seen.add(ident)
        result = [_decode_plain_container(item, f"{path}[{index}]", depth + 1, seen)
                  for index, item in enumerate(value)]
        seen.remove(ident)
        return result
    if isinstance(value, Mapping):
        if "$type" in value:
            raise ValueError(f"{path}: typed contract envelopes are not plain detail data")
        if set(value) == {"$map"}:
            inner = value["$map"]
            if not isinstance(inner, Mapping):
                raise ValueError(f"{path}: malformed $map envelope")
            value = inner
        elif set(value) == {"$tuple"}:
            inner = value["$tuple"]
            if type(inner) not in (list, tuple):
                raise ValueError(f"{path}: malformed $tuple envelope")
            value = inner
            return _decode_plain_container(value, path, depth + 1, seen)
        ident = id(value)
        if ident in seen:
            raise ValueError(f"{path}: cyclic mapping")
        seen.add(ident)
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError(f"{path}: mapping keys must be strings")
            if key in _RESERVED_WIRE_KEYS:
                raise ValueError(f"{path}.{key}: reserved wire key is not accepted in plain data")
            result[key] = _decode_plain_container(item, f"{path}.{key}", depth + 1, seen)
        seen.remove(ident)
        return result
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is float:
        raise ValueError(f"{path}: non-finite number")
    raise ValueError(f"{path}: unsupported plain detail value {type(value).__name__}")


def _encode_plain(value, path, depth=0, seen=None):
    if depth > _MAX_HELPER_DEPTH:
        raise ValueError(f"{path}: nesting exceeds {_MAX_HELPER_DEPTH} levels")
    if seen is None:
        seen = set()
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{path}: non-finite number")
        return value
    if type(value) in (list, tuple):
        ident = id(value)
        if ident in seen:
            raise ValueError(f"{path}: cyclic sequence")
        seen.add(ident)
        result = {"$tuple": [_encode_plain(item, f"{path}[{index}]", depth + 1, seen)
                              for index, item in enumerate(value)]}
        seen.remove(ident)
        return result
    if isinstance(value, Mapping):
        ident = id(value)
        if ident in seen:
            raise ValueError(f"{path}: cyclic mapping")
        seen.add(ident)
        result = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError(f"{path}: mapping keys must be strings")
            if key in _RESERVED_WIRE_KEYS:
                raise ValueError(f"{path}.{key}: reserved wire key is not accepted in plain data")
            result[key] = _encode_plain(item, f"{path}.{key}", depth + 1, seen)
        seen.remove(ident)
        return {"$map": result}
    raise ValueError(f"{path}: expected finite JSON-compatible data, got {type(value).__name__}")


def _envelope(type_name, fields):
    if set(fields) != set(_CONTRACT_FIELD_NAMES[type_name]):
        raise ValueError(f"internal helper error: incomplete {type_name} fields")
    return {"$type": type_name, "$fields": fields}


def _window(value, path):
    fields = _typed_fields(value, "Window", path)
    name = _require_string(fields["name"], path + ".name", nonempty=True)
    if name not in {"reference", "alert"}:
        raise ValueError(f"{path}.name: must be 'reference' or 'alert'")
    start, end = fields["start_ms_utc"], fields["end_ms_utc"]
    if type(start) is not int or type(end) is not int:
        raise ValueError(f"{path}: window boundaries must be integer UTC milliseconds")
    if end <= start:
        raise ValueError(f"{path}: end_ms_utc must be greater than start_ms_utc")
    return _envelope("Window", {"name": name, "start_ms_utc": start, "end_ms_utc": end})


def _entity_ref(value, path):
    fields = _typed_fields(value, "EntityRef", path)
    role = _require_string(fields["role"], path + ".role", nonempty=True)
    identifier = _require_string(fields["identifier"], path + ".identifier", nonempty=True)
    return _envelope("EntityRef", {"role": role, "identifier": identifier})


def _entity_key(entity_envelope):
    fields = entity_envelope["$fields"]
    return fields["role"], fields["identifier"]


def _string_sequence(value, path, *, nonempty=False):
    values = _sequence_contents(value, path)
    result = [_require_string(item, f"{path}[{index}]", nonempty=nonempty)
              for index, item in enumerate(values)]
    return {"$tuple": result}


def make_window(name, start_ms_utc, end_ms_utc):
    name = _require_string(name, "Window.name", nonempty=True)
    if name not in {"reference", "alert"}:
        raise ValueError("Window.name: must be 'reference' or 'alert'")
    if type(start_ms_utc) is not int or type(end_ms_utc) is not int:
        raise ValueError("Window boundaries must be integer UTC milliseconds")
    if end_ms_utc <= start_ms_utc:
        raise ValueError("Window.end_ms_utc must be greater than start_ms_utc")
    return _envelope("Window", {"name": name, "start_ms_utc": start_ms_utc,
                                  "end_ms_utc": end_ms_utc})


def make_entity_ref(role, identifier):
    role = _require_string(role, "EntityRef.role", nonempty=True)
    identifier = _require_string(identifier, "EntityRef.identifier", nonempty=True)
    return _envelope("EntityRef", {"role": role, "identifier": identifier})


def _evidence(value, path):
    fields = _typed_fields(value, "Evidence", path)
    return _make_evidence(fields["source"], fields["window"], fields["statistic"],
        fields["value"], fields["contributing_entities"], fields["support"],
        _mapping_contents(fields["details"], path + ".details").get("ranking_eligible"),
        fields["details"], path)


def _make_evidence(source, window, statistic, value, contributing_entities,
                   support, ranking_eligible, details, path="Evidence"):
    source = _require_string(source, path + ".source", nonempty=True)
    window = _window(window, path + ".window")
    statistic = _require_string(statistic, path + ".statistic", nonempty=True)
    support = _require_support(support, path + ".support")
    if type(ranking_eligible) is not bool:
        raise ValueError(f"{path}.ranking_eligible: an explicit boolean is required")
    if value is not None and type(value) not in (str, bool, int, float):
        raise ValueError(f"{path}.value: must be a finite JSON scalar or null")
    if type(value) is float and not math.isfinite(value):
        raise ValueError(f"{path}.value: must be finite")
    contributors = _sequence_contents(contributing_entities, path + ".contributing_entities")
    entities = [_entity_ref(item, f"{path}.contributing_entities[{index}]")
                for index, item in enumerate(contributors)]
    if ranking_eligible and (support <= 0 or not entities):
        raise ValueError(f"{path}.ranking_eligible: true requires positive support and nonempty typed contributors")
    detail_map = _decode_plain_container(_mapping_contents(details, path + ".details"),
                                         path + ".details")
    existing = detail_map.get("ranking_eligible", ranking_eligible)
    if type(existing) is not bool or existing != ranking_eligible:
        raise ValueError(f"{path}.details.ranking_eligible contradicts the explicit eligibility argument")
    copied = dict(detail_map)
    copied["ranking_eligible"] = ranking_eligible
    return _envelope("Evidence", {"source": source, "window": window,
        "statistic": statistic, "value": value,
        "contributing_entities": {"$tuple": entities}, "support": support,
        "details": _encode_plain(copied, path + ".details")})


def make_evidence(source, window, statistic, value, contributing_entities,
                  support, ranking_eligible, details):
    return _make_evidence(source, window, statistic, value, contributing_entities,
        support, ranking_eligible, details)


def _candidate(value, path):
    fields = _typed_fields(value, "Candidate", path)
    return _envelope("Candidate", {"entity": _entity_ref(fields["entity"], path + ".entity"),
        "rationale": _require_string(fields["rationale"], path + ".rationale")})


def make_candidate(entity, rationale=""):
    return _envelope("Candidate", {"entity": _entity_ref(entity, "Candidate.entity"),
        "rationale": _require_string(rationale, "Candidate.rationale")})


def make_candidate_set(candidates, source):
    source = _require_string(source, "CandidateSet.source", nonempty=True)
    items = _sequence_contents(candidates, "CandidateSet.candidates")
    copied = [_candidate(item, f"CandidateSet.candidates[{index}]")
              for index, item in enumerate(items)]
    keys = [_entity_key(item["$fields"]["entity"]) for item in copied]
    if len(set(keys)) != len(keys):
        raise ValueError("CandidateSet.candidates: duplicate entity identity")
    return _envelope("CandidateSet", {"candidates": {"$tuple": copied}, "source": source})


def _aggregate_row(value, path):
    fields = _typed_fields(value, "AggregateRow", path)
    return _make_aggregate_row(fields["entity"], fields["metric_key"], fields["window"],
        fields["statistic"], fields["value"], fields["support"], fields["source_tables"],
        fields["contributing_entities"], fields["details"], path)


def _make_aggregate_row(entity, metric_key, window, statistic, value, support,
                        source_tables, contributing_entities, details, path="AggregateRow"):
    entity = _entity_ref(entity, path + ".entity")
    metric_key = _require_string(metric_key, path + ".metric_key", nonempty=True)
    window = _window(window, path + ".window")
    statistic = _require_string(statistic, path + ".statistic", nonempty=True)
    if value is not None:
        _require_number(value, path + ".value")
    support = _require_support(support, path + ".support")
    tables = _string_sequence(source_tables, path + ".source_tables", nonempty=True)
    contributors = _sequence_contents(contributing_entities, path + ".contributing_entities")
    entities = [_entity_ref(item, f"{path}.contributing_entities[{index}]")
                for index, item in enumerate(contributors)]
    detail_map = _decode_plain_container(_mapping_contents(details, path + ".details"),
                                         path + ".details")
    return _envelope("AggregateRow", {"entity": entity, "metric_key": metric_key,
        "window": window, "statistic": statistic, "value": value, "support": support,
        "source_tables": tables, "contributing_entities": {"$tuple": entities},
        "details": _encode_plain(detail_map, path + ".details")})


def make_aggregate_row(entity, metric_key, window, statistic, value, support,
                       source_tables, contributing_entities=(), details=None):
    return _make_aggregate_row(entity, metric_key, window, statistic, value, support,
        source_tables, contributing_entities, {} if details is None else details)


def _aggregate(value, path):
    fields = _typed_fields(value, "Aggregate", path)
    return _make_aggregate(fields["window"], fields["rows"], fields["source_tables"],
        fields["metadata"], path)


def _make_aggregate(window, rows, source_tables, metadata, path="Aggregate"):
    window = _window(window, path + ".window")
    row_values = _sequence_contents(rows, path + ".rows")
    row_envelopes = [_aggregate_row(item, f"{path}.rows[{index}]")
                     for index, item in enumerate(row_values)]
    window_fields = window["$fields"]
    for index, row in enumerate(row_envelopes):
        row_window = row["$fields"]["window"]["$fields"]
        if row_window != window_fields:
            raise ValueError(f"{path}.rows[{index}].window: must equal Aggregate.window")
    tables = _string_sequence(source_tables, path + ".source_tables", nonempty=True)
    metadata = _decode_plain_container(_mapping_contents(metadata, path + ".metadata"),
                                       path + ".metadata")
    return _envelope("Aggregate", {"window": window, "rows": {"$tuple": row_envelopes},
        "source_tables": tables, "metadata": _encode_plain(metadata, path + ".metadata")})


def make_aggregate(window, rows, source_tables, metadata=None):
    return _make_aggregate(window, rows, source_tables, {} if metadata is None else metadata)


def _scored_candidate(value, path):
    fields = _typed_fields(value, "ScoredCandidate", path)
    evidence = _sequence_contents(fields["evidence"], path + ".evidence")
    return _make_scored_candidate(fields["entity"], fields["score"], fields["category"],
        evidence, fields["analysis_family"], path)


_ALLOWED_CATEGORIES = frozenset({None, "CPU fault", "db close", "db connection limit",
                                 "network delay", "network loss"})


def _make_scored_candidate(entity, score, category, evidence, analysis_family,
                           path="ScoredCandidate"):
    entity = _entity_ref(entity, path + ".entity")
    score = _require_number(score, path + ".score")
    if category is not None and (type(category) is not str or category not in _ALLOWED_CATEGORIES):
        raise ValueError(f"{path}.category: unsupported category")
    evidence_values = _sequence_contents(evidence, path + ".evidence")
    evidence_envelopes = [_evidence(item, f"{path}.evidence[{index}]")
                          for index, item in enumerate(evidence_values)]
    if analysis_family is not None:
        analysis_family = _require_string(analysis_family, path + ".analysis_family", nonempty=True)
    return _envelope("ScoredCandidate", {"entity": entity, "score": score,
        "category": category, "evidence": {"$tuple": evidence_envelopes},
        "analysis_family": analysis_family})


def make_scored_candidate(entity, score, category, evidence, analysis_family=None):
    return _make_scored_candidate(entity, score, category, evidence, analysis_family)


def _scored_candidates(value, path):
    fields = _typed_fields(value, "ScoredCandidates", path)
    return _make_scored_candidates(fields["candidates"], fields["scorer"],
        fields["status"], fields["status_details"], path)


def _make_scored_candidates(candidates, scorer, status, status_details, path="ScoredCandidates"):
    scorer = _require_string(scorer, path + ".scorer", nonempty=True)
    if type(status) is not str or status not in {"ready", "abstained"}:
        raise ValueError(f"{path}.status: must be 'ready' or 'abstained'")
    values = _sequence_contents(candidates, path + ".candidates")
    rows = [_scored_candidate(item, f"{path}.candidates[{index}]")
            for index, item in enumerate(values)]
    keys = [_entity_key(row["$fields"]["entity"]) for row in rows]
    if len(set(keys)) != len(keys):
        raise ValueError(f"{path}.candidates: duplicate entity identity")
    details = _decode_plain_container(_mapping_contents(status_details, path + ".status_details"),
                                      path + ".status_details")
    return _envelope("ScoredCandidates", {"candidates": {"$tuple": rows},
        "scorer": scorer, "status": status,
        "status_details": _encode_plain(details, path + ".status_details")})


def make_scored_candidates(candidates, scorer, status="ready", status_details=None):
    return _make_scored_candidates(candidates, scorer, status,
        {} if status_details is None else status_details)


def entity_identifier(entity_ref):
    return _require_string(_typed_fields(entity_ref, "EntityRef", "EntityRef")["identifier"],
                           "EntityRef.identifier", nonempty=True)


def entity_id(entity_ref):
    return entity_identifier(entity_ref)


def aggregate_rows(aggregate):
    fields = _typed_fields(aggregate, "Aggregate", "Aggregate")
    values = _sequence_contents(fields["rows"], "Aggregate.rows")
    for index, item in enumerate(values):
        _typed_fields(item, "AggregateRow", f"Aggregate.rows[{index}]")
    return tuple(_readonly(item) for item in values)


def candidate_items(candidate_set):
    fields = _typed_fields(candidate_set, "CandidateSet", "CandidateSet")
    values = _sequence_contents(fields["candidates"], "CandidateSet.candidates")
    for index, item in enumerate(values):
        _typed_fields(item, "Candidate", f"CandidateSet.candidates[{index}]")
    return tuple(_readonly(item) for item in values)


def native_query_rows(aggregate):
    fields = _typed_fields(aggregate, "Aggregate", "Aggregate")
    metadata = _mapping_contents(fields["metadata"], "Aggregate.metadata")
    query = _mapping_contents(metadata.get("public_query"), "Aggregate.metadata.public_query") \
        if "public_query" in metadata else None
    if not isinstance(query, Mapping):
        raise ValueError("Aggregate.metadata.public_query: expected native query metadata mapping")
    rows = _sequence_contents(query.get("rows_data"), "Aggregate.metadata.public_query.rows_data")
    result = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or "$type" in row:
            raise ValueError(f"Aggregate.metadata.public_query.rows_data[{index}]: expected plain CSV row mapping")
        row = _decode_plain_container(row, f"Aggregate.metadata.public_query.rows_data[{index}]")
        if any(type(key) is not str for key in row):
            raise ValueError(f"Aggregate.metadata.public_query.rows_data[{index}]: keys must be strings")
        for key, item in row.items():
            if item is None or type(item) in (str, bool, int):
                continue
            if type(item) is float and math.isfinite(item):
                continue
            raise ValueError(f"Aggregate.metadata.public_query.rows_data[{index}].{key}: expected a flat finite CSV scalar")
        result.append(_readonly(row))
    return tuple(result)


TYPED_HELPER_FUNCTIONS = {
    "make_window": make_window,
    "make_entity_ref": make_entity_ref,
    "make_evidence": make_evidence,
    "make_candidate": make_candidate,
    "make_candidate_set": make_candidate_set,
    "make_aggregate_row": make_aggregate_row,
    "make_aggregate": make_aggregate,
    "make_scored_candidate": make_scored_candidate,
    "make_scored_candidates": make_scored_candidates,
    "entity_identifier": entity_identifier,
    "entity_id": entity_id,
    "aggregate_rows": aggregate_rows,
    "candidate_items": candidate_items,
    "native_query_rows": native_query_rows,
}


def _validate_helper_registry():
    expected = {"read_case_json", "read_catalog_json", "contract_fields"} | set(TYPED_HELPER_FUNCTIONS)
    if HELPER_NAMES != expected:
        raise RuntimeError("helper API metadata and worker namespace differ")


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


# Runtime names that have actual counterparts in the worker.
Runtime = RuntimeView
Category = Literal["CPU fault", "db close", "db connection limit", "network delay", "network loss"] | None

IR_RESULT_TRANSPORT_VERSION = "e4-typed-ir-result/1"
IR_RESULT_TRANSPORT_CODEC = "zlib+base64"
IR_RESULT_DECODED_LIMIT_BYTES = 1024 * 1024


def _bounded_json_bytes(value, maximum_bytes):
    """Encode JSON incrementally and stop after a bounded decoded result size."""
    encoder = json.JSONEncoder(allow_nan=False, separators=(",", ":"), ensure_ascii=True)
    chunks = []
    total = 0
    for encoded_chunk in encoder.iterencode(value):
        # JSONEncoder emits one chunk per primitive/string in some cases. Split
        # that text before UTF-8 encoding so a large value does not become a
        # second unbounded byte allocation.
        for offset in range(0, len(encoded_chunk), 8192):
            chunk = encoded_chunk[offset:offset + 8192].encode("utf-8")
            total += len(chunk)
            if total > maximum_bytes:
                return None, total
            chunks.append(chunk)
    return b"".join(chunks), total


def _ir_error_response(code, message, required_bytes, allowed_bytes, exact):
    return json.dumps({"ok": False, "result": None,
        "error": {"code": code, "message": message[:400],
            "required_bytes": int(required_bytes), "allowed_bytes": int(allowed_bytes),
            "exact": bool(exact)}, "stdout": "", "stderr": ""},
        allow_nan=False, separators=(",", ":")).encode("utf-8")


def _encode_ir_success_response(result, *, protocol_bytes, decoded_limit_bytes):
    """Return one complete bounded typed-IR success response or a small error."""
    plain_prefix = b'{"ok":true,"result":'
    plain_suffix = b',"stdout":"","stderr":""}'
    raw, measured = _bounded_json_bytes(result, decoded_limit_bytes)
    if raw is None:
        return _ir_error_response("ir_output_limit",
            "typed IR result exceeds the decoded-result byte limit",
            measured, decoded_limit_bytes, False)
    plain_size = len(plain_prefix) + len(raw) + len(plain_suffix)
    if plain_size <= protocol_bytes:
        return plain_prefix + raw + plain_suffix

    digest = hashlib.sha256(raw).hexdigest()
    compressed = zlib.compress(raw, level=9)
    payload_chars = 4 * ((len(compressed) + 2) // 3)
    envelope = {"$transport": IR_RESULT_TRANSPORT_VERSION,
        "codec": IR_RESULT_TRANSPORT_CODEC, "decoded_bytes": len(raw),
        "sha256": digest, "payload": ""}
    envelope_empty = json.dumps(envelope, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    envelope_size = len(envelope_empty) + payload_chars  # replace empty string contents
    wire_size = len(plain_prefix) + envelope_size + len(plain_suffix)
    if wire_size > protocol_bytes:
        return _ir_error_response("ir_output_limit",
            "compressed typed IR result exceeds the wire byte limit",
            wire_size, protocol_bytes, True)
    envelope["payload"] = base64.b64encode(compressed).decode("ascii")
    envelope_bytes = json.dumps(envelope, allow_nan=False,
        separators=(",", ":")).encode("utf-8")
    response = plain_prefix + envelope_bytes + plain_suffix
    if len(response) != wire_size or len(response) > protocol_bytes:
        return _ir_error_response("ir_output_limit",
            "typed IR result exceeded the measured wire byte limit",
            len(response), protocol_bytes, True)
    return response


def main():
    request = json.loads(sys.stdin.read())
    limits = request["limits"]
    resource.setrlimit(resource.RLIMIT_CPU, (limits["cpu_seconds"], limits["cpu_seconds"]))
    resource.setrlimit(resource.RLIMIT_AS, (limits["memory_bytes"], limits["memory_bytes"]))
    resource.setrlimit(resource.RLIMIT_FSIZE, (limits["file_bytes"], limits["file_bytes"]))
    resource.setrlimit(resource.RLIMIT_NOFILE, (limits["open_files"], limits["open_files"]))

    authoring_profile = request.get("authoring_profile")
    if authoring_profile in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
        # This path loads only the fixed trusted interpreter. The model payload
        # is JSON IR and is revalidated by that interpreter; no source is parsed,
        # compiled, or evaluated.
        try:
            import importlib.util
            module_path = Path("/sandbox/m4_operator_ir_worker.py")
            if authoring_profile == "typed-operator-ir-v2":
                module_path = Path("/sandbox/m4_operator_ir_v2_worker.py")
            module_spec = importlib.util.spec_from_file_location("_trusted_m4_operator_ir", module_path)
            if module_spec is None or module_spec.loader is None:
                raise RuntimeError("trusted operator IR interpreter is unavailable")
            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
            runtime = dict(request.get("runtime", {}))
            runtime["ir_family"] = request.get("ir_family")
            runtime["ir_statistic"] = request.get("ir_statistic")
            result = module.execute_ir(request["typed_operator_ir"], request["inputs"],
                request["params"], runtime, request["trusted_context"])
            response = _encode_ir_success_response(result,
                protocol_bytes=int(limits["protocol_bytes"]),
                decoded_limit_bytes=int(limits["ir_result_bytes"]))
            sys.__stdout__.buffer.write(response)
        except BaseException as exc:
            sys.__stdout__.write(json.dumps({"ok": False, "result": None,
                "error": {"code": "operator_ir_error", "message": str(exc)[:1000],
                    "type": type(exc).__name__}, "stdout": "", "stderr": ""},
                allow_nan=False, separators=(",", ":")))
        return
    if authoring_profile not in (None, TYPED_RECORD_PROFILE):
        raise ValueError(f"unsupported authoring_profile {authoring_profile!r}")
    validate_source(request["source"], authoring_profile)
    import ast
    safe_builtins = dict(SAFE_BUILTINS)
    allowed_modules = ALLOWED_MODULES
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

    _validate_helper_registry()
    namespace = {"__builtins__": safe_builtins,
                 "read_case_json": read_case_json,
                 "read_catalog_json": read_catalog_json}
    if authoring_profile == TYPED_RECORD_PROFILE:
        namespace.update({name: helper for name, helper in TYPED_HELPER_FUNCTIONS.items()
                          if name not in {"aggregate_rows", "candidate_items"}})
        namespace.update(TYPED_RECORD_HELPER_FUNCTIONS)
        namespace.update({name: globals()[name] for name in TYPED_RECORD_TYPE_NAMES
                          if name in globals()})
    else:
        namespace["contract_fields"] = contract_fields
        namespace.update(TYPED_HELPER_FUNCTIONS)
    source = request["source"]
    tree = ast.parse(source, mode="exec")
    code = compile(tree, "<generated-operator>", "exec",
                   dont_inherit=authoring_profile != TYPED_RECORD_PROFILE, optimize=2)
    out = LimitedText(limits["stdout_bytes"])
    err = LimitedText(limits["stderr_bytes"])
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            exec(code, namespace, namespace)
            # Preserve legacy generated-operator behavior: inputs remain plain
            # dict/list JSON. Typed helper accessors return read-only views.
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
