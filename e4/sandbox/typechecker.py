"""Strict Mypy type-checking stub generator and validator for model-authored operators."""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
import tempfile
import tokenize
from pathlib import Path
from typing import Any, Mapping

from .ast_validator import validate_source

PROFILE = "typed-operator-code-v1"
CHECK_TIMEOUT_SECONDS = 10
MAX_OUTPUT_BYTES = 16384


class TypeCheckError(ValueError):
    """Raised when generated operator code fails static type-checking."""

    def __init__(
        self,
        message: str,
        line: int | None = None,
        column: int | None = None,
        error_code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ):
        super().__init__(message)
        self.line = line
        self.column = column
        self.error_code = error_code
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "message": str(self),
            "line": self.line,
            "column": self.column,
            "error_code": self.error_code,
            "details": self.details,
        }


def _type_for(value_type: str, nullable: bool = False) -> str:
    types = {
        "string": "str",
        "integer": "int",
        "number": "float",
        "positive_integer": "int",
        "string_list": "Sequence[str]",
        "boolean": "bool",
        "object": "Mapping[str, Any]",
        "array": "Sequence[Any]",
    }
    res = types.get(value_type, "Any")
    return f"{res} | None" if nullable and res != "Any" else res


def _generate_stub(
    spec: Mapping[str, Any],
    dataset_schema: Any = None,
) -> str:
    ports = spec.get("input_ports", [])
    params = spec.get("parameters", {})
    output_type = spec.get("output_type", "Aggregate")

    lines = [
        "from typing import Callable, Generic, Literal, Mapping, Optional, Protocol, Sequence, TypeVar, TypedDict, Union, overload, Iterator",
        "JSONValue = str | int | float | bool | None | list['JSONValue'] | tuple['JSONValue', ...] | Mapping[str, 'JSONValue']",
        "JSONMapping = Mapping[str, JSONValue]",
        "Scalar = str | int | float | bool | None",
        "T = TypeVar('T', covariant=True)",
        "",
        "class WindowView(Protocol):",
        "    @property\n    def name(self) -> Literal['reference', 'alert']: ...",
        "    @property\n    def start_ms_utc(self) -> int: ...",
        "    @property\n    def end_ms_utc(self) -> int: ...",
        "    @property\n    def duration_ms(self) -> int: ...",
        "",
        "class EntityRefView(Protocol):",
        "    @property\n    def role(self) -> str: ...",
        "    @property\n    def identifier(self) -> str: ...",
        "    @property\n    def canonical_id(self) -> str: ...",
        "",
        "class CoverageView(Protocol):",
        "    @property\n    def complete(self) -> bool: ...",
        "    @property\n    def truncated(self) -> bool: ...",
        "    @property\n    def offset(self) -> int: ...",
        "    @property\n    def next_offset(self) -> int | None: ...",
        "    @property\n    def total_matches(self) -> int: ...",
        "    @property\n    def support(self) -> int: ...",
        "",
        "class MetricPointView(Protocol):",
        "    @property\n    def source_table(self) -> str: ...",
        "    @property\n    def timestamp_ms_utc(self) -> int: ...",
        "    @property\n    def entity_role(self) -> str: ...",
        "    @property\n    def entity_identifier(self) -> str: ...",
        "    @property\n    def origin_row_id(self) -> str: ...",
        "    @property\n    def metric_name(self) -> str: ...",
        "    @property\n    def metric_key(self) -> str: ...",
        "    @property\n    def value(self) -> float | None: ...",
        "    @property\n    def unit(self) -> str | None: ...",
        "    @property\n    def details(self) -> JSONMapping: ...",
        "",
        "class SpanView(Protocol):",
        "    @property\n    def source_table(self) -> str: ...",
        "    @property\n    def timestamp_ms_utc(self) -> int: ...",
        "    @property\n    def trace_id(self) -> str: ...",
        "    @property\n    def span_id(self) -> str: ...",
        "    @property\n    def parent_id(self) -> str | None: ...",
        "    @property\n    def duration_ms(self) -> float | None: ...",
        "    @property\n    def duration_value(self) -> float | None: ...",
        "    @property\n    def success(self) -> bool | None: ...",
        "    @property\n    def call_type(self) -> str | None: ...",
        "    @property\n    def executing_component_role(self) -> str | None: ...",
        "    @property\n    def executing_component_identifier(self) -> str | None: ...",
        "    @property\n    def logical_service_role(self) -> str | None: ...",
        "    @property\n    def logical_service_identifier(self) -> str | None: ...",
        "    @property\n    def database_target_role(self) -> str | None: ...",
        "    @property\n    def database_target_identifier(self) -> str | None: ...",
        "    @property\n    def details(self) -> JSONMapping: ...",
        "",
        "class LogRecordView(Protocol):",
        "    @property\n    def source_table(self) -> str: ...",
        "    @property\n    def timestamp_ms_utc(self) -> int: ...",
        "    @property\n    def log_name(self) -> str | None: ...",
        "    @property\n    def entity_role(self) -> str | None: ...",
        "    @property\n    def entity_identifier(self) -> str | None: ...",
        "    @property\n    def value(self) -> str | None: ...",
        "    @property\n    def severity(self) -> str | None: ...",
        "    @property\n    def details(self) -> JSONMapping: ...",
        "",
        "class QueryResult(Generic[T], Protocol):",
        "    @property\n    def rows(self) -> Sequence[T]: ...",
        "    @property\n    def window(self) -> WindowView: ...",
        "    @property\n    def source_tables(self) -> Sequence[str]: ...",
        "    @property\n    def coverage(self) -> CoverageView: ...",
        "    def __iter__(self) -> Iterator[T]: ...",
        "    def __len__(self) -> int: ...",
        "",
        "class SummaryResult(Protocol):",
        "    @property\n    def value(self) -> float | int | None: ...",
        "    @property\n    def support(self) -> int: ...",
        "    @property\n    def missing_count(self) -> int: ...",
        "    @property\n    def nonfinite_count(self) -> int: ...",
        "    @property\n    def invalid_count(self) -> int: ...",
        "",
        "class AggregateGroupView(Protocol):",
        "    @property\n    def key(self) -> tuple[Scalar, ...]: ...",
        "    @property\n    def group_fields(self) -> Sequence[str]: ...",
        "    @property\n    def value(self) -> float | int | str | None: ...",
        "    @property\n    def support(self) -> int: ...",
        "    @property\n    def missing_count(self) -> int: ...",
        "    @property\n    def nonfinite_count(self) -> int: ...",
        "    @property\n    def invalid_count(self) -> int: ...",
        "",
        "class EvidenceView(Protocol):",
        "    @property\n    def source(self) -> str: ...",
        "    @property\n    def window(self) -> WindowView: ...",
        "    @property\n    def statistic(self) -> str: ...",
        "    @property\n    def value(self) -> float | str | bool | None: ...",
        "    @property\n    def contributing_entities(self) -> Sequence[EntityRefView]: ...",
        "    @property\n    def support(self) -> int: ...",
        "    @property\n    def ranking_eligible(self) -> bool: ...",
        "    @property\n    def details(self) -> JSONMapping: ...",
        "",
        "class CandidateView(Protocol):",
        "    @property\n    def entity(self) -> EntityRefView: ...",
        "    @property\n    def rationale(self) -> str: ...",
        "",
        "class CandidateSetView(Protocol):",
        "    @property\n    def candidates(self) -> Sequence[CandidateView]: ...",
        "    @property\n    def source(self) -> str: ...",
        "",
        "class AggregateRowView(Protocol):",
        "    @property\n    def entity(self) -> EntityRefView: ...",
        "    @property\n    def metric_key(self) -> str: ...",
        "    @property\n    def window(self) -> WindowView: ...",
        "    @property\n    def statistic(self) -> str: ...",
        "    @property\n    def value(self) -> float | None: ...",
        "    @property\n    def support(self) -> int: ...",
        "    @property\n    def source_tables(self) -> Sequence[str]: ...",
        "    @property\n    def contributing_entities(self) -> Sequence[EntityRefView]: ...",
        "    @property\n    def details(self) -> JSONMapping: ...",
        "",
        "class AggregateView(Protocol):",
        "    @property\n    def window(self) -> WindowView: ...",
        "    @property\n    def rows(self) -> Sequence[AggregateRowView]: ...",
        "    @property\n    def source_tables(self) -> Sequence[str]: ...",
        "    @property\n    def metadata(self) -> JSONMapping: ...",
        "",
        "class ScoredCandidateView(Protocol):",
        "    @property\n    def entity(self) -> EntityRefView: ...",
        "    @property\n    def score(self) -> float: ...",
        "    @property\n    def category(self) -> str | None: ...",
        "    @property\n    def evidence(self) -> Sequence[EvidenceView]: ...",
        "    @property\n    def analysis_family(self) -> str | None: ...",
        "",
        "class ScoredCandidatesView(Protocol):",
        "    @property\n    def candidates(self) -> Sequence[ScoredCandidateView]: ...",
        "    @property\n    def scorer(self) -> str: ...",
        "    @property\n    def status(self) -> Literal['ready', 'abstained']: ...",
        "    @property\n    def status_details(self) -> JSONMapping: ...",
        "",
        "class TelemetryView(Protocol):",
        "    @property\n    def case_id(self) -> str: ...",
        "    @property\n    def reference_window(self) -> WindowView: ...",
        "    @property\n    def alert_window(self) -> WindowView: ...",
        "    @property\n    def source_tables(self) -> Sequence[str]: ...",
        "    @property\n    def point_count(self) -> int: ...",
        "",
        "# Target Output and Constructor Types",
        "Window = WindowView",
        "EntityRef = EntityRefView",
        "Evidence = EvidenceView",
        "Candidate = CandidateView",
        "CandidateSet = CandidateSetView",
        "AggregateRow = AggregateRowView",
        "Aggregate = AggregateView",
        "ScoredCandidate = ScoredCandidateView",
        "ScoredCandidates = ScoredCandidatesView",
        "TelemetryHandle = TypedDict('TelemetryHandle', {'$type': Literal['TelemetryHandle'], '$fields': Mapping[str, JSONValue]})",
        "Telemetry = TelemetryHandle",
        "",
        "# Helper functions",
        "def telemetry_view(telemetry: TelemetryHandle | TelemetryView) -> TelemetryView: ...",
        "def metric_points(telemetry: TelemetryView, source_table: str, window: WindowView, entity_role: str | None = ..., entity_identifier: str | None = ..., metric_name: str | None = ..., metric_key: str | None = ..., limit: int = ..., offset: int = ...) -> QueryResult[MetricPointView]: ...",
        "def span_records(telemetry: TelemetryView, source_table: str, window: WindowView, component_role: str | None = ..., component_identifier: str | None = ..., limit: int = ..., offset: int = ...) -> QueryResult[SpanView]: ...",
        "def log_records(telemetry: TelemetryView, source_table: str, window: WindowView, entity_identifier: str | None = ..., log_name: str | None = ..., limit: int = ..., offset: int = ...) -> QueryResult[LogRecordView]: ...",
        "def aggregate_records(telemetry: TelemetryView, source_table: str, window: WindowView, record_type: Literal['metric', 'span', 'log'], field: str, statistic: Literal['count', 'sum', 'mean', 'min', 'max', 'quantile'], group_by: Sequence[str] = ..., time_bucket_ms: int | None = ..., quantile: float | None = ..., limit: int = ..., offset: int = ...) -> QueryResult[AggregateGroupView]: ...",
        "@overload\ndef group_by(rows: Sequence[MetricPointView], fields: Sequence[str]) -> Mapping[tuple[Scalar, ...], Sequence[MetricPointView]]: ...",
        "@overload\ndef group_by(rows: Sequence[SpanView], fields: Sequence[str]) -> Mapping[tuple[Scalar, ...], Sequence[SpanView]]: ...",
        "@overload\ndef group_by(rows: Sequence[LogRecordView], fields: Sequence[str]) -> Mapping[tuple[Scalar, ...], Sequence[LogRecordView]]: ...",
        "def summarize(values: Sequence[float | int | None], statistic: Literal['count', 'distinct', 'sum', 'mean', 'min', 'max', 'quantile'], quantile: float | None = ...) -> SummaryResult: ...",
        "@overload\ndef top_k(rows: Sequence[MetricPointView], field: str, k: int, descending: bool = ...) -> Sequence[MetricPointView]: ...",
        "@overload\ndef top_k(rows: Sequence[SpanView], field: str, k: int, descending: bool = ...) -> Sequence[SpanView]: ...",
        "@overload\ndef top_k(rows: Sequence[LogRecordView], field: str, k: int, descending: bool = ...) -> Sequence[LogRecordView]: ...",
        "def make_window(name: Literal['reference', 'alert'], start_ms_utc: int, end_ms_utc: int) -> WindowView: ...",
        "def make_entity_ref(role: str, identifier: str) -> EntityRefView: ...",
        "def make_evidence(source: str, window: WindowView, statistic: str, value: float | str | bool | None, contributing_entities: Sequence[EntityRefView], support: int, ranking_eligible: bool = ..., details: JSONMapping | None = ...) -> EvidenceView: ...",
        "def make_candidate(entity: EntityRefView, rationale: str = ...) -> CandidateView: ...",
        "def make_candidate_set(candidates: Sequence[CandidateView], source: str) -> CandidateSetView: ...",
        "def make_aggregate_row(entity: EntityRefView, metric_key: str, window: WindowView, statistic: str, value: float | None, support: int, source_tables: Sequence[str] = ..., contributing_entities: Sequence[EntityRefView] = ..., details: JSONMapping | None = ...) -> AggregateRowView: ...",
        "def make_aggregate(window: WindowView, rows: Sequence[AggregateRowView], source_tables: Sequence[str] = ..., metadata: JSONMapping | None = ...) -> AggregateView: ...",
        "def make_scored_candidate(entity: EntityRefView, score: float, category: str | None = ..., evidence: Sequence[EvidenceView] = ..., analysis_family: str | None = ...) -> ScoredCandidateView: ...",
        "def make_scored_candidates(candidates: Sequence[ScoredCandidateView], scorer: str, status: Literal['ready', 'abstained'] = ..., status_details: JSONMapping | None = ...) -> ScoredCandidatesView: ...",
        "def entity_identifier(entity_ref: EntityRefView) -> str: ...",
        "def entity_id(entity_ref: EntityRefView) -> str: ...",
        "def aggregate_view(agg: AggregateView) -> AggregateView: ...",
        "def candidate_views(cset: CandidateSetView) -> tuple[CandidateView, ...]: ...",
        "def scored_views(sc: ScoredCandidatesView) -> tuple[ScoredCandidateView, ...]: ...",
        "",
        "# Inputs TypedDict for the operator",
        "class Inputs(TypedDict, total=False):",
    ]

    for p in ports:
        ptype = p.get("type", "Aggregate")
        ptarget = {
            "Telemetry": "TelemetryHandle",
            "Aggregate": "AggregateView",
            "CandidateSet": "CandidateSetView",
            "ScoredCandidates": "ScoredCandidatesView",
        }.get(ptype, "AggregateView")
        if p.get("variadic"):
            ptarget = f"Sequence[{ptarget}]"
        lines.append(f"    {p.get('name', 'input')}: {ptarget}")

    if not ports:
        lines.append("    pass")

    lines.append("")
    lines.append("class Params(TypedDict, total=False):")
    for pname, pval in params.items():
        vtype = _type_for(pval.get("type", "string"), pval.get("nullable", False))
        lines.append(f"    {pname}: {vtype}")
    if not params:
        lines.append("    pass")

    lines.append("")
    lines.append("class Runtime(Protocol):")
    lines.append("    dag_depth: int")
    lines.append("    pass")

    lines.append("")
    lines.append(f"Output = {output_type}View")
    return "\n".join(lines) + "\n"


def _annotate_source(source: str) -> str:
    """Prepend stub import and ensure operator signature is typed for Mypy."""
    byte_source = source.encode("utf-8")
    byte_lines = byte_source.splitlines(keepends=True)
    tree = ast.parse(source)
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "operator"]
    node = funcs[0]

    offsets = [0]
    for line in byte_lines:
        offsets.append(offsets[-1] + len(line))

    inserts: list[tuple[int, int, bytes]] = []
    aliases = ("Inputs", "Params", "Runtime")
    for arg, alias in zip(node.args.args, aliases):
        arg_start = offsets[arg.lineno - 1] + arg.col_offset
        arg_end = offsets[arg.end_lineno - 1] + arg.end_col_offset
        if arg.annotation is None:
            inserts.append((arg_end, arg_end, f": {alias}".encode()))
        else:
            ann_start = offsets[arg.annotation.lineno - 1] + arg.annotation.col_offset
            ann_end = offsets[arg.annotation.end_lineno - 1] + arg.annotation.end_col_offset
            inserts.append((ann_start, ann_end, alias.encode()))

    # Find where function header closes to add return type
    tokens = tokenize.tokenize(iter(byte_lines).__next__)
    depth = 0
    in_header = False
    close_offset = None
    for token in tokens:
        if token.start[0] == node.lineno and token.type == tokenize.NAME and token.string == "operator":
            in_header = True
        if in_header and token.type == tokenize.OP:
            if token.string == "(":
                depth += 1
            elif token.string == ")":
                depth -= 1
                if depth == 0:
                    row = token.end[0] - 1
                    col = token.end[1]
                    close_offset = offsets[row] + len(byte_lines[row].decode("utf-8")[:col].encode("utf-8"))
                    break

    if close_offset is None:
        raise TypeCheckError("Could not locate operator signature")

    if node.returns is not None:
        ret_start = offsets[node.returns.lineno - 1] + node.returns.col_offset
        ret_end = offsets[node.returns.end_lineno - 1] + node.returns.end_col_offset
        inserts.append((ret_start, ret_end, b"Output"))
    else:
        inserts.append((close_offset, close_offset, b" -> Output"))

    transformed = byte_source
    for lo, hi, data in sorted(inserts, reverse=True):
        transformed = transformed[:lo] + data + transformed[hi:]

    header = (
        "from typing import Any, Mapping, Sequence, Literal, Optional, Union\n"
        "from _e4_typed_profile_api import (\n"
        "    Inputs, Params, Runtime, Output,\n"
        "    WindowView, EntityRefView, CoverageView, MetricPointView, SpanView, LogRecordView,\n"
        "    QueryResult, SummaryResult, AggregateGroupView, EvidenceView, CandidateView,\n"
        "    CandidateSetView, AggregateRowView, AggregateView, ScoredCandidateView, ScoredCandidatesView,\n"
        "    TelemetryView, Window, EntityRef, Evidence, Candidate, CandidateSet, AggregateRow,\n"
        "    Aggregate, ScoredCandidate, ScoredCandidates, Telemetry, TelemetryHandle, Scalar,\n"
        "    telemetry_view, metric_points, span_records, log_records, aggregate_records,\n"
        "    group_by, summarize, top_k, make_window, make_entity_ref, make_evidence,\n"
        "    make_candidate, make_candidate_set, make_aggregate_row, make_aggregate,\n"
        "    make_scored_candidate, make_scored_candidates, entity_identifier, entity_id,\n"
        "    aggregate_view, candidate_views, scored_views\n"
        ")\n\n"
    ).encode()

    return (header + transformed).decode("utf-8")


def check_operator_types(
    spec: Mapping[str, Any],
    dataset_schema: Any = None,
) -> dict[str, Any]:
    """Strictly type-check authored operator using Mypy in an isolated temp environment."""
    profile = spec.get("authoring_profile", PROFILE)
    if profile != PROFILE:
        raise TypeCheckError(f"Spec must select authoring_profile '{PROFILE}', got '{profile}'")

    source = spec.get("source")
    if not isinstance(source, str) or not source.strip():
        raise TypeCheckError("Spec must define non-empty Python 'source'")

    # Validate AST security first
    validate_source(source)

    stub = _generate_stub(spec, dataset_schema)
    annotated = _annotate_source(source)

    with tempfile.TemporaryDirectory(prefix="e4-typecheck-") as tmpdir:
        root = Path(tmpdir)
        (root / "_e4_typed_profile_api.pyi").write_text(stub, encoding="utf-8")
        (root / "candidate.py").write_text(annotated, encoding="utf-8")
        (root / "mypy.ini").write_text("[mypy]\nshow_error_codes = True\n", encoding="utf-8")

        env = {
            "PATH": os.environ.get("PATH", os.defpath),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "MYPYPATH": tmpdir,
        }

        cmd = [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--disallow-any-expr",
            "--disallow-any-explicit",
            "--disallow-any-unimported",
            "--warn-unused-ignores",
            "--no-incremental",
            "--no-site-packages",
            f"--config-file={root / 'mypy.ini'}",
            "--show-error-codes",
            "--show-column-numbers",
            "--no-error-summary",
            "candidate.py",
        ]

        try:
            proc = subprocess.run(
                cmd,
                cwd=tmpdir,
                env=env,
                capture_output=True,
                text=True,
                timeout=CHECK_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise TypeCheckError("Mypy type-checking timed out") from exc

        if proc.returncode != 0:
            output = (proc.stdout + proc.stderr)[:MAX_OUTPUT_BYTES]
            matches = list(
                re.finditer(
                    r"candidate\.py:(\d+):(\d+): error: (.*?)(?: \[([^]]+)\])?$",
                    output,
                    re.M,
                )
            )
            if matches:
                first = matches[0]
                # Header offset adjustment: header has 13 lines
                line = max(1, int(first.group(1)) - 13)
                column = int(first.group(2))
                err_code = first.group(4) or "type_error"
                msg_lines = [
                    f"Line {max(1, int(m.group(1)) - 13)}, Col {m.group(2)}: {m.group(3).strip()} [{m.group(4) or 'type_error'}]"
                    for m in matches
                ]
                combined_msg = "\n".join(msg_lines)
                raise TypeCheckError(
                    f"Mypy type check failed:\n{combined_msg}",
                    line=line,
                    column=column,
                    error_code=err_code,
                    details={"errors": msg_lines},
                )
            raise TypeCheckError(f"Mypy exited with status {proc.returncode}:\n{output.strip()}")

    return {"status": "passed"}
