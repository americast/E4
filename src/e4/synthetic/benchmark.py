"""Frozen synthetic E4 task suite, static repair policy, and independent checks."""

from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from e4.dsl.cache import CacheStore, digest_value
from e4.contracts import EntityRef, MetricPoint, Span, Telemetry, Window
from e4.dsl.verifier import DAGValidationError, verify_dag
from e4.dsl.executor import (ExecutionFailure, NodeContext, SerialExecutor)
from e4.agent.model_client import LocalModelClient, ModelCall
from e4.dsl.registry import OperatorRegistry
from e4.dsl.runtime import RuntimeContext, RuntimeSchema
from e4.sandbox.runner import run_generated_operator
from e4.paths import ROOT


DEFAULT_SUITE = ROOT / "config/synthetic/suite_v3.json"
REPAIR_LIMIT = 2
OUTPUT_NORMALIZATION_POLICY = "single_outer_fence_v1"
ALLOWED_CATEGORIES = {"CPU fault", "db close", "db connection limit",
                      "network delay", "network loss"}
_REPAIRABLE_CODES = {"json_syntax", "json_fence", "json_schema", "dag_validation",
                     "python_syntax", "python_signature", "python_ast"}
_FORBIDDEN_NODES = (ast.ClassDef, ast.AsyncFunctionDef, ast.Lambda, ast.Global,
                    ast.Nonlocal, ast.With, ast.AsyncWith, ast.Try, ast.Raise,
                    ast.Delete, ast.Yield, ast.YieldFrom, ast.Await)
_ALLOWED_IMPORTS = {"math", "statistics", "itertools", "collections"}
_RESTRICTED_NAMES = {"open", "eval", "exec", "compile", "globals", "locals", "getattr",
                     "setattr", "delattr", "vars", "input", "breakpoint", "memoryview"}
_CHECKER_FILES = (
    "src/e4/synthetic/benchmark.py", "src/e4/agent/model_client.py", "src/e4/sandbox/runner.py",
    "src/e4/sandbox/worker.py", "src/e4/dsl/verifier.py", "src/e4/dsl/executor.py",
    "src/e4/operators/telecom/operators.py", "src/e4/operators/numerics.py", "src/e4/contracts.py",
    "src/e4/dsl/runtime.py", "src/e4/dsl/registry.py", "src/e4/dsl/cache.py",
    "scripts/synthetic/run_benchmark.py", "config/runtime/schema.json",
    "config/runtime/values.json", "config/records/decisions.json",
    "config/telecom/metric_kind_policy.json", "config/telecom/seed_playbooks.json",
    "pyproject.toml", "requirements.lock",
)


class StaticTaskError(ValueError):
    def __init__(self, code: str, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


class SemanticTaskError(ValueError):
    def __init__(self, code: str, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


@dataclass(frozen=True)
class TaskOutcome:
    task_id: str
    kind: str
    passed: bool
    initial_passed: bool
    repairs: int
    static_passed: bool | None
    semantic_passed: bool
    result: Mapping[str, Any]
    calls: tuple[Mapping[str, Any], ...]
    elapsed_seconds: float
    input_tokens: int
    output_tokens: int
    repair_errors: tuple[Mapping[str, Any], ...]
    error: Mapping[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        return {"task_id": self.task_id, "kind": self.kind, "passed": self.passed,
                "initial_passed": self.initial_passed, "repairs": self.repairs,
                "static_passed": self.static_passed,
                "semantic_passed": self.semantic_passed, "result": dict(self.result),
                "calls": [dict(call) for call in self.calls],
                "elapsed_seconds": self.elapsed_seconds,
                "input_tokens": self.input_tokens, "output_tokens": self.output_tokens,
                "repair_errors": [dict(error) for error in self.repair_errors],
                "error": dict(self.error) if self.error else None}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def checker_sha256(root: Path = ROOT) -> tuple[str, dict[str, str]]:
    hashes: dict[str, str] = {}
    aggregate = hashlib.sha256()
    for relative in _CHECKER_FILES:
        path = root / relative
        file_hash = sha256_file(path)
        hashes[relative] = file_hash
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(bytes.fromhex(file_hash))
        aggregate.update(b"\0")
    return aggregate.hexdigest(), hashes


def validate_suite(suite: Mapping[str, Any]) -> list[dict[str, Any]]:
    supported_versions = {"e4-m3-synthetic-suite/2": "m3-synthetic-v2",
                          "e4-m3-synthetic-suite/3": "m3-synthetic-v3"}
    if supported_versions.get(suite.get("schema_version")) != suite.get("suite_id"):
        raise ValueError("unsupported M3 synthetic suite version")
    if suite.get("repair_limit") != REPAIR_LIMIT or suite.get("normalization_policy") != OUTPUT_NORMALIZATION_POLICY:
        raise ValueError("suite repair and normalization policy differ from the reviewed checker policy")
    tasks = suite.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 20:
        raise ValueError("frozen M3 suite must contain exactly 20 tasks")
    counts = Counter(task.get("kind") for task in tasks if isinstance(task, dict))
    expected_counts = {"dag": 8, "operator": 6, "diagnosis": 4, "schema": 2}
    if dict(counts) != expected_counts:
        raise ValueError(f"suite task distribution must be {expected_counts}; got {dict(counts)}")
    ids = [task.get("id") for task in tasks]
    if any(not isinstance(task_id, str) or not task_id for task_id in ids) or len(set(ids)) != 20:
        raise ValueError("suite task identifiers must be unique non-empty strings")
    for task in tasks:
        if task.get("kind") not in expected_counts or not isinstance(task.get("prompt"), str):
            raise ValueError(f"invalid task entry: {task.get('id')}")
        if task["kind"] == "operator":
            checks = task.get("checks")
            if not isinstance(checks, list) or len(checks) < 2:
                raise ValueError(f"operator task {task['id']} needs at least two held-out checks")
            contract = task.get("public_contract")
            if (not isinstance(contract, Mapping) or
                    set(contract) != {"inputs", "params", "runtime", "output"} or
                    any(not isinstance(contract[key], Mapping) for key in contract)):
                raise ValueError(f"operator task {task['id']} needs a public input/output schema")
            for schema_name, fields in contract.items():
                if any(not isinstance(name, str) or not name or
                       not isinstance(description, str) or not description
                       for name, description in fields.items()):
                    raise ValueError(f"operator task {task['id']} has an invalid {schema_name} schema")
            for case in checks:
                if any(set(case.get(name, {})) != set(contract[schema_name])
                       for name, schema_name in (("inputs", "inputs"), ("params", "params"),
                                                 ("runtime", "runtime"), ("expected", "output"))):
                    raise ValueError(f"operator task {task['id']} held-out shape differs from its public schema")
    return tasks


def runtime_context() -> RuntimeContext:
    schema = RuntimeSchema.from_json(ROOT / "config/runtime/schema.json")
    supplied = json.loads((ROOT / "config/runtime/values.json").read_text())
    reference = Window("reference", 0, 1_800_000)
    alert = Window("alert", 1_800_000, 3_600_000)
    return RuntimeContext.build(schema, supplied,
                               {"reference_window": reference, "alert_window": alert})


def task_prompt(task: Mapping[str, Any], registry: OperatorRegistry,
                runtime: RuntimeContext) -> str:
    kind = task["kind"]
    sections = [task["prompt"].strip()]
    if kind in {"dag", "diagnosis"}:
        sections.append("E4 JSON DAG schema: {\"schema_version\":\"e4-dag/1\",\"nodes\":[{\"id\":string,\"operator\":string,\"version\":string,\"inputs\":object,\"params\":object,\"bindings\":object}],\"return\":{\"node\":string}}. Nodes are ordered and may reference only earlier nodes. The sole return must be a RankedAnswer produced by answers.combine. Return one JSON object without prose.")
        sections.append("Registered operator signatures:\n" + json.dumps(registry.signatures(),
                                                                          ensure_ascii=False, sort_keys=True,
                                                                          separators=(",", ":")))
        runtime_schema = json.loads((ROOT / "config/runtime/schema.json").read_text())
        seed_library = json.loads((ROOT / "config/telecom/seed_playbooks.json").read_text())
        sections.append("Published runtime schema:\n" + json.dumps(runtime_schema,
            ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        sections.append("Allowlisted runtime values:\n" + json.dumps(runtime.as_dict()["values"],
                                                                        ensure_ascii=False, sort_keys=True,
                                                                        separators=(",", ":")))
        sections.append("Reviewed generic seed DAG examples (structures only):\n" + json.dumps(
            seed_library.get("examples", {}), ensure_ascii=False, sort_keys=True,
            separators=(",", ":")))
        sections.append("Telemetry input schema: metrics are typed by source table, entity role/id, metric name, BOMC id, item id, UTC-millisecond timestamp, and finite value or missing value. Trace spans are identified by (traceId,id), and pid is resolved only within the same trace; component, logical service, and database target are separate roles. Windows are adjacent half-open 30-minute UTC-millisecond intervals. Deployment catalog IDs name backend components and mappings; catalog membership does not make a component faulty.")
        if kind == "diagnosis":
            sections.append("Synthetic telemetry recipe mapping: each metric_series item provides table/entity/entity_role/metric_name/bomc_id/item_id and separate reference/alert value arrays; values become MetricPoints in their named half-open window at evenly spaced UTC-millisecond timestamps. Each trace_groups item creates count traces in its named window with trace_id derived from trace_prefix and index. Its root and child templates become Spans: span_id maps to id, parent_id stays within that trace, elapsed maps to elapsed_raw, and call_type is preserved; component, service, and database map respectively to executing_component, logical_service, and database_target. Success is taken from success, success_pattern, or fail_first as present. These recipe fields are the complete public input; non-telemetry annotations are not part of the input.")
            sections.append("Diagnosis category output schema: category must be one of " +
                            json.dumps(sorted(ALLOWED_CATEGORIES), ensure_ascii=False) +
                            " or null. Choose only from this public enum.")
    elif kind == "operator":
        sections.append("Operator execution contract: use exactly `def operator(inputs, params, runtime)`. Inputs and params are ordinary JSON mappings accessed by string keys. Runtime is a recursively read-only view and supports both runtime.field and runtime['field']. Allowed imports are math, statistics, itertools, and collections. The exact available builtins are abs, all, any, bool, dict, enumerate, float, int, len, list, max, min, range, round, set, sorted, str, sum, tuple, zip, print, Exception, and ValueError. Other builtins, including isinstance and type, are unavailable. Forbidden syntax includes classes, async functions/with, lambdas, global/nonlocal, with, try/except, raise, delete, yield, await, and restricted dynamic/host names such as open, eval, exec, compile, globals, locals, getattr, setattr, delattr, vars, input, breakpoint, and memoryview. Optional read_case_json/read_catalog_json helpers require explicit mounts; neither mount is provided for these tasks. Return finite JSON-compatible values only.")
        contract = task.get("public_contract")
        if not isinstance(contract, Mapping):
            raise ValueError(f"operator task {task.get('id')} has no public input/output schema")
        sections.append("Public operator interface schema (field names and JSON types only):\n" +
                        json.dumps(contract, ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":")))
    public_input = task.get("input", task.get("telemetry"))
    if kind == "diagnosis" and isinstance(public_input, Mapping):
        # Keep prompt input limited to fields consumed by telemetry_from_spec.
        public_input = {key: value for key, value in public_input.items()
                        if key in {"case_id", "reference_window", "alert_window",
                                   "deployment_entities", "metric_series", "trace_groups"}}
    if public_input is not None:
        sections.append("Synthetic task input (public to this task):\n" +
                        json.dumps(public_input, ensure_ascii=False, sort_keys=True, indent=2,
                                   allow_nan=False))
    return "\n\n".join(sections)


def build_calibration_prompt(registry: OperatorRegistry, runtime: RuntimeContext) -> str:
    """Representative full-library prompt for per-model context calibration."""
    calibration_task = {"id": "calibration-full-library", "kind": "dag",
        "prompt": "Calibration request: propose one valid DAG composition that uses metric contrast, trace failure contrast, trace latency contrast, conditional flow attribution, and dependency propagation. Include reference/alert aggregates where required, one shared candidate generator, and one final rank node. This is a prompt-size and generation calibration, not one of the frozen 20 benchmark tasks."}
    return task_prompt(calibration_task, registry, runtime)


def normalize_single_fence(text: str, language: str) -> tuple[str, bool]:
    """Allow raw output or one outer Markdown fence with the requested language."""
    if not isinstance(text, str) or not text.strip():
        raise StaticTaskError("json_syntax" if language == "json" else "python_syntax",
                              "model returned empty final content")
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped, False
    import re
    match = re.fullmatch(r"```([A-Za-z0-9_-]*)[ \t]*\n(.*?)\n?```", stripped, flags=re.DOTALL)
    if not match:
        raise StaticTaskError("json_fence" if language == "json" else "python_syntax",
                              "expected exactly one outer fenced block with no surrounding text")
    fence_language, body = match.groups()
    if fence_language and fence_language.casefold() != language.casefold():
        raise StaticTaskError("json_fence" if language == "json" else "python_syntax",
                              f"fence language must be {language}")
    body = body.strip()
    if not body:
        raise StaticTaskError("json_syntax" if language == "json" else "python_syntax",
                              "fenced final artifact is empty")
    return body, True


def parse_strict_json_object(text: str) -> tuple[dict[str, Any], bool]:
    normalized, fenced = normalize_single_fence(text, "json")

    def pairs_no_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError(f"non-finite JSON value {value} is forbidden")

    try:
        value = json.loads(normalized, object_pairs_hook=pairs_no_duplicates,
                           parse_constant=reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise StaticTaskError("json_syntax", f"invalid strict JSON: {exc}") from exc
    if type(value) is not dict:
        raise StaticTaskError("json_schema", "final JSON artifact must be an object")
    return value, fenced


def validate_operator_source(source_text: str) -> tuple[str, ast.Module]:
    source, _fenced = normalize_single_fence(source_text, "python")
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise StaticTaskError("python_syntax", f"Python syntax error: {exc.msg}",
                              {"line": exc.lineno, "offset": exc.offset}) from exc
    operators = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.name == "operator"]
    if len(operators) != 1 or isinstance(operators[0], ast.AsyncFunctionDef):
        raise StaticTaskError("python_signature", "source must define one synchronous operator function")
    args = operators[0].args
    if (len(args.args) != 3 or args.posonlyargs or args.vararg or args.kwarg or
            args.kwonlyargs or args.defaults or args.kw_defaults or
            [arg.arg for arg in args.args] != ["inputs", "params", "runtime"]):
        raise StaticTaskError("python_signature",
                              "signature must be def operator(inputs, params, runtime)")
    for node in ast.walk(tree):
        if isinstance(node, _FORBIDDEN_NODES):
            raise StaticTaskError("python_ast", f"restricted Python construct: {type(node).__name__}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("_"):
            raise StaticTaskError("python_ast", "private attribute access is restricted")
        if isinstance(node, ast.Name) and (node.id.startswith("__") or node.id in _RESTRICTED_NAMES):
            raise StaticTaskError("python_ast", f"restricted name: {node.id}")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = ([item.name.split(".")[0] for item in node.names]
                     if isinstance(node, ast.Import) else [node.module or ""])
            if getattr(node, "level", 0) or any(name not in _ALLOWED_IMPORTS for name in names):
                raise StaticTaskError("python_ast", "source imports a restricted module")
    return source + "\n", tree


def _deep_equal(actual: Any, expected: Any, *, tolerance: float = 1e-9) -> bool:
    if isinstance(expected, bool) or expected is None or isinstance(expected, (str, int)):
        return type(actual) is type(expected) and actual == expected
    if isinstance(expected, float):
        return type(actual) in (int, float) and not isinstance(actual, bool) and math.isclose(
            float(actual), expected, rel_tol=tolerance, abs_tol=tolerance)
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(
            _deep_equal(left, right, tolerance=tolerance) for left, right in zip(actual, expected))
    if isinstance(expected, dict):
        return type(actual) is dict and actual.keys() == expected.keys() and all(
            _deep_equal(actual[key], expected[key], tolerance=tolerance) for key in expected)
    return False


def _finite_number(value: Any) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, ValueError):
        return False


def _operator_field_matches(actual: Any, expected: Any, schema: str,
                            *, tolerance: float = 1e-9) -> bool:
    if schema == "integer":
        return type(actual) is int and type(expected) is int and actual == expected
    if schema == "boolean":
        return type(actual) is bool and type(expected) is bool and actual is expected
    if schema.startswith("finite number"):
        if expected is None:
            return actual is None
        return (_finite_number(actual) and _finite_number(expected) and
                math.isclose(float(actual), float(expected), rel_tol=tolerance, abs_tol=tolerance))
    if schema.startswith("array of integers"):
        return (type(actual) is list and type(expected) is list and len(actual) == len(expected) and
                all(type(value) is int for value in actual) and
                all(type(value) is int for value in expected) and actual == expected and
                ("sorted ascending" not in schema or actual == sorted(actual)))
    if schema.startswith("array of finite numbers"):
        return (type(actual) is list and type(expected) is list and len(actual) == len(expected) and
                all(_finite_number(value) for value in actual) and
                all(_finite_number(value) for value in expected) and
                all(math.isclose(float(left), float(right), rel_tol=tolerance, abs_tol=tolerance)
                    for left, right in zip(actual, expected)))
    return False


def _operator_output_matches(actual: Any, expected: Any,
                             schema: Mapping[str, Any]) -> bool:
    if (type(actual) is not dict or type(expected) is not dict or
            actual.keys() != expected.keys() or schema.keys() != expected.keys()):
        return False
    return all(isinstance(schema[key], str) and
               _operator_field_matches(actual[key], expected[key], schema[key])
               for key in expected)


def _validate_dag_task(task_id: str, value: Mapping[str, Any], registry: OperatorRegistry,
                       runtime: RuntimeContext, run_dir: Path) -> dict[str, Any]:
    try:
        verified = verify_dag(value, registry, runtime,
                              max_nodes=int(runtime.snapshot.values["max_nodes"]),
                              max_depth=int(runtime.snapshot.values["dag_depth"]))
    except DAGValidationError as exc:
        raise StaticTaskError("dag_validation", str(exc), exc.as_dict()) from exc
    ops = [node.operator.name for node in verified.nodes]
    scorer_nodes = [node for node in verified.nodes if node.operator.output_type == "ScoredCandidates"]
    scorers = sorted(node.operator.name for node in scorer_nodes)
    aggregate_nodes = [node for node in verified.nodes if node.operator.name in {"metrics.aggregate", "traces.aggregate"}]
    actual_aggregates = sorted(f"{node.operator.name}:{node.params.get('window')}" for node in aggregate_nodes)
    expected_scorers = _DAG_TASKS[task_id]["scorers"]
    expected_aggregates = _DAG_TASKS[task_id]["aggregates"]
    if sorted(scorers) != sorted(expected_scorers) or actual_aggregates != sorted(expected_aggregates):
        raise SemanticTaskError("dag_composition_mismatch",
                                "valid DAG does not implement the requested operator composition",
                                {"scorers": scorers, "aggregates": actual_aggregates})
    if ops.count("candidates.generate") != 1 or ops.count("answers.combine") != 1:
        raise SemanticTaskError("dag_shared_nodes", "DAG must share one candidate generator and final ranker")
    required_binding = _DAG_TASKS[task_id].get("rank_binding")
    required_override = _DAG_TASKS[task_id].get("rank_override")
    if required_binding:
        rank = next(node for node in verified.nodes if node.operator.name == "answers.combine")
        if (rank.bindings.get("limit") != required_binding or
                rank.params.get("limit") != required_override):
            raise SemanticTaskError("runtime_binding_mismatch",
                                    "rank limit must include the requested runtime binding and explicit override")
    aggregate_by_id = {node.node_id: node for node in aggregate_nodes}
    for scorer in scorer_nodes:
        if scorer.operator.name in {"metrics.contrast", "traces.error_contrast", "traces.latency_contrast"}:
            family = "metrics.aggregate" if scorer.operator.name == "metrics.contrast" else "traces.aggregate"
            for port, window_name in (("reference", "reference"), ("alert", "alert")):
                ref = scorer.inputs[port][0]
                aggregate = aggregate_by_id.get(ref)
                if aggregate is None or aggregate.operator.name != family or aggregate.params.get("window") != window_name:
                    raise SemanticTaskError("dag_window_wiring",
                        f"{scorer.operator.name}.{port} must use {family} for {window_name}")
    # Execute each accepted composition against the same fixed, label-free synthetic telemetry.
    telemetry = composition_telemetry()
    sources = [node for node in verified.nodes if node.operator.name == "case.telemetry"]
    if len(sources) != 1:
        raise SemanticTaskError("dag_telemetry_source", "DAG must have one telemetry source")
    executor = SerialExecutor(registry, CacheStore(run_dir / "core_cache", enabled=False))
    try:
        executed = executor.execute_document(value, runtime,
            lambda: {sources[0].node_id: telemetry}, cache_enabled=False)
    except ExecutionFailure as exc:
        raise SemanticTaskError("dag_execution_failure", str(exc), exc.as_dict()) from exc
    flow_status = None
    if "flow.conditional_contribution" in scorers:
        from e4.operators.telecom.operators import generate_candidates, score_conditional_flow
        flow_context = NodeContext(telemetry.case_id, "composition_flow_probe",
            "flow.conditional_contribution@1", runtime.snapshot.values,
            runtime.snapshot.provenance, telemetry.reference_window,
            telemetry.alert_window, telemetry.data_sha256)
        candidates = generate_candidates({"data": telemetry, "aggregates": ()}, {}, flow_context)
        flow_result = score_conditional_flow({"data": telemetry, "candidates": candidates},
                                             {}, flow_context)
        flow_status = flow_result.status
        if flow_status != "ready":
            raise SemanticTaskError("composition_flow_fixture_abstained",
                                    "fixed composition fixture must exercise supported conditional flow",
                                    {"status": flow_status,
                                     "details": dict(flow_result.status_details)})
    return {"valid": True, "node_count": verified.measured_node_count,
            "measured_graph_depth": verified.max_depth, "operators": ops,
            "scorers": scorers, "aggregates": actual_aggregates,
            "rank_runtime_binding": required_binding,
            "rank_explicit_override": required_override,
            "flow_status": flow_status,
            "executed_answer_top3": [{"role": item.entity.role,
                                       "component": item.entity.identifier,
                                       "score": item.score}
                                      for item in executed.answer.leads[:3]]}


_DAG_TASKS = {
    "dag_01_metric_and_trace_error": {
        "scorers": ["metrics.contrast", "traces.error_contrast"],
        "aggregates": ["metrics.aggregate:reference", "metrics.aggregate:alert",
                       "traces.aggregate:reference", "traces.aggregate:alert"]},
    "dag_02_metric_and_trace_latency": {
        "scorers": ["metrics.contrast", "traces.latency_contrast"],
        "aggregates": ["metrics.aggregate:reference", "metrics.aggregate:alert",
                       "traces.aggregate:reference", "traces.aggregate:alert"]},
    "dag_03_metric_and_conditional_flow": {
        "scorers": ["metrics.contrast", "flow.conditional_contribution"],
        "aggregates": ["metrics.aggregate:reference", "metrics.aggregate:alert"]},
    "dag_04_trace_error_and_dependency": {
        "scorers": ["traces.error_contrast", "dependencies.propagation"],
        "aggregates": ["traces.aggregate:reference", "traces.aggregate:alert"]},
    "dag_05_trace_latency_and_flow": {
        "scorers": ["traces.latency_contrast", "flow.conditional_contribution"],
        "aggregates": ["traces.aggregate:reference", "traces.aggregate:alert"]},
    "dag_06_flow_and_dependency": {
        "scorers": ["flow.conditional_contribution", "dependencies.propagation"],
        "aggregates": []},
    "dag_07_metric_error_dependency": {
        "scorers": ["metrics.contrast", "traces.error_contrast", "dependencies.propagation"],
        "aggregates": ["metrics.aggregate:reference", "metrics.aggregate:alert",
                       "traces.aggregate:reference", "traces.aggregate:alert"]},
    "dag_08_runtime_bound_three_family": {
        "scorers": ["metrics.contrast", "traces.latency_contrast", "dependencies.propagation"],
        "aggregates": ["metrics.aggregate:reference", "metrics.aggregate:alert",
                       "traces.aggregate:reference", "traces.aggregate:alert"],
        "rank_binding": "runtime.lead_count", "rank_override": 3},
}


def _validate_operator_task(task: Mapping[str, Any], source: str) -> dict[str, Any]:
    outputs = []
    semantic_failures = []
    for index, case in enumerate(task["checks"]):
        started = time.perf_counter()
        result = run_generated_operator(source, case["inputs"], case["params"], case["runtime"],
                                        task.get("output_type", "object"))
        elapsed = time.perf_counter() - started
        outputs.append({"case_index": index, "expected": case["expected"],
                        "actual": result.get("result"), "ok": result.get("ok"),
                        "error": result.get("error"), "elapsed_seconds": elapsed,
                        "sandbox_execution": result})
        if not result.get("ok"):
            error = result.get("error") or {}
            if error.get("code") == "output_type_mismatch":
                semantic_failures.append({"case_index": index, "error": error,
                                          "sandbox_execution": result})
            else:
                semantic_failures.append({"case_index": index, "error": error,
                                          "sandbox_execution": result})
            continue
        if not _operator_output_matches(result.get("result"), case["expected"],
                                        task["public_contract"]["output"]):
            semantic_failures.append({"case_index": index, "actual": result.get("result"),
                                      "expected": case["expected"],
                                      "sandbox_execution": result})
    if semantic_failures:
        raise SemanticTaskError("operator_semantic_mismatch",
                                "operator failed one or more independent held-out inputs",
                                {"failures": semantic_failures, "held_out_checks": outputs})
    if task.get("require_nonconstant") and len({digest_value(item["actual"]) for item in outputs}) < 2:
        raise SemanticTaskError("constant_operator", "operator returned the same result for every held-out input")
    return {"source": source, "source_sha256": hashlib.sha256(source.encode()).hexdigest(),
            "compile_pass": True, "independent_checks": outputs}


def _validate_diagnosis_task(task: Mapping[str, Any], value: Mapping[str, Any],
                             registry: OperatorRegistry, runtime: RuntimeContext,
                             run_dir: Path) -> dict[str, Any]:
    if set(value) != {"dag", "category"}:
        raise StaticTaskError("json_schema", "diagnosis response must have exactly dag and category keys")
    category = value["category"]
    if category is not None and (not isinstance(category, str) or category not in ALLOWED_CATEGORIES):
        raise StaticTaskError("json_schema", "category must be null or an allowed source fault category")
    if type(value["dag"]) is not dict:
        raise StaticTaskError("json_schema", "dag field must be an E4 DAG JSON object")
    try:
        verified = verify_dag(value["dag"], registry, runtime,
                              max_nodes=int(runtime.snapshot.values["max_nodes"]),
                              max_depth=int(runtime.snapshot.values["dag_depth"]))
    except DAGValidationError as exc:
        raise StaticTaskError("dag_validation", str(exc), exc.as_dict()) from exc
    telemetry = telemetry_from_spec(task["telemetry"])
    source_nodes = [node for node in verified.nodes if node.operator.name == "case.telemetry"]
    if len(source_nodes) != 1:
        raise SemanticTaskError("diagnosis_graph_input", "diagnosis DAG must contain one telemetry source")
    external = {source_nodes[0].node_id: telemetry}
    executor = SerialExecutor(registry, CacheStore(run_dir / "core_cache", enabled=False))
    started = time.perf_counter()
    try:
        result = executor.execute_document(value["dag"], runtime, lambda: external,
                                           cache_enabled=False)
    except ExecutionFailure as exc:
        raise SemanticTaskError("diagnosis_execution_failure", str(exc), exc.as_dict()) from exc
    elapsed = time.perf_counter() - started
    top = [{"role": candidate.entity.role, "component": candidate.entity.identifier,
            "score": candidate.score, "category": candidate.category}
           for candidate in result.answer.leads[:3]]
    expected = task["check"]
    expected_id = expected["expected_component"].strip().casefold()
    rank = next((index + 1 for index, item in enumerate(top)
                 if item["component"].strip().casefold() == expected_id), None)
    role_rank = next((index + 1 for index, item in enumerate(top)
                      if item["role"] == expected["expected_role"] and
                      item["component"].strip().casefold() == expected_id), None)
    ranked = rank is not None
    category_ok = category == expected["expected_category"]
    if not ranked:
        raise SemanticTaskError("diagnosis_mismatch", "executed DAG does not rank the synthetic cause in the top three",
                                {"ranked_top3": top, "ranked_expected_component": False})
    from e4.dsl.executor import node_record_to_dict
    return {"execution_elapsed_seconds": elapsed, "ranked_top3": top,
            "expected_component_rank": rank, "top1_correct": rank == 1,
            "expected_role_rank": role_rank, "role_matches": role_rank is not None,
            "top3_correct": True, "answer_category": category,
            "category_correct": category_ok, "execution_records":
            [node_record_to_dict(record) for record in result.records],
            "execution_cache_hits": result.cache_hits}


def _validate_schema_task(task: Mapping[str, Any], value: Mapping[str, Any],
                          fenced: bool) -> dict[str, Any]:
    check = task["check"]
    if check.get("require_plain_json") and fenced:
        raise StaticTaskError("json_fence", "this task requires plain JSON without a Markdown fence")
    exact = set(check["exact_keys"])
    if set(value) != exact:
        raise StaticTaskError("json_schema", "JSON object has missing or extra keys",
                              {"expected_keys": sorted(exact), "actual_keys": sorted(value)})
    expected = check["expected"]
    # Schema/type errors are repairable; well-formed but wrong literal values are semantic.
    for key, expected_value in expected.items():
        actual = value[key]
        if expected_value is None or isinstance(expected_value, (str, bool)):
            if type(actual) is not type(expected_value):
                raise StaticTaskError("json_schema", f"field {key} has the wrong JSON type")
        elif isinstance(expected_value, int) and not isinstance(expected_value, bool):
            if type(actual) is not int:
                raise StaticTaskError("json_schema", f"field {key} must be an integer")
    if not _deep_equal(value, expected):
        raise SemanticTaskError("schema_value_mismatch", "JSON has the required schema but wrong values")
    return {"schema_pass": True, "plain_json": not fenced, "parsed": value}


def telemetry_from_spec(spec: Mapping[str, Any]) -> Telemetry:
    """Expand the compact frozen synthetic telemetry recipe into immutable contracts."""
    case_id = str(spec["case_id"])
    reference = Window("reference", *spec["reference_window"])
    alert = Window("alert", *spec["alert_window"])
    windows = {"reference": reference, "alert": alert}
    metrics: list[MetricPoint] = []
    for series in spec.get("metric_series", []):
        for window_name in ("reference", "alert"):
            window = windows[window_name]
            values = series[window_name]
            interval = (window.end_ms_utc - window.start_ms_utc) // max(1, len(values))
            for index, value in enumerate(values):
                timestamp = window.start_ms_utc + index * interval
                entity = EntityRef(series.get("entity_role", "component"), series["entity"])
                metrics.append(MetricPoint(
                    table=series["table"], timestamp_ms_utc=timestamp, entity=entity,
                    metric_name=series["metric_name"], bomc_id=series.get("bomc_id"),
                    item_id=series.get("item_id"), value=float(value), raw_value=str(value),
                    row_key=f"{window_name}-{series['entity']}-{index}"))
    spans: list[Span] = []
    for group in spec.get("trace_groups", []):
        window = windows[group["window"]]
        count = int(group["count"])
        stride = (window.end_ms_utc - window.start_ms_utc) // max(1, count)
        for index in range(count):
            trace_id = f"{group['trace_prefix']}-{index:03d}"
            timestamp = window.start_ms_utc + index * stride
            for span_spec in (group["root"], group["child"]):
                parent_id = span_spec["parent_id"]
                success_pattern = span_spec.get("success_pattern")
                if success_pattern is not None:
                    success = success_pattern[index]
                elif "fail_first" in span_spec:
                    success = not (index < int(span_spec["fail_first"]))
                else:
                    success = span_spec["success"]
                spans.append(Span(
                    trace_id=trace_id, span_id=span_spec["span_id"], parent_id=parent_id,
                    start_ms_utc=timestamp, elapsed_raw=str(span_spec["elapsed"]),
                    success=success, success_status="valid", success_raw=str(success).lower(),
                    call_type=span_spec["call_type"],
                    executing_component=(EntityRef("component", span_spec["component"])
                                         if span_spec.get("component") else None),
                    logical_service=(EntityRef("service", span_spec["service"])
                                     if span_spec.get("service") else None),
                    database_target=(EntityRef("database", span_spec["database"])
                                     if span_spec.get("database") else None),
                    parent_resolved_in_clip=bool(parent_id),
                    parent_unresolved_in_clip=False))
    catalog = {identifier: {"identifier": identifier}
               for identifier in spec.get("deployment_entities", [])}
    data_hash = digest_value({"case_id": case_id, "telemetry_spec": spec,
                              "adapter_version": "m3-synthetic/1"})
    return Telemetry(case_id, reference, alert, tuple(metrics), tuple(spans), catalog,
                     data_hash, {"synthetic_spec": data_hash}, {}, {}, "m3-synthetic/1")


def composition_telemetry() -> Telemetry:
    """Fixed rich fixture for executing all eight DAG composition tasks."""
    spec = {
        "case_id": "synthetic-dag-composition",
        "reference_window": [0, 1_800_000], "alert_window": [1_800_000, 3_600_000],
        "deployment_entities": ["node_app_01", "node_db_01", "node_noise_01",
                                "node_noise_02", "node_noise_03", "node_noise_04"],
        "metric_series": [
            {"table": "metric/metric_node.csv", "entity": "node_app_01",
             "metric_name": "CPU_util_pct", "bomc_id": "node", "item_id": "cpu",
             "reference": [12, 13, 11, 12, 14, 13, 12, 11, 13, 12, 14, 13],
             "alert": [91, 93, 92, 94, 91, 92, 93, 91, 94, 92, 93, 91]},
            {"table": "metric/metric_node.csv", "entity": "node_db_01",
             "metric_name": "CPU_util_pct", "bomc_id": "node", "item_id": "cpu",
             "reference": [20, 21, 19, 20, 21, 19, 20, 21, 19, 20, 21, 19],
             "alert": [22, 23, 21, 22, 23, 21, 22, 23, 21, 22, 23, 21]},
            {"table": "metric/metric_app.csv", "entity": "svc_api",
             "entity_role": "service", "metric_name": "avg_time", "bomc_id": "metric_app",
             "item_id": "avg_time", "reference": [90, 92, 91, 89, 93, 90, 91, 92, 89, 90, 93, 91],
             "alert": [500, 520, 510, 530, 505, 515, 525, 510, 520, 505, 530, 515]},
            {"table": "metric/metric_service.csv", "entity": "node_db_01",
             "metric_name": "tnsping_result_time", "bomc_id": "service", "item_id": "tnsping",
             "reference": [5, 6, 5, 7, 6, 5, 6, 5, 7, 6, 5, 6],
             "alert": [35, 38, 34, 40, 36, 35, 39, 34, 38, 36, 35, 40]},
        ],
        "trace_groups": [
            {"window": "reference", "count": 40, "trace_prefix": "dag-ref",
             "root": {"span_id": "root", "parent_id": None, "call_type": "OSB",
                      "service": "svc_api", "component": "node_app_01", "success": True,
                      "elapsed": "100"},
             "child": {"span_id": "db", "parent_id": "root", "call_type": "JDBC",
                       "component": "node_db_01", "database": None,
                       "success": True, "elapsed": "20"}},
            {"window": "alert", "count": 40, "trace_prefix": "dag-alert",
             "root": {"span_id": "root", "parent_id": None, "call_type": "OSB",
                      "service": "svc_api", "component": "node_app_01", "success": False,
                      "elapsed": "500"},
             "child": {"span_id": "db", "parent_id": "root", "call_type": "JDBC",
                       "component": "node_db_01", "database": None,
                       "success": False, "elapsed": "320"}},
        ],
    }
    return telemetry_from_spec(spec)


def _repair_prompt(original_prompt: str, previous_content: str,
                   error: StaticTaskError) -> str:
    # Never echo checker details into repair context: some checks execute code
    # against held-out inputs, and details can contain execution artifacts.
    diagnostic = json.dumps({"code": error.code, "message": str(error)},
                            ensure_ascii=False, sort_keys=True)
    return (original_prompt + "\n\nStatic validator error (repair only this syntax/schema issue):\n" +
            diagnostic + "\n\nPrevious final response:\n" + previous_content +
            "\n\nReturn the corrected artifact in the requested format only. Do not add prose.")


def run_task(task: Mapping[str, Any], *, registry: OperatorRegistry,
             runtime: RuntimeContext, client: LocalModelClient,
             run_dir: Path, response_limit: int | None = None,
             on_call: Callable[[ModelCall], None] | None = None) -> TaskOutcome:
    start = time.perf_counter()
    task_id, kind = task["id"], task["kind"]
    base_prompt = task_prompt(task, registry, runtime)
    prompt = base_prompt
    calls: list[Mapping[str, Any]] = []
    repair_errors: list[Mapping[str, Any]] = []
    repairs = 0
    initial_passed = False
    static_passed: bool | None = None
    semantic_passed = False
    result: dict[str, Any] = {}
    error_record: Mapping[str, Any] | None = None
    input_tokens = 0
    output_tokens = 0
    while True:
        call = client.complete(prompt, task_id=task_id, attempt=len(calls) + 1,
                               max_tokens=response_limit)
        calls.append(call.as_dict())
        if on_call is not None:
            on_call(call)
        usage_input = _usage_count(call.usage, "prompt_tokens", "input_tokens")
        input_tokens += usage_input or int(call.prompt_token_count or 0)
        output_tokens += _usage_count(call.usage, "completion_tokens", "output_tokens")
        if call.error:
            error_record = dict(call.error)
            break
        finish = call.response_metadata.get("finish_reason")
        if finish in {"length", "max_tokens"}:
            static_passed = False
            error_record = {"code": "truncated_output", "message": "model hit its generation limit"}
            break
        content = call.content or ""
        try:
            if kind == "operator":
                static_passed = False
                source, _tree = validate_operator_source(content)
                static_passed = True
                checked = _validate_operator_task(task, source)
                result = checked
            else:
                parsed, fenced = parse_strict_json_object(content)
                static_passed = True
                if kind == "dag":
                    result = _validate_dag_task(task_id, parsed, registry, runtime, run_dir)
                elif kind == "diagnosis":
                    result = _validate_diagnosis_task(task, parsed, registry, runtime, run_dir)
                elif kind == "schema":
                    result = _validate_schema_task(task, parsed, fenced)
                else:
                    raise ValueError(f"unsupported task kind {kind}")
            semantic_passed = True
            if not calls[:-1]:
                initial_passed = True
            break
        except StaticTaskError as exc:
            if exc.code != "output_type":
                static_passed = False
            repair_errors.append(exc.as_dict())
            if repairs >= int(REPAIR_LIMIT):
                error_record = exc.as_dict()
                break
            repairs += 1
            prompt = _repair_prompt(base_prompt, content, exc)
        except SemanticTaskError as exc:
            static_passed = True
            error_record = exc.as_dict()
            break
        except Exception as exc:
            error_record = {"code": "task_checker_error",
                            "message": f"{type(exc).__name__}: {exc}"}
            break
    passed = semantic_passed and error_record is None
    return TaskOutcome(task_id, kind, passed, initial_passed, repairs,
                       static_passed, semantic_passed, result, tuple(calls),
                       time.perf_counter() - start, input_tokens, output_tokens,
                       tuple(repair_errors), error_record)


def _usage_count(usage: Mapping[str, Any], primary: str, alternate: str) -> int:
    value = usage.get(primary, usage.get(alternate, 0))
    return int(value) if type(value) in (int, float) and value >= 0 else 0


def run_suite(suite_path: Path, run_dir: Path, profile: Mapping[str, Any], *,
              client: LocalModelClient | None = None,
              task_ids: set[str] | None = None) -> dict[str, Any]:
    suite = json.loads(suite_path.read_text())
    all_tasks = validate_suite(suite)
    tasks = [task for task in all_tasks if task_ids is None or task["id"] in task_ids]
    if task_ids is not None and {task["id"] for task in tasks} != task_ids:
        raise ValueError("requested task filter contains unknown task ids")
    run_dir.mkdir(parents=True, exist_ok=True)
    runtime = runtime_context()
    from e4.operators.telecom.operators import telecom_registry
    registry = telecom_registry()
    model_client = client or LocalModelClient(profile, base_url=os.environ.get("E4_BASE_URL"))
    suite_hash = sha256_file(suite_path)
    check_hash, checker_files = checker_sha256()
    call_path = run_dir / "m3_model_calls.jsonl"
    task_path = run_dir / "m3_task_results.jsonl"
    manifest_path = run_dir / "m3_benchmark_manifest.json"
    summary_path = run_dir / "m3_benchmark_summary.json"
    for output_path in (call_path, task_path, manifest_path, summary_path):
        if output_path.exists():
            raise FileExistsError(f"refusing to overwrite existing benchmark artifact: {output_path}")
    manifest = {
        "suite_id": suite.get("suite_id"), "suite_path": str(suite_path.resolve()),
        "suite_sha256": suite_hash, "checker_sha256": check_hash,
        "checker_files": checker_files,
        "task_ids": [task["id"] for task in tasks], "task_count": len(tasks),
        "task_distribution": dict(Counter(task["kind"] for task in tasks)),
        "repair_limit": int(REPAIR_LIMIT), "normalization_policy": OUTPUT_NORMALIZATION_POLICY,
        "model_alias": profile["model_alias"], "profile": dict(profile),
        "base_url": model_client.base_url,
        "context_tokens": model_client.context_tokens,
        "max_generation_tokens": model_client.max_generation_tokens,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "calls_file": str(call_path), "task_results_file": str(task_path),
        "launcher_record": str(run_dir / "launch.json"),
        "gpu_memory_record": str(run_dir / "gpu-memory.json"),
    }
    with manifest_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    outcomes = []
    with call_path.open("x", encoding="utf-8") as call_stream, \
            task_path.open("x", encoding="utf-8") as task_stream:
        def save_call(call: ModelCall) -> None:
            row = {"record_type": "model_call", **call.as_dict()}
            call_stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"),
                                         allow_nan=False) + "\n")
            call_stream.flush()
        for task in tasks:
            outcome = run_task(task, registry=registry, runtime=runtime,
                               client=model_client, run_dir=run_dir, on_call=save_call)
            outcomes.append(outcome)
            task_stream.write(json.dumps(outcome.as_dict(), ensure_ascii=False,
                                         separators=(",", ":"), allow_nan=False) + "\n")
            task_stream.flush()
    summary = summarize_outcomes(outcomes)
    summary.update({"suite_id": suite.get("suite_id"), "suite_sha256": suite_hash,
                    "checker_sha256": check_hash, "finished_utc": datetime.now(timezone.utc).isoformat(),
                    "calls_file": str(call_path)})
    with summary_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return summary


def run_calibration(run_dir: Path, profile: Mapping[str, Any], *,
                    client: LocalModelClient | None = None) -> dict[str, Any]:
    """One separate full-library prompt call; not part of the frozen 20 tasks."""
    run_dir.mkdir(parents=True, exist_ok=True)
    runtime = runtime_context()
    from e4.operators.telecom.operators import telecom_registry
    registry = telecom_registry()
    model_client = client or LocalModelClient(profile, base_url=os.environ.get("E4_BASE_URL"))
    path = run_dir / "m3_prompt_context_calibration.json"
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing calibration artifact: {path}")
    prompt = build_calibration_prompt(registry, runtime)
    call = model_client.complete(prompt, task_id="m3-full-library-calibration", attempt=1)
    checker_hash, checker_files = checker_sha256()
    record = {
        "record_type": "m3-prompt-context-calibration/1",
        "model_alias": profile["model_alias"],
        "profile": dict(profile),
        "suite_sha256": sha256_file(DEFAULT_SUITE),
        "checker_sha256": checker_hash,
        "checker_files": checker_files,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "prompt_characters": len(prompt),
        "model_call": call.as_dict(),
        "context_tokens": model_client.context_tokens,
        "generation_tokens": model_client.max_generation_tokens,
        "within_context_budget": (call.prompt_token_count is not None and
                                  call.prompt_token_count + model_client.max_generation_tokens
                                  <= model_client.context_tokens),
        "gpu_memory_record": str(run_dir / "gpu-memory.json"),
        "finished_utc": datetime.now(timezone.utc).isoformat(),
    }
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True,
                                allow_nan=False) + "\n")
    return record


def summarize_outcomes(outcomes: Sequence[TaskOutcome]) -> dict[str, Any]:
    categories: dict[str, dict[str, Any]] = {}
    for outcome in outcomes:
        entry = categories.setdefault(outcome.kind, {"tasks": 0, "initial_correct": 0,
            "correct_after_static_repair": 0, "final_correct": 0, "repairs": 0,
            "compile_pass": 0, "semantic_pass": 0, "input_tokens": 0,
            "output_tokens": 0, "elapsed_seconds": 0.0,
            "primary_top3_correct": 0, "secondary_top1_correct": 0,
            "category_correct": 0, "category_scored": 0})
        entry["tasks"] += 1
        entry["initial_correct"] += int(outcome.initial_passed)
        entry["correct_after_static_repair"] += int(outcome.passed and outcome.repairs > 0)
        entry["final_correct"] += int(outcome.passed)
        entry["repairs"] += outcome.repairs
        entry["compile_pass"] += int(outcome.static_passed is True and outcome.kind == "operator")
        entry["semantic_pass"] += int(outcome.semantic_passed)
        entry["input_tokens"] += outcome.input_tokens
        entry["output_tokens"] += outcome.output_tokens
        entry["elapsed_seconds"] += outcome.elapsed_seconds
        if outcome.kind == "diagnosis":
            entry["primary_top3_correct"] += int(bool(outcome.result.get("top3_correct")))
            entry["secondary_top1_correct"] += int(bool(outcome.result.get("top1_correct")))
            if isinstance(outcome.result.get("category_correct"), bool):
                entry["category_scored"] += 1
                entry["category_correct"] += int(outcome.result["category_correct"])
    diagnoses = [outcome for outcome in outcomes if outcome.kind == "diagnosis"]
    diagnosis_results = [outcome.result for outcome in diagnoses if outcome.semantic_passed]
    category_count = sum(isinstance(result.get("category_correct"), bool) for result in diagnosis_results)
    total = len(outcomes)
    return {"task_count": total, "initial_correct_count": sum(x.initial_passed for x in outcomes),
            "correct_after_static_repair_count": sum(x.passed and x.repairs > 0 for x in outcomes),
            "final_correct_count": sum(x.passed for x in outcomes),
            "compile_pass_count": sum(x.static_passed is True and x.kind == "operator" for x in outcomes),
            "semantic_pass_count": sum(x.semantic_passed for x in outcomes),
            "repair_count": sum(x.repairs for x in outcomes),
            "input_tokens": sum(x.input_tokens for x in outcomes),
            "output_tokens": sum(x.output_tokens for x in outcomes),
            "elapsed_seconds": sum(x.elapsed_seconds for x in outcomes),
            "diagnosis_task_count": len(diagnoses),
            "diagnosis_primary_top3_correct": sum(bool(r.get("top3_correct")) for r in diagnosis_results),
            "diagnosis_secondary_top1_correct": sum(bool(r.get("top1_correct")) for r in diagnosis_results),
            "diagnosis_category_correct": sum(bool(r.get("category_correct")) for r in diagnosis_results),
            "diagnosis_category_scored_count": category_count,
            "per_category": categories}
