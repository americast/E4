"""Closed, typed, source-free stage-3 operator synthesis profile."""
from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal, Mapping, cast

from .cache import canonical_json
from .m4_operator_ir_worker import ENGINE_ABI, IR_SCHEMA, KERNELS, PROFILE
from .m4_operator_ir_v2_worker import (ENGINE_ABI as ENGINE_ABI_V2,
    IR_SCHEMA as IR_SCHEMA_V2, KERNELS as KERNELS_V2, PROFILE as PROFILE_V2,
    BASE_WORKER_SHA256 as V2_BASE_WORKER_SHA256)
from .m4_capabilities import (ComputationAvailability, ComputationWindowSupport,
                              OperatorIRCapabilityReport)

ROOT = Path(__file__).resolve().parents[1]
_PORTS: list[dict[str, Any]] = [
    {"name": "reference", "type": "Aggregate", "required": True, "variadic": False,
     "minimum_items": 1, "maximum_items": None},
    {"name": "alert", "type": "Aggregate", "required": True, "variadic": False,
     "minimum_items": 1, "maximum_items": None},
    {"name": "candidates", "type": "CandidateSet", "required": True, "variadic": False,
     "minimum_items": 1, "maximum_items": None},
]
_REQUIREMENTS = {
    "reference": {"source_columns": {"metric/metric_app.csv": ["timestamp", "mrt", "tc"],
        "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"]},
        "statistics": ["median"], "group_by": [],
        "support_unit": "timestamp", "coverage": "full_window"},
    "alert": {"source_columns": {"metric/metric_app.csv": ["timestamp", "mrt", "tc"],
        "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"]},
        "statistics": ["median"], "group_by": [],
        "support_unit": "timestamp", "coverage": "full_window"},
    "candidates": {"source_columns": {}, "statistics": [], "group_by": ["component"],
        "support_unit": None, "coverage": "full_window"},
}


class OperatorIRError(ValueError):
    pass


def _sha(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _canonical_program(program: Mapping[str, Any]) -> dict[str, Any]:
    """Discard node labels while preserving ordered typed computation."""
    ids = {row["id"]: f"n{index}" for index, row in enumerate(program["nodes"])}
    nodes = []
    for row in program["nodes"]:
        nodes.append({**row, "id": ids[row["id"]],
            "inputs": {port: (f"$input.{ref[7:]}" if ref.startswith("$input.") else ids[ref])
                for port, ref in row["inputs"].items()}})
    return {"schema_version": program["schema_version"], "nodes": nodes,
        "return": {"node": ids[program["return"]["node"]]}}


def _policy(root: Path = ROOT) -> dict[str, Any]:
    metric_path = root / "config/bank_metric_kind_policy.json"
    kpi_path = root / "config/bank_dev_kpi_policy.json"
    metric = json.loads(metric_path.read_text(encoding="utf-8"))
    kpi = json.loads(kpi_path.read_text(encoding="utf-8"))
    allow = sorted(metric.get("source_semantics", {}).get("metric_container", {})
                   .get("ranking_allowlist", {}).keys())
    if metric.get("schema_version") != "e4-bank-metric-kind-policy/1" or not allow:
        raise OperatorIRError("trusted metric ranking policy is unavailable")
    if kpi.get("schema_version") != "e4-bank-dev-kpi-policy/1":
        raise OperatorIRError("trusted primary KPI policy is unavailable")
    return {"metric": metric, "kpi": kpi, "component_allowlist": allow,
        "component_policy_sha256": hashlib.sha256(metric_path.read_bytes()).hexdigest(),
        "kpi_policy_sha256": hashlib.sha256(kpi_path.read_bytes()).hexdigest()}


def _types_for(program: Mapping[str, Any], policy: Mapping[str, Any], *,
        schema_version: str = IR_SCHEMA,
        kernels_by_identity: Mapping[str, Mapping[str, Any]] = KERNELS,
        external_input_types: Mapping[str, str] | None = None) -> dict[str, str]:
    if not isinstance(program, Mapping) or set(program) != {"schema_version", "nodes", "return"} or program.get("schema_version") != schema_version:
        raise OperatorIRError(f"program must use the exact {schema_version} schema")
    external_input_types = external_input_types or {
        "reference": "Aggregate", "alert": "Aggregate", "candidates": "CandidateSet"}
    nodes = program.get("nodes")
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 40:
        raise OperatorIRError("program must contain between 1 and 40 nodes")
    types: dict[str, str] = {}
    incoming: dict[str, set[str]] = {}
    depth: dict[str, int] = {}
    kernels: list[str] = []
    evidence_sources: dict[str, str] = {}
    for row in nodes:
        if not isinstance(row, Mapping) or set(row) != {"id", "kernel", "version", "inputs", "params", "bindings"}:
            raise OperatorIRError("each program node requires exact id/kernel/version/inputs/params/bindings fields")
        node_id, kernel = row["id"], row["kernel"]
        if not isinstance(node_id, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", node_id) or node_id in types:
            raise OperatorIRError("node ids must be unique lowercase identifiers")
        identity = f"{kernel}@{row['version']}"
        definition = kernels_by_identity.get(identity)
        if definition is None:
            raise OperatorIRError(f"kernel {identity!r} is not in the private fixed catalog")
        refs = row["inputs"]
        if not isinstance(refs, Mapping) or set(refs) != set(definition["inputs"]):
            expected_ports = sorted(definition["inputs"])
            received_ports = sorted(refs) if isinstance(refs, Mapping) else None
            raise OperatorIRError(
                f"{node_id} input ports must be exactly {expected_ports}; received {received_ports}")
        parents = set()
        for port, ref in refs.items():
            expected = definition["inputs"][port]
            if isinstance(ref, str) and ref.startswith("$input."):
                actual = external_input_types.get(ref[7:])
                if actual is None:
                    raise OperatorIRError(f"{node_id}.{port} references an undeclared operator input")
            elif isinstance(ref, str) and ref in types:
                actual = types[ref]
                parents.add(ref)
            else:
                raise OperatorIRError(
                    f"{node_id}.{port} must reference a prior node id or declared external input "
                    f"as $input.<name> (valid forms include $input.reference, $input.alert, "
                    f"$input.candidates, and conditional $input.starter_input; available external inputs: "
                    f"{sorted(external_input_types)})")
            if actual != expected:
                raise OperatorIRError(f"{node_id}.{port} expects {expected}, received {actual}")
        params, bindings = row["params"], row["bindings"]
        required = set(definition["params"])
        if not isinstance(params, Mapping) or not isinstance(bindings, Mapping) or set(params) & set(bindings) or set(params) | set(bindings) != required:
            raise OperatorIRError(
                f"{node_id} must provide exactly kernel parameters {sorted(required)} once; "
                f"put ordinary values in params and runtime-bound names in bindings "
                f"(received params={sorted(params) if isinstance(params, Mapping) else None}, "
                f"bindings={sorted(bindings) if isinstance(bindings, Mapping) else None})")
        for name, descriptor in definition["params"].items():
            if name in bindings:
                if bindings[name] != f"runtime.{descriptor.get('runtime_field')}" or not descriptor.get("runtime_field"):
                    expected = (f"bindings.{name}=runtime.{descriptor['runtime_field']}"
                        if descriptor.get("runtime_field") else "an ordinary params value")
                    raise OperatorIRError(f"{node_id}.{name} requires {expected}")
            elif descriptor.get("runtime_field"):
                raise OperatorIRError(
                    f"{node_id}.{name} must be omitted from params and set as "
                    f"bindings.{name}=runtime.{descriptor['runtime_field']}")
            else:
                value = params[name]
                if descriptor["type"] == "string": valid = isinstance(value, str)
                elif descriptor["type"] == "positive_integer": valid = type(value) is int and value > 0
                else: valid = type(value) in (int, float)
                if not valid or (descriptor.get("choices") and value not in descriptor["choices"]):
                    raise OperatorIRError(f"{node_id}.{name} violates its kernel parameter type")
                if descriptor.get("choices") and value not in descriptor["choices"]:
                    raise OperatorIRError(f"{node_id}.{name} is outside its kernel choices")
                if name == "source_field":
                    if value not in policy["component_allowlist"]:
                        raise OperatorIRError("source_field is outside the current trusted component-metric allowlist")
                if name == "selector_ref" and value not in policy.get("selector_refs", set()):
                    raise OperatorIRError("selector_ref is outside the trusted stable metric selector catalog")
                if name == "selector_ref" and value not in policy.get("component_selector_refs", set()):
                    raise OperatorIRError("series.select_metric_rule requires a component-metric selector reference")
        types[node_id] = definition["output"]
        incoming[node_id] = parents
        depth[node_id] = 1 + max((depth[parent] for parent in parents), default=0)
        if depth[node_id] > 12:
            raise OperatorIRError("typed operator IR exceeds the protected depth limit of 12")
        kernels.append(kernel)
        if kernel == "evidence.from_scores":
            evidence_sources[node_id] = refs["scores"]
        elif kernel == "scores.assemble" and evidence_sources.get(refs["evidence"]) != refs["scores"]:
            raise OperatorIRError("scores.assemble evidence must come from the exact ComponentScores node it consumes")
    returned = program.get("return")
    if not isinstance(returned, Mapping) or set(returned) != {"node"} or returned.get("node") not in types or types[returned["node"]] != "ScoredCandidates":
        raise OperatorIRError("program return must point to ScoredCandidates")
    # Every value flows through typed input and output contracts. Kernel motifs
    # are not treated as proof of semantic novelty; the body digest is.
    reachable: set[str] = set()
    todo = [returned["node"]]
    while todo:
        node = todo.pop()
        if node not in reachable:
            reachable.add(node)
            todo.extend(incoming[node])
    if len(reachable) != len(nodes):
        raise OperatorIRError("all program nodes must contribute to the returned analysis")
    if schema_version == IR_SCHEMA:
        required = {"series.component_metrics", "numbers.reduce_component",
            "evidence.from_scores", "scores.assemble"}
        has_statistic = bool({"series.reduce", "series.correlate"} & set(kernels))
    else:
        required = {"numbers.reduce_component", "evidence.from_scores", "scores.assemble"}
        has_metric_source = "series.component_metrics" in kernels
        has_log_source = "logs.pattern_counts" in kernels
        if not has_metric_source and not has_log_source:
            raise OperatorIRError("V2 analysis must consume a supported metric or normalized log input")
        has_statistic = bool({"series.reduce", "series.correlate", "logs.exposure_rate",
            "series.normalize_reference_scale", "series.reference_quantile_exceedance",
            "series.sigma_exceedance", "series.window_association"} & set(kernels))
    if not required.issubset(kernels) or not has_statistic:
        raise OperatorIRError("analysis must compute a supported statistic, reduce by component, and assemble trusted evidence")
    return types


def normalize_operator_ir_spec(spec: Mapping[str, Any], *, root: Path = ROOT) -> dict[str, Any]:
    if isinstance(spec, Mapping) and spec.get("authoring_profile") == PROFILE_V2:
        return _normalize_operator_ir_v2_spec(spec, root=root)
    compact_fields = {"name", "version", "description", "family", "output_type",
        "parameters", "authoring_profile", "program"}
    legacy_fields = compact_fields | {"input_ports", "input_requirements"}
    if not isinstance(spec, Mapping) or set(spec) not in (compact_fields, legacy_fields):
        raise OperatorIRError("typed operator spec fields are not exact")
    name, version, description, family = (spec.get(key) for key in ("name", "version", "description", "family"))
    if not isinstance(name, str) or not re.fullmatch(r"evolved\.[a-z][a-z0-9_]{0,62}", name):
        raise OperatorIRError("operator name must use evolved.<lowercase_name>")
    if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", version):
        raise OperatorIRError("operator version is invalid")
    if not isinstance(description, str) or not description.strip() or len(description) > 1024:
        raise OperatorIRError("operator description must contain 1 to 1024 characters")
    if not isinstance(family, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", family):
        raise OperatorIRError("operator family is invalid")
    if spec.get("authoring_profile") != PROFILE or spec.get("output_type") != "ScoredCandidates" or spec.get("parameters") != {}:
        raise OperatorIRError("new operators require typed-operator-ir-v1, no outer parameters, and ScoredCandidates output")
    requirements: dict[str, dict[str, Any]]
    if set(spec) == legacy_fields:
        raw_ports = spec.get("input_ports")
        if not isinstance(raw_ports, list) or len(raw_ports) != 3 or any(
                not isinstance(row, Mapping) or set(row) - {"name", "type", "required", "variadic", "minimum_items", "maximum_items"}
                for row in raw_ports):
            raise OperatorIRError("typed operator ports are fixed to reference Aggregate, alert Aggregate, candidates CandidateSet")
        if [(row.get("name"), row.get("type")) for row in raw_ports] != [
                (row["name"], row["type"]) for row in _PORTS]:
            raise OperatorIRError("typed operator ports are fixed to reference Aggregate, alert Aggregate, candidates CandidateSet")
        from .m4_capabilities import validate_input_requirements
        try:
            requirements = validate_input_requirements(spec.get("input_requirements"),
                cast(list[Mapping[str, Any]], _PORTS))
        except Exception as exc:
            raise OperatorIRError(f"typed operator input requirements are invalid: {exc}") from exc
        if requirements != _REQUIREMENTS:
            raise OperatorIRError("typed operator requirements must retain full-window metric samples and component candidate provenance")
    else:
        requirements = cast(dict[str, dict[str, Any]], _REQUIREMENTS)
    program = spec.get("program")
    if not isinstance(program, Mapping):
        raise OperatorIRError("program must be a typed operator IR object")
    policy = _policy(root)
    _types_for(program, policy)
    program = _canonical_program(program)
    # Program blobs are stored as ordinary JSON, unlike core cache values which
    # encode mappings through a typed $map envelope. Bind the identical on-disk
    # representation used by the library snapshot verifier.
    program_sha = hashlib.sha256(json.dumps(program, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
    kernel_sha = _sha({"abi": ENGINE_ABI, "kernels": KERNELS})
    interpreter_sha = hashlib.sha256(Path(__file__).with_name("m4_operator_ir_worker.py").read_bytes()).hexdigest()
    sig = {"name": name, "declared_version": version, "description": description.strip(),
        "family": family, "input_ports": _PORTS, "output_type": "ScoredCandidates",
        "parameters": {}, "authoring_profile": PROFILE, "input_requirements": requirements,
        "program_sha256": program_sha,
        "kernel_abi_sha256": kernel_sha, "interpreter_sha256": interpreter_sha,
        "policy_pins": {
            "component_metric_policy_sha256": policy["component_policy_sha256"],
            "primary_kpi_policy_sha256": policy["kpi_policy_sha256"]}}
    sig["signature_sha256"] = _sha(sig)
    identity = _sha({"signature": sig, "program_sha256": program_sha,
        "kernel_abi_sha256": kernel_sha, "interpreter_sha256": interpreter_sha,
        "policy_pins": sig["policy_pins"]})
    return {**sig, "version": f"{version}+{identity[:16]}", "program": program,
        "program_sha256": program_sha, "source_sha256": program_sha,
        "contract_hash": identity}


def trusted_runtime_context(root: Path = ROOT) -> dict[str, Any]:
    policy = _policy(root)
    return {"component_metric_allowlist": policy["component_allowlist"],
        "primary_metric_allowlist": policy["kpi"].get("primary_metric_allowlist", []),
        "primary_kpi_minimum_support": int(policy["kpi"]["minimum_support_per_window"]),
        "primary_kpi_policy_sha256": policy["kpi_policy_sha256"],
        "component_metric_policy_sha256": policy["component_policy_sha256"]}


def _v2_policy(root: Path = ROOT) -> dict[str, Any]:
    base = _policy(root)
    from .bank_metric_policy import load_bank_metric_policy
    metric_policy = load_bank_metric_policy(root / "config/bank_metric_kind_policy.json")
    path = root / "config/typed_operator_ir_v2_policy.json"
    try:
        v2 = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OperatorIRError(f"typed-operator-ir-v2 policy is unavailable: {exc}") from exc
    if (v2.get("schema_version") != "e4-typed-operator-ir-v2-policy/1" or
            v2.get("authoring_profile") != PROFILE_V2 or
            v2.get("ir_schema_version") != IR_SCHEMA_V2 or
            v2.get("engine_abi") != ENGINE_ABI_V2):
        raise OperatorIRError("typed-operator-ir-v2 policy version pins are invalid")
    selector_catalog_by_table = {table: [row.to_dict()
        for row in metric_policy.selector_catalog(table)] for table in (
            "bank/metric_container.csv", "bank/metric_app.csv", "bank/trace_span.csv",
            "bank/log/log_service.csv")}
    selector_catalog = [row for rows in selector_catalog_by_table.values() for row in rows]
    return {**base, "v2": v2, "v2_policy_path": path,
        "v2_policy_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "bank_metric_policy_sha256": metric_policy.sha256,
        "metric_policy": metric_policy,
        "selector_catalog": selector_catalog,
        "selector_refs": {row["selector_ref"] for row in selector_catalog},
        "component_selector_refs": {row["selector_ref"] for row in
            selector_catalog_by_table["bank/metric_container.csv"]},
        "selector_catalog_by_table": selector_catalog_by_table,
        "v2_worker_sha256": hashlib.sha256(
            Path(__file__).with_name("m4_operator_ir_v2_worker.py").read_bytes()).hexdigest(),
        "v2_base_worker_sha256": V2_BASE_WORKER_SHA256}


_PORTS_V2 = [dict(row) for row in _PORTS]
_STARTER_INPUT_PORT_V2 = {"name": "starter_input", "type": "PaperStarterInput",
    "required": False, "variadic": False, "minimum_items": 1, "maximum_items": None}


def _ports_for_v2_program(program: Mapping[str, Any]) -> list[dict[str, Any]]:
    kernels = {str(row.get("kernel", "")) for row in program.get("nodes", [])}
    has_log_kernel = any(kernel.startswith("logs.") for kernel in kernels)
    uses_metric_inputs = bool(kernels & {"series.component_metrics", "series.primary_kpi",
        "series.observed_kpi", "series.align_kpi", "series.align_kpi_both_windows",
        "series.correlate", "series.window_association", "series.reduce"})
    ports = [dict(row) for row in _PORTS_V2]
    if not uses_metric_inputs:
        for row in ports:
            if row["name"] in {"reference", "alert"}:
                row["required"] = False
    ports.append({**_STARTER_INPUT_PORT_V2, "required": has_log_kernel})
    return ports


def _requirements_for_v2_program(program: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    kernels = {str(row["kernel"]) for row in program["nodes"]}
    uses_components = "series.component_metrics" in kernels
    uses_kpi = bool(kernels & {"series.primary_kpi", "series.observed_kpi",
        "series.align_kpi", "series.align_kpi_both_windows", "series.correlate",
        "series.window_association"})
    reference_columns: dict[str, list[str]] = {}
    alert_columns: dict[str, list[str]] = {}
    reference_groups_by_table: dict[str, list[str]] = {}
    alert_groups_by_table: dict[str, list[str]] = {}
    if uses_components:
        table = "metric/metric_container.csv"
        reference_columns[table] = ["timestamp", "cmdb_id", "kpi_name", "value"]
        alert_columns[table] = ["timestamp", "cmdb_id", "kpi_name", "value"]
        if uses_kpi:
            reference_groups_by_table[table] = ["component"]
            alert_groups_by_table[table] = ["component"]
    if uses_kpi:
        table = "metric/metric_app.csv"
        reference_columns[table] = ["timestamp", "tc", "mrt"]
        alert_columns[table] = ["timestamp", "tc", "mrt"]
        reference_groups_by_table[table] = ["app_kpi"]
        alert_groups_by_table[table] = ["app_kpi"]
    uses_logs = any(kernel.startswith("logs.") for kernel in kernels)
    def metric_requirements(source_columns: dict[str, list[str]], *, window_is_used: bool,
                            groups_by_table: dict[str, list[str]]) -> dict[str, Any]:
        requirement = {"source_columns": source_columns,
            "statistics": ["median"] if uses_components or uses_kpi else [],
            # Preserve the legacy flat contract for component-only V2 programs.
            # Mixed component/KPI programs carry exact per-table groupings.
            "group_by": ["component"] if uses_components and not uses_kpi else [],
            "support_unit": "timestamp" if uses_components or uses_kpi else None,
            "coverage": "full_window" if window_is_used else "any"}
        if groups_by_table:
            requirement["group_by_by_table"] = groups_by_table
        return requirement
    return {
        "reference": metric_requirements(reference_columns,
            window_is_used=uses_components or uses_kpi, groups_by_table=reference_groups_by_table),
        "alert": metric_requirements(alert_columns,
            window_is_used=uses_components or uses_kpi, groups_by_table=alert_groups_by_table),
        "candidates": {"source_columns": {}, "statistics": [],
            "group_by": ["component"], "support_unit": None, "coverage": "full_window"},
        "starter_input": ({"source_columns": {"log/log_service.csv": [
                "log_id", "timestamp", "cmdb_id", "log_name", "value"]},
            "statistics": ["normalized_log_pattern_count", "window_log_denominator"],
            "group_by": ["component", "log_name", "window"],
            "support_unit": "independent_time_block", "coverage": "full_window"}
            if uses_logs else {"source_columns": {}, "statistics": [], "group_by": [],
                "support_unit": None, "coverage": "any"})}


def _normalize_operator_ir_v2_spec(spec: Mapping[str, Any], *, root: Path) -> dict[str, Any]:
    compact_fields = {"name", "version", "description", "family", "output_type",
        "parameters", "authoring_profile", "program"}
    legacy_fields = compact_fields | {"input_ports", "input_requirements"}
    if not isinstance(spec, Mapping) or set(spec) not in (compact_fields, legacy_fields):
        raise OperatorIRError("typed-operator-ir-v2 spec fields are not exact")
    name, version, description, family = (spec.get(key) for key in
        ("name", "version", "description", "family"))
    if not isinstance(name, str) or not re.fullmatch(r"evolved\.[a-z][a-z0-9_]{0,62}", name):
        raise OperatorIRError("operator name must use evolved.<lowercase_identifier>")
    if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", version):
        raise OperatorIRError("operator version is invalid")
    if not isinstance(description, str) or not description.strip() or len(description) > 1024:
        raise OperatorIRError("operator description must contain 1 to 1024 characters")
    if not isinstance(family, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", family):
        raise OperatorIRError("operator family is invalid")
    if (spec.get("authoring_profile") != PROFILE_V2 or
            spec.get("output_type") != "ScoredCandidates" or spec.get("parameters") != {}):
        raise OperatorIRError("V2 operators require typed-operator-ir-v2, no outer params, and ScoredCandidates output")
    program = spec.get("program")
    if not isinstance(program, Mapping):
        raise OperatorIRError("V2 program must be a typed operator IR object")
    policy = _v2_policy(root)
    ports = _ports_for_v2_program(program)
    _types_for(program, policy, schema_version=IR_SCHEMA_V2,
        kernels_by_identity=KERNELS_V2,
        external_input_types={"reference": "Aggregate", "alert": "Aggregate",
            "candidates": "CandidateSet", "starter_input": "OperatorIRLogInput"})
    requirements = _requirements_for_v2_program(program)
    from .m4_capabilities import validate_input_requirements
    if set(spec) == legacy_fields:
        raw_ports = spec.get("input_ports")
        allowed_port_fields = {"name", "type", "required", "variadic", "minimum_items", "maximum_items"}
        if (not isinstance(raw_ports, list) or len(raw_ports) != len(ports) or
                any(not isinstance(row, Mapping) or set(row) - allowed_port_fields
                    for row in raw_ports) or
                [(row.get("name"), row.get("type")) for row in raw_ports] != [
                    (row["name"], row["type"]) for row in ports]):
            raise OperatorIRError("V2 operator inputs are fixed to reference, alert, and candidates")
        try:
            declared = validate_input_requirements(spec.get("input_requirements"),
                cast(list[Mapping[str, Any]], ports), allow_table_specific=True)
        except Exception as exc:
            raise OperatorIRError(f"V2 dynamic input requirements are invalid: {exc}") from exc
        if declared != requirements:
            raise OperatorIRError("V2 input requirements do not match the reachable kernel set")
    canonical_program = _canonical_program(program)
    program_sha = hashlib.sha256(json.dumps(canonical_program, ensure_ascii=False,
        sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
    interpreter_sha = policy["v2_worker_sha256"]
    bundle_sha = _sha({"abi": ENGINE_ABI_V2, "kernels": KERNELS_V2,
        "base_worker_sha256": policy["v2_base_worker_sha256"]})
    pins = {"component_metric_policy_sha256": policy["component_policy_sha256"],
        "bank_metric_policy_sha256": policy["bank_metric_policy_sha256"],
        "primary_kpi_policy_sha256": policy["kpi_policy_sha256"],
        "typed_operator_ir_v2_policy_sha256": policy["v2_policy_sha256"],
        "v1_base_worker_sha256": policy["v2_base_worker_sha256"]}
    sig = {"name": name, "declared_version": version, "description": description.strip(),
        "family": family, "input_ports": ports, "output_type": "ScoredCandidates",
        "parameters": {}, "authoring_profile": PROFILE_V2,
        "input_requirements": requirements, "program_sha256": program_sha,
        "kernel_abi_sha256": bundle_sha, "interpreter_sha256": interpreter_sha,
        "policy_pins": pins}
    sig["signature_sha256"] = _sha(sig)
    identity = _sha({"signature": sig, "program_sha256": program_sha,
        "kernel_abi_sha256": bundle_sha, "interpreter_sha256": interpreter_sha,
        "policy_pins": pins})
    return {**sig, "version": f"{version}+{identity[:16]}",
        "program": canonical_program, "source_sha256": program_sha,
        "contract_hash": identity}


def operator_ir_capability_report(starter_input: Any, registry: Any, *,
        runtime_values: Mapping[str, Any], root: Path = ROOT) -> dict[str, Any]:
    """Report observed IR data/support without asserting ranking or diagnosis.

    This pure host-side provider reads typed PaperStarterInput data and trusted
    policy/runtime values. It performs no I/O, evaluator access, or model call.
    In particular, univariate metric requirements are derived without requiring
    an app KPI.
    """
    from .contracts import PaperStarterInput
    if not isinstance(starter_input, PaperStarterInput):
        raise OperatorIRError("capability provider requires trusted PaperStarterInput")
    if not isinstance(runtime_values, Mapping):
        raise OperatorIRError("runtime_values must be a resolved runtime mapping")
    if not callable(getattr(registry, "signatures", None)):
        raise OperatorIRError("capability provider requires an OperatorRegistry")
    policy = _policy(root)
    telemetry = starter_input.telemetry
    from .bank_metric_policy import load_bank_metric_policy
    metric_policy = load_bank_metric_policy(root / "config/bank_metric_kind_policy.json")
    kpi_policy = policy["kpi"]
    min_kpi = int(kpi_policy["minimum_support_per_window"])
    app_allow = set(kpi_policy.get("primary_metric_allowlist", ()))
    minimum_pairs = runtime_values.get("minimum_support")
    if type(minimum_pairs) is not int or minimum_pairs <= 0:
        minimum_pairs = None
    minimum_blocks = runtime_values.get("minimum_independent_blocks")
    if type(minimum_blocks) is not int or minimum_blocks <= 0:
        minimum_blocks = None
    block_ms = starter_input.analysis_block_duration_ms

    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    # Collapse duplicate source records to their median at each exact timestamp,
    # matching aggregate semantics without pretending duplicate rows add support.
    grouped: dict[tuple[str, str, str], dict[str, dict[int, list[float]]]] = {}
    observed_field_names: dict[str, set[str]] = {}
    support_by_source_field: dict[tuple[str, str], dict[str, set[int]]] = {}
    for point in telemetry.metrics:
        if point.value is None or not math.isfinite(float(point.value)):
            continue
        window_name = next((name for name, window in windows.items()
            if window.contains(point.timestamp_ms_utc)), None)
        if window_name is None:
            continue
        observed_field_names.setdefault(point.table, set()).add(point.metric_name)
        support_by_source_field.setdefault((point.table, point.metric_name),
            {"reference": set(), "alert": set()})[window_name].add(point.timestamp_ms_utc)
        grouped.setdefault((point.table, point.metric_name, point.entity.identifier),
            {"reference": {}, "alert": {}})[window_name].setdefault(
                point.timestamp_ms_utc, []).append(float(point.value))
    samples: dict[tuple[str, str, str], dict[str, dict[int, float]]] = {
        key: {window: {timestamp: statistics.median(values)
            for timestamp, values in by_timestamp.items()}
        for window, by_timestamp in by_window.items()}
        for key, by_window in grouped.items()}

    def support_window(window_name: Literal["reference", "alert"], values: Mapping[int, float], *,
                       minimum: int | None, paired: int | None = None,
                       block_requirement: int | None = None) -> ComputationWindowSupport:
        window = windows[window_name]
        blocks = {(timestamp - window.start_ms_utc) // block_ms for timestamp in values}
        constant = len(set(values.values())) <= 1 if values else None
        computable = len(values) > 0
        if block_requirement is not None:
            computable = computable and len(blocks) >= block_requirement
        reasons = []
        if not values:
            reasons.append("no_finite_observations")
        elif minimum is not None and len(values) < minimum:
            reasons.append("below_declared_selection_support")
        if block_requirement is not None and len(blocks) < block_requirement:
            reasons.append("below_declared_independent_block_support")
        if constant is True:
            reasons.append("constant_series")
        return ComputationWindowSupport(window_name, bool(values), len(values), minimum,
            paired, len(blocks), block_requirement, constant, computable, tuple(reasons))

    kpi_rows: list[dict[str, Any]] = []
    for (table, metric_name, identifier), by_window in samples.items():
        if table != "bank/metric_app.csv" or f"metric_app.{metric_name}" not in app_allow:
            continue
        ref = by_window["reference"]
        alt = by_window["alert"]
        kpi_rows.append({"field": f"metric_app.{metric_name}", "id": identifier,
            "reference": ref, "alert": alt, "min_window_support": min(len(ref), len(alt)),
            "rankable_by_primary_policy": len(ref) >= min_kpi and len(alt) >= min_kpi})
    kpi_rows.sort(key=lambda row: (-row["min_window_support"],
        row["id"].casefold(), row["id"], row["field"]))
    primary: dict[str, Any] | None = next((row for row in kpi_rows if row["rankable_by_primary_policy"]), None)
    observed_kpi: dict[str, Any] | None = next((row for row in kpi_rows
        if row["reference"] and row["alert"]), None)
    best_observed_support = max((row["min_window_support"] for row in kpi_rows), default=0)
    kpi_data = bool(kpi_rows)
    kpi_computable = any(row["reference"] and row["alert"] for row in kpi_rows)
    kpi_policy_eligible = primary is not None
    kpi_reasons = []
    if not kpi_data:
        kpi_reasons.append("primary_kpi_data_absent")
    elif primary is None:
        kpi_reasons.append("primary_kpi_below_declared_policy_support")
    primary_kpi = ComputationAvailability("primary_kpi_policy_selection",
        "present" if kpi_data else "absent",
        "computable" if kpi_computable else "not_computable",
        # KPI context selection is not diagnostic ranking or evidence strength.
        "unknown",
        required_inputs=("bank/metric_app.csv:mrt",),
        observed_fields=tuple(sorted({row["field"] for row in kpi_rows})),
        windows=(() if observed_kpi is None else (
            support_window("reference", observed_kpi["reference"], minimum=min_kpi),
            support_window("alert", observed_kpi["alert"], minimum=min_kpi))),
        selected_identity=primary["id"] if primary else None,
        selection_reason=("support_first_policy_threshold_met" if primary else
            "no_observed_kpi_meets_24_timestamp_per_window_policy"),
        reasons=tuple(kpi_reasons),
        details={"minimum_timestamps_per_window": min_kpi,
            "meets_primary_kpi_policy": kpi_policy_eligible,
            "best_observed_support_per_window": best_observed_support,
            "observed_kpi_candidate_count": len(kpi_rows),
            "observed_kpi_candidates": [{"field": row["field"], "opaque_id": row["id"],
                "reference_unique_timestamps": len(row["reference"]),
                "alert_unique_timestamps": len(row["alert"]),
                "meets_primary_policy": row["rankable_by_primary_policy"]}
                for row in kpi_rows[:64]],
            "omitted_candidate_count": max(0, len(kpi_rows) - 64),
            "selection_policy_sha256": policy["kpi_policy_sha256"]})

    component_series: list[dict[str, Any]] = []
    from .bank_metric_policy import classify_bank_metric
    for (table, metric_name, identifier), by_window in sorted(samples.items()):
        if table != "bank/metric_container.csv":
            continue
        semantics = classify_bank_metric(table, metric_name,
            policy_path=root / "config/bank_metric_kind_policy.json").to_dict()
        if semantics["ranking_eligible"] is not True:
            continue
        ref, alt = by_window["reference"], by_window["alert"]
        if not ref and not alt:
            continue
        component_series.append({"field": metric_name, "component_id": identifier,
            "reference": ref, "alert": alt, "semantics": semantics})
    support_rows: list[dict[str, Any]] = []
    computable_count = 0
    for row in component_series[:256]:
        ref, alt = row["reference"], row["alert"]
        ref_blocks = {(ts - windows["reference"].start_ms_utc) // block_ms for ts in ref}
        alt_blocks = {(ts - windows["alert"].start_ms_utc) // block_ms for ts in alt}
        meets_blocks = (minimum_blocks is not None and
            len(ref_blocks) >= minimum_blocks and len(alt_blocks) >= minimum_blocks)
        can_compute = bool(ref and alt and meets_blocks)
        computable_count += int(can_compute)
        support_rows.append({"component_id": row["component_id"], "field": row["field"],
            "unit": row["semantics"].get("unit", "undeclared"),
            "kind": row["semantics"].get("kind", "unknown"),
            "unit_status": row["semantics"].get("unit_status", "undeclared"),
            "direction": row["semantics"].get("direction", "unknown"),
            "transform": row["semantics"].get("transform", "none"),
            "reset_policy": row["semantics"].get("reset_policy", "unverified"),
            "classification_status": row["semantics"].get("classification_status", "unknown"),
            "rule_id": row["semantics"].get("rule_id"),
            "classification_provenance": list(row["semantics"].get("provenance", ())),
            "reference_unique_timestamps": len(ref), "alert_unique_timestamps": len(alt),
            "reference_independent_blocks": len(ref_blocks),
            "alert_independent_blocks": len(alt_blocks),
            "reference_constant": len(set(ref.values())) <= 1 if ref else None,
            "alert_constant": len(set(alt.values())) <= 1 if alt else None,
            "computable_arithmetic_change": can_compute})
    metric_data = bool(component_series)
    metric_computable = computable_count > 0
    metric_computability_state: Literal["computable", "not_computable", "unknown"] = (
        "unknown" if minimum_blocks is None else
        "computable" if metric_computable else "not_computable")
    univariate = ComputationAvailability("component_univariate_window_change",
        "present" if metric_data else "absent",
        metric_computability_state,
        "unknown", required_inputs=("bank/metric_container.csv:exact-classified gauge/rate",),
        observed_fields=tuple(sorted({row["field"] for row in component_series})),
        reasons=(() if metric_computable else (
            "minimum_independent_blocks_runtime_requirement_missing" if minimum_blocks is None else
            "no_component_series_meets_window_and_block_support",)),
        details={"minimum_independent_blocks": minimum_blocks,
            "data_source_table": "bank/metric_container.csv",
            "ranking_state_reason": "availability is not a computed effect or statistically calibrated ranking",
            "series_support": support_rows,
            "omitted_series_count": max(0, len(component_series) - 256),
            "metric_policy_sha256": policy["component_policy_sha256"]})

    association_support: list[dict[str, Any]] = []
    association_computable_count = 0
    association_support_count = 0
    if observed_kpi is not None:
        for row in component_series[:256]:
            pair_counts = {}
            block_counts = {}
            constant_flags = {}
            for window_name in ("reference", "alert"):
                component_values = row[window_name]
                kpi_values = observed_kpi[window_name]
                shared = set(component_values) & set(kpi_values)
                pair_counts[window_name] = len(shared)
                block_counts[window_name] = len({(ts - windows[window_name].start_ms_utc) // block_ms
                    for ts in shared})
                component_constant = len({component_values[ts] for ts in shared}) <= 1 if shared else None
                kpi_constant = len({kpi_values[ts] for ts in shared}) <= 1 if shared else None
                constant_flags[window_name] = {"component": component_constant,
                    "kpi": kpi_constant}
            meets = (minimum_pairs is not None and
                all(pair_counts[name] >= minimum_pairs for name in ("reference", "alert")))
            nonconstant = all(
                constant_flags[name][series_name] is False
                for name in ("reference", "alert")
                for series_name in ("component", "kpi"))
            computable = meets and nonconstant
            if meets:
                association_support_count += 1
            if computable:
                association_computable_count += 1
            association_support.append({"component_id": row["component_id"], "field": row["field"],
                "opaque_kpi_id": observed_kpi["id"],
                "reference_exact_shared_pairs": pair_counts["reference"],
                "alert_exact_shared_pairs": pair_counts["alert"],
                "reference_pair_blocks": block_counts["reference"],
                "alert_pair_blocks": block_counts["alert"], "constant_flags": constant_flags,
                "meets_declared_pair_support": meets,
                "has_constant_input_series": not nonconstant,
                "computable_by_declared_minimum_pairs": computable})
    association = ComputationAvailability("kpi_association_both_windows",
        "present" if metric_data and kpi_data else ("partial" if metric_data or kpi_data else "absent"),
        ("unknown" if minimum_pairs is None and observed_kpi is not None else
         "computable" if association_computable_count else "not_computable"),
        "unknown", required_inputs=("bank/metric_container.csv", "bank/metric_app.csv:mrt"),
        observed_fields=tuple(sorted({row["field"] for row in component_series})),
        selected_identity=observed_kpi["id"] if observed_kpi else None,
        selection_reason=("observed_support_first_candidate_for_separate_association; primary policy selection remains separate"
            if observed_kpi and primary is None else "primary_policy_selected" if primary else None),
        reasons=(() if association_computable_count else (
            "no_observed_primary_kpi" if observed_kpi is None else
            "minimum_exact_pair_support_runtime_requirement_missing" if minimum_pairs is None else
            "no_series_meets_declared_exact_pair_support" if association_support_count == 0 else
            "all_supported_series_constant_in_at_least_one_window",)),
        details={"minimum_exact_pairs_per_window": minimum_pairs,
            "primary_kpi_policy_selected": primary is not None,
            "primary_kpi_minimum_timestamps_per_window": min_kpi,
            "series_meeting_pair_support_count": association_support_count,
            "computable_series_count": association_computable_count,
            "rankability_state_reason": "computable correlation is not itself significance, ranking, or causality",
            "series_support": association_support,
            "omitted_series_count": max(0, len(component_series) - 256)})

    logs_supported = starter_input.capabilities.get("native_log_patterns") == "available"
    log_rows = starter_input.native_log_summaries
    patterns = starter_input.native_log_patterns
    if logs_supported:
        observed_field_names.setdefault("bank/log/log_service.csv", set()).add(
            "log_pattern_exposure_rate")
    log_count_semantics = metric_policy.classify(
        "bank/log/log_service.csv", "log_volume_count").to_dict()
    denominator_ready = bool(logs_supported and any(row.row_count > 0 for row in log_rows))
    logs_computable = bool(denominator_ready and patterns)
    log_comp = ComputationAvailability("native_log_pattern_composition",
        "present" if logs_supported else "absent",
        "computable" if logs_computable else "not_computable", "unknown",
        required_inputs=("trusted_full_window_log_pattern_counts", "per_window_record_denominator"),
        observed_fields=("normalized_pattern_sha256",),
        reasons=(() if logs_computable else (
            "no_complete_native_log_pattern_summary" if not logs_supported else
            "no_pattern_with_nonzero_denominator",)),
        details={"analysis_block_duration_ms": block_ms,
            "derived_count_semantics": log_count_semantics,
            "summary_count": len(log_rows), "pattern_summary_count": len(patterns),
            "full_window_denominator_records": [{"component_id": row.component_id,
                "log_name": row.log_name, "window": row.window.name,
                "row_count": row.row_count, "source_rows_scanned": row.source_rows_scanned,
                "source_sha256": list(row.source_sha256)} for row in log_rows[:64]],
            "pattern_features": [{"component_id": row.component_id,
                "log_name": row.log_name, "window": row.window.name,
                "pattern_sha256": row.pattern_sha256, "normalized_tokens": list(row.tokens),
                "row_support": row.row_support, "block_row_count": row.block_row_count,
                "block_start_ms_utc": row.block_start_ms_utc,
                "block_duration_ms": row.block_duration_ms,
                "source_rows_scanned": row.source_rows_scanned,
                "source_sha256": list(row.source_sha256)} for row in patterns[:128]],
            "ranking_state_reason": "pattern summaries support composition analysis, not failure/error labels",
            "omitted_summary_count": max(0, len(log_rows) - 64),
            "omitted_pattern_count": max(0, len(patterns) - 128)})

    selector_rows: list[dict[str, Any]] = []
    for table in ("bank/metric_container.csv", "bank/metric_app.csv",
                  "bank/trace_span.csv", "bank/log/log_service.csv"):
        observed = sorted(observed_field_names.get(table, set()))
        for selector in metric_policy.selector_catalog(table):
            resolution = metric_policy.resolve_selector(selector.selector_ref, table, observed,
                case_data_sha256=telemetry.data_sha256)
            resolved_fields = []
            for semantics in resolution.matches:
                field_row: dict[str, Any] = semantics.to_dict()
                field_support = support_by_source_field.get((table, semantics.field_name))
                if field_support is not None:
                    field_row["window_support"] = {
                        window: len(field_support[window]) for window in ("reference", "alert")}
                if table == "bank/log/log_service.csv" and semantics.field_name == "log_pattern_exposure_rate":
                    field_row["pattern_summary_support"] = {
                        "reference_pattern_rows": sum(row.row_support for row in patterns
                            if row.window.name == "reference"),
                        "alert_pattern_rows": sum(row.row_support for row in patterns
                            if row.window.name == "alert"),
                        "reference_log_denominator_rows": sum(row.row_count for row in log_rows
                            if row.window.name == "reference"),
                        "alert_log_denominator_rows": sum(row.row_count for row in log_rows
                            if row.window.name == "alert"),
                        "full_window_summary_available": logs_supported}
                resolved_fields.append(field_row)
            selector_rows.append({"selector": selector.to_dict(),
                "resolution_status": resolution.status,
                "match_count": len(resolution.matches),
                "matches": resolved_fields,
                "policy_sha256": resolution.policy_sha256,
                "case_data_sha256": resolution.case_data_sha256})

    signatures = registry.signatures()
    registry_digest = _sha(sorted((row["name"], row["version"], row["output_type"])
        for row in signatures))
    provenance = {"metric_policy_sha256": policy["component_policy_sha256"],
        "bank_metric_policy_sha256": metric_policy.sha256,
        "primary_kpi_policy_sha256": policy["kpi_policy_sha256"],
        "telemetry_data_sha256": telemetry.data_sha256,
        "telemetry_source_hashes_sha256": _sha(dict(telemetry.file_sha256)),
        "registry_signatures_sha256": registry_digest,
        "runtime_values_sha256": _sha(dict(runtime_values))}
    report = OperatorIRCapabilityReport(primary_kpi,
        (univariate, association, log_comp), provenance, tuple(selector_rows))
    return report.as_dict()


def operator_ir_log_input(starter_input: Any):
    """Project PaperStarterInput to the closed raw-text-free V2 log contract."""
    from .bank_metric_policy import classify_bank_metric
    from .contracts import OperatorIRLogBlockDenominator, OperatorIRLogInput, PaperStarterInput
    if not isinstance(starter_input, PaperStarterInput):
        raise OperatorIRError("V2 log projection requires trusted PaperStarterInput")
    available = starter_input.capabilities.get("native_log_patterns") == "available"
    summaries = tuple(starter_input.native_log_summaries)
    patterns = tuple(starter_input.native_log_patterns) if available else ()
    telemetry = starter_input.telemetry
    windows = {"reference": telemetry.reference_window, "alert": telemetry.alert_window}
    denominators: dict[tuple[str, str, str, int], float] = {}
    for point in telemetry.metrics:
        if (point.table != "bank/log/log_service.csv" or
                point.metric_name != "log_volume_count" or point.value is None or
                not math.isfinite(float(point.value))):
            continue
        window_name = next((name for name, window in windows.items()
            if window.contains(point.timestamp_ms_utc)), None)
        if window_name is None:
            continue
        log_name = point.item_id
        if not log_name:
            continue
        window = windows[window_name]
        block_start = window.start_ms_utc + ((point.timestamp_ms_utc - window.start_ms_utc) //
            starter_input.analysis_block_duration_ms) * starter_input.analysis_block_duration_ms
        key = (point.entity.identifier, log_name, window_name, block_start)
        numeric_count = float(point.value)
        if not numeric_count.is_integer():
            raise OperatorIRError("trusted log row-count bucket is not an integer event count")
        if key in denominators:
            raise OperatorIRError("trusted log row-count buckets contain duplicate component/window/time keys")
        denominators[key] = numeric_count
    block_denominators = tuple(OperatorIRLogBlockDenominator(component, log_name,
        windows[window_name], block_start, int(count))
        for (component, log_name, window_name, block_start), count in sorted(denominators.items()))
    count_semantics = classify_bank_metric("bank/log/log_service.csv", "log_volume_count",
        policy_path=ROOT / "config/bank_metric_kind_policy.json").to_dict()
    count_semantics["analysis_block_duration_ms"] = starter_input.analysis_block_duration_ms
    if starter_input.analysis_block_duration_ms != 60_000:
        count_semantics["projection_transform"] = "sum_zero_filled_minute_counts_into_analysis_block"
    hashes_seen = {value for row in summaries for value in row.source_sha256}
    hashes_seen.update(value for row in patterns for value in row.source_sha256)
    hashes = tuple(sorted(hashes_seen))
    return OperatorIRLogInput("available" if available else "unsupported", available,
        starter_input.analysis_block_duration_ms, summaries, patterns,
        block_denominators, count_semantics, hashes, telemetry.data_sha256)


def compact_operator_ir_inputs(inputs: Mapping[str, Any], *, root: Path = ROOT) -> dict[str, Any]:
    """Retain every consumed full-window series while shedding irrelevant aggregate rows."""
    import hashlib
    from .contracts import Aggregate
    policy = _policy(root)
    keep_component = set(policy["component_allowlist"])
    keep_kpi = set(policy["kpi"].get("primary_metric_allowlist", []))
    result = dict(inputs)
    for port in ("reference", "alert"):
        aggregate = inputs.get(port)
        if not isinstance(aggregate, Aggregate):
            raise OperatorIRError(f"{port} input is not a trusted Aggregate")
        rows = []
        counts = {"retained": 0, "omitted_statistic": 0,
            "omitted_unconsumed_metric": 0, "omitted_unrankable_metric": 0}
        for row in aggregate.rows:
            if row.statistic != "median":
                counts["omitted_statistic"] += 1
                continue
            details = row.details
            source_table = details.get("source_table")
            source_field = details.get("source_field")
            if row.entity.role == "component" and source_table == "bank/metric_container.csv":
                if source_field not in keep_component:
                    counts["omitted_unconsumed_metric"] += 1
                    continue
                if (details.get("ranking_eligible") is not True or
                        details.get("metric_kind") not in {"gauge", "rate"}):
                    counts["omitted_unrankable_metric"] += 1
                    continue
            elif (row.entity.role == "app_kpi" and source_table == "bank/metric_app.csv" and
                  f"metric_app.{source_field}" in keep_kpi):
                pass
            else:
                counts["omitted_unconsumed_metric"] += 1
                continue
            samples = details.get("bucket_samples")
            if not isinstance(samples, (tuple, list)):
                raise OperatorIRError("approved full-window series lacks bucket_samples")
            rows.append(row)
            counts["retained"] += 1
        metadata = dict(aggregate.metadata)
        metadata["ir_compaction"] = {"schema_version": "e4-typed-operator-ir-input-compaction/1",
            "original_aggregate_sha256": hashlib.sha256(canonical_json(aggregate)).hexdigest(),
            "retained_rows": counts["retained"], "omitted_rows": counts,
            "retained_roles": ["component", "app_kpi"],
            "retained_statistic": "median with full-window bucket_samples",
            "omitted_rows_are_nonconsumed": True}
        result[port] = Aggregate(aggregate.window, tuple(rows), aggregate.source_tables, metadata)
    return result


def typed_operator_ir_schema() -> dict[str, Any]:
    """Private authoring schema; call only while constructing the evolve prompt."""
    policy = _policy()
    kernels = json.loads(json.dumps(KERNELS))
    kernels["series.select_field@1"]["params"]["source_field"]["choices"] = list(
        policy["component_allowlist"])
    return {"schema_version": IR_SCHEMA, "authoring_profile": PROFILE,
        "spec_fields": {"name": "evolved.<lowercase_identifier>", "version": "short declared version",
            "description": "bounded explanation tied to a failed-analysis limitation", "family": "lowercase identifier",
            "output_type": "ScoredCandidates", "parameters": {},
            "authoring_profile": PROFILE,
            "program": {"schema_version": IR_SCHEMA,
                "nodes": "ordered array of {id,kernel,version,inputs,params,bindings}; references must point to prior nodes or $input.port",
                "return": {"node": "node id producing ScoredCandidates"}}},
        "fixed_host_abi": {"input_ports": _PORTS, "input_requirements": _REQUIREMENTS,
            "authoring_rule": "Informational only; do not include input_ports or input_requirements in the authored spec. The host injects these fixed values."},
        "kernels": kernels,
        "approved_component_metric_fields": policy["component_allowlist"],
        "primary_kpi_policy": {"metric_allowlist": policy["kpi"].get("primary_metric_allowlist", []),
            "minimum_unique_timestamps_per_window": int(policy["kpi"]["minimum_support_per_window"]),
            "selection": "paired reference+alert only; maximize min window support; ties use opaque tc ID casefold, then exact ID, then field; no fallback"},
        "kernel_semantics": {
            "series.component_metrics": "Trusted full-window median rows for observed candidate components, filtered by current source-field allowlist and ranking-eligible gauge/rate policy; each row carries complete timestamped bucket_samples and exact source/case pins.",
            "series.primary_kpi": "Trusted app_kpi mrt series only. Requires minimum unique finite timestamps in both adjacent windows and selects one KPI by the frozen support-first rule; it is context only and never a component candidate.",
            "series.align_kpi": "Backward-compatible alert-only alignment: exact shared alert timestamps, no interpolation; compare each alert value with its series' reference-window median baseline.",
            "series.align_kpi_both_windows": "Generic joint-window association: exact shared timestamps in both reference and alert windows, no interpolation; center component and KPI values in both windows on their respective reference-window medians. This preserves coordinated step changes as temporal variation; association is not causality.",
            "series.reduce": "Within each runtime-sized time block, take the median of bucket samples. Then compute mean, median, population variance, or nearest-rank q90 at index floor(0.9*(n-1)) across independent block medians. Require the runtime minimum block count in both windows.",
            "series.correlate": "Spearman correlation preserves signed rho in [-1,1], uses average ranks for ties and exact aligned per-timestamp component/KPI deviations; constant or under-supported series are ineligible. Minimum pair support establishes computability, not significance or causality. Reference-median centering changes values but not alert-window ranks, so association alone is not a change test.",
            "numbers.window_compare": "difference=alert-reference; relative_change=(alert-reference)/max(abs(reference), absolute_scale_floor); ratio=alert/max(abs(reference), absolute_scale_floor). The scale floor is trusted runtime, not a model literal.",
            "numbers.reduce_component": "Combines only eligible scores. Only strictly positive final component scores can emit ranked evidence; zero or negative signed statistics remain ineligible. Non-dimensionless values from different source fields or units abstain; dimensionless scores may combine across approved fields.",
            "evidence.from_scores": "Host derives evidence values and lineage from trusted component scores; IR cannot construct or assert them.",
            "scores.assemble": "Host joins trusted scores and evidence to exact candidate identities; evidence must derive from the same score node."},
        "score_combination_semantics": "Registered scorers in one analysis family combine by maximum raw score per component, then family-level normalization. A new evidence branch can leave the score and ordering unchanged when another same-family score is larger; no family renaming, score inflation, or forced ranking change is justified.",
        "private_types": ["ComponentSeries", "KPISeries", "AlignedSeriesPairs",
            "WindowSummaries", "KeyedNumbers", "ComponentScores", "EvidenceSet", "ScoredCandidates"],
        "runtime_bindings": "When a kernel parameter names runtime_field, it must bind exactly to runtime.<field>; values are bounded by frozen runtime policy.",
        "limits": {"maximum_nodes": 40, "maximum_series": 4096,
            "maximum_depth": 12,
            "maximum_points": 250000, "maximum_timestamp_pairs": 1000000,
            "maximum_operations": 5000000,
            "execution": "existing isolated M4 worker limits: CPU 10 seconds, wall 15 seconds, memory 512 MiB, request 16 MiB"},
        "trust": "Component identities, source hashes, case identity, KPI selection, output evidence and eligibility derive from typed trusted inputs and pinned policy. No entity/case/evidence literals are accepted.",
        "provenance": {"component_metric_policy_sha256": policy["component_policy_sha256"],
            "primary_kpi_policy_sha256": policy["kpi_policy_sha256"],
            "kernel_abi_sha256": _sha({"abi": ENGINE_ABI, "kernels": KERNELS}),
            "interpreter_sha256": hashlib.sha256(Path(__file__).with_name("m4_operator_ir_worker.py").read_bytes()).hexdigest()}}


def trusted_runtime_context_v2(root: Path = ROOT,
        inputs: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Trusted V2 policy bundle; legacy KPI selection remains a separate pin."""
    v2 = _v2_policy(root)
    from .bank_metric_policy import load_bank_metric_policy
    metric_policy = load_bank_metric_policy(root / "config/bank_metric_kind_policy.json")
    observed_fields: dict[str, set[str]] = {}
    case_hashes: set[str] = set()
    input_source_hashes: set[str] = set()
    if isinstance(inputs, Mapping):
        from .contracts import Aggregate, OperatorIRLogInput
        for value in inputs.values():
            if isinstance(value, Aggregate):
                data_sha = value.metadata.get("case_data_sha256")
                if isinstance(data_sha, str):
                    case_hashes.add(data_sha)
                source_hashes = value.metadata.get("source_hashes", {})
                if isinstance(source_hashes, Mapping):
                    input_source_hashes.update(str(digest) for digest in source_hashes.values()
                        if isinstance(digest, str))
                for row in value.rows:
                    table, field = row.details.get("source_table"), row.details.get("source_field")
                    if isinstance(table, str) and isinstance(field, str):
                        observed_fields.setdefault(table, set()).add(field)
            elif isinstance(value, OperatorIRLogInput):
                case_hashes.add(value.case_data_sha256)
                observed_fields.setdefault("bank/log/log_service.csv", set()).add("log_volume_count")
                if value.status == "available" and value.complete and value.patterns:
                    observed_fields["bank/log/log_service.csv"].add("log_pattern_exposure_rate")
    if len(case_hashes) > 1:
        raise OperatorIRError("V2 trusted inputs do not share one exact case-data hash")
    case_hash = next(iter(case_hashes), None)
    metric_semantics: dict[str, dict[str, Any]] = {}
    selector_catalog: dict[str, dict[str, Any]] = {}
    selector_catalog_by_table: dict[str, list[dict[str, Any]]] = {}
    selector_resolutions: dict[str, dict[str, Any]] = {}
    for table in ("bank/metric_container.csv", "bank/metric_app.csv",
                  "bank/trace_span.csv", "bank/log/log_service.csv"):
        fields = observed_fields.get(table, set())
        for field in sorted(fields):
            metric_semantics[f"{table}:{field}"] = metric_policy.classify(table, field).to_dict()
        catalog = metric_policy.selector_catalog(table)
        catalog_rows = [item.to_dict() for item in catalog]
        selector_catalog_by_table[table] = catalog_rows
        for item in catalog:
            selector_catalog[item.selector_ref] = item.to_dict()
            if case_hash is not None:
                resolution = metric_policy.resolve_selector(item.selector_ref, table,
                    sorted(fields), case_data_sha256=case_hash)
                selector_resolutions[item.selector_ref] = resolution.to_dict()
    return {**trusted_runtime_context(root),
        "typed_operator_ir_v2_policy_sha256": v2["v2_policy_sha256"],
        "typed_operator_ir_v2_policy": v2["v2"],
        "v2_base_worker_sha256": v2["v2_base_worker_sha256"],
        "bank_metric_policy_sha256": metric_policy.sha256,
        "observed_metric_semantics": metric_semantics,
        "metric_selector_catalog": selector_catalog,
        "metric_selector_catalog_by_table": selector_catalog_by_table,
        "metric_selector_resolutions": selector_resolutions,
        "case_data_sha256": case_hash,
        "input_source_hashes_sha256": _sha(sorted(input_source_hashes)) if input_source_hashes else None}


def compact_operator_ir_inputs_v2(inputs: Mapping[str, Any],
        program: Mapping[str, Any], *, root: Path = ROOT) -> dict[str, Any]:
    """Compact V2 inputs from reachable kernel dependencies, not a global KPI rule."""
    kernels = {str(row["kernel"]) for row in program.get("nodes", [])}
    needs_kpi = bool(kernels & {"series.primary_kpi", "series.observed_kpi",
        "series.align_kpi", "series.align_kpi_both_windows", "series.correlate"})
    uses_metric_inputs = bool(kernels & {"series.component_metrics", "series.primary_kpi",
        "series.observed_kpi", "series.align_kpi", "series.align_kpi_both_windows",
        "series.correlate", "series.reduce", "series.select_metric_rule",
        "series.normalize_reference_scale", "series.reference_quantile_exceedance",
        "series.sigma_exceedance"})
    compact = dict(inputs)
    if uses_metric_inputs:
        from .bank_metric_policy import load_bank_metric_policy
        from .contracts import Aggregate
        metric_policy = load_bank_metric_policy(root / "config/bank_metric_kind_policy.json")
        allowed_kpis = set(_policy(root)["kpi"].get("primary_metric_allowlist", ()))
        for port in ("reference", "alert"):
            aggregate = inputs.get(port)
            if not isinstance(aggregate, Aggregate):
                raise OperatorIRError(f"{port} V2 input is not a trusted Aggregate")
            rows = []
            counts = {"retained": 0, "omitted_statistic": 0,
                "omitted_unconsumed_metric": 0, "omitted_unrankable_metric": 0}
            for row in aggregate.rows:
                if row.statistic != "median":
                    counts["omitted_statistic"] += 1
                    continue
                source_table = row.details.get("source_table")
                source_field = row.details.get("source_field")
                if (row.entity.role == "component" and
                        source_table == "bank/metric_container.csv" and
                        isinstance(source_field, str)):
                    semantics = metric_policy.classify(source_table, source_field).to_dict()
                    if (not semantics["ranking_eligible"] or
                            semantics["kind"] not in {"gauge", "rate"}):
                        counts["omitted_unrankable_metric"] += 1
                        continue
                    details = {**dict(row.details), "metric_kind": semantics["kind"],
                        "unit": semantics["unit"], "unit_status": semantics["unit_status"],
                        "direction": semantics["direction"], "transform": semantics["transform"],
                        "reset_policy": semantics["reset_policy"],
                        "classification_status": semantics["classification_status"],
                        "rule_id": semantics["rule_id"], "rule_version": semantics["rule_version"],
                        "selector_ref": semantics["selector_ref"],
                        "semantics_provenance": semantics["provenance"],
                        "ranking_eligible": True}
                elif (row.entity.role == "app_kpi" and needs_kpi and
                        source_table == "bank/metric_app.csv" and
                        f"metric_app.{source_field}" in allowed_kpis):
                    details = dict(row.details)
                else:
                    counts["omitted_unconsumed_metric"] += 1
                    continue
                if not isinstance(details.get("bucket_samples"), (tuple, list)):
                    raise OperatorIRError("V2 retained aggregate lacks full-window bucket_samples")
                rows.append(replace(row, details=details))
                counts["retained"] += 1
            metadata = dict(aggregate.metadata)
            metadata["ir_compaction"] = {"schema_version": "e4-typed-operator-ir-v2-input-compaction/1",
                "original_aggregate_sha256": hashlib.sha256(canonical_json(aggregate)).hexdigest(),
                "retained_rows": counts["retained"], "omitted_rows": counts,
                "required_metric_sources_derived_from_reachable_kernels": True,
                "kpi_rows_retained": needs_kpi,
                "bank_metric_policy_sha256": metric_policy.sha256}
            compact[port] = Aggregate(aggregate.window, tuple(rows), aggregate.source_tables, metadata)
    if uses_metric_inputs and not needs_kpi:
        for port in ("reference", "alert"):
            aggregate = compact.get(port)
            if aggregate is None:
                continue
            if not isinstance(aggregate, Aggregate):
                raise OperatorIRError(f"{port} V2 input is not a trusted Aggregate")
            compacted_rows = tuple(row for row in aggregate.rows
                if not (row.entity.role == "app_kpi" and
                    row.details.get("source_table") == "bank/metric_app.csv"))
            metadata = dict(aggregate.metadata)
            summary = dict(metadata.get("ir_compaction", {}))
            summary.update({"schema_version": "e4-typed-operator-ir-v2-input-compaction/1",
                "primary_kpi_rows_omitted": len(aggregate.rows) - len(compacted_rows),
                "program_requires_kpi": False,
                "requirements_derived_from_reachable_kernels": True})
            metadata["ir_compaction"] = summary
            compact[port] = Aggregate(aggregate.window, compacted_rows, aggregate.source_tables, metadata)
    uses_logs = any(kernel.startswith("logs.") for kernel in kernels)
    if uses_logs:
        from .contracts import PaperStarterInput
        starter = inputs.get("starter_input")
        if not isinstance(starter, PaperStarterInput):
            raise OperatorIRError("reachable log kernels require a trusted PaperStarterInput")
        compact["starter_input"] = operator_ir_log_input(starter)
    else:
        compact.pop("starter_input", None)
    return compact


def typed_operator_ir_v2_schema(root: Path = ROOT) -> dict[str, Any]:
    """Private, closed V2 language schema; V1 remains unchanged for replay."""
    policy = _v2_policy(root)
    kernels = json.loads(json.dumps(KERNELS_V2))
    component_selectors = [row for row in policy["selector_catalog"]
        if row["source_table"] == "bank/metric_container.csv"]
    kernels["series.select_metric_rule@1"]["params"]["selector_ref"]["choices"] = [
        row["selector_ref"] for row in component_selectors]
    return {"schema_version": IR_SCHEMA_V2, "authoring_profile": PROFILE_V2,
        "spec_fields": {"name": "evolved.<lowercase_identifier>",
            "version": "short declared version",
            "description": "bounded computation explanation tied to an observed capability limitation",
            "family": "lowercase identifier", "output_type": "ScoredCandidates",
            "parameters": {}, "authoring_profile": PROFILE_V2,
            "program": {"schema_version": IR_SCHEMA_V2,
                "nodes": "ordered typed kernel nodes; inputs reference a prior node id or a declared input as $input.<name>",
                "return": {"node": "node id producing ScoredCandidates"}},
            "input_ports": "host fixed; model authors only program"},
        "fixed_host_abi": {"input_ports": _PORTS_V2,
            "optional_log_port": {**_STARTER_INPUT_PORT_V2,
                "type": "PaperStarterInput",
                "required_when": "a reachable logs.* kernel is present"},
            "requirements": "injected per spec by _requirements_for_v2_program; application KPI is required only when a reachable kernel consumes it"},
        "authoring_rules": {
            "external_references": "Use $input.reference, $input.alert, or $input.candidates for those declared ports; use $input.starter_input only for a reachable logs.* kernel. Bare strings are prior node ids and are valid only after that node has appeared.",
            "runtime_bindings": "For a kernel parameter named P with runtime_field F, omit P from params and write bindings[P]=runtime.F. Other parameters go in params under their exact kernel parameter names.",
            "window_parameters": "A kernel's required window parameter is an ordinary params.window value unless its catalog descriptor explicitly declares runtime_field; never omit a required parameter or infer a runtime binding."},
        "kernels": kernels,
        "metric_selector_catalog": component_selectors,
        "support_semantics": policy["v2"]["support_semantics"],
        "legacy_primary_kpi_policy": {"policy_sha256": policy["kpi_policy_sha256"],
            "minimum_unique_timestamps_per_window": int(policy["kpi"]["minimum_support_per_window"]),
            "role": "legacy primary selection only; V2 data/computation availability is reported separately"},
        "log_summary_semantics": policy["v2"]["log_summary_semantics"],
        "trust": "Trusted input identities, policy, source hashes, and data support are host-provided. No Python/source, raw log text, case/entity/evidence literals, or labels are allowed.",
        "provenance": {"component_metric_policy_sha256": policy["component_policy_sha256"],
            "primary_kpi_policy_sha256": policy["kpi_policy_sha256"],
            "typed_operator_ir_v2_policy_sha256": policy["v2_policy_sha256"],
            "v1_base_worker_sha256": policy["v2_base_worker_sha256"],
            "v2_interpreter_sha256": policy["v2_worker_sha256"],
            "kernel_abi_sha256": _sha({"abi": ENGINE_ABI_V2,
                "kernels": KERNELS_V2,
                "base_worker_sha256": policy["v2_base_worker_sha256"]})}}


def model_facing_operator_ir_v2_schema(root: Path = ROOT) -> dict[str, Any]:
    """Compact prompt view of the full V2 schema without dropping legal IR facts.

    Host validation continues to use ``typed_operator_ir_v2_schema``. This
    projection interns selector choices and removes only redundant selector
    metadata and implementation-only hash pins.
    """
    schema = typed_operator_ir_v2_schema(root)
    optional_log_port = schema["fixed_host_abi"]["optional_log_port"]
    optional_log_port.update({"name": "starter_input",
        "kernel_input_type": "OperatorIRLogInput",
        "host_projection": "The host converts the trusted PaperStarterInput to the closed OperatorIRLogInput before passing it to logs.* kernels; author DAG references to $input.starter_input only."})
    selector_parameter = schema["kernels"]["series.select_metric_rule@1"][
        "params"]["selector_ref"]
    original_choices = selector_parameter.pop("choices")
    if original_choices != [row["selector_ref"] for row in schema["metric_selector_catalog"]
            if row["source_table"] == "bank/metric_container.csv"]:
        raise OperatorIRError("V2 prompt selector catalog differs from the host kernel choices")
    selector_parameter["choices_ref"] = "metric_selector_catalog"
    # V2's normalizer still accepts a legacy declared-port form for replay,
    # but paper evolution asks the model to author only the exact compact spec.
    # The host-owned ports and dynamic requirements remain under fixed_host_abi.
    schema["spec_fields"].pop("input_ports", None)
    schema["metric_selector_catalog"] = [{
        "selector_ref": row["selector_ref"],
        "description": row["description"]}
        for row in schema["metric_selector_catalog"]]
    schema["metric_selector_catalog_source_table"] = "bank/metric_container.csv"
    # The capability summary is the source for observed policy/case data
    # provenance. Keep each distinct schema-policy digest once here; worker and
    # interpreter identities are host execution pins, not authoring facts.
    schema["provenance"] = {key: schema["provenance"][key] for key in (
        "component_metric_policy_sha256", "primary_kpi_policy_sha256",
        "typed_operator_ir_v2_policy_sha256")}
    schema["legacy_primary_kpi_policy"].pop("policy_sha256", None)
    schema["prompt_projection"] = {
        "schema_version": "e4-typed-operator-ir-v2-model-schema/1",
        "semantics": "Only repeated selector metadata and host execution pins are interned or omitted. All kernels, versions, types, ports, legal params, runtime bindings, reachable-kernel requirements, selector refs and selector descriptions remain present."}
    return schema


def model_facing_trusted_runtime_context_v2(
        context: Mapping[str, Any]) -> dict[str, Any]:
    """Project trusted V2 runtime context to references already present in prompt.

    Exact selector resolutions, observed metric semantics, and data/source
    digests are supplied once by the compact capability summary. The full
    context returned by ``trusted_runtime_context_v2`` remains unchanged for
    compiler and worker validation.
    """
    if not isinstance(context, Mapping):
        raise OperatorIRError("V2 prompt runtime context must be a mapping")
    support = context.get("primary_kpi_minimum_support")
    if type(support) is not int or support <= 0:
        support = int(_policy()["kpi"]["minimum_support_per_window"])
    allowlist = context.get("primary_metric_allowlist")
    if not isinstance(allowlist, list) or not allowlist:
        allowlist = ["metric_app.mrt"]
    return {"schema_version": "e4-typed-operator-ir-v2-prompt-runtime/1",
        "primary_metric_allowlist": list(allowlist),
        "primary_kpi_minimum_support": support,
        "metric_selector_catalog_ref": "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog",
        "metric_selector_resolutions_ref": "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.metric_selector_catalog[*].support_profile_ref -> selector_case_resolution_profiles.profiles[*].case_resolution",
        "semantic_profile_catalog_ref": "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.selector_semantic_profiles",
        "policy_provenance_ref": "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.provenance",
        "selector_resolution_provenance_ref": "PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG.selector_resolution_provenance",
        "data_source_provenance_ref": "TRUSTED TYPED-IR V2 DATA/SUPPORT CAPABILITY SUMMARY.provenance"}
