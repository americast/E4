"""Schema-only M4 bootstrap for a versioned Bank deployment library."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .bank_adapter import BANK_PROMPT_PATH, BANK_SOURCE_REVISION, sha256_file
from .benchmark import REPAIR_LIMIT, parse_strict_json_object
from .cache import CacheStore, canonical_json
from .contracts import RankedAnswer
from .dag import DAGValidationError, verify_dag
from .executor import ExecutionFailure, SerialExecutor
from .m4_controller import (M4ControllerError, M4ModelClient, M4_SYSTEM_PROMPT, _append_jsonl,
    _canonical_hash, _implementation_fingerprint, _materialize_seeds_for_registry, _plain, _write_json, build_m4_runtime, public_case_digest,
    _validate_graph, data_operator_contract_schema, generated_spec_shape_example,
    reject_identifier_literals, typed_contract_wire_schema,
    validate_bank_answer)
from .m4_data import M4QueryBudget, make_m4_registry
from .m4_fixtures import (fixture_expected_top_component, synthetic_case_runtime,
                          write_synthetic_public_case)
from .m4_generated import (generated_spec_contract_schema, register_generated_specs,
                           validate_generated_spec)
from .m4_library import (create_library_snapshot, playbook_graph_hash,
                         snapshot_library)
from .model_client import LocalModelClient
from .registry import OperatorRegistry


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_SYSTEM_PROMPT = (
    "Build schema-specific E4 operator contracts and DAG playbooks from the published schema only. "
    "No incident, telemetry rows, labels, or diagnosis history are available. Return only the requested JSON."
)


class M4BootstrapModelClient(M4ModelClient):
    def make_payload(self, prompt: str, *, max_tokens: int | None = None) -> dict[str, Any]:
        payload = super().make_payload(prompt, max_tokens=max_tokens)
        payload["messages"][0]["content"] = BOOTSTRAP_SYSTEM_PROMPT
        return payload


def _materialize_seeds(seed_library: Mapping[str, Any], registry: OperatorRegistry) -> dict[str, Any]:
    versions: dict[str, str] = {}
    for row in registry.signatures():
        if row["name"] == "bank.answers.combine":
            if row["version"].startswith("1+m4."):
                versions[row["name"]] = row["version"]
        elif row["name"] not in versions:
            versions[row["name"]] = row["version"]
    examples = seed_library.get("examples")
    if not isinstance(examples, Mapping) or not examples:
        raise M4ControllerError("seed_schema", "frozen Bank seed library has no examples")
    materialized = {}
    for name, doc in examples.items():
        value = json.loads(json.dumps(doc, ensure_ascii=False, allow_nan=False))
        for node in value.get("nodes", []):
            operator_name = node.get("operator")
            if operator_name not in versions:
                raise M4ControllerError("seed_operator_missing", f"no registry version for {operator_name}")
            node["version"] = versions[operator_name]
        materialized[name] = value
    return materialized


def _schema_only_context(root: Path, registry: OperatorRegistry,
                         seed_library: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    # Only pinned repository/schema identity and the declared native columns
    # cross this boundary. Do not include date lists, observations, manifests,
    # case IDs, source counts, or data-derived metric-name allowlists.
    schema = {
        "schema_version": "e4-bank-schema-bootstrap/1",
        "upstream": {"repository": "https://github.com/microsoft/OpenRCA",
                     "revision": BANK_SOURCE_REVISION,
                     "schema_document": BANK_PROMPT_PATH},
        "tables": {
            "metric/metric_app.csv": {"columns": ["timestamp", "rr", "sr", "cnt", "mrt", "tc"],
                                      "timestamp_unit": "epoch seconds (upstream declared)",
                                      "identity": "tc is opaque application-KPI identity, not a component"},
            "metric/metric_container.csv": {"columns": ["timestamp", "cmdb_id", "kpi_name", "value"],
                                            "timestamp_unit": "epoch seconds (upstream declared)",
                                            "identity": "exact cmdb_id and kpi_name; absolute KPI units may be undeclared"},
            "trace/trace_span.csv": {"columns": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
                                     "timestamp_unit": "epoch milliseconds (upstream declaration; selected source-date files can differ)",
                                     "identity": "trace-local (trace_id,span_id); no success or call-type field; duration unit undeclared"},
            "log/log_service.csv": {"columns": ["log_id", "timestamp", "cmdb_id", "log_name", "value"],
                                    "timestamp_unit": "epoch seconds (upstream declared)",
                                    "semantics": "value is raw text; no error/severity/GC/OOM parser is provided"}},
        "static_deployment_metadata": {
            "official_candidate_component_ids": list(_official_candidates()),
            "topology": "pod-level entities and horizontal traces; no verified vertical aliases or host/service/database links",
            "unknown_relationship_policy": "preserve exact IDs, do not infer aliases"},
        "runtime_schema": json.loads((root / "config/runtime_schema.json").read_text(encoding="utf-8")),
        "protected_runtime_values": policy["protected_runtime_values"],
        "runtime_override_contract": policy["model_runtime_overrides"],
        "registered_operators": registry.signatures(),
        "typed_contract_wire_schema": typed_contract_wire_schema(),
        "data_operator_contract_schema": data_operator_contract_schema(),
        "generated_spec_contract_schema": generated_spec_contract_schema(),
        "generated_spec_shape_example": generated_spec_shape_example(),
        "fixed_bank_seed_examples": _materialize_seeds(seed_library, registry),
        "bootstrap_budgets": policy["bootstrap"],
        "public_query_and_worker_limits": policy["query_and_generation"],
        "operator_retention": policy["library"],
    }
    return schema


def _official_candidates() -> tuple[str, ...]:
    from e4infra.bank_prep import BANK_OFFICIAL_CANDIDATE_COMPONENTS
    return tuple(BANK_OFFICIAL_CANDIDATE_COMPONENTS)


def _prompt(context: Mapping[str, Any]) -> str:
    return ("BOOTSTRAP CONTRACT\nGenerate a concise useful schema-specific addition; one operator and one playbook are enough "
        "when justified. Two operators and four additional playbooks are hard maximums, not quotas. Generate using only this schema, these static candidate names, runtime contracts, registered operator "
        "signatures, and fixed four Bank seeds. There are no case rows, incident IDs, observed per-case IDs, "
        "counts, labels, prior outputs, or evaluator results in the prompt. Do not add unsupported trace success, "
        "call type, root, service/database roles, aliases, or failure/status classifiers. You may propose "
        "deterministic parameterized pattern checks over raw log.value excerpts, but do not invent source fields. Keep thresholds "
        "bounded: Aggregates produced by Bank operators may exceed the 2 MiB generated-worker input cap; use "
        "the registered bank.public.project operator to select declared scalar summaries with explicit filters, "
        "offset 0..100000 and limit 1..256 before passing oversized summary aggregates to generated operators. It "
        "preserves full-window row.value/support/identity/source/contributors and scalar details, omits non-scalar "
        "details/metadata with explicit omitted-key and coverage records, does not recompute statistics, and caps "
        "output at 512 KiB/4096-byte strings. It rejects native query pages and re-projection. Keep raw "
        "bank.public.query pages for operators that need raw rows or log text. "
        "as declared parameters or runtime fields; numeric literals other than 0 and 1 are rejected. New operators "
        "must use evolved.<lower_snake> and the sandboxed operator(inputs,params,runtime) entrypoint. For typed inputs, "
        "inputs is a port mapping of cache wire envelopes; read fields with contract_fields(inputs['port'],'Aggregate'). "
        "contract_fields unwraps $fields and returns recursively read-only mapping values and immutable tuples; do not mutate them. "
        "read_case_json/read_catalog_json have no mount for M4 generated operators and will fail if called. "
        "For typed outputs return exact $type/$fields cache envelopes and nested $map/$tuple markers. Allowed imports "
        "are math, statistics, itertools, collections; allowed builtins are abs,all,any,bool,dict,enumerate,float,int,len,list,max,min,range,round,set,sorted,str,sum,tuple,zip,print,Exception,ValueError and contract_fields. "
        "Filesystem, network, reflection, classes, try/except, dynamic imports and recursion are unavailable. "
        "Bounded for/while iteration over supplied data is permitted under the worker resource limits. "
        "Propose only portable schema-specific utility/scoring operators and reusable analysis playbooks. Return exactly "
        "{operators:[operator_spec],playbooks:[{name,dag}]}. Each operator spec has exactly "
        "{name,version,description,family,input_ports,output_type,parameters,source}. Each DAG uses "
        "{schema_version:'e4-dag/1',nodes:[{id,operator,version,inputs,params,bindings}],return:{node}}. Every node's inputs, params, and bindings must be JSON objects/maps (use {} when empty); inputs maps ports to node IDs, params maps parameter names to JSON values, bindings maps parameter names to runtime field names. Every playbook DAG must return the trusted versioned bank.answers.combine operator that produces RankedAnswer, never an intermediate Aggregate, CandidateSet, or ScoredCandidates. "
        "Do not include validation outputs, expected answers, fixture IDs, target components, or labels.\n\n"
        "SCHEMA-ONLY BOOTSTRAP CONTEXT\n" +
        json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))


def _validate_bundle(bundle: Any, context: Mapping[str, Any], registry: OperatorRegistry,
                     root: Path, policy: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(bundle, Mapping) or set(bundle) != {"operators", "playbooks"}:
        raise M4ControllerError("bootstrap_schema", "bootstrap result must contain exactly operators and playbooks", static=True)
    operators = bundle["operators"]
    playbooks = bundle["playbooks"]
    if (not isinstance(operators, list) or len(operators) > int(policy["bootstrap"]["maximum_new_operators"]) or
            not isinstance(playbooks, list) or len(playbooks) > int(policy["bootstrap"]["maximum_new_playbooks"])):
        raise M4ControllerError("bootstrap_budget", "bootstrap proposal count exceeds the frozen budget", static=True)
    normalized = []
    for spec in operators:
        candidate = validate_generated_spec(spec)
        reject_identifier_literals(candidate["source"], set(_official_candidates()))
        normalized.append(candidate)
    names = [item["name"] for item in normalized]
    if len(names) != len(set(names)):
        raise M4ControllerError("bootstrap_duplicate_operator", "bootstrap operator names must be unique", static=True)
    version_mapping = {(item["name"], item["declared_version"]): item["version"] for item in normalized}
    validation_registry = make_m4_registry(None, normalized, root=root)
    normalized_books: list[dict[str, Any]] = []
    for index, item in enumerate(playbooks):
        if (not isinstance(item, Mapping) or set(item) != {"name", "dag"} or
                not isinstance(item["name"], str) or not item["name"].strip()):
            raise M4ControllerError("bootstrap_playbook_schema", f"playbooks[{index}] must contain name and dag", static=True)
        from .m4_controller import _resolve_generated_versions
        document = _resolve_generated_versions(item["dag"], version_mapping)
        normalized_books.append({"name": item["name"], "document": document,
            "provenance": {"source": "schema_only_bootstrap"}})
    runtime = synthetic_case_runtime()
    for book in normalized_books:
        try:
            _validate_graph(book["document"], validation_registry, runtime, stage="bootstrap")
        except (M4ControllerError, DAGValidationError) as exc:
            raise M4ControllerError("bootstrap_playbook_static", f"{book['name']}: {exc}", static=True) from exc
    return normalized, normalized_books, version_mapping


def _copy_fixture_policy(root: Path, fixture_root: Path) -> None:
    for relative in ("config/bank_metric_kind_policy.json", "config/bank_dev_kpi_policy.json"):
        path = fixture_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / relative, path)


def _execute_synthetic_playbook(document: Mapping[str, Any], generated_specs: list[dict[str, Any]],
                                scenario: str, fixture_root: Path, *, root: Path,
                                alpha_id: str = "fixture_pod_alpha",
                                beta_id: str = "fixture_pod_beta",
                                run_dir: Path, index: str) -> tuple[bool, dict[str, Any]]:
    _copy_fixture_policy(root, fixture_root)
    case = write_synthetic_public_case(fixture_root, scenario=scenario,
                                       alpha_id=alpha_id, beta_id=beta_id)
    query_budget = M4QueryBudget(run_dir / "synthetic_audit" / index)
    registry = make_m4_registry(case, generated_specs, root=fixture_root,
        audit_dir=run_dir / "synthetic_audit" / index,
        shared_query_budget=query_budget)
    runtime = build_m4_runtime(case.alert, root=root)
    try:
        verified = _validate_graph(document, registry, runtime, stage="bootstrap")
        source_node = next(node.node_id for node in verified.nodes if node.operator.external_input)
        result = SerialExecutor(registry, CacheStore(run_dir / "synthetic_cache" / index,
            enabled=False)).execute_verified(verified, runtime,
            {source_node: case.telemetry}, cache_enabled=False)
        if not isinstance(result.answer, RankedAnswer):
            return False, {"status": "wrong_return_contract"}
        validate_bank_answer(result.answer, case)
        top_ids = [row.entity.identifier.casefold() for row in result.answer.leads[:3]]
        expected = fixture_expected_top_component(alpha_id).casefold()
        passed = (expected in top_ids) if scenario == "positive" else not top_ids
        return passed, {"status": "passed" if passed else "fixture_behavior_mismatch",
            "scenario": scenario, "top_components": top_ids,
            "answer_digest": hashlib.sha256(canonical_json(result.answer)).hexdigest(),
            "query_budget": {"requests": query_budget.requests,
                             "rows": query_budget.rows, "bytes": query_budget.bytes}}
    except Exception as exc:
        return False, {"status": "fixture_execution_failed",
                       "error_type": type(exc).__name__, "error": str(exc)[:512]}


def _validate_playbooks(books: list[dict[str, Any]], generated_specs: list[dict[str, Any]],
                        seed_docs: Mapping[str, Any], root: Path, run_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seed_hashes = {playbook_graph_hash(doc) for doc in seed_docs.values()}
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen_hashes = set(seed_hashes)
    for book_index, book in enumerate(books):
        graph_hash = playbook_graph_hash(book["document"])
        if graph_hash in seen_hashes:
            rejected.append({"name": book["name"], "graph_sha256": graph_hash,
                             "reason": "isomorphic_to_protected_seed_or_prior_proposal"})
            continue
        outcomes = []
        all_passed = True
        for scenario, alpha_id, beta_id, fixture_name in (
                ("positive", "fixture_pod_alpha", "fixture_pod_beta", "positive"),
                ("positive", "counterfactual_component_theta", "counterfactual_component_delta", "renamed_positive"),
                ("no_change", "fixture_pod_alpha", "fixture_pod_beta", "no_change"),
                ("high_stable", "fixture_pod_alpha", "fixture_pod_beta", "high_stable"),
                ("missing", "fixture_pod_alpha", "fixture_pod_beta", "missing")):
            fixture_root = run_dir / "fixtures" / f"proposal_{book_index}_{fixture_name}"
            fixture_root.mkdir(parents=True, exist_ok=False)
            passed, outcome = _execute_synthetic_playbook(book["document"], generated_specs,
                scenario, fixture_root, root=root, alpha_id=alpha_id, beta_id=beta_id,
                run_dir=run_dir, index=f"proposal_{book_index}_{fixture_name}")
            outcomes.append(outcome)
            all_passed = all_passed and passed
        if all_passed:
            accepted.append({**book, "graph_sha256": graph_hash,
                             "synthetic_outcomes": outcomes})
            seen_hashes.add(graph_hash)
        else:
            rejected.append({"name": book["name"], "graph_sha256": graph_hash,
                "reason": "synthetic_behavior_gate_failed", "outcomes": outcomes})
    return accepted, rejected


def run_bootstrap(*, profile_path: Path, run_dir: Path,
                  client: LocalModelClient | None = None, root: Path = ROOT,
                  policy_path: Path | None = None) -> dict[str, Any]:
    """Generate and verify an immutable deployment library from schema-only context."""
    root = root.resolve(strict=True)
    policy_path = policy_path or root / "config/m4_runtime.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy.get("schema_version") != "e4-m4-runtime-policy/1":
        raise M4ControllerError("m4_policy_version", "unsupported M4 bootstrap policy")
    if run_dir.exists():
        raise FileExistsError(f"bootstrap output directory must be fresh: {run_dir}")
    run_dir.mkdir(parents=True)
    for directory in ("prompts", "attempts", "fixtures", "audit"):
        (run_dir / directory).mkdir()
    calls_path = run_dir / "model_calls.jsonl"
    calls_path.touch(exist_ok=False)
    status_path = run_dir / "bootstrap_status.json"
    _write_json(status_path, {"phase": "schema_context", "status": "running"})
    started = time.perf_counter()
    calls: list[dict[str, Any]] = []
    rejection_rows: list[dict[str, Any]] = []
    model_response_obtained = False
    try:
        profile_path = profile_path.resolve(strict=True)
        if not profile_path.is_relative_to(root):
            raise M4ControllerError("profile_path", "bootstrap profile must resolve inside project root")
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        base_registry = make_m4_registry(None, [], root=root,
            audit_dir=run_dir / "audit" / "schema_bootstrap")
        seed_library = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
        context = _schema_only_context(root, base_registry, seed_library, policy)
        prompt = _prompt(context)
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        bootstrap_model = client or M4BootstrapModelClient(profile,
            base_url=os.environ.get("E4_BASE_URL"),
            request_timeout=float(policy["serving"]["model_timeout_seconds"]))
        call_counter = 0
        repair_count = 0
        previous = ""
        errors: list[dict[str, Any]] = []
        valid_bundle = None
        normalized_specs: list[dict[str, Any]] = []
        generated_books: list[dict[str, Any]] = []
        version_mapping = {}
        while True:
            call_counter += 1
            current_prompt = prompt if repair_count == 0 else (prompt +
                "\n\nStatic schema/typed-contract errors only; repair the bundle shape/spec or graph references. "
                "No synthetic execution outcomes are available.\n" + json.dumps(errors, sort_keys=True) +
                "\n\nPrevious response:\n" + previous + "\nReturn corrected JSON only.")
            prompt_file = run_dir / "prompts" / f"bootstrap_call{call_counter}.txt"
            prompt_file.write_text(current_prompt, encoding="utf-8")
            call = bootstrap_model.complete(current_prompt, task_id="m4_schema_bootstrap", attempt=call_counter)
            row = call.as_dict()
            row["m4_stage"] = "bootstrap"
            row["m4_static_repair"] = repair_count > 0
            row["prompt_file"] = str(prompt_file.relative_to(run_dir))
            calls.append(row)
            _append_jsonl(calls_path, row)
            if call.error:
                raise M4ControllerError("bootstrap_model_failed", "bootstrap model call failed")
            if call.response_metadata.get("finish_reason") in {"length", "max_tokens"}:
                raise M4ControllerError("bootstrap_truncated", "bootstrap response ended at token limit")
            previous = call.content or ""
            if len(previous.encode("utf-8")) > int(policy["bootstrap"]["maximum_response_bytes"]):
                raise M4ControllerError("bootstrap_response_size", "bootstrap response exceeded the frozen byte limit")
            try:
                bundle, _fenced = parse_strict_json_object(previous)
                normalized_specs, generated_books, version_mapping = _validate_bundle(
                    bundle, context, base_registry, root, policy)
                valid_bundle = bundle
                model_response_obtained = True
                break
            except Exception as exc:
                error = exc.as_dict() if hasattr(exc, "as_dict") else {
                    "code": getattr(exc, "code", "bootstrap_static_invalid"), "message": str(exc)}
                errors.append(error)
                if repair_count >= int(policy["bootstrap"]["static_repair_limit"]):
                    rejection_rows.append({"phase": "static_bundle", "errors": errors,
                                           "raw_response": previous})
                    raise M4ControllerError("bootstrap_static_invalid", "bootstrap bundle remained statically invalid")
                repair_count += 1

        assert valid_bundle is not None
        _write_json(run_dir / "bootstrap_bundle.json", valid_bundle)
        (run_dir / "rejections.jsonl").touch(exist_ok=False)
        if not generated_books:
            _append_jsonl(run_dir / "rejections.jsonl", {"reason": "no_generated_playbooks_proposed"})
        _write_json(status_path, {"phase": "synthetic_validation", "status": "running"}, exclusive=False)

        # Validate the four protected seeds with synthetic positive and control
        # cases before creating any bootstrap snapshot.
        from .m4_controller import _materialize_seeds_for_registry
        seed_docs = _materialize_seeds_for_registry(seed_library, base_registry)
        seed_checks = []
        for seed_index, (seed_name, document) in enumerate(sorted(seed_docs.items())):
            outcomes = []
            for scenario, alpha_id, beta_id, fixture_name in (
                    ("positive", "fixture_pod_alpha", "fixture_pod_beta", "positive"),
                    ("positive", "counterfactual_component_theta", "counterfactual_component_delta", "renamed_positive"),
                    ("no_change", "fixture_pod_alpha", "fixture_pod_beta", "no_change"),
                    ("high_stable", "fixture_pod_alpha", "fixture_pod_beta", "high_stable"),
                    ("missing", "fixture_pod_alpha", "fixture_pod_beta", "missing")):
                fixture_root = run_dir / "fixtures" / f"seed_{seed_index}_{fixture_name}"
                fixture_root.mkdir()
                passed, outcome = _execute_synthetic_playbook(document, [], scenario,
                    fixture_root, root=root, alpha_id=alpha_id, beta_id=beta_id,
                    run_dir=run_dir, index=f"seed_{seed_index}_{fixture_name}")
                outcomes.append(outcome)
                if not passed:
                    rejection_rows.append({"name": seed_name, "scenario": fixture_name,
                                           "reason": "protected_seed_synthetic_fixture_failed",
                                           "outcome": outcome})
            seed_checks.append({"name": seed_name, "outcomes": outcomes,
                                "passed": all(item["status"] == "passed" for item in outcomes)})
        if not all(row["passed"] for row in seed_checks):
            raise M4ControllerError("protected_seed_fixture_failed", "a fixed Bank seed failed its synthetic behavior contract")

        accepted_books, rejected_books = _validate_playbooks(generated_books, normalized_specs,
            seed_docs, root, run_dir)
        rejection_rows.extend(rejected_books)
        accepted_hashes = {row["graph_sha256"] for row in accepted_books}
        reachable_spec_keys: set[str] = set()
        accepted_docs = []
        from .m4_library import _playbook_operator_keys
        normalized_by_key = {f"{spec['name']}@{spec['version']}": spec for spec in normalized_specs}
        for book in accepted_books:
            refs = _playbook_operator_keys(book["document"])
            reachable_spec_keys.update(refs & set(normalized_by_key))
            accepted_docs.append({"name": book["name"], "document": book["document"],
                "provenance": {**book["provenance"],
                    "synthetic_outcomes": book["synthetic_outcomes"]}})
        accepted_specs = [spec for key, spec in normalized_by_key.items() if key in reachable_spec_keys]
        for key in set(normalized_by_key) - reachable_spec_keys:
            rejection_rows.append({"operator": key, "reason": "not_reachable_from_a_semantically_verified_bootstrap_playbook"})
        for item in rejection_rows:
            _append_jsonl(run_dir / "rejections.jsonl", item)

        builtin_registry = make_m4_registry(None, [], root=root,
            audit_dir=run_dir / "audit" / "library_snapshot")
        library_provenance = {
            "model_alias": profile.get("model_alias"),
            "profile_sha256": sha256_file(profile_path),
            "prompt_sha256": prompt_hash,
            "bootstrap_calls": len(calls),
            "bootstrap_model_response_obtained": bool(calls and valid_bundle is not None),
            "m4_runtime_policy_sha256": sha256_file(policy_path),
            "implementation_fingerprint": _implementation_fingerprint(),
            "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
            "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
            "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json"),
            "bank_input_manifest_used": False,
            "schema_context_sha256": _canonical_hash(context),
            "static_deployment_context": "official candidate IDs and pinned declared CSV schema only; no case-observed entities or counts",
            "synthetic_validation": seed_checks,
            "verified_bootstrap_graph_hashes": sorted(accepted_hashes),
            "rejected_proposal_count": len(rejection_rows),
            "started_utc": datetime.now(timezone.utc).isoformat()}
        library_dir = run_dir / "library"
        snapshot = create_library_snapshot(seed_docs, accepted_specs, accepted_docs,
            library_provenance, library_dir, builtin_specs=builtin_registry.signatures(),
            total_operator_cap=int(policy["library"]["total_operator_cap"]),
            verified_playbook_graph_hashes=accepted_hashes)
        bootstrap_status = "complete_with_additions" if accepted_specs or accepted_docs else "complete_seed_only"
        result = {"schema_version": "e4-m4-bootstrap-result/1", "status": bootstrap_status,
            "model_response_received": model_response_obtained, "model_call_count": len(calls),
            "static_repairs": repair_count,
            "accepted_generated_operators": [f"{spec['name']}@{spec['version']}" for spec in accepted_specs],
            "accepted_generated_playbooks": [book["name"] for book in accepted_docs],
            "rejected_proposal_count": len(rejection_rows),
            "verified_seed_playbooks": seed_checks,
            "library_snapshot_id": snapshot.snapshot_id,
            "library_operator_count": len(snapshot.operators),
            "library_playbook_count": len(snapshot.playbooks),
            "library_path": str(library_dir),
            "library_snapshot_sha256": sha256_file(snapshot.path),
            "model_cost": _call_cost(calls),
            "wall_seconds": time.perf_counter() - started,
            "implementation_fingerprint": _implementation_fingerprint(),
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "bootstrap_result.json", result)
        _write_json(status_path, {"phase": "complete", "status": bootstrap_status}, exclusive=False)
        return {"result": result, "snapshot": snapshot, "run_dir": str(run_dir)}
    except Exception as exc:
        failure = {"schema_version": "e4-m4-bootstrap-result/1", "status": "failed",
            "model_response_received": model_response_obtained, "model_call_count": len(calls),
            "model_cost": _call_cost(calls), "error": {"code": getattr(exc, "code", "bootstrap_failed"),
                                                         "message": str(exc)},
            "wall_seconds": time.perf_counter() - started,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "bootstrap_result.json", failure)
        _write_json(status_path, {"phase": "failed", "status": "failed", "error": failure["error"]}, exclusive=False)
        if rejection_rows:
            path = run_dir / "rejections.jsonl"
            if not path.exists():
                path.touch()
            for row in rejection_rows:
                _append_jsonl(path, row)
        raise


def _call_cost(calls: list[Mapping[str, Any]]) -> dict[str, Any]:
    def sum_usage(key: str, alt: str) -> int | None:
        values = []
        for call in calls:
            usage = call.get("usage", {})
            value = usage.get(key, usage.get(alt)) if isinstance(usage, Mapping) else None
            if type(value) is not int:
                return None
            values.append(value)
        return sum(values)
    return {"call_count": len(calls), "prompt_tokens": sum_usage("prompt_tokens", "input_tokens"),
            "completion_tokens": sum_usage("completion_tokens", "output_tokens"),
            "elapsed_seconds": sum(float(row.get("elapsed_seconds", 0)) for row in calls),
            "generation_elapsed_seconds": sum(float(row.get("generation_elapsed_seconds", 0)) for row in calls)}
