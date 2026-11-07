"""Deterministic application of structured library submissions."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from .controller.library import (
    LibraryManager,
    Playbook,
    _shipped_paper_operator_names,
    _shipped_paper_playbook_names,
)
from .controller.repair import StaticRepairController
from .controller.schemas import REQUIRED_SPEC_FIELDS
from .data.submissions import _plain_json, _reject_json_constant, _unique_object
from .dag import verify_dag
from .onboarding import normalize_dataset_schema
from .registry import OperatorSpec


class LibrarySubmissionError(ValueError):
    """Structured response or atomic application failure."""

    def __init__(self, stage: str, message: str, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.stage = stage
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"stage": self.stage, "message": str(self), "details": self.details}


def _exact_keys(value: Mapping[str, Any], allowed: set[str], stage: str) -> None:
    actual = set(value)
    if actual != allowed:
        raise LibrarySubmissionError(
            stage,
            "response object has missing or extra fields",
            {"missing": sorted(allowed - actual), "extra": sorted(actual - allowed)},
        )


def _nonempty_text(value: Any, field: str, stage: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LibrarySubmissionError(stage, f"{field} must be nonempty text")
    return value


def parse_library_submission(response: str | Mapping[str, Any]) -> dict[str, Any]:
    """Parse an exact additions/unchanged/blocked response without repairing it."""
    try:
        if isinstance(response, str):
            value = json.loads(
                response,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            )
        elif isinstance(response, Mapping):
            value = _plain_json(response)
        else:
            raise ValueError("response must be JSON text or a mapping")
        value = _plain_json(value)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise LibrarySubmissionError("submission", "response is not strict JSON", {"error": str(exc)[:200]}) from exc
    if not isinstance(value, dict):
        raise LibrarySubmissionError("submission", "response root must be a JSON object")

    status = value.get("status")
    if not isinstance(status, str):
        raise LibrarySubmissionError("submission", "status must be a string")
    if status in {"unchanged", "blocked"}:
        _exact_keys(value, {"status", "reason"}, "submission")
        _nonempty_text(value["reason"], "reason", "submission")
        return value
    if status != "additions":
        raise LibrarySubmissionError("submission", "status must be additions, unchanged, or blocked")

    if "proposed_operators" not in value or "playbooks" not in value:
        raise LibrarySubmissionError("submission", "additions require proposed_operators and playbooks")
    operators = value["proposed_operators"]
    books = value["playbooks"]
    if not isinstance(operators, list) or not isinstance(books, list):
        raise LibrarySubmissionError("submission", "proposed_operators and playbooks must be arrays")
    if operators:
        _exact_keys(value, {"status", "proposed_operators", "gap_record", "playbooks"}, "submission")
        gap = value["gap_record"]
        if not isinstance(gap, dict):
            raise LibrarySubmissionError("submission", "source additions require a gap_record object")
        _exact_keys(gap, {"gap_description", "proposed_operator_names"}, "gap_record")
        _nonempty_text(gap["gap_description"], "gap_description", "gap_record")
        names = gap["proposed_operator_names"]
        if not isinstance(names, list) or not names or any(not isinstance(name, str) or not name for name in names):
            raise LibrarySubmissionError("gap_record", "proposed_operator_names must be a nonempty list of names")
        if len(set(names)) != len(names):
            raise LibrarySubmissionError("gap_record", "proposed_operator_names must be unique")
        seen_operator_names: set[str] = set()
        for index, operator in enumerate(operators):
            if not isinstance(operator, Mapping) or set(operator) != set(REQUIRED_SPEC_FIELDS):
                raise LibrarySubmissionError("submission", "operator entry must use the exact typed proposal fields", {"index": index})
            name = operator.get("name")
            if not isinstance(name, str) or name in seen_operator_names:
                raise LibrarySubmissionError("submission", "operator names must be nonempty and unique", {"index": index})
            seen_operator_names.add(name)
    else:
        _exact_keys(value, {"status", "proposed_operators", "playbooks"}, "submission")
    if not books:
        raise LibrarySubmissionError("submission", "additions must contain at least one playbook")
    if len(books) > 3:
        raise LibrarySubmissionError("submission", "one additions response may contain at most three playbooks")
    seen_playbook_names: set[str] = set()
    for index, book in enumerate(books):
        name, _, _ = _check_book_document(book, index)
        if name in seen_playbook_names:
            raise LibrarySubmissionError("submission", "playbook names must be unique", {"name": name})
        seen_playbook_names.add(name)
    return value


_PLAYBOOK_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")
_PLAYBOOK_FIELDS = {"name", "description", "dag"}
_DAG_FIELDS = {"schema_version", "nodes", "return_node"}
_NODE_FIELDS = {"id", "operator", "version", "inputs", "params", "bindings"}


def _check_book_document(book: Any, index: int) -> tuple[str, str, dict[str, Any]]:
    if not isinstance(book, Mapping):
        raise LibrarySubmissionError("playbooks", "playbook entries must be objects", {"index": index})
    if set(book) != _PLAYBOOK_FIELDS:
        raise LibrarySubmissionError(
            "playbooks",
            "playbook entry has missing or extra fields",
            {"index": index, "missing": sorted(_PLAYBOOK_FIELDS - set(book)), "extra": sorted(set(book) - _PLAYBOOK_FIELDS)},
        )
    name = book["name"]
    if not isinstance(name, str) or not _PLAYBOOK_NAME.fullmatch(name):
        raise LibrarySubmissionError("playbooks", "playbook name must be a simple identifier", {"index": index})
    description = _nonempty_text(book["description"], "description", "playbooks")
    dag = book["dag"]
    if not isinstance(dag, Mapping) or not {"nodes", "return_node"}.issubset(dag) or set(dag) - _DAG_FIELDS:
        raise LibrarySubmissionError("playbooks", "dag must contain nodes/return_node and optional schema_version", {"name": name})
    nodes = dag.get("nodes")
    if not isinstance(nodes, list):
        raise LibrarySubmissionError("playbooks", "dag.nodes must be an array", {"name": name})
    for node_index, node in enumerate(nodes):
        if not isinstance(node, Mapping) or set(node) - _NODE_FIELDS:
            raise LibrarySubmissionError("playbooks", "DAG node has invalid or extra fields", {"name": name, "node_index": node_index})
    return name, description, dict(dag)


def _return_reachable_node_ids(dag: Mapping[str, Any]) -> set[str]:
    nodes = dag.get("nodes", [])
    by_id = {node.get("id"): node for node in nodes if isinstance(node, Mapping) and isinstance(node.get("id"), str)}
    pending = [dag.get("return_node")]
    reachable: set[str] = set()
    while pending:
        node_id = pending.pop()
        if not isinstance(node_id, str) or node_id in reachable or node_id not in by_id:
            continue
        reachable.add(node_id)
        inputs = by_id[node_id].get("inputs", {})
        if not isinstance(inputs, Mapping):
            continue
        for target in inputs.values():
            targets = [target] if isinstance(target, str) else target if isinstance(target, list) else []
            pending.extend(item for item in targets if isinstance(item, str) and item in by_id)
    return reachable


def _clone_library(base: LibraryManager, registry: Any | None = None, playbooks: list[Playbook] | None = None) -> LibraryManager:
    cloned = LibraryManager(
        registry=registry or base.registry.clone(),
        playbooks=playbooks if playbooks is not None else base.list_playbooks(),
        mode=base.mode,
        origin=base.origin,
        default_operator_names=tuple(base._default_operator_names),
        default_playbook_names=tuple(base._default_playbook_names),
    )
    cloned._admitted_operators = list(base._admitted_operators)
    cloned._admitted_playbooks = list(base._admitted_playbooks)
    return cloned


def _validate_mode(base: LibraryManager, status: str, operators: list[Any], books: list[Any]) -> None:
    if base.mode not in {"paper", "clean_slate"}:
        raise LibrarySubmissionError("mode", "bootstrap submissions require paper or clean_slate library mode")
    if base.mode == "clean_slate":
        paper_operators = _shipped_paper_operator_names()
        paper_books = _shipped_paper_playbook_names()
        if any(op.name in paper_operators for op in base.registry.all_operators()):
            raise LibrarySubmissionError("mode", "clean-slate base library contains shipped paper operators")
        if any(book.name in paper_books for book in base.list_playbooks()):
            raise LibrarySubmissionError("mode", "clean-slate base library contains shipped paper playbooks")
        if status == "unchanged":
            raise LibrarySubmissionError("mode", "clean-slate mode cannot finish with an unchanged empty library")
        if not operators or not books:
            raise LibrarySubmissionError("mode", "clean-slate additions require at least one new operator and playbook")


def apply_library_submission(
    response: str | Mapping[str, Any],
    base_library: LibraryManager,
    schema: Any,
) -> LibraryManager:
    """Atomically validate and apply one model response to a cloned library."""
    submission = parse_library_submission(response)
    status = submission["status"]
    if status == "blocked":
        raise LibrarySubmissionError("blocked", submission["reason"])
    operators = submission.get("proposed_operators", [])
    books = submission.get("playbooks", [])
    _validate_mode(base_library, status, operators, books)
    try:
        normalized_schema = normalize_dataset_schema(schema)
    except Exception as exc:
        raise LibrarySubmissionError("schema", "dataset schema is invalid", {"error_type": type(exc).__name__}) from exc
    if normalized_schema is None:
        raise LibrarySubmissionError("schema", "a validated dataset schema is required")
    if status == "unchanged":
        preview = _clone_library(base_library)
        if base_library.mode == "paper" and not preview.list_playbooks_for_schema(normalized_schema):
            raise LibrarySubmissionError("mode", "unchanged response cannot finalize an empty or unprepared paper library")
        return preview

    added_specs: list[OperatorSpec] = []
    if operators:
        from .controller.schemas import REQUIRED_SPEC_FIELDS

        for index, operator in enumerate(operators):
            if not isinstance(operator, Mapping) or set(operator) != set(REQUIRED_SPEC_FIELDS):
                raise LibrarySubmissionError("operators", "operator entry must use the exact typed proposal fields", {"index": index})
        bundle = {
            "gap_record": submission["gap_record"],
            "proposed_operators": operators,
        }
        try:
            errors, added_specs = StaticRepairController(max_repairs=0).validate_and_typecheck(
                bundle,
                dataset_schema=normalized_schema,
                existing_library=base_library,
            )
        except Exception as exc:
            raise LibrarySubmissionError(
                "operators",
                "operator proposal validation failed",
                {"error_type": type(exc).__name__},
            ) from exc
        if errors:
            raise LibrarySubmissionError("operators", "operator source or metadata validation failed", {"errors": errors})

    existing_names = {op.name for op in base_library.registry.all_operators()}
    existing_versions = {(op.name, op.version) for op in base_library.registry.all_operators()}
    new_names: set[str] = set()
    for spec in added_specs:
        if spec.name in existing_names or spec.name in new_names or (spec.name, spec.version) in existing_versions:
            raise LibrarySubmissionError("operators", "operator name/version replaces an existing library entry", {"name": spec.name, "version": spec.version})
        new_names.add(spec.name)

    book_docs = [_check_book_document(book, index) for index, book in enumerate(books)]
    existing_book_names = {book.name for book in base_library.list_playbooks()}
    seen_book_names: set[str] = set()
    for name, _, _ in book_docs:
        if name in existing_book_names or name in seen_book_names:
            raise LibrarySubmissionError("playbooks", "playbook name replaces or duplicates an existing entry", {"name": name})
        seen_book_names.add(name)

    cloned_registry = base_library.registry.clone()
    try:
        for spec in added_specs:
            cloned_registry.register(spec)
    except Exception as exc:
        raise LibrarySubmissionError("operators", "validated operator registration failed", {"error_type": type(exc).__name__}) from exc

    new_playbooks: list[Playbook] = []
    used_new_operators: set[str] = set()
    for name, description, dag in book_docs:
        try:
            verified = verify_dag(dag, cloned_registry, dataset_schema=normalized_schema)
        except Exception as exc:
            raise LibrarySubmissionError("playbooks", "DAG validation failed", {"name": name, "error_type": type(exc).__name__, "reason": str(exc)[:240]}) from exc
        if not verified.nodes:
            raise LibrarySubmissionError("playbooks", "DAG contains no executable nodes", {"name": name})
        return_spec = next(node.operator for node in verified.nodes if node.node_id == verified.return_node)
        if return_spec.output_type not in {"ScoredCandidates", "RankedAnswer"}:
            raise LibrarySubmissionError("playbooks", "DAG return node must produce ScoredCandidates or RankedAnswer", {"name": name, "output_type": return_spec.output_type})
        reachable = _return_reachable_node_ids(dag)
        node_ids = {node.node_id for node in verified.nodes}
        if reachable != node_ids:
            raise LibrarySubmissionError("playbooks", "DAG contains nodes disconnected from its return node", {"name": name, "unreachable_nodes": sorted(node_ids - reachable)})
        reachable_operators = {node.operator.name for node in verified.nodes if node.node_id in reachable}
        used_new_operators.update(reachable_operators & new_names)
        new_playbooks.append(Playbook(
            name=name,
            description=description,
            dag=verified.as_dict(),
            metadata={
                "submission_status": "additions",
                "submission_mode": base_library.mode,
                "provenance": "structured_adapter_submission",
            },
        ))

    unused = sorted(new_names - used_new_operators)
    if unused:
        raise LibrarySubmissionError("playbooks", "new operators must be reachable from an additions playbook return node", {"unused_operators": unused})

    result = _clone_library(base_library, registry=cloned_registry)
    for book in new_playbooks:
        result.add_playbook(book)
    return result
