"""Content-addressed, immutable M4 deployment-library snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from .m4_generated import validate_generated_spec
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec


SCHEMA_VERSION = "e4-m4-library/1"
TOTAL_OPERATOR_CAP = 19
LEGACY_CAPACITY_POLICY = "legacy-v1"
PAPER_STARTER_CAPACITY_POLICY = "reconstructed-paper-starter-v1"
_CAPACITY_POLICIES = {LEGACY_CAPACITY_POLICY, PAPER_STARTER_CAPACITY_POLICY}
_PAPER_UTILITY_SIGNATURES = {
    "case.telemetry": ("Telemetry", "utility"),
    "paper.input.prepare": ("PaperStarterInput", "paper_data_access"),
    "bank.public.query": ("Aggregate", "bank_public_query"),
    "bank.public.project": ("Aggregate", "bank_public_projection"),
    "bank.metrics.aggregate": ("Aggregate", "bank_aggregation"),
    "bank.traces.aggregate": ("Aggregate", "bank_aggregation"),
}
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
_SNAPSHOT_ID = re.compile(r"^[0-9a-f]{64}$")
_LOCK_NAME = ".m4-library.lock"
_HEAD_NAME = "CURRENT"


class LibraryError(ValueError):
    """Base error for invalid snapshots or library transitions."""


class LibraryConflictError(LibraryError):
    """An immutable identity collision or compare-and-swap conflict."""


class LibraryIntegrityError(LibraryError):
    """A snapshot's content hash or internal references are invalid."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LibraryError(f"library data must be finite JSON: {exc}") from exc


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(child) for child in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw(child) for child in value]
    return value


@dataclass(frozen=True)
class LibrarySnapshot:
    snapshot_id: str
    path: Path
    root: Path
    document: Mapping[str, Any]

    @property
    def parent_snapshot_id(self) -> str | None:
        return self.document.get("parent_snapshot_id")

    @property
    def operators(self) -> Mapping[str, Mapping[str, Any]]:
        return MappingProxyType({operator_key(item): item for item in self.document["operators"]})

    @property
    def playbooks(self) -> Mapping[str, Mapping[str, Any]]:
        return MappingProxyType({item["name"]: item for item in self.document["playbooks"]})


def operator_key(spec: Mapping[str, Any]) -> str:
    name, version = spec.get("name"), spec.get("version")
    if not isinstance(name, str) or not isinstance(version, str):
        raise LibraryError("operator name and version must be strings")
    return f"{name}@{version}"


def _is_paper_utility(row: Mapping[str, Any]) -> bool:
    """Recognize only the frozen builtin plumbing contracts as uncounted utilities.

    Operator families are model-authored for generated operators, so neither a
    `utility` family nor a utility marker can exempt a proposal from retention.
    """
    if row.get("kind") != "builtin":
        return False
    signature = row.get("signature", {})
    expected = _PAPER_UTILITY_SIGNATURES.get(row.get("name"))
    if expected is None:
        return False
    output_type, family = expected
    if signature.get("output_type") != output_type or signature.get("family") != family:
        return False
    return (row.get("name") != "case.telemetry" or
            signature.get("external_input") is True)


def _paper_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    utilities = [row for row in rows if _is_paper_utility(row)]
    diagnostics = [row for row in rows if not _is_paper_utility(row)]
    return {"diagnostic_operator_count": len(diagnostics),
            "utility_operator_count": len(utilities),
            "utility_operator_families": dict(sorted(Counter(
                str(row.get("signature", {}).get("family", "unspecified"))
                for row in utilities).items()))}


def _validate_identity(name: Any, version: Any) -> None:
    if not isinstance(name, str) or not _NAME.fullmatch(name) or name in {".", ".."} or ".." in name.split("."):
        raise LibraryError(f"unsafe library identity name: {name!r}")
    if not isinstance(version, str) or not _VERSION.fullmatch(version) or ".." in version:
        raise LibraryError(f"unsafe library identity version: {version!r}")


def _normalize_builtin(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze a public registry signature without trying to serialize code objects."""
    if not isinstance(spec, Mapping):
        raise LibraryError("builtin operator signatures must be objects")
    row = _thaw(spec)
    _validate_identity(row.get("name"), row.get("version"))
    if not isinstance(row.get("inputs"), list) or not isinstance(row.get("output_type"), str):
        raise LibraryError(f"builtin operator signature is incomplete: {row.get('name')}")
    if "implementation" in row or "source" in row:
        raise LibraryError("builtin registry signatures cannot carry executable implementation objects")
    signature_sha = _sha(_canonical(row))
    return {"name": row["name"], "version": row["version"], "kind": "builtin",
            "signature": row, "signature_sha256": signature_sha,
            "source_sha256": None, "source": None,
            "invoked_case_ids": [], "admission_provenance": {"source": "pinned_builtin_registry"}}


def _is_composite_spec(spec: Mapping[str, Any]) -> bool:
    return spec.get("authoring_profile") == "operator-dag-v1"


def _operator_registry_from_rows(rows: Sequence[Mapping[str, Any]]) -> OperatorRegistry:
    """Rebuild signature-only operators for snapshot DSL validation; no code is executed."""
    registry = OperatorRegistry()

    def unavailable_implementation(*_args, **_kwargs):
        raise LibraryIntegrityError("signature-only operator catalog cannot execute")

    for item in rows:
        signature = item.get("signature", {})
        raw_ports = signature.get("inputs", signature.get("input_ports", []))
        ports = tuple(PortSpec(row["name"], row.get("type", row.get("value_type")),
            row.get("required", True), row.get("variadic", False),
            row.get("minimum_items", 1), row.get("maximum_items")) for row in raw_ports)
        parameters = {}
        for name, row in signature.get("parameters", {}).items():
            parameters[name] = ParameterSpec(name=name,
                value_type=row.get("type", row.get("value_type")),
                required=row.get("required", False), nullable=row.get("nullable", False),
                default=row.get("default"), has_default=row.get("has_default", "default" in row),
                runtime_field=row.get("runtime_field"), estimator=None,
                description=row.get("description", ""), choices=tuple(row.get("choices", ())),
                constraints=row.get("constraints", {}))
        external = bool(signature.get("external_input", False))
        registry.register(OperatorSpec(name=item["name"], version=item["version"],
            inputs=ports, output_type=signature["output_type"], parameters=parameters,
            family=signature.get("family", "library"), cacheable=False,
            implementation=None if external else unavailable_implementation,
            description=signature.get("description", ""),
            external_input=external))
    return registry


def _composite_specs_from_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for item in rows:
        signature = item.get("signature", {})
        if signature.get("authoring_profile") != "operator-dag-v1":
            continue
        result.append({"name": signature["name"], "version": signature["version"],
            "authoring_profile": signature["authoring_profile"],
            "dependencies": list(signature.get("dependencies", [])),
            "program_effects": list(signature.get("program_effects", [])),
            "input_ports": signature["input_ports"], "output_type": signature["output_type"],
            "program": item["program"]})
    return result


def _composite_raw_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    parameters = {}
    for name, row in spec["parameters"].items():
        item = {key: _thaw(value) for key, value in row.items() if key != "has_default"}
        if not row.get("has_default"):
            item.pop("default", None)
        parameters[name] = item
    return {"name": spec["name"],
        "version": spec.get("declared_version", spec.get("version")),
        "description": spec["description"], "family": spec["family"],
        "input_ports": _thaw(spec["input_ports"]), "output_type": spec["output_type"],
        "parameters": parameters,
        "authoring_profile": spec["authoring_profile"], "program": _thaw(spec["program"])}


def _normalize_generated(spec: Mapping[str, Any], *, kind: str,
                         registry: OperatorRegistry | None = None,
                         composite_specs: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    if _is_composite_spec(spec):
        if registry is None:
            raise LibraryError("composite operator normalization requires the active registered catalog")
        from .m4_composite import normalize_composite_spec
        normalized = normalize_composite_spec(_composite_raw_spec(spec), registry,
            composite_specs=composite_specs)
        name, version = normalized["name"], normalized["version"]
        _validate_identity(name, version)
        signature = {key: _thaw(value) for key, value in normalized.items()
            if key != "program"}
        program = _thaw(normalized["program"])
        program_sha = normalized["program_sha256"]
        return {"name": name, "version": version, "kind": kind,
            "signature": signature, "signature_sha256": normalized["signature_sha256"],
            "source_sha256": program_sha, "source": None,
            "program": program, "program_sha256": program_sha,
            "invoked_case_ids": [], "admission_provenance": {}}
    if spec.get("authoring_profile") in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
        normalized = validate_generated_spec(spec)
        name, version = normalized["name"], normalized["version"]
        _validate_identity(name, version)
        program = _thaw(normalized["program"])
        program_sha = normalized["program_sha256"]
        return {"name": name, "version": version, "kind": kind,
            "signature": {key: _thaw(value) for key, value in normalized.items() if key != "program"},
            "signature_sha256": normalized["signature_sha256"],
            "source_sha256": program_sha, "source": None,
            "program": program, "program_sha256": program_sha,
            "invoked_case_ids": [], "admission_provenance": {}}
    normalized = validate_generated_spec(spec)
    name, version = normalized["name"], normalized["version"]
    _validate_identity(name, version)
    source = normalized["source"]
    source_hash = _sha(source.encode("utf-8"))
    if source_hash != normalized.get("source_sha256"):
        raise LibraryIntegrityError(f"generated source hash mismatch for {name}@{version}")
    return {"name": name, "version": version, "kind": kind,
            "signature": {key: _thaw(value) for key, value in normalized.items() if key != "source"},
            "signature_sha256": normalized["signature_sha256"],
            "source_sha256": source_hash, "source": source,
            "invoked_case_ids": [], "admission_provenance": {}}


def _operator_identity_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {key: _thaw(row.get(key)) for key in
        ("name", "version", "kind", "signature", "signature_sha256", "source_sha256", "source")}
    for optional in ("program", "program_sha256"):
        if optional in row:
            result[optional] = _thaw(row[optional])
    return result


def canonicalize_playbook_graph(document: Mapping[str, Any]) -> dict[str, Any]:
    """Canonicalize a rooted DAG by DFS, ignoring only node IDs and node-array order.

    Starting at the return node and traversing named ports in sorted order and
    variadic references in their original order preserves sharing and input
    positions without factorial graph-isomorphism search.
    """
    if (not isinstance(document, Mapping) or set(document) != {"schema_version", "nodes", "return"}
            or document.get("schema_version") != "e4-dag/1"):
        raise LibraryError("playbook document must use e4-dag/1")
    rows = document.get("nodes")
    returned = document.get("return")
    if not isinstance(rows, list) or not rows or not isinstance(returned, Mapping) or not isinstance(returned.get("node"), str):
        raise LibraryError("playbook requires nonempty nodes and a named return node")
    ids: dict[str, int] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or set(row) != {"id", "operator", "version", "inputs", "params", "bindings"}:
            raise LibraryError("playbook nodes must be objects")
        node_id = row.get("id")
        if not isinstance(node_id, str) or not node_id or node_id in ids:
            raise LibraryError("playbook node IDs must be unique nonempty strings")
        ids[node_id] = index
        if not isinstance(row.get("operator"), str) or not isinstance(row.get("version"), str):
            raise LibraryError("playbook operator/version must be strings")
        if not isinstance(row.get("inputs"), Mapping) or not isinstance(row.get("params"), Mapping) or not isinstance(row.get("bindings"), Mapping):
            raise LibraryError("playbook inputs, params, and bindings must be objects")
    if returned["node"] not in ids:
        raise LibraryError("playbook return node is missing")

    by_id = {row["id"]: row for row in rows}
    canonical_ids: dict[str, str] = {}
    ordered_ids: list[str] = []
    active: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in active:
            raise LibraryError("playbook graph contains a cycle")
        if node_id in canonical_ids:
            return
        canonical_ids[node_id] = f"n{len(ordered_ids)}"
        ordered_ids.append(node_id)
        active.add(node_id)
        row = by_id[node_id]
        for port, refs in sorted(row["inputs"].items(), key=lambda item: str(item[0])):
            if isinstance(refs, str):
                sequence = [refs]
            elif isinstance(refs, list) and all(isinstance(ref, str) for ref in refs):
                sequence = refs
            else:
                raise LibraryError(f"input {port!r} must be a node ID or node ID array")
            for ref in sequence:
                if ref not in by_id:
                    raise LibraryError(f"playbook input references unknown node {ref!r}")
                visit(ref)
        active.remove(node_id)

    visit(returned["node"])
    if len(ordered_ids) != len(rows):
        raise LibraryError("all playbook nodes must be reachable from the returned node")
    normalized = []
    for node_id in ordered_ids:
        row = by_id[node_id]
        inputs = {}
        for port, refs in sorted(row["inputs"].items(), key=lambda item: str(item[0])):
            if isinstance(refs, str):
                inputs[str(port)] = canonical_ids[refs]
            elif isinstance(refs, list) and all(isinstance(ref, str) for ref in refs):
                inputs[str(port)] = [canonical_ids[ref] for ref in refs]
            else:
                raise LibraryError(f"input {port!r} must be a node ID or node ID array")
        normalized.append({"id": canonical_ids[node_id], "operator": row["operator"],
            "version": row["version"], "inputs": inputs,
            "params": _thaw(row["params"]), "bindings": _thaw(row["bindings"])})
    return {"schema_version": "e4-dag/1", "nodes": normalized,
            "return": {"node": canonical_ids[returned["node"]]}}


def playbook_graph_hash(document: Mapping[str, Any]) -> str:
    return _sha(_canonical(canonicalize_playbook_graph(document)))


def _normal_playbook(name: str, document: Mapping[str, Any], kind: str,
                     provenance: Mapping[str, Any] | None = None) -> dict[str, Any]:
    _validate_identity(name, "1")
    canonical_graph = canonicalize_playbook_graph(document)
    return {"name": name, "kind": kind, "document": _thaw(document),
            "graph_sha256": _sha(_canonical(canonical_graph)),
            "canonical_graph": canonical_graph,
            "provenance": _thaw(provenance or {}), "invoked_case_ids": []}


def _root_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    lock = root / _LOCK_NAME
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise LibraryConflictError("another library transition holds the snapshot lock") from exc
    os.write(fd, f"pid={os.getpid()} time_ns={time.time_ns()}\n".encode())
    os.fsync(fd)
    return fd, lock


def _read_head(root: Path) -> str | None:
    path = root / _HEAD_NAME
    if not path.exists():
        return None
    value = path.read_text(encoding="ascii").strip()
    if not _SNAPSHOT_ID.fullmatch(value):
        raise LibraryIntegrityError("library CURRENT pointer is malformed")
    return value


def _load_snapshot_file(path: Path, root: Path | None = None) -> LibrarySnapshot:
    raw = path.read_bytes()
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LibraryIntegrityError(f"invalid snapshot JSON: {path}") from exc
    if not isinstance(envelope, dict) or set(envelope) != {"snapshot_id", "payload"}:
        raise LibraryIntegrityError("snapshot envelope fields are invalid")
    snapshot_id, payload = envelope["snapshot_id"], envelope["payload"]
    if not isinstance(snapshot_id, str) or not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise LibraryIntegrityError("snapshot ID is malformed")
    if _sha(_canonical(payload)) != snapshot_id:
        raise LibraryIntegrityError("snapshot content hash mismatch")
    _validate_payload(payload)
    return LibrarySnapshot(snapshot_id, path.resolve(), (root or path.parent).resolve(), _freeze(payload))


def _validate_payload(payload: Any) -> None:
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise LibraryIntegrityError("unsupported deployment-library schema")
    for field in ("operators", "playbooks", "lineage", "provenance", "policy", "immutable_operator_keys"):
        if field not in payload:
            raise LibraryIntegrityError(f"snapshot lacks {field}")
    if not isinstance(payload["operators"], list) or not isinstance(payload["playbooks"], list):
        raise LibraryIntegrityError("snapshot operators/playbooks must be arrays")
    if (not isinstance(payload["lineage"], list) or not isinstance(payload["provenance"], dict)
            or not isinstance(payload["policy"], dict)):
        raise LibraryIntegrityError("snapshot lineage, provenance, and policy have invalid types")
    try:
        validation_registry = _operator_registry_from_rows(payload["operators"])
        active_composites = _composite_specs_from_rows(payload["operators"])
    except Exception as exc:
        raise LibraryIntegrityError("operator signatures cannot form a registered catalog") from exc
    seen_ops, seen_books, seen_graphs = set(), set(), set()
    for item in payload["operators"]:
        key = operator_key(item)
        _validate_identity(item["name"], item["version"])
        if key in seen_ops:
            raise LibraryIntegrityError(f"duplicate operator identity {key}")
        seen_ops.add(key)
        if item.get("kind") not in {"builtin", "seed", "generated"}:
            raise LibraryIntegrityError(f"invalid operator kind for {key}")
        signature = item.get("signature")
        if not isinstance(signature, dict):
            raise LibraryIntegrityError(f"operator signature is invalid for {key}")
        if (signature.get("name") != item.get("name") or
                signature.get("version") != item.get("version")):
            raise LibraryIntegrityError(f"operator identity differs from its signature for {key}")
        if item.get("kind") == "builtin":
            if _sha(_canonical(signature)) != item.get("signature_sha256"):
                raise LibraryIntegrityError(f"operator signature hash mismatch for {key}")
        elif signature.get("signature_sha256") != item.get("signature_sha256"):
            raise LibraryIntegrityError(f"operator signature hash mismatch for {key}")
        if item.get("source") is not None:
            if not isinstance(item["source"], str) or _sha(item["source"].encode()) != item.get("source_sha256"):
                raise LibraryIntegrityError(f"operator source hash mismatch for {key}")
            if item.get("kind") == "builtin":
                raise LibraryIntegrityError(f"builtin {key} cannot embed generated source")
            signature = item.get("signature")
            try:
                parameter_spec = {name: {field: value for field, value in fields.items()
                                         if field != "has_default" and
                                         not (field == "default" and not fields.get("has_default"))}
                                  for name, fields in signature["parameters"].items()}
                raw_spec = {"name": signature["name"], "version": signature["declared_version"],
                    "description": signature["description"], "family": signature["family"],
                    "input_ports": signature["input_ports"], "output_type": signature["output_type"],
                    "parameters": parameter_spec, "source": item["source"]}
                # These fields are absent from legacy signatures by design. Keep
                # them omitted in that case so historical canonical hashes stay
                # byte-for-byte stable, while preserving the typed authoring
                # contract for newly generated operators.
                for optional_field in ("authoring_profile", "input_requirements"):
                    if optional_field in signature:
                        raw_spec[optional_field] = signature[optional_field]
                normalized = validate_generated_spec(raw_spec)
            except Exception as exc:
                raise LibraryIntegrityError(f"generated contract is invalid for {key}") from exc
            expected_signature = {field: value for field, value in normalized.items() if field != "source"}
            if _canonical(expected_signature) != _canonical(signature):
                raise LibraryIntegrityError(f"generated signature/source identity mismatch for {key}")
        elif item.get("kind") != "builtin" and item.get("program") is not None:
            if (signature.get("authoring_profile") not in {"operator-dag-v1", "typed-operator-ir-v1",
                    "typed-operator-ir-v2"} or
                    not isinstance(item.get("program_sha256"), str) or
                    item.get("program_sha256") != signature.get("program_sha256") or
                    item.get("source_sha256") != item.get("program_sha256") or
                    _sha(_canonical(item["program"])) != item.get("program_sha256")):
                raise LibraryIntegrityError(f"generated program hash/profile mismatch for {key}")
            try:
                if signature["authoring_profile"] in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
                    raw_spec = {field: signature[field] for field in (
                        "name", "description", "family", "input_ports", "output_type",
                        "parameters", "authoring_profile", "input_requirements")}
                    raw_spec["version"] = signature["declared_version"]
                    raw_spec["program"] = item["program"]
                    normalized = validate_generated_spec(raw_spec)
                else:
                    from .m4_composite import normalize_composite_spec
                    raw_spec = {field: signature[field] for field in (
                        "name", "description", "family", "input_ports", "output_type",
                        "parameters", "authoring_profile")}
                    raw_spec["version"] = signature["declared_version"]
                    raw_spec["program"] = item["program"]
                    normalized = normalize_composite_spec(raw_spec, validation_registry,
                        composite_specs=active_composites)
            except Exception as exc:
                raise LibraryIntegrityError(f"generated program contract is invalid for {key}") from exc
            expected_signature = {field: value for field, value in normalized.items()
                if field != "program"}
            if _canonical(expected_signature) != _canonical(signature):
                raise LibraryIntegrityError(f"composite signature/program identity mismatch for {key}")
        elif item.get("kind") != "builtin":
            raise LibraryIntegrityError(f"generated operator {key} lacks source or composite program")
        elif item.get("source_sha256") is not None:
            raise LibraryIntegrityError(f"builtin operator {key} cannot declare generated source bytes")
        invoked = item.get("invoked_case_ids")
        if (not isinstance(invoked, list) or any(not isinstance(case, str) or not case for case in invoked)
                or len(set(invoked)) != len(invoked)):
            raise LibraryIntegrityError(f"invalid invocation history for {key}")
    for book in payload["playbooks"]:
        name = book.get("name")
        _validate_identity(name, "1")
        if name in seen_books:
            raise LibraryIntegrityError(f"duplicate playbook name {name}")
        seen_books.add(name)
        expected_graph = playbook_graph_hash(book.get("document", {}))
        if expected_graph != book.get("graph_sha256"):
            raise LibraryIntegrityError(f"playbook graph hash mismatch: {name}")
        graph_hash = book.get("graph_sha256")
        if not isinstance(graph_hash, str) or not _SNAPSHOT_ID.fullmatch(graph_hash):
            raise LibraryIntegrityError(f"malformed playbook graph digest: {name}")
        if graph_hash in seen_graphs:
            raise LibraryIntegrityError(f"duplicate playbook graph: {name}")
        if book.get("kind") not in {"seed", "generated"}:
            raise LibraryIntegrityError(f"invalid playbook kind: {name}")
        seen_graphs.add(graph_hash)
        book_invoked = book.get("invoked_case_ids")
        if (not isinstance(book_invoked, list) or
                any(not isinstance(case, str) or not case for case in book_invoked) or
                len(set(book_invoked)) != len(book_invoked)):
            raise LibraryIntegrityError(f"invalid playbook invocation history: {name}")
    immutable = payload["immutable_operator_keys"]
    policy = payload["policy"]
    capacity_policy = policy.get("capacity_policy", LEGACY_CAPACITY_POLICY)
    if capacity_policy not in _CAPACITY_POLICIES:
        raise LibraryIntegrityError("snapshot declares an unsupported capacity policy")
    builtin_keys = {operator_key(row) for row in payload["operators"] if row["kind"] == "builtin"}
    expected_immutable = (builtin_keys if capacity_policy == LEGACY_CAPACITY_POLICY else
        {operator_key(row) for row in payload["operators"]
         if row["kind"] == "builtin" and _is_paper_utility(row)})
    if (not isinstance(immutable, list) or any(not isinstance(key, str) for key in immutable)
            or len(set(immutable)) != len(immutable)
            or set(immutable) != expected_immutable):
        raise LibraryIntegrityError("immutable operator set is invalid")
    if policy.get("total_operator_cap") != TOTAL_OPERATOR_CAP:
        raise LibraryIntegrityError("snapshot retention policy does not match frozen total operator cap")
    if capacity_policy == LEGACY_CAPACITY_POLICY:
        if len(payload["operators"]) > TOTAL_OPERATOR_CAP:
            raise LibraryIntegrityError("snapshot exceeds the frozen total operator cap")
    else:
        counts = _paper_counts(payload["operators"])
        if counts["diagnostic_operator_count"] > TOTAL_OPERATOR_CAP:
            raise LibraryIntegrityError("snapshot exceeds the frozen paper diagnostic operator cap")
        if policy.get("paper_counts") != counts:
            raise LibraryIntegrityError("paper diagnostic/utility counts differ from active operator entries")
    expected_policy_versions = {operator_key(row): row["version"] for row in payload["operators"]
                                if row["kind"] == "builtin"}
    if policy.get("builtin_policy_versions") != expected_policy_versions:
        raise LibraryIntegrityError("pinned builtin policy versions differ from builtin operator entries")
    if not _playbook_keys_resolve(payload["playbooks"], payload["operators"]):
        raise LibraryIntegrityError("snapshot playbook references an unavailable operator version")


def load_library_snapshot(path: Path | str) -> LibrarySnapshot:
    """Load and verify an immutable snapshot file or a store's CURRENT pointer."""
    path = Path(path)
    if path.is_dir():
        snapshot_id = _read_head(path)
        if snapshot_id is None:
            raise FileNotFoundError(f"no current library snapshot in {path}")
        return _load_snapshot_file(path / f"{snapshot_id}.json", path)
    return _load_snapshot_file(path, path.parent)


def snapshot_library(root: Path | str) -> LibrarySnapshot:
    return load_library_snapshot(Path(root))


def generated_specs_from_snapshot(snapshot: LibrarySnapshot) -> list[dict[str, Any]]:
    """Return verified generated specs as ordinary JSON data for registry creation."""
    specs = []
    for entry in snapshot.document["operators"]:
        if entry["kind"] == "builtin":
            continue
        signature = _thaw(entry["signature"])
        if signature.get("authoring_profile") in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
            spec = {field: signature[field] for field in ("name", "version", "declared_version",
                "description", "family", "input_ports", "output_type", "parameters",
                "authoring_profile", "input_requirements", "program_sha256", "source_sha256",
                "signature_sha256", "contract_hash", "kernel_abi_sha256", "interpreter_sha256", "policy_pins")
                if field in signature}
            spec["program"] = _thaw(entry["program"])
            normalized = validate_generated_spec(spec)
            if operator_key(normalized) != operator_key(entry):
                raise LibraryIntegrityError("typed operator IR identity changed while thawing snapshot")
            specs.append(normalized)
            continue
        if signature.get("authoring_profile") == "operator-dag-v1":
            spec = {key: signature[key] for key in (
                "name", "version", "declared_version", "description", "family",
                "input_ports", "output_type", "parameters", "authoring_profile",
                "program_sha256", "signature_sha256", "dependencies", "program_effects")
                if key in signature}
            spec["program"] = _thaw(entry["program"])
            if operator_key(spec) != operator_key(entry):
                raise LibraryIntegrityError("composite spec identity changed while thawing snapshot")
            specs.append(spec)
            continue
        spec = signature
        spec["source"] = entry["source"]
        normalized = validate_generated_spec(spec)
        if operator_key(normalized) != operator_key(entry):
            raise LibraryIntegrityError("generated spec identity changed while thawing snapshot")
        specs.append(normalized)
    return sorted(specs, key=operator_key)


def assert_library_registry_compatible(snapshot: LibrarySnapshot, registry: Any) -> None:
    """Check pinned builtin signatures/policy versions against the active registry."""
    from .m4_generated import assert_snapshot_registry_compatible
    assert_snapshot_registry_compatible(snapshot, registry)


def _write_snapshot(payload: dict[str, Any], root: Path, *, expected_current_id: str | None,
                    allow_initial: bool = False) -> LibrarySnapshot:
    _validate_payload(payload)
    snapshot_id = _sha(_canonical(payload))
    envelope = {"snapshot_id": snapshot_id, "payload": payload}
    bytes_out = _canonical(envelope) + b"\n"
    root.mkdir(parents=True, exist_ok=True)
    fd, lock = _root_lock(root)
    try:
        current = _read_head(root)
        if allow_initial:
            if current is not None and current != snapshot_id:
                raise LibraryConflictError("library already has a different CURRENT snapshot")
        elif current != expected_current_id:
            raise LibraryConflictError(f"library CURRENT changed: expected {expected_current_id}, found {current}")
        destination = root / f"{snapshot_id}.json"
        if destination.exists():
            existing = _load_snapshot_file(destination, root)
            if existing.snapshot_id != snapshot_id:
                raise LibraryIntegrityError("existing content-addressed snapshot is corrupt")
        else:
            tmp_fd, tmp_name = tempfile.mkstemp(prefix=".snapshot-", suffix=".tmp", dir=root)
            try:
                with os.fdopen(tmp_fd, "wb") as stream:
                    stream.write(bytes_out)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.chmod(tmp_name, 0o444)
                try:
                    os.link(tmp_name, destination)
                except FileExistsError:
                    _load_snapshot_file(destination, root)
            finally:
                Path(tmp_name).unlink(missing_ok=True)
        tmp_fd, tmp_name = tempfile.mkstemp(prefix=".current-", suffix=".tmp", dir=root)
        try:
            with os.fdopen(tmp_fd, "w", encoding="ascii") as stream:
                stream.write(snapshot_id + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, root / _HEAD_NAME)
        finally:
            Path(tmp_name).unlink(missing_ok=True)
    finally:
        os.close(fd)
        lock.unlink(missing_ok=True)
    return _load_snapshot_file(root / f"{snapshot_id}.json", root)


def _source_playbooks(values: Any, kind: str) -> list[dict[str, Any]]:
    if values is None:
        return []
    if isinstance(values, Mapping):
        rows = [{"name": name, "document": doc} for name, doc in values.items()]
    elif isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
        rows = list(values)
    else:
        raise LibraryError("playbooks must be a mapping or array")
    result = []
    for item in rows:
        if not isinstance(item, Mapping):
            raise LibraryError("playbook entries must be objects")
        result.append(_normal_playbook(item.get("name"), item.get("document", item.get("dag")), kind,
                                      item.get("provenance")))
    return result


def _source_playbooks_safely(values: Any, kind: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if values is None:
        return [], []
    if isinstance(values, Mapping):
        rows = [{"name": name, "document": doc} for name, doc in values.items()]
    elif isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
        rows = list(values)
    else:
        return [], [{"reason": "playbooks_not_mapping_or_array"}]
    good, dropped = [], []
    for index, item in enumerate(rows):
        try:
            good.extend(_source_playbooks([item], kind))
        except Exception as exc:
            name = item.get("name") if isinstance(item, Mapping) else None
            dropped.append({"name": name if isinstance(name, str) else None,
                            "index": index, "reason": type(exc).__name__})
    return good, dropped


def create_library_snapshot(seed_documents: Any, generated_specs: Sequence[Mapping[str, Any]],
                            generated_playbooks: Any, provenance: Mapping[str, Any],
                            directory: Path | str, parent: LibrarySnapshot | str | None = None, *,
                            builtin_specs: Sequence[Mapping[str, Any]] = (),
                            total_operator_cap: int = TOTAL_OPERATOR_CAP,
                            capacity_policy: str = LEGACY_CAPACITY_POLICY,
                            verified_playbook_graph_hashes: Iterable[str] = ()) -> LibrarySnapshot:
    """Create the initial immutable deployment snapshot.

    Schema-bootstrap generated operators share the total-19 retention pool with
    later evolving additions. Only registry builtins and seed playbooks are protected.
    """
    if capacity_policy not in _CAPACITY_POLICIES:
        raise LibraryError(f"unsupported capacity policy: {capacity_policy}")
    if total_operator_cap != TOTAL_OPERATOR_CAP:
        raise LibraryError(f"frozen total operator cap is {TOTAL_OPERATOR_CAP}")
    parent_snapshot = None
    if parent is not None:
        if isinstance(parent, LibrarySnapshot):
            parent_snapshot = parent
            parent_id = parent.snapshot_id
            root = Path(directory)
            if parent.root.resolve() != root.resolve():
                raise LibraryError("parent snapshot belongs to a different library store")
        else:
            parent_id = str(parent)
            root = Path(directory)
            if not _SNAPSHOT_ID.fullmatch(parent_id):
                raise LibraryError("parent must be a full snapshot content hash")
            parent_snapshot = _load_snapshot_file(root / f"{parent_id}.json", root)
    else:
        parent_id, root = None, Path(directory)
    builtin_rows = [_normalize_builtin(item) for item in builtin_specs]
    validation_rows = list(builtin_rows)
    if parent_snapshot is not None:
        by_key = {operator_key(row): row for row in validation_rows}
        by_key.update({operator_key(row): _thaw(row)
            for row in parent_snapshot.document["operators"]})
        validation_rows = list(by_key.values())
    candidate_legacy_rows = [_normalize_generated(item, kind="generated")
        for item in generated_specs if not _is_composite_spec(item)]
    validation_by_key = {operator_key(row): row for row in validation_rows}
    validation_by_key.update({operator_key(row): row for row in candidate_legacy_rows})
    validation_rows = list(validation_by_key.values())
    validation_registry = _operator_registry_from_rows(validation_rows)
    active_composites = _composite_specs_from_rows(validation_rows)
    seed_specs = list(candidate_legacy_rows)
    seed_specs.extend(_normalize_generated(item, kind="generated", registry=validation_registry,
        composite_specs=active_composites) for item in generated_specs if _is_composite_spec(item))
    operators = _merge_operator_rows(builtin_rows, seed_specs)
    if capacity_policy == LEGACY_CAPACITY_POLICY:
        if len(builtin_rows) > total_operator_cap:
            raise LibraryError("immutable builtin operator set alone exceeds cap 19")
        bootstrap_overflow = max(0, len(operators) - total_operator_cap)
        evictable_rows = [row for row in operators if row["kind"] != "builtin"]
    else:
        bootstrap_overflow = max(0, sum(not _is_paper_utility(row) for row in operators) - total_operator_cap)
        evictable_rows = [row for row in operators if not _is_paper_utility(row)]
    bootstrap_evicted = []
    if bootstrap_overflow:
        evictable_rows.sort(key=lambda row: (len(row["invoked_case_ids"]), operator_key(row)))
        bootstrap_evicted = [operator_key(row) for row in evictable_rows[:bootstrap_overflow]]
        if len(bootstrap_evicted) < bootstrap_overflow:
            raise LibraryError("bootstrap operator set exceeds cap 19 after protected utilities")
        evicted_set = set(bootstrap_evicted)
        operators = [row for row in operators if operator_key(row) not in evicted_set]
    if isinstance(seed_documents, Mapping):
        playbook_source = seed_documents.get("examples", seed_documents.get("playbooks", seed_documents))
    else:
        playbook_source = seed_documents
    seed_books, dropped_seed_books = _source_playbooks_safely(playbook_source, "seed")
    all_seed_books = list(seed_books)
    all_generated_books, dropped_generated_books = _source_playbooks_safely(generated_playbooks, "generated")
    generated_books = list(all_generated_books)
    verified_hashes = set(verified_playbook_graph_hashes)
    unverified_bootstrap = [book for book in generated_books if book["graph_sha256"] not in verified_hashes]
    generated_books = [book for book in generated_books if book["graph_sha256"] in verified_hashes]
    dropped_generated_books.extend({"name": book["name"], "reason": "graph_not_verified"}
                                   for book in unverified_bootstrap)
    available_operator_keys = {operator_key(row) for row in operators}
    seed_books = [book for book in seed_books if _playbook_operator_keys(book["document"]) <= available_operator_keys]
    generated_books = [book for book in generated_books
                       if _playbook_operator_keys(book["document"]) <= available_operator_keys]
    kept_seed_names = {book["name"] for book in seed_books}
    dropped_seed_books.extend({"name": book["name"], "reason": "operator_evicted"}
        for book in all_seed_books if book["name"] not in kept_seed_names)
    kept_generated_names = {book["name"] for book in generated_books}
    unverified_names = {book["name"] for book in unverified_bootstrap}
    dropped_generated_books.extend({"name": book["name"], "reason": "operator_evicted"}
        for book in all_generated_books
        if book["name"] not in kept_generated_names and book["name"] not in unverified_names)
    playbooks = _dedupe_playbooks(seed_books, generated_books)
    immutable_keys = sorted(operator_key(row) for row in builtin_rows if
        capacity_policy == LEGACY_CAPACITY_POLICY or _is_paper_utility(row))
    policy = {"total_operator_cap": total_operator_cap,
        "builtin_policy_versions": {operator_key(row): row["version"] for row in operators
                                     if row["kind"] == "builtin"},
        "cap_interpretation": ("19 total operators including immutable builtins/public query and bootstrap-generated operators; only builtins are immutable"
            if capacity_policy == LEGACY_CAPACITY_POLICY else
            "19 active non-utility diagnostic operators; protected data/query utilities are counted and reported separately"),
        "retention": ("evict least-used evolving generated operator; ties by qualified name; never evict immutable operators"
            if capacity_policy == LEGACY_CAPACITY_POLICY else
            "evict least distinct-case-used active diagnostics; ties by qualified name; protect utility operators and accepted dependency closure"),
        "playbook_retention": ("seed playbooks protected; generated playbooks removed when invalid or referencing evicted operators"
            if capacity_policy == LEGACY_CAPACITY_POLICY else
            "remove active playbooks when invalid or referencing unavailable operators; canonical origin templates remain in the separate frozen artifact"),
        "playbook_deduplication": "exact DAG isomorphism modulo node IDs only; parameters/bindings/ports/return preserved"}
    if capacity_policy != LEGACY_CAPACITY_POLICY:
        policy["capacity_policy"] = capacity_policy
        policy["paper_counts"] = _paper_counts(operators)
    creation_event = {"event": "snapshot_created", "parent_snapshot_id": parent_id,
        "bootstrap_operator_keys": sorted(operator_key(row) for row in seed_specs),
        "bootstrap_evicted_operator_keys": bootstrap_evicted,
        "seed_playbook_names": sorted(row["name"] for row in seed_books),
        "generated_playbook_names": sorted(row["name"] for row in generated_books),
        "dropped_seed_playbooks": dropped_seed_books,
        "dropped_generated_playbooks": dropped_generated_books}
    if capacity_policy != LEGACY_CAPACITY_POLICY and bootstrap_evicted:
        creation_event["bootstrap_eviction_reason"] = "least_distinct_case_usage_then_qualified_name"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "parent_snapshot_id": parent_id,
        "operators": sorted(operators, key=operator_key),
        "playbooks": sorted(playbooks, key=lambda row: row["name"]),
        "immutable_operator_keys": immutable_keys,
        "provenance": _thaw(provenance),
        "lineage": [creation_event],
        "policy": policy,
    }
    return _write_snapshot(payload, root, expected_current_id=parent_id,
                           allow_initial=parent is None)


def _merge_operator_rows(*groups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for group in groups:
        for item in group:
            row = _thaw(item)
            key = operator_key(row)
            prior = merged.get(key)
            if prior is not None:
                if _canonical(_operator_identity_payload(prior)) != _canonical(_operator_identity_payload(row)):
                    raise LibraryConflictError(f"operator identity collision at {key}")
                continue
            merged[key] = row
    return list(merged.values())


def _dedupe_playbooks(*groups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    by_graph: dict[str, str] = {}
    for group in groups:
        for item in group:
            row = _thaw(item)
            name, graph_hash = row["name"], row["graph_sha256"]
            if name in by_name:
                if by_name[name]["graph_sha256"] != graph_hash:
                    if row["kind"] == "generated":
                        continue
                    raise LibraryConflictError(f"seed playbook name collision: {name}")
                continue
            existing_name = by_graph.get(graph_hash)
            if existing_name is not None:
                # Seeds are supplied before generated playbooks, so a duplicate
                # generated graph cannot displace the protected seed identity.
                continue
            by_name[name] = row
            by_graph[graph_hash] = name
    return list(by_name.values())


def _usage_update(payload: dict[str, Any], case_id: str | None,
                  used_operator_keys: Iterable[str], used_playbook_names: Iterable[str]) -> None:
    if case_id is None:
        return
    if not isinstance(case_id, str) or not case_id.strip() or len(case_id) > 128:
        raise LibraryError("invocation case ID must be nonempty and bounded")
    if isinstance(used_operator_keys, str) or isinstance(used_playbook_names, str):
        raise LibraryError("usage identities must be iterable collections, not strings")
    operator_keys = set(used_operator_keys)
    playbook_names = set(used_playbook_names)
    if any(not isinstance(value, str) or not value for value in operator_keys | playbook_names):
        raise LibraryError("usage identities must be nonempty strings")
    operators = {operator_key(item): item for item in payload["operators"]}
    playbooks = {item["name"]: item for item in payload["playbooks"]}
    unknown_ops, unknown_books = operator_keys - set(operators), playbook_names - set(playbooks)
    if unknown_ops or unknown_books:
        raise LibraryError(f"invocation references unknown library entries: {sorted(unknown_ops | unknown_books)}")
    changed = False
    for key in operator_keys:
        seen = set(operators[key]["invoked_case_ids"])
        changed = changed or case_id not in seen
        seen.add(case_id)
        operators[key]["invoked_case_ids"] = sorted(seen)
    for name in playbook_names:
        seen = set(playbooks[name]["invoked_case_ids"])
        changed = changed or case_id not in seen
        seen.add(case_id)
        playbooks[name]["invoked_case_ids"] = sorted(seen)
    if changed:
        payload["lineage"].append({"event": "library_invocation", "case_id": case_id,
            "operator_keys": sorted(operator_keys), "playbook_names": sorted(playbook_names),
            "usage_counts_are_distinct_incident_ids": True})


def record_library_invocation(snapshot: LibrarySnapshot, *, invocation_case_id: str,
                              used_operator_keys: Iterable[str],
                              used_playbook_names: Iterable[str]) -> LibrarySnapshot:
    """Record distinct-case usage as a new immutable child snapshot."""
    if not isinstance(invocation_case_id, str) or not invocation_case_id.strip():
        raise LibraryError("invocation_case_id is required for usage accounting")
    if _read_head(snapshot.root) != snapshot.snapshot_id:
        raise LibraryConflictError("invocation parent is no longer CURRENT")
    payload = _thaw(snapshot.document)
    payload["parent_snapshot_id"] = snapshot.snapshot_id
    before = _sha(_canonical(payload))
    _usage_update(payload, invocation_case_id, used_operator_keys, used_playbook_names)
    if before == _sha(_canonical(payload)):
        return snapshot
    return _write_snapshot(payload, snapshot.root, expected_current_id=snapshot.snapshot_id)


def create_library_metadata_child(snapshot: LibrarySnapshot, *,
                                  provenance_updates: Mapping[str, Any],
                                  lineage_event: Mapping[str, Any]) -> LibrarySnapshot:
    """Create a CAS child snapshot changing metadata only; operator/playbook bytes stay exact."""
    if _read_head(snapshot.root) != snapshot.snapshot_id:
        raise LibraryConflictError("metadata child parent is no longer CURRENT")
    if not isinstance(provenance_updates, Mapping) or not isinstance(lineage_event, Mapping):
        raise LibraryError("metadata child updates must be JSON objects")
    payload = _thaw(snapshot.document)
    payload["parent_snapshot_id"] = snapshot.snapshot_id
    payload["provenance"].update(_thaw(provenance_updates))
    payload["lineage"].append(_thaw(lineage_event))
    child = _write_snapshot(payload, snapshot.root, expected_current_id=snapshot.snapshot_id)
    if (_canonical(_thaw(child.document["operators"])) != _canonical(_thaw(snapshot.document["operators"]))
            or _canonical(_thaw(child.document["playbooks"])) != _canonical(_thaw(snapshot.document["playbooks"]))):
        raise LibraryIntegrityError("metadata-only child changed library operator/playbook content")
    return child


def admit_after_accepted(snapshot: LibrarySnapshot, generated_specs: Sequence[Mapping[str, Any]],
                         playbooks: Any, provenance: Mapping[str, Any], *, accepted: bool,
                         verified_reachable_operator_keys: Iterable[str],
                         invocation_case_id: str | None = None,
                         used_operator_keys: Iterable[str] = (),
                         used_playbook_names: Iterable[str] = (),
                         verified_playbook_graph_hashes: Iterable[str] = ()) -> LibrarySnapshot:
    """Admit only generated operators reachable in an accepted Stage-3 DAG.

    Rejection returns the identical snapshot. The caller can separately record
    verified execution usage with ``record_library_invocation``; that creates a
    metadata-only child snapshot while keeping operator/playbook content fixed.
    """
    if type(accepted) is not bool:
        raise LibraryError("accepted feedback must be a literal bool")
    if not accepted:
        return snapshot
    if not isinstance(invocation_case_id, str) or not invocation_case_id.strip():
        raise LibraryError("accepted Stage-3 admission requires an invocation case ID")
    if _read_head(snapshot.root) != snapshot.snapshot_id:
        raise LibraryConflictError("admission parent is no longer CURRENT")
    payload = _thaw(snapshot.document)
    capacity_policy = payload["policy"].get("capacity_policy", LEGACY_CAPACITY_POLICY)
    if capacity_policy == PAPER_STARTER_CAPACITY_POLICY and len(generated_specs) > 3:
        raise LibraryError("paper-starter admissions accept at most three transient operator proposals")
    payload["parent_snapshot_id"] = snapshot.snapshot_id
    validation_rows = payload["operators"]
    validation_registry = _operator_registry_from_rows(validation_rows)
    active_composites = _composite_specs_from_rows(validation_rows)
    if isinstance(verified_reachable_operator_keys, str) or isinstance(verified_playbook_graph_hashes, str):
        raise LibraryError("verified reachability proofs must be collections, not strings")
    reachable_values = list(verified_reachable_operator_keys)
    if any(not isinstance(key, str) or not key for key in reachable_values):
        raise LibraryError("reachable operator keys must be nonempty strings")
    reachable = set(reachable_values)
    candidate_specs = []
    rejected_specs = []
    for index, item in enumerate(generated_specs):
        try:
            normalized = _normalize_generated(item, kind="generated",
                registry=validation_registry, composite_specs=active_composites)
        except Exception as exc:
            rejected_specs.append({"index": index, "reason": type(exc).__name__})
            continue
        if operator_key(normalized) in reachable:
            candidate_specs.append(normalized)
        else:
            rejected_specs.append({"name": normalized["name"], "version": normalized["version"],
                                   "reason": "operator_not_reachable"})
    for row in candidate_specs:
        prior = snapshot.operators.get(operator_key(row))
        if prior and _canonical(_operator_identity_payload(prior)) != _canonical(_operator_identity_payload(row)):
            raise LibraryConflictError(f"generated operator source/signature collision at {operator_key(row)}")
    for row in candidate_specs:
        row["admission_provenance"] = _thaw(provenance)
    operators = _merge_operator_rows(payload["operators"], candidate_specs)
    # Only verified generated playbook graph hashes are eligible for admission.
    verified_graph_values = list(verified_playbook_graph_hashes)
    if any(not isinstance(digest, str) or not _SNAPSHOT_ID.fullmatch(digest) for digest in verified_graph_values):
        raise LibraryError("verified playbook hashes must be SHA-256 hex digests")
    verified_graphs = set(verified_graph_values)
    parsed_books, invalid_playbooks = _source_playbooks_safely(playbooks, "generated")
    candidate_books = []
    for book in parsed_books:
        if book["graph_sha256"] in verified_graphs:
            candidate_books.append(book)
        else:
            invalid_playbooks.append({"name": book["name"], "reason": "graph_not_verified"})
    available = {operator_key(row) for row in operators}
    filtered_books = []
    for book in candidate_books:
        refs = _playbook_operator_keys(book["document"])
        if refs <= available:
            filtered_books.append(book)
        else:
            invalid_playbooks.append({"name": book["name"], "reason": "operator_unavailable"})
    existing_book_names = {row["name"] for row in payload["playbooks"]}
    books = _dedupe_playbooks(payload["playbooks"], filtered_books)
    payload["operators"] = operators
    payload["playbooks"] = books
    _usage_update(payload, invocation_case_id, used_operator_keys, used_playbook_names)
    operators, books = payload["operators"], payload["playbooks"]
    protected = set(payload["immutable_operator_keys"])
    accepted_closure: set[str] = set()
    if capacity_policy == PAPER_STARTER_CAPACITY_POLICY:
        by_key = {operator_key(row): row for row in operators}
        pending = set(reachable)
        for book in filtered_books:
            pending.update(_playbook_operator_keys(book["document"]))
        while pending:
            key = pending.pop()
            if key in accepted_closure or key not in by_key:
                continue
            accepted_closure.add(key)
            row = by_key[key]
            signature = row.get("signature", {})
            if signature.get("authoring_profile") == "operator-dag-v1":
                program = row.get("program", {})
                for node in program.get("nodes", []) if isinstance(program, Mapping) else []:
                    if isinstance(node, Mapping) and isinstance(node.get("operator"), str) and isinstance(node.get("version"), str):
                        pending.add(f"{node['operator']}@{node['version']}")
            for dependency in signature.get("dependencies", []):
                if isinstance(dependency, str):
                    pending.add(dependency)
                elif isinstance(dependency, Mapping) and isinstance(dependency.get("name"), str) and isinstance(dependency.get("version"), str):
                    pending.add(f"{dependency['name']}@{dependency['version']}")
        protected.update(accepted_closure)
        generated = [row for row in operators if not _is_paper_utility(row)]
    else:
        generated = [row for row in operators if operator_key(row) not in protected]
    cap = payload["policy"]["total_operator_cap"]
    overflow = ((sum(not _is_paper_utility(row) for row in operators) - cap)
                if capacity_policy == PAPER_STARTER_CAPACITY_POLICY else len(operators) - cap)
    evicted: list[str] = []
    dependency_invalidated: list[str] = []
    if overflow > 0:
        if capacity_policy == PAPER_STARTER_CAPACITY_POLICY:
            generated = [row for row in generated if operator_key(row) not in protected]
        generated.sort(key=lambda row: (len(row["invoked_case_ids"]), operator_key(row)))
        evicted = [operator_key(row) for row in generated[:overflow]]
        if len(evicted) < overflow:
            raise LibraryError("accepted dependency closure and protected utilities exceed the paper diagnostic cap")
        evicted_set = set(evicted)
        operators = [row for row in operators if operator_key(row) not in evicted_set]
        # A retained composite is invalid if any generated operator called by
        # its body was evicted. Propagate that invalidation through nested
        # composites before retaining playbooks, while keeping the existing
        # least-used operator eviction order and total capacity unchanged.
        changed = True
        while changed:
            changed = False
            for row in list(operators):
                signature = row.get("signature", {})
                if (row.get("kind") == "builtin" or
                        signature.get("authoring_profile") != "operator-dag-v1"):
                    continue
                body = row.get("program", {})
                called = {f"{node.get('operator')}@{node.get('version')}"
                    for node in body.get("nodes", []) if isinstance(node, Mapping)}
                missing_dependencies = called & evicted_set
                if missing_dependencies:
                    key = operator_key(row)
                    operators.remove(row)
                    evicted_set.add(key)
                    dependency_invalidated.append(key)
                    changed = True
        retained = {operator_key(row) for row in operators}
        kept_books = []
        for book in books:
            if ((capacity_policy == LEGACY_CAPACITY_POLICY and book["kind"] == "seed") or
                    _playbook_operator_keys(book["document"]) <= retained):
                kept_books.append(book)
            else:
                invalid_playbooks.append({"name": book["name"], "reason": "operator_evicted"})
        books = kept_books
    # Ensure schema-bootstrap seed playbooks never disappear through a name or
    # graph collision in an evolving update.
    seed_names = {book["name"] for book in payload["playbooks"] if book["kind"] == "seed"}
    if capacity_policy == LEGACY_CAPACITY_POLICY and not seed_names <= {book["name"] for book in books}:
        raise LibraryIntegrityError("admission attempted to remove a protected seed playbook")
    admitted_playbook_names = {book["name"] for book in books} - existing_book_names
    candidate_playbook_names = {book["name"] for book in filtered_books}
    invalid_playbook_names = {item.get("name") for item in invalid_playbooks if item.get("name")}
    payload["operators"] = sorted(operators, key=operator_key)
    payload["playbooks"] = sorted(books, key=lambda row: row["name"])
    payload["lineage"].append({"event": "accepted_stage3_admission",
        "case_id": invocation_case_id,
        "accepted": True,
        "provenance": _thaw(provenance),
        "admitted_operator_keys": sorted(operator_key(row) for row in candidate_specs),
        "rejected_operator_proposals": rejected_specs,
        "admitted_playbook_names": sorted(admitted_playbook_names),
        "deduplicated_playbook_names": sorted(candidate_playbook_names - admitted_playbook_names - invalid_playbook_names),
        "dropped_invalid_playbooks": invalid_playbooks,
        "verified_reachable_operator_keys": sorted(reachable),
        "verified_playbook_graph_hashes": sorted(verified_graphs),
        "evicted_operator_keys": evicted,
        "dependency_invalidated_composite_keys": sorted(dependency_invalidated),
        "retention_cap_total": cap,
        "retention_policy": capacity_policy,
        "accepted_dependency_closure": sorted(accepted_closure),
        "eviction_reason": "least_distinct_case_usage_then_qualified_name" if evicted and capacity_policy == PAPER_STARTER_CAPACITY_POLICY else None})
    if capacity_policy == PAPER_STARTER_CAPACITY_POLICY:
        payload["policy"]["paper_counts"] = _paper_counts(payload["operators"])
        payload["policy"]["builtin_policy_versions"] = {
            operator_key(row): row["version"] for row in payload["operators"]
            if row["kind"] == "builtin"}
    return _write_snapshot(payload, snapshot.root, expected_current_id=snapshot.snapshot_id)


def _playbook_operator_keys(document: Mapping[str, Any]) -> set[str]:
    rows = document.get("nodes", [])
    return {f"{row['operator']}@{row['version']}" for row in rows
            if isinstance(row, Mapping) and isinstance(row.get("operator"), str)
            and isinstance(row.get("version"), str)}


def rollback_to_snapshot(root: Path | str, snapshot_id: str, *, expected_current_id: str) -> LibrarySnapshot:
    """Switch CURRENT to an existing verified immutable snapshot without rewriting it."""
    root = Path(root)
    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise LibraryError("rollback target must be a full content hash")
    target = _load_snapshot_file(root / f"{snapshot_id}.json", root)
    fd, lock = _root_lock(root)
    try:
        current = _read_head(root)
        if current != expected_current_id:
            raise LibraryConflictError(f"rollback parent changed: expected {expected_current_id}, found {current}")
        tmp_fd, tmp_name = tempfile.mkstemp(prefix=".current-", suffix=".tmp", dir=root)
        try:
            with os.fdopen(tmp_fd, "w", encoding="ascii") as stream:
                stream.write(snapshot_id + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, root / _HEAD_NAME)
        finally:
            Path(tmp_name).unlink(missing_ok=True)
        log = root / "rollback_events.jsonl"
        with log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"from": current, "to": snapshot_id,
                "event": "rollback_pointer_changed"}, sort_keys=True, separators=(",", ":")) + "\n")
    finally:
        os.close(fd)
        lock.unlink(missing_ok=True)
    return target


def _playbook_keys_resolve(books: Sequence[Mapping[str, Any]], operators: Sequence[Mapping[str, Any]]) -> bool:
    available = {operator_key(item) for item in operators}
    return all(_playbook_operator_keys(row["document"]) <= available for row in books)

