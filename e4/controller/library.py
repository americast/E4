"""Library management for storing, querying, and admitting playbooks and operators."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..dag import VerifiedDAG
from ..registry import OperatorRegistry, OperatorSpec, _jsonable


def _same_operator_contract(left: OperatorSpec, right: OperatorSpec) -> bool:
    return (
        left.name == right.name
        and left.version == right.version
        and left.inputs == right.inputs
        and left.output_type == right.output_type
        and dict(left.parameters) == dict(right.parameters)
        and left.family == right.family
        and dict(left.input_requirements) == dict(right.input_requirements)
    )


@dataclass(frozen=True, slots=True)
class Playbook:
    name: str
    description: str
    dag: Mapping[str, Any]
    target_components: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "dag": _jsonable(self.dag),
            "target_components": list(self.target_components),
            "metadata": _jsonable(self.metadata),
        }


class LibraryManager:
    """Manages baseline and admitted playbooks, tracking library provenance."""

    def __init__(
        self,
        registry: OperatorRegistry | None = None,
        playbooks: Sequence[Playbook] | None = None,
        *,
        mode: str = "custom",
        origin: str = "user",
        default_operator_names: Sequence[str] = (),
        default_playbook_names: Sequence[str] = (),
    ):
        if mode not in {"paper", "clean_slate", "custom"}:
            raise ValueError(f"Unsupported library mode {mode!r}")
        self.registry = registry or OperatorRegistry()
        self.mode = mode
        self.origin = origin
        self._default_operator_names = set(default_operator_names)
        self._default_playbook_names = set(default_playbook_names)
        self._playbooks: dict[str, Playbook] = {}
        if playbooks:
            for pb in playbooks:
                self._playbooks[pb.name] = pb
        self._admitted_operators: list[OperatorSpec] = []
        self._admitted_playbooks: list[Playbook] = []

    def add_playbook(self, playbook: Playbook | Mapping[str, Any]) -> None:
        if isinstance(playbook, Playbook):
            pb = playbook
        elif isinstance(playbook, Mapping):
            pb = Playbook(
                name=str(playbook["name"]),
                description=str(playbook.get("description", "")),
                dag=dict(playbook.get("dag", {})),
                target_components=tuple(playbook.get("target_components", ())),
                metadata=dict(playbook.get("metadata", {})),
            )
        else:
            raise TypeError(f"Expected Playbook or Mapping, got {type(playbook).__name__}")
        self._playbooks[pb.name] = pb

    def get_playbook(self, name: str) -> Playbook | None:
        return self._playbooks.get(name)

    def list_playbooks(self) -> list[Playbook]:
        return list(self._playbooks.values())

    def list_playbooks_for_schema(self, schema: Any = None) -> list[Playbook]:
        """Return per-schema books and retain all previously registered operators."""
        if self.mode != "paper" or schema is None:
            return self.list_playbooks()
        from ..onboarding import normalize_dataset_schema
        from ..starters.paper import build_paper_registry, get_paper_playbooks

        normalized = normalize_dataset_schema(schema)
        if normalized is None:
            return self.list_playbooks()

        compatible_registry = build_paper_registry(normalized)
        for op in compatible_registry.all_operators():
            if self.registry.has(op.name, op.version):
                if not _same_operator_contract(self.registry.require(op.name, op.version), op):
                    raise ValueError(f"Conflicting shipped paper operator contract for {op.qualified_name}")
            else:
                self.registry.register(op)
                self._default_operator_names.add(op.name)

        compatible_books = get_paper_playbooks(normalized)
        paper_default_names = set(self._default_playbook_names)
        paper_default_names.update(
            book.name for book in self._playbooks.values()
            if book.metadata.get("source") == "reconstructed-paper-starter-v1"
        )
        custom_books = [
            book for book in self._playbooks.values()
            if book.name not in paper_default_names
            and book.metadata.get("source") != "reconstructed-paper-starter-v1"
        ]
        return [*compatible_books, *custom_books]

    @property
    def manifest(self) -> dict[str, Any]:
        return {
            "format_version": 1,
            "mode": self.mode,
            "origin": self.origin,
            "default_operator_names": sorted(self._default_operator_names),
            "default_playbook_names": sorted(self._default_playbook_names),
            "operator_names": sorted(op.name for op in self.registry.all_operators()),
            "playbook_names": sorted(self._playbooks),
        }

    def _operators_for_schema(self, schema: Any = None, registry: OperatorRegistry | None = None) -> list[OperatorSpec]:
        source_registry = registry or self.registry
        operators = source_registry.all_operators()
        if self.mode != "paper" or schema is None:
            return operators
        from ..onboarding import normalize_dataset_schema
        from ..starters.paper import build_paper_registry

        normalized = normalize_dataset_schema(schema)
        compatible_names = {
            op.name for op in build_paper_registry(normalized).all_operators()
        } if normalized is not None else set()
        paper_names = self._default_operator_names | _shipped_paper_operator_names()
        return [
            op for op in operators
            if op.name not in paper_names or op.name in compatible_names
        ]

    def signatures_for_schema(
        self, schema: Any = None, registry: OperatorRegistry | None = None
    ) -> list[dict[str, Any]]:
        """Return source-free signatures active for a schema and optional candidate registry."""
        result = []
        for op in sorted(self._operators_for_schema(schema, registry), key=lambda item: (item.name, item.version)):
            data = op.as_dict()
            result.append({
                key: data[key]
                for key in (
                    "name", "version", "family", "description", "inputs", "output_type",
                    "parameters", "input_requirements",
                )
            })
        return result

    def as_dict(self, schema: Any = None) -> dict[str, Any]:
        """Return a prompt-safe active inventory; source code is intentionally omitted."""
        playbooks_to_show = self.list_playbooks_for_schema(schema) if schema is not None else self.list_playbooks()
        operators = self.signatures_for_schema(schema)
        playbooks = []
        for book in playbooks_to_show:
            book_doc = book.as_dict()
            dag = book_doc["dag"] if isinstance(book_doc["dag"], Mapping) else {}
            nodes = dag.get("nodes", [])
            playbooks.append({
                "name": book.name,
                "description": book.description,
                "nodes": [
                    {
                        "id": node.get("id"),
                        "operator": node.get("operator"),
                        "version": node.get("version", "1"),
                        "inputs": node.get("inputs", {}),
                        "params": node.get("params", {}),
                        "bindings": node.get("bindings", {}),
                    }
                    for node in nodes if isinstance(node, Mapping)
                ],
                "return_node": dag.get("return_node"),
            })
        return {
            "mode": self.mode,
            "origin": self.origin,
            "operators": operators,
            "playbooks": playbooks,
        }

    def admit_after_accepted(
        self,
        playbook_name: str,
        description: str,
        dag: VerifiedDAG,
        new_operators: Sequence[OperatorSpec] = (),
    ) -> None:
        """Admit confirmed operators and playbook into the active library."""
        for op in new_operators:
            if not self.registry.has(op.name, op.version):
                self.registry.register(op)
            self._admitted_operators.append(op)

        pb = Playbook(
            name=playbook_name,
            description=description,
            dag=dag.as_dict(),
            metadata={"status": "admitted", "operator_count": len(new_operators)},
        )
        self.add_playbook(pb)
        self._admitted_playbooks.append(pb)

    @classmethod
    def load_from_dir(
        cls,
        directory: Path | str,
        registry: OperatorRegistry | None = None,
        base_registry: OperatorRegistry | None = None,
        mode: str | None = None,
    ) -> LibraryManager:
        p = Path(directory)
        manifest_path = p / "library.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else {}
        stored_mode = manifest.get("mode", "custom")
        selected_mode = mode or stored_mode
        if selected_mode not in {"paper", "clean_slate", "custom"}:
            raise ValueError(f"Unsupported library mode {selected_mode!r}")
        if mode is not None and not manifest:
            raise ValueError(
                f"Requested library mode '{mode}' requires a persisted library.json manifest"
            )
        if mode is not None and mode != stored_mode:
            raise ValueError(
                f"Requested library mode '{mode}' conflicts with saved mode '{stored_mode}'"
            )
        base = registry or base_registry
        if selected_mode == "paper":
            from ..starters.paper import build_paper_registry

            paper_registry = build_paper_registry(None)
            if not isinstance(paper_registry, OperatorRegistry):
                raise TypeError("build_paper_registry() must return an OperatorRegistry")
            if base is None:
                base = paper_registry
            else:
                base = base.clone()
                for paper_op in paper_registry.all_operators():
                    if base.has(paper_op.name, paper_op.version):
                        registered = base.require(paper_op.name, paper_op.version)
                        if not _same_operator_contract(registered, paper_op):
                            raise ValueError(
                                f"Conflicting shipped paper operator contract for {paper_op.qualified_name}"
                            )
                    else:
                        base.register(paper_op)
        if selected_mode == "clean_slate" and base is not None:
            paper_names = _shipped_paper_operator_names()
            contaminated = [
                op.name for op in base.all_operators() if op.name in paper_names
            ]
            if contaminated:
                raise ValueError(
                    "Clean-slate mode cannot merge shipped paper operators from base registry: "
                    + ", ".join(sorted(contaminated))
                )
        mgr = cls(registry=base.clone() if base else None)
        mgr.mode = selected_mode
        mgr.origin = manifest.get("origin", "paper" if selected_mode == "paper" else ("clean-slate" if selected_mode == "clean_slate" else "user"))
        mgr._default_operator_names = set(manifest.get("default_operator_names", ()))
        mgr._default_playbook_names = set(manifest.get("default_playbook_names", ()))
        if selected_mode == "paper":
            mgr._default_operator_names.update(_shipped_paper_operator_names())
            mgr._default_playbook_names.update(_shipped_paper_playbook_names())
        if selected_mode == "clean_slate" and mgr.origin == "shipped-paper":
            raise ValueError("Clean-slate mode cannot load a library marked as shipped paper")
        if selected_mode == "clean_slate":
            known_paper_ops = _shipped_paper_operator_names()
            known_paper_books = _shipped_paper_playbook_names()
            if mgr._default_operator_names & known_paper_ops or mgr._default_playbook_names & known_paper_books:
                raise ValueError("Clean-slate manifest cannot reference shipped paper defaults")
        op_file = p / "operators.json"
        if op_file.is_file():
            raw_ops = json.loads(op_file.read_text(encoding="utf-8"))
            if selected_mode == "clean_slate":
                paper_names = _shipped_paper_operator_names()
                contaminated = [
                    item.get("name") for item in raw_ops
                    if isinstance(item, Mapping) and item.get("name") in paper_names
                ]
                if contaminated:
                    raise ValueError(
                        "Clean-slate library contains shipped paper operators: "
                        + ", ".join(sorted(contaminated))
                    )
            for op_data in raw_ops:
                spec = OperatorSpec.from_dict(op_data)
                if mgr.registry.has(spec.name, spec.version):
                    existing = mgr.registry.require(spec.name, spec.version)
                    if not _same_operator_contract(existing, spec):
                        raise ValueError(
                            f"Library operator {spec.qualified_name} conflicts with registered contract"
                        )
                    if existing.implementation and not spec.implementation:
                        if spec.source and existing.source and spec.source != existing.source:
                            raise ValueError(
                                f"Library source conflicts with shipped implementation for {spec.qualified_name}"
                            )
                        spec = replace(
                            spec,
                            implementation=existing.implementation,
                            source=spec.source or existing.source,
                        )
                        mgr.registry._operators[(spec.name, spec.version)] = spec
                else:
                    mgr.registry.register(spec)

        pb_file = p / "playbooks.json"
        if pb_file.is_file():
            raw = json.loads(pb_file.read_text(encoding="utf-8"))
            if selected_mode == "clean_slate":
                paper_names = _shipped_paper_playbook_names()
                contaminated = [
                    item.get("name") for item in raw
                    if isinstance(item, Mapping) and item.get("name") in paper_names
                ]
                if contaminated:
                    raise ValueError(
                        "Clean-slate library contains shipped paper playbooks: "
                        + ", ".join(sorted(contaminated))
                    )
            for item in raw:
                mgr.add_playbook(
                    Playbook(
                        name=item["name"],
                        description=item.get("description", ""),
                        dag=item["dag"],
                        target_components=tuple(item.get("target_components", ())),
                        metadata=item.get("metadata", {}),
                    )
                )
        return mgr

    def save_to_dir(self, directory: Path | str) -> None:
        p = Path(directory)
        p.mkdir(parents=True, exist_ok=True)
        op_file = p / "operators.json"
        ops_payload = [op.as_dict() for op in self.registry.all_operators()]
        op_file.write_text(json.dumps(ops_payload, indent=2), encoding="utf-8")
        pb_file = p / "playbooks.json"
        payload = [pb.as_dict() for pb in self._playbooks.values()]
        pb_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        (p / "library.json").write_text(json.dumps(self.manifest, indent=2), encoding="utf-8")


def _shipped_paper_operator_names() -> set[str]:
    """Get canonical shipped operator identities without inferring from prefixes."""
    try:
        from ..starters.paper import paper_catalog
    except ImportError:
        return set()
    catalog = paper_catalog()
    if isinstance(catalog, Mapping):
        operators = catalog.get("operators", ())
        return {
            op.get("name") for op in operators
            if isinstance(op, Mapping) and isinstance(op.get("name"), str)
        }
    return set()


def _shipped_paper_playbook_names() -> set[str]:
    """Get canonical shipped playbook identities without inferring from prefixes."""
    try:
        from ..starters.paper import paper_catalog
    except ImportError:
        return set()
    catalog = paper_catalog()
    if isinstance(catalog, Mapping):
        playbooks = catalog.get("playbooks", ())
        return {
            book.get("name") for book in playbooks
            if isinstance(book, Mapping) and isinstance(book.get("name"), str)
        }
    return set()
