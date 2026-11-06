"""Library management for storing, querying, and admitting playbooks and operators."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..dag import VerifiedDAG
from ..registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec


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
            "dag": dict(self.dag),
            "target_components": list(self.target_components),
            "metadata": dict(self.metadata),
        }


class LibraryManager:
    """Manages baseline and admitted playbooks, tracking library provenance."""

    def __init__(
        self,
        registry: OperatorRegistry | None = None,
        playbooks: Sequence[Playbook] | None = None,
    ):
        self.registry = registry or OperatorRegistry()
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
    ) -> LibraryManager:
        p = Path(directory)
        base = registry or base_registry
        mgr = cls(registry=base.clone() if base else None)
        op_file = p / "operators.json"
        if op_file.is_file():
            raw_ops = json.loads(op_file.read_text(encoding="utf-8"))
            for op_data in raw_ops:
                spec = OperatorSpec.from_dict(op_data)
                if mgr.registry.has(spec.name, spec.version):
                    existing = mgr.registry.require(spec.name, spec.version)
                    if len(existing.inputs) == len(spec.inputs) and existing.output_type == spec.output_type:
                        if existing.implementation and not spec.implementation:
                            spec = OperatorSpec(
                                name=spec.name,
                                version=spec.version,
                                inputs=spec.inputs,
                                output_type=spec.output_type,
                                parameters=spec.parameters,
                                family=spec.family,
                                description=spec.description,
                                input_requirements=spec.input_requirements,
                                implementation=existing.implementation,
                                source=spec.source or existing.source,
                            )
                            mgr.registry._operators[(spec.name, spec.version)] = spec
                else:
                    mgr.registry.register(spec)

        pb_file = p / "playbooks.json"
        if pb_file.is_file():
            raw = json.loads(pb_file.read_text(encoding="utf-8"))
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
