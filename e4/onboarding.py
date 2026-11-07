"""Small public helpers for selecting an executable initial library."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .controller.library import (
    LibraryManager,
    _shipped_paper_operator_names,
    _shipped_paper_playbook_names,
)
from .registry import OperatorRegistry
from .types import ColumnSchema, DatasetSchema, TableSchema


def normalize_dataset_schema(schema: DatasetSchema | Mapping[str, Any] | None) -> DatasetSchema | None:
    """Convert a JSON schema description to the canonical typed schema."""
    if schema is None or isinstance(schema, DatasetSchema):
        return schema
    if not isinstance(schema, Mapping):
        raise TypeError(f"Expected DatasetSchema, mapping, or None; got {type(schema).__name__}")
    raw_tables = schema.get("tables", ())
    if not isinstance(raw_tables, (list, tuple)):
        raise TypeError("dataset schema 'tables' must be a sequence")
    tables = []
    for item in raw_tables:
        if not isinstance(item, Mapping):
            raise TypeError("Each dataset schema table must be a mapping")
        name = item.get("name")
        record_type = item.get("record_type")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Each dataset schema table needs a non-empty string name")
        if record_type not in {"metric", "span", "log"}:
            raise ValueError(f"Table '{name}' needs record_type 'metric', 'span', or 'log'")
        raw_columns = item.get("columns", ())
        if not isinstance(raw_columns, (list, tuple)):
            raise TypeError(f"Table '{name}' columns must be a sequence")
        columns_list = []
        for column in raw_columns:
            if isinstance(column, ColumnSchema):
                columns_list.append(column)
                continue
            if not isinstance(column, Mapping):
                raise TypeError(f"Table '{name}' columns must contain mappings")
            column_name = column.get("name")
            data_type = column.get("data_type")
            nullable = column.get("nullable", False)
            unit = column.get("unit")
            description = column.get("description", "")
            if not isinstance(column_name, str) or not column_name.strip():
                raise ValueError(f"Table '{name}' has a column without a non-empty string name")
            if not isinstance(data_type, str) or not data_type:
                raise ValueError(f"Column '{column_name}' in '{name}' needs a data_type string")
            if type(nullable) is not bool:
                raise TypeError(f"Column '{column_name}' nullable must be boolean")
            if unit is not None and not isinstance(unit, str):
                raise TypeError(f"Column '{column_name}' unit must be string or null")
            if not isinstance(description, str):
                raise TypeError(f"Column '{column_name}' description must be a string")
            columns_list.append(ColumnSchema(column_name, data_type, nullable, unit, description))
        raw_roles = item.get("entity_roles", ())
        if not isinstance(raw_roles, (list, tuple)) or any(
            not isinstance(role, str) or not role for role in raw_roles
        ):
            raise TypeError(f"Table '{name}' entity_roles must be a sequence of non-empty strings")
        table_description = item.get("description", "")
        if not isinstance(table_description, str):
            raise TypeError(f"Table '{name}' description must be a string")
        tables.append(TableSchema(
            name=name,
            record_type=record_type,
            columns=tuple(columns_list),
            entity_roles=tuple(raw_roles),
            description=table_description,
        ))
    time_unit = schema.get("time_unit", "ms_utc")
    description = schema.get("description", "")
    if not isinstance(time_unit, str) or not time_unit:
        raise ValueError("dataset schema time_unit must be a non-empty string")
    if not isinstance(description, str):
        raise TypeError("dataset schema description must be a string")
    return DatasetSchema(
        tables=tuple(tables),
        time_unit=time_unit,
        description=description,
    )


def _paper_module():
    try:
        from .starters import paper
    except ImportError as exc:
        raise RuntimeError("The shipped paper library is not available in this installation") from exc
    return paper


def paper_catalog() -> dict[str, Any]:
    """Return the complete shipped paper catalog, independent of schema availability."""
    catalog = _paper_module().paper_catalog()
    if not isinstance(catalog, Mapping):
        raise TypeError("e4.starters.paper.paper_catalog() must return a mapping")
    return dict(catalog)


def paper_availability(schema: DatasetSchema | Mapping[str, Any] | None) -> dict[str, Any]:
    """Return per-entry compatible and unavailable reasons for this schema."""
    normalized = normalize_dataset_schema(schema)
    result = _paper_module().paper_availability(normalized)
    if not isinstance(result, Mapping):
        raise TypeError("e4.starters.paper.paper_availability() must return a mapping")
    return dict(result)


def paper_reference_examples() -> list[dict[str, Any]]:
    """Return labeled paper examples that are informational, never registered."""
    examples = _paper_module().paper_reference_examples()
    return [dict(item) for item in examples]


def prepare_library(
    schema: DatasetSchema | Mapping[str, Any] | None,
    *,
    mode: str = "paper",
    library: LibraryManager | Path | str | None = None,
) -> LibraryManager:
    """Prepare a paper-compatible or explicitly empty clean-slate library.

    Existing libraries are loaded only when their persisted mode agrees with
    the requested mode. A clean-slate library is never merged with shipped
    paper capabilities.
    """
    if mode not in {"paper", "clean_slate"}:
        raise ValueError("mode must be 'paper' or 'clean_slate'")
    normalized = normalize_dataset_schema(schema)
    if library is not None:
        if isinstance(library, LibraryManager):
            manager = library
        elif isinstance(library, (Path, str)):
            manager = LibraryManager.load_from_dir(library, mode=mode)
        else:
            raise TypeError(f"Unsupported library value {type(library).__name__}")
        if manager.mode != mode:
            raise ValueError(
                f"Requested library mode '{mode}' conflicts with supplied library mode '{manager.mode}'"
            )
        if mode == "clean_slate":
            _reject_paper_contamination(manager)
        return manager

    if mode == "clean_slate":
        return LibraryManager(mode="clean_slate", origin="user-clean-slate")

    paper = _paper_module()
    registry = paper.build_paper_registry(normalized)
    playbooks = paper.get_paper_playbooks(normalized) or []
    if not isinstance(registry, OperatorRegistry):
        raise TypeError("build_paper_registry() must return an OperatorRegistry")
    return LibraryManager(
        registry=registry,
        playbooks=playbooks,
        mode="paper",
        origin="shipped-paper",
        default_operator_names=[operator.name for operator in registry.all_operators()],
        default_playbook_names=[book.name for book in playbooks],
    )


def _reject_paper_contamination(manager: LibraryManager) -> None:
    paper_ops = _shipped_paper_operator_names()
    paper_books = _shipped_paper_playbook_names()
    found_ops = sorted({op.name for op in manager.registry.all_operators()} & paper_ops)
    found_books = sorted({book.name for book in manager.list_playbooks()} & paper_books)
    manifest_ops = sorted(manager._default_operator_names & paper_ops)
    manifest_books = sorted(manager._default_playbook_names & paper_books)
    if manager.origin == "shipped-paper" or found_ops or found_books or manifest_ops or manifest_books:
        contaminated = found_ops + found_books + manifest_ops + manifest_books
        detail = ", ".join(contaminated) if contaminated else manager.origin
        raise ValueError(f"Clean-slate mode cannot load a paper library: {detail}")
