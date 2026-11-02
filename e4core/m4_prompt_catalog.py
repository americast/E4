"""Lossless columnar prompt projection for compact registered signatures."""
from __future__ import annotations

import json
import hashlib
from collections import defaultdict
from typing import Any, Mapping


COLUMNAR_SCHEMA = "e4-paper-signature-contract-columnar/1"
SOURCE_SCHEMA = "e4-paper-signature-contract-refs/2"
EVOLVE_SOURCE_SCHEMA = "e4-paper-evolve-operator-catalog/2"
_INPUT_COLUMNS = ("name", "contract_ref")


class PromptCatalogError(ValueError):
    pass


def _json_copy(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False,
            separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PromptCatalogError("signature catalog must contain finite JSON data") from exc


def pack_registered_signature_catalog(catalog: Mapping[str, Any], *,
        omit_parameter_descriptions: bool = False) -> dict[str, Any]:
    """Pack schema/2 signatures; optionally omit only nonempty param prose.

    Operator rows use a shared ordered column list plus sparse per-row extras.
    Input and parameter references are positional name/ref pairs. Parameter
    contracts are grouped only when their exact key sets match; a shape table
    stores its field order once and preserves every value, including missing,
    null, false, and numeric zero. When requested, parameter descriptions are
    omitted with one aggregate map count/hash and a full-host-catalog reference.
    """
    source = _json_copy(catalog)
    if isinstance(source, dict) and source.get("schema") == EVOLVE_SOURCE_SCHEMA:
        return _pack_evolve_catalog(source)
    if (not isinstance(source, dict) or source.get("schema") != SOURCE_SCHEMA or
            not isinstance(source.get("operators"), list) or
            not isinstance(source.get("input_contracts"), Mapping) or
            not isinstance(source.get("parameter_contracts"), Mapping) or
            not isinstance(source.get("input_requirement_contracts"), Mapping)):
        raise PromptCatalogError("expected e4-paper-signature-contract-refs/2 catalog")
    description_omission = None
    if omit_parameter_descriptions:
        if "parameter_description_omission" in source:
            raise PromptCatalogError("source catalog already has parameter-description omission metadata")
        omitted = {}
        for ref, raw_contract in source["parameter_contracts"].items():
            if not isinstance(raw_contract, dict):
                raise PromptCatalogError("parameter contracts must be objects")
            description = raw_contract.get("description")
            if isinstance(description, str) and description:
                omitted[ref] = description
                raw_contract.pop("description")
        description_omission = {
            "schema_version": "e4-paper-parameter-description-omission/1",
            "omitted_parameter_description_count": len(omitted),
            "omitted_parameter_description_map_sha256": hashlib.sha256(json.dumps(
                omitted, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                allow_nan=False).encode("utf-8")).hexdigest(),
            "full_host_catalog_ref": "trusted_registered_operator_signatures.parameter_contracts",
        }
    operator_rows = source["operators"]
    for index, row in enumerate(operator_rows):
        if (not isinstance(row, Mapping) or not isinstance(row.get("name"), str) or
                not isinstance(row.get("version"), str) or
                not isinstance(row.get("inputs"), list) or
                not isinstance(row.get("parameters"), Mapping)):
            raise PromptCatalogError(f"operator row {index} has an invalid signed shape")
        for port_index, port in enumerate(row["inputs"]):
            if (not isinstance(port, Mapping) or set(port) != set(_INPUT_COLUMNS) or
                    not isinstance(port.get("name"), str) or
                    not isinstance(port.get("contract_ref"), str)):
                raise PromptCatalogError(f"operator row {index} input {port_index} is malformed")
        if any(not isinstance(name, str) or not isinstance(ref, str)
                for name, ref in row["parameters"].items()):
            raise PromptCatalogError(f"operator row {index} parameter refs are malformed")

    operator_special = {"inputs", "parameters"}
    common_columns = [key for key in operator_rows[0]
        if key not in operator_special and all(key in row for row in operator_rows)] if operator_rows else []
    # Keep a stable familiar ordering for the primary signature fields; any
    # additional all-row fields follow in their source order.
    preferred = ("name", "version", "output_type", "family", "description")
    columns = [key for key in preferred if key in common_columns]
    columns.extend(key for key in common_columns if key not in columns)
    rows = [[row[key] for key in columns] for row in operator_rows]
    extras = {str(index): {key: value for key, value in row.items()
        if key not in set(columns) | operator_special}
        for index, row in enumerate(operator_rows)
        if any(key not in set(columns) | operator_special for key in row)}
    inputs_by_operator = [None if "inputs" not in row else [
        [port[column] for column in _INPUT_COLUMNS] for port in row["inputs"]]
        for row in operator_rows]
    parameters_by_operator = [None if "parameters" not in row else [
        [name, ref] for name, ref in row["parameters"].items()]
        for row in operator_rows]

    contract_rows: Mapping[str, Any] = source["parameter_contracts"]
    choice_json_counts: dict[str, int] = defaultdict(int)
    choice_payloads: dict[str, Any] = {}
    for contract in contract_rows.values():
        if isinstance(contract, Mapping) and isinstance(contract.get("choices"), list):
            choice_json, choice_payload = _json_copy(contract["choices"]), None
            # Serialize once here to get the same stable identity used by the
            # grouped contract encoder below.
            choice_json = json.dumps(choice_json, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False)
            choice_payload = json.loads(choice_json)
            choice_json_counts[choice_json] += 1
            choice_payloads[choice_json] = choice_payload
    shared_choice_json = sorted(key for key, count in choice_json_counts.items() if count > 1)
    choice_ids = {encoded: f"choices_{index}"
        for index, encoded in enumerate(shared_choice_json, start=1)}
    choice_lists = {choice_ids[encoded]: choice_payloads[encoded]
        for encoded in shared_choice_json}
    shape_groups: dict[tuple[str, ...], list[tuple[str, Mapping[str, Any]]]] = defaultdict(list)
    for ref, contract in contract_rows.items():
        if not isinstance(ref, str) or not isinstance(contract, Mapping) or any(
                not isinstance(key, str) for key in contract):
            raise PromptCatalogError("parameter contract table has a malformed row")
        keys = tuple(sorted(contract))
        shape_groups[keys].append((ref, contract))
    shape_tables = []
    for keys, grouped in sorted(shape_groups.items()):
        shape_tables.append({"column_order": list(keys),
            "rows": [[ref, *[_pack_parameter_cell(key, contract[key], choice_ids)
                for key in keys]] for ref, contract in grouped]})
    packed = {key: value for key, value in source.items()
        if key not in {"schema", "operators", "parameter_contracts",
            "input_requirement_contracts"}}
    packed.update({"schema": COLUMNAR_SCHEMA, "source_schema": SOURCE_SCHEMA,
        "operator_table": {"column_order": columns, "rows": rows,
            "input_row_columns": list(_INPUT_COLUMNS),
            "inputs_by_operator": inputs_by_operator,
            "parameters_by_operator": parameters_by_operator,
            "sparse_extras_by_operator": extras},
        "parameter_contract_table": {"row_order": list(contract_rows),
            "shape_tables": shape_tables, "choice_lists": choice_lists,
            "choice_reference_rule": "A choices cell shaped exactly as {$choice_list_ref:ref} resolves in choice_lists; choice lists are shared only when their full canonical JSON arrays are identical."},
        "input_requirement_contract_table": _pack_shape_table(
            source["input_requirement_contracts"], label="input requirement"),
        "packing_guide": ("operator_table.rows[i] aligns with column_order; inputs_by_operator[i] and "
            "parameters_by_operator[i] are aligned pair lists [name,contract_ref]. sparse_extras_by_operator "
            "uses zero-based operator row indexes. parameter_contract_table and "
            "input_requirement_contract_table each carry a row_order; shape-table rows are "
            "[contract_ref, values in column_order].")})
    if description_omission is not None:
        packed["parameter_description_omission"] = description_omission
    return packed


def _pack_shape_table(table: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    groups: dict[tuple[str, ...], list[tuple[str, Mapping[str, Any]]]] = defaultdict(list)
    for ref, contract in table.items():
        if not isinstance(ref, str) or not isinstance(contract, Mapping) or any(
                not isinstance(key, str) for key in contract):
            raise PromptCatalogError(f"{label} contract table has a malformed row")
        groups[tuple(sorted(contract))].append((ref, contract))
    shapes = [{"column_order": list(keys),
        "rows": [[ref, *[contract[key] for key in keys]] for ref, contract in rows]}
        for keys, rows in sorted(groups.items())]
    return {"row_order": list(table), "shape_tables": shapes}


def _expand_shape_table(table: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    row_order = table.get("row_order")
    shape_tables = table.get("shape_tables")
    if (not isinstance(row_order, list) or any(not isinstance(ref, str) for ref in row_order) or
            not isinstance(shape_tables, list)):
        raise PromptCatalogError(f"{label} contract table has malformed row order")
    contracts: dict[str, dict[str, Any]] = {}
    for shape in shape_tables:
        if not isinstance(shape, Mapping):
            raise PromptCatalogError(f"{label} contract shape must be an object")
        keys, grouped = shape.get("column_order"), shape.get("rows")
        if (not isinstance(keys, list) or any(not isinstance(key, str) for key in keys) or
                keys != sorted(set(keys)) or not isinstance(grouped, list)):
            raise PromptCatalogError(f"{label} contract shape has invalid columns")
        for cells in grouped:
            if not isinstance(cells, list) or len(cells) != len(keys) + 1:
                raise PromptCatalogError(f"{label} contract row differs from its shape")
            ref = cells[0]
            if not isinstance(ref, str) or ref in contracts:
                raise PromptCatalogError(f"{label} contract ref is malformed or duplicated")
            contracts[ref] = dict(zip(keys, cells[1:]))
    if len(row_order) != len(contracts) or set(row_order) != set(contracts):
        raise PromptCatalogError(f"{label} contract row order differs from its definitions")
    return {ref: contracts[ref] for ref in row_order}


def _pack_evolve_catalog(source: dict[str, Any]) -> dict[str, Any]:
    rows = source.get("operators")
    requirements = source.get("input_requirement_contracts")
    if not isinstance(rows, list) or not isinstance(requirements, Mapping):
        raise PromptCatalogError("evolve signature catalog has malformed operators/requirements")
    required_columns = ("name", "version", "output_type", "family", "description")
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or any(not isinstance(row.get(key), str)
                for key in required_columns):
            raise PromptCatalogError(f"evolve operator row {index} lacks its exact identity/description fields")
        if "input_requirements_ref" in row and not isinstance(row["input_requirements_ref"], str):
            raise PromptCatalogError(f"evolve operator row {index} has an invalid requirements ref")
    shared_fields: dict[str, Any] = {}
    for key in ("omitted_serving_dag_fields",):
        if rows and all(key in row and row[key] == rows[0][key] for row in rows):
            shared_fields[key] = _json_copy(rows[0][key])
    common = [key for key in rows[0]
        if key not in shared_fields and all(key in row for row in rows)] if rows else []
    preferred = list(required_columns)
    columns = [key for key in preferred if key in common]
    columns.extend(key for key in common if key not in columns)
    extras = {str(index): {key: value for key, value in row.items()
        if key not in columns and key not in shared_fields}
        for index, row in enumerate(rows)
        if any(key not in columns and key not in shared_fields for key in row)}
    packed = {key: value for key, value in source.items()
        if key not in {"schema", "operators", "input_requirement_contracts"}}
    packed.update({"schema": COLUMNAR_SCHEMA, "source_schema": EVOLVE_SOURCE_SCHEMA,
        "operator_table": {"column_order": columns,
            "rows": [[row[key] for key in columns] for row in rows],
            "shared_fields": shared_fields,
            "sparse_extras_by_operator": extras},
        "input_requirement_contract_table": _pack_shape_table(requirements,
            label="input requirement"),
        "packing_guide": ("operator_table.shared_fields apply to every row; rows[i] aligns with column_order; sparse_extras_by_operator uses "
            "zero-based operator row indexes. input_requirement_contract_table.row_order restores reference "
            "order; each shape-table row is [contract_ref, values in column_order].")})
    return packed


def _pack_parameter_cell(key: str, value: Any,
        choice_ids: Mapping[str, str]) -> Any:
    if key != "choices" or not isinstance(value, list):
        return value
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False)
    ref = choice_ids.get(encoded)
    return value if ref is None else {"$choice_list_ref": ref}


def expand_registered_signature_catalog(packed: Mapping[str, Any]) -> dict[str, Any]:
    """Restore the exact schema/2 compact catalog from its columnar prompt view."""
    value = _json_copy(packed)
    if (not isinstance(value, dict) or value.get("schema") != COLUMNAR_SCHEMA or
            not isinstance(value.get("source_schema"), str) or
            not isinstance(value.get("operator_table"), Mapping)):
        raise PromptCatalogError("unsupported columnar signature catalog schema")
    if value["source_schema"] == EVOLVE_SOURCE_SCHEMA:
        return _expand_evolve_catalog(value)
    if (value["source_schema"] != SOURCE_SCHEMA or
            not isinstance(value.get("parameter_contract_table"), Mapping) or
            not isinstance(value.get("input_requirement_contract_table"), Mapping)):
        raise PromptCatalogError("unsupported columnar signature catalog source schema")
    table = value["operator_table"]
    columns = table.get("column_order")
    rows = table.get("rows")
    input_columns = table.get("input_row_columns")
    inputs_rows = table.get("inputs_by_operator")
    parameter_rows = table.get("parameters_by_operator")
    extras = table.get("sparse_extras_by_operator")
    if (not isinstance(columns, list) or any(not isinstance(key, str) for key in columns) or
            not isinstance(rows, list) or not isinstance(input_columns, list) or
            input_columns != list(_INPUT_COLUMNS) or
            not isinstance(inputs_rows, list) or len(inputs_rows) != len(rows) or
            not isinstance(parameter_rows, list) or len(parameter_rows) != len(rows) or
            not isinstance(extras, Mapping)):
        raise PromptCatalogError("operator column table has malformed dimensions")
    operators: list[dict[str, Any]] = []
    for index, cells in enumerate(rows):
        if not isinstance(cells, list) or len(cells) != len(columns):
            raise PromptCatalogError(f"operator row {index} does not match its column order")
        row = dict(zip(columns, cells))
        raw_extra = extras.get(str(index), {})
        if not isinstance(raw_extra, Mapping) or set(row) & set(raw_extra):
            raise PromptCatalogError(f"operator row {index} extras collide with common fields")
        row.update(raw_extra)
        raw_inputs = inputs_rows[index]
        if raw_inputs is not None:
            if (not isinstance(raw_inputs, list) or any(not isinstance(port, list) or
                    len(port) != len(_INPUT_COLUMNS) for port in raw_inputs)):
                raise PromptCatalogError(f"operator row {index} input pair table is malformed")
            row["inputs"] = [dict(zip(_INPUT_COLUMNS, port)) for port in raw_inputs]
        raw_parameters = parameter_rows[index]
        if raw_parameters is not None:
            if (not isinstance(raw_parameters, list) or any(not isinstance(pair, list) or
                    len(pair) != 2 or not isinstance(pair[0], str) or
                    not isinstance(pair[1], str) for pair in raw_parameters)):
                raise PromptCatalogError(f"operator row {index} parameter pair table is malformed")
            names = [pair[0] for pair in raw_parameters]
            if len(names) != len(set(names)):
                raise PromptCatalogError(f"operator row {index} repeats a parameter name")
            row["parameters"] = dict(raw_parameters)
        operators.append(row)

    contract_table = value["parameter_contract_table"]
    row_order = contract_table.get("row_order")
    shape_tables = contract_table.get("shape_tables")
    choice_lists = contract_table.get("choice_lists")
    if (not isinstance(row_order, list) or any(not isinstance(ref, str) for ref in row_order) or
            not isinstance(shape_tables, list) or not isinstance(choice_lists, Mapping) or
            any(not isinstance(ref, str) for ref in choice_lists)):
        raise PromptCatalogError("parameter contract table has malformed row order")
    canonical_choices: dict[str, str] = {}
    for ref, choices in choice_lists.items():
        if not isinstance(choices, list):
            raise PromptCatalogError("shared parameter choice list must be an array")
        encoded = json.dumps(choices, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False)
        if encoded in canonical_choices:
            raise PromptCatalogError("choice list catalog duplicates an exact array")
        canonical_choices[encoded] = ref
    expected_choice_ids = {encoded: f"choices_{index}"
        for index, encoded in enumerate(sorted(canonical_choices), start=1)}
    if canonical_choices != expected_choice_ids:
        raise PromptCatalogError("shared choice-list refs/order are not stable canonical assignments")
    contracts: dict[str, dict[str, Any]] = {}
    for shape in shape_tables:
        if not isinstance(shape, Mapping):
            raise PromptCatalogError("parameter contract shape must be an object")
        keys, grouped = shape.get("column_order"), shape.get("rows")
        if (not isinstance(keys, list) or any(not isinstance(key, str) for key in keys) or
                keys != sorted(set(keys)) or not isinstance(grouped, list)):
            raise PromptCatalogError("parameter contract shape has invalid columns")
        for cells in grouped:
            if not isinstance(cells, list) or len(cells) != len(keys) + 1:
                raise PromptCatalogError("parameter contract row differs from its shape")
            ref = cells[0]
            if not isinstance(ref, str) or ref in contracts:
                raise PromptCatalogError("parameter contract ref is malformed or duplicated")
            contract = dict(zip(keys, cells[1:]))
            packed_choices = contract.get("choices")
            if isinstance(packed_choices, Mapping) and set(packed_choices) == {"$choice_list_ref"}:
                choice_ref = packed_choices["$choice_list_ref"]
                if not isinstance(choice_ref, str) or choice_ref not in choice_lists:
                    raise PromptCatalogError("parameter contract references an unknown choice list")
                contract["choices"] = choice_lists[choice_ref]
            contracts[ref] = contract
    if len(row_order) != len(contracts) or set(row_order) != set(contracts):
        raise PromptCatalogError("parameter contract row order differs from its definitions")
    ordered_contracts = {ref: contracts[ref] for ref in row_order}

    input_requirements = _expand_shape_table(value["input_requirement_contract_table"],
        label="input requirement")
    result = {key: item for key, item in value.items()
        if key not in {"schema", "source_schema", "operator_table", "parameter_contract_table",
            "input_requirement_contract_table", "packing_guide"}}
    result.update({"schema": SOURCE_SCHEMA,
        "operators": operators, "parameter_contracts": ordered_contracts,
        "input_requirement_contracts": input_requirements})
    return result


def _expand_evolve_catalog(value: dict[str, Any]) -> dict[str, Any]:
    table = value["operator_table"]
    columns, rows = table.get("column_order"), table.get("rows")
    extras = table.get("sparse_extras_by_operator")
    if (not isinstance(columns, list) or any(not isinstance(key, str) for key in columns) or
            not isinstance(rows, list) or not isinstance(extras, Mapping)):
        raise PromptCatalogError("evolve operator table has malformed columns/rows")
    operators = []
    for index, cells in enumerate(rows):
        if not isinstance(cells, list) or len(cells) != len(columns):
            raise PromptCatalogError(f"evolve operator row {index} differs from its column order")
        row = dict(zip(columns, cells))
        optional = extras.get(str(index), {})
        if not isinstance(optional, Mapping) or set(row) & set(optional):
            raise PromptCatalogError(f"evolve operator row {index} extras collide with common fields")
        shared = table.get("shared_fields", {})
        if not isinstance(shared, Mapping) or set(row) & set(shared) or set(optional) & set(shared):
            raise PromptCatalogError("evolve operator shared fields collide with row values")
        row.update(shared)
        row.update(optional)
        operators.append(row)
    requirement_table = value.get("input_requirement_contract_table")
    if not isinstance(requirement_table, Mapping):
        raise PromptCatalogError("evolve catalog has no input-requirement contract table")
    requirements = _expand_shape_table(requirement_table, label="input requirement")
    result = {key: item for key, item in value.items()
        if key not in {"schema", "source_schema", "operator_table",
            "input_requirement_contract_table", "packing_guide"}}
    result.update({"schema": EVOLVE_SOURCE_SCHEMA, "operators": operators,
        "input_requirement_contracts": requirements})
    return result
