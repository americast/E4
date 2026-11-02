"""Explicit metadata-contract migration for the retained legacy log scorer.

This preserves the legacy operator source bytes and authoring profile. It only
adds a host-validated input_requirements signature, creating a new operator
version and rewriting exact playbook references in a new child snapshot.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.m4_generated import validate_generated_spec
from e4core.m4_library import (_normalize_generated, _normal_playbook, _operator_identity_payload,
    _thaw, _write_snapshot, generated_specs_from_snapshot, load_library_snapshot, operator_key)

TARGET_NAME = "evolved.log_pattern_scorer"
APPROVED_SOURCE_SHA256 = "6434d4312587714d9146019b1c3961fde47602c5a04f85f86646eac31e60a679"
LOG_INPUT_REQUIREMENTS = {
    "candidates": {"source_columns": {}, "statistics": [], "group_by": [],
        "support_unit": None, "coverage": "any"},
    "logs": {"source_columns": {"log/log_service.csv": ["cmdb_id", "value"]},
        "statistics": ["raw"], "group_by": [], "support_unit": "raw_row", "coverage": "page"}}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _raw_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    parameters = {}
    for name, row in spec["parameters"].items():
        item = {key: _thaw(value) for key, value in row.items() if key != "has_default"}
        if not row.get("has_default"):
            item.pop("default", None)
        parameters[name] = item
    return {"name": spec["name"], "version": spec.get("declared_version", spec["version"]),
        "description": spec["description"], "family": spec["family"],
        "input_ports": _thaw(spec["input_ports"]), "output_type": spec["output_type"],
        "parameters": parameters, "source": spec["source"]}


def migrate(*, source_library: Path, output_library: Path,
            expected_source_snapshot_id: str,
            expected_source_sha256: str) -> dict[str, Any]:
    source_library = Path(source_library).resolve(strict=True)
    source = load_library_snapshot(source_library)
    if not expected_source_snapshot_id or source.snapshot_id != expected_source_snapshot_id:
        raise ValueError("input snapshot ID differs from the explicit migration source pin")
    if not expected_source_sha256 or sha(source.path.read_bytes()) != expected_source_sha256:
        raise ValueError("input snapshot file SHA differs from the explicit migration source pin")
    output_library = Path(output_library).resolve()
    if (output_library == source_library or output_library in source_library.parents or
            source_library in output_library.parents):
        raise ValueError("migration output must be disjoint from the immutable source library")
    specs = generated_specs_from_snapshot(source)
    matches = [row for row in specs if row.get("name") == TARGET_NAME]
    if len(matches) != 1:
        raise ValueError("source library must contain exactly one target log scorer")
    old = matches[0]
    if old.get("authoring_profile") is not None or not isinstance(old.get("source"), str):
        raise ValueError("migration requires the existing source-based legacy scorer without a typed-profile flip")
    old_source_sha = sha(old["source"].encode("utf-8"))
    if old_source_sha != old.get("source_sha256") or old_source_sha != APPROVED_SOURCE_SHA256:
        raise ValueError("legacy scorer source bytes do not match its content hash")
    if tuple(source.operators[operator_key(old)].get("invoked_case_ids", ())) != ():
        raise ValueError("legacy scorer already has invocation IDs; this migration is scoped to pristine starter")

    raw = _raw_spec(old)
    raw["input_requirements"] = LOG_INPUT_REQUIREMENTS
    new = validate_generated_spec(raw)
    if (new.get("authoring_profile") is not None or
            new["source_sha256"] != old_source_sha or new["source"] != old["source"] or
            new["version"] == old["version"]):
        raise ValueError("migration changed algorithm/source identity or failed to version the new contract")
    new_row = _normalize_generated(raw, kind="generated")
    new_row["invoked_case_ids"] = []
    new_row["admission_provenance"] = _thaw(source.operators[operator_key(old)].get("admission_provenance", {}))

    payload = _thaw(source.document)
    payload["parent_snapshot_id"] = source.snapshot_id
    original_other_operator_rows = {operator_key(row): _operator_identity_payload(row)
        for row in payload["operators"] if operator_key(row) != operator_key(old)}
    payload["operators"] = [row for row in payload["operators"] if operator_key(row) != operator_key(old)]
    payload["operators"].append(new_row)
    rewritten_nodes = 0
    rewritten_playbooks = []
    playbooks = []
    for book in payload["playbooks"]:
        original_document = _thaw(book["document"])
        document = _thaw(book["document"])
        changed = 0
        for node in document.get("nodes", []):
            if (node.get("operator") == old["name"] and node.get("version") == old["version"]):
                node["version"] = new["version"]
                changed += 1
        rewritten_nodes += changed
        if changed:
            rewritten_playbooks.append(book["name"])
        normalized_book = _normal_playbook(book["name"], document, book["kind"], book.get("provenance"))
        normalized_book["invoked_case_ids"] = list(book.get("invoked_case_ids", []))
        if not changed and normalized_book["document"] != original_document:
            raise ValueError(f"unmodified playbook changed during migration: {book['name']}")
        playbooks.append(normalized_book)
    if not rewritten_nodes:
        raise ValueError("migration found no playbook reference to the old exact operator version")
    payload["playbooks"] = playbooks
    payload["lineage"].append({"event": "legacy_input_contract_migration",
        "parent_snapshot_id": source.snapshot_id, "operator_name": old["name"],
        "old_version": old["version"], "new_version": new["version"],
        "source_sha256_unchanged": old_source_sha,
        "contract_sha256": new["signature_sha256"],
        "playbook_names_rewritten": sorted(rewritten_playbooks),
        "playbook_node_references_rewritten": rewritten_nodes,
        "authoring_profile_unchanged": True,
        "revalidation_status": "not_revalidated_after_contract_migration"})

    if output_library.exists():
        raise FileExistsError("migration output library already exists")
    shutil.copytree(source_library, output_library, symlinks=False)
    migrated = _write_snapshot(payload, output_library,
        expected_current_id=source.snapshot_id)
    reloaded = load_library_snapshot(migrated.path)
    reloaded_spec = next(row for row in generated_specs_from_snapshot(reloaded)
        if row["name"] == TARGET_NAME)
    if (reloaded_spec["version"] != new["version"] or
            reloaded_spec["source_sha256"] != old_source_sha or
            reloaded_spec.get("authoring_profile") is not None or
            reloaded_spec["input_requirements"] != LOG_INPUT_REQUIREMENTS):
        raise ValueError("migrated source-free signature did not survive persist/reload")
    # Compare the inherited operator records directly after replacing the target,
    # proving unrelated source/program identities did not move during migration.
    expected_other_operators = dict(original_other_operator_rows)
    actual_other_operators = {key: _operator_identity_payload(row)
        for key, row in reloaded.operators.items() if key != operator_key(reloaded_spec)}
    if expected_other_operators != actual_other_operators:
        raise ValueError("migration changed the identity of an unrelated operator")
    from e4core.cache import implementation_fingerprint
    migrated_implementation_fingerprint = implementation_fingerprint()
    return {"schema_version": "e4-legacy-input-contract-migration/1",
        "event": "explicit_legacy_input_contract_migration",
        "parent_snapshot_id": source.snapshot_id,
        "parent_snapshot_sha256": sha(source.path.read_bytes()),
        "new_snapshot_id": migrated.snapshot_id,
        "new_snapshot_sha256": sha(migrated.path.read_bytes()),
        "operator": TARGET_NAME, "old_version": old["version"],
        "new_version": new["version"], "old_source_sha256": old_source_sha,
        "new_source_sha256": new["source_sha256"],
        "source_bytes_unchanged": True, "authoring_profile": None,
        "approved_source_sha256": APPROVED_SOURCE_SHA256,
        "inherited_source_implementation_fingerprint": source.document.get("provenance", {}).get("implementation_fingerprint"),
        "migration_tool_implementation_fingerprint": migrated_implementation_fingerprint,
        "revalidation_status": "not_revalidated_after_contract_migration",
        "unrelated_operator_count": len(expected_other_operators),
        "unmodified_playbooks_preserved": True,
        "old_signature_sha256": old["signature_sha256"],
        "new_signature_sha256": new["signature_sha256"],
        "declared_input_requirements": LOG_INPUT_REQUIREMENTS,
        "rewritten_playbooks": sorted(rewritten_playbooks),
        "rewritten_playbook_node_references": rewritten_nodes,
        "old_snapshot_remains_readable": load_library_snapshot(source.path).snapshot_id == source.snapshot_id,
        "created_utc": datetime.now(timezone.utc).isoformat()}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-library", type=Path, required=True)
    parser.add_argument("--output-library", type=Path, required=True)
    parser.add_argument("--expected-source-snapshot-id", required=True)
    parser.add_argument("--expected-source-sha256", required=True)
    args = parser.parse_args(argv)
    report = migrate(source_library=args.source_library, output_library=args.output_library,
        expected_source_snapshot_id=args.expected_source_snapshot_id,
        expected_source_sha256=args.expected_source_sha256)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
