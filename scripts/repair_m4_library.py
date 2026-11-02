#!/usr/bin/env python3
"""Create a pinned child library applying the reviewed legacy contract repair."""

from __future__ import annotations

import argparse
import copy
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

from e4core.bank_adapter import sha256_file
from e4core.bank_ops import bank_registry
from e4core.cache import canonical_json
from e4core.m4_controller import _implementation_fingerprint, _snapshot_source_pins
from e4core.m4_generated import validate_generated_spec
from e4core.m4_library import (_normalize_generated, _operator_identity_payload,
    _thaw, _write_snapshot, canonicalize_playbook_graph, generated_specs_from_snapshot,
    load_library_snapshot, operator_key, playbook_graph_hash)


SCHEMA_VERSION = "e4-m4-legacy-operator-contract-repair/1"
TARGET_OPERATOR = "evolved.log_pattern_scorer"
OLD_VERSION = "1+23690222713563f3"
OLD_SOURCE_SHA256 = "ad9c47d6d89853cc6b9eabf9b85722fa613cb3f8dd83f29f885c43368dcceb12"
NEW_SOURCE_SHA256 = "6434d4312587714d9146019b1c3961fde47602c5a04f85f86646eac31e60a679"
NEW_VERSION = "1+5ae68443b341841b"
LEGACY_METRIC_POLICY_PATH = ROOT / "config/bank_metric_kind_policy_v1_legacy.json"
LEGACY_METRIC_POLICY_SHA256 = "693a35cb2c1531f8a61b6414c55a9a08d88cf63010e121383b2e0eb735c2a604"
POLICY_BOUND_BUILTIN_NAMES = {"bank.metrics.aggregate", "bank.metrics.contrast",
    "bank.traces.aggregate", "bank.traces.latency_contrast", "bank.logs.volume_contrast"}
ALLOWED_OUTPUT_ROOT = (ROOT / "artifacts/e4_improvement/typed_v11").resolve()
REPAIR_FIELDS = ["ScoredCandidate.entity", "Evidence.contributing_entities", "Evidence.source"]


class RepairError(ValueError):
    """The requested migration does not match the authorized narrow repair."""


def _library_inventory(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): sha256_file(path)
        for path in sorted(root.rglob("*")) if path.is_file()}


def _policy_path_for_snapshot(snapshot) -> Path:
    """Select only a policy file whose exact digest is pinned by the snapshot."""
    provenance = snapshot.document.get("provenance", {})
    pinned_digest = provenance.get("bank_metric_policy_sha256") if isinstance(provenance, Mapping) else None
    current_path = ROOT / "config/bank_metric_kind_policy.json"
    current_digest = sha256_file(current_path)
    if pinned_digest == current_digest:
        return current_path
    if LEGACY_METRIC_POLICY_PATH.is_file():
        legacy_digest = sha256_file(LEGACY_METRIC_POLICY_PATH)
        if legacy_digest != LEGACY_METRIC_POLICY_SHA256:
            raise RepairError("preserved legacy Bank metric policy digest changed")
        if pinned_digest == legacy_digest:
            return LEGACY_METRIC_POLICY_PATH
    raise RepairError("source snapshot Bank metric policy SHA does not match a current or preserved exact policy file")


def _validate_policy_bound_builtins(snapshot, metric_policy_path: Path) -> None:
    """Verify the snapshot's metric built-ins against the selected exact policy."""
    registry = bank_registry(metric_policy_path=metric_policy_path)
    registered = {f"{row['name']}@{row['version']}": row
        for row in registry.signatures() if row["name"] in POLICY_BOUND_BUILTIN_NAMES}
    seen: set[str] = set()
    for entry in snapshot.document.get("operators", ()):
        if not isinstance(entry, Mapping) or entry.get("kind") != "builtin":
            continue
        if entry.get("name") not in POLICY_BOUND_BUILTIN_NAMES:
            continue
        key = operator_key(entry)
        expected = registered.get(key)
        signature = entry.get("signature")
        expected_hash = entry.get("signature_sha256")
        if (expected is None or not isinstance(signature, Mapping) or
                _thaw(signature) != expected or expected_hash != hashlib.sha256(_canonical(expected)).hexdigest()):
            raise RepairError(f"source snapshot metric builtin does not match its selected policy version: {key}")
        seen.add(entry["name"])
    if seen != POLICY_BOUND_BUILTIN_NAMES:
        raise RepairError("source snapshot does not contain the complete pinned metric builtin set")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")


def _approved_source(old_source: str) -> str:
    replacements = (
        ("'contributing_entities': {'$tuple': [entity_ref]}",
         "'contributing_entities': {'$tuple': [cand['entity']]}"),
        ("'source': 'log/log_service.csv'", "'source': 'bank/log/log_service.csv'"),
        ("'entity': entity_ref", "'entity': cand['entity']"),
    )
    result = old_source
    for old, new in replacements:
        if result.count(old) != 1:
            raise RepairError("frozen source does not contain each approved repair location exactly once")
        result = result.replace(old, new, 1)
    if hashlib.sha256(result.encode("utf-8")).hexdigest() != NEW_SOURCE_SHA256:
        raise RepairError("derived source does not match the reviewed proposal hash")
    return result


def _raw_parameters(parameters: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name, row in parameters.items():
        value = {key: _thaw(child) for key, child in row.items() if key != "has_default"}
        if not row.get("has_default"):
            value.pop("default", None)
        result[name] = value
    return result


def _replace_operator_refs(document: Mapping[str, Any], old_key: str,
                           new_key: str) -> tuple[dict[str, Any], int]:
    old_name, old_version = old_key.rsplit("@", 1)
    new_name, new_version = new_key.rsplit("@", 1)
    if old_name != new_name:
        raise RepairError("legacy repair cannot rename an operator")
    result = _thaw(document)
    changed = 0
    for node in result.get("nodes", []):
        if (isinstance(node, dict) and node.get("operator") == old_name and
                node.get("version") == old_version):
            node["version"] = new_version
            changed += 1
    return result, changed


def repair_library(*, source_library: Path, expected_source_snapshot_id: str,
        expected_source_snapshot_sha256: str, proposed_source: Path,
        expected_old_source_sha256: str, expected_new_source_sha256: str,
        output_dir: Path, reason: str,
        allowed_output_root: Path | None = None) -> dict[str, Any]:
    """Apply the one reviewed source repair into a new content-addressed store."""
    source_library = source_library.resolve(strict=True)
    if not source_library.is_dir():
        raise RepairError("source library must be a snapshot store directory")
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 1024:
        raise RepairError("a nonempty bounded migration reason is required")
    if expected_old_source_sha256 != OLD_SOURCE_SHA256:
        raise RepairError("old source hash is not the approved frozen scorer hash")
    if expected_new_source_sha256 != NEW_SOURCE_SHA256:
        raise RepairError("proposed source hash is not the reviewed repair hash")

    source_before = _library_inventory(source_library)
    source_snapshot = load_library_snapshot(source_library)
    if source_snapshot.snapshot_id != expected_source_snapshot_id:
        raise RepairError("source CURRENT snapshot ID differs from the required pin")
    source_snapshot_file_sha256 = sha256_file(source_snapshot.path)
    if source_snapshot_file_sha256 != expected_source_snapshot_sha256:
        raise RepairError("source snapshot file hash differs from the required pin")
    metric_policy_path = _policy_path_for_snapshot(source_snapshot)
    try:
        _snapshot_source_pins(source_snapshot, ROOT, ROOT / "config/m4_runtime.json",
            metric_policy_path=metric_policy_path)
        _validate_policy_bound_builtins(source_snapshot, metric_policy_path)
    except Exception as exc:
        raise RepairError(f"source snapshot has not been revalidated against current V11 pins: {exc}") from exc

    specs = generated_specs_from_snapshot(source_snapshot)
    old_spec = next((spec for spec in specs if spec["name"] == TARGET_OPERATOR and
                     spec["version"] == OLD_VERSION), None)
    if old_spec is None or old_spec.get("source_sha256") != OLD_SOURCE_SHA256:
        raise RepairError("target legacy operator identity/source hash differs from the reviewed pin")
    old_key = f"{TARGET_OPERATOR}@{OLD_VERSION}"
    old_row = source_snapshot.operators.get(old_key)
    if old_row is None or tuple(old_row.get("invoked_case_ids", ())) != ():
        raise RepairError("the starter legacy operator must have zero prior invocation IDs")

    old_source = old_spec.get("source")
    if not isinstance(old_source, str):
        raise RepairError("target operator does not contain source bytes")
    derived_source = _approved_source(old_source)
    proposed_source = proposed_source.resolve(strict=True)
    proposed_bytes = proposed_source.read_bytes()
    if hashlib.sha256(proposed_bytes).hexdigest() != NEW_SOURCE_SHA256:
        raise RepairError("reviewed proposed source file hash mismatch")
    if proposed_bytes.decode("utf-8") != derived_source:
        raise RepairError("proposed source differs from the exact three-field repair")

    raw_spec = {"name": old_spec["name"], "version": old_spec["declared_version"],
        "description": old_spec["description"], "family": old_spec["family"],
        "input_ports": _thaw(old_spec["input_ports"]),
        "output_type": old_spec["output_type"],
        "parameters": _raw_parameters(old_spec["parameters"]), "source": derived_source}
    old_normalized = validate_generated_spec(old_spec)
    new_normalized = validate_generated_spec(raw_spec)
    stable_fields = ("name", "declared_version", "description", "family", "input_ports",
        "output_type", "parameters", "signature_sha256")
    if any(_canonical(old_normalized[field]) != _canonical(new_normalized[field])
           for field in stable_fields):
        raise RepairError("repair changed operator signature or analytical parameters")
    new_key = f"{TARGET_OPERATOR}@{new_normalized['version']}"
    if new_normalized["version"] != NEW_VERSION or new_normalized["source_sha256"] != NEW_SOURCE_SHA256:
        raise RepairError("normalized new identity differs from the reviewed version/hash")

    payload = _thaw(source_snapshot.document)
    payload["parent_snapshot_id"] = source_snapshot.snapshot_id
    old_operators = list(payload["operators"])
    target_count = sum(operator_key(row) == old_key for row in old_operators)
    if target_count != 1:
        raise RepairError("source snapshot must contain exactly one target operator row")
    new_row = _normalize_generated(new_normalized, kind="generated")
    new_row["invoked_case_ids"] = list(old_row["invoked_case_ids"])
    new_row["admission_provenance"] = _thaw(old_row.get("admission_provenance", {}))
    operators = []
    for row in old_operators:
        if operator_key(row) == old_key:
            operators.append(new_row)
        else:
            operators.append(row)
    operators.sort(key=operator_key)
    payload["operators"] = operators

    changed_playbooks = []
    for book in payload["playbooks"]:
        migrated, references_changed = _replace_operator_refs(book["document"], old_key, new_key)
        if not references_changed:
            continue
        book["document"] = migrated
        book["canonical_graph"] = canonicalize_playbook_graph(migrated)
        book["graph_sha256"] = playbook_graph_hash(migrated)
        changed_playbooks.append(book["name"])
    if not changed_playbooks:
        raise RepairError("no stored playbook references the pinned legacy operator")

    provenance = payload["provenance"]
    policy_pin_keys = ("m4_runtime_policy_sha256", "bank_metric_policy_sha256",
        "bank_kpi_policy_sha256", "bank_seed_library_sha256")
    policy_and_profile_pins = {key: provenance[key] for key in policy_pin_keys if key in provenance}
    original_bootstrap = provenance.get("original_bootstrap_provenance", {})
    profile_sha256 = original_bootstrap.get("profile_sha256") if isinstance(original_bootstrap, Mapping) else None
    current_profile_sha256 = sha256_file(ROOT / "config/gemma_m3_inference.json")
    if profile_sha256 != current_profile_sha256:
        raise RepairError("source model profile pin does not match current V11 profile")
    policy_and_profile_pins["profile_sha256"] = profile_sha256
    implementation = _implementation_fingerprint()
    provenance["implementation_fingerprint"] = implementation
    migration_record = {"schema_version": SCHEMA_VERSION, "source_snapshot_id": source_snapshot.snapshot_id,
        "old_operator_key": old_key, "old_source_sha256": OLD_SOURCE_SHA256,
        "new_operator_key": new_key, "new_source_sha256": NEW_SOURCE_SHA256,
        "repair_fields": list(REPAIR_FIELDS), "reason": reason.strip(),
        "implementation_fingerprint": implementation,
        "policy_and_profile_pins": policy_and_profile_pins,
        "model_calls": 0, "gold_accessed": False}
    provenance["legacy_operator_contract_repair"] = migration_record
    payload["lineage"].append({"event": "authorized_legacy_operator_contract_repair",
        "case_id": None, **migration_record, "changed_playbook_names": sorted(changed_playbooks)})

    output_dir = output_dir.resolve()
    approved_output_root = (allowed_output_root or ALLOWED_OUTPUT_ROOT).resolve()
    if not output_dir.is_relative_to(approved_output_root):
        raise RepairError(f"repair output must be under {approved_output_root}")
    if output_dir.exists():
        raise FileExistsError(f"repair output directory must be fresh: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    child_library = output_dir / "library"
    try:
        shutil.copytree(source_library, child_library)
        child_snapshot = _write_snapshot(payload, child_library,
            expected_current_id=source_snapshot.snapshot_id)
        reloaded = load_library_snapshot(child_snapshot.path)
        if reloaded.snapshot_id != child_snapshot.snapshot_id:
            raise RepairError("repaired snapshot failed its content-addressed reload")
        if len(reloaded.operators) != 11 or len(reloaded.playbooks) != 5:
            raise RepairError("repair must preserve the 11-operator/5-playbook starter counts")
        if sum(1 for row in reloaded.document["operators"] if row["kind"] == "builtin") != 10:
            raise RepairError("repair must preserve exactly ten builtin operators")
        if reloaded.operators.get(old_key) is not None or reloaded.operators.get(new_key) is None:
            raise RepairError("repaired target version did not replace the old key")
        if tuple(reloaded.operators[new_key].get("invoked_case_ids", ())) != ():
            raise RepairError("repaired starter operator must have zero invocation IDs")
        if any(old_key == f"{node.get('operator')}@{node.get('version')}"
                for book in reloaded.document["playbooks"] for node in book["document"]["nodes"]):
            raise RepairError("a playbook still references the retired operator version")

        source_after = _library_inventory(source_library)
        if source_after != source_before:
            raise RepairError("source library changed during migration")
        other_old = {operator_key(row): _operator_identity_payload(row) for row in old_operators
            if operator_key(row) != old_key}
        other_new = {operator_key(row): _operator_identity_payload(row)
            for row in reloaded.document["operators"] if operator_key(row) != new_key}
        other_unchanged = _canonical(other_old) == _canonical(other_new)
        if not other_unchanged:
            raise RepairError("migration changed a non-target operator")
        report = {"schema_version": SCHEMA_VERSION, "status": "repaired",
            "reason": reason.strip(), "source_library": str(source_library),
            "source_snapshot_id": source_snapshot.snapshot_id,
            "source_snapshot_file_sha256": source_snapshot_file_sha256,
            "source_library_inventory_sha256": hashlib.sha256(_canonical(source_before)).hexdigest(),
            "source_library_inventory_unchanged": True,
            "new_library": str(child_library), "new_snapshot_id": reloaded.snapshot_id,
            "new_snapshot_file_sha256": sha256_file(reloaded.path),
            "parent_snapshot_id": reloaded.parent_snapshot_id,
            "old_operator_key": old_key, "old_source_sha256": OLD_SOURCE_SHA256,
            "new_operator_key": new_key, "new_source_sha256": NEW_SOURCE_SHA256,
            "repair_fields": list(REPAIR_FIELDS), "exact_three_field_patch": True,
            "other_operator_content_unchanged": other_unchanged,
            "playbook_changes_only_operator_version_references": True,
            "changed_playbook_names": sorted(changed_playbooks),
            "counting_logic_unchanged": True,
            "builtin_count": 10, "library_operator_count": len(reloaded.operators),
            "library_playbook_count": len(reloaded.playbooks),
            "starter_usage_count": len(reloaded.operators[new_key]["invoked_case_ids"]),
            "policy_and_profile_pins": policy_and_profile_pins,
            "implementation_fingerprint": implementation,
            "model_calls": 0, "gold_accessed": False,
            "migration_lineage_event": "authorized_legacy_operator_contract_repair",
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        report_path = output_dir / "repair_result.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2,
            sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        return report
    except Exception:
        shutil.rmtree(output_dir, ignore_errors=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-library", type=Path, required=True)
    parser.add_argument("--expected-source-snapshot-id", required=True)
    parser.add_argument("--expected-source-snapshot-sha256", required=True)
    parser.add_argument("--expected-old-source-sha256", required=True)
    parser.add_argument("--proposed-source", type=Path, required=True)
    parser.add_argument("--expected-new-source-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--reason", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = repair_library(source_library=args.source_library,
            expected_source_snapshot_id=args.expected_source_snapshot_id,
            expected_source_snapshot_sha256=args.expected_source_snapshot_sha256,
            expected_old_source_sha256=args.expected_old_source_sha256,
            proposed_source=args.proposed_source,
            expected_new_source_sha256=args.expected_new_source_sha256,
            output_dir=args.output_dir, reason=args.reason)
    except Exception as exc:
        print(json.dumps({"status": "rejected", "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
