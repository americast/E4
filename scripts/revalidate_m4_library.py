#!/usr/bin/env python3
"""Revalidate a pristine seed-only M4 library after implementation-only fixes."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.bank_adapter import sha256_file
from e4core.cache import implementation_fingerprint
from e4core.m4_bootstrap import _execute_synthetic_playbook
from e4core.m4_controller import _materialize_seeds_for_registry
from e4core.m4_data import make_m4_registry
from e4core.m4_fixtures import fixture_expected_top_component
from e4core.m4_library import (LibraryError, LibraryIntegrityError,
    assert_library_registry_compatible, create_library_metadata_child,
    generated_specs_from_snapshot, load_library_snapshot, operator_key,
    playbook_graph_hash, snapshot_library, _playbook_operator_keys, _thaw)

FROZEN_SEED_CASES = (
    ("positive", "fixture_pod_alpha", "fixture_pod_beta", "positive"),
    ("positive", "counterfactual_component_theta", "counterfactual_component_delta", "renamed_positive"),
    ("no_change", "fixture_pod_alpha", "fixture_pod_beta", "no_change"),
    ("high_stable", "fixture_pod_alpha", "fixture_pod_beta", "high_stable"),
    ("missing", "fixture_pod_alpha", "fixture_pod_beta", "missing"),
)
PROTOCOL_SOURCE_POLICY_SHA256 = "115edcde76e806d153b309264258fb950fe8054a145e3dceddd719d989609734"
PROTOCOL_MIGRATION_FIELDS = {
    "serving.evolution_protocol_version": "e4-separated-evolve-serve/1",
    "serving.maximum_stage3_static_repairs": 2,
    "serving.maximum_model_calls": 10,
    "serving.maximum_aggregate_generated_tokens": 73728,
}


class RevalidationError(RuntimeError):
    pass


def _assert_no_symlink_path(path: Path, root: Path) -> None:
    raw = Path(path).absolute()
    root = root.resolve(strict=True)
    try:
        relative = raw.relative_to(root)
    except ValueError as exc:
        raise RevalidationError("source paths must remain inside project root") from exc
    current = root
    if current.is_symlink():
        raise RevalidationError(f"project root cannot be a symlink: {current}")
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RevalidationError(f"source/artifact path cannot traverse symlinks: {current}")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_hash(value: Any) -> str:
    return _sha(json.dumps(_thaw(value), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8"))


def _bootstrap_fixture_outcomes_match(outcomes: Any) -> bool:
    """Check the bootstrap gate's stored scenario sequence and synthetic targets."""
    if (not isinstance(outcomes, Sequence) or isinstance(outcomes, (str, bytes)) or
            len(outcomes) != len(FROZEN_SEED_CASES)):
        return False
    for outcome, (scenario, alpha_id, _beta_id, _fixture_name) in zip(outcomes, FROZEN_SEED_CASES):
        if not isinstance(outcome, Mapping) or outcome.get("scenario") != scenario or outcome.get("status") != "passed":
            return False
        top_components = outcome.get("top_components")
        if (not isinstance(top_components, Sequence) or
                isinstance(top_components, (str, bytes))):
            return False
        if scenario == "positive":
            if alpha_id.casefold() not in {value.casefold() for value in top_components
                                           if isinstance(value, str)}:
                return False
        elif top_components:
            return False
    return True


def _file_inventory(root: Path) -> list[dict[str, Any]]:
    root = root.resolve(strict=True)
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RevalidationError(f"library/artifact tree contains a symlink: {path}")
        if path.is_file():
            rows.append({"path": path.relative_to(root).as_posix(),
                         "size_bytes": path.stat().st_size,
                         "sha256": sha256_file(path)})
    return rows


def _policy_pins(root: Path, policy_path: Path, profile_path: Path) -> dict[str, str]:
    return {
        "m4_runtime_policy_sha256": sha256_file(policy_path),
        "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json"),
        "profile_sha256": sha256_file(profile_path),
    }


def _protocol_policy_migration(*, source_policy_path: Path, policy_path: Path,
                               root: Path, provenance: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the one explicit V4-policy to split-protocol migration."""
    _assert_no_symlink_path(source_policy_path, root)
    _assert_no_symlink_path(policy_path, root)
    source_policy_path = Path(source_policy_path).resolve(strict=True)
    policy_path = Path(policy_path).resolve(strict=True)
    if (not source_policy_path.is_relative_to(root) or not policy_path.is_relative_to(root)):
        raise RevalidationError("source/current policy paths must remain inside project root")
    source_sha = sha256_file(source_policy_path)
    if source_sha != PROTOCOL_SOURCE_POLICY_SHA256:
        raise RevalidationError("preserved V4 source policy SHA-256 differs from the approved migration pin")
    if provenance.get("m4_runtime_policy_sha256") != source_sha:
        raise RevalidationError("preserved V4 source policy hash does not match original snapshot provenance")
    try:
        source = json.loads(source_policy_path.read_text(encoding="utf-8"))
        current = json.loads(policy_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RevalidationError("source/current runtime policy is not valid UTF-8 JSON") from exc
    if not isinstance(source, dict) or not isinstance(current, dict):
        raise RevalidationError("source/current runtime policy must be JSON objects")
    source_serving = source.get("serving")
    current_serving = current.get("serving")
    if (not isinstance(source_serving, dict) or not isinstance(current_serving, dict) or
            source_serving.get("stages") != ["select", "compose", "synthesize"] or
            source_serving.get("static_repair_limit_per_attempt") != 2 or
            type(source_serving.get("maximum_diagnostic_attempts")) is not int or
            source_serving.get("maximum_diagnostic_attempts") != 3):
        raise RevalidationError("preserved V4 serving policy lacks the required select/compose/synthesize base protocol")
    if (source.get("schema_version") != "e4-m4-runtime-policy/1" or
            source.get("policy_name") != "bank-m4-controller-input16m-cpu10-wall15-v3"):
        raise RevalidationError("preserved source policy is not the expected V4 M4 runtime policy")

    expected = json.loads(json.dumps(source))
    expected_serving = expected["serving"]
    for dotted, value in PROTOCOL_MIGRATION_FIELDS.items():
        _, key = dotted.split(".", 1)
        expected_serving[key] = value
    if current != expected:
        raise RevalidationError("current policy contains an unapproved V4-to-split-protocol difference")
    differences = []
    for dotted, value in PROTOCOL_MIGRATION_FIELDS.items():
        key = dotted.split(".", 1)[1]
        differences.append({"path": dotted, "old": None, "new": value,
                            "reason": "approved serving-protocol migration addition"})
    return {"migration": "V4 policy to separated evolve/serve protocol",
            "source_policy_path": str(source_policy_path),
            "source_policy_sha256": source_sha,
            "current_policy_path": str(policy_path),
            "current_policy_sha256": sha256_file(policy_path),
            "approved_differences": differences}


def _verify_pristine_snapshot(snapshot, *, expected_snapshot_id: str,
                              registry, root: Path, policy_path: Path,
                              profile_path: Path, bootstrap_artifact_dir: Path) -> dict[str, Any]:
    if snapshot.snapshot_id != expected_snapshot_id:
        raise RevalidationError("source library CURRENT does not match the explicitly selected snapshot ID")
    if snapshot.parent_snapshot_id is not None:
        raise RevalidationError("source library is not the original parentless bootstrap snapshot")
    provenance = dict(snapshot.document["provenance"])
    old_fingerprint = provenance.get("implementation_fingerprint")
    if (not isinstance(old_fingerprint, str) or len(old_fingerprint) != 64 or
            any(char not in "0123456789abcdef" for char in old_fingerprint)):
        raise RevalidationError("source snapshot lacks its original implementation fingerprint")
    pins = _policy_pins(root, policy_path, profile_path)
    for key, expected in pins.items():
        if provenance.get(key) != expected:
            raise RevalidationError(f"source snapshot pin differs from current frozen {key}")
    assert_library_registry_compatible(snapshot, registry)
    if generated_specs_from_snapshot(snapshot):
        raise RevalidationError("revalidation accepts only seed-only snapshots with no generated operators")
    if len(snapshot.operators) != 10 or any(entry["kind"] != "builtin"
                                            for entry in snapshot.operators.values()):
        raise RevalidationError("source snapshot must contain exactly the ten current immutable M4 built-ins")
    if any(entry.get("invoked_case_ids") for entry in snapshot.operators.values()):
        raise RevalidationError("source snapshot already contains case usage and is not pristine")
    if len(snapshot.playbooks) != 4 or any(entry["kind"] != "seed"
                                            for entry in snapshot.playbooks.values()):
        raise RevalidationError("source snapshot must contain exactly the four protected Bank seed playbooks")
    if any(entry.get("invoked_case_ids") for entry in snapshot.playbooks.values()):
        raise RevalidationError("source snapshot already contains playbook usage and is not pristine")
    if any(event.get("event") != "snapshot_created" for event in snapshot.document["lineage"]):
        raise RevalidationError("source snapshot has non-bootstrap lineage and is not pristine")

    seed_config = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    expected_docs = _materialize_seeds_for_registry(seed_config, registry)
    expected_graphs = {name: playbook_graph_hash(document) for name, document in expected_docs.items()}
    actual_graphs = {name: entry["graph_sha256"] for name, entry in snapshot.playbooks.items()}
    if set(expected_graphs) != set(actual_graphs) or expected_graphs != actual_graphs:
        raise RevalidationError("source snapshot seed playbook graphs differ from the four current M4 seeds")

    result_path = bootstrap_artifact_dir / "bootstrap_result.json"
    if not result_path.is_file():
        raise RevalidationError("original bootstrap_result.json is missing")
    bootstrap_result = json.loads(result_path.read_text(encoding="utf-8"))
    if (bootstrap_result.get("status") != "complete_seed_only" or
            bootstrap_result.get("library_snapshot_id") != snapshot.snapshot_id):
        raise RevalidationError("bootstrap artifact result does not match the selected seed-only snapshot")
    if bootstrap_result.get("library_snapshot_sha256") != sha256_file(snapshot.path):
        raise RevalidationError("bootstrap artifact snapshot digest does not match the selected immutable snapshot")
    bootstrap_inventory = _file_inventory(bootstrap_artifact_dir)
    return {"original_implementation_fingerprint": old_fingerprint,
            "current_implementation_fingerprint": implementation_fingerprint(),
            "policy_and_profile_pins": pins,
            "registry_builtin_count": len(registry.signatures()),
            "verified_builtin_count": len(snapshot.operators),
            "seed_graph_hashes": expected_graphs,
            "bootstrap_artifact_dir": str(bootstrap_artifact_dir),
            "bootstrap_result_sha256": sha256_file(result_path),
            "bootstrap_artifact_file_inventory": bootstrap_inventory}


def _verify_pristine_generated_bootstrap(snapshot, *, expected_snapshot_id: str,
                                        expected_snapshot_sha256: str,
                                        base_registry, root: Path,
                                        policy_path: Path, profile_path: Path,
                                        bootstrap_artifact_dir: Path,
                                        source_policy_path: Path | None = None) -> dict[str, Any]:
    """Verify an original parentless bootstrap while preserving its generated contents."""
    if snapshot.snapshot_id != expected_snapshot_id:
        raise RevalidationError("source library CURRENT differs from the explicitly selected bootstrap snapshot ID")
    if snapshot.parent_snapshot_id is not None:
        raise RevalidationError("generated-bootstrap preservation requires the original parentless snapshot")
    if (not isinstance(expected_snapshot_sha256, str) or len(expected_snapshot_sha256) != 64 or
            any(char not in "0123456789abcdef" for char in expected_snapshot_sha256)):
        raise RevalidationError("an explicit 64-character original snapshot SHA-256 is required")
    actual_snapshot_sha256 = sha256_file(snapshot.path)
    if actual_snapshot_sha256 != expected_snapshot_sha256:
        raise RevalidationError("original bootstrap snapshot SHA-256 differs from the explicit pin")

    provenance = dict(snapshot.document["provenance"])
    if (provenance.get("bank_input_manifest_used") is not False or
            provenance.get("bootstrap_model_response_obtained") is not True):
        raise RevalidationError("generated bootstrap provenance must show schema-only, label-free generation")
    old_fingerprint = provenance.get("implementation_fingerprint")
    if (not isinstance(old_fingerprint, str) or len(old_fingerprint) != 64 or
            any(char not in "0123456789abcdef" for char in old_fingerprint)):
        raise RevalidationError("original bootstrap lacks its implementation fingerprint")
    pins = _policy_pins(root, policy_path, profile_path)
    migration = None
    provenance_expected = dict(pins)
    if source_policy_path is not None:
        migration = _protocol_policy_migration(source_policy_path=source_policy_path,
            policy_path=policy_path, root=root, provenance=provenance)
        provenance_expected["m4_runtime_policy_sha256"] = migration["source_policy_sha256"]
    for key, expected in provenance_expected.items():
        if provenance.get(key) != expected:
            raise RevalidationError(f"original bootstrap pin differs from current frozen {key}")

    lineage = snapshot.document.get("lineage", ())
    if (len(lineage) != 1 or lineage[0].get("event") != "snapshot_created" or
            lineage[0].get("parent_snapshot_id") is not None or
            lineage[0].get("bootstrap_evicted_operator_keys")):
        raise RevalidationError("snapshot has non-bootstrap lineage or bootstrap evictions")
    if any(entry.get("invoked_case_ids") for entry in snapshot.document["operators"]):
        raise RevalidationError("original bootstrap contains operator usage and is not pristine")
    if any(entry.get("invoked_case_ids") for entry in snapshot.document["playbooks"]):
        raise RevalidationError("original bootstrap contains playbook usage and is not pristine")

    kind_counts = Counter(entry["kind"] for entry in snapshot.document["operators"])
    book_kind_counts = Counter(entry["kind"] for entry in snapshot.document["playbooks"])
    if kind_counts["builtin"] != 10 or kind_counts["seed"] != 0:
        raise RevalidationError("original bootstrap must contain exactly ten immutable built-ins")
    if book_kind_counts["seed"] != 4:
        raise RevalidationError("original bootstrap must retain exactly four protected seed playbooks")
    if len(snapshot.operators) != len(snapshot.document["operators"]) or len(snapshot.playbooks) != len(snapshot.document["playbooks"]):
        raise RevalidationError("original bootstrap contains duplicate operator or playbook identities")

    try:
        generated_specs = generated_specs_from_snapshot(snapshot)
    except Exception as exc:
        raise RevalidationError("retained generated operator source/signature validation failed") from exc
    generated_operator_keys = sorted(operator_key(spec) for spec in generated_specs)
    generated_book_entries = [entry for entry in snapshot.document["playbooks"]
                              if entry["kind"] == "generated"]
    generated_book_names = sorted(entry["name"] for entry in generated_book_entries)
    available_keys = set(snapshot.operators)
    for book in generated_book_entries:
        references = _playbook_operator_keys(book["document"])
        if not references or not references <= available_keys:
            raise RevalidationError(f"generated playbook {book['name']} has unresolved operator references")
        book_provenance = book.get("provenance", {})
        outcomes = book_provenance.get("synthetic_outcomes", [])
        if (book_provenance.get("source") != "schema_only_bootstrap" or
                not _bootstrap_fixture_outcomes_match(outcomes)):
            raise RevalidationError(f"generated playbook {book['name']} lacks a complete passing bootstrap fixture record")

    registry = make_m4_registry(None, generated_specs, root=root, audit_dir=None)
    assert_library_registry_compatible(snapshot, registry)
    seed_config = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    expected_seed_docs = _materialize_seeds_for_registry(seed_config, base_registry)
    expected_seed_graphs = {name: playbook_graph_hash(document)
                            for name, document in expected_seed_docs.items()}
    actual_seed_graphs = {entry["name"]: entry["graph_sha256"]
                          for entry in snapshot.document["playbooks"] if entry["kind"] == "seed"}
    if len(expected_seed_graphs) != 4 or actual_seed_graphs != expected_seed_graphs:
        raise RevalidationError("original bootstrap does not preserve the exact four frozen seed playbooks")

    result_path = bootstrap_artifact_dir / "bootstrap_result.json"
    _assert_no_symlink_path(result_path, root)
    if not result_path.is_file():
        raise RevalidationError("original bootstrap_result.json is missing")
    bootstrap_result = json.loads(result_path.read_text(encoding="utf-8"))
    expected_status = ("complete_with_additions" if generated_operator_keys or generated_book_names
                       else "complete_seed_only")
    if (bootstrap_result.get("status") != expected_status or
            bootstrap_result.get("library_snapshot_id") != snapshot.snapshot_id or
            bootstrap_result.get("library_snapshot_sha256") != actual_snapshot_sha256 or
            bootstrap_result.get("library_operator_count") != len(snapshot.operators) or
            bootstrap_result.get("library_playbook_count") != len(snapshot.playbooks) or
            bootstrap_result.get("implementation_fingerprint") != old_fingerprint or
            type(bootstrap_result.get("model_call_count")) is not int or
            bootstrap_result.get("model_call_count") != provenance.get("bootstrap_calls")):
        raise RevalidationError("bootstrap result status, snapshot, count, or provenance does not match the original library")
    reported_generated_ops = bootstrap_result.get("accepted_generated_operators")
    reported_generated_books = bootstrap_result.get("accepted_generated_playbooks")
    if (not isinstance(reported_generated_ops, list) or
            sorted(reported_generated_ops) != generated_operator_keys or
            not isinstance(reported_generated_books, list) or
            sorted(reported_generated_books) != generated_book_names):
        raise RevalidationError("bootstrap accepted-addition metadata differs from retained generated contents")
    verified_seeds = bootstrap_result.get("verified_seed_playbooks")
    if (not isinstance(verified_seeds, list) or
            sorted(row.get("name") for row in verified_seeds if isinstance(row, Mapping)) !=
            sorted(expected_seed_graphs) or
            any(row.get("passed") is not True or
                not _bootstrap_fixture_outcomes_match(row.get("outcomes"))
                for row in verified_seeds if isinstance(row, Mapping))):
        raise RevalidationError("bootstrap verified-seed metadata differs from the four retained seeds")

    return {"original_implementation_fingerprint": old_fingerprint,
        "current_implementation_fingerprint": implementation_fingerprint(),
        "policy_and_profile_pins": pins,
        "builtin_count": kind_counts["builtin"], "seed_playbook_count": book_kind_counts["seed"],
        "generated_operator_keys": generated_operator_keys,
        "generated_operator_source_hashes": {operator_key(spec): spec["source_sha256"]
                                               for spec in generated_specs},
        "generated_playbook_names": generated_book_names,
        "generated_playbook_graph_hashes": {entry["name"]: entry["graph_sha256"]
                                            for entry in generated_book_entries},
        "generated_operator_count": len(generated_operator_keys),
        "generated_playbook_count": len(generated_book_names),
        "library_operator_count": len(snapshot.operators),
        "library_playbook_count": len(snapshot.playbooks),
        "seed_graph_hashes": expected_seed_graphs,
        "source_snapshot_sha256": actual_snapshot_sha256,
        "protocol_policy_migration": migration,
        "bootstrap_status": bootstrap_result["status"],
        "bootstrap_artifact_dir": str(bootstrap_artifact_dir),
        "bootstrap_result_sha256": sha256_file(result_path),
        "original_bootstrap_model_calls": bootstrap_result["model_call_count"],
        "bootstrap_artifact_file_inventory": _file_inventory(bootstrap_artifact_dir)}


def revalidate_seed_only_library(*, source_library: Path, output_dir: Path,
                                 profile_path: Path, bootstrap_artifact_dir: Path,
                                 expected_snapshot_id: str, reason: str,
                                 root: Path = ROOT,
                                 policy_path: Path | None = None) -> dict[str, Any]:
    """Verify and re-parent a pristine seed-only snapshot in a new copied store."""
    root = root.resolve(strict=True)
    policy_path = (policy_path or root / "config/m4_runtime.json").resolve(strict=True)
    profile_path = profile_path.resolve(strict=True)
    bootstrap_artifact_dir = bootstrap_artifact_dir.resolve(strict=True)
    source_library = source_library.resolve(strict=True)
    output_dir = output_dir.resolve()
    if (not profile_path.is_relative_to(root) or not policy_path.is_relative_to(root) or
            not bootstrap_artifact_dir.is_relative_to(root) or not source_library.is_relative_to(root) or
            not output_dir.is_relative_to(root)):
        raise RevalidationError("all source, artifact, output, profile, and policy paths must remain inside project root")
    if source_library != (bootstrap_artifact_dir / "library").resolve():
        raise RevalidationError("source library must be the library directory inside its recorded bootstrap artifact")
    if not isinstance(reason, str) or not reason.strip() or len(reason.encode("utf-8")) > 4096:
        raise RevalidationError("revalidation reason must be nonempty text up to 4096 bytes")
    if (output_dir == source_library or output_dir.is_relative_to(source_library) or
            source_library.is_relative_to(output_dir) or output_dir == bootstrap_artifact_dir or
            output_dir.is_relative_to(bootstrap_artifact_dir) or bootstrap_artifact_dir.is_relative_to(output_dir)):
        raise RevalidationError("new revalidation output must not overlap source or bootstrap artifacts")
    if (source_library / ".m4-library.lock").exists():
        raise RevalidationError("source library currently has a transaction lock")
    if output_dir.exists():
        raise FileExistsError(f"revalidation output must be fresh: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    source_inventory_before = _file_inventory(source_library)
    source_snapshot = snapshot_library(source_library)
    registry = make_m4_registry(None, [], root=root, audit_dir=None)
    verification = _verify_pristine_snapshot(source_snapshot,
        expected_snapshot_id=expected_snapshot_id, registry=registry, root=root,
        policy_path=policy_path, profile_path=profile_path,
        bootstrap_artifact_dir=bootstrap_artifact_dir)

    output_dir.mkdir(exist_ok=False)
    copied_store = output_dir / "library"
    shutil.copytree(source_library, copied_store, symlinks=False)
    copied_snapshot = snapshot_library(copied_store)
    if copied_snapshot.snapshot_id != source_snapshot.snapshot_id:
        raise RevalidationError("copied source snapshot does not preserve its content hash")

    seed_config = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    seed_docs = _materialize_seeds_for_registry(seed_config, registry)
    fixture_results = []
    for seed_index, (seed_name, document) in enumerate(sorted(seed_docs.items())):
        for scenario, alpha_id, beta_id, fixture_name in FROZEN_SEED_CASES:
            fixture_root = output_dir / "fixtures" / f"seed_{seed_index}_{fixture_name}"
            fixture_root.mkdir(parents=True, exist_ok=False)
            passed, result = _execute_synthetic_playbook(document, [], scenario,
                fixture_root, root=root, alpha_id=alpha_id, beta_id=beta_id,
                run_dir=output_dir, index=f"seed_{seed_index}_{fixture_name}")
            fixture_results.append({"seed": seed_name, "fixture": fixture_name,
                                    "passed": passed, "result": result})
    if len(fixture_results) != 20 or not all(row["passed"] for row in fixture_results):
        failure = {"schema_version": "e4-m4-library-revalidation/1", "status": "failed",
            "source_snapshot_id": source_snapshot.snapshot_id,
            "reason": reason, "verification": verification,
            "seed_fixture_results": fixture_results,
            "source_inventory_unchanged": source_inventory_before == _file_inventory(source_library)}
        (output_dir / "revalidation_result.json").write_text(json.dumps(failure,
            ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        raise RevalidationError("one or more fixed seed synthetic validations failed")

    new_fingerprint = implementation_fingerprint()
    original_provenance = dict(copied_snapshot.document["provenance"])
    provenance_updates = {
        "original_bootstrap_provenance": original_provenance,
        "implementation_fingerprint": new_fingerprint,
        "revalidation": {"reason": reason.strip(),
            "source_snapshot_id": copied_snapshot.snapshot_id,
            "original_implementation_fingerprint": verification["original_implementation_fingerprint"],
            "new_implementation_fingerprint": new_fingerprint,
            "profile_sha256": verification["policy_and_profile_pins"]["profile_sha256"],
            "policy_and_profile_pins": verification["policy_and_profile_pins"],
            "bootstrap_artifact_dir": str(bootstrap_artifact_dir),
            "bootstrap_result_sha256": verification["bootstrap_result_sha256"],
            "seed_fixture_result_sha256": _canonical_hash(fixture_results),
            "validated_seed_fixture_count": len(fixture_results),
            "generated_operators_added": 0,
            "model_calls": 0, "gold_accessed": False},
    }
    lineage_event = {"event": "seed_only_snapshot_revalidated",
        "parent_snapshot_id": copied_snapshot.snapshot_id,
        "reason": reason.strip(),
        "old_implementation_fingerprint": verification["original_implementation_fingerprint"],
        "new_implementation_fingerprint": new_fingerprint,
        "bootstrap_artifact_dir": str(bootstrap_artifact_dir),
        "bootstrap_result_sha256": verification["bootstrap_result_sha256"],
        "seed_fixture_result_sha256": _canonical_hash(fixture_results),
        "validated_seed_fixture_count": len(fixture_results),
        "generated_operators_added": 0, "model_calls": 0, "gold_accessed": False}
    new_snapshot = create_library_metadata_child(copied_snapshot,
        provenance_updates=provenance_updates, lineage_event=lineage_event)
    if (new_snapshot.document["operators"] != copied_snapshot.document["operators"] or
            new_snapshot.document["playbooks"] != copied_snapshot.document["playbooks"]):
        raise RevalidationError("metadata-only revalidation changed operators or playbooks")
    source_inventory_after = _file_inventory(source_library)
    if source_inventory_after != source_inventory_before:
        raise RevalidationError("original source library changed during revalidation")
    report = {"schema_version": "e4-m4-library-revalidation/1", "status": "revalidated_seed_only",
        "reason": reason.strip(), "source_library": str(source_library),
        "source_snapshot_id": source_snapshot.snapshot_id,
        "new_library": str(copied_store), "new_snapshot_id": new_snapshot.snapshot_id,
        "parent_snapshot_id": new_snapshot.parent_snapshot_id,
        "operator_content_unchanged": True, "playbook_content_unchanged": True,
        "source_inventory_unchanged": True, "verification": verification,
        "seed_fixture_results": fixture_results,
        "seed_fixture_result_sha256": _canonical_hash(fixture_results),
        "model_calls": 0, "gold_accessed": False,
        "finished_utc": datetime.now(timezone.utc).isoformat()}
    (output_dir / "revalidation_result.json").write_text(json.dumps(report,
        ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return report


def revalidate_generated_bootstrap_library(*, source_library: Path, output_dir: Path,
                                           profile_path: Path, bootstrap_artifact_dir: Path,
                                           expected_snapshot_id: str,
                                           expected_snapshot_sha256: str,
                                           reason: str, root: Path = ROOT,
                                           policy_path: Path | None = None,
                                           source_policy_path: Path | None = None) -> dict[str, Any]:
    """Revalidate an exact generated bootstrap without dropping generated content."""
    root = root.resolve(strict=True)
    policy_path = (policy_path or root / "config/m4_runtime.json").resolve(strict=True)
    profile_path = profile_path.resolve(strict=True)
    if source_policy_path is not None:
        _assert_no_symlink_path(Path(source_policy_path), root)
        source_policy_path = Path(source_policy_path).resolve(strict=True)
    _assert_no_symlink_path(Path(bootstrap_artifact_dir), root)
    _assert_no_symlink_path(Path(source_library), root)
    bootstrap_artifact_dir = Path(bootstrap_artifact_dir).resolve(strict=True)
    source_library = Path(source_library).resolve(strict=True)
    output_dir = Path(output_dir).resolve()
    if (not profile_path.is_relative_to(root) or not policy_path.is_relative_to(root) or
            not bootstrap_artifact_dir.is_relative_to(root) or
            not source_library.is_relative_to(root) or not output_dir.is_relative_to(root)):
        raise RevalidationError("all source, artifact, output, profile, and policy paths must remain inside project root")
    if source_library != (bootstrap_artifact_dir / "library").resolve():
        raise RevalidationError("source library must be the library directory inside its recorded bootstrap artifact")
    if not isinstance(reason, str) or not reason.strip() or len(reason.encode("utf-8")) > 4096:
        raise RevalidationError("revalidation reason must be nonempty text up to 4096 bytes")
    if (output_dir == source_library or output_dir.is_relative_to(source_library) or
            source_library.is_relative_to(output_dir) or output_dir == bootstrap_artifact_dir or
            output_dir.is_relative_to(bootstrap_artifact_dir) or bootstrap_artifact_dir.is_relative_to(output_dir)):
        raise RevalidationError("new revalidation output must not overlap source or bootstrap artifacts")
    if (source_library / ".m4-library.lock").exists():
        raise RevalidationError("source library currently has a transaction lock")
    if output_dir.exists():
        raise FileExistsError(f"revalidation output must be fresh: {output_dir}")

    source_inventory_before = _file_inventory(source_library)
    bootstrap_inventory_before = _file_inventory(bootstrap_artifact_dir)
    try:
        source_snapshot = snapshot_library(source_library)
    except (LibraryError, OSError, json.JSONDecodeError) as exc:
        raise RevalidationError("original generated bootstrap snapshot failed integrity validation") from exc
    base_registry = make_m4_registry(None, [], root=root, audit_dir=None)
    verification = _verify_pristine_generated_bootstrap(source_snapshot,
        expected_snapshot_id=expected_snapshot_id,
        expected_snapshot_sha256=expected_snapshot_sha256,
        base_registry=base_registry, root=root, policy_path=policy_path,
        profile_path=profile_path, bootstrap_artifact_dir=bootstrap_artifact_dir,
        source_policy_path=source_policy_path)
    verification["source_library_inventory_sha256"] = _canonical_hash(source_inventory_before)
    verification["bootstrap_artifact_inventory_sha256"] = _canonical_hash(bootstrap_inventory_before)
    generated_specs = generated_specs_from_snapshot(source_snapshot)
    registry = make_m4_registry(None, generated_specs, root=root, audit_dir=None)
    assert_library_registry_compatible(source_snapshot, registry)

    output_dir.mkdir(parents=True, exist_ok=False)
    fixture_results = []
    for book_index, book in enumerate(sorted(source_snapshot.document["playbooks"],
                                              key=lambda row: row["name"])):
        # Snapshot documents are immutable mapping/tuple envelopes. Fixture validators
        # consume the same plain JSON dict/list shape used by bootstrap validation.
        book_document = _thaw(book["document"])
        for fixture_index, (scenario, alpha_id, beta_id, fixture_name) in enumerate(FROZEN_SEED_CASES):
            fixture_key = f"playbook_{book_index}_fixture_{fixture_index}_{fixture_name}"
            fixture_root = output_dir / "fixtures" / fixture_key
            fixture_root.mkdir(parents=True, exist_ok=False)
            try:
                passed, result = _execute_synthetic_playbook(book_document, generated_specs,
                    scenario, fixture_root, root=root, alpha_id=alpha_id, beta_id=beta_id,
                    run_dir=output_dir, index=fixture_key)
            except Exception as exc:
                passed, result = False, {"status": "fixture_runner_exception",
                    "error_type": type(exc).__name__, "error": str(exc)[:512]}
            fixture_results.append({"playbook": book["name"], "kind": book["kind"],
                                    "fixture": fixture_name, "passed": bool(passed),
                                    "result": result})

    source_inventory_after = _file_inventory(source_library)
    bootstrap_inventory_after = _file_inventory(bootstrap_artifact_dir)
    source_unchanged = source_inventory_after == source_inventory_before
    bootstrap_unchanged = bootstrap_inventory_after == bootstrap_inventory_before
    if not source_unchanged or not bootstrap_unchanged:
        raise RevalidationError("original bootstrap library or artifact changed during revalidation")

    expected_fixture_count = len(source_snapshot.playbooks) * len(FROZEN_SEED_CASES)
    if (len(fixture_results) != expected_fixture_count or
            not all(row["passed"] for row in fixture_results)):
        failure = {"schema_version": "e4-m4-generated-bootstrap-revalidation/1",
            "status": "failed", "mode": "preserve_generated_bootstrap",
            "source_snapshot_id": source_snapshot.snapshot_id,
            "source_snapshot_sha256": sha256_file(source_snapshot.path),
            "reason": reason.strip(), "verification": verification,
            "fixture_count": len(fixture_results), "fixture_results": fixture_results,
            "source_library_inventory_sha256": _canonical_hash(source_inventory_before),
            "bootstrap_artifact_inventory_sha256": _canonical_hash(bootstrap_inventory_before),
            "source_inventory_unchanged": source_unchanged,
            "bootstrap_artifact_inventory_unchanged": bootstrap_unchanged,
            "operators_removed": 0, "playbooks_removed": 0,
            "model_calls": 0, "gold_accessed": False}
        (output_dir / "revalidation_result.json").write_text(json.dumps(failure,
            ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        raise RevalidationError("one or more retained bootstrap playbook fixtures failed")

    copied_store = output_dir / "library"
    shutil.copytree(source_library, copied_store, symlinks=False)
    copied_snapshot = snapshot_library(copied_store)
    if copied_snapshot.snapshot_id != source_snapshot.snapshot_id:
        raise RevalidationError("copied bootstrap does not preserve its original snapshot identity")
    operator_content_sha256 = _canonical_hash(source_snapshot.document["operators"])
    playbook_content_sha256 = _canonical_hash(source_snapshot.document["playbooks"])
    new_fingerprint = implementation_fingerprint()
    original_provenance = dict(copied_snapshot.document["provenance"])
    fixture_hash = _canonical_hash(fixture_results)
    revalidation_metadata = {
        "mode": "preserve_generated_bootstrap",
        "reason": reason.strip(),
        "source_snapshot_id": copied_snapshot.snapshot_id,
        "source_snapshot_sha256": sha256_file(copied_snapshot.path),
        "original_implementation_fingerprint": verification["original_implementation_fingerprint"],
        "new_implementation_fingerprint": new_fingerprint,
        "policy_and_profile_pins": verification["policy_and_profile_pins"],
        "bootstrap_artifact_dir": str(bootstrap_artifact_dir),
        "bootstrap_result_sha256": verification["bootstrap_result_sha256"],
        "original_bootstrap_status": verification["bootstrap_status"],
        "protocol_policy_migration": verification.get("protocol_policy_migration"),
        "original_bootstrap_model_calls": verification["original_bootstrap_model_calls"],
        "original_generated_operator_keys": verification["generated_operator_keys"],
        "original_generated_operator_source_hashes": verification["generated_operator_source_hashes"],
        "original_generated_playbook_names": verification["generated_playbook_names"],
        "original_generated_playbook_graph_hashes": verification["generated_playbook_graph_hashes"],
        "operator_content_sha256": operator_content_sha256,
        "playbook_content_sha256": playbook_content_sha256,
        "source_library_inventory_sha256": verification["source_library_inventory_sha256"],
        "bootstrap_artifact_inventory_sha256": verification["bootstrap_artifact_inventory_sha256"],
        "fixture_results_sha256": fixture_hash,
        "seed_playbook_fixture_count": verification["seed_playbook_count"] * len(FROZEN_SEED_CASES),
        "generated_playbook_fixture_count": verification["generated_playbook_count"] * len(FROZEN_SEED_CASES),
        "validated_playbook_fixture_count": len(fixture_results),
        "model_calls": 0, "gold_accessed": False,
    }
    child = create_library_metadata_child(copied_snapshot,
        provenance_updates={"original_bootstrap_provenance": original_provenance,
            "implementation_fingerprint": new_fingerprint,
            "m4_runtime_policy_sha256": verification["policy_and_profile_pins"]["m4_runtime_policy_sha256"],
            "revalidation": revalidation_metadata},
        lineage_event={"event": "generated_bootstrap_snapshot_revalidated",
            **{key: value for key, value in revalidation_metadata.items()
               if key not in {"reason", "policy_and_profile_pins"}}})
    if (_canonical_hash(child.document["operators"]) != operator_content_sha256 or
            _canonical_hash(child.document["playbooks"]) != playbook_content_sha256):
        raise RevalidationError("metadata-only revalidation changed bootstrap operator/playbook content")
    if (_file_inventory(source_library) != source_inventory_before or
            _file_inventory(bootstrap_artifact_dir) != bootstrap_inventory_before):
        raise RevalidationError("original bootstrap library/artifact changed during revalidation")
    report = {"schema_version": "e4-m4-generated-bootstrap-revalidation/1",
        "status": "revalidated_generated_bootstrap",
        "mode": "preserve_generated_bootstrap", "reason": reason.strip(),
        "source_library": str(source_library), "source_snapshot_id": source_snapshot.snapshot_id,
        "source_snapshot_sha256": sha256_file(source_snapshot.path),
        "new_library": str(copied_store), "new_snapshot_id": child.snapshot_id,
        "parent_snapshot_id": child.parent_snapshot_id,
        "builtin_count": verification["builtin_count"],
        "seed_playbook_count": verification["seed_playbook_count"],
        "generated_operator_count": verification["generated_operator_count"],
        "generated_playbook_count": verification["generated_playbook_count"],
        "library_operator_count": verification["library_operator_count"],
        "library_playbook_count": verification["library_playbook_count"],
        "seed_playbook_fixture_count": verification["seed_playbook_count"] * len(FROZEN_SEED_CASES),
        "generated_playbook_fixture_count": verification["generated_playbook_count"] * len(FROZEN_SEED_CASES),
        "fixture_count": len(fixture_results),
        "operator_content_sha256": operator_content_sha256,
        "playbook_content_sha256": playbook_content_sha256,
        "source_library_inventory_sha256": verification["source_library_inventory_sha256"],
        "bootstrap_artifact_inventory_sha256": verification["bootstrap_artifact_inventory_sha256"],
        "operator_content_unchanged": True, "playbook_content_unchanged": True,
        "source_inventory_unchanged": True, "bootstrap_artifact_inventory_unchanged": True,
        "verification": verification, "fixture_results": fixture_results,
        "protocol_policy_migration": verification.get("protocol_policy_migration"),
        "fixture_results_sha256": fixture_hash,
        "model_calls": 0, "gold_accessed": False,
        "finished_utc": datetime.now(timezone.utc).isoformat()}
    (output_dir / "revalidation_result.json").write_text(json.dumps(report,
        ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-library", required=True, type=Path)
    parser.add_argument("--bootstrap-artifact-dir", required=True, type=Path)
    parser.add_argument("--expected-snapshot-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--profile", type=Path, default=ROOT / "config/gemma_m3_inference.json")
    parser.add_argument("--policy", type=Path, default=ROOT / "config/m4_runtime.json")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--preserve-generated-bootstrap", action="store_true",
                        help="explicitly revalidate and preserve a pristine generated bootstrap snapshot")
    parser.add_argument("--expected-snapshot-sha256",
                        help="required snapshot-file SHA-256 for --preserve-generated-bootstrap")
    parser.add_argument("--source-policy", type=Path,
                        help="preserved V4 runtime policy for the explicit separated-protocol migration; only with --preserve-generated-bootstrap")
    args = parser.parse_args(argv)
    try:
        if args.preserve_generated_bootstrap:
            if not args.expected_snapshot_sha256:
                parser.error("--preserve-generated-bootstrap requires --expected-snapshot-sha256")
            result = revalidate_generated_bootstrap_library(source_library=args.source_library,
                output_dir=args.output_dir, profile_path=args.profile,
                bootstrap_artifact_dir=args.bootstrap_artifact_dir,
                expected_snapshot_id=args.expected_snapshot_id,
                expected_snapshot_sha256=args.expected_snapshot_sha256,
                reason=args.reason, root=ROOT, policy_path=args.policy,
                source_policy_path=args.source_policy)
        else:
            if args.expected_snapshot_sha256:
                parser.error("--expected-snapshot-sha256 is only valid with --preserve-generated-bootstrap")
            if args.source_policy:
                parser.error("--source-policy is only valid with --preserve-generated-bootstrap")
            result = revalidate_seed_only_library(source_library=args.source_library,
                output_dir=args.output_dir, profile_path=args.profile,
                bootstrap_artifact_dir=args.bootstrap_artifact_dir,
                expected_snapshot_id=args.expected_snapshot_id, reason=args.reason,
                root=ROOT, policy_path=args.policy)
        output = {"status": result["status"],
            "source_snapshot_id": result["source_snapshot_id"],
            "new_snapshot_id": result["new_snapshot_id"],
            "library_path": result["new_library"]}
        if args.preserve_generated_bootstrap:
            output["fixture_count"] = len(result["fixture_results"])
        else:
            output["seed_fixture_count"] = len(result["seed_fixture_results"])
        print(json.dumps(output, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"M4 library revalidation failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
