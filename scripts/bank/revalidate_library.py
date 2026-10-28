#!/usr/bin/env python3
"""Revalidate a pristine seed-only M4 library after implementation-only fixes."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.adapters.bank import sha256_file
from e4.dsl.cache import implementation_fingerprint
from e4.agent.bank.bootstrap import _execute_synthetic_playbook
from e4.agent.bank.controller import _materialize_seeds_for_registry
from e4.operators.bank.query import make_m4_registry
from e4.agent.bank.fixtures import fixture_expected_top_component
from e4.agent.library import (LibraryError, LibraryIntegrityError,
                              assert_library_registry_compatible, create_library_metadata_child,
                              generated_specs_from_snapshot, load_library_snapshot, playbook_graph_hash,
                              snapshot_library)

FROZEN_SEED_CASES = (
    ("positive", "fixture_pod_alpha", "fixture_pod_beta", "positive"),
    ("positive", "counterfactual_component_theta", "counterfactual_component_delta", "renamed_positive"),
    ("no_change", "fixture_pod_alpha", "fixture_pod_beta", "no_change"),
    ("high_stable", "fixture_pod_alpha", "fixture_pod_beta", "high_stable"),
    ("missing", "fixture_pod_alpha", "fixture_pod_beta", "missing"),
)


class RevalidationError(RuntimeError):
    pass


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_hash(value: Any) -> str:
    return _sha(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8"))


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
        "bank_metric_policy_sha256": sha256_file(root / "config/bank/metric_kind_policy.json"),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank/kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank/seed_playbooks.json"),
        "profile_sha256": sha256_file(profile_path),
    }


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

    seed_config = json.loads((root / "config/bank/seed_playbooks.json").read_text(encoding="utf-8"))
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


def revalidate_seed_only_library(*, source_library: Path, output_dir: Path,
                                 profile_path: Path, bootstrap_artifact_dir: Path,
                                 expected_snapshot_id: str, reason: str,
                                 root: Path = ROOT,
                                 policy_path: Path | None = None) -> dict[str, Any]:
    """Verify and re-parent a pristine seed-only snapshot in a new copied store."""
    root = root.resolve(strict=True)
    policy_path = (policy_path or root / "config/bank/controller_policy.json").resolve(strict=True)
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

    seed_config = json.loads((root / "config/bank/seed_playbooks.json").read_text(encoding="utf-8"))
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-library", required=True, type=Path)
    parser.add_argument("--bootstrap-artifact-dir", required=True, type=Path)
    parser.add_argument("--expected-snapshot-id", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--profile", type=Path, default=ROOT / "config/models/gemma.json")
    parser.add_argument("--policy", type=Path, default=ROOT / "config/bank/controller_policy.json")
    parser.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    try:
        result = revalidate_seed_only_library(source_library=args.source_library,
            output_dir=args.output_dir, profile_path=args.profile,
            bootstrap_artifact_dir=args.bootstrap_artifact_dir,
            expected_snapshot_id=args.expected_snapshot_id, reason=args.reason,
            root=ROOT, policy_path=args.policy)
        print(json.dumps({"status": result["status"],
            "source_snapshot_id": result["source_snapshot_id"],
            "new_snapshot_id": result["new_snapshot_id"],
            "library_path": result["new_library"],
            "seed_fixture_count": len(result["seed_fixture_results"])}, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"M4 library revalidation failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
