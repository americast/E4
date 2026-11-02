#!/usr/bin/env python3
"""Freeze the reconstructed paper starter and its Bank deployment projection.

This command performs no model inference and does not open Bank case or label
data. Each invocation creates a new content-addressed run directory.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import inspect
import json
import platform
import secrets
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.contracts import PaperStarterInput, Telemetry, Window
from e4core.cache import implementation_fingerprint
from e4core.m4_data import make_m4_registry
from e4core.m4_library import (
    PAPER_STARTER_CAPACITY_POLICY,
    create_library_snapshot,
    load_library_snapshot,
    playbook_graph_hash,
)
from e4core.m5_manifest import _manifest_files
from e4core.paper_starter import (
    PROFILE,
    active_bank_diagnostic_inventory,
    available_bank_adaptation_books,
    bank_profile_playbooks,
    canonical_paper_library_source,
    make_paper_starter_registry,
)
from e4core.paper_starter_fixtures import validate_starter_fixture_suite


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(child) for child in value]
    return value


def _json_bytes(value: Any) -> bytes:
    return json.dumps(_plain(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_json_bytes(value) + b"\n")


def _archive_sources(run_root: Path, inventory: Mapping[str, str]) -> tuple[str, str]:
    archive = run_root / "frozen_source_snapshot.tar.gz"
    with tarfile.open(archive, "w:gz", compresslevel=6) as bundle:
        for relative, expected_hash in sorted(inventory.items()):
            source = ROOT / relative
            if source.is_symlink() or not source.is_file() or _sha(source.read_bytes()) != expected_hash:
                raise RuntimeError(f"source changed while building reproducibility archive: {relative}")
            bundle.add(source, arcname=relative, recursive=False)
    return str(archive.relative_to(ROOT)), _sha(archive.read_bytes())


def _source_hash(function: Any) -> str | None:
    try:
        return _sha(inspect.getsource(function).encode("utf-8"))
    except (OSError, TypeError):
        return None


def _estimator_pins(registry: Any) -> dict[str, Any]:
    return {name: {"source_sha256": _source_hash(function),
                   "callable": f"{function.__module__}.{function.__qualname__}"}
            for name, function in sorted(registry.estimators.items())}


def _telemetry_fixture() -> Telemetry:
    return Telemetry(
        case_id="synthetic-paper-starter-freeze",
        reference_window=Window("reference", 0, 60_000),
        alert_window=Window("alert", 60_000, 120_000),
        metrics=(), spans=(), deployment_catalog={}, data_sha256="0" * 64,
        file_sha256={}, duplicate_counts={}, quality_counts={},
        adapter_version="freeze-fixture/1", file_bytes={},
    )


def _bank_fixture_input() -> PaperStarterInput:
    return PaperStarterInput(
        telemetry=_telemetry_fixture(), sessions=None, urls=None,
        capabilities={"sessions": "unsupported", "urls": "unsupported",
            "native_logs": "available", "native_log_patterns": "available",
            "component_metrics": "available", "app_mrt": "available",
            "trace_dependencies": "available", "trace_root_duration": "available"},
        analysis_block_duration_ms=60_000,
    )


def _registry_signatures(registry: Any) -> dict[str, dict[str, Any]]:
    return {f"{row['name']}@{row['version']}": row for row in registry.signatures()}


def _implementation_pins(registries: tuple[Any, ...], names: set[str]) -> dict[str, Any]:
    result = {}
    for name in sorted(names):
        found = None
        for registry in registries:
            matches = [row for row in registry.signatures() if row["name"] == name]
            if matches:
                found = registry.require(name, matches[0]["version"])
                break
        if found is None:
            raise RuntimeError(f"active operator implementation is unavailable: {name}")
        result[f"{found.name}@{found.version}"] = {
            "implementation_kind": ("external_input" if found.external_input else "callable"),
            "callable": (None if found.implementation is None else
                f"{found.implementation.__module__}.{found.implementation.__qualname__}"),
            "source_sha256": _source_hash(found.implementation),
        }
        if found.implementation is None and not found.external_input:
            raise RuntimeError(f"active operator implementation is unavailable: {name}")
    return result


def _runtime_source_pins() -> dict[str, str]:
    sources = {
        "implementation_fingerprint": implementation_fingerprint(),
        "m4_runtime_policy_sha256": _sha((ROOT / "config/m4_runtime.json").read_bytes()),
        "bank_metric_policy_sha256": _sha((ROOT / "config/bank_metric_kind_policy.json").read_bytes()),
        "bank_kpi_policy_sha256": _sha((ROOT / "config/bank_dev_kpi_policy.json").read_bytes()),
        "bank_seed_library_sha256": _sha((ROOT / "config/bank_seed_library.json").read_bytes()),
        "paper_starter_catalog_sha256": _sha((ROOT / "config/paper_starter_catalog.json").read_bytes()),
        "paper_starter_playbooks_sha256": _sha((ROOT / "config/paper_starter_playbooks.json").read_bytes()),
        "paper_starter_runtime_sha256": _sha((ROOT / "config/paper_starter_runtime.json").read_bytes()),
        "paper_bank_adaptations_sha256": _sha((ROOT / "config/paper_bank_adaptations.json").read_bytes()),
    }
    return sources


def freeze(output_root: Path) -> Path:
    source_inventory_before = _manifest_files(ROOT)
    fixture_validation = validate_starter_fixture_suite()
    if not fixture_validation.get("passed") or fixture_validation.get("canonical_book_count") != 11:
        raise RuntimeError("rich synthetic fixture execution did not validate all canonical and Bank books")
    full_registry = make_paper_starter_registry(bank_only=False)
    canonical_source = canonical_paper_library_source(full_registry)
    canonical_names = {row["name"] for row in canonical_source["builtin_specs"]}
    canonical_docs = {row["name"]: row for row in canonical_source["playbooks"]}
    if len(canonical_names) != 19 or len(canonical_docs) != 11:
        raise RuntimeError("canonical paper origin must contain 19 operators and 11 DAGs")

    implementation_pins = {}
    for name in sorted(canonical_names):
        matches = [row for row in full_registry.signatures() if row["name"] == name]
        if len(matches) != 1:
            raise RuntimeError(f"canonical operator identity is ambiguous: {name}")
        registered = full_registry.require(name, matches[0]["version"])
        if not callable(registered.implementation):
            raise RuntimeError(f"canonical operator has no executable implementation: {name}")
        implementation_pins[f"{name}@{matches[0]['version']}"] = {
            "callable": f"{registered.implementation.__module__}.{registered.implementation.__qualname__}",
            "source_sha256": _source_hash(registered.implementation),
        }

    # These are typed data/input plumbing referenced by canonical templates;
    # the origin diagnostic inventory remains exactly the catalog's 19 rows.
    utility_names = {"case.telemetry", "paper.input.prepare"}
    full_rows = full_registry.signatures()
    utility_rows = [row for row in full_rows if row["name"] in utility_names]
    if {row["name"] for row in utility_rows} != utility_names:
        raise RuntimeError("canonical DAG utility signatures are incomplete")

    fixture = _bank_fixture_input()
    bank_starter_registry = make_paper_starter_registry(bank_only=True, starter_input=fixture)
    inventory = active_bank_diagnostic_inventory(fixture, root=ROOT)
    adaptation_books = list(available_bank_adaptation_books(fixture, root=ROOT))
    if inventory["diagnostic_operator_count"] > 19:
        raise RuntimeError("Bank projection exceeds the 19 diagnostic operator cap")

    # Add the existing bounded query/projection utilities without constructing
    # a BankCase or accessing prepared telemetry files.
    legacy_bank_registry = make_m4_registry(None, [], root=ROOT)
    signatures_by_key = _registry_signatures(bank_starter_registry)
    for key, signature in _registry_signatures(legacy_bank_registry).items():
        signatures_by_key.setdefault(key, signature)
    active_names = set(inventory["diagnostic_operator_names"])
    active_utility_names = set(inventory["utility_operator_names"])
    selected_names = active_names | active_utility_names
    selected_rows = [row for row in signatures_by_key.values() if row["name"] in selected_names]
    if {row["name"] for row in selected_rows} != selected_names:
        missing = sorted(selected_names - {row["name"] for row in selected_rows})
        raise RuntimeError(f"Bank projection references unregistered operators: {missing}")

    active_books = list(bank_profile_playbooks(bank_starter_registry, starter=None))
    if not active_books:
        raise RuntimeError("Bank projection has no supported canonical or deployment playbooks")
    active_graphs: dict[str, dict[str, Any]] = {}
    for book in active_books:
        digest = playbook_graph_hash(book["document"])
        active_graphs.setdefault(digest, book)
    active_books = list(active_graphs.values())
    active_operator_keys = {f"{row['name']}@{row['version']}" for row in selected_rows}
    for book in active_books:
        referenced = {f"{node['operator']}@{node['version']}"
                      for node in book["document"]["nodes"]}
        if not referenced <= active_operator_keys:
            raise RuntimeError(f"active Bank book references absent operators: {book['name']}")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)
    run_root = output_root / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    canonical_root = run_root / "canonical_library"
    bank_root = run_root / "bank_library"
    source_pins = _runtime_source_pins()
    baseline_manifest_path = ROOT / "artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json"
    baseline_manifest = json.loads(baseline_manifest_path.read_text(encoding="utf-8"))
    truth_pin = baseline_manifest.get("trusted_truth_pin", {})
    if (truth_pin.get("access") != "trusted_evaluator_after_m6_go_only" or
            not isinstance(truth_pin.get("path"), str) or
            not isinstance(truth_pin.get("sha256"), str)):
        raise RuntimeError("frozen baseline manifest lacks its trusted truth metadata pin")
    sample_path = ROOT / "config/bank_sample.json"
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    model_manifest = json.loads((ROOT / "config/model_manifest.json").read_text(encoding="utf-8"))
    scored_case_ids = sample.get("selection", {}).get("scored_ids")
    if not isinstance(scored_case_ids, list) or len(scored_case_ids) != 10 or len(set(scored_case_ids)) != 10:
        raise RuntimeError("Bank sample does not declare the exact ten scored cases")

    origin_rows = list(canonical_source["builtin_specs"]) + utility_rows
    canonical_snapshot = create_library_snapshot(
        {"examples": canonical_source["playbooks"]}, [], [],
        {"profile": PROFILE, "artifact_role": "immutable_canonical_origin",
         **source_pins,
         "inventory_sha256": canonical_source["inventory_sha256"]}, canonical_root,
        builtin_specs=origin_rows, capacity_policy=PAPER_STARTER_CAPACITY_POLICY,
    )
    bank_snapshot = create_library_snapshot(
        {"examples": active_books}, [], [],
        {"profile": PROFILE, "artifact_role": "active_bank_projection",
         "canonical_origin_snapshot_id": canonical_snapshot.snapshot_id,
         **source_pins,
         "diagnostic_operator_names": sorted(active_names)}, bank_root,
        builtin_specs=selected_rows, capacity_policy=PAPER_STARTER_CAPACITY_POLICY,
    )
    if len(canonical_snapshot.playbooks) != 11:
        raise RuntimeError("canonical origin snapshot did not retain all eleven DAG templates")
    if len(bank_snapshot.playbooks) != len(active_books):
        raise RuntimeError("active Bank snapshot did not retain every validated capability book")
    # Re-load from disk so the manifest only points to artifacts that pass the
    # same content-address and contract validation used by runtime consumers.
    canonical_snapshot = load_library_snapshot(canonical_snapshot.path)
    bank_snapshot = load_library_snapshot(bank_snapshot.path)
    bank_counts = bank_snapshot.document["policy"]["paper_counts"]
    if (bank_counts["diagnostic_operator_count"] != inventory["diagnostic_operator_count"] or
            bank_counts["utility_operator_count"] != inventory["utility_count_excluded"]):
        raise RuntimeError("Bank snapshot diagnostic/utility accounting differs from its capability projection")

    dependency_names = ("numpy", "scipy", "scikit-learn", "mypy")
    dependencies = {}
    for name in dependency_names:
        try:
            dependencies[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            dependencies[name] = None
    source_inventory_after = _manifest_files(ROOT)
    if source_inventory_before != source_inventory_after:
        raise RuntimeError("source/config inventory changed during freeze validation; rerun against a stable tree")
    file_pins = dict(source_inventory_after)
    source_archive_path, source_archive_sha256 = _archive_sources(run_root, source_inventory_after)
    if source_inventory_after != _manifest_files(ROOT):
        raise RuntimeError("source/config inventory changed while archiving; frozen run is invalid")
    manifest = {
        "schema_version": "e4-paper-starter-freeze/1",
        "run_id": run_id,
        "profile": PROFILE,
        "capacity_policy": PAPER_STARTER_CAPACITY_POLICY,
        "runtime_source_pins": source_pins,
        "initial_library_path": str(bank_root.relative_to(ROOT)),
        "initial_library_snapshot_id": bank_snapshot.snapshot_id,
        "canonical_origin_path": str(canonical_root.relative_to(ROOT)),
        "scored_case_ids": scored_case_ids,
        "shard_groups": {"local": scored_case_ids[:5], "wingtip": scored_case_ids[5:]},
        "bank_sample_sha256": _sha(sample_path.read_bytes()),
        "baseline_manifest_path": str(baseline_manifest_path.relative_to(ROOT)),
        "baseline_manifest_sha256": _sha(baseline_manifest_path.read_bytes()),
        "trusted_truth_pin": truth_pin,
        "model_profile_path": "config/gemma_m3_inference.json",
        "model_profile_sha256": _sha((ROOT / "config/gemma_m3_inference.json").read_bytes()),
        "model_manifest_path": "config/model_manifest.json",
        "model_manifest_sha256": _sha((ROOT / "config/model_manifest.json").read_bytes()),
        "model_checkpoint_sha256": model_manifest.get("sha256"),
        "model_revision": model_manifest.get("revision"),
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {"python": platform.python_version(), "dependencies": dependencies},
        "fixture_validation": fixture_validation,
        "fixture_validation_sha256": _sha(_json_bytes(fixture_validation)),
        "repository_source_inventory_sha256": _sha(_json_bytes(source_inventory_after)),
        "repository_source_files_sha256": source_inventory_after,
        "source_archive_path": source_archive_path,
        "source_archive_sha256": source_archive_sha256,
        "e4_implementation_fingerprint": source_pins["implementation_fingerprint"],
        "canonical_origin": {
            "snapshot_id": canonical_snapshot.snapshot_id,
            "operator_count": len(canonical_names),
            "utility_count_excluded": len(utility_rows),
            "playbook_count": len(canonical_docs),
            "playbook_graph_sha256": {name: playbook_graph_hash(row["document"])
                                       for name, row in sorted(canonical_docs.items())},
            "inventory_sha256": canonical_source["inventory_sha256"],
            "runtime_policy_sha256": canonical_source["runtime_policy_sha256"],
            "operator_signatures_sha256": _sha(_json_bytes(canonical_source["builtin_specs"])),
            "implementation_sources": implementation_pins,
        },
        "active_bank_projection": {
            "snapshot_id": bank_snapshot.snapshot_id,
            "diagnostic_operator_count": inventory["diagnostic_operator_count"],
            "diagnostic_operator_names": sorted(active_names),
            "utility_operator_count": len(active_utility_names),
            "utility_operator_names": sorted(active_utility_names),
            "supported_canonical_book_names": sorted(
                row["name"] for row in active_books if row["name"] in canonical_docs),
            "active_bank_book_names": sorted(row["name"] for row in active_books),
            "active_bank_dag_sha256": {row["name"]: playbook_graph_hash(row["document"])
                for row in sorted(active_books, key=lambda item: item["name"])},
            "active_bank_book_count": len(active_books),
            "bank_adaptation_books": [{"name": row["name"],
                "operators": list(row["operators"]), "requires": list(row["requires"])}
                for row in adaptation_books],
            "inventory": inventory,
        },
        "registry_signatures_sha256": {
            "canonical_full_registry": _sha(_json_bytes(full_registry.signatures())),
            "active_bank_registry": _sha(_json_bytes(bank_starter_registry.signatures())),
            "legacy_bank_registry": _sha(_json_bytes(legacy_bank_registry.signatures())),
            "active_bank_projection": _sha(_json_bytes(selected_rows)),
        },
        "active_bank_implementation_sources": _implementation_pins(
            (bank_starter_registry, legacy_bank_registry), selected_names),
        "estimators": {"canonical_full_registry": _estimator_pins(full_registry),
                       "active_bank_registry": _estimator_pins(bank_starter_registry),
                       "legacy_bank_registry": _estimator_pins(legacy_bank_registry)},
        "pinned_files_sha256": file_pins,
        "no_model_checkpoint_or_truth_bytes_read": True,
        "trusted_truth_access": "metadata pin only; truth bytes are read only by the later trusted audit evaluator",
    }
    _write_json(run_root / "freeze_manifest.json", manifest)
    _write_json(run_root / "canonical_origin.json", {
        "profile": PROFILE, "operator_count": len(canonical_names),
        "utility_names_excluded_from_count": sorted(utility_names),
        "operators": canonical_source["builtin_specs"],
        "playbooks": [{"name": row["name"], "document": row["document"],
            "graph_sha256": playbook_graph_hash(row["document"]),
            "provenance": row.get("provenance", {})}
            for row in canonical_source["playbooks"]],
        "manifest_sha256": _sha(_json_bytes(manifest)),
    })
    return run_root


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path,
        default=ROOT / "artifacts" / "paper_starter")
    args = parser.parse_args()
    path = freeze(args.output_root.resolve())
    print(path)


if __name__ == "__main__":
    main()
