#!/usr/bin/env python3
"""Prepare sealed, independent 10-case reruns for selected inference seeds.

This helper creates metadata and audit roots only. It never starts model
servers, submits model requests, or reads the trusted evaluator ledger.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import secrets
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
LAUNCH_ROOT = Path(__file__).resolve().parent
ALLOWED_SEEDS = (42, 43, 44)
HISTORICAL_REPORT_PATH = Path("artifacts/seed_variability/reports/pilot_20261025_s43_s44_v2/seed_variability_report.json")
CASE_ORDER = (
    "bank-20210304T0100-utc8", "bank-20210304T1800-utc8",
    "bank-20210306T0330-utc8", "bank-20210307T0300-utc8",
    "bank-20210307T0330-utc8", "bank-20210309T0800-utc8",
    "bank-20210309T1730-utc8", "bank-20210312T2030-utc8",
    "bank-20210324T1100-utc8", "bank-20210325T2000-utc8",
)
HELPER_PATHS = (
    "artifacts/escalation_fixes/implementation_20261026/launch/prepare_frozen_rerun.py",
    "artifacts/escalation_fixes/implementation_20261026/launch/local_driver.py",
    "artifacts/escalation_fixes/implementation_20261026/launch/remote_driver.py",
    "artifacts/escalation_fixes/implementation_20261026/launch/worker_profile.py",
    "artifacts/escalation_fixes/implementation_20261026/launch/run_frozen_rerun_queue.py",
    "artifacts/escalation_fixes/implementation_20261026/launch/report_frozen_reruns.py",
    "artifacts/seed_variability/launch/pilot_report.py",
    "artifacts/seed_variability/launch/pilot_verify.py",
    "artifacts/paper_starter/launch/local_driver.py",
    "artifacts/paper_starter/launch/remote_driver.py",
    "artifacts/paper_starter/launch/start_wingtip_server.py",
    "artifacts/paper_starter/launch/finalize_audit.py",
    "artifacts/e4_improvement/remote_wingtip/remote_inference.py",
    "scripts/run_paper_starter_audit.py", "scripts/run_m3_model.py", "scripts/run_m4_bank.py",
)
CONFIG_PATHS = (
    "config/m4_runtime.json", "config/bank_dev_kpi_policy.json",
    "config/bank_metric_kind_policy.json", "config/paper_starter_runtime.json",
    "config/paper_starter_catalog.json", "config/paper_starter_playbooks.json",
    "config/paper_bank_adaptations.json", "config/typed_operator_ir_v2_policy.json",
)


class RerunError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_new_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _inside_root(path: Path, label: str, *, must_exist: bool = False) -> Path:
    resolved = path.resolve(strict=must_exist)
    if not resolved.is_relative_to(ROOT):
        raise RerunError(f"{label} must remain inside the project root")
    return resolved


def _library_snapshot_pin(directory_relative: str, expected_snapshot_id: Any,
                          label: str) -> dict[str, str]:
    """Verify CURRENT and return a pin for its immutable content-addressed file."""
    if (not isinstance(directory_relative, str) or
            not isinstance(expected_snapshot_id, str) or
            not re.fullmatch(r"[0-9a-f]{64}", expected_snapshot_id)):
        raise RerunError(f"{label} path or snapshot ID is malformed")
    raw_directory = ROOT / directory_relative
    if raw_directory.is_symlink():
        raise RerunError(f"{label} library directory cannot be a symlink")
    directory = _inside_root(raw_directory, f"{label} library directory", must_exist=True)
    if not directory.is_dir():
        raise RerunError(f"{label} library path must be a directory")
    current_path = directory / "CURRENT"
    if current_path.is_symlink() or not current_path.is_file():
        raise RerunError(f"{label} library CURRENT pointer is missing or unsafe")
    try:
        current_id = current_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise RerunError(f"{label} library CURRENT pointer is unreadable") from exc
    if current_id != expected_snapshot_id:
        raise RerunError(f"{label} library CURRENT does not resolve to the frozen snapshot ID")
    from e4core.m4_library import snapshot_library
    try:
        current_snapshot = snapshot_library(directory)
    except Exception as exc:
        raise RerunError(f"{label} current library snapshot failed integrity validation") from exc
    if current_snapshot.snapshot_id != expected_snapshot_id:
        raise RerunError(f"{label} library CURRENT snapshot differs from its frozen ID")
    snapshot_path = directory / f"{expected_snapshot_id}.json"
    if snapshot_path.is_symlink() or not snapshot_path.is_file():
        raise RerunError(f"{label} immutable snapshot JSON is missing or unsafe")
    snapshot_path = _inside_root(snapshot_path, f"{label} immutable snapshot JSON", must_exist=True)
    try:
        immutable_snapshot = snapshot_library(snapshot_path)
    except Exception as exc:
        raise RerunError(f"{label} immutable snapshot JSON failed integrity validation") from exc
    if immutable_snapshot.snapshot_id != expected_snapshot_id:
        raise RerunError(f"{label} immutable snapshot ID differs from CURRENT")
    return {"directory_path": directory_relative,
        "snapshot_file_path": snapshot_path.relative_to(ROOT).as_posix(),
        "snapshot_file_sha256": _sha(snapshot_path)}


def _profile_bytes(base_bytes: bytes, seed: int) -> bytes:
    if seed not in ALLOWED_SEEDS:
        raise RerunError(f"unsupported inference seed {seed}")
    try:
        before = json.loads(base_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RerunError("frozen model profile is not valid UTF-8 JSON") from exc
    if before.get("seed") != 42:
        raise RerunError("new freeze must use the protected seed42 model profile as its parent")
    if seed == 42:
        changed = base_bytes
    else:
        text = base_bytes.decode("utf-8")
        pattern = re.compile(r'(?m)^(\s*"seed"\s*:\s*)42(\s*,?\s*)$')
        updated, count = pattern.subn(lambda match: match.group(1) + str(seed) + match.group(2), text)
        if count != 1:
            raise RerunError("base profile must contain exactly one standalone seed 42 field")
        changed = updated.encode("utf-8")
    after = json.loads(changed)
    if (after.get("seed") != seed or
            {key: value for key, value in after.items() if key != "seed"} !=
            {key: value for key, value in before.items() if key != "seed"}):
        raise RerunError("derived model profile changed fields beyond the authorized inference seed")
    return changed


def _helper_hashes() -> dict[str, str]:
    return {path: _sha(ROOT / path) for path in HELPER_PATHS}


def _config_hashes() -> dict[str, str]:
    return {path: _sha(ROOT / path) for path in CONFIG_PATHS}


def _prepare(freeze_path: Path, output_root: Path, seeds: tuple[int, ...]) -> dict[str, Any]:
    if not seeds or len(set(seeds)) != len(seeds) or any(seed not in ALLOWED_SEEDS for seed in seeds):
        raise RerunError("seeds must be a nonempty unique subset of 42, 43, and 44")
    freeze_path = _inside_root(freeze_path, "freeze manifest", must_exist=True)
    output_root = _inside_root(output_root, "run root")
    if output_root.exists() or output_root.is_symlink():
        raise RerunError(f"run root already exists; preserving it: {output_root}")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from scripts.run_paper_starter_audit import _load_freeze

    freeze, verified_freeze_path = _load_freeze(freeze_path)
    if (freeze.get("scored_case_ids") != list(CASE_ORDER) or
            freeze.get("shard_groups") != {"local": list(CASE_ORDER[:5]),
                "wingtip": list(CASE_ORDER[5:])}):
        raise RerunError("freeze does not contain the fixed ten-case order and 5+5 split")
    base_profile_path = _inside_root(ROOT / freeze["model_profile_path"],
        "frozen model profile", must_exist=True)
    base_profile_bytes = base_profile_path.read_bytes()
    base_profile = json.loads(base_profile_bytes)
    if (_sha(base_profile_path) != freeze.get("model_profile_sha256") or
            base_profile.get("seed") != 42):
        raise RerunError("new freeze profile does not match the frozen seed42 profile")
    historical_report_path = ROOT / HISTORICAL_REPORT_PATH
    if historical_report_path.is_symlink() or not historical_report_path.is_file():
        raise RerunError("immutable historical seed-variability report is unavailable")
    historical_report_sha256 = _sha(historical_report_path)
    initial_library_pin = _library_snapshot_pin(
        freeze.get("initial_library_path"), freeze.get("initial_library_snapshot_id"),
        "initial Bank")
    canonical_origin = freeze.get("canonical_origin")
    if not isinstance(canonical_origin, dict):
        raise RerunError("freeze canonical-origin metadata is malformed")
    canonical_origin_pin = _library_snapshot_pin(
        freeze.get("canonical_origin_path"), canonical_origin.get("snapshot_id"),
        "canonical-origin")

    output_root.mkdir(parents=True, exist_ok=False)
    seed_rows: list[dict[str, Any]] = []
    for seed in seeds:
        seed_root = output_root / f"seed{seed}"
        seed_root.mkdir(exist_ok=False)
        model_run_id = f"e4r{seed}-{secrets.token_hex(4)}"
        model_run_root = ROOT / "artifacts/m3/model_runs" / model_run_id
        if model_run_root.exists() or model_run_root.is_symlink():
            raise RerunError(f"canonical model run root already exists: {model_run_root}")
        profile_path = seed_root / "profiles" / f"gemma_seed{seed}.json"
        profile_path.parent.mkdir()
        profile_path.write_bytes(_profile_bytes(base_profile_bytes, seed))
        profile_relative = profile_path.relative_to(ROOT).as_posix()
        profile_sha = _sha(profile_path)
        derived = copy.deepcopy(freeze)
        derived["model_profile_path"] = profile_relative
        derived["model_profile_sha256"] = profile_sha
        changed_keys = sorted(key for key in freeze
            if freeze[key] != derived.get(key))
        expected_changed_keys = (["model_profile_path"] if seed == 42 else
            ["model_profile_path", "model_profile_sha256"])
        if changed_keys != expected_changed_keys:
            raise RerunError(f"seed{seed} freeze changed fields beyond its profile pins")
        derived_path = seed_root / "derived_freeze_manifest.json"
        _write_new_json(derived_path, derived)
        seed_rows.append({"seed": seed,
            "seed_root": seed_root.relative_to(ROOT).as_posix(),
            "model_run_id": model_run_id,
            "model_run_root": model_run_root.relative_to(ROOT).as_posix(),
            "profile_path": profile_relative, "profile_sha256": profile_sha,
            "profile_seed_delta_only": True,
            "derived_freeze_path": derived_path.relative_to(ROOT).as_posix(),
            "derived_freeze_sha256": _sha(derived_path),
            "initial_library_snapshot_id": freeze["initial_library_snapshot_id"],
            "case_ids": list(CASE_ORDER),
            "shard_groups": freeze["shard_groups"],
            "expected_request_seed": seed})

    manifest_path = output_root / "rerun_manifest.json"
    helper_archive_path = output_root / "rerun_helper_sources.tar.gz"
    if helper_archive_path.exists() or helper_archive_path.is_symlink():
        raise RerunError("helper source archive path already exists")
    with tarfile.open(helper_archive_path, "x:gz") as archive:
        for relative in (*HELPER_PATHS, *CONFIG_PATHS):
            archive.add(ROOT / relative, arcname=relative, recursive=False)
        for row in seed_rows:
            for relative in (row["profile_path"], row["derived_freeze_path"]):
                archive.add(ROOT / relative, arcname=relative, recursive=False)
    manifest = {"schema_version": "e4-paper-starter-frozen-rerun/1",
        "status": "prepared_no_model_launch",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "new_freeze_path": verified_freeze_path.relative_to(ROOT).as_posix(),
        "new_freeze_sha256": _sha(verified_freeze_path),
        "freeze_run_id": freeze["run_id"],
        "source_archive_path": freeze["source_archive_path"],
        "source_archive_sha256": freeze["source_archive_sha256"],
        "historical_report_path": HISTORICAL_REPORT_PATH.as_posix(),
        "historical_report_sha256": historical_report_sha256,
        "implementation_fingerprint": freeze["e4_implementation_fingerprint"],
        "source_inventory_sha256": freeze["repository_source_inventory_sha256"],
        "pinned_files_sha256": freeze["pinned_files_sha256"],
        "runtime_source_pins": freeze["runtime_source_pins"],
        "baseline_manifest_path": freeze["baseline_manifest_path"],
        "baseline_manifest_sha256": freeze["baseline_manifest_sha256"],
        "baseline_reuse": {"manifest_path": freeze["baseline_manifest_path"],
            "manifest_sha256": freeze["baseline_manifest_sha256"],
            "historical_t2_inference_seed": 42,
            "interpretation": "The frozen audit combiner reuses the original seed42 T2 baseline only. It is not a seed-matched T2 result for derived E4 seeds43 or44; report those historical per-seed T2 results only from their separately pinned pilot records."},
        "model_manifest_path": freeze["model_manifest_path"],
        "model_manifest_sha256": freeze["model_manifest_sha256"],
        "model_checkpoint_sha256": freeze["model_checkpoint_sha256"],
        "model_revision": freeze["model_revision"],
        "base_profile_path": freeze["model_profile_path"],
        "base_profile_sha256": freeze["model_profile_sha256"],
        "base_profile_seed": base_profile["seed"],
        "config_sha256": _config_hashes(),
        "helper_source_sha256": _helper_hashes(),
        "helper_source_archive_path": helper_archive_path.relative_to(ROOT).as_posix(),
        "helper_source_archive_sha256": _sha(helper_archive_path),
        "dataset_seed": 42,
        "model_inference_seeds": list(seeds),
        "seed_scope": "only model profile inference seed changes; dataset and protected statistical/runtime seeds remain frozen",
        "case_order": list(CASE_ORDER),
        "shard_groups": freeze["shard_groups"],
        "initial_library_snapshot_id": freeze["initial_library_snapshot_id"],
        "initial_library_path": freeze["initial_library_path"],
        "initial_library_snapshot_file_path": initial_library_pin["snapshot_file_path"],
        "initial_library_snapshot_sha256": initial_library_pin["snapshot_file_sha256"],
        "canonical_origin_path": freeze["canonical_origin_path"],
        "canonical_origin_snapshot_id": freeze["canonical_origin"]["snapshot_id"],
        "canonical_origin_snapshot_file_path": canonical_origin_pin["snapshot_file_path"],
        "canonical_origin_snapshot_sha256": canonical_origin_pin["snapshot_file_sha256"],
        "initial_library_fresh_per_seed": True,
        "initial_library_fresh_per_shard": True,
        "cases_reused": True,
        "official_m5_scored_claim": False,
        "fresh_holdout_claim": False,
        "gpu_jobs_launched": False,
        "completion_requests_made": False,
        "trusted_evaluator_read": False,
        "launch_command_templates": {
            "local": [".venv/bin/python", "scripts/run_m3_model.py", "--profile",
                "<seed_root>/profiles/gemma_seed<seed>.json", "--run-id", "<model_run_id>",
                "--run-timeout", "40000", "--",
                ".venv/bin/python",
                "artifacts/escalation_fixes/implementation_20261026/launch/local_driver.py",
                "--seed", "<seed>", "--rerun-manifest", "<run_root>/rerun_manifest.json"],
            "wingtip": [".venv/bin/python",
                "artifacts/escalation_fixes/implementation_20261026/launch/remote_driver.py",
                "--seed", "<seed>", "--rerun-manifest", "<run_root>/rerun_manifest.json",
                "--audit-dir", "<audit_dir from launch_handoff.json>",
                "--provenance", "<fresh remote server provenance>", "--port", "<port>"]},
        "seeds": seed_rows}
    manifest["launch_commands_by_seed"] = {}
    for row in seed_rows:
        seed = row["seed"]
        manifest_rel = (output_root / "rerun_manifest.json").relative_to(ROOT).as_posix()
        manifest["launch_commands_by_seed"][str(seed)] = {
            "local": [".venv/bin/python", "scripts/run_m3_model.py", "--profile",
                row["profile_path"], "--run-id", row["model_run_id"], "--run-timeout", "40000",
                "--", ".venv/bin/python",
                "artifacts/escalation_fixes/implementation_20261026/launch/local_driver.py",
                "--seed", str(seed), "--rerun-manifest", manifest_rel],
            "wingtip": [".venv/bin/python",
                "artifacts/escalation_fixes/implementation_20261026/launch/remote_driver.py",
                "--seed", str(seed), "--rerun-manifest", manifest_rel,
                "--audit-dir", f"<from {row['model_run_root']}/launch_handoff.json>",
                "--provenance", "<fresh remote server provenance>", "--port", "<port>"]}
    _write_new_json(manifest_path, manifest)
    manifest_sha = _sha(manifest_path)
    for row in seed_rows:
        seed_root = ROOT / row["seed_root"]
        seed_manifest = {"schema_version": "e4-paper-starter-frozen-rerun-seed/1",
            "rerun_manifest_path": manifest_path.relative_to(ROOT).as_posix(),
            "rerun_manifest_sha256": manifest_sha, **row,
            "model_requests_started": False}
        seed_manifest_path = seed_root / "seed_manifest.json"
        _write_new_json(seed_manifest_path, seed_manifest)
        seal = {"schema_version": "e4-paper-starter-rerun-seal/1",
            "status": "sealed_before_model_requests",
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "seed": row["seed"],
            "rerun_manifest_path": manifest_path.relative_to(ROOT).as_posix(),
            "rerun_manifest_sha256": manifest_sha,
            "seed_manifest_path": seed_manifest_path.relative_to(ROOT).as_posix(),
            "seed_manifest_sha256": _sha(seed_manifest_path),
            "new_freeze_path": verified_freeze_path.relative_to(ROOT).as_posix(),
            "new_freeze_sha256": _sha(verified_freeze_path),
            "derived_freeze_path": row["derived_freeze_path"],
            "derived_freeze_sha256": row["derived_freeze_sha256"],
            "profile_path": row["profile_path"],
            "profile_sha256": row["profile_sha256"],
            "expected_request_seed": row["seed"],
            "helper_source_sha256": manifest["helper_source_sha256"],
            "config_sha256": manifest["config_sha256"],
            "initial_library_snapshot_id": row["initial_library_snapshot_id"],
            "completion_requests_started": False}
        _write_new_json(seed_root / "launch_seal.json", seal)

    return manifest


def validate_launch_bundle(manifest_path: Path, seed: int, *,
                           require_local_handoff: bool = False) -> tuple[dict[str, Any], dict[str, Any], Path]:
    """Verify every pre-request seal and the selected seed's current source pins."""
    if seed not in ALLOWED_SEEDS:
        raise RerunError("unsupported inference seed")
    manifest_path = _inside_root(manifest_path, "rerun manifest", must_exist=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise RerunError("rerun manifest must be a JSON object")
    seed_row = next((row for row in manifest.get("seeds", []) if row.get("seed") == seed), None)
    if (manifest.get("schema_version") != "e4-paper-starter-frozen-rerun/1" or
            manifest.get("status") != "prepared_no_model_launch" or not isinstance(seed_row, dict)):
        raise RerunError("rerun manifest does not authorize this seed")
    if (_sha(ROOT / manifest["new_freeze_path"]) != manifest["new_freeze_sha256"] or
            manifest.get("helper_source_sha256") != _helper_hashes() or
            manifest.get("config_sha256") != _config_hashes()):
        raise RerunError("freeze, helper source, or behavior config changed after sealing")
    historical_path = _inside_root(ROOT / manifest.get("historical_report_path", ""),
        "historical comparison report", must_exist=True)
    if _sha(historical_path) != manifest.get("historical_report_sha256"):
        raise RerunError("historical comparison report changed after sealing")
    for directory_key, file_key, id_key, digest_key in (
            ("initial_library_path", "initial_library_snapshot_file_path",
             "initial_library_snapshot_id", "initial_library_snapshot_sha256"),
            ("canonical_origin_path", "canonical_origin_snapshot_file_path",
             "canonical_origin_snapshot_id", "canonical_origin_snapshot_sha256")):
        pin = _library_snapshot_pin(manifest.get(directory_key), manifest.get(id_key),
            directory_key)
        if (pin["snapshot_file_path"] != manifest.get(file_key) or
                pin["snapshot_file_sha256"] != manifest.get(digest_key)):
            raise RerunError(f"frozen library snapshot file changed after sealing: {directory_key}")
    helper_archive = _inside_root(ROOT / manifest["helper_source_archive_path"],
        "helper source archive", must_exist=True)
    if _sha(helper_archive) != manifest.get("helper_source_archive_sha256"):
        raise RerunError("rerun helper source archive changed after sealing")
    if (manifest.get("model_inference_seeds") != [row["seed"] for row in manifest["seeds"]] or
            manifest.get("dataset_seed") != 42 or manifest.get("case_order") != list(CASE_ORDER) or
            manifest.get("shard_groups") != {"local": list(CASE_ORDER[:5]),
                "wingtip": list(CASE_ORDER[5:])}):
        raise RerunError("sealed case plan or protected dataset seed changed")

    seed_root = _inside_root(ROOT / seed_row["seed_root"], "seed output root", must_exist=True)
    seed_manifest_path = seed_root / "seed_manifest.json"
    seal_path = seed_root / "launch_seal.json"
    seed_manifest = json.loads(seed_manifest_path.read_text(encoding="utf-8"))
    seal = json.loads(seal_path.read_text(encoding="utf-8"))
    manifest_sha = _sha(manifest_path)
    if (seed_manifest.get("rerun_manifest_sha256") != manifest_sha or
            seed_manifest.get("seed") != seed or seal.get("seed") != seed or
            seed_manifest.get("model_requests_started") is not False or
            seal.get("completion_requests_started") is not False or
            seal.get("expected_request_seed") != seed or
            seal.get("rerun_manifest_sha256") != manifest_sha or
            seal.get("seed_manifest_sha256") != _sha(seed_manifest_path) or
            seal.get("new_freeze_path") != manifest.get("new_freeze_path") or
            seal.get("new_freeze_sha256") != manifest.get("new_freeze_sha256") or
            seal.get("derived_freeze_path") != seed_row["derived_freeze_path"] or
            seal.get("derived_freeze_sha256") != seed_row["derived_freeze_sha256"] or
            seal.get("profile_path") != seed_row["profile_path"] or
            seal.get("profile_sha256") != seed_row["profile_sha256"] or
            seal.get("initial_library_snapshot_id") !=
                seed_row["initial_library_snapshot_id"] or
            seal.get("helper_source_sha256") != _helper_hashes() or
            seal.get("config_sha256") != _config_hashes()):
        raise RerunError("per-seed launch seal is missing, mismatched, or stale")
    profile_path = _inside_root(ROOT / seed_row["profile_path"],
        "selected seed profile", must_exist=True)
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    derived_path = _inside_root(ROOT / seed_row["derived_freeze_path"],
        "derived seed freeze", must_exist=True)
    derived = json.loads(derived_path.read_text(encoding="utf-8"))
    if (profile.get("seed") != seed or _sha(profile_path) != seed_row["profile_sha256"] or
            _sha(derived_path) != seed_row["derived_freeze_sha256"] or
            derived.get("model_profile_path") != seed_row["profile_path"] or
            derived.get("model_profile_sha256") != seed_row["profile_sha256"] or
            seed_manifest.get("profile_path") != seed_row["profile_path"] or
            seed_manifest.get("derived_freeze_path") != seed_row["derived_freeze_path"] or
            seed_manifest.get("derived_freeze_sha256") != _sha(derived_path) or
            seed_manifest.get("profile_sha256") != _sha(profile_path)):
        raise RerunError("derived model profile or freeze differs from its sealed seed")
    base_freeze_path = _inside_root(ROOT / manifest["new_freeze_path"],
        "base freeze", must_exist=True)
    if _sha(base_freeze_path) != manifest.get("new_freeze_sha256"):
        raise RerunError("base freeze hash differs from the rerun manifest")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from scripts.run_paper_starter_audit import _load_freeze
    base_freeze, verified_freeze_path = _load_freeze(base_freeze_path)
    if (verified_freeze_path != base_freeze_path or
            base_freeze.get("run_id") != manifest.get("freeze_run_id") or
            base_freeze.get("repository_source_inventory_sha256") !=
                manifest.get("source_inventory_sha256") or
            base_freeze.get("runtime_source_pins") != manifest.get("runtime_source_pins") or
            base_freeze.get("model_manifest_sha256") != manifest.get("model_manifest_sha256") or
            base_freeze.get("model_checkpoint_sha256") != manifest.get("model_checkpoint_sha256") or
            base_freeze.get("model_revision") != manifest.get("model_revision") or
            base_freeze.get("initial_library_snapshot_id") !=
                manifest.get("initial_library_snapshot_id") or
            base_freeze.get("initial_library_path") != manifest.get("initial_library_path") or
            base_freeze.get("canonical_origin_path") != manifest.get("canonical_origin_path") or
            base_freeze.get("canonical_origin", {}).get("snapshot_id") !=
                manifest.get("canonical_origin_snapshot_id")):
        raise RerunError("new freeze no longer matches the sealed source/model/library metadata")
    base_freeze = json.loads(base_freeze_path.read_text(encoding="utf-8"))
    baseline_path = _inside_root(ROOT / manifest["baseline_manifest_path"],
        "historical baseline manifest", must_exist=True)
    if (_sha(baseline_path) != manifest.get("baseline_manifest_sha256") or
            base_freeze.get("baseline_manifest_path") != manifest.get("baseline_manifest_path") or
            base_freeze.get("baseline_manifest_sha256") != manifest.get("baseline_manifest_sha256") or
            manifest.get("baseline_reuse", {}).get("historical_t2_inference_seed") != 42):
        raise RerunError("historical T2 seed42 baseline reuse metadata changed")
    base_profile_path = _inside_root(ROOT / base_freeze["model_profile_path"],
        "base model profile", must_exist=True)
    base_profile = json.loads(base_profile_path.read_text(encoding="utf-8"))
    if ({key: value for key, value in profile.items() if key != "seed"} !=
            {key: value for key, value in base_profile.items() if key != "seed"} or
            {key: value for key, value in derived.items()
                if key not in {"model_profile_path", "model_profile_sha256"}} !=
            {key: value for key, value in base_freeze.items()
                if key not in {"model_profile_path", "model_profile_sha256"}}):
        raise RerunError("seed derived profile/freeze changes more than the permitted inference seed")
    model_run_parts = Path(seed_row["model_run_root"]).parts
    if (len(model_run_parts) != 4 or model_run_parts[:3] !=
            ("artifacts", "m3", "model_runs") or
            model_run_parts[-1] != seed_row.get("model_run_id") or
            not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", str(seed_row.get("model_run_id")))):
        raise RerunError("sealed canonical model run ID/path is malformed")

    if require_local_handoff:
        model_run_root = _inside_root(ROOT / seed_row["model_run_root"],
            "canonical model run root")
        handoff_path = model_run_root / "launch_handoff.json"
        if not handoff_path.is_file() or handoff_path.is_symlink():
            raise RerunError("local driver has not produced a valid launch handoff")
        handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
        audit_dir = _inside_root(ROOT / handoff.get("audit_dir", ""),
            "audit directory", must_exist=True)
        if (handoff.get("status") not in {"local_shard_starting", "complete"} or
                not audit_dir.is_relative_to(model_run_root) or
                handoff.get("run_root") != seed_row["model_run_root"] or
                handoff.get("freeze_manifest_sha256") != _sha(derived_path)):
            raise RerunError("local handoff does not belong to this seed's frozen run")
        audit_manifest_path = audit_dir / "audit_manifest.json"
        audit_manifest = json.loads(audit_manifest_path.read_text(encoding="utf-8"))
        if (audit_manifest.get("freeze_manifest_sha256") != _sha(derived_path) or
                audit_manifest.get("case_ids") != list(CASE_ORDER) or
                audit_manifest.get("shard_groups") != manifest["shard_groups"] or
                audit_manifest.get("initial_library_snapshot_id") !=
                    manifest["initial_library_snapshot_id"]):
            raise RerunError("prepared audit manifest changed or belongs to another frozen run")
    return manifest, seed_row, seed_root


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze-manifest", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--seeds", nargs="+", type=int, default=[42, 43, 44])
    args = parser.parse_args()
    manifest = _prepare(args.freeze_manifest, args.run_root, tuple(args.seeds))
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
