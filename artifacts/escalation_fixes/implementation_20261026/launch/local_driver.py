#!/usr/bin/env python3
"""Seal-checking local wrapper for the shared paper-starter audit driver."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from artifacts.escalation_fixes.implementation_20261026.launch.prepare_frozen_rerun import (
    validate_launch_bundle,
)
from artifacts.escalation_fixes.implementation_20261026.launch.worker_profile import (
    bind_selected_profile, verify_shard_request_seeds,
)


def _run_paper_audit(audit_module: Any, paper_run: Any, freeze_path: Path, *,
                     seed: int, seed_row: Mapping[str, Any]) -> dict[str, Any]:
    with bind_selected_profile(audit_module, seed=seed,
            profile_relative=seed_row["profile_path"],
            profile_sha256=seed_row["profile_sha256"]):
        return paper_run(freeze_path)


def run(seed: int, rerun_manifest: Path) -> dict[str, Any]:
    run_dir_value = os.environ.get("E4_RUN_DIR")
    if not run_dir_value:
        raise RuntimeError("local rerun driver must run inside scripts/run_m3_model.py")
    manifest, seed_row, _seed_root = validate_launch_bundle(rerun_manifest, seed)
    run_dir = Path(run_dir_value).resolve(strict=True)
    model_run_root = (ROOT / seed_row["model_run_root"]).resolve(strict=True)
    if run_dir != model_run_root:
        raise RuntimeError("model launcher run root differs from the sealed canonical model-run directory")
    from artifacts.paper_starter.launch.local_driver import run as paper_run
    from scripts import run_paper_starter_audit as audit

    freeze_path = ROOT / seed_row["derived_freeze_path"]
    result = _run_paper_audit(audit, paper_run, freeze_path,
        seed=seed, seed_row=seed_row)
    handoff = json.loads((run_dir / "launch_handoff.json").read_text(encoding="utf-8"))
    audit_dir = ROOT / handoff["audit_dir"]
    audit_manifest_path = audit_dir / "audit_manifest.json"
    audit_manifest = json.loads(audit_manifest_path.read_text(encoding="utf-8"))
    if (handoff.get("freeze_manifest_sha256") != seed_row["derived_freeze_sha256"] or
            audit_manifest.get("model_profile_sha256") != seed_row["profile_sha256"] or
            audit_manifest.get("case_ids") != manifest["case_order"] or
            audit_manifest.get("shard_groups") != manifest["shard_groups"]):
        raise RuntimeError("prepared audit did not preserve the sealed seed, case order, and shard split")
    shard_root = audit_dir / "bank10_local"
    shard_state = json.loads((shard_root / "bank10_status.json").read_text(encoding="utf-8"))
    proof = verify_shard_request_seeds(shard_state, shard_root, shard="local", seed=seed,
        profile_relative=seed_row["profile_path"], profile_sha256=seed_row["profile_sha256"],
        expected_cases=manifest["shard_groups"]["local"],
        initial_snapshot_id=manifest["initial_library_snapshot_id"])
    proof_path = run_dir / "local_request_seed_proof.json"
    fd = os.open(proof_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(proof, ensure_ascii=False, sort_keys=True,
            indent=2, allow_nan=False) + "\n")
    return {"status": result["status"], "seed": seed,
        "metadata_seed_root": seed_row["seed_root"],
        "run_root": seed_row["model_run_root"], "audit_dir": handoff["audit_dir"],
        "audit_manifest_sha256": _sha(audit_manifest_path),
        "request_seed_proof_path": proof_path.relative_to(ROOT).as_posix(),
        "request_seed_proof_sha256": _sha(proof_path),
        "freeze_manifest_sha256": seed_row["derived_freeze_sha256"],
        "profile_sha256": seed_row["profile_sha256"]}


def _sha(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", required=True, type=int, choices=(42, 43, 44))
    parser.add_argument("--rerun-manifest", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.seed, args.rerun_manifest), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
