#!/usr/bin/env python3
"""Artifact-only E4 seed-variability pilot launcher.

The pilot keeps the run06 source, controller, policies, budgets, evaluator, and
initial Bank library fixed. It prepares immutable profile/freeze/audit artifacts
for decoding seeds 43 and 44. Run only after the root orchestrator starts the
appropriate local or Wingtip model server.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[3]
LAUNCH_ROOT = Path(__file__).resolve().parent
BASE_FREEZE_PATH = ROOT / "artifacts/paper_starter/20261024T090946Z-49d25e18/freeze_manifest.json"
BASE_FREEZE_SHA256 = "d7cb721d44cf11fb56b44e0933bca64d2dcb3fdeedf8733b12c29142175e1d4c"
EXPECTED_STARTER_ID = "35c9231e85d55abd0a07aff9a86104b86e5af0d80b923a80a8361cd1db2863c2"
PILOT_SCHEMA = "e4-m5-seed-variability-pilot/1"
SEEDS = (43, 44)
CASE_ORDER = (
    "bank-20210304T0100-utc8", "bank-20210304T1800-utc8",
    "bank-20210306T0330-utc8", "bank-20210307T0300-utc8",
    "bank-20210307T0330-utc8", "bank-20210309T0800-utc8",
    "bank-20210309T1730-utc8", "bank-20210312T2030-utc8",
    "bank-20210324T1100-utc8", "bank-20210325T2000-utc8",
)


class PilotError(RuntimeError):
    pass


def _sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha(path: Path) -> str:
    return _sha_bytes(path.read_bytes())


def _hash_json(value: Any) -> str:
    return _sha_bytes(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8"))


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + ".pending")
    pending.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(pending, path)


def _audit_module():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from scripts import run_paper_starter_audit as audit
    return audit


def _base_frozen_inputs() -> tuple[dict[str, Any], Any, Path, dict[str, Any], Path]:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from e4core.m4_library import snapshot_library

    if _sha(BASE_FREEZE_PATH) != BASE_FREEZE_SHA256:
        raise PilotError("run06 freeze manifest hash differs from the approved base")
    audit = _audit_module()
    freeze, freeze_path = audit._load_freeze(BASE_FREEZE_PATH)
    if freeze.get("scored_case_ids") != list(CASE_ORDER):
        raise PilotError("run06 case order differs from the fixed pilot order")
    if freeze.get("shard_groups") != {"local": list(CASE_ORDER[:5]),
                                      "wingtip": list(CASE_ORDER[5:])}:
        raise PilotError("run06 shard split differs from the fixed pilot split")
    starter_path = ROOT / freeze["initial_library_path"]
    starter = snapshot_library(starter_path)
    if starter.snapshot_id != EXPECTED_STARTER_ID:
        raise PilotError("run06 starter snapshot identity changed")
    base_profile_path = ROOT / freeze["model_profile_path"]
    profile = json.loads(base_profile_path.read_text(encoding="utf-8"))
    if profile.get("seed") != 42 or _sha(base_profile_path) != freeze["model_profile_sha256"]:
        raise PilotError("the protected seed42 base profile differs from the run06 freeze")
    return freeze, starter, base_profile_path, profile, freeze_path


def _profile_copy(seed: int, base_bytes: bytes) -> bytes:
    if seed not in SEEDS:
        raise PilotError(f"seed must be one of {SEEDS}")
    text = base_bytes.decode("utf-8")
    pattern = re.compile(r'(?m)^(\s*"seed"\s*:\s*)42(\s*,?\s*)$')
    changed, count = pattern.subn(lambda match: match.group(1) + str(seed) + match.group(2), text)
    if count != 1:
        raise PilotError("base profile must contain exactly one standalone seed 42 field")
    before = json.loads(base_bytes)
    after = json.loads(changed)
    if after.get("seed") != seed or {k: v for k, v in after.items() if k != "seed"} != {
            k: v for k, v in before.items() if k != "seed"}:
        raise PilotError("seed copy changed a field other than inference seed")
    return changed.encode("utf-8")


def _helper_hashes() -> dict[str, str]:
    paths = (
        "scripts/run_paper_starter_audit.py", "scripts/run_m4_bank.py",
        "scripts/run_m3_model.py", "e4core/m4_controller.py",
        "e4core/m4_library.py", "e4core/m4_data.py", "e4core/m5_evaluation.py",
        "config/m4_runtime.json", "config/paper_starter_runtime.json",
    )
    return {path: _sha(ROOT / path) for path in paths}


def _e4_helper_hashes() -> dict[str, str]:
    paths = (
        "artifacts/seed_variability/launch/pilot.py",
        "artifacts/seed_variability/launch/remote_driver.py",
        "artifacts/seed_variability/launch/smoke_test_pilot.py",
        "artifacts/paper_starter/launch/local_driver.py",
        "artifacts/paper_starter/launch/remote_driver.py",
        "scripts/run_paper_starter_audit.py", "scripts/run_m4_bank.py",
        "scripts/run_m3_model.py", "e4core/m4_controller.py",
    )
    return {path: _sha(ROOT / path) for path in paths}


def seal_e4_seed(seed: int) -> dict[str, Any]:
    """Bind per-seed E4 helper sources and pilot manifest before any requests."""
    if seed not in SEEDS:
        raise PilotError("unsupported seed")
    pilot_path = LAUNCH_ROOT / "pilot_manifest.json"
    pilot = json.loads(pilot_path.read_text(encoding="utf-8"))
    seed_row = json.loads((LAUNCH_ROOT / f"seed{seed}/seed_manifest.json").read_text(encoding="utf-8"))
    if (seed_row.get("pilot_manifest_sha256") != _sha(pilot_path) or
            seed_row.get("profile_sha256") != pilot["seed_profiles"][str(seed)]["sha256"]):
        raise PilotError("seed manifest does not match the shared pilot profile pins")
    freeze_path = ROOT / seed_row["derived_freeze_path"]
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    profile_path = ROOT / freeze["model_profile_path"]
    helpers = _e4_helper_hashes()
    seal = {
        "schema_version": "e4-seed-variability-e4-launch-seal/1",
        "status": "sealed_before_model_requests",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "seed": seed,
        "pilot_manifest_path": str(pilot_path.relative_to(ROOT)),
        "pilot_manifest_sha256": _sha(pilot_path),
        "derived_freeze_path": str(freeze_path.relative_to(ROOT)),
        "derived_freeze_sha256": _sha(freeze_path),
        "profile_path": str(profile_path.relative_to(ROOT)),
        "profile_sha256": _sha(profile_path),
        "expected_request_seed": seed,
        "initial_library_snapshot_id": EXPECTED_STARTER_ID,
        "helper_source_sha256": helpers,
        "model_requests_started": False,
    }
    path = LAUNCH_ROOT / f"seed{seed}/e4_launch_seal.json"
    if path.exists() or path.is_symlink():
        raise PilotError("E4 launch seal already exists; preserving the first seal")
    _atomic_json(path, seal)
    return seal


def _verify_e4_seal(seed: int) -> dict[str, Any]:
    path = LAUNCH_ROOT / f"seed{seed}/e4_launch_seal.json"
    seal = json.loads(path.read_text(encoding="utf-8"))
    pilot_path = ROOT / seal["pilot_manifest_path"]
    freeze_path = ROOT / seal["derived_freeze_path"]
    profile_path = ROOT / seal["profile_path"]
    if (seal.get("seed") != seed or seal.get("expected_request_seed") != seed or
            seal.get("pilot_manifest_sha256") != _sha(pilot_path) or
            seal.get("derived_freeze_sha256") != _sha(freeze_path) or
            seal.get("profile_sha256") != _sha(profile_path) or
            seal.get("helper_source_sha256") != _e4_helper_hashes()):
        raise PilotError("sealed E4 profile/helper provenance changed before launch")
    return seal


def namespace_profile_smoke(seed: int) -> dict[str, Any]:
    """Run a no-model request-payload check inside the real restricted project view."""
    if seed not in SEEDS:
        raise PilotError("unsupported seed")
    from scripts import run_m4_bank as supervisor
    from e4core.cache import implementation_fingerprint

    seed_manifest = json.loads((LAUNCH_ROOT / f"seed{seed}/seed_manifest.json").read_text())
    profile_path = ROOT / seed_manifest["profile_path"]
    with tempfile.TemporaryDirectory(prefix=f"seed{seed}_view_", dir=LAUNCH_ROOT) as view_tmp, \
         tempfile.TemporaryDirectory(prefix=f"seed{seed}_run_", dir=LAUNCH_ROOT) as run_tmp:
        view_root = Path(view_tmp) / "project"
        run_dir = Path(run_tmp) / "case"
        run_dir.mkdir()
        pins = supervisor.create_restricted_project_view(ROOT, view_root,
            profile_path=profile_path, case_id=None, split="scored")
        relative = pins["profile_relative_path"]
        if relative != seed_manifest["profile_path"] or pins["view_profile_sha256"] != seed_manifest["profile_sha256"]:
            raise PilotError("restricted worker view did not copy the exact selected seed profile")
        expected_view_path = "/m4project/" + relative
        worker_source = (
            "import json,os; from pathlib import Path; "
            "from e4core.m4_controller import M4ModelClient; "
            "path=Path(os.environ['E4_MODEL_PROFILE']); data=json.loads(path.read_text()); "
            "payload=M4ModelClient(data).payload_for_stage('compose','profile propagation smoke'); "
            "print(json.dumps({'path':str(path),'profile_seed':data['seed'],'payload_seed':payload['seed'],"
            "'model':payload['model']}))"
        )
        command = supervisor._bwrap_command(view_root=view_root, run_dir=run_dir,
            library_root=None, profile_relative=relative, mode="bootstrap",
            case_id=None, split="scored", endpoint="http://127.0.0.1:8080",
            feedback_socket=None, implementation_fingerprint_value=implementation_fingerprint(),
            command_override=["/m4project/.venv/bin/python", "-c", worker_source])
        completed = subprocess.run(command, cwd=ROOT, env={}, capture_output=True,
            text=True, timeout=180, check=False)
        if completed.returncode != 0:
            raise PilotError(f"restricted no-model profile smoke failed: {completed.stderr[-1000:]}")
        output = json.loads(completed.stdout.strip().splitlines()[-1])
        if (output.get("path") != expected_view_path or output.get("profile_seed") != seed or
                output.get("payload_seed") != seed):
            raise PilotError("restricted worker used a different profile or request seed")
        return {"seed": seed, "profile_path_in_view": expected_view_path,
            "profile_sha256": seed_manifest["profile_sha256"],
            "profile_seed": output["profile_seed"], "payload_seed": output["payload_seed"],
            "model_alias": output["model"], "completion_requests_made": 0,
            "worker_command_profile_path": relative,
            "implementation_fingerprint": implementation_fingerprint()}


def _write_profile_and_freeze(seed: int, freeze: Mapping[str, Any],
                              base_profile_path: Path, base_profile: Mapping[str, Any],
                              seed_root: Path) -> dict[str, Any]:
    seed_root.mkdir(parents=True, exist_ok=False)
    profile_rel = Path("artifacts/seed_variability/launch") / f"seed{seed}" / "profiles" / f"gemma_seed{seed}.json"
    profile_path = ROOT / profile_rel
    profile_path.parent.mkdir(parents=True, exist_ok=False)
    profile_bytes = _profile_copy(seed, base_profile_path.read_bytes())
    profile_path.write_bytes(profile_bytes)
    profile_sha = _sha(profile_path)

    derived = copy.deepcopy(dict(freeze))
    derived["model_profile_path"] = profile_rel.as_posix()
    derived["model_profile_sha256"] = profile_sha
    changed_keys = sorted(key for key in freeze
        if freeze[key] != derived.get(key))
    if changed_keys != ["model_profile_path", "model_profile_sha256"]:
        raise PilotError(f"derived freeze changed unexpected keys: {changed_keys}")
    derived_path = seed_root / "derived_freeze_manifest.json"
    _atomic_json(derived_path, derived)

    return {
        "seed": seed,
        "profile_path": profile_rel.as_posix(),
        "profile_sha256": profile_sha,
        "profile_seed_delta_only": True,
        "base_profile_path": str(base_profile_path.relative_to(ROOT)),
        "base_profile_sha256": _sha(base_profile_path),
        "derived_freeze_path": str(derived_path.relative_to(ROOT)),
        "derived_freeze_sha256": _sha(derived_path),
        "changed_freeze_keys": changed_keys,
    }


def prepare_all() -> dict[str, Any]:
    """Create per-seed profiles, derived freeze manifests and fresh audit roots."""
    if (LAUNCH_ROOT / "pilot_manifest.json").exists():
        raise PilotError("pilot manifest already exists; preserving the first preparation")
    freeze, starter, base_profile_path, profile, freeze_path = _base_frozen_inputs()
    seed_details = []
    for seed in SEEDS:
        seed_root = LAUNCH_ROOT / f"seed{seed}"
        details = _write_profile_and_freeze(seed, freeze, base_profile_path, profile, seed_root)
        audit = _audit_module()
        audit_dir = audit.prepare(ROOT / details["derived_freeze_path"],
                                  audit_root=seed_root / "audits")
        audit_manifest_path = audit_dir / "audit_manifest.json"
        audit_manifest = json.loads(audit_manifest_path.read_text(encoding="utf-8"))
        if (audit_manifest.get("initial_library_snapshot_id") != EXPECTED_STARTER_ID or
                audit_manifest.get("case_ids") != list(CASE_ORDER) or
                audit_manifest.get("shard_groups") != {"local": list(CASE_ORDER[:5]),
                                                        "wingtip": list(CASE_ORDER[5:])}):
            raise PilotError("prepared audit does not match the fresh run06 starter/order")
        details["audit_dir"] = str(audit_dir.relative_to(ROOT))
        details["audit_manifest_sha256"] = _sha(audit_manifest_path)
        details["audit_initial_library_snapshot_id"] = audit_manifest["initial_library_snapshot_id"]
        details["audit_initial_library_path"] = audit_manifest["initial_library_path"]
        seed_details.append(details)

    starter_path = Path(starter.path)
    baseline_path = ROOT / freeze["baseline_manifest_path"]
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    resource_budgets = baseline.get("resource_budgets")
    if not isinstance(resource_budgets, Mapping):
        raise PilotError("frozen T2 resource budgets are missing")
    profile_seed_delta_only = all(row["profile_seed_delta_only"] for row in seed_details)
    manifest = {
        "schema_version": PILOT_SCHEMA,
        "status": "prepared_no_model_launch",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "pilot_scope": "E4 decoding-seed variability only; same ten run06 scored incidents and original starter per seed",
        "systems": ["E4", "Terminus-2"],
        "decoding_seeds": list(SEEDS),
        "seed_scope": "inference profile seed only; protected runtime/algorithm seed stays frozen at42",
        "base_run06_freeze_path": str(freeze_path.relative_to(ROOT)),
        "base_run06_freeze_sha256": _sha(freeze_path),
        "run06_freeze_path": str(freeze_path.relative_to(ROOT)),
        "run06_freeze_sha256": _sha(freeze_path),
        "base_run_id": freeze["run_id"],
        "source_archive_path": freeze["source_archive_path"],
        "source_archive_sha256": freeze["source_archive_sha256"],
        "implementation_fingerprint": freeze["e4_implementation_fingerprint"],
        "source_inventory_sha256": freeze["repository_source_inventory_sha256"],
        "model_manifest_path": freeze["model_manifest_path"],
        "model_manifest_sha256": freeze["model_manifest_sha256"],
        "model_checkpoint_sha256": freeze["model_checkpoint_sha256"],
        "model_revision": freeze["model_revision"],
        "base_model_profile_path": freeze["model_profile_path"],
        "base_model_profile_sha256": freeze["model_profile_sha256"],
        "base_model_profile_seed": profile["seed"],
        "profile_seed_delta_only": profile_seed_delta_only,
        "initial_library_path": freeze["initial_library_path"],
        "initial_library_snapshot_id": starter.snapshot_id,
        "initial_bank_snapshot_id": starter.snapshot_id,
        "initial_library_snapshot_path": str(starter_path.relative_to(ROOT)),
        "initial_library_snapshot_sha256": _sha(starter_path),
        "initial_library_snapshot_fresh_per_seed": True,
        "canonical_origin_snapshot_id": freeze["canonical_origin"]["snapshot_id"],
        "case_order": list(CASE_ORDER),
        "case_ids": list(CASE_ORDER),
        "shard_groups": {"local": list(CASE_ORDER[:5]), "wingtip": list(CASE_ORDER[5:])},
        "bank_sample_sha256": freeze["bank_sample_sha256"],
        "baseline_manifest_path": freeze["baseline_manifest_path"],
        "baseline_manifest_sha256": freeze["baseline_manifest_sha256"],
        "t2_frozen_resource_budgets": dict(resource_budgets),
        "e4_runtime_pins": freeze["runtime_source_pins"],
        "helper_hashes": _helper_hashes(),
        "seeds": seed_details,
        "seed_profiles": {str(row["seed"]): {
            "path": row["profile_path"], "sha256": row["profile_sha256"],
            "seed_delta_only": row["profile_seed_delta_only"]} for row in seed_details},
        "gpu_jobs_launched": False,
        "completion_requests_made": False,
        "trusted_evaluator_read": False,
    }
    pilot_manifest_path = LAUNCH_ROOT / "pilot_manifest.json"
    _atomic_json(pilot_manifest_path, manifest)
    pilot_sha = _sha(pilot_manifest_path)
    for details in seed_details:
        seed_root = ROOT / f"artifacts/seed_variability/launch/seed{details['seed']}"
        receipt = {
            "schema_version": "e4-seed-variability-seed/1",
            "pilot_manifest_path": str(pilot_manifest_path.relative_to(ROOT)),
            "pilot_manifest_sha256": pilot_sha,
            **details,
            "worker_profile_path_in_view": "/m4project/" + details["profile_path"],
            "expected_request_seed": details["seed"],
            "expected_initial_library_snapshot_id": EXPECTED_STARTER_ID,
            "helper_hashes": manifest["helper_hashes"],
            "model_launch_performed": False,
        }
        _atomic_json(seed_root / "seed_manifest.json", receipt)
    return manifest


def _profile_relative_for_seed(seed: int, audit_dir: Path) -> str:
    manifest_path = audit_dir / "audit_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    derived = ROOT / manifest["freeze_manifest_path"]
    freeze = json.loads(derived.read_text(encoding="utf-8"))
    profile_relative = str(freeze["model_profile_path"])
    parts = Path(profile_relative).parts
    if (not parts or parts[0] != "artifacts" or ".." in parts or
            f"seed{seed}" not in parts):
        raise PilotError("derived profile path is not the selected seed's project-relative artifact")
    profile_path = ROOT / profile_relative
    if _sha(profile_path) != freeze["model_profile_sha256"]:
        raise PilotError("derived seed profile hash changed")
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    if profile.get("seed") != seed:
        raise PilotError("derived profile inference seed does not match selected pilot seed")
    return Path(profile_relative).as_posix()


def _worker_command_for_profile(case_id: str, case_root: Path, library_root: Path,
                                view_root: Path, feedback_address: str, *,
                                remote: bool, profile_relative: str) -> list[str]:
    """Clone the frozen audit worker wiring with only the profile path parameterized."""
    from scripts import run_m4_bank as supervisor
    from e4core.cache import implementation_fingerprint

    parts = Path(profile_relative).parts
    if (not parts or parts[0] != "artifacts" or ".." in parts or
            not any(part.startswith("seed") for part in parts)):
        raise PilotError("worker profile path must be a project-relative seed artifact")
    worker_profile = "/m4project/" + Path(profile_relative).as_posix()
    profile_literal = json.dumps(worker_profile)
    source = (
        "import sys; from pathlib import Path; "
        "from scripts.run_m4_bank import BinaryFeedbackProxy; "
        "from e4core.m4_controller import run_diagnostic; "
        "proxy=BinaryFeedbackProxy(sys.argv[1]); "
        "run_diagnostic(sys.argv[2], library_root=Path('/m4library'), "
        "run_dir=Path('/m4run/diagnostic'), feedback=proxy, "
        f"profile_path=Path({profile_literal}), "
        "root=Path('/m4project'), split='scored'); proxy.close()"
    )
    command = ["/m4project/.venv/bin/python", "-c", source, feedback_address, case_id]
    built = supervisor._bwrap_command(view_root=view_root, run_dir=case_root,
        library_root=library_root, profile_relative=profile_relative,
        mode="diagnostic", case_id=case_id, split="scored",
        endpoint=os.environ.get("E4_BASE_URL", "http://127.0.0.1:8080"),
        feedback_socket=feedback_address,
        implementation_fingerprint_value=implementation_fingerprint(),
        command_override=command)
    if remote:
        separator = built.index("--")
        built[separator:separator] = ["--dir", "/run", "--dir", "/run/e4remote",
            "--ro-bind", "/run/e4remote-source", "/run/e4remote"]
    return built


def _verify_shard_request_seed(state: Mapping[str, Any], shard_root: Path,
                               seed: int, initial_snapshot_id: str) -> dict[str, Any]:
    requests = []
    cases = state.get("completed_cases")
    if not isinstance(cases, list) or not cases:
        raise PilotError("completed shard contains no case records")
    expected_cases = list(CASE_ORDER[:5]) if state.get("shard") == "local" else list(CASE_ORDER[5:])
    if [row.get("case_id") for row in cases] != expected_cases:
        raise PilotError("completed shard case order differs from fixed pilot order")
    if cases[0].get("input_library_snapshot_id") != initial_snapshot_id:
        raise PilotError("seed shard did not start from the fresh original run06 starter")
    for index, row in enumerate(cases, start=1):
        result_path = ROOT / row["diagnostic_result_path"]
        calls_path = result_path.parent / "model_calls.jsonl"
        if not calls_path.is_file():
            raise PilotError(f"case{index:02d} has no request log")
        for line in calls_path.read_text(encoding="utf-8").splitlines():
            call = json.loads(line)
            payload = call.get("request_payload")
            if not isinstance(payload, Mapping) or payload.get("seed") != seed:
                raise PilotError(f"case{index:02d} request payload seed differs from {seed}")
            requests.append({"case_id": row["case_id"],
                "call_id": call.get("call_id"),
                "request_payload_sha256": _hash_json(payload),
                "request_seed": payload["seed"],
                "model": payload.get("model")})
    if not requests:
        raise PilotError("completed shard emitted no model request payloads")
    return {"schema_version": "e4-seed-request-propagation/1", "seed": seed,
        "shard": state["shard"], "request_count": len(requests),
        "all_request_seeds_match": True,
        "initial_library_snapshot_id": initial_snapshot_id,
        "case_ids": expected_cases, "requests": requests}


def run_shard(seed: int, shard: str, audit_dir: Path, server_receipt: Path, *,
              proof_path: Path | None = None) -> dict[str, Any]:
    if seed not in SEEDS or shard not in {"local", "wingtip"}:
        raise PilotError("unsupported seed or shard")
    _verify_e4_seal(seed)
    audit = _audit_module()
    audit_dir = audit_dir.resolve(strict=True)
    profile_relative = _profile_relative_for_seed(seed, audit_dir)
    original_worker = audit._worker_command
    def seed_worker(case_id, case_root, library_root, view_root, feedback_address, *, remote):
        return _worker_command_for_profile(case_id, case_root, library_root,
            view_root, feedback_address, remote=remote, profile_relative=profile_relative)
    audit._worker_command = seed_worker
    try:
        state = audit.run_shard(audit_dir, shard, server_receipt=server_receipt)
    finally:
        audit._worker_command = original_worker
    run_root = audit_dir / f"bank10_{shard}"
    seed_manifest = json.loads((LAUNCH_ROOT / f"seed{seed}" / "seed_manifest.json").read_text())
    proof = _verify_shard_request_seed(state, run_root, seed,
        seed_manifest["expected_initial_library_snapshot_id"])
    proof_path = proof_path or LAUNCH_ROOT / f"seed{seed}" / f"{shard}_request_seed_proof.json"
    _atomic_json(proof_path, proof)
    return state


def run_local_seed(seed: int) -> dict[str, Any]:
    """Use the frozen local driver/receipt/preflight with only the worker profile path bound."""
    if seed not in SEEDS:
        raise PilotError("unsupported seed")
    _verify_e4_seal(seed)
    from importlib.util import module_from_spec, spec_from_file_location

    seed_manifest_path = LAUNCH_ROOT / f"seed{seed}/seed_manifest.json"
    seed_manifest = json.loads(seed_manifest_path.read_text(encoding="utf-8"))
    freeze_path = ROOT / seed_manifest["derived_freeze_path"]
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    profile_relative = _profile_relative_for_seed(seed, ROOT / seed_manifest["audit_dir"])

    driver_path = ROOT / "artifacts/paper_starter/launch/local_driver.py"
    spec = spec_from_file_location(f"seedpilot_local_driver_{seed}", driver_path)
    if spec is None or spec.loader is None:
        raise PilotError("frozen local launch driver cannot be loaded")
    driver = module_from_spec(spec)
    spec.loader.exec_module(driver)

    audit = _audit_module()
    original_worker = audit._worker_command
    def seed_worker(case_id, case_root, library_root, view_root, feedback_address, *, remote):
        return _worker_command_for_profile(case_id, case_root, library_root,
            view_root, feedback_address, remote=remote, profile_relative=profile_relative)
    audit._worker_command = seed_worker
    try:
        handoff = driver.run(freeze_path)
    finally:
        audit._worker_command = original_worker

    audit_dir = ROOT / handoff["audit_dir"]
    state_path = audit_dir / "bank10_local" / "bank10_status.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    run_dir = Path(os.environ["E4_RUN_DIR"]).resolve(strict=True)
    proof = _verify_shard_request_seed(state, audit_dir / "bank10_local", seed,
        seed_manifest["expected_initial_library_snapshot_id"])
    _atomic_json(run_dir / "e4_profile_propagation_proof.json", proof)
    handoff["e4_profile_propagation_proof_path"] = str(
        (run_dir / "e4_profile_propagation_proof.json").relative_to(ROOT))
    handoff["e4_profile_propagation_proof_sha256"] = _sha(run_dir / "e4_profile_propagation_proof.json")
    return handoff


def preflight_seed(seed: int, backend: str, output_dir: Path) -> dict[str, Any]:
    if seed not in SEEDS:
        raise PilotError("unsupported seed")
    _verify_e4_seal(seed)
    seed_root = LAUNCH_ROOT / f"seed{seed}"
    seed_manifest = json.loads((seed_root / "seed_manifest.json").read_text(encoding="utf-8"))
    freeze_path = ROOT / seed_manifest["derived_freeze_path"]
    audit = _audit_module()
    return audit.preflight(freeze_path, output_dir, backend=backend)


def combine_seed(seed: int, audit_dir: Path, local_status: Path, wingtip_status: Path) -> dict[str, Any]:
    if seed not in SEEDS:
        raise PilotError("unsupported seed")
    audit = _audit_module()
    result = audit.combine(audit_dir, local_status, wingtip_status)
    return result


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare", help="prepare profiles/freeze/audit roots for seeds 43 and 44")
    p_seal = sub.add_parser("seal", help="seal E4 helpers and profiles before requests")
    p_seal.add_argument("--seed", type=int, choices=SEEDS, required=True)
    p_smoke = sub.add_parser("namespace-smoke", help="test restricted worker profile propagation without a model call")
    p_smoke.add_argument("--seed", type=int, choices=SEEDS, required=True)
    p_preflight = sub.add_parser("preflight", help="tokenizer-only per-seed prompt preflight")
    p_preflight.add_argument("--seed", type=int, choices=SEEDS, required=True)
    p_preflight.add_argument("--backend", choices=("local", "wingtip"), required=True)
    p_preflight.add_argument("--output-dir", type=Path, required=True)
    p_local = sub.add_parser("run-local", help="run local E4 shard with seed-bound worker profile")
    p_local.add_argument("--seed", type=int, choices=SEEDS, required=True)
    p_shard = sub.add_parser("run-shard", help="run one existing protected audit shard")
    p_shard.add_argument("--seed", type=int, choices=SEEDS, required=True)
    p_shard.add_argument("--shard", choices=("local", "wingtip"), required=True)
    p_shard.add_argument("--audit-dir", type=Path, required=True)
    p_shard.add_argument("--server-receipt", type=Path, required=True)
    p_combine = sub.add_parser("combine", help="use the unchanged audit reconciliation helper")
    p_combine.add_argument("--seed", type=int, choices=SEEDS, required=True)
    p_combine.add_argument("--audit-dir", type=Path, required=True)
    p_combine.add_argument("--local-status", type=Path, required=True)
    p_combine.add_argument("--wingtip-status", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            result = prepare_all()
        elif args.command == "seal":
            result = seal_e4_seed(args.seed)
        elif args.command == "namespace-smoke":
            result = namespace_profile_smoke(args.seed)
        elif args.command == "preflight":
            result = preflight_seed(args.seed, args.backend, args.output_dir)
        elif args.command == "run-local":
            result = run_local_seed(args.seed)
        elif args.command == "run-shard":
            result = run_shard(args.seed, args.shard, args.audit_dir, args.server_receipt)
        else:
            result = combine_seed(args.seed, args.audit_dir, args.local_status, args.wingtip_status)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"seed variability pilot failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_cli())
