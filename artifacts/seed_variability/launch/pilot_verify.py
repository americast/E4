"""Independent, read-only provenance/cost verifier for the seed-variability pilot.

The verifier reads manifests, request payload fields, runtime snapshots, and
resource counters only. It never opens trusted evaluation ledgers or predicts
correctness; the root finalizer owns trusted rescoring.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[3]
LAUNCH_ROOT = ROOT / "artifacts/seed_variability/launch"
EXPECTED_CASES = (
    "bank-20210304T0100-utc8", "bank-20210304T1800-utc8",
    "bank-20210306T0330-utc8", "bank-20210307T0300-utc8",
    "bank-20210307T0330-utc8", "bank-20210309T0800-utc8",
    "bank-20210309T1730-utc8", "bank-20210312T2030-utc8",
    "bank-20210324T1100-utc8", "bank-20210325T2000-utc8",
)
SEEDS = (43, 44)
_E4_HELPERS = (
    "artifacts/seed_variability/launch/pilot.py",
    "artifacts/seed_variability/launch/remote_driver.py",
    "artifacts/seed_variability/launch/smoke_test_pilot.py",
    "artifacts/paper_starter/launch/local_driver.py",
    "artifacts/paper_starter/launch/remote_driver.py",
    "scripts/run_paper_starter_audit.py", "scripts/run_m4_bank.py",
    "scripts/run_m3_model.py", "e4core/m4_controller.py",
)


class PilotVerificationError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _load(path: Path) -> Any:
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise PilotVerificationError(f"missing or symlinked artifact: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PilotVerificationError(f"invalid JSON artifact {path}: {exc}") from exc


def _project_path(value: str) -> Path:
    if not isinstance(value, str):
        raise PilotVerificationError("artifact path must be a string")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise PilotVerificationError(f"artifact path is not safe and project-relative: {value}")
    path = (ROOT / relative).resolve(strict=True)
    if not path.is_relative_to(ROOT.resolve()):
        raise PilotVerificationError(f"artifact path escapes the project root: {value}")
    return path


def _resolve_recorded_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(ROOT.resolve()):
            raise PilotVerificationError(f"recorded path escapes project root: {value}")
        return resolved
    return _project_path(value)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise PilotVerificationError("recorded timestamp must be text")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PilotVerificationError(f"malformed timestamp: {value}") from exc
    return result if result.tzinfo is not None else result.replace(tzinfo=timezone.utc)


def verify_profile_delta(base: Mapping[str, Any], derived: Mapping[str, Any], *, seed: int) -> None:
    if seed not in SEEDS or base.get("seed") != 42 or derived.get("seed") != seed:
        raise PilotVerificationError("profile seeds do not match base42 and requested pilot seed")
    if {key: value for key, value in base.items() if key != "seed"} != {
            key: value for key, value in derived.items() if key != "seed"}:
        raise PilotVerificationError("derived profile changes a field other than decoding seed")


def verify_request_payload(payload: Mapping[str, Any], profile: Mapping[str, Any], *,
                           seed: int, label: str) -> None:
    expected = {"seed": seed, "temperature": profile["temperature"],
        "top_p": profile["top_p"], "top_k": profile["top_k"]}
    mismatches = {key: (payload.get(key), value) for key, value in expected.items()
                  if payload.get(key) != value}
    maximum = profile["max_generation_tokens"]
    if type(payload.get("max_tokens")) is not int or not 1 <= payload["max_tokens"] <= maximum:
        mismatches["max_tokens"] = (payload.get("max_tokens"), f"1..{maximum}")
    if mismatches:
        raise PilotVerificationError(f"{label} request differs from derived profile: {mismatches}")


def verify_t2_request_payload(payload: Mapping[str, Any], profile: Mapping[str, Any], *,
                              seed: int, label: str) -> None:
    top_k = payload.get("extra_body", {}).get("top_k") if isinstance(payload.get("extra_body"), Mapping) else None
    expected = {"seed": seed, "temperature": profile["temperature"], "top_p": profile["top_p"],
                "top_k": profile["top_k"], "model": f"openai/{profile['model_alias']}"}
    mismatches = {key: (payload.get(key), value) for key, value in expected.items()
                  if (top_k if key == "top_k" else payload.get(key)) != value}
    maximum = profile["max_generation_tokens"]
    if type(payload.get("max_tokens")) is not int or not 1 <= payload["max_tokens"] <= maximum:
        mismatches["max_tokens"] = (payload.get("max_tokens"), f"1..{maximum}")
    if mismatches:
        raise PilotVerificationError(f"{label} request differs from derived T2 profile: {mismatches}")


def verify_prepared_pilot(*, launch_root: Path = LAUNCH_ROOT) -> dict[str, Any]:
    """Check the two seed profiles/freezes are strict run06 derivations."""
    launch_root = Path(launch_root).resolve(strict=True)
    pilot_path = launch_root / "pilot_manifest.json"
    pilot = _load(pilot_path)
    if pilot.get("schema_version") != "e4-m5-seed-variability-pilot/1":
        raise PilotVerificationError("unsupported seed pilot manifest schema")
    if (pilot.get("status") != "prepared_no_model_launch" or
            pilot.get("gpu_jobs_launched") is not False or
            pilot.get("completion_requests_made") is not False or
            pilot.get("trusted_evaluator_read") is not False):
        raise PilotVerificationError("prepared manifest reports model/evaluator activity")
    if pilot.get("decoding_seeds") != list(SEEDS) or pilot.get("case_order") != list(EXPECTED_CASES):
        raise PilotVerificationError("pilot seed list or fixed case order changed")
    if pilot.get("case_ids") != list(EXPECTED_CASES):
        raise PilotVerificationError("pilot incident IDs differ from the fixed case order")
    expected_shards = {"local": list(EXPECTED_CASES[:5]), "wingtip": list(EXPECTED_CASES[5:])}
    if pilot.get("shard_groups") != expected_shards:
        raise PilotVerificationError("E4 five-case shard grouping changed")

    run06_path = _project_path(pilot["base_run06_freeze_path"])
    if sha256_file(run06_path) != pilot.get("base_run06_freeze_sha256"):
        raise PilotVerificationError("run06 base freeze hash changed")
    run06 = _load(run06_path)
    if (run06.get("run_id") != pilot.get("base_run_id") or
            run06.get("scored_case_ids") != list(EXPECTED_CASES) or
            run06.get("shard_groups") != expected_shards or
            run06.get("initial_library_snapshot_id") != pilot.get("initial_library_snapshot_id")):
        raise PilotVerificationError("run06 freeze differs in case order, shards, or starter snapshot")
    archive = _project_path(run06["source_archive_path"])
    if (sha256_file(archive) != run06.get("source_archive_sha256") or
            pilot.get("source_archive_sha256") != run06.get("source_archive_sha256") or
            pilot.get("implementation_fingerprint") != run06.get("e4_implementation_fingerprint")):
        raise PilotVerificationError("run06 source archive/fingerprint changed")
    for relative, expected in pilot.get("helper_hashes", {}).items():
        if sha256_file(_project_path(relative)) != expected:
            raise PilotVerificationError(f"frozen core helper changed: {relative}")
    # Bind no-inference preflight classifications to the implementation that
    # was frozen for run06, not to a free-form error string in the journal.
    preflight_source = "e4core/model_client.py"
    expected_preflight_source_sha = run06.get("repository_source_files_sha256", {}).get(preflight_source)
    if (not isinstance(expected_preflight_source_sha, str) or
            sha256_file(ROOT / preflight_source) != expected_preflight_source_sha):
        raise PilotVerificationError("model-client preflight classification source is not pinned by run06")

    baseline_path = _project_path(pilot["baseline_manifest_path"])
    if sha256_file(baseline_path) != pilot.get("baseline_manifest_sha256"):
        raise PilotVerificationError("historical T2 freeze hash changed")
    baseline = _load(baseline_path)
    if (baseline.get("seed") != 42 or baseline.get("case_ids") != list(EXPECTED_CASES) or
            dict(baseline.get("resource_budgets", {})) != pilot.get("t2_frozen_resource_budgets")):
        raise PilotVerificationError("historical T2 dataset seed, cases, or budgets changed")
    sample = _load(ROOT / "config/bank_sample.json")
    if (sample.get("selection", {}).get("seed") != 42 or
            sample.get("selection", {}).get("scored_ids") != list(EXPECTED_CASES)):
        raise PilotVerificationError("Bank sample seed42 or scored case order changed")

    base_profile_path = _project_path(pilot["base_model_profile_path"])
    if sha256_file(base_profile_path) != pilot.get("base_model_profile_sha256"):
        raise PilotVerificationError("protected base profile hash changed")
    base_profile = _load(base_profile_path)
    reports = {}
    for seed in SEEDS:
        seed_dir = launch_root / f"seed{seed}"
        seed_manifest_path = seed_dir / "seed_manifest.json"
        seed_manifest = _load(seed_manifest_path)
        profile_info = pilot.get("seed_profiles", {}).get(str(seed), {})
        if (seed_manifest.get("seed") != seed or
                seed_manifest.get("pilot_manifest_sha256") != sha256_file(pilot_path) or
                seed_manifest.get("profile_sha256") != profile_info.get("sha256") or
                seed_manifest.get("expected_request_seed") != seed or
                seed_manifest.get("model_launch_performed") is not False):
            raise PilotVerificationError(f"seed{seed} manifest differs from prepared root profile pins")
        profile_path = _project_path(profile_info["path"])
        profile = _load(profile_path)
        verify_profile_delta(base_profile, profile, seed=seed)
        if sha256_file(profile_path) != profile_info["sha256"]:
            raise PilotVerificationError(f"seed{seed} profile digest changed")
        derived_path = _project_path(seed_manifest["derived_freeze_path"])
        derived = _load(derived_path)
        expected_freeze = dict(run06)
        expected_freeze["model_profile_path"] = profile_info["path"]
        expected_freeze["model_profile_sha256"] = profile_info["sha256"]
        if derived != expected_freeze or sha256_file(derived_path) != seed_manifest.get("derived_freeze_sha256"):
            raise PilotVerificationError(f"seed{seed} freeze differs from run06 beyond profile path/hash")
        if seed_manifest.get("changed_freeze_keys") != ["model_profile_path", "model_profile_sha256"]:
            raise PilotVerificationError(f"seed{seed} records unexpected freeze changes")
        audit_dir = _project_path(seed_manifest["audit_dir"])
        audit_path = audit_dir / "audit_manifest.json"
        audit = _load(audit_path)
        if (sha256_file(audit_path) != seed_manifest.get("audit_manifest_sha256") or
                audit.get("case_ids") != list(EXPECTED_CASES) or
                audit.get("shard_groups") != expected_shards or
                audit.get("initial_library_snapshot_id") != pilot.get("initial_library_snapshot_id") or
                audit.get("model_profile_sha256") != profile_info.get("sha256")):
            raise PilotVerificationError(f"seed{seed} audit manifest differs from base profile/cases/starter")
        reports[str(seed)] = {"profile_sha256": profile_info["sha256"],
            "seed_only_delta": True,
            "derived_freeze_sha256": sha256_file(derived_path),
            "audit_manifest_sha256": sha256_file(audit_path),
            "initial_library_snapshot_id": audit["initial_library_snapshot_id"]}
    return {"status": "prepared_inputs_verified", "pilot_manifest_sha256": sha256_file(pilot_path),
        "run06_freeze_sha256": sha256_file(run06_path),
        "context_preflight_source_path": preflight_source,
        "context_preflight_source_sha256": expected_preflight_source_sha,
        "historical_t2_manifest_sha256": sha256_file(baseline_path),
        "case_count": 10, "case_order": list(EXPECTED_CASES),
        "shard_groups": expected_shards, "seeds": reports,
        "evaluator_or_accuracy_read": False}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if path.is_symlink() or not path.is_file():
        raise PilotVerificationError(f"missing or symlinked request journal: {path}")
    rows = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PilotVerificationError(f"invalid JSON journal line {path}:{index}") from exc
        if not isinstance(value, dict):
            raise PilotVerificationError(f"non-object journal line {path}:{index}")
        rows.append(value)
    return rows


def _request_totals(rows: list[dict[str, Any]], profile: Mapping[str, Any], seed: int,
                    *, t2: bool, label: str,
                    context_preflight_source_sha256: str | None = None) -> dict[str, Any]:
    prompt_tokens = completion_tokens = 0
    preflight_prompt_tokens = 0
    preflight_rejected_attempts = 0
    unknown_unreached_attempts = 0
    incomplete_http_usage = 0
    preflight_http_ops = 0
    conservative_budget_tokens = 0
    stage_counts: dict[str, int] = {}
    request_hashes = []
    confirmed_no_inference_calls = []
    for row in rows:
        if t2:
            if row.get("event") != "request_started":
                continue
            payload = row.get("request")
            if not isinstance(payload, Mapping):
                raise PilotVerificationError(f"{label} request_started lacks payload")
            verify_t2_request_payload(payload, profile, seed=seed, label=label)
            request_hashes.append(canonical_sha256(payload))
            call_id = row.get("call_id")
            # Response records are accounted by joining on call_id after parsing.
        else:
            payload = row.get("request_payload")
            if not isinstance(payload, Mapping):
                raise PilotVerificationError(f"{label} E4 call lacks actual request payload")
            verify_request_payload(payload, profile, seed=seed, label=label)
            request_hashes.append(canonical_sha256(payload))
            stage = row.get("m4_stage")
            if stage not in {"stage1_select", "stage2_compose", "stage3_synthesize"}:
                raise PilotVerificationError(f"{label} E4 call has unknown stage")
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
            usage = row.get("usage") if isinstance(row.get("usage"), Mapping) else {}
            status_code = row.get("http_status")
            error = row.get("error") if isinstance(row.get("error"), Mapping) else {}
            error_code = error.get("code")
            generation_elapsed = row.get("generation_elapsed_seconds")
            preflight_rows = row.get("preflight_calls")
            if isinstance(preflight_rows, list):
                preflight_http_ops += len(preflight_rows)
            preflight_succeeded = (isinstance(preflight_rows, list) and len(preflight_rows) == 2 and
                [item.get("endpoint") for item in preflight_rows if isinstance(item, Mapping)] ==
                    ["/apply-template", "/tokenize"] and
                all(item.get("http_status") == 200 and item.get("error") is None
                    for item in preflight_rows if isinstance(item, Mapping)))
            budget_rejected = (error_code == "context_budget_exceeded" and
                type(error.get("prompt_tokens")) is int and
                type(error.get("max_generation_tokens")) is int and
                type(error.get("context_tokens")) is int and
                error["prompt_tokens"] + error["max_generation_tokens"] > error["context_tokens"])
            preflight_rejected = (context_preflight_source_sha256 is not None and
                    error_code == "context_budget_exceeded" and budget_rejected and
                    preflight_succeeded) or (context_preflight_source_sha256 is not None and
                    error_code == "context_preflight_error" and generation_elapsed == 0 and
                    isinstance(preflight_rows, list) and len(preflight_rows) >= 1)
            if status_code is None and generation_elapsed == 0 and preflight_rejected:
                # Local /apply-template and /tokenize succeeded or rejected before
                # the /v1/chat/completions request. Tokenizer counts are recorded
                # separately and are not model prompt/completion usage.
                preflight_rejected_attempts += 1
                preflight_count = error.get("prompt_tokens") if budget_rejected else row.get("prompt_token_count")
                if type(preflight_count) is int:
                    preflight_prompt_tokens += preflight_count
                requested = row.get("m4_requested_generation_tokens")
                if type(requested) is int:
                    conservative_budget_tokens += requested
                confirmed_no_inference_calls.append({"call_id": row.get("call_id"),
                    "stage": row.get("m4_stage"), "error_code": error_code,
                    "http_status": None, "generation_elapsed_seconds": 0.0,
                    "preflight_endpoints": [item.get("endpoint") for item in preflight_rows
                        if isinstance(item, Mapping)],
                    "preflight_statuses": [item.get("http_status") for item in preflight_rows
                        if isinstance(item, Mapping)],
                    "preflight_prompt_tokens_not_inference_usage": preflight_count,
                    "classification_source_path": "e4core/model_client.py",
                    "classification_source_sha256": context_preflight_source_sha256,
                    "confirmed_inference_prompt_tokens": 0,
                    "confirmed_inference_completion_tokens": 0,
                    "requested_generation_budget_tokens": requested})
            elif status_code is None:
                unknown_unreached_attempts += 1
                requested = row.get("m4_requested_generation_tokens")
                if type(requested) is int:
                    conservative_budget_tokens += requested
            else:
                prompt_value = usage.get("prompt_tokens", usage.get("input_tokens"))
                completion_value = usage.get("completion_tokens", usage.get("output_tokens"))
                if type(prompt_value) is int:
                    prompt_tokens += prompt_value
                if type(completion_value) is int:
                    completion_tokens += completion_value
                    conservative_budget_tokens += completion_value
                if type(prompt_value) is not int or type(completion_value) is not int:
                    incomplete_http_usage += 1
                    requested = row.get("m4_requested_generation_tokens")
                    if type(completion_value) is not int and type(requested) is int:
                        conservative_budget_tokens += requested

    if t2:
        # Request and response rows are interleaved in the existing journal.
        response_by_call = {row.get("call_id"): row for row in rows
                            if row.get("event") == "response_received"}
        request_rows = [row for row in rows if row.get("event") == "request_started"]
        for row in request_rows:
            response = response_by_call.get(row.get("call_id"), {})
            if type(response.get("prompt_tokens")) is int and type(response.get("completion_tokens")) is int:
                prompt_tokens += response["prompt_tokens"]
                completion_tokens += response["completion_tokens"]
                conservative_budget_tokens += response["completion_tokens"]
            else:
                incomplete_http_usage += 1
    if not request_hashes:
        raise PilotVerificationError(f"{label} has no outgoing model requests")
    if not t2:
        controller_call_attempts = len(request_hashes)
        http_inference_requests = sum(1 for row in rows if row.get("http_status") is not None)
        if controller_call_attempts != http_inference_requests + preflight_rejected_attempts + unknown_unreached_attempts:
            raise PilotVerificationError(f"{label} model-call attempts do not reconcile to HTTP/preflight outcomes")
    else:
        controller_call_attempts = len(request_hashes)
        http_inference_requests = len(request_hashes)
    return {"controller_call_attempts": controller_call_attempts,
        "http_inference_requests": http_inference_requests,
        "request_payload_sha256": request_hashes,
        "prompt_tokens_observed": prompt_tokens if incomplete_http_usage == 0 and unknown_unreached_attempts == 0 else None,
        "completion_tokens_observed": completion_tokens if incomplete_http_usage == 0 and unknown_unreached_attempts == 0 else None,
        "preflight_prompt_tokens_not_billed": preflight_prompt_tokens,
        "preflight_rejected_attempts": preflight_rejected_attempts,
        "preflight_http_operations": preflight_http_ops,
        "confirmed_no_inference_calls": confirmed_no_inference_calls,
        "conservative_generated_budget_tokens_charged": conservative_budget_tokens,
        "unclassified_unreached_attempts": unknown_unreached_attempts,
        "http_calls_with_incomplete_usage": incomplete_http_usage,
        "usage_complete": incomplete_http_usage == 0 and unknown_unreached_attempts == 0,
        "online_inference_usage_basis": "observed HTTP response usage plus run06-source-pinned preflight early returns",
        "classification_source_sha256": context_preflight_source_sha256,
        "stage_call_counts": stage_counts}


def verify_e4_seed(seed: int, run_root: Path, *, launch_root: Path = LAUNCH_ROOT) -> dict[str, Any]:
    prepared = verify_prepared_pilot(launch_root=launch_root)
    if seed not in SEEDS:
        raise PilotVerificationError("unsupported E4 pilot seed")
    launch_root = Path(launch_root).resolve(strict=True)
    supplied_root = _resolve_recorded_path(str(run_root))
    if (supplied_root.name == f"e4s{seed}" and
            supplied_root.parent == (ROOT / "artifacts/m3/model_runs").resolve()):
        run_root = supplied_root
        requested_audit_root = None
    elif (supplied_root.parent.name == "audits" and
            supplied_root.parent.parent.name == f"e4s{seed}" and
            supplied_root.parent.parent.parent == (ROOT / "artifacts/m3/model_runs").resolve()):
        # The launch handoff gives an exact audit-root path. Accepting it makes
        # the caller's selected trajectory explicit instead of silently falling
        # back to a prepared audit location.
        run_root = supplied_root.parent.parent
        requested_audit_root = supplied_root
    else:
        raise PilotVerificationError(f"seed{seed} E4 run/audit root is outside its fixed run ID")
    run_doc = _load(run_root / "run.json")
    outer_doc = _load(run_root / "outer-supervisor.json")
    if (run_doc.get("ok") is not True or run_doc.get("exit_code") != 0 or
            run_doc.get("command_exit_code") != 0 or outer_doc.get("exit_code") != 0 or
            outer_doc.get("bubblewrap_exit_code") != 0 or outer_doc.get("error") is not None):
        raise PilotVerificationError(f"seed{seed} E4 launcher/outer supervisor did not complete successfully")
    seed_dir = launch_root / f"seed{seed}"
    seal = _load(seed_dir / "e4_launch_seal.json")
    pilot_path = launch_root / "pilot_manifest.json"
    pilot = _load(pilot_path)
    seed_manifest = _load(seed_dir / "seed_manifest.json")
    freeze_path = _project_path(seed_manifest["derived_freeze_path"])
    profile_path = _project_path(seed_manifest["profile_path"])
    if (seal.get("seed") != seed or seal.get("expected_request_seed") != seed or
            seal.get("model_requests_started") is not False or
            seal.get("pilot_manifest_sha256") != sha256_file(pilot_path) or
            seal.get("derived_freeze_sha256") != sha256_file(freeze_path) or
            seal.get("profile_sha256") != sha256_file(profile_path)):
        raise PilotVerificationError(f"seed{seed} E4 launch seal is invalid")
    for relative, digest in seal.get("helper_source_sha256", {}).items():
        if sha256_file(_project_path(relative)) != digest:
            raise PilotVerificationError(f"seed{seed} E4 helper seal changed: {relative}")
    profile = _load(profile_path)
    local_handoff_path = run_root / "local_shard_handoff.json"
    remote_handoff_path = run_root / "wingtip_shard_handoff.json"
    local_handoff, remote_handoff = _load(local_handoff_path), _load(remote_handoff_path)
    if (local_handoff.get("run_root") != str(run_root.relative_to(ROOT)) or
            local_handoff.get("status") != "complete" or
            remote_handoff.get("status") != "complete"):
        raise PilotVerificationError(f"seed{seed} E4 local/Wingtip handoff is incomplete")
    audit_dir = _project_path(local_handoff["audit_dir"])
    if not audit_dir.is_relative_to((run_root / "audits").resolve()):
        raise PilotVerificationError(f"seed{seed} handoff audit directory is not inside the supplied E4 run root")
    if requested_audit_root is not None and requested_audit_root != audit_dir:
        raise PilotVerificationError(f"seed{seed} supplied E4 audit root differs from its run handoff")
    actual_audit = _load(audit_dir / "audit_manifest.json")
    if (remote_handoff.get("audit_dir") != str(audit_dir.relative_to(ROOT)) or
            actual_audit.get("freeze_manifest_sha256") != sha256_file(freeze_path) or
            actual_audit.get("case_ids") != list(EXPECTED_CASES) or
            actual_audit.get("shard_groups") != {
                "local": list(EXPECTED_CASES[:5]), "wingtip": list(EXPECTED_CASES[5:])} or
            actual_audit.get("initial_library_snapshot_id") !=
                prepared["seeds"][str(seed)]["initial_library_snapshot_id"] or
            actual_audit.get("model_profile_sha256") != sha256_file(profile_path)):
        raise PilotVerificationError(f"seed{seed} completed E4 audit differs from prepared source/profile/starter")
    local_receipt_path = _project_path(local_handoff["local_server_receipt_path"])
    remote_receipt_path = run_root / "wingtip_server_receipt.json"
    local_receipt, remote_receipt = _load(local_receipt_path), _load(remote_receipt_path)
    profile_sha = sha256_file(profile_path)
    if (sha256_file(local_receipt_path) != local_handoff.get("local_server_receipt_sha256") or
            local_receipt.get("profile_sha256") != profile_sha or
            sha256_file(remote_receipt_path) != remote_handoff.get("remote_server_receipt_sha256") or
            remote_receipt.get("profile_sha256") != profile_sha or
            remote_receipt.get("client_profile_seed") != seed or
            remote_receipt.get("server_base_profile_sha256") !=
                sha256_file(_project_path(pilot["base_model_profile_path"]))):
        raise PilotVerificationError(f"seed{seed} E4 server receipts differ from derived client profile")
    reports = {}
    total_calls = total_prompt = total_completion = 0
    total_prompt_known = total_completion_known = True
    first_call_started: datetime | None = None
    initial_id = prepared["seeds"][str(seed)]["initial_library_snapshot_id"]
    for shard, case_ids in (("local", list(EXPECTED_CASES[:5])),
                            ("wingtip", list(EXPECTED_CASES[5:]))):
        shard_root = audit_dir / f"bank10_{shard}"
        if not shard_root.is_dir():
            raise PilotVerificationError(f"seed{seed} E4 shard missing: {shard}")
        expected_case_dirs = {f"case{index:02d}" for index in range(1, 6)}
        observed_case_dirs = {path.name for path in shard_root.iterdir()
                              if path.is_dir() and path.name.startswith("case")}
        if observed_case_dirs != expected_case_dirs:
            raise PilotVerificationError(f"seed{seed} E4 {shard} trajectory directory set changed")
        previous_library = initial_id
        case_reports = []
        for index, case_id in enumerate(case_ids, 1):
            case_root = shard_root / f"case{index:02d}"
            supervisor = _load(case_root / "supervisor_manifest.json")
            if (supervisor.get("case_id") != case_id or
                    supervisor.get("input_library_snapshot_id") != previous_library or
                    supervisor.get("profile_sha256") != sha256_file(profile_path)):
                raise PilotVerificationError(f"seed{seed} {shard} case{index:02d} supervisor pins differ")
            view_entry = supervisor.get("restricted_view_file_inventory", {}).get(
                seed_manifest["profile_path"])
            if not isinstance(view_entry, Mapping) or view_entry.get("sha256") != sha256_file(profile_path):
                raise PilotVerificationError(f"seed{seed} {shard} case{index:02d} view lacks derived profile")
            diagnostic_dir = case_root / "diagnostic"
            diag_manifest = _load(diagnostic_dir / "diagnostic_manifest.json")
            if diag_manifest.get("profile_sha256") != sha256_file(profile_path):
                raise PilotVerificationError(f"seed{seed} {shard} case{index:02d} controller profile differs")
            calls_path = diagnostic_dir / "model_calls.jsonl"
            call_rows = _read_jsonl(calls_path)
            totals = _request_totals(call_rows, profile, seed, t2=False,
                                     label=f"seed{seed} {shard} {case_id}",
                                     context_preflight_source_sha256=prepared["context_preflight_source_sha256"])
            call_times = [_timestamp(row["started_utc"]) for row in call_rows
                          if isinstance(row.get("started_utc"), str)]
            if call_times:
                current_first = min(call_times)
                first_call_started = current_first if first_call_started is None else min(
                    first_call_started, current_first)
            runtime_seeds = []
            for runtime_path in sorted((diagnostic_dir / "attempts").glob("*/runtime.json")):
                runtime_doc = _load(runtime_path)
                runtime_seeds.append(runtime_doc.get("values", {}).get("seed"))
            if not runtime_seeds or set(runtime_seeds) != {42}:
                raise PilotVerificationError(f"seed{seed} {shard} {case_id} algorithm runtime seed changed")
            current_path = diagnostic_dir / "library" / "CURRENT"
            if current_path.is_symlink() or not current_path.is_file():
                raise PilotVerificationError(f"seed{seed} {shard} {case_id} output library missing")
            current_id = current_path.read_text(encoding="utf-8").strip()
            if len(current_id) != 64:
                raise PilotVerificationError(f"seed{seed} {shard} {case_id} output library identity malformed")
            diagnostic_result_path = diagnostic_dir / "diagnostic_result.json"
            diagnostic_result = _load(diagnostic_result_path)
            aggregate = diagnostic_result.get("model_calls")
            aggregate = aggregate if isinstance(aggregate, Mapping) else {}
            case_reports.append({"case_id": case_id, **totals,
                # Keep controller-provided raw aggregate values exactly as
                # recorded (including null); separately report journal-derived
                # online inference usage below.
                "raw_aggregate_prompt_tokens": aggregate.get("prompt_tokens"),
                "raw_aggregate_completion_tokens": aggregate.get("completion_tokens"),
                "derived_online_inference_prompt_tokens": totals["prompt_tokens_observed"],
                "derived_online_inference_completion_tokens": totals["completion_tokens_observed"],
                "runtime_seed_values": [42], "output_library_snapshot_id": current_id,
                "diagnostic_result_sha256": sha256_file(diagnostic_result_path)})
            previous_library = current_id
            total_calls += totals["controller_call_attempts"]
            if type(totals["prompt_tokens_observed"]) is int:
                total_prompt += totals["prompt_tokens_observed"]
            else:
                total_prompt_known = False
            if type(totals["completion_tokens_observed"]) is int:
                total_completion += totals["completion_tokens_observed"]
            else:
                total_completion_known = False
        reports[shard] = {"case_ids": case_ids, "initial_library_snapshot_id": initial_id,
            "final_library_snapshot_id": previous_library, "cases": case_reports}
    if first_call_started is None or _timestamp(seal["created_utc"]) > first_call_started:
        raise PilotVerificationError(f"seed{seed} E4 helper seal is absent or postdates its first controller call")
    return {"seed": seed, "execution_root": str(run_root.relative_to(ROOT)),
        "audit_dir": str(audit_dir.relative_to(ROOT)), "case_count": 10, "shards": reports,
        "total_controller_call_attempts": total_calls,
        "observed_prompt_tokens": total_prompt if total_prompt_known else None,
        "observed_completion_tokens": total_completion if total_completion_known else None,
        "derived_online_inference_prompt_tokens": total_prompt if total_prompt_known else None,
        "derived_online_inference_completion_tokens": total_completion if total_completion_known else None,
        "prompt_token_total_complete": total_prompt_known,
        "completion_token_total_complete": total_completion_known,
        "evaluator_or_accuracy_read": False}


def verify_t2_seed(seed: int, run_root: Path, *, launch_root: Path = LAUNCH_ROOT) -> dict[str, Any]:
    prepared = verify_prepared_pilot(launch_root=launch_root)
    if seed not in SEEDS:
        raise PilotVerificationError("unsupported T2 pilot seed")
    run_root = Path(run_root).resolve(strict=True)
    pilot_path = Path(launch_root).resolve(strict=True) / "pilot_manifest.json"
    pilot = _load(pilot_path)
    profile_path = _project_path(pilot["seed_profiles"][str(seed)]["path"])
    profile = _load(profile_path)
    checkpoint = _load(run_root / "t2_seed_checkpoint.json")
    if (checkpoint.get("status") != "complete" or checkpoint.get("seed") != seed or
            checkpoint.get("dataset_seed") != 42 or checkpoint.get("case_order") != list(EXPECTED_CASES) or
            checkpoint.get("pilot_manifest_sha256") != sha256_file(pilot_path) or
            checkpoint.get("profile_sha256") != sha256_file(profile_path) or
            checkpoint.get("initial_library_snapshot_id") != prepared["seeds"][str(seed)]["initial_library_snapshot_id"]):
        raise PilotVerificationError(f"seed{seed} T2 checkpoint is incomplete or differs from frozen inputs")
    helper_receipt = _load(run_root / "t2_seed_helper_receipt.json")
    if (helper_receipt.get("seed") != seed or helper_receipt.get("profile_sha256") != sha256_file(profile_path) or
            helper_receipt.get("pilot_manifest_sha256") != sha256_file(pilot_path) or
            helper_receipt.get("initial_library_snapshot_id") != checkpoint["initial_library_snapshot_id"]):
        raise PilotVerificationError(f"seed{seed} T2 helper seal/receipt differs from frozen profile or starter")
    for relative, digest in helper_receipt.get("helper_hashes", {}).items():
        if sha256_file(_project_path(relative)) != digest:
            raise PilotVerificationError(f"seed{seed} T2 launch helper changed: {relative}")
    jobs = checkpoint.get("jobs")
    if not isinstance(jobs, Mapping) or list(jobs) != list(EXPECTED_CASES):
        raise PilotVerificationError(f"seed{seed} T2 job chain differs from fixed case order")
    reports = []
    for case_id in EXPECTED_CASES:
        job = jobs[case_id].get("Terminus-2") if isinstance(jobs[case_id], Mapping) else None
        if not isinstance(job, Mapping) or job.get("status") != "complete":
            raise PilotVerificationError(f"seed{seed} T2 case incomplete: {case_id}")
        history = job.get("history", [])
        # Preserve all trajectories. Resumed infrastructure attempts must be
        # accounted for before a complete result can be summarized.
        if history:
            raise PilotVerificationError(f"seed{seed} T2 case {case_id} has prior attempts requiring explicit cost reconciliation")
        artifact_root = _project_path(str(Path(job["artifact_root"]).resolve().relative_to(ROOT)))
        audit_path = artifact_root / "output" / "t2_seed_request_audit.json"
        proof = _load(audit_path)
        journal = _project_path(proof["model_http_calls_path"])
        if (proof.get("schema_version") != "e4-t2-seed-request-audit/1" or
                proof.get("seed") != seed or proof.get("all_request_seeds_match") is not True or
                sha256_file(journal) != proof.get("model_http_calls_sha256")):
            raise PilotVerificationError(f"seed{seed} T2 request audit invalid: {case_id}")
        binding_path = _project_path(proof["profile_binding_path"])
        if sha256_file(binding_path) != proof.get("profile_binding_sha256"):
            raise PilotVerificationError(f"seed{seed} T2 profile-binding hash changed: {case_id}")
        binding = _load(binding_path)
        if (binding.get("seed") != seed or binding.get("profile_sha256") != sha256_file(profile_path) or
                binding.get("profile_changes_only_seed") is not True or
                binding.get("effective_llm_call_kwargs", {}).get("seed") != seed):
            raise PilotVerificationError(f"seed{seed} T2 effective profile binding invalid: {case_id}")
        journal_rows = _read_jsonl(journal)
        totals = _request_totals(journal_rows, profile, seed, t2=True, label=f"seed{seed} T2 {case_id}")
        if totals["request_count"] != proof.get("request_count") or totals["request_payload_sha256"] != [
                row.get("request_payload_sha256") for row in proof.get("requests", [])]:
            raise PilotVerificationError(f"seed{seed} T2 request proof differs from actual journal: {case_id}")
        cost = job.get("cost", {})
        if (cost.get("model_calls") != totals["request_count"] or
                (totals["requests_with_incomplete_usage"] == 0 and
                 (cost.get("prompt_tokens") != totals["prompt_tokens_observed"] or
                  cost.get("completion_tokens") != totals["completion_tokens_observed"]))):
            raise PilotVerificationError(f"seed{seed} T2 request/cost accounting mismatch: {case_id}")
        reports.append({"case_id": case_id, **totals,
            "model_seconds": cost.get("model_seconds"), "wall_seconds": cost.get("wall_seconds"),
            "request_journal_sha256": sha256_file(journal)})
    return {"seed": seed, "case_count": len(reports), "cases": reports,
        "total_model_calls": sum(row["request_count"] for row in reports),
        "observed_prompt_tokens": sum(row["prompt_tokens_observed"] for row in reports),
        "observed_completion_tokens": sum(row["completion_tokens_observed"] for row in reports),
        "evaluator_or_accuracy_read": False}


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, choices=SEEDS)
    parser.add_argument("--t2-run-root", type=Path)
    parser.add_argument("--e4-run-root", type=Path)
    args = parser.parse_args()
    result: dict[str, Any] = {"prepared": verify_prepared_pilot()}
    if args.seed is not None:
        if args.e4_run_root:
            result["e4"] = verify_e4_seed(args.seed, args.e4_run_root,
                                           launch_root=LAUNCH_ROOT)
        if args.t2_run_root:
            result["t2"] = verify_t2_seed(args.seed, args.t2_run_root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    import argparse
    raise SystemExit(_cli())
