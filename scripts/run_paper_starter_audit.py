#!/usr/bin/env python3
"""Run the paper-starter exploratory audit on the existing ten scored Bank cases.

This reuses the frozen Gemma profile and trusted evaluator. The worker receives a
single case in a restricted bwrap view; labels remain in the host evaluator.
Results describe reuse of the historical M5 T2 cases and are not a fresh holdout.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PAPER_AUDIT_CONFIG_FILES = (
    "paper_starter_catalog.json", "paper_starter_playbooks.json",
    "paper_starter_runtime.json", "paper_bank_adaptations.json",
    "typed_operator_ir_v2_policy.json", "paper_starter_authoring.json",
)

from e4core.cache import implementation_fingerprint
from e4core.m4_library import load_library_snapshot, snapshot_library, stage3_authoring_profile
from e4core.m5_evaluation import EvaluationSession
from e4core.m5_manifest import _manifest_files


class AuditError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hash_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".pending")
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _view_inventory(view_root: Path) -> tuple[dict[str, Any], str]:
    inventory = {}
    for path in sorted(view_root.rglob("*")):
        if not path.is_file() or ".venv" in path.relative_to(view_root).parts:
            continue
        inventory[path.relative_to(view_root).as_posix()] = {
            "sha256": _sha(path), "bytes": path.stat().st_size}
    encoded = json.dumps(inventory, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return inventory, hashlib.sha256(encoded).hexdigest()


def _install_paper_audit_config(view_root: Path, *, root: Path = ROOT) -> None:
    """Add opt-in paper profile configs after the legacy/base project view is built."""
    config_dir = view_root / "config"
    if not config_dir.is_dir():
        raise AuditError("restricted paper view has no config directory")
    for name in PAPER_AUDIT_CONFIG_FILES:
        shutil.copyfile(root / "config" / name, config_dir / name)


def _load_freeze(path: Path) -> tuple[dict[str, Any], Path]:
    path = path.resolve(strict=True)
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("schema_version") != "e4-paper-starter-freeze/1":
        raise AuditError("paper-starter freeze receipt has an unsupported schema")
    if doc.get("e4_implementation_fingerprint") != implementation_fingerprint():
        raise AuditError("paper-starter implementation fingerprint changed after freeze")
    inventory = _manifest_files(ROOT)
    if (_hash_json(inventory) != doc.get("repository_source_inventory_sha256") or
            inventory != doc.get("repository_source_files_sha256")):
        raise AuditError("paper-starter source/config inventory differs from the freeze receipt")
    archive = ROOT / doc.get("source_archive_path", "")
    if archive.is_symlink() or not archive.is_file() or _sha(archive) != doc.get("source_archive_sha256"):
        raise AuditError("frozen reproducibility source archive is missing or changed")
    bank_root = ROOT / doc["initial_library_path"]
    snapshot = snapshot_library(bank_root)
    if snapshot.snapshot_id != doc.get("initial_library_snapshot_id"):
        raise AuditError("frozen paper Bank library identity changed")
    selected_authoring = stage3_authoring_profile(snapshot)
    if (doc.get("stage3_authoring_profile", "typed-operator-ir-v2") != selected_authoring):
        raise AuditError("freeze receipt authoring profile differs from the persisted Bank snapshot policy")
    origin = snapshot_library(ROOT / doc["canonical_origin_path"])
    if (stage3_authoring_profile(origin) != selected_authoring or
        origin.snapshot_id != doc.get("canonical_origin", {}).get("snapshot_id") or
            len(origin.playbooks) != 11 or
            origin.document["policy"]["paper_counts"]["diagnostic_operator_count"] != 19):
        raise AuditError("immutable canonical origin snapshot no longer verifies as 19 diagnostics and 11 DAGs")
    return doc, path


def prepare(freeze_path: Path, *, audit_root: Path | None = None) -> Path:
    freeze, freeze_path = _load_freeze(freeze_path)
    case_ids = freeze.get("scored_case_ids")
    groups = freeze.get("shard_groups")
    if (not isinstance(case_ids, list) or len(case_ids) != 10 or len(set(case_ids)) != 10 or
            groups != {"local": case_ids[:5], "wingtip": case_ids[5:]}):
        raise AuditError("freeze receipt does not contain the fixed ordered ten-case/two-shard plan")
    base = (audit_root or ROOT / "artifacts/paper_starter/audits").resolve()
    if not base.is_relative_to(ROOT):
        raise AuditError("audit output root must stay inside the project")
    base.mkdir(parents=True, exist_ok=True)
    audit_dir = base / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4))
    audit_dir.mkdir(exist_ok=False)
    model_manifest_path = ROOT / freeze["model_manifest_path"]
    profile_path = ROOT / freeze["model_profile_path"]
    if (_sha(model_manifest_path) != freeze["model_manifest_sha256"] or
            _sha(profile_path) != freeze["model_profile_sha256"]):
        raise AuditError("Gemma model/profile metadata differs from the paper freeze")
    manifest = {"schema_version": "e4-paper-starter-bank10-audit/1",
        "status": "prepared", "mode": "exploratory_reused_scored_cases",
        "freeze_manifest_path": str(freeze_path.relative_to(ROOT)),
        "freeze_manifest_sha256": _sha(freeze_path),
        "freeze_run_id": freeze["run_id"],
        "initial_library_path": freeze["initial_library_path"],
        "initial_library_snapshot_id": freeze["initial_library_snapshot_id"],
        "stage3_authoring_profile": freeze.get("stage3_authoring_profile", "typed-operator-ir-v2"),
        "canonical_origin_snapshot_id": freeze["canonical_origin"]["snapshot_id"],
        "source_fingerprint": freeze["e4_implementation_fingerprint"],
        "source_inventory_sha256": freeze["repository_source_inventory_sha256"],
        "model_profile_path": freeze["model_profile_path"],
        "model_profile_sha256": freeze["model_profile_sha256"],
        "model_manifest_path": freeze["model_manifest_path"],
        "model_manifest_sha256": freeze["model_manifest_sha256"],
        "model_checkpoint_sha256": freeze["model_checkpoint_sha256"],
        "model_revision": freeze["model_revision"],
        "baseline_manifest_path": freeze["baseline_manifest_path"],
        "baseline_manifest_sha256": freeze["baseline_manifest_sha256"],
        "trusted_truth_pin": freeze["trusted_truth_pin"],
        "bank_sample_sha256": freeze["bank_sample_sha256"],
        "case_ids": case_ids, "shard_groups": groups,
        "official_m5_scored_claim": False, "fresh_holdout_claim": False,
        "case_reuse_annotation": "These are the exact historical M5 T2 scored cases; results are exploratory and do not constitute an independent holdout."}
    _atomic_json(audit_dir / "audit_manifest.json", manifest)
    _atomic_json(audit_dir / "audit_status.json", {"status": "prepared",
        "manifest_sha256": _sha(audit_dir / "audit_manifest.json"),
        "case_ids": case_ids, "shard_groups": groups})
    return audit_dir


def _truth_rows(manifest: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    pin = manifest["trusted_truth_pin"]
    path = ROOT / pin["path"]
    if path.is_symlink() or path.resolve() != path.absolute() or _sha(path) != pin["sha256"]:
        raise AuditError("trusted scored truth does not match the frozen M5 metadata pin")
    rows: dict[str, tuple[str, ...]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        case_id = row.get("incident_id")
        if row.get("split") != "scored" or case_id not in manifest["case_ids"] or case_id in rows:
            raise AuditError("trusted scored truth contains an invalid or duplicate case row")
        targets = row.get("ground_truth")
        if not isinstance(targets, list) or not targets:
            raise AuditError("trusted truth target list is empty or malformed")
        values = [item.get("component") for item in targets if isinstance(item, Mapping)]
        if len(values) != len(targets) or any(not isinstance(value, str) or not value for value in values):
            raise AuditError("trusted truth component identity is malformed")
        if len({value.casefold() for value in values}) != len(values):
            raise AuditError("trusted truth has duplicate targets")
        rows[case_id] = tuple(values)
    if set(rows) != set(manifest["case_ids"]):
        raise AuditError("trusted truth does not cover the fixed ten-case denominator")
    return rows


def _validate_server_receipt(path: Path, manifest: Mapping[str, Any], shard: str, *,
                             require_endpoint: bool = True) -> dict[str, Any]:
    path = path.resolve(strict=True)
    if path.is_symlink():
        raise AuditError("server receipt cannot be a symlink")
    receipt = json.loads(path.read_text(encoding="utf-8"))
    endpoint = os.environ.get("E4_BASE_URL") if require_endpoint else receipt.get("endpoint")
    expected_backend = "local" if shard == "local" else "wingtip"
    if (receipt.get("schema_version") != "e4-paper-starter-server-receipt/1" or
            receipt.get("backend") != expected_backend or receipt.get("endpoint") != endpoint or
            receipt.get("model_manifest_sha256") != manifest["model_manifest_sha256"] or
            receipt.get("checkpoint_sha256") != manifest["model_checkpoint_sha256"] or
            receipt.get("model_revision") != manifest["model_revision"] or
            receipt.get("profile_sha256") != manifest["model_profile_sha256"] or
            receipt.get("weights_loaded") is not True):
        raise AuditError("model server receipt differs from the frozen checkpoint/profile/endpoint")
    for prefix in ("provenance", "tokenizer_preflight"):
        relative = receipt.get(f"{prefix}_path")
        expected_hash = receipt.get(f"{prefix}_sha256")
        if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise AuditError(f"server receipt {prefix} path is unsafe")
        target = ROOT / relative
        if target.is_symlink() or not target.is_file() or _sha(target) != expected_hash:
            raise AuditError(f"server receipt {prefix} artifact is missing or changed")
    tokenizer = json.loads((ROOT / receipt["tokenizer_preflight_path"]).read_text(encoding="utf-8"))
    if (tokenizer.get("profile_sha256") != manifest["model_profile_sha256"] or
            tokenizer.get("backend") != expected_backend or
            tokenizer.get("completion_requests_made") != 0 or
            tokenizer.get("context_reserve_tokens") != 8192 or
            tokenizer.get("prompt_count") != 40 or tokenizer.get("all_fit") is not True or
            tokenizer.get("prompt_phases") != ["select", "compose", "evolve", "stage3_serve"] or
            tokenizer.get("case_ids") != manifest["case_ids"] or
            tokenizer.get("source_fingerprint") != manifest["source_fingerprint"] or
            tokenizer.get("initial_library_snapshot_id") != manifest["initial_library_snapshot_id"] or
            tokenizer.get("stage3_authoring_profile", "typed-operator-ir-v2") != manifest.get("stage3_authoring_profile", "typed-operator-ir-v2") or
            tokenizer.get("freeze_manifest_sha256") != manifest["freeze_manifest_sha256"]):
        raise AuditError("tokenizer preflight receipt does not prove current-profile fit with the 8192-token reserve")
    prompt_archive = ROOT / tokenizer.get("prompt_archive_path", "")
    if prompt_archive.is_symlink() or not prompt_archive.is_file() or _sha(prompt_archive) != tokenizer.get("prompt_archive_sha256"):
        raise AuditError("tokenizer prompt/payload archive is missing or changed")
    return {"path": str(path.relative_to(ROOT)), "sha256": _sha(path),
        "provenance_path": receipt["provenance_path"],
        "provenance_sha256": receipt["provenance_sha256"],
        "tokenizer_preflight_path": receipt["tokenizer_preflight_path"],
        "tokenizer_preflight_sha256": receipt["tokenizer_preflight_sha256"],
        "backend": expected_backend, "endpoint": endpoint,
        "checkpoint_sha256": receipt["checkpoint_sha256"],
        "model_revision": receipt["model_revision"],
        "profile_sha256": receipt["profile_sha256"]}


def preflight(freeze_path: Path, output_dir: Path, *, backend: str) -> dict[str, Any]:
    """Render and tokenize all initial-history prompts without completions."""
    freeze, freeze_path = _load_freeze(freeze_path)
    if backend not in {"local", "wingtip"}:
        raise AuditError("tokenizer preflight backend must be local or wingtip")
    endpoint = os.environ.get("E4_BASE_URL")
    if not endpoint:
        raise AuditError("tokenizer preflight requires the root-managed model endpoint")
    from scripts.run_m4_bank import _validate_local_endpoint
    _validate_local_endpoint(endpoint)
    from e4core.bank_adapter import load_bank_case
    from e4core.m4_controller import (M4ModelClient, M4QueryBudget, _policy,
        _split_composite_specs, _stage_prompt, _logical_registry, build_m4_runtime,
        public_case_digest)
    from e4core.m4_library import generated_specs_from_snapshot
    from e4core.paper_starter import (available_bank_profile_playbooks,
        PROFILE, make_paper_starter_input, make_paper_starter_registry)

    output_dir = output_dir.resolve()
    if not output_dir.is_relative_to(ROOT):
        raise AuditError("tokenizer preflight output must stay inside the project")
    if output_dir.exists() or output_dir.is_symlink():
        raise AuditError("tokenizer preflight output directory already exists")
    output_dir.mkdir(parents=True)
    snapshot = snapshot_library(ROOT / freeze["initial_library_path"])
    if snapshot.snapshot_id != freeze["initial_library_snapshot_id"]:
        raise AuditError("preflight library differs from the immutable freeze")
    profile_path = ROOT / freeze["model_profile_path"]
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    from e4core.m4_library import stage3_authoring_profile
    selected_authoring = stage3_authoring_profile(snapshot)
    harness_profile = ("reconstructed-paper-starter-v1-typed-operator-code-v1"
        if selected_authoring == "typed-operator-code-v1" else PROFILE)
    client = M4ModelClient(profile, base_url=endpoint, harness_profile=harness_profile)
    if client.max_generation_tokens != int(profile["max_generation_tokens"]):
        raise AuditError("tokenizer preflight generation reserve differs from the frozen Gemma profile")
    if client.context_tokens != int(profile["context_tokens"]):
        raise AuditError("tokenizer preflight context differs from the frozen Gemma profile")
    policy = _policy(ROOT, ROOT / "config/m4_runtime.json")
    stages = (("select", "select"), ("compose", "compose"),
              ("evolve", "evolve"), ("stage3_serve", "stage3_serve"))
    records = []
    archive = []
    for case_id in freeze["scored_case_ids"]:
        case = load_bank_case(case_id, "scored", root=ROOT)
        starter = make_paper_starter_input(case.telemetry, root=ROOT)
        specs = generated_specs_from_snapshot(snapshot)
        legacy_specs, composites = _split_composite_specs(specs)
        active_keys = {f"{row['name']}@{row['version']}" for row in snapshot.document["operators"]
                       if row["kind"] == "builtin" or
                       row.get("signature", {}).get("authoring_profile") != "operator-dag-v1"}
        primitive = make_paper_starter_registry(bank_only=True, starter_input=starter,
            generated_specs=legacy_specs, active_operator_keys=active_keys, root=ROOT)
        registry = _logical_registry(primitive, composites)
        available = {row["name"] for row in available_bank_profile_playbooks(
            [dict(row) for row in snapshot.playbooks.values()], starter)}
        case_policy = {**policy, "_paper_case_capabilities": dict(starter.capabilities),
                       "_paper_case_available_books": sorted(available)}
        runtime = build_m4_runtime(case.alert, root=ROOT)
        digest = public_case_digest(case, root=ROOT)
        for prompt_stage, payload_stage in stages:
            prompt = _stage_prompt(prompt_stage, digest=digest, registry=registry,
                snapshot=snapshot, runtime=runtime, history=[], policy=case_policy,
                query_budget=M4QueryBudget(None), composite_operator_specs=composites)
            payload = client.payload_for_stage(payload_stage, prompt)
            status, body, applied, error = client._post_json("/apply-template", dict(payload))
            if error or not isinstance(applied, Mapping) or not isinstance(applied.get("prompt"), str):
                raise AuditError(f"template rendering failed for {case_id}/{payload_stage}: {error or status}")
            tokenize_request = {"content": applied["prompt"], "add_special": True, "parse_special": True}
            token_status, token_body, token_response, token_error = client._post_json("/tokenize", tokenize_request)
            tokens = token_response.get("tokens") if isinstance(token_response, Mapping) else None
            if (token_error or not isinstance(tokens, list) or
                    any(type(token) is not int for token in tokens)):
                raise AuditError(f"tokenization failed for {case_id}/{payload_stage}: {token_error or token_status}")
            prompt_tokens = len(tokens)
            if prompt_tokens + 8192 > client.context_tokens:
                raise AuditError(f"rendered prompt exceeds the 8192-token generation reserve: {case_id}/{payload_stage}")
            archive.append({"case_id": case_id, "stage": payload_stage, "prompt": prompt,
                "request_payload": payload, "rendered_prompt": applied["prompt"],
                "token_count": prompt_tokens,
                "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                "payload_sha256": _hash_json(payload),
                "rendered_prompt_sha256": hashlib.sha256(applied["prompt"].encode()).hexdigest(),
                "token_ids_sha256": _hash_json(tokens),
                "apply_template_http_status": status,
                "tokenize_http_status": token_status})
            records.append({"case_id": case_id, "stage": payload_stage,
                "prompt_tokens": prompt_tokens, "generation_reserve_tokens": 8192,
                "context_tokens": client.context_tokens,
                "fits": prompt_tokens + 8192 <= client.context_tokens})
    prompt_archive = output_dir / "prompt_tokenizer_archive.json"
    _atomic_json(prompt_archive, {"schema_version": "e4-paper-starter-tokenizer-archive/1",
        "freeze_manifest_path": str(freeze_path.relative_to(ROOT)),
        "freeze_manifest_sha256": _sha(freeze_path), "source_fingerprint": implementation_fingerprint(),
        "initial_library_snapshot_id": snapshot.snapshot_id,
        "stage3_authoring_profile": selected_authoring,
        "model_profile_sha256": _sha(profile_path), "backend": backend,
        "prompts": archive})
    report = {"schema_version": "e4-paper-starter-tokenizer-preflight/1",
        "status": "complete", "backend": backend, "endpoint": endpoint,
        "profile_sha256": _sha(profile_path), "source_fingerprint": implementation_fingerprint(),
        "freeze_manifest_sha256": _sha(freeze_path),
        "initial_library_snapshot_id": snapshot.snapshot_id,
        "stage3_authoring_profile": selected_authoring,
        "case_ids": freeze["scored_case_ids"], "prompt_count": len(records),
        "context_tokens": client.context_tokens, "context_reserve_tokens": client.max_generation_tokens,
        "prompt_phases": ["select", "compose", "evolve", "stage3_serve"],
        "prompt_history_coverage": "initial empty history per case; full-history later attempts rely on runtime preflight",
        "completion_requests_made": 0, "all_fit": len(records) == 40 and all(row["fits"] for row in records),
        "results": records, "prompt_archive_path": str(prompt_archive.relative_to(ROOT)),
        "prompt_archive_sha256": _sha(prompt_archive)}
    if not report["all_fit"]:
        raise AuditError("tokenizer preflight did not fit all thirty case-stage prompts")
    report_path = output_dir / "tokenizer_preflight.json"
    _atomic_json(report_path, report)
    return report


def _slots(result: Mapping[str, Any], session: EvaluationSession) -> list[dict[str, Any]]:
    stages = result.get("stages", [])
    ledger = session.trusted_ledger()
    if (not isinstance(stages, list) or len(stages) > 3 or
            [row.get("stage") for row in stages] !=
            ["stage1_select", "stage2_compose", "stage3_synthesize"][:len(stages)]):
        raise AuditError("controller stage records do not preserve fixed three-slot order")
    slots = []
    submitted = 0
    accepted_seen = False
    for index in range(3):
        name = ("stage1_select", "stage2_compose", "stage3_synthesize")[index]
        if index >= len(stages):
            slots.append({"attempt": index + 1, "stage": name,
                "status": "not_run_after_acceptance" if accepted_seen else "not_reached",
                "accepted": None})
            continue
        stage = stages[index]
        if stage.get("status") in {"accepted", "rejected"}:
            if submitted >= len(ledger) or ledger[submitted]["accepted"] is not (stage["status"] == "accepted"):
                raise AuditError("trusted feedback ledger differs from a submitted controller stage")
            row = ledger[submitted]
            slots.append({"attempt": index + 1, "stage": name, "status": "submitted",
                "accepted": row["accepted"], "valid": row["valid"],
                "correct_rank": row["correct_rank"]})
            submitted += 1
            accepted_seen = accepted_seen or row["accepted"] is True
        else:
            slots.append({"attempt": index + 1, "stage": name, "status": "failed",
                "accepted": False, "valid": False, "error_code": stage.get("status")})
    if submitted != len(ledger):
        raise AuditError("trusted evaluator received an unaccounted submission")
    return slots


def _worker_command(case_id: str, case_root: Path, library_root: Path,
                    view_root: Path, feedback_address: str, *, remote: bool) -> list[str]:
    from scripts import run_m4_bank as supervisor
    profile = ROOT / "config/gemma_m3_inference.json"
    source = ("import sys; from pathlib import Path; "
        "from scripts.run_m4_bank import BinaryFeedbackProxy; "
        "from e4core.m4_controller import run_diagnostic; "
        "proxy=BinaryFeedbackProxy(sys.argv[1]); "
        "run_diagnostic(sys.argv[2], library_root=Path('/m4library'), "
        "run_dir=Path('/m4run/diagnostic'), feedback=proxy, "
        "profile_path=Path('/m4project/config/gemma_m3_inference.json'), "
        "root=Path('/m4project'), split='scored'); proxy.close()")
    command = ["/m4project/.venv/bin/python", "-c", source, feedback_address, case_id]
    built = supervisor._bwrap_command(view_root=view_root, run_dir=case_root,
        library_root=library_root, profile_relative=profile.relative_to(ROOT).as_posix(),
        mode="diagnostic", case_id=case_id, split="scored",
        endpoint=os.environ.get("E4_BASE_URL", "http://127.0.0.1:8080"),
        feedback_socket=feedback_address,
        implementation_fingerprint_value=implementation_fingerprint(), command_override=command)
    if remote:
        separator = built.index("--")
        built[separator:separator] = ["--dir", "/run", "--dir", "/run/e4remote",
            "--ro-bind", "/run/e4remote-source", "/run/e4remote"]
    return built


def _run_case(case_id: str, index: int, input_library: Path, shard_root: Path,
              manifest: Mapping[str, Any], truth: Mapping[str, Sequence[str]], shard: str) -> dict[str, Any]:
    from e4core.m5_tasks import load_bank_case_view
    from scripts import run_m4_bank as supervisor

    if implementation_fingerprint() != manifest["source_fingerprint"]:
        raise AuditError("source fingerprint changed before scored case")
    case_name = f"case{index:02d}"
    case_root = shard_root / case_name
    if case_root.exists():
        raise AuditError(f"preserving existing first trajectory: {case_name}")
    case_root.mkdir(parents=True)
    current = snapshot_library(input_library)
    view = load_bank_case_view(case_id, split="scored", root=ROOT)
    session = EvaluationSession(case_id=case_id, allowed_component_ids=view.component_ids,
        truth_ids=truth[case_id])
    profile = ROOT / manifest["model_profile_path"]
    view_root = shard_root / "views" / case_name / "project"
    view_root.parent.mkdir(parents=True, exist_ok=False)
    view_pins = supervisor.create_restricted_project_view(ROOT, view_root,
        profile_path=profile, case_id=case_id, split="scored")
    # The paper starter is an opt-in source profile with separately frozen config.
    _install_paper_audit_config(view_root)
    view_inventory, view_digest = _view_inventory(view_root)
    supervisor_doc = {"schema_version": "e4-paper-starter-audit-supervisor/1",
        "case_id": case_id, "shard": shard, "restricted_view_sha256": view_digest,
        "restricted_view_file_count": len(view_inventory),
        "restricted_view_file_inventory": view_inventory,
        "profile_sha256": _sha(profile), "source_fingerprint": manifest["source_fingerprint"],
        "input_library_snapshot_id": current.snapshot_id}
    _atomic_json(case_root / "supervisor_manifest.json", supervisor_doc)
    audit_feedback = shard_root / "trusted" / f"{case_name}_feedback.jsonl"
    audit_feedback.parent.mkdir(parents=True, exist_ok=True)
    audit_feedback.touch(exist_ok=False)
    if not os.environ.get("E4_BASE_URL"):
        raise AuditError("case shard must run under a root-managed local/Wingtip model server")
    supervisor._validate_local_endpoint(os.environ.get("E4_BASE_URL"))
    remote = shard == "wingtip"
    if remote != (os.environ.get("E4_REMOTE_TRANSPORT") == "ssh-unix-forward-via-loopback-relay"):
        raise AuditError("model transport does not match the selected hardware shard")
    status_out = {"case_wall_timeout": False}
    exit_code = supervisor._run_with_feedback(
        lambda address: _worker_command(case_id, case_root, input_library, view_root,
            address, remote=remote), case_id=case_id, evaluator=_SessionEvaluator(session),
        audit_path=audit_feedback, stdout_path=case_root / "outer.stdout.log",
        stderr_path=case_root / "outer.stderr.log", timeout_seconds=7200, status_out=status_out)
    result_path = case_root / "diagnostic" / "diagnostic_result.json"
    ledger_path = shard_root / "trusted" / f"{case_name}_ledger.json"
    _atomic_json(ledger_path, {"case_id": case_id, "ledger": session.trusted_ledger(),
        "trusted_truth_sha256": manifest["trusted_truth_pin"]["sha256"], "worker_exit_code": exit_code})
    if not result_path.is_file():
        raise AuditError(f"{case_name} produced no completed diagnostic result; trajectory preserved")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    slots = _slots(result, session)
    if (result.get("case_id") != case_id or
            (result.get("accepted") is True) != any(row["accepted"] is True for row in slots)):
        raise AuditError(f"{case_name} controller result disagrees with fixed case or trusted ledger")
    output_library = case_root / "diagnostic" / "library"
    after = snapshot_library(output_library)
    cursor = after
    lineage_seen = set()
    while cursor.snapshot_id != current.snapshot_id:
        if cursor.snapshot_id in lineage_seen or not cursor.parent_snapshot_id:
            raise AuditError("case output library does not descend from its shard input")
        lineage_seen.add(cursor.snapshot_id)
        cursor = load_library_snapshot(cursor.path.parent / f"{cursor.parent_snapshot_id}.json")
    summary = {"case_id": case_id, "status": result.get("status"),
        "accepted": result.get("accepted") is True, "accepted_stage": result.get("accepted_stage"),
        "stage_slots": slots, "worker_exit_code": exit_code,
        "case_wall_timeout": status_out["case_wall_timeout"],
        "input_library_snapshot_id": current.snapshot_id,
        "output_library_snapshot_id": after.snapshot_id,
        "library_path": str(output_library.relative_to(ROOT)),
        "diagnostic_result_path": str(result_path.relative_to(ROOT)),
        "diagnostic_result_sha256": _sha(result_path),
        "ledger_path": str(ledger_path.relative_to(ROOT)), "ledger_sha256": _sha(ledger_path),
        "model_calls": (result.get("model_calls") or {}).get("model_call_count"),
        "prompt_tokens": (result.get("model_calls") or {}).get("prompt_tokens"),
        "completion_tokens": (result.get("model_calls") or {}).get("completion_tokens"),
        "charged_generated_tokens": (result.get("model_calls") or {}).get("charged_generated_tokens"),
        "model_seconds": (result.get("model_calls") or {}).get("model_elapsed_seconds"),
        "wall_seconds": result.get("wall_seconds")}
    _atomic_json(shard_root / "trusted" / f"{case_name}_summary.json", summary)
    if exit_code != 0:
        raise AuditError(f"{case_name} worker failed; preserving the completed failed trajectory")
    return summary


class _SessionEvaluator:
    def __init__(self, session: EvaluationSession):
        self.session = session

    def evaluate(self, case_id: str, answer: Any) -> dict[str, bool]:
        from e4core.contracts import RankedAnswer
        if case_id != self.session.case_id or not isinstance(answer, RankedAnswer):
            raise AuditError("trusted feedback received a malformed answer")
        return self.session.submit({"ranked_components": [row.entity.identifier for row in answer.leads[:3]]})


def _verify_audit(audit_dir: Path) -> tuple[dict[str, Any], Path]:
    manifest_path = audit_dir / "audit_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    status_path = audit_dir / "audit_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    freeze_path = ROOT / manifest["freeze_manifest_path"]
    freeze, _ = _load_freeze(freeze_path)
    if (manifest.get("freeze_manifest_sha256") != _sha(freeze_path) or
            manifest.get("initial_library_snapshot_id") != freeze.get("initial_library_snapshot_id") or
            manifest.get("stage3_authoring_profile") != freeze.get("stage3_authoring_profile", "typed-operator-ir-v2") or
            manifest.get("source_fingerprint") != implementation_fingerprint() or
            status.get("manifest_sha256") != _sha(manifest_path) or
            status.get("case_ids") != manifest.get("case_ids") or
            _sha(ROOT / manifest["baseline_manifest_path"]) != manifest["baseline_manifest_sha256"] or
            _sha(ROOT / "config/bank_sample.json") != manifest["bank_sample_sha256"]):
        raise AuditError("audit manifest no longer matches its frozen source/library receipt")
    return manifest, manifest_path


def run_shard(audit_dir: Path, shard: str, *, server_receipt: Path,
              resume_from: Path | None = None) -> dict[str, Any]:
    audit_dir = audit_dir.resolve(strict=True)
    manifest, manifest_path = _verify_audit(audit_dir)
    if shard not in {"local", "wingtip"}:
        raise AuditError("shard must be local or wingtip")
    server_pin = _validate_server_receipt(server_receipt, manifest, shard)
    shard_cases = manifest["shard_groups"][shard]
    run_root = audit_dir / f"bank10_{shard}"
    completed: list[dict[str, Any]] = []
    current_library = ROOT / manifest["initial_library_path"]
    if resume_from is not None:
        prior = resume_from.resolve(strict=True)
        prior_state = json.loads((prior / "bank10_status.json").read_text(encoding="utf-8"))
        if (prior_state.get("audit_manifest_sha256") != _sha(manifest_path) or
                prior_state.get("case_ids") != shard_cases or prior_state.get("shard") != shard or
                prior_state.get("server_receipt", {}).get("backend") != shard or
                prior_state.get("server_receipt", {}).get("checkpoint_sha256") != server_pin["checkpoint_sha256"] or
                prior_state.get("server_receipt", {}).get("profile_sha256") != server_pin["profile_sha256"]):
            raise AuditError("resume checkpoint belongs to another frozen shard")
        completed = list(prior_state.get("completed_cases", []))
        if [row.get("case_id") for row in completed] != shard_cases[:len(completed)]:
            raise AuditError("resume checkpoint is not a fixed-order completed prefix")
        previous_id = manifest["initial_library_snapshot_id"]
        for row in completed:
            _verify_case_summary(row, previous_id)
            previous_id = row["output_library_snapshot_id"]
        if completed:
            current_library = ROOT / completed[-1]["library_path"]
        interrupted = prior_state.get("current_case")
        if interrupted and len(completed) < len(shard_cases):
            case_index = len(completed) + 1
            expected_case = shard_cases[case_index - 1]
            if interrupted.get("case_id") != expected_case:
                raise AuditError("interrupted trajectory is not the next fixed-order case")
            # Preserve the first attempt and account for it in the ten-case
            # denominator. If it lacks a completed result, it is a failed slot;
            # the next case starts from the last valid library snapshot.
            summary_path = prior / "trusted" / f"case{case_index:02d}_summary.json"
            if summary_path.is_file():
                recovered = json.loads(summary_path.read_text(encoding="utf-8"))
                _verify_case_summary(recovered, previous_id)
                if recovered.get("case_id") != expected_case:
                    raise AuditError("uncheckpointed completed trajectory belongs to another case")
                completed.append(recovered)
                current_library = ROOT / recovered["library_path"]
                previous_id = recovered["output_library_snapshot_id"]
            else:
                failed = {"case_id": expected_case, "status": "failed_infrastructure",
                "accepted": False, "accepted_stage": None,
                "stage_slots": [{"attempt": i, "status": "failed_infrastructure",
                    "accepted": False, "valid": False} for i in (1, 2, 3)],
                "worker_exit_code": None, "case_wall_timeout": False,
                "input_library_snapshot_id": previous_id,
                "output_library_snapshot_id": previous_id,
                "library_path": str(current_library.relative_to(ROOT)),
                "preserved_failed_trajectory": str((prior / f"case{case_index:02d}").relative_to(ROOT))}
                completed.append(failed)
        run_root = audit_dir / f"bank10_{shard}_resume_{secrets.token_hex(4)}"
    if run_root.exists():
        raise AuditError("shard directory already exists; choose another continuation ID")
    run_root.mkdir()
    truth = _truth_rows(manifest)
    state = {"schema_version": "e4-paper-starter-bank10-shard/1", "status": "running",
        "audit_manifest_sha256": _sha(manifest_path), "shard": shard,
        "server_receipt": server_pin,
        "case_ids": shard_cases, "initial_library_snapshot_id": manifest["initial_library_snapshot_id"],
        "completed_cases": completed, "current_case": None}
    _atomic_json(run_root / "bank10_status.json", state)
    start = len(completed)
    for index, case_id in enumerate(shard_cases[start:], start=start + 1):
        state["current_case"] = {"case_id": case_id, "case_index": index, "status": "running"}
        _atomic_json(run_root / "bank10_status.json", state)
        summary = _run_case(case_id, index, current_library, run_root, manifest, truth, shard)
        completed.append(summary)
        current_library = ROOT / summary["library_path"]
        state["completed_cases"] = completed
        state["current_case"] = None
        _atomic_json(run_root / "bank10_status.json", state)
    state["status"] = "complete"
    state["finished_utc"] = datetime.now(timezone.utc).isoformat()
    _atomic_json(run_root / "bank10_status.json", state)
    return state


def _verify_case_summary(row: Mapping[str, Any], expected_input_snapshot_id: str) -> None:
    if row.get("input_library_snapshot_id") != expected_input_snapshot_id:
        raise AuditError("resume checkpoint library chain is broken")
    if row.get("status") == "failed_infrastructure" and row.get("preserved_failed_trajectory"):
        return
    result_path = ROOT / str(row.get("diagnostic_result_path", ""))
    ledger_path = ROOT / str(row.get("ledger_path", ""))
    if (not result_path.is_file() or _sha(result_path) != row.get("diagnostic_result_sha256") or
            not ledger_path.is_file() or _sha(ledger_path) != row.get("ledger_sha256")):
        raise AuditError("resume checkpoint result or evaluator ledger hash does not match")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if (result.get("case_id") != row.get("case_id") or
            (result.get("accepted") is True) != any(
                slot.get("accepted") is True for slot in row.get("stage_slots", []))):
        raise AuditError("resume checkpoint result does not match its stage slots")
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    if (ledger.get("case_id") != row.get("case_id") or
            ledger.get("worker_exit_code") != row.get("worker_exit_code") or
            len(ledger.get("ledger", [])) > 3):
        raise AuditError("resume checkpoint evaluator ledger is malformed")
    snapshot = snapshot_library(ROOT / str(row.get("library_path", "")))
    if snapshot.snapshot_id != row.get("output_library_snapshot_id"):
        raise AuditError("resume checkpoint output library identity changed")
    cursor = snapshot
    seen = set()
    while cursor.snapshot_id != expected_input_snapshot_id:
        if cursor.snapshot_id in seen or not cursor.parent_snapshot_id:
            raise AuditError("resume checkpoint library snapshot does not descend from its input")
        seen.add(cursor.snapshot_id)
        cursor = load_library_snapshot(cursor.path.parent / f"{cursor.parent_snapshot_id}.json")


def combine(audit_dir: Path, local_status: Path, wingtip_status: Path) -> dict[str, Any]:
    audit_dir = audit_dir.resolve(strict=True)
    manifest, manifest_path = _verify_audit(audit_dir)
    states = {"local": json.loads(local_status.read_text(encoding="utf-8")),
              "wingtip": json.loads(wingtip_status.read_text(encoding="utf-8"))}
    for shard, state in states.items():
        if (state.get("status") != "complete" or state.get("shard") != shard or
                state.get("audit_manifest_sha256") != _sha(manifest_path) or
                state.get("case_ids") != manifest["shard_groups"][shard] or
                len(state.get("completed_cases", [])) != 5):
            raise AuditError(f"{shard} shard is incomplete or mismatched")
        receipt_path = ROOT / state.get("server_receipt", {}).get("path", "")
        if _validate_server_receipt(receipt_path, manifest, shard, require_endpoint=False) != state.get("server_receipt"):
            raise AuditError(f"{shard} server/tokenizer receipt changed after its run")
        previous = manifest["initial_library_snapshot_id"]
        for row in state["completed_cases"]:
            _verify_case_summary(row, previous)
            previous = row.get("output_library_snapshot_id")
    cases = states["local"]["completed_cases"] + states["wingtip"]["completed_cases"]
    if [row["case_id"] for row in cases] != manifest["case_ids"]:
        raise AuditError("two five-case chains do not reconstruct the frozen ten-case order")
    truth = _truth_rows(manifest)
    for row in cases:
        _reconcile_case(row, truth)
    denominator = len(cases)
    def total(key: str) -> int | float | None:
        values = [row.get(key) for row in cases]
        return None if any(type(value) not in (int, float) for value in values) else sum(values)
    prefix = {f"stage{n}_prefix_successes_of_{denominator}": sum(any(
        slot.get("accepted") is True for slot in row["stage_slots"][:n]) for row in cases)
        for n in (1, 2, 3)}
    rescues = {}
    for stage in (2, 3):
        reached = [row for row in cases if row["stage_slots"][stage - 1]["status"]
                   not in {"not_reached", "not_run_after_acceptance"}]
        rescues[f"stage{stage}"] = {"rescued_after_prior_miss": sum(
            row["stage_slots"][stage - 1].get("accepted") is True and not any(
                slot.get("accepted") is True for slot in row["stage_slots"][:stage - 1])
            for row in reached), "cases_reached": len(reached)}
    result = {"schema_version": "e4-paper-starter-bank10-exploratory-result/1",
        "status": "complete", "audit_mode": manifest["mode"],
        "case_reuse_annotation": manifest["case_reuse_annotation"],
        "official_m5_scored_claim": False, "fresh_holdout_claim": False,
        "case_count": denominator, "case_ids": manifest["case_ids"],
        "cumulative_top3_prefix_successes": prefix, "later_stage_rescues": rescues,
        "total_model_calls": total("model_calls"), "total_prompt_tokens": total("prompt_tokens"),
        "total_completion_tokens": total("completion_tokens"),
        "total_charged_generated_tokens": total("charged_generated_tokens"),
        "total_model_seconds": total("model_seconds"), "total_wall_seconds": total("wall_seconds"),
        "shard_chains": {shard: {"initial_library_snapshot_id": manifest["initial_library_snapshot_id"],
            "final_library_snapshot_id": state["completed_cases"][-1]["output_library_snapshot_id"]}
            for shard, state in states.items()},
        "library_handoff_across_shards": False, "cases": cases}
    path = audit_dir / "combined_bank10_result.json"
    if path.exists():
        raise AuditError("combined result already exists; preserving it")
    _atomic_json(path, result)
    return result


def _reconcile_case(row: Mapping[str, Any], truth: Mapping[str, Sequence[str]]) -> None:
    if row.get("status") == "failed_infrastructure" and row.get("preserved_failed_trajectory"):
        if row.get("accepted") is not False:
            raise AuditError("preserved failed trajectory must remain unsuccessful in the denominator")
        return
    from e4core.m5_tasks import load_bank_case_view
    case_id = row["case_id"]
    trusted = json.loads((ROOT / row["ledger_path"]).read_text(encoding="utf-8"))
    view = load_bank_case_view(case_id, split="scored", root=ROOT)
    session = EvaluationSession(case_id=case_id, allowed_component_ids=view.component_ids,
        truth_ids=truth[case_id])
    for item in trusted.get("ledger", []):
        session.submit(item.get("submitted"))
    if session.trusted_ledger() != trusted.get("ledger"):
        raise AuditError("trusted ledger does not reproduce from the frozen evaluator")
    rescored = any(item["accepted"] for item in session.trusted_ledger())
    if rescored != (row.get("accepted") is True):
        raise AuditError("summary acceptance differs from independent host rescoring")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--prepare", action="store_true")
    modes.add_argument("--preflight", action="store_true")
    modes.add_argument("--run-shard", choices=("local", "wingtip"))
    modes.add_argument("--combine", action="store_true")
    parser.add_argument("--freeze-manifest", type=Path)
    parser.add_argument("--audit-dir", type=Path)
    parser.add_argument("--resume-from", type=Path)
    parser.add_argument("--server-receipt", type=Path)
    parser.add_argument("--backend", choices=("local", "wingtip"))
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--local-status", type=Path)
    parser.add_argument("--wingtip-status", type=Path)
    args = parser.parse_args(argv)
    if args.prepare:
        if args.freeze_manifest is None:
            parser.error("--prepare requires --freeze-manifest")
        print(prepare(args.freeze_manifest))
    elif args.preflight:
        if args.freeze_manifest is None or args.output_dir is None or args.backend is None:
            parser.error("--preflight requires --freeze-manifest, --output-dir, and --backend")
        print(json.dumps(preflight(args.freeze_manifest, args.output_dir,
            backend=args.backend), sort_keys=True))
    elif args.run_shard:
        if args.audit_dir is None or args.server_receipt is None:
            parser.error("--run-shard requires --audit-dir and --server-receipt")
        print(json.dumps(run_shard(args.audit_dir, args.run_shard,
            server_receipt=args.server_receipt, resume_from=args.resume_from), sort_keys=True))
    else:
        if args.audit_dir is None or args.local_status is None or args.wingtip_status is None:
            parser.error("--combine requires --audit-dir, --local-status, and --wingtip-status")
        print(json.dumps(combine(args.audit_dir, args.local_status, args.wingtip_status), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
