#!/usr/bin/env python3
"""Run one frozen paper Bank case through the typed Python authoring controller.

This is an engineering canary on the first case from an existing freeze, not
an accuracy estimate or holdout. It uses the paper audit's restricted worker,
binary feedback, and pinned server receipt; labels remain in the host evaluator.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.m4_library import snapshot_library, stage3_authoring_profile
from scripts import run_m4_bank as supervisor
from scripts import run_paper_starter_audit as paper_audit


CODE_PROFILE = "typed-operator-code-v1"


class CanaryError(RuntimeError):
    pass


def _relative(path: Path) -> str:
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(ROOT):
        raise CanaryError("canary artifacts must stay inside the project root")
    return resolved.relative_to(ROOT).as_posix()


def run_canary(*, freeze_path: Path, output_dir: Path, endpoint: str,
               server_receipt: Path) -> dict[str, Any]:
    freeze, freeze_path = paper_audit._load_freeze(freeze_path)
    snapshot = snapshot_library(ROOT / freeze["initial_library_path"])
    selected_profile = stage3_authoring_profile(snapshot)
    if selected_profile != CODE_PROFILE:
        raise CanaryError(f"freeze selects {selected_profile}; expected {CODE_PROFILE}")
    if (not isinstance(freeze.get("scored_case_ids"), list) or
            len(freeze["scored_case_ids"]) != 10):
        raise CanaryError("freeze does not contain the fixed ten scored cases")
    case_id = freeze["scored_case_ids"][0]
    output_dir = output_dir.resolve()
    if not output_dir.is_relative_to(ROOT):
        raise CanaryError("canary output directory must stay inside the project root")
    if output_dir.exists() or output_dir.is_symlink():
        raise CanaryError("canary output directory already exists; preserving existing artifacts")
    output_dir.mkdir(parents=True)

    # Reuse the existing frozen audit manifest and receipt validation. prepare
    # performs no model completions and binds this canary to the same source,
    # profile, initial library, truth pin, and fixed case order.
    audit_root = output_dir / "audit"
    audit_dir = paper_audit.prepare(freeze_path, audit_root=audit_root)
    manifest, _manifest_path = paper_audit._verify_audit(audit_dir)
    old_endpoint = os.environ.get("E4_BASE_URL")
    old_transport = os.environ.get("E4_REMOTE_TRANSPORT")
    os.environ["E4_BASE_URL"] = endpoint
    os.environ.pop("E4_REMOTE_TRANSPORT", None)
    canary_root = output_dir / "single_case"
    record_path = output_dir / "canary_result.json"
    try:
        supervisor._validate_local_endpoint(endpoint)
        server_pin = paper_audit._validate_server_receipt(
            server_receipt, manifest, "local", require_endpoint=True)
        truth = paper_audit._truth_rows(manifest)
        if case_id not in truth:
            raise CanaryError("first frozen scored case is absent from the host-only truth ledger")

        canary_root.mkdir(parents=True, exist_ok=False)
        try:
            summary = paper_audit._run_case(case_id, 1,
                ROOT / manifest["initial_library_path"], canary_root,
                manifest, {case_id: truth[case_id]}, "local")
            result = {
                "schema_version": "e4-paper-operator-code-canary/1",
                "status": "complete",
                "claim": "single-case engineering canary on a reused frozen scored incident; not an accuracy estimate, holdout, or model-selection result",
                "case_id": case_id,
                "freeze_manifest_path": _relative(freeze_path),
                "freeze_manifest_sha256": paper_audit._sha(freeze_path),
                "source_fingerprint": manifest["source_fingerprint"],
                "stage3_authoring_profile": selected_profile,
                "initial_library_snapshot_id": manifest["initial_library_snapshot_id"],
                "server_receipt": server_pin,
                "endpoint": endpoint,
                "case_run_root": _relative(canary_root),
                "case_summary": summary,
            }
            paper_audit._atomic_json(record_path, result)
            result["result_path"] = _relative(record_path)
            result["result_sha256"] = paper_audit._sha(record_path)
            digest_path = output_dir / "canary_result.sha256"
            digest_path.write_text(result["result_sha256"] + "\n", encoding="ascii")
            return result
        except Exception as exc:
            partial = {"schema_version": "e4-paper-operator-code-canary/1",
                "status": "failed", "claim": "single-case engineering canary; no accuracy claim",
                "case_id": case_id, "freeze_manifest_path": _relative(freeze_path),
                "freeze_manifest_sha256": paper_audit._sha(freeze_path),
                "source_fingerprint": manifest["source_fingerprint"],
                "stage3_authoring_profile": selected_profile,
                "initial_library_snapshot_id": manifest["initial_library_snapshot_id"],
                "server_receipt": server_pin, "endpoint": endpoint,
                "case_run_root": _relative(canary_root),
                "error": {"type": type(exc).__name__, "message": str(exc)[:1000]}}
            paper_audit._atomic_json(record_path, partial)
            (output_dir / "canary_result.sha256").write_text(
                paper_audit._sha(record_path) + "\n", encoding="ascii")
            raise
    finally:
        if old_endpoint is None:
            os.environ.pop("E4_BASE_URL", None)
        else:
            os.environ["E4_BASE_URL"] = old_endpoint
        if old_transport is not None:
            os.environ["E4_REMOTE_TRANSPORT"] = old_transport


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, required=True,
        help="verified paper-starter freeze manifest")
    parser.add_argument("--output-dir", type=Path, required=True,
        help="new artifact directory inside the project")
    parser.add_argument("--endpoint", required=True,
        help="root-managed loopback endpoint matching the pinned local server receipt")
    parser.add_argument("--server-receipt", type=Path, required=True,
        help="existing checkpoint/profile/tokenizer receipt for the frozen model")
    args = parser.parse_args(argv)
    try:
        result = run_canary(freeze_path=args.freeze, output_dir=args.output_dir,
            endpoint=args.endpoint, server_receipt=args.server_receipt)
    except Exception as exc:
        parser.exit(1, f"canary failed: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
