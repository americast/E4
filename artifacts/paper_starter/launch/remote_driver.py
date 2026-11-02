#!/usr/bin/env python3
"""Wingtip outer/inner driver for the frozen paper-starter five-case shard."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REMOTE_INFERENCE = ROOT / "artifacts/e4_improvement/remote_wingtip/remote_inference.py"
REMOTE_SOCKET_IN_NAMESPACE = "/run/e4remote/forward.sock"
TRANSPORT_MARKER = "ssh-unix-forward-via-loopback-relay"
PYTHON = ROOT / ".venv/bin/python"


class RemoteAuditError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"preserving existing artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + ".pending")
    pending.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(pending, path)


def _replace_json(path: Path, value: Any) -> None:
    if path.is_symlink():
        raise RemoteAuditError(f"refusing to replace a symlink: {path}")
    pending = path.with_name(path.name + ".pending")
    pending.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(pending, path)


def _validate_server_provenance(provenance_path: Path, remote_port: int) -> dict[str, Any]:
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    attestation = transport.verify_remote_attestation(provenance_path, remote_port=remote_port)
    raw = json.loads(provenance_path.read_text(encoding="utf-8"))
    pid = raw.get("server_pid")
    pgid = raw.get("process_group_id")
    ready = raw.get("server_ready_utc")
    health = raw.get("health_check")
    try:
        ready_at = datetime.fromisoformat(str(ready).replace("Z", "+00:00"))
        if ready_at.tzinfo is None:
            ready_at = ready_at.replace(tzinfo=timezone.utc)
    except ValueError:
        ready_at = None
    age = datetime.now(timezone.utc) - ready_at if ready_at is not None else None
    if (type(pid) is not int or pid <= 1 or type(pgid) is not int or pgid <= 1 or
            not isinstance(ready, str) or not ready or ready_at is None or
            age is None or age < timedelta(minutes=-5) or age > timedelta(hours=24) or
            raw.get("remote_host") != transport.REMOTE_HOST or not isinstance(health, dict) or
            health.get("http_status") != 200 or health.get("body", {}).get("status") != "ok"):
        raise RemoteAuditError("fresh remote provenance lacks live PID, ready time, or successful health receipt")
    return {**attestation, "server_pid": pid, "process_group_id": pgid,
        "server_ready_utc": ready, "health_check": health}


def _write_remote_receipt(*, run_root: Path, audit_dir: Path, freeze_path: Path,
                          provenance_path: Path, remote_port: int) -> tuple[Path, dict[str, Any]]:
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    from scripts.run_paper_starter_audit import preflight
    attestation = _validate_server_provenance(provenance_path, remote_port)
    health = transport.local_health_probe()
    if health.get("http_status") != 200:
        raise RemoteAuditError("live remote health probe through the SSH relay failed")
    tokenizer_dir = run_root / "wingtip_preflight"
    token = preflight(freeze_path, tokenizer_dir, backend="wingtip")
    tokenizer_path = tokenizer_dir / "tokenizer_preflight.json"
    receipt = {"schema_version": "e4-paper-starter-server-receipt/1",
        "backend": "wingtip", "endpoint": os.environ.get("E4_BASE_URL", "http://127.0.0.1:8080"),
        "model_manifest_sha256": _sha(ROOT / "config/model_manifest.json"),
        "checkpoint_sha256": attestation["remote_checkpoint_sha256"],
        "model_revision": json.loads((ROOT / "config/model_manifest.json").read_text())["revision"],
        "profile_sha256": _sha(ROOT / "config/gemma_m3_inference.json"),
        "weights_loaded": True,
        "remote_server_pid": attestation["server_pid"],
        "remote_process_group_id": attestation["process_group_id"],
        "remote_server_ready_utc": attestation["server_ready_utc"],
        "remote_server_health_check": attestation["health_check"],
        "forwarded_health_probe": health,
        "server_command_sha256": hashlib.sha256(json.dumps(
            attestation["remote_server_command"], separators=(",", ":")).encode()).hexdigest(),
        "provenance_path": str(provenance_path.relative_to(ROOT)),
        "provenance_sha256": _sha(provenance_path),
        "tokenizer_preflight_path": str(tokenizer_path.relative_to(ROOT)),
        "tokenizer_preflight_sha256": _sha(tokenizer_path),
        "tokenizer_prompt_archive_path": token["prompt_archive_path"],
        "tokenizer_prompt_archive_sha256": token["prompt_archive_sha256"]}
    if receipt["weights_loaded"] is not True:
        raise RemoteAuditError("remote attestation does not establish the live model server")
    receipt_path = run_root / "wingtip_server_receipt.json"
    _atomic_json(receipt_path, receipt)
    return receipt_path, receipt


def _inner(*, run_root: Path, audit_dir: Path, provenance_path: Path,
           remote_port: int) -> dict[str, Any]:
    from scripts.run_paper_starter_audit import run_shard
    handoff_path = run_root / "launch_handoff.json"
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    if (handoff.get("status") not in {"local_shard_starting", "complete"} or
            (ROOT / handoff.get("audit_dir", "")).resolve(strict=True) != audit_dir.resolve(strict=True)):
        raise RemoteAuditError("frozen local launch handoff is missing or belongs to another audit")
    local_receipt = ROOT / handoff.get("local_server_receipt_path", "")
    if (not local_receipt.is_file() or _sha(local_receipt) != handoff.get("local_server_receipt_sha256")):
        raise RemoteAuditError("local launch receipt in shard handoff is missing or changed")
    freeze_path = ROOT / json.loads((audit_dir / "audit_manifest.json").read_text())["freeze_manifest_path"]
    receipt_path, receipt = _write_remote_receipt(run_root=run_root, audit_dir=audit_dir,
        freeze_path=freeze_path, provenance_path=provenance_path, remote_port=remote_port)
    state = run_shard(audit_dir, "wingtip", server_receipt=receipt_path)
    result = {"schema_version": "e4-paper-starter-wingtip-handoff/1",
        "status": state["status"], "audit_dir": str(audit_dir.relative_to(ROOT)),
        "remote_server_receipt_path": str(receipt_path.relative_to(ROOT)),
        "remote_server_receipt_sha256": _sha(receipt_path),
        "wingtip_status_path": str((audit_dir / "bank10_wingtip" / "bank10_status.json").relative_to(ROOT)),
        "wingtip_status_sha256": _sha(audit_dir / "bank10_wingtip" / "bank10_status.json"),
        "remote_server_pid": receipt["remote_server_pid"],
        "forwarded_health_probe": receipt["forwarded_health_probe"],
        "finished_utc": datetime.now(timezone.utc).isoformat()}
    _atomic_json(run_root / "wingtip_shard_handoff.json", result)
    return result


def _owned_socket_dir(path: Path) -> tuple[Path, Path]:
    raw = Path(os.path.abspath(path))
    if raw.parent != Path("/tmp") or not raw.name.startswith("ew"):
        raise RemoteAuditError("SSH helper returned an unsafe socket directory")
    info = raw.lstat()
    if (stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or
            info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
        raise RemoteAuditError("SSH socket directory is not an owned mode-0700 directory")
    socket_path = raw / "forward.sock"
    socket_info = socket_path.lstat()
    if (stat.S_ISLNK(socket_info.st_mode) or not stat.S_ISSOCK(socket_info.st_mode) or
            socket_info.st_uid != os.getuid() or {item.name for item in raw.iterdir()} != {"forward.sock"}):
        raise RemoteAuditError("SSH socket path is not the sole owned socket in its directory")
    return raw, socket_path


def _cleanup_socket_dir(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"status": "not_created"}
    raw = Path(os.path.abspath(path))
    if raw.parent != Path("/tmp") or not raw.name.startswith("ew"):
        return {"status": "refused_unsafe_path"}
    try:
        info = raw.lstat()
        if (stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or
                info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
            return {"status": "refused_unowned_directory"}
        socket_path = raw / "forward.sock"
        try:
            socket_info = socket_path.lstat()
            if (not stat.S_ISLNK(socket_info.st_mode) and stat.S_ISSOCK(socket_info.st_mode) and
                    socket_info.st_uid == os.getuid()):
                socket_path.unlink()
        except FileNotFoundError:
            pass
        raw.rmdir()
        return {"status": "cleaned"}
    except FileNotFoundError:
        return {"status": "already_absent"}
    except OSError as exc:
        return {"status": "cleanup_failed", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}


def outer(*, audit_dir: Path, provenance_path: Path, remote_port: int) -> dict[str, Any]:
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    from scripts.run_paper_starter_audit import _verify_audit
    audit_dir = audit_dir.resolve(strict=True)
    manifest, _ = _verify_audit(audit_dir)
    handoff_path = audit_dir.parent.parent / "launch_handoff.json"
    handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    run_root = (ROOT / handoff["run_root"]).resolve(strict=True)
    if (handoff.get("status") not in {"local_shard_starting", "complete"} or
            (ROOT / handoff.get("audit_dir", "")).resolve(strict=True) != audit_dir or
            handoff.get("freeze_manifest_sha256") != manifest.get("freeze_manifest_sha256")):
        raise RemoteAuditError("local freeze handoff is missing or does not match --audit-dir")
    local_receipt_path = ROOT / handoff.get("local_server_receipt_path", "")
    if not local_receipt_path.is_file() or _sha(local_receipt_path) != handoff.get("local_server_receipt_sha256"):
        raise RemoteAuditError("local launch receipt changed after the immutable freeze handoff")
    local_receipt = json.loads(local_receipt_path.read_text(encoding="utf-8"))
    model_manifest = json.loads((ROOT / manifest["model_manifest_path"]).read_text(encoding="utf-8"))
    if (local_receipt.get("backend") != "local" or local_receipt.get("weights_loaded") is not True or
            local_receipt.get("model_manifest_sha256") != manifest["model_manifest_sha256"] or
            local_receipt.get("checkpoint_sha256") != model_manifest.get("sha256") or
            local_receipt.get("profile_sha256") != manifest["model_profile_sha256"]):
        raise RemoteAuditError("local server receipt is malformed or was not produced by the local launch driver")
    if os.environ.get("E4_RUN_DIR"):
        configured = Path(os.environ["E4_RUN_DIR"]).resolve(strict=True)
        if configured != run_root:
            raise RemoteAuditError("E4_RUN_DIR differs from the local model run root in handoff")
    attestation = _validate_server_provenance(provenance_path, remote_port)
    status_path = run_root / "wingtip_remote_status.json"
    if status_path.exists() or status_path.is_symlink():
        raise RemoteAuditError("preserving an existing Wingtip launch trajectory")
    state = {"schema_version": "e4-paper-starter-wingtip-outer/1", "status": "starting",
        "audit_dir": str(audit_dir.relative_to(ROOT)), "audit_manifest_sha256": _sha(audit_dir / "audit_manifest.json"),
        "remote_provenance_path": str(Path(provenance_path).resolve(strict=True).relative_to(ROOT)),
        "remote_provenance_sha256": _sha(provenance_path), "remote_attestation": attestation,
        "remote_port": remote_port, "run_root": str(run_root.relative_to(ROOT)),
        "started_utc": datetime.now(timezone.utc).isoformat()}
    _atomic_json(status_path, state)
    ssh_process = None
    socket_dir = None
    cleanup = {"status": "not_started"}
    try:
        ssh_log = run_root / "wingtip_ssh.log"
        ssh_process, socket_dir, connect_seconds = transport.connect_ssh(remote_port, ssh_log=ssh_log)
        owned_dir, socket_path = _owned_socket_dir(socket_dir)
        command = ["/usr/bin/env", f"E4_REMOTE_TRANSPORT={TRANSPORT_MARKER}",
            str(PYTHON), str(Path(__file__).resolve()), "--inner",
            "--run-root", str(run_root), "--audit-dir", str(audit_dir),
            "--provenance", str(Path(provenance_path).resolve()), "--port", str(remote_port)]
        namespace_command = [str(PYTHON), str(REMOTE_INFERENCE), "--namespace-child",
            "--socket-path", REMOTE_SOCKET_IN_NAMESPACE, "--run-dir", str(run_root),
            "--worker-mode", "diagnostic", "--", *command]
        stdout_path = run_root / "wingtip_remote.stdout.log"
        stderr_path = run_root / "wingtip_remote.stderr.log"
        result = transport.run_bwrap(command=namespace_command, socket_dir=owned_dir,
            run_dir=run_root, stdout_path=stdout_path, stderr_path=stderr_path,
            timeout_seconds=40000)
        record = result.get("namespace_record")
        if (result.get("exit_code") != 0 or not isinstance(record, dict) or
                record.get("command_exit_code") != 0 or not record.get("health_probe")):
            raise RemoteAuditError("Wingtip isolated preflight/shard failed; trajectory and logs are preserved")
        wingtip_handoff_path = run_root / "wingtip_shard_handoff.json"
        wingtip_handoff = json.loads(wingtip_handoff_path.read_text(encoding="utf-8"))
        state.update({"status": wingtip_handoff.get("status"), "ssh_connect_seconds": connect_seconds,
            "ssh_socket_path": str(socket_path), "namespace_record": record,
            "wingtip_handoff_path": str(wingtip_handoff_path.relative_to(ROOT)),
            "wingtip_handoff_sha256": _sha(wingtip_handoff_path),
            "finished_utc": datetime.now(timezone.utc).isoformat()})
        _atomic_json(run_root / "wingtip_remote_complete.json", state)
        return state
    except Exception as exc:
        state.update({"status": "failed_infrastructure",
            "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
            "failed_utc": datetime.now(timezone.utc).isoformat()})
        raise
    finally:
        transport.terminate_own_process(ssh_process)
        cleanup = _cleanup_socket_dir(socket_dir)
        state["ssh_cleanup"] = cleanup
        state["ssh_process_terminated"] = ssh_process is None or ssh_process.poll() is not None
        _replace_json(status_path, state)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--inner", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--run-root", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.inner:
        if args.audit_dir is None or args.run_root is None:
            parser.error("inner mode requires --audit-dir and --run-root")
        result = _inner(run_root=args.run_root.resolve(strict=True), audit_dir=args.audit_dir.resolve(strict=True),
            provenance_path=args.provenance.resolve(strict=True), remote_port=args.port)
    else:
        if args.audit_dir is None:
            parser.error("outer mode requires --audit-dir")
        result = outer(audit_dir=args.audit_dir, provenance_path=args.provenance, remote_port=args.port)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
