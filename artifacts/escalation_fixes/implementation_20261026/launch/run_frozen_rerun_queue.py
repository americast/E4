#!/usr/bin/env python3
"""Autonomously run sealed local/Wingtip shards for inference seeds 42/43/44.

Local M3 jobs run sequentially. Each Wingtip shard starts as soon as its local
launch handoff exists and the remote queue processes seeds sequentially. This
coordinator never resumes or replaces an existing trajectory.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from artifacts.escalation_fixes.implementation_20261026.launch.prepare_frozen_rerun import (
    ALLOWED_SEEDS, validate_launch_bundle,
)


class QueueError(RuntimeError):
    pass


def _require_remote_cleanup(seed: int, cleanup: Mapping[str, Any]) -> None:
    if cleanup.get("status") not in {"terminated", "already_absent"}:
        raise QueueError(f"seed{seed} remote cleanup refused or failed identity check")


def _sha(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read(path: Path) -> dict[str, Any] | None:
    if not path.is_file() or path.is_symlink():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


class _QueueState:
    def __init__(self, path: Path, manifest_path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.document = {"schema_version": "e4-frozen-rerun-queue/1",
            "status": "running", "rerun_manifest_path": manifest_path.relative_to(ROOT).as_posix(),
            "rerun_manifest_sha256": _sha(manifest_path),
            "started_utc": datetime.now(timezone.utc).isoformat(),
            "seeds": {}, "events": []}
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(self.document, sort_keys=True, indent=2) + "\n")

    def update(self, event: str, *, seed: int | None = None,
               fields: dict[str, Any] | None = None) -> None:
        with self.lock:
            now = datetime.now(timezone.utc).isoformat()
            row = {"event": event, "utc": now, **dict(fields or {})}
            self.document["events"].append(row)
            self.document["updated_utc"] = now
            if seed is not None:
                seed_row = self.document["seeds"].setdefault(str(seed), {"seed": seed})
                seed_row.update({"last_event": event, "updated_utc": now, **dict(fields or {})})
            pending = self.path.with_name(self.path.name + ".pending")
            pending.write_text(json.dumps(self.document, ensure_ascii=False,
                sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")
            os.replace(pending, self.path)

    def finish(self, status: str, *, error: str | None = None) -> None:
        with self.lock:
            self.document.update({"status": status,
                "error": error,
                "finished_utc": datetime.now(timezone.utc).isoformat()})
            pending = self.path.with_name(self.path.name + ".pending")
            pending.write_text(json.dumps(self.document, ensure_ascii=False,
                sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")
            os.replace(pending, self.path)


def _clean_child_env() -> dict[str, str]:
    env = dict(os.environ)
    for name in ("E4_RUN_DIR", "E4_BASE_URL", "E4_BWRAPPED", "E4_M4_INNER",
                 "E4_M4_IMPLEMENTATION_FINGERPRINT", "E4_REMOTE_TRANSPORT"):
        env.pop(name, None)
    return env


def _local_command(manifest: dict[str, Any], seed: int) -> list[str]:
    command = list(manifest["launch_commands_by_seed"][str(seed)]["local"])
    # Python executables are relative to ROOT; script/profile/manifest paths
    # are made explicit so command logs are self-contained.
    for index in (1, 3, 10, 14):
        command[index] = str(ROOT / command[index])
    return command


def _remote_command(seed: int, manifest_path: Path, audit_dir: Path,
        provenance_path: Path, port: int) -> list[str]:
    driver = ROOT / "artifacts/escalation_fixes/implementation_20261026/launch/remote_driver.py"
    return [str(ROOT / ".venv/bin/python"), str(driver),
        "--seed", str(seed), "--rerun-manifest", str(manifest_path),
        "--audit-dir", str(audit_dir), "--provenance", str(provenance_path),
        "--port", str(port)]


def _terminate_owned_child(process: subprocess.Popen[Any]) -> dict[str, Any]:
    """Terminate only the new session created for this queue's subprocess."""
    if process.poll() is not None:
        return {"status": "already_exited", "exit_code": process.returncode}
    try:
        pgid = os.getpgid(process.pid)
    except ProcessLookupError:
        return {"status": "already_exited"}
    if pgid != process.pid:
        return {"status": "refused_unowned_process_group", "pid": process.pid, "pgid": pgid}
    os.killpg(pgid, signal.SIGTERM)
    try:
        return {"status": "terminated", "exit_code": process.wait(timeout=30)}
    except subprocess.TimeoutExpired:
        if process.poll() is None and os.getpgid(process.pid) == pgid:
            os.killpg(pgid, signal.SIGKILL)
        try:
            return {"status": "killed_after_timeout", "exit_code": process.wait(timeout=10)}
        except subprocess.TimeoutExpired:
            return {"status": "cleanup_timeout", "pid": process.pid, "pgid": pgid}


def _local_completion(seed_root: Path, model_run_root: Path, *, seed: int,
                      profile_sha: str, run_id: str) -> dict[str, Any]:
    run = _read(model_run_root / "run.json")
    outer = _read(model_run_root / "outer-supervisor.json")
    launch = _read(model_run_root / "launch.json")
    if not run or not outer or not launch:
        raise QueueError(f"seed{seed} local M3 launcher lacks final receipts")
    gpu = run.get("gpu_memory", {})
    if (run.get("ok") is not True or run.get("exit_code") != 0 or
            run.get("command_exit_code") != 0 or run.get("server_exit_code") is None or
            type(run.get("server_load_seconds")) not in (int, float) or
            run.get("server_load_seconds", 0) <= 0 or
            gpu.get("reserve_satisfied") is not True or
            run.get("run_id") != run_id or
            outer.get("exit_code") != 0 or outer.get("bubblewrap_exit_code") != 0 or
            outer.get("error") is not None or launch.get("profile_sha256") != profile_sha):
        raise QueueError(f"seed{seed} local model launcher did not close under its sealed profile")
    handoff_path = model_run_root / "launch_handoff.json"
    handoff = _read(handoff_path)
    proof_path = model_run_root / "local_request_seed_proof.json"
    proof = _read(proof_path)
    if (not handoff or handoff.get("status") != "complete" or not proof or
            proof.get("seed") != seed or proof.get("all_request_seeds_match") is not True or
            proof.get("case_count") != 5 or proof.get("request_count", 0) <= 0 or
            proof.get("profile_sha256") != profile_sha):
        raise QueueError(f"seed{seed} local shard lacks complete handoff/request-seed proof")
    audit_dir = ROOT / handoff["audit_dir"]
    status_path = audit_dir / "bank10_local" / "bank10_status.json"
    state = _read(status_path)
    if not state or state.get("status") != "complete" or len(state.get("completed_cases", [])) != 5:
        raise QueueError(f"seed{seed} local shard did not complete the five fixed cases")
    return {"handoff_path": handoff_path.relative_to(ROOT).as_posix(),
        "handoff_sha256": _sha(handoff_path), "audit_dir": handoff["audit_dir"],
        "local_status_path": status_path.relative_to(ROOT).as_posix(),
        "local_status_sha256": _sha(status_path),
        "request_seed_proof_path": proof_path.relative_to(ROOT).as_posix(),
        "request_seed_proof_sha256": _sha(proof_path),
        "local_model_run_sha256": _sha(model_run_root / "run.json")}


def _combine_seed(seed: int, audit_dir: Path, manifest: dict[str, Any], *,
                  profile_sha: str) -> dict[str, Any]:
    from scripts.run_paper_starter_audit import combine
    local_path = audit_dir / "bank10_local" / "bank10_status.json"
    remote_path = audit_dir / "bank10_wingtip" / "bank10_status.json"
    local, remote = _read(local_path), _read(remote_path)
    if (not local or not remote or local.get("status") != "complete" or
            remote.get("status") != "complete" or
            len(local.get("completed_cases", [])) != 5 or
            len(remote.get("completed_cases", [])) != 5):
        raise QueueError(f"seed{seed} cannot combine incomplete shard trajectories")
    for shard, expected in (("local", manifest["shard_groups"]["local"]),
                            ("wingtip", manifest["shard_groups"]["wingtip"])):
        state = local if shard == "local" else remote
        if [row.get("case_id") for row in state["completed_cases"]] != expected:
            raise QueueError(f"seed{seed} {shard} shard order differs from the sealed plan")
    audit_manifest = _read(audit_dir / "audit_manifest.json")
    if (not audit_manifest or audit_manifest.get("model_profile_sha256") != profile_sha or
            audit_manifest.get("freeze_manifest_sha256") is None):
        raise QueueError(f"seed{seed} combined audit manifest has wrong profile/freeze pin")
    combined_path = audit_dir / "combined_bank10_result.json"
    if combined_path.exists() or combined_path.is_symlink():
        raise QueueError(f"seed{seed} combined result already exists; preserving it")
    result = combine(audit_dir, local_path, remote_path)
    if not combined_path.is_file():
        raise QueueError(f"seed{seed} audit combiner did not write its trusted result")
    return {"combined_result_path": combined_path.relative_to(ROOT).as_posix(),
        "combined_result_sha256": _sha(combined_path),
        "case_count": result.get("case_count"),
        "cumulative_top3_prefix_successes": result.get("cumulative_top3_prefix_successes"),
        "local_status_sha256": _sha(local_path), "wingtip_status_sha256": _sha(remote_path)}


def _finalize_waiting_seeds(manifest_path: Path, manifest: dict[str, Any],
        state: _QueueState) -> str | None:
    for seed in manifest["model_inference_seeds"]:
        queue_row = state.document["seeds"].get(str(seed), {})
        if isinstance(queue_row.get("combined"), dict):
            continue
        seed_row = next(row for row in manifest["seeds"] if row["seed"] == seed)
        model_run_root = ROOT / seed_row["model_run_root"]
        local_run = _read(model_run_root / "run.json")
        if not local_run:
            continue
        try:
            _local_completion(ROOT / seed_row["seed_root"], model_run_root,
                seed=seed, profile_sha=seed_row["profile_sha256"],
                run_id=seed_row["model_run_id"])
        except Exception:
            continue
        handoff = _read(model_run_root / "launch_handoff.json")
        if not handoff:
            continue
        audit_dir = ROOT / handoff["audit_dir"]
        remote_status = _read(audit_dir / "bank10_wingtip" / "bank10_status.json")
        proof = _read(model_run_root / "wingtip_request_seed_proof.json")
        cleanup = queue_row.get("cleanup")
        if (not remote_status or remote_status.get("status") != "complete" or
                not proof or proof.get("seed") != seed or
                proof.get("all_request_seeds_match") is not True or
                proof.get("profile_sha256") != seed_row["profile_sha256"] or
                not isinstance(cleanup, Mapping) or
                cleanup.get("status") not in {"terminated", "already_absent"}):
            continue
        try:
            combined = _combine_seed(seed, audit_dir, manifest,
                profile_sha=seed_row["profile_sha256"])
        except Exception as exc:
            return f"seed{seed} final combination failed: {type(exc).__name__}: {str(exc)[:800]}"
        state.update("seed_complete", seed=seed, fields={"combined": combined,
            "request_count_local": _read(model_run_root / "local_request_seed_proof.json").get("request_count"),
            "request_count_wingtip": proof["request_count"],
            "cleanup_status": cleanup["status"]})
    return None


def _remote_worker(remote_tasks: "queue.Queue[int | None]", *,
        manifest_path: Path, manifest: dict[str, Any], state: _QueueState,
        stop: threading.Event, wingtip_port: int, timeout_seconds: int) -> None:
    from artifacts.paper_starter.launch.start_wingtip_server import start as start_wingtip
    from artifacts.paper_starter.launch.finalize_audit import _cleanup_owned_server

    while True:
        seed = remote_tasks.get()
        if seed is None:
            return
        if stop.is_set():
            state.update("remote_not_started_after_prior_failure", seed=seed)
            continue
        provenance_path: Path | None = None
        try:
            rerun, seed_row, _metadata_root = validate_launch_bundle(
                manifest_path, seed, require_local_handoff=True)
            model_run_root = (ROOT / seed_row["model_run_root"]).resolve(strict=True)
            handoff_path = model_run_root / "launch_handoff.json"
            handoff = _read(handoff_path)
            if not handoff:
                raise QueueError(f"seed{seed} local handoff disappeared")
            audit_dir = (ROOT / handoff["audit_dir"]).resolve(strict=True)
            state.update("wingtip_server_starting", seed=seed,
                fields={"model_run_root": seed_row["model_run_root"],
                    "audit_dir": audit_dir.relative_to(ROOT).as_posix()})
            provenance_path = model_run_root / "wingtip_server_provenance.json"
            started = start_wingtip(model_run_root, wingtip_port)
            if provenance_path != ROOT / started["provenance_path"]:
                raise QueueError("Wingtip startup returned provenance outside its model-run root")
            if started.get("status") != "ready" or not provenance_path.is_file():
                raise QueueError(f"seed{seed} Wingtip startup did not return verified provenance")
            state.update("wingtip_server_ready", seed=seed, fields={
                "provenance_path": started["provenance_path"],
                "provenance_sha256": started["provenance_sha256"],
                "remote_server_pid": started["remote_attestation"].get("server_pid")})
            command = _remote_command(seed, manifest_path, audit_dir,
                provenance_path, wingtip_port)
            output_path = model_run_root / "queue_wingtip.stdout.log"
            error_path = model_run_root / "queue_wingtip.stderr.log"
            if output_path.exists() or error_path.exists():
                raise QueueError(f"seed{seed} Wingtip queue logs already exist")
            with output_path.open("xb") as stdout, error_path.open("xb") as stderr:
                process = subprocess.Popen(command, cwd=ROOT, env=_clean_child_env(),
                    stdout=stdout, stderr=stderr, start_new_session=True, close_fds=True)
                try:
                    exit_code = process.wait(timeout=timeout_seconds + 300)
                except subprocess.TimeoutExpired as exc:
                    child_cleanup = _terminate_owned_child(process)
                    state.update("wingtip_driver_timeout_cleanup", seed=seed,
                        fields={"process_cleanup": child_cleanup})
                    raise QueueError(f"seed{seed} Wingtip driver exceeded its outer deadline") from exc
            if exit_code != 0:
                raise QueueError(f"seed{seed} Wingtip driver exited {exit_code}; logs preserved")
            handoff_result = _read(model_run_root / "wingtip_shard_handoff.json")
            proof = _read(model_run_root / "wingtip_request_seed_proof.json")
            if (not handoff_result or handoff_result.get("status") != "complete" or
                    not proof or proof.get("seed") != seed or
                    proof.get("all_request_seeds_match") is not True or
                    proof.get("case_count") != 5 or proof.get("request_count", 0) <= 0 or
                    proof.get("profile_sha256") != seed_row["profile_sha256"]):
                raise QueueError(f"seed{seed} Wingtip handoff lacks actual request-seed proof")
            remote_state = _read(audit_dir / "bank10_wingtip" / "bank10_status.json")
            if not remote_state or remote_state.get("status") != "complete":
                raise QueueError(f"seed{seed} Wingtip shard state is not complete")
            cleanup = _cleanup_owned_server(provenance_path)
            state.update("wingtip_server_cleanup", seed=seed,
                fields={"cleanup": cleanup})
            _require_remote_cleanup(seed, cleanup)
            local = _read(audit_dir / "bank10_local" / "bank10_status.json")
            if local and local.get("status") == "complete":
                combined = _combine_seed(seed, audit_dir, rerun,
                    profile_sha=seed_row["profile_sha256"])
                state.update("seed_complete", seed=seed, fields={
                    "combined": combined, "request_count_local": _read(
                        model_run_root / "local_request_seed_proof.json").get("request_count"),
                    "request_count_wingtip": proof["request_count"],
                    "cleanup_status": cleanup["status"]})
            else:
                state.update("wingtip_complete_local_pending", seed=seed,
                    fields={"request_count_wingtip": proof["request_count"],
                        "cleanup_status": cleanup["status"]})
        except Exception as exc:
            if provenance_path is not None and provenance_path.is_file():
                cleanup = _cleanup_owned_server(provenance_path)
                state.update("wingtip_failure_server_cleanup", seed=seed,
                    fields={"cleanup": cleanup})
            state.update("remote_seed_failed", seed=seed,
                fields={"error": f"{type(exc).__name__}: {str(exc)[:1000]}"})
            stop.set()
            return


def run(manifest_path: Path, *, wingtip_port: int = 18083,
        poll_seconds: float = 5.0, timeout_seconds: int = 40000) -> dict[str, Any]:
    manifest_path = manifest_path.resolve(strict=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "e4-paper-starter-frozen-rerun/1":
        raise QueueError("unsupported rerun manifest schema")
    seeds = manifest.get("model_inference_seeds")
    if not isinstance(seeds, list) or not seeds or any(seed not in ALLOWED_SEEDS for seed in seeds):
        raise QueueError("manifest has no supported seed list")
    run_root = manifest_path.parent
    status_path = run_root / "queue_status.json"
    state = _QueueState(status_path, manifest_path)
    remote_tasks: "queue.Queue[int | None]" = queue.Queue()
    stop = threading.Event()
    remote_thread = threading.Thread(target=_remote_worker,
        kwargs={"remote_tasks": remote_tasks, "manifest_path": manifest_path,
            "manifest": manifest, "state": state, "stop": stop,
            "wingtip_port": wingtip_port, "timeout_seconds": timeout_seconds},
        name="paper-rerun-wingtip-queue", daemon=False)
    remote_thread.start()
    failure: str | None = None
    active_local_process: subprocess.Popen[Any] | None = None
    try:
        for seed in seeds:
            if stop.is_set():
                state.update("local_queue_stopped_after_remote_failure", seed=seed)
                break
            rerun, seed_row, seed_root = validate_launch_bundle(manifest_path, seed)
            model_run_root = ROOT / seed_row["model_run_root"]
            if model_run_root.exists() or model_run_root.is_symlink():
                raise QueueError(f"seed{seed} model-run path already exists; refusing to retry")
            command = _local_command(rerun, seed)
            stdout_path, stderr_path = seed_root / "local_queue.stdout.log", seed_root / "local_queue.stderr.log"
            if stdout_path.exists() or stderr_path.exists():
                raise QueueError(f"seed{seed} local queue logs already exist; no retry")
            state.update("local_starting", seed=seed,
                fields={"model_run_id": seed_row["model_run_id"],
                    "model_run_root": seed_row["model_run_root"], "command": command})
            with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
                process = subprocess.Popen(command, cwd=ROOT, env=_clean_child_env(),
                    stdout=stdout, stderr=stderr, start_new_session=True, close_fds=True)
                active_local_process = process
                deadline = time.monotonic() + timeout_seconds + 1800
                handoff_path = model_run_root / "launch_handoff.json"
                while time.monotonic() < deadline and not handoff_path.is_file():
                    code = process.poll()
                    if code is not None:
                        raise QueueError(f"seed{seed} local model process exited {code} before handoff")
                    time.sleep(poll_seconds)
                if not handoff_path.is_file():
                    raise QueueError(f"seed{seed} local run produced no handoff before deadline")
                handoff = _read(handoff_path)
                if not handoff or handoff.get("status") not in {"local_shard_starting", "complete"}:
                    raise QueueError(f"seed{seed} local handoff is malformed")
                remote_tasks.put(seed)
                state.update("local_handoff_ready_remote_queued", seed=seed,
                    fields={"handoff_path": handoff_path.relative_to(ROOT).as_posix(),
                        "handoff_sha256": _sha(handoff_path),
                        "audit_dir": handoff.get("audit_dir")})
                try:
                    exit_code = process.wait(timeout=max(1.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired as exc:
                    raise QueueError(f"seed{seed} local model command exceeded its outer deadline") from exc
            if exit_code != 0:
                raise QueueError(f"seed{seed} local model command exited {exit_code}; trajectory preserved")
            active_local_process = None
            local_result = _local_completion(seed_root, model_run_root,
                seed=seed, profile_sha=seed_row["profile_sha256"],
                run_id=seed_row["model_run_id"])
            state.update("local_complete", seed=seed, fields=local_result)
    except Exception as exc:
        local_cleanup = (None if active_local_process is None else
            _terminate_owned_child(active_local_process))
        failure = f"{type(exc).__name__}: {str(exc)[:1000]}"
        stop.set()
        state.update("queue_failed", fields={"error": failure,
            "local_process_cleanup": local_cleanup})
    finally:
        remote_tasks.put(None)
        remote_thread.join()
        if failure is None and stop.is_set():
            failure = "remote queue failed"
        if failure is None:
            failure = _finalize_waiting_seeds(manifest_path, manifest, state)
        if failure is None:
            incomplete = [seed for seed in seeds
                if not isinstance(state.document["seeds"].get(str(seed), {}).get("combined"), dict)]
            if incomplete:
                failure = f"seeds lack reconciled local/Wingtip results: {incomplete}"
        final_status = "complete" if failure is None else "failed_infrastructure"
        state.finish(final_status, error=failure)
    if failure is None:
        try:
            from artifacts.escalation_fixes.implementation_20261026.launch.report_frozen_reruns import (
                write_immutable_report,
            )
            report_receipt = write_immutable_report(manifest_path, status_path)
            state.update("report_written", fields={"report_receipt": report_receipt})
        except Exception as exc:
            # Reporting is a separate artifact-only finalizer. Keep the verified
            # model trajectories complete even if a report artifact cannot be written.
            state.update("report_failed", fields={
                "report_error": f"{type(exc).__name__}: {str(exc)[:1000]}"})
    if failure is not None:
        raise QueueError(failure)
    return _read(status_path) or {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rerun-manifest", type=Path, required=True)
    parser.add_argument("--wingtip-port", type=int, default=18083)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=int, default=40000)
    args = parser.parse_args()
    result = run(args.rerun_manifest, wingtip_port=args.wingtip_port,
        poll_seconds=args.poll_seconds, timeout_seconds=args.timeout_seconds)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
