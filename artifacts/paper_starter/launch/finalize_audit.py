#!/usr/bin/env python3
"""Wait for both paper-starter shards, reconcile ledgers, report, and release owned Wingtip PID."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FinalizeError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _replace_json(path: Path, value: Any) -> None:
    pending = path.with_name(path.name + ".pending")
    pending.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(pending, path)


def _exclusive_text(path: Path, value: str) -> str:
    """Publish a complete file atomically without ever replacing an old result."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.{time.time_ns()}.pending")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        os.unlink(temporary)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return _sha(path)


def _handoff(run_root: Path) -> tuple[dict[str, Any], Path]:
    path = run_root / "launch_handoff.json"
    if not path.is_file() or path.is_symlink():
        raise FileNotFoundError("local launch_handoff.json is not available yet")
    handoff = json.loads(path.read_text(encoding="utf-8"))
    if handoff.get("status") not in {"local_shard_starting", "complete"}:
        raise FinalizeError(f"local handoff has terminal status {handoff.get('status')!r}")
    audit_dir = (ROOT / handoff["audit_dir"]).resolve(strict=True)
    if not audit_dir.is_relative_to(run_root):
        raise FinalizeError("audit directory does not belong to this model run root")
    return handoff, audit_dir


def _status(run_root: Path, audit_dir: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    local_path = audit_dir / "bank10_local" / "bank10_status.json"
    wingtip_path = audit_dir / "bank10_wingtip" / "bank10_status.json"
    local = json.loads(local_path.read_text()) if local_path.is_file() else None
    wingtip = json.loads(wingtip_path.read_text()) if wingtip_path.is_file() else None
    outer_path = run_root / "wingtip_remote_status.json"
    outer = json.loads(outer_path.read_text()) if outer_path.is_file() else None
    failed = None
    for label, state in (("local", local), ("wingtip", wingtip), ("wingtip_outer", outer)):
        if state and state.get("status") in {"failed_infrastructure", "failed"}:
            failed = f"{label} status is {state['status']}"
            break
    # A terminal local supervisor failure invalidates an incomplete local
    # shard even when its last checkpoint still says `running`.
    local_incomplete = not isinstance(local, dict) or local.get("status") != "complete"
    if failed is None and local_incomplete:
        for name in ("outer-supervisor.json", "run.json"):
            source = run_root / name
            if not source.is_file():
                continue
            try:
                doc = json.loads(source.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            exit_values = [doc.get(key) for key in ("exit_code", "bubblewrap_exit_code", "server_exit_code")]
            nonzero_exit = any(type(value) is int and value != 0 for value in exit_values)
            failed_status = doc.get("status") in {"failed", "failed_infrastructure", "error"}
            if nonzero_exit or failed_status:
                failed = f"local {name} records terminal failure while the shard is incomplete"
                break
    return local, wingtip, failed


def _complete(local: dict[str, Any] | None, wingtip: dict[str, Any] | None) -> bool:
    return all(isinstance(state, dict) and state.get("status") == "complete" and
        len(state.get("completed_cases", [])) == 5 for state in (local, wingtip))


def _engineering_attempt(path: Path) -> dict[str, Any]:
    root = path.resolve(strict=True)
    if not root.is_dir() or not root.is_relative_to(ROOT):
        raise FinalizeError(f"engineering attempt must be a project directory: {path}")
    evidence = None
    for name in ("engineering_abort.json", "paper_starter_finalize_status.json",
                 "outer-supervisor.json", "local_shard_handoff.json", "launch_handoff.json"):
        candidate = root / name
        if candidate.is_file() and not candidate.is_symlink():
            evidence = candidate
            break
    if evidence is None:
        raise FinalizeError(f"engineering attempt has no preserved status evidence: {root}")
    document = json.loads(evidence.read_text(encoding="utf-8"))
    reason = document.get("reason") or document.get("incomplete_reason") or document.get("error")
    status_value = document.get("status")
    if status_value is None and any(type(document.get(key)) is int and document.get(key) != 0
            for key in ("exit_code", "bubblewrap_exit_code", "server_exit_code")):
        status_value = "failed_infrastructure"
    trajectories: list[dict[str, Any]] = []
    handoff_path = root / "launch_handoff.json"
    if handoff_path.is_file() and not handoff_path.is_symlink():
        try:
            handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
            audit_dir = (ROOT / handoff["audit_dir"]).resolve(strict=True)
            if audit_dir.is_relative_to(root):
                for shard in ("local", "wingtip"):
                    state_path = audit_dir / f"bank10_{shard}" / "bank10_status.json"
                    if not state_path.is_file() or state_path.is_symlink():
                        continue
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                    for row in state.get("completed_cases", []):
                        trajectories.append({"shard": shard, "case_id": row.get("case_id"),
                            "controller_status": row.get("status", "completed"),
                            "stage_statuses": [slot.get("status") for slot in row.get("stage_slots", [])]})
                    current = state.get("current_case")
                    if current and current.get("case_id"):
                        case_root = state_path.parent / f"case{int(current.get('case_index', 0)):02d}"
                        result_path = case_root / "diagnostic" / "diagnostic_result.json"
                        stage_statuses: list[str] = []
                        controller_status = str(current.get("status", "running"))
                        if result_path.is_file():
                            result = json.loads(result_path.read_text(encoding="utf-8"))
                            controller_status = result.get("status", controller_status)
                            stage_statuses = [item.get("status") for item in result.get("stages", [])]
                        else:
                            for stage_record in sorted((case_root / "diagnostic" / "attempts").glob("*/stage_record.json")):
                                try:
                                    stage_statuses.append(json.loads(stage_record.read_text()).get("status", "recorded"))
                                except (OSError, json.JSONDecodeError):
                                    stage_statuses.append("record_malformed")
                        trajectories.append({"shard": shard, "case_id": current["case_id"],
                            "controller_status": controller_status, "stage_statuses": stage_statuses})
        except (OSError, KeyError, ValueError, json.JSONDecodeError):
            trajectories.append({"shard": "unknown", "case_id": None,
                "controller_status": "trajectory_metadata_unavailable", "stage_statuses": []})
    return {"run_root": str(root.relative_to(ROOT)),
        "status": status_value or "incomplete_engineering_attempt",
        "reason": str(reason or "partial engineering trajectory preserved")[:1000],
        "evidence_path": str(evidence.relative_to(ROOT)),
        "evidence_sha256": _sha(evidence), "trajectories": trajectories}


def _results_markdown(result: dict[str, Any], freeze: dict[str, Any], *, run_root: Path,
                      engineering_attempts: list[dict[str, Any]]) -> str:
    counts = result["cumulative_top3_prefix_successes"]
    denominator = result["case_count"]
    t2 = 4
    lines = [
        "# Paper Starter Bank10 Exploratory Results",
        "",
        f"Run root: `{run_root.relative_to(ROOT)}`  ",
        f"Freeze: `{freeze['run_id']}`  ",
        f"Profile: `{freeze['profile']}`  ",
        f"Initial Bank snapshot: `{freeze['active_bank_projection']['snapshot_id']}`",
        "",
        f"This is the corrected implementation revision frozen as `{freeze['run_id']}`. It follows preserved partial engineering runs and records their failures below. This run is an exploratory reuse of the exact historical M5 T2 scored cases; it is not a fresh holdout and is not an official M5/M6 result. The ten-case denominator below belongs only to this corrected revision. Historical T2 was 4/10 on these same cases; that result was not rerun for this report.",
        "",
        "## Cumulative top three success",
        "",
        "| Stage prefix | Correct cases | Accuracy |",
        "| --- | ---: | ---: |",
    ]
    for stage in (1, 2, 3):
        correct = counts[f"stage{stage}_prefix_successes_of_{denominator}"]
        lines.append(f"| Stage {stage} | {correct}/{denominator} | {100.0 * correct / denominator:.1f}% |")
    lines += ["", f"Historical M5 T2 on the same cases: **{t2}/10 (40.0%)**.", "",
        "## Per-case controller slots", "",
        "| Case | Stage 1 | Stage 2 | Stage 3 | Accepted |", "| --- | --- | --- | --- | ---: |"]
    for row in result["cases"]:
        slots = row["stage_slots"]
        cells = []
        for slot in slots:
            status = slot.get("status", "not_reached")
            if slot.get("accepted") is True:
                status += " ✓"
            elif slot.get("accepted") is False:
                status += " ·"
            cells.append(status)
        accepted = "yes" if row.get("accepted") else "no"
        lines.append(f"| `{row['case_id']}` | {cells[0]} | {cells[1]} | {cells[2]} | {accepted} |")
    lines += ["", "## Resource totals", "",
        f"- Model calls: {result.get('total_model_calls')}.",
        f"- Prompt tokens: {result.get('total_prompt_tokens')}.",
        f"- Completion tokens: {result.get('total_completion_tokens')}.",
        f"- Charged generated tokens: {result.get('total_charged_generated_tokens')}.",
        f"- Model seconds: {result.get('total_model_seconds')}.",
        f"- Wall seconds: {result.get('total_wall_seconds')}.",
        "- Local and Wingtip library chains started from the same immutable snapshot and remained independent.",
        "", "The trusted host reconciled all ten evaluator ledgers, diagnostic results, and library lineages before this report was written.", ""]
    if engineering_attempts:
        lines += ["## Preserved prior engineering attempts", "",
            "These incomplete trajectories are reported as engineering/API failures, not diagnostic misses. They are excluded from the corrected revision’s ten-case denominator and no failed attempt was removed.", "",
            "| Preserved run root | Status | Failure reason | Evidence |",
            "| --- | --- | --- | --- |"]
        for attempt in engineering_attempts:
            reason = attempt["reason"].replace("|", "\\|")
            lines.append(f"| `{attempt['run_root']}` | {attempt['status']} | {reason} | `{attempt['evidence_path']}` (`{attempt['evidence_sha256'][:12]}`) |")
            for trajectory in attempt.get("trajectories", []):
                stages = ", ".join(str(item) for item in trajectory.get("stage_statuses", [])) or "no completed controller stage"
                lines.append(f"| ↳ {trajectory.get('shard')} trajectory `{trajectory.get('case_id')}` | {trajectory.get('controller_status')} | partial attempt preserved; excluded from corrected denominator | stages: {stages} |")
        lines.append("")
    return "\n".join(lines)


def _cleanup_owned_server(provenance_path: Path) -> dict[str, Any]:
    """SSH to verify the exact remote process identity, then signal only its PID."""
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    if not provenance_path.is_file() or provenance_path.is_symlink():
        return {"status": "refused_missing_or_symlinked_provenance"}
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    pid = provenance.get("server_pid")
    pgid = provenance.get("process_group_id")
    uid = provenance.get("server_uid", provenance.get("server_owner_uid"))
    command = provenance.get("server_command")
    expected_exe = provenance.get("verified_files", {}).get("llama-server", {}).get("path")
    if expected_exe is None and isinstance(command, list) and command:
        expected_exe = command[0]
    if (type(pid) is not int or pid <= 1 or type(pgid) is not int or pgid <= 1 or
            type(uid) is not int or uid < 0 or not isinstance(command, list) or
            not all(isinstance(arg, str) for arg in command) or not isinstance(expected_exe, str)):
        return {"status": "refused_incomplete_process_identity",
            "provenance_sha256": _sha(provenance_path)}
    remote_code = r'''import base64,json,os,signal,sys,time
pid,pgid,uid=int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3])
expected_exe=sys.argv[4]
expected_argv=json.loads(base64.urlsafe_b64decode(sys.argv[5].encode()).decode())
def inspect():
 p=f"/proc/{pid}"
 if not os.path.exists(p): return None,"absent"
 try:
  st=os.stat(p); exe=os.readlink(p+"/exe"); argv=[x.decode() for x in open(p+"/cmdline","rb").read().split(b"\0") if x]
  actual_pgid=os.getpgid(pid)
 except (ProcessLookupError,FileNotFoundError):
  if not os.path.exists(p): return None,"absent"
  return None,"inspection_race"
 return {"uid":st.st_uid,"exe":exe,"argv":argv,"pgid":actual_pgid},None
identity,state=inspect()
if state=="absent": print(json.dumps({"status":"already_absent","pid":pid})); sys.exit(0)
if state is not None: print(json.dumps({"status":"refused_process_inspection_race","pid":pid,"state":state})); sys.exit(24)
exe_ok=identity["exe"]==expected_exe or os.path.realpath(identity["exe"])==os.path.realpath(expected_exe)
if (identity["uid"]!=uid or not exe_ok or identity["argv"]!=expected_argv or identity["pgid"]!=pgid):
 print(json.dumps({"status":"refused_identity_mismatch","pid":pid,"observed":identity})); sys.exit(20)
os.kill(pid,signal.SIGTERM)
deadline=time.monotonic()+10
while time.monotonic()<deadline:
 ident,state=inspect()
 if state=="absent": print(json.dumps({"status":"terminated","signal":"SIGTERM","pid":pid,"verified_uid":uid,"verified_pgid":pgid})); sys.exit(0)
 if state is not None: print(json.dumps({"status":"refused_process_inspection_race_after_term","pid":pid,"state":state})); sys.exit(25)
 ok=(ident["uid"]==uid and (ident["exe"]==expected_exe or os.path.realpath(ident["exe"])==os.path.realpath(expected_exe)) and ident["argv"]==expected_argv and ident["pgid"]==pgid)
 if not ok: print(json.dumps({"status":"refused_identity_changed_after_term","pid":pid,"observed":ident})); sys.exit(21)
 time.sleep(.2)
ident,state=inspect()
if state=="absent": print(json.dumps({"status":"terminated","signal":"SIGTERM","pid":pid})); sys.exit(0)
if state is not None: print(json.dumps({"status":"refused_process_inspection_race_before_kill","pid":pid,"state":state})); sys.exit(26)
ok=(ident["uid"]==uid and (ident["exe"]==expected_exe or os.path.realpath(ident["exe"])==os.path.realpath(expected_exe)) and ident["argv"]==expected_argv and ident["pgid"]==pgid)
if not ok: print(json.dumps({"status":"refused_identity_changed_before_kill","pid":pid,"observed":ident})); sys.exit(22)
os.kill(pid,signal.SIGKILL)
deadline=time.monotonic()+5
while time.monotonic()<deadline:
 _,state=inspect()
 if state=="absent": print(json.dumps({"status":"terminated","signal":"SIGKILL","pid":pid,"verified_uid":uid,"verified_pgid":pgid})); sys.exit(0)
 time.sleep(.2)
print(json.dumps({"status":"kill_timeout","pid":pid})); sys.exit(23)
'''
    argv_blob = base64.urlsafe_b64encode(json.dumps(command, separators=(",", ":")).encode()).decode()
    remote_parts = ["/usr/bin/python3", "-c", remote_code, str(pid), str(pgid), str(uid), expected_exe, argv_blob]
    ssh = ["ssh", "-T", "-S", "none", "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", "ExitOnForwardFailure=yes",
        transport.REMOTE_HOST, shlex.join(remote_parts)]
    try:
        process = subprocess.run(ssh, capture_output=True, text=True, timeout=35, check=False)
        stdout = process.stdout.strip().splitlines()
        response = json.loads(stdout[-1]) if stdout else {"status": "missing_remote_identity_response"}
        return {**response, "ssh_exit_code": process.returncode,
            "ssh_stdout_sha256": hashlib.sha256(process.stdout.encode()).hexdigest(),
            "ssh_stderr_sha256": hashlib.sha256(process.stderr.encode()).hexdigest(),
            "provenance_path": str(provenance_path.relative_to(ROOT)),
            "provenance_sha256": _sha(provenance_path)}
    except Exception as exc:
        return {"status": "cleanup_command_failed", "error": f"{type(exc).__name__}: {str(exc)[:500]}",
            "provenance_path": str(provenance_path.relative_to(ROOT)),
            "provenance_sha256": _sha(provenance_path)}


def _report(run_root: Path, result: dict[str, Any], freeze: dict[str, Any],
            engineering_attempts: list[dict[str, Any]]) -> dict[str, Any]:
    md = _results_markdown(result, freeze, run_root=run_root,
        engineering_attempts=engineering_attempts)
    artifact_path = run_root / "paper_starter_bank10_results.md"
    # Keep the established run04 doc immutable; every later run gets a versioned
    # result page and exclusive creation protects a same-run retry from overwrite.
    docs_path = ROOT / "docs" / f"PAPER_STARTER_BANK_RESULTS_{freeze['run_id']}.md"
    artifact_sha = _exclusive_text(artifact_path, md)
    docs_sha = _exclusive_text(docs_path, md)
    return {"artifact_result_path": str(artifact_path.relative_to(ROOT)),
        "artifact_result_sha256": artifact_sha,
        "documentation_result_path": str(docs_path.relative_to(ROOT)),
        "documentation_result_sha256": docs_sha}


def monitor(run_root: Path, *, timeout_seconds: float = 40000,
            poll_seconds: float = 10,
            engineering_run_roots: list[Path] | None = None) -> dict[str, Any]:
    from scripts.run_paper_starter_audit import combine
    run_root = run_root.resolve(strict=True)
    if not run_root.is_relative_to(ROOT):
        raise FinalizeError("monitor run root must stay inside the project")
    status_path = run_root / "paper_starter_finalize_status.json"
    if status_path.exists() or status_path.is_symlink():
        raise FinalizeError(f"preserving existing finalizer status: {status_path}")
    started = time.monotonic()
    engineering_attempts = [_engineering_attempt(path) for path in (engineering_run_roots or [])]
    status: dict[str, Any] = {"schema_version": "e4-paper-starter-finalize-monitor/1",
        "status": "monitoring", "run_root": str(run_root.relative_to(ROOT)),
        "timeout_seconds": timeout_seconds, "poll_seconds": poll_seconds,
        "started_utc": datetime.now(timezone.utc).isoformat()}
    _replace_json(status_path, status)
    audit_dir: Path | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    terminal_failure: str | None = None
    try:
        while time.monotonic() - started < timeout_seconds:
            try:
                handoff = json.loads((run_root / "launch_handoff.json").read_text(encoding="utf-8"))
                audit_dir = (ROOT / handoff["audit_dir"]).resolve(strict=True)
                if not audit_dir.is_relative_to(run_root):
                    raise FinalizeError("launch handoff audit directory is outside this run root")
                local, wingtip, failure = _status(run_root, audit_dir)
                status.update({"audit_dir": str(audit_dir.relative_to(ROOT)),
                    "local_status": None if local is None else local.get("status"),
                    "wingtip_status": None if wingtip is None else wingtip.get("status"),
                    "last_checked_utc": datetime.now(timezone.utc).isoformat()})
                _replace_json(status_path, status)
                if failure:
                    terminal_failure = failure
                    break
                if _complete(local, wingtip):
                    combined = audit_dir / "combined_bank10_result.json"
                    if combined.exists():
                        raise FinalizeError("combined result already exists; refusing to overwrite")
                    result = combine(audit_dir,
                        audit_dir / "bank10_local" / "bank10_status.json",
                        audit_dir / "bank10_wingtip" / "bank10_status.json")
                    manifest = json.loads((audit_dir / "audit_manifest.json").read_text(encoding="utf-8"))
                    freeze_path = ROOT / manifest["freeze_manifest_path"]
                    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
                    status.update({"status": "complete", "combined_result_path": str(combined.relative_to(ROOT)),
                        "combined_result_sha256": _sha(combined),
                        "preserved_engineering_attempts": engineering_attempts,
                        "report_outputs": _report(run_root, result, freeze, engineering_attempts),
                        "completed_utc": datetime.now(timezone.utc).isoformat()})
                    _replace_json(status_path, status)
                    break
            except FileNotFoundError:
                status["last_checked_utc"] = datetime.now(timezone.utc).isoformat()
                _replace_json(status_path, status)
            time.sleep(poll_seconds)
        else:
            terminal_failure = f"monitor timeout after {timeout_seconds:.0f} seconds"
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:1000]}"
    finally:
        provenance_path = run_root / "wingtip_server_provenance.json"
        status["remote_server_cleanup"] = _cleanup_owned_server(provenance_path)
        status["finished_utc"] = datetime.now(timezone.utc).isoformat()
        if error:
            status["status"] = "failed_finalize"
            status["error"] = error
        elif terminal_failure:
            status["status"] = "incomplete"
            status["incomplete_reason"] = terminal_failure
        elif status.get("status") != "complete":
            status["status"] = "incomplete"
        _replace_json(status_path, status)
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True,
        help="fresh local model run root that owns this audit and Wingtip server")
    parser.add_argument("--timeout-seconds", type=float, default=40000)
    parser.add_argument("--poll-seconds", type=float, default=10)
    parser.add_argument("--engineering-run-root", type=Path, action="append", default=[],
        help="preserved incomplete engineering attempt to mention outside the ten-case denominator; repeatable")
    args = parser.parse_args()
    if args.timeout_seconds <= 0 or not 1 <= args.poll_seconds <= 60:
        parser.error("timeout must be positive and poll interval must be within 1..60 seconds")
    result = monitor(args.run_root, timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds, engineering_run_roots=args.engineering_run_root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("status") == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
