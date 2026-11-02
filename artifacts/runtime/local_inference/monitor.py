#!/usr/bin/env python3
"""Fail-closed VRAM supervisor for one receipt-owned inference server."""
from __future__ import annotations

import json
import hashlib
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_UUIDS = {"GPU-6311f251-73be-4bd2-971d-63d6c7d0e947",
                  "GPU-e43dc7e5-2eeb-ec4d-14bb-c0f1bec4efd2"}
RESERVE_MIB = 3072
MAX_SAMPLES = 86400


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def gitrev(path: Path) -> str:
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


def gpu_rows() -> list[dict[str, object]]:
    fields = "index,name,uuid,pci.bus_id,memory.total,memory.used,memory.free,compute_cap,driver_version"
    result = subprocess.run(["nvidia-smi", f"--query-gpu={fields}", "--format=csv,noheader,nounits"],
                           capture_output=True, text=True, check=True, timeout=10)
    rows = []
    for line in result.stdout.splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) != 9:
            raise RuntimeError(f"unexpected nvidia-smi row: {line}")
        rows.append({"index": int(p[0]), "name": p[1], "uuid": p[2], "pci_bus_id": p[3],
                     "total_mib": int(p[4]), "used_mib": int(p[5]), "free_mib": int(p[6]),
                     "compute_capability": p[7], "driver_version": p[8]})
    if {str(r["uuid"]) for r in rows} != EXPECTED_UUIDS:
        raise RuntimeError("GPU UUID set differs from the approved local P100/V100 pair")
    return rows


def atomic_json(path: Path, value: dict[str, object]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def health(url: str) -> dict[str, object] | None:
    try:
        with urllib.request.urlopen(url + "/health", timeout=3) as response:
            body = response.read(64 * 1024).decode("utf-8", "replace")
            return {"http_status": response.status, "body": json.loads(body)}
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None


def proc_start_ticks(pid: int) -> str:
    raw = Path(f"/proc/{pid}/stat").read_text()
    return raw[raw.rfind(")") + 2:].split()[19]


def terminate_owned(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def server_identity(receipt: dict[str, object]) -> tuple[bool, str]:
    pid = int(receipt["server_pid"])
    proc = Path(f"/proc/{pid}")
    if not proc.exists():
        return False, "server process exited"
    try:
        uid = int(next(line for line in (proc / "status").read_text().splitlines()
                       if line.startswith("Uid:")).split()[1])
        stat = (proc / "stat").read_text()
        ticks = stat[stat.rfind(")") + 2:].split()[19]
        command = (proc / "cmdline").read_bytes().split(b"\0")
        executable = Path(os.fsdecode(command[0])).resolve()
    except (OSError, IndexError, StopIteration):
        return False, "server identity could not be read"
    if (uid != receipt["server_uid"] or ticks != receipt["server_proc_start_ticks"] or
            executable != Path(str(receipt["server_path"])).resolve()):
        return False, "server PID identity changed; no signal sent"
    return True, "owned server"


def check_sample(rows: list[dict[str, object]] | None, error: str | None = None) -> str | None:
    if error:
        return f"GPU sampler error: {error}"
    if rows is None or {str(row["uuid"]) for row in rows} != EXPECTED_UUIDS:
        return "GPU sampler did not return the pinned P100/V100 UUID pair"
    low = [f"{row['uuid']}={row['free_mib']} MiB" for row in rows
           if int(row["free_mib"]) < RESERVE_MIB]
    return f"VRAM reserve below {RESERVE_MIB} MiB: {', '.join(low)}" if low else None


def append_event(path: Path, event: dict[str, object]) -> None:
    event["time_utc"] = datetime.now(timezone.utc).isoformat()
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def fail_server(receipt: dict[str, object], reason: str) -> dict[str, object]:
    owned, detail = server_identity(receipt)
    failure = {"event": "fatal", "state": "fatal", "reason": reason, "identity_check": detail}
    if owned:
        pid = int(receipt["server_pid"])
        try:
            os.kill(pid, signal.SIGTERM)
            failure["server_signal"] = "SIGTERM"
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and server_identity(receipt)[0]:
                time.sleep(0.1)
            if server_identity(receipt)[0]:
                os.kill(pid, signal.SIGKILL)
                failure["server_signal"] = "SIGTERM,SIGKILL"
        except ProcessLookupError:
            failure["server_signal"] = "SIGTERM (already exited)"
    else:
        failure["server_signal"] = "none"
    return failure


def watch(run_dir: Path) -> int:
    receipt = json.loads((run_dir / "receipt.json").read_text(encoding="utf-8"))
    log = run_dir / "gpu-monitor.jsonl"
    append_event(log, {"event": "monitor-start", "state": "watching",
                       "server_pid": receipt["server_pid"], "reserve_mib": RESERVE_MIB})
    for sample_number in range(MAX_SAMPLES):
        owned, detail = server_identity(receipt)
        if not owned:
            # Ordinary server shutdown is expected; an identity mismatch is recorded and never signaled.
            append_event(log, {"event": "server-exit", "detail": detail,
                               "fatal": detail != "server process exited"})
            return 0
        try:
            rows = gpu_rows()
            error = None
        except Exception as exc:
            rows, error = None, f"{type(exc).__name__}: {exc}"
        reason = check_sample(rows, error)
        append_event(log, {"event": "sample", "sample": sample_number, "gpus": rows,
                           "error": error, "reserve_failure": reason})
        if reason:
            append_event(log, fail_server(receipt, reason))
            return 1
        if sample_number == 0:
            append_event(log, {"event": "monitor-ready", "state": "watching"})
        time.sleep(1)
    append_event(log, fail_server(receipt, "monitor sample limit reached"))
    return 1


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: monitor.py RUN_DIRECTORY", file=sys.stderr)
        return 2
    try:
        return watch(Path(sys.argv[1]).resolve(strict=True))
    except Exception as exc:
        print(f"local GPU monitor: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
