#!/usr/bin/env python3
"""Start/stop the trusted local Gemma inference server only (no code execution)."""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from monitor import (EXPECTED_UUIDS, RESERVE_MIB, atomic_json, gitrev, gpu_rows, health,
                     proc_start_ticks, sha, terminate_owned)

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/runtime/local_inference/runs"
PROFILE = ROOT / "config/gemma_m3_inference.json"
MANIFEST = ROOT / "config/model_manifest.json"
SERVER = ROOT / "build-local-p100-v100/bin/llama-server"
MONITOR = ROOT / "artifacts/runtime/local_inference/monitor.py"
STARTER = Path(__file__).resolve()
EXPECTED_MODEL_SHA = "179cfb99212709597eae5929112cfca677e1bbf566178b479ae1da0c4772874b"
PINNED_LLAMA = "d81235049384534c167caea52b85a694f6103d14"

def start() -> int:
    profile = json.loads(PROFILE.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    model = (ROOT / profile["model_path"]).resolve(strict=True)
    server = SERVER.resolve(strict=True)
    if not os.access(server, os.X_OK):
        raise RuntimeError(f"server is not executable: {server}")
    if profile["context_tokens"] != 32768 or profile["max_generation_tokens"] != 8192:
        raise RuntimeError("profile context/generation differs from the authorized 32768/8192 values")
    if profile["reasoning_mode"] != "on" or profile["reasoning_budget"] != 2048:
        raise RuntimeError("profile reasoning settings differ from the authorized values")
    if profile["model_alias"] != "e4-gemma-m3" or model.name != manifest["filename"]:
        raise RuntimeError("profile alias/model filename differs from pinned configuration")
    # Hash before touching the GPUs; do not load a checkpoint with an unexpected digest.
    model_sha = sha(model)
    if model_sha != EXPECTED_MODEL_SHA or model_sha != manifest["sha256"]:
        raise RuntimeError(f"checkpoint SHA mismatch: {model_sha}")
    baseline = gpu_rows()
    for sample in baseline:
        if int(sample["free_mib"]) < RESERVE_MIB:
            raise RuntimeError(f"GPU reserve preflight failed for {sample['uuid']}: {sample['free_mib']} MiB")

    port = 19250
    url = f"http://127.0.0.1:{port}"
    if health(url) is not None:
        raise RuntimeError(f"refusing to reuse existing service on {url}")
    OUT.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:10]
    run_dir = OUT / run_id
    run_dir.mkdir(exist_ok=False)
    log_path = run_dir / "server.log"
    command = [str(server), "--model", str(model), "--alias", profile["model_alias"],
               "--host", "127.0.0.1", "--port", str(port), "--ctx-size", str(profile["context_tokens"]),
               "--n-gpu-layers", "99", "--parallel", "1", "--offline",
               "--reasoning", "on", "--reasoning-budget", "2048",
               "--device", "CUDA0,CUDA1", "--split-mode", "layer", "--tensor-split", "0.5,0.5",
               "--verbosity", "4"]
    device_result = subprocess.run([str(server), "--list-devices"], cwd=ROOT,
                                   capture_output=True, text=True, timeout=20)
    if device_result.returncode != 0 or "CUDA0:" not in device_result.stdout or "CUDA1:" not in device_result.stdout:
        raise RuntimeError("server device preflight did not report both CUDA0 and CUDA1")
    revision = gitrev(ROOT / "third_party/llama.cpp")
    if revision != PINNED_LLAMA:
        raise RuntimeError(f"llama.cpp revision mismatch: {revision}")
    receipt: dict[str, object] = {
        "schema": "e4-local-inference-service/1", "run_id": run_id, "state": "starting",
        "created_utc": datetime.now(timezone.utc).isoformat(), "run_directory": str(run_dir),
        "base_url": url, "profile_path": str(PROFILE), "profile_sha256": sha(PROFILE),
        "manifest_path": str(MANIFEST), "manifest_sha256": sha(MANIFEST),
        "model_path": str(model), "model_sha256": model_sha, "model_size_bytes": model.stat().st_size,
        "model_alias": profile["model_alias"], "context_tokens": 32768,
        "max_generation_tokens_client_limit": 8192, "reasoning_mode": "on", "reasoning_budget": 2048,
        "server_path": str(server), "server_sha256": sha(server),
        "starter_path": str(STARTER), "starter_sha256": sha(STARTER),
        "gpu_monitor_path": str(MONITOR), "gpu_monitor_sha256": sha(MONITOR),
        "server_log_verbosity": 4,
        "llama_cpp_revision": revision, "repository_revision": gitrev(ROOT),
        "gpu_baseline": baseline, "minimum_free_vram_mib": RESERVE_MIB,
        "device_selection": ["CUDA0", "CUDA1"], "split_mode": "layer", "tensor_split": "0.5,0.5",
        "server_device_preflight": {"exit_code": device_result.returncode,
                                    "stdout": device_result.stdout, "stderr": device_result.stderr},
        "server_command": command, "server_log": str(log_path),
        "security_scope": "trusted model inference service only; no operator/evaluator execution",
    }
    atomic_json(run_dir / "receipt.json", receipt)
    env = dict(os.environ)
    env.pop("CUDA_VISIBLE_DEVICES", None)
    # Keep the newly built server's own libraries ahead of system CUDA libraries.
    env["LD_LIBRARY_PATH"] = str(server.parent) + ":/usr/local/cuda-12.9/lib64"
    with log_path.open("xb") as log:
        proc = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        receipt["server_pid"] = proc.pid
        receipt["server_uid"] = os.getuid()
        receipt["server_proc_start_ticks"] = proc_start_ticks(proc.pid)
        atomic_json(run_dir / "receipt.json", receipt)
        deadline = time.monotonic() + int(profile["model_init_timeout_seconds"])
        samples: list[dict[str, object]] = []
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                receipt.update(state="failed", exit_code=proc.returncode,
                               failure="server exited before health check")
                receipt["gpu_samples_during_startup"] = samples
                atomic_json(run_dir / "receipt.json", receipt)
                print(f"server exited ({proc.returncode}); evidence preserved in {run_dir}", file=sys.stderr)
                return 1
            try:
                rows = gpu_rows()
            except Exception as exc:
                receipt.update(state="failed", failure=f"GPU sampling failed: {exc}",
                               gpu_samples_during_startup=samples)
                atomic_json(run_dir / "receipt.json", receipt)
                terminate_owned(proc)
                raise
            samples.append({"time_utc": datetime.now(timezone.utc).isoformat(), "gpus": rows})
            low = [r for r in rows if int(r["free_mib"]) < RESERVE_MIB]
            if low:
                receipt.update(state="failed", failure="minimum VRAM reserve breached during startup",
                               gpu_samples_during_startup=samples)
                atomic_json(run_dir / "receipt.json", receipt)
                terminate_owned(proc)
                print(f"VRAM reserve breached; evidence preserved in {run_dir}", file=sys.stderr)
                return 1
            checked = health(url)
            if checked:
                time.sleep(1)
                rows = gpu_rows()
                samples.append({"time_utc": datetime.now(timezone.utc).isoformat(), "gpus": rows})
                if any(int(r["free_mib"]) < RESERVE_MIB for r in rows):
                    receipt.update(state="failed", failure="minimum VRAM reserve breached after health",
                                   health=checked, gpu_samples_during_startup=samples)
                    atomic_json(run_dir / "receipt.json", receipt)
                    terminate_owned(proc)
                    return 1
                log_text = log_path.read_text(encoding="utf-8", errors="replace")
                proof = [line[-1000:] for line in log_text.splitlines()
                         if re.search(r"offload|model loaded|model buffer|loaded .*layer", line, re.I)]
                both_devices = any("CUDA0" in line for line in proof) and any("CUDA1" in line for line in proof)
                if not both_devices:
                    receipt.update(state="failed", failure="health succeeded but dual-GPU loaded-weight proof absent",
                                   health=checked, gpu_samples_during_startup=samples,
                                   loaded_weights_log_evidence=proof[-80:])
                    atomic_json(run_dir / "receipt.json", receipt)
                    terminate_owned(proc)
                    print(f"dual-GPU load proof absent; evidence preserved in {run_dir}", file=sys.stderr)
                    return 1
                receipt.update(state="ready", ready_utc=datetime.now(timezone.utc).isoformat(),
                               health=checked, gpu_samples_during_startup=samples,
                               loaded_weights_log_evidence=proof[-80:])
                atomic_json(run_dir / "receipt.json", receipt)
                monitor = subprocess.Popen([sys.executable, str(MONITOR), str(run_dir)],
                                           cwd=ROOT, stdin=subprocess.DEVNULL,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                           start_new_session=True)
                receipt.update(gpu_monitor_pid=monitor.pid,
                               gpu_monitor_proc_start_ticks=proc_start_ticks(monitor.pid))
                atomic_json(run_dir / "receipt.json", receipt)
                monitor_log = run_dir / "gpu-monitor.jsonl"
                monitor_deadline = time.monotonic() + 10
                while time.monotonic() < monitor_deadline and monitor.poll() is None:
                    if monitor_log.is_file() and '"event": "monitor-ready"' in monitor_log.read_text():
                        break
                    time.sleep(0.1)
                else:
                    receipt.update(state="failed", failure="GPU monitor did not start")
                    atomic_json(run_dir / "receipt.json", receipt)
                    terminate_owned(proc)
                    return 1
                receipt["gpu_monitor_ready_utc"] = datetime.now(timezone.utc).isoformat()
                atomic_json(run_dir / "receipt.json", receipt)
                print(json.dumps({"run_id": run_id, "run_directory": str(run_dir),
                                  "base_url": url, "server_pid": proc.pid,
                                  "gpu_monitor_pid": monitor.pid, "state": "ready"}))
                return 0
            time.sleep(1)
        receipt.update(state="failed", failure="health check timeout", gpu_samples_during_startup=samples)
        atomic_json(run_dir / "receipt.json", receipt)
        terminate_owned(proc)
        print(f"health timeout; evidence preserved in {run_dir}", file=sys.stderr)
        return 1
    return 1

def stop(run_dir: Path) -> int:
    receipt_path = (run_dir / "receipt.json").resolve(strict=True)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    pid = int(receipt["server_pid"])
    if receipt.get("server_uid") != os.getuid() or proc_start_ticks(pid) != receipt["server_proc_start_ticks"]:
        raise RuntimeError("PID ownership/start-time check failed; refusing cleanup")
    cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    if not cmdline or Path(os.fsdecode(cmdline[0])).resolve() != Path(receipt["server_path"]).resolve():
        raise RuntimeError("PID command does not match the recorded server; refusing cleanup")
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and Path(f"/proc/{pid}").exists():
        if proc_start_ticks(pid) != receipt["server_proc_start_ticks"]:
            break
        time.sleep(0.2)
    if Path(f"/proc/{pid}").exists() and proc_start_ticks(pid) == receipt["server_proc_start_ticks"]:
        os.kill(pid, signal.SIGKILL)
    receipt.update(state="stopped", stopped_utc=datetime.now(timezone.utc).isoformat())
    atomic_json(receipt_path, receipt)
    print(f"stopped owned server pid {pid}")
    return 0
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("start", help="start the pinned local Gemma service")
    stop_parser = commands.add_parser("stop", help="stop only a server owned by a receipt")
    stop_parser.add_argument("run_directory", type=Path)
    args = parser.parse_args()
    try:
        return start() if args.action == "start" else stop(args.run_directory)
    except Exception as exc:
        print(f"local inference starter: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
