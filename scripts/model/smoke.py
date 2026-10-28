#!/usr/bin/env python3
"""Start llama.cpp and run the smoke suite inside a network-isolated bubblewrap namespace."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def healthy(url: str) -> bool:
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/health", timeout=3) as response:
            response.read()
        return True
    except (OSError, urllib.error.URLError):
        return False


def network_snapshot() -> dict:
    # socket.if_nameindex is scoped to the process's current network namespace;
    # /sys/class/net can reflect the host because bwrap reuses host sysfs.
    interfaces = sorted(name for _, name in socket.if_nameindex())
    routes = Path("/proc/net/route").read_text().splitlines()[1:]
    defaults = [line for line in routes if line.split()[1:2] == ["00000000"]]
    result = {"interfaces": interfaces, "routes": routes, "default_routes": defaults}
    if interfaces != ["lo"] or defaults:
        raise RuntimeError(f"unexpected interfaces or routes in offline namespace: {result}")
    return result


def run(args: argparse.Namespace) -> int:
    cfg = json.loads((ROOT / "config/models/smoke.json").read_text())
    manifest = json.loads((ROOT / "config/models/gemma_manifest.json").read_text())
    model = (ROOT / cfg["model_path"]).resolve()
    binary = (ROOT / args.server).resolve()
    if not model.is_file():
        raise SystemExit(f"model missing: {model}")
    if not binary.is_file():
        raise SystemExit(f"llama-server missing: {binary}")
    if not manifest.get("revision") or not manifest.get("sha256"):
        raise SystemExit("model manifest is not pinned; resolve and verify it before inference")
    bwrap = shutil.which("bwrap")
    if not bwrap:
        raise SystemExit("bubblewrap is required for network-isolated inference")

    port = args.port or int(cfg["base_url"].rsplit(":", 1)[1])
    base_url = f"http://127.0.0.1:{port}"
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir = ROOT / "artifacts/infrastructure/runs" / run_id
    log_path = run_dir / "llama-server.log"
    artifact = run_dir / "smoke.jsonl"
    run_dir.mkdir(parents=True, exist_ok=True)
    command = [str(binary), "--model", str(model), "--alias", cfg["model_alias"],
               "--host", "127.0.0.1", "--port", str(port),
               "--ctx-size", str(cfg["context_tokens"]), "--n-gpu-layers", "99", "--parallel", "1",
               "--offline", "--reasoning", "on", "--reasoning-format", "deepseek",
               "--reasoning-budget", str(cfg.get("reasoning_budget", 512))]
    if not args.inside:
        wrapper = [bwrap, "--ro-bind", "/", "/", "--dev-bind", "/dev", "/dev",
                   "--proc", "/proc", "--tmpfs", "/tmp", "--bind", str(ROOT), str(ROOT),
                   "--chdir", str(ROOT), "--unshare-net", "--setenv", "E4_BASE_URL", base_url,
                   "--setenv", "E4_BWRAPPED", "1", "--", sys.executable, str(Path(__file__).resolve()),
                   "--inside", "--server", args.server, "--port", str(port),
                   "--init-timeout", str(args.init_timeout), "--run-id", run_id]
        return subprocess.run(wrapper, cwd=ROOT).returncode

    if not os.environ.get("E4_BWRAPPED"):
        raise RuntimeError("internal launcher mode requires the bubblewrap namespace marker")

    net = network_snapshot()
    metadata = {"event": "server_start", "time_utc": datetime.now(timezone.utc).isoformat(),
                "llama_cpp_revision": subprocess.run(
                    ["git", "-C", str(ROOT / "third_party/llama.cpp"), "rev-parse", "HEAD"],
                    capture_output=True, text=True).stdout.strip(),
                "model_revision": manifest["revision"], "model_sha256": manifest["sha256"],
                "context_tokens": cfg["context_tokens"], "network_namespace": "unshared",
                "network_snapshot": net, "run_id": run_id,
                "artifact": str(artifact), "server_log": str(log_path),
                "server_command": command}
    (run_dir / "launch.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata))
    with log_path.open("w") as log:
        server = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                  start_new_session=True)
        try:
            deadline = time.monotonic() + args.init_timeout
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError(f"llama-server exited during startup ({server.returncode}); see {log_path}")
                if healthy(base_url):
                    print(f"llama-server ready at {base_url} after {args.init_timeout - max(0, deadline-time.monotonic()):.1f}s")
                    smoke_cmd = [sys.executable, "-m", "e4.launch.smoke", "--artifact", str(artifact)]
                    return subprocess.run(smoke_cmd, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT / "src"),
                                                                       "E4_BASE_URL": base_url}).returncode
                time.sleep(2)
            raise TimeoutError(f"llama-server did not become ready within {args.init_timeout}s; see {log_path}")
        finally:
            if server.poll() is None:
                os.killpg(server.pid, signal.SIGTERM)
                try:
                    server.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGKILL)
                    server.wait()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="third_party/llama.cpp/build-v100/bin/llama-server")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--init-timeout", type=int, default=1800)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--run-id", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        return run(args)
    except Exception as exc:
        print(f"local smoke failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
