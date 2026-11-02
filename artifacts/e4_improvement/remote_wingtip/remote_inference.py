#!/usr/bin/env python3
"""Use a remote pinned llama.cpp endpoint through SSH Unix-socket forwarding.

The host starts one SSH local forward to remote loopback. Bubblewrap exposes only
that socket directory to a private PID/network namespace. A small TCP-to-Unix relay
inside the namespace serves the unchanged E4 client at 127.0.0.1:8080.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import http.client
import http.server
import json
import os
import resource
import select
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
V7_ROOT = ROOT / "artifacts/e4_improvement/typed_v7"
SOURCE_INVENTORY = V7_ROOT / "verification/final_source_inventory.json"
RELEASE_MANIFEST = ROOT / "artifacts/e4_improvement/releases/v7_typed_20261019/source_manifest.json"
REVALIDATION_REPORT = V7_ROOT / "revalidated_r2/revalidation_result.json"
INITIAL_LIBRARY = V7_ROOT / "revalidated_r2/library"
PROFILE = ROOT / "config/gemma_m3_inference.json"
POLICY = ROOT / "config/m4_runtime.json"
MODEL_MANIFEST = ROOT / "config/model_manifest.json"
EXPECTED_FINGERPRINT = "d519f1dd547e8fac85499131f10ee6e1205725cfcaadafb98816379025e47775"
EXPECTED_SOURCE_COUNT = 141
EXPECTED_PROFILE_SHA256 = "34ca3c7209ff366ebaa1595a52d3dc2c8bccce89e7ef8e26ef5f732f043e5955"
EXPECTED_POLICY_SHA256 = "51788743fe1db7539c3a5cf2375f3f7bdb7b5c479053e5b99dc28176a5ba2849"
EXPECTED_INITIAL_SNAPSHOT = "0e0413efb893ee26ffa1555a50bb02ef901f478981571e60745ea92865eeda22"
# Updated from the typed-v7 queue/release artifacts; paths remain entirely local.
EXPECTED_RELEASE_MANIFEST_SHA256 = "5ef0687c86dea949a9ca688e9bb6925e1cb33a698c3f3b14e3ec2a27ace8b416"
EXPECTED_REVALIDATION_SHA256 = "a0c79934b9ab130e4f79d17ed7878ccf6c72662be6ad5ee04660482061e7f0c4"
EXPECTED_REMOTE_LLAMA_CPP_REVISION = "d81235049384534c167caea52b85a694f6103d14"
REMOTE_HOST = "ssinha97@wingtip-gpu2.cc.gatech.edu"
REMOTE_HARDWARE = "NVIDIA Quadro RTX 8000 (Turing SM75)"
LOCAL_COMPARISON_HARDWARE = "NVIDIA V100 (Volta SM70)"
LOCAL_API_PORT = 8080
DEFAULT_REMOTE_PORT = 18081
REMOTE_SOCKET_IN_NAMESPACE = "/run/e4remote/forward.sock"
RUNS = (
    {"run_id": "e4w7dev1r3", "case_id": "bank-20210323T0630-utc8",
     "library_root": INITIAL_LIBRARY},
    {"run_id": "e4w7dev2r3", "case_id": "bank-20210325T1530-utc8",
     "library_root": None},
)
FAILED_FIRST_ATTEMPT_STATUS = V7_ROOT / "remote_wingtip_development_pair_status.json"
FAILED_FIRST_ATTEMPT_DIR = ROOT / "artifacts/m3/model_runs/e4w7dev1"
FAILED_R2_STATUS = V7_ROOT / "remote_wingtip_development_pair_r2_status.json"
FAILED_R2_FIRST_DIR = ROOT / "artifacts/m3/model_runs/e4w7dev1r2"
FAILED_R2_SECOND_DIR = ROOT / "artifacts/m3/model_runs/e4w7dev2r2"
PAIR_ATTEMPT = "engineering_retry_r3"
NEUTRAL_SMOKE_RUN_ID = "e4w7native_smoke"
TOKENIZER_RUN_ID = "e4w7tok"
TOKENIZER_PAYLOAD_DIR = V7_ROOT / "preflight/payloads"
TOKENIZER_HELPER = V7_ROOT / "preflight/tokenize_only.py"


class RemoteLauncherError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_project_path(path: Path) -> Path:
    raw = Path(path).absolute()
    try:
        rel = raw.relative_to(ROOT)
    except ValueError as exc:
        raise RemoteLauncherError(f"path is outside the frozen project: {path}") from exc
    current = ROOT
    for part in rel.parts:
        current = current / part
        if current.is_symlink():
            raise RemoteLauncherError(f"project path cannot traverse symlinks: {current}")
    return raw.resolve(strict=True)


def verify_local_pins() -> dict[str, Any]:
    from e4core.cache import implementation_fingerprint
    from e4core.m4_library import load_library_snapshot

    inventory_path = safe_project_path(SOURCE_INVENTORY)
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    files = inventory.get("source_files")
    if (not isinstance(files, dict) or len(files) != EXPECTED_SOURCE_COUNT or
            inventory.get("fingerprint") != EXPECTED_FINGERPRINT):
        raise RemoteLauncherError("typed-v7 final 141-file inventory or fingerprint is mismatched")
    mismatches = []
    for relative, expected in sorted(files.items()):
        path = safe_project_path(ROOT / relative)
        if not path.is_file() or sha256_file(path) != expected:
            mismatches.append(relative)
    if mismatches:
        raise RemoteLauncherError(f"frozen V7 source files changed: {mismatches[:10]}")
    fingerprint = implementation_fingerprint()
    if fingerprint != EXPECTED_FINGERPRINT:
        raise RemoteLauncherError(f"current implementation fingerprint differs: {fingerprint}")

    profile_path = safe_project_path(PROFILE)
    policy_path = safe_project_path(POLICY)
    model_manifest_path = safe_project_path(MODEL_MANIFEST)
    if sha256_file(profile_path) != EXPECTED_PROFILE_SHA256:
        raise RemoteLauncherError("original frozen inference profile changed")
    if sha256_file(policy_path) != EXPECTED_POLICY_SHA256:
        raise RemoteLauncherError("frozen M4 runtime policy changed")
    model_manifest = json.loads(model_manifest_path.read_text(encoding="utf-8"))
    if (model_manifest.get("sha256") != "179cfb99212709597eae5929112cfca677e1bbf566178b479ae1da0c4772874b" or
            model_manifest.get("revision") != "59dde24573e7e61570dba08b18a2e1fe246955ed"):
        raise RemoteLauncherError("original Gemma model manifest differs from frozen checkpoint pin")

    release_path = safe_project_path(RELEASE_MANIFEST)
    if sha256_file(release_path) != EXPECTED_RELEASE_MANIFEST_SHA256:
        raise RemoteLauncherError("typed-v7 release manifest hash changed")
    release = json.loads(release_path.read_text(encoding="utf-8"))
    if (release.get("implementation_fingerprint") != EXPECTED_FINGERPRINT or
            release.get("source_files") != files or
            release.get("model_profile_sha256") != EXPECTED_PROFILE_SHA256 or
            release.get("runtime_policy_sha256") != EXPECTED_POLICY_SHA256):
        raise RemoteLauncherError("typed-v7 release manifest source/profile/policy pins do not match")

    revalidation_path = safe_project_path(REVALIDATION_REPORT)
    if sha256_file(revalidation_path) != EXPECTED_REVALIDATION_SHA256:
        raise RemoteLauncherError("typed-v7 initial-library revalidation report hash changed")
    revalidation = json.loads(revalidation_path.read_text(encoding="utf-8"))
    if (revalidation.get("status") != "revalidated_generated_bootstrap" or
            revalidation.get("new_snapshot_id") != EXPECTED_INITIAL_SNAPSHOT or
            revalidation.get("fixture_count") != 25 or
            revalidation.get("operator_content_unchanged") is not True or
            revalidation.get("playbook_content_unchanged") is not True or
            revalidation.get("source_inventory_unchanged") is not True or
            revalidation.get("bootstrap_artifact_inventory_unchanged") is not True or
            revalidation.get("model_calls") != 0 or revalidation.get("gold_accessed") is not False):
        raise RemoteLauncherError("typed-v7 library revalidation readiness report is invalid")
    library = load_library_snapshot(safe_project_path(INITIAL_LIBRARY))
    if library.snapshot_id != EXPECTED_INITIAL_SNAPSHOT:
        raise RemoteLauncherError("typed-v7 initial library CURRENT differs from its pinned snapshot")
    return {"fingerprint": fingerprint, "source_file_count": len(files),
        "source_inventory_sha256": sha256_file(inventory_path),
        "release_manifest_sha256": EXPECTED_RELEASE_MANIFEST_SHA256,
        "revalidation_report_sha256": EXPECTED_REVALIDATION_SHA256,
        "profile_sha256": EXPECTED_PROFILE_SHA256, "policy_sha256": EXPECTED_POLICY_SHA256,
        "model_manifest_sha256": sha256_file(model_manifest_path),
        "model_checkpoint_sha256": model_manifest["sha256"],
        "initial_library_snapshot_id": library.snapshot_id}


def verify_remote_attestation(path: Path, *, remote_port: int) -> dict[str, Any]:
    manifest = json.loads(safe_project_path(MODEL_MANIFEST).read_text(encoding="utf-8"))
    provenance_path = safe_project_path(path)
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    if provenance.get("verified") is not True:
        raise RemoteLauncherError("remote provenance must explicitly confirm parent verification")
    checkpoint_sha256 = provenance.get("model_sha256")
    if checkpoint_sha256 != manifest.get("sha256"):
        raise RemoteLauncherError("remote checkpoint attestation does not match the frozen Gemma SHA-256")
    llama_revision = provenance.get("llama_cpp_revision")
    if llama_revision != EXPECTED_REMOTE_LLAMA_CPP_REVISION:
        raise RemoteLauncherError("remote llama.cpp revision differs from the pinned llama.cpp commit")
    server_sha256 = provenance.get("llama_server_sha256")
    if len(server_sha256) != 64 or any(char not in "0123456789abcdef" for char in server_sha256):
        raise RemoteLauncherError("remote llama-server binary SHA-256 attestation must be lowercase hex")
    bind_host = provenance.get("bind_host")
    bind_port = provenance.get("bind_port")
    if bind_host != "127.0.0.1" or bind_port != remote_port:
        raise RemoteLauncherError("remote model server must be attested as loopback-only on the selected port")
    command = provenance.get("server_command")
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        raise RemoteLauncherError("remote provenance must contain the exact server command argument list")
    required_args = {"--host": "127.0.0.1", "--port": str(remote_port), "--alias": "e4-gemma-m3",
        "--ctx-size": "32768", "--n-gpu-layers": "99", "--parallel": "1",
        "--reasoning": "on", "--reasoning-budget": "2048"}
    for flag, expected in required_args.items():
        if flag not in command or command[command.index(flag) + 1] != expected:
            raise RemoteLauncherError(f"remote server command differs from frozen setting {flag}={expected}")
    if "--offline" not in command:
        raise RemoteLauncherError("remote llama-server must retain offline mode")
    if not Path(command[0]).is_absolute():
        raise RemoteLauncherError("remote llama-server command must name an absolute binary path")
    model_arg = command[command.index("--model") + 1] if "--model" in command else ""
    if not Path(model_arg).is_absolute():
        raise RemoteLauncherError("remote llama-server command must name an absolute GGUF path")
    hardware = provenance.get("hardware")
    if (not isinstance(hardware, dict) or "Quadro RTX 8000" not in str(hardware.get("name", "")) or
            not hardware.get("compute_capability") or
            type(hardware.get("total_mib")) is not int or hardware["total_mib"] < 46000 or
            not hardware.get("driver_version")):
        raise RemoteLauncherError("remote GPU attestation must identify the 48 GiB Quadro RTX 8000 and driver")
    return {"remote_provenance_path": str(provenance_path),
        "remote_provenance_sha256": sha256_file(provenance_path),
        "remote_checkpoint_sha256": checkpoint_sha256,
        "remote_llama_cpp_revision": llama_revision,
        "remote_llama_server_sha256": server_sha256,
        "remote_server_command": command,
        "remote_hardware": hardware,
        "remote_binary_build_architecture": provenance.get("build_architecture"),
        "remote_build_flags": provenance.get("build_flags"),
        "remote_cuda_libraries": provenance.get("libraries"),
        "remote_bind_host": bind_host, "remote_bind_port": bind_port,
        "attestation_source": "parent-verified remote build artifact; launcher executes no remote commands"}


class RelayMetrics:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.connections: list[dict[str, Any]] = []

    def add(self, row: dict[str, Any]) -> None:
        with self.lock:
            self.connections.append(row)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            rows = [dict(row) for row in self.connections]
        return {"connection_count": len(rows),
            "client_bytes_total": sum(row.get("client_bytes", 0) for row in rows),
            "remote_bytes_total": sum(row.get("remote_bytes", 0) for row in rows),
            "relay_wall_seconds_total": sum(row.get("wall_seconds", 0.0) for row in rows),
            "relay_thread_cpu_seconds_total": sum(row.get("thread_cpu_seconds", 0.0) for row in rows),
            "connections": rows}


class TCPToUnixRelay:
    """Forward loopback TCP byte streams to one mounted SSH Unix socket."""

    def __init__(self, unix_socket: Path, *, host: str = "127.0.0.1", port: int = LOCAL_API_PORT):
        self.unix_socket = Path(unix_socket)
        self.metrics = RelayMetrics()
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                started = time.monotonic()
                thread_cpu_start = time.thread_time()
                row: dict[str, Any] = {"request_line": None, "client_bytes": 0, "remote_bytes": 0}
                remote = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    remote.settimeout(10)
                    remote.connect(str(owner.unix_socket))
                    remote.settimeout(None)
                    sockets = (self.request, remote)
                    first_client_bytes = bytearray()
                    half_closed = set()
                    while len(half_closed) < 2:
                        readable, _, _ = select.select([sock for sock in sockets if sock not in half_closed], [], [], 30)
                        if not readable:
                            continue
                        for source in readable:
                            data = source.recv(65536)
                            if not data:
                                half_closed.add(source)
                                peer = remote if source is self.request else self.request
                                try:
                                    peer.shutdown(socket.SHUT_WR)
                                except OSError:
                                    pass
                                continue
                            destination = remote if source is self.request else self.request
                            destination.sendall(data)
                            if source is self.request:
                                row["client_bytes"] += len(data)
                                if len(first_client_bytes) < 512:
                                    first_client_bytes.extend(data[:512 - len(first_client_bytes)])
                                    if b"\r\n" in first_client_bytes:
                                        row["request_line"] = first_client_bytes.split(b"\r\n", 1)[0].decode("ascii", "replace")
                            else:
                                row["remote_bytes"] += len(data)
                except (OSError, TimeoutError) as exc:
                    row["relay_error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
                finally:
                    remote.close()
                    row["wall_seconds"] = time.monotonic() - started
                    row["thread_cpu_seconds"] = time.thread_time() - thread_cpu_start
                    owner.metrics.add(row)

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = False
            daemon_threads = False
            block_on_close = True

        self.server = Server((host, port), Handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever,
            kwargs={"poll_interval": 0.1}, name="remote-unix-http-relay", daemon=True)

    @property
    def address(self) -> tuple[str, int]:
        return self.server.server_address

    def start(self) -> None:
        self.server_thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)


def local_health_probe(base_url: str = f"http://127.0.0.1:{LOCAL_API_PORT}", *, timeout: float = 4.0) -> dict[str, Any]:
    started = time.perf_counter()
    with urllib.request.urlopen(base_url + "/health", timeout=timeout) as response:
        body = response.read(4096)
        status = response.status
    if status != 200:
        raise RemoteLauncherError(f"remote model health endpoint returned HTTP {status}")
    return {"endpoint": "/health", "http_status": status,
        "response_bytes": len(body), "round_trip_seconds": time.perf_counter() - started}


def build_bwrap_command(*, run_dir: Path, socket_dir: Path, command: list[str]) -> list[str]:
    bwrap = shutil.which("bwrap")
    if not bwrap:
        raise RemoteLauncherError("bubblewrap is required for the remote-inference controller namespace")
    run_dir = safe_project_path(run_dir)
    socket_dir = Path(socket_dir).resolve(strict=True)
    socket_path = socket_dir / "forward.sock"
    if len(os.fsencode(str(socket_path))) >= 100:
        raise RemoteLauncherError("SSH forwarding Unix socket path must be shorter than 100 bytes")
    args = [bwrap, "--unshare-net", "--unshare-pid", "--die-with-parent", "--clearenv",
        "--setenv", "PATH", f"{ROOT}/.venv/bin:/usr/bin:/bin",
        "--setenv", "HOME", "/tmp", "--setenv", "LANG", "C.UTF-8",
        "--setenv", "LC_ALL", "C.UTF-8", "--setenv", "PYTHONNOUSERSITE", "1",
        "--setenv", "OMP_NUM_THREADS", "1", "--setenv", "OPENBLAS_NUM_THREADS", "1",
        "--setenv", "MKL_NUM_THREADS", "1", "--setenv", "PYTHONDONTWRITEBYTECODE", "1",
        "--ro-bind", "/usr", "/usr", "--ro-bind", "/lib", "/lib"]
    if Path("/lib64").exists():
        args += ["--ro-bind", "/lib64", "/lib64"]
    if Path("/etc/ld.so.cache").exists():
        args += ["--dir", "/etc", "--ro-bind", "/etc/ld.so.cache", "/etc/ld.so.cache"]
    if Path("/sys").exists():
        args += ["--ro-bind", "/sys", "/sys"]
    # Mount the one SSH socket directory outside /tmp first, then hide the host /tmp.
    args += ["--dev", "/dev", "--proc", "/proc", "--tmpfs", "/run",
        "--dir", "/run/e4remote", "--ro-bind", str(socket_dir), "/run/e4remote",
        "--dir", "/run/e4remote-source", "--ro-bind", str(socket_dir), "/run/e4remote-source",
        "--tmpfs", "/tmp", "--symlink", "/usr/bin", "/bin",
        "--symlink", "/usr/sbin", "/sbin",
        "--ro-bind", str(ROOT), str(ROOT), "--bind", str(run_dir), str(run_dir),
        "--chdir", str(ROOT), "--setenv", "E4_BWRAPPED", "1",
        "--setenv", "E4_BASE_URL", f"http://127.0.0.1:{LOCAL_API_PORT}",
        "--setenv", "E4_RUN_DIR", str(run_dir),
        "--setenv", "E4_MODEL_PROFILE", str(PROFILE),
        "--setenv", "E4_REMOTE_TRANSPORT", "ssh-unix-forward-via-loopback-relay",
        "--", *command]
    return args


def namespace_child(*, socket_path: Path, run_dir: Path, command: list[str]) -> dict[str, Any]:
    if not socket_path.is_socket():
        raise RemoteLauncherError(f"mounted SSH forward socket is missing: {socket_path}")
    relay = TCPToUnixRelay(socket_path)
    child_cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    own_cpu_start = time.process_time()
    relay.start()
    try:
        health = None
        deadline = time.monotonic() + 60
        health_error = None
        while time.monotonic() < deadline:
            try:
                health = local_health_probe()
                break
            except Exception as exc:
                health_error = f"{type(exc).__name__}: {str(exc)[:300]}"
                time.sleep(0.5)
        if health is None:
            raise RemoteLauncherError(f"remote model health check failed through SSH relay: {health_error}")
        started_utc = datetime.now(timezone.utc).isoformat()
        started = time.monotonic()
        result = subprocess.run(command, cwd=ROOT, env={
            "PATH": f"{ROOT}/.venv/bin:/usr/bin:/bin", "HOME": "/tmp",
            "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1", "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
            "E4_BASE_URL": f"http://127.0.0.1:{LOCAL_API_PORT}",
            "E4_RUN_DIR": str(run_dir), "E4_MODEL_PROFILE": str(PROFILE)}, check=False)
        wall = time.monotonic() - started
        child_cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        record = {"command_exit_code": result.returncode, "health_probe": health,
            "controller_wall_seconds": wall,
            "controller_child_cpu_seconds": (child_cpu_after.ru_utime + child_cpu_after.ru_stime -
                child_cpu_before.ru_utime - child_cpu_before.ru_stime),
            "relay_parent_cpu_seconds": time.process_time() - own_cpu_start,
            "relay_metrics": relay.metrics.snapshot(),
            "controller_started_utc": started_utc}
        print(json.dumps(record, sort_keys=True), flush=True)
        return record
    finally:
        relay.close()


def ssh_command(socket_path: Path, port: int, host: str = REMOTE_HOST) -> list[str]:
    if not 1 <= port <= 65535:
        raise RemoteLauncherError("remote loopback port must be within 1..65535")
    if host != REMOTE_HOST:
        raise RemoteLauncherError(f"remote SSH host is fixed to {REMOTE_HOST}")
    return ["ssh", "-N", "-T", "-S", "none", "-o", "BatchMode=yes",
        "-o", "ExitOnForwardFailure=yes", "-o", "StrictHostKeyChecking=yes",
        "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
        "-L", f"{socket_path}:127.0.0.1:{port}", host]


def terminate_own_process(process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def connect_ssh(port: int, *, ssh_log: Path) -> tuple[subprocess.Popen[Any], Path, float]:
    started = time.monotonic()
    socket_dir = Path(tempfile.mkdtemp(prefix="ew7", dir="/tmp"))
    socket_dir.chmod(0o700)
    socket_path = socket_dir / "forward.sock"
    if len(os.fsencode(str(socket_path))) >= 100:
        shutil.rmtree(socket_dir)
        raise RemoteLauncherError("short /tmp SSH forward socket path exceeds Unix-domain limit")
    with ssh_log.open("x", encoding="utf-8") as log:
        process = subprocess.Popen(ssh_command(socket_path, port), stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=None)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            shutil.rmtree(socket_dir, ignore_errors=True)
            raise RemoteLauncherError(f"SSH local forward exited early with status {process.returncode}; see {ssh_log}")
        if socket_path.is_socket():
            return process, socket_dir, time.monotonic() - started
        time.sleep(0.1)
    terminate_own_process(process)
    shutil.rmtree(socket_dir, ignore_errors=True)
    raise RemoteLauncherError(f"SSH local Unix forward did not become ready; see {ssh_log}")


def run_bwrap(command: list[str], *, socket_dir: Path, run_dir: Path,
              stdout_path: Path, stderr_path: Path, timeout_seconds: float) -> dict[str, Any]:
    wrapper = build_bwrap_command(run_dir=run_dir, socket_dir=socket_dir, command=command)
    network_namespace_before = os.stat("/proc/self/ns/net").st_ino
    with stdout_path.open("x", encoding="utf-8") as stdout, stderr_path.open("x", encoding="utf-8") as stderr:
        process = subprocess.Popen(wrapper, cwd=ROOT, env={}, stdout=stdout, stderr=stderr,
                                   start_new_session=True)
        started = time.monotonic()
        try:
            code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            terminate_own_process(process)
            raise RemoteLauncherError(f"isolated command exceeded {timeout_seconds:.0f}s wall timeout")
    namespace_record = None
    for line in reversed(stdout_path.read_text(encoding="utf-8", errors="replace").splitlines()):
        try:
            decoded = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict) and "command_exit_code" in decoded and "relay_metrics" in decoded:
            namespace_record = decoded
            break
    return {"exit_code": code, "wall_seconds": time.monotonic() - started,
            "outer_network_namespace_inode": network_namespace_before,
            "wrapper_command": wrapper, "namespace_record": namespace_record}


def command_for_case(run: dict[str, Any], *, output_root: Path) -> list[str]:
    return [str(ROOT / ".venv/bin/python"), "scripts/run_m4_bank.py",
        "--mode", "diagnostic", "--profile", str(PROFILE),
        "--case-id", run["case_id"], "--run-dir", "dev",
        "--library-root", str(run["library_root"])]


def development_run_definitions() -> list[dict[str, Any]]:
    definitions = [dict(row) for row in RUNS]
    definitions[1]["library_root"] = (ROOT / "artifacts/m3/model_runs" /
        f"{definitions[0]['run_id']}/dev/diagnostic/library")
    return definitions


def verify_expected_r2_infrastructure_failure() -> dict[str, Any]:
    """Pin r3 to the preserved r2 hardware failure, never count it as a diagnostic miss."""
    verify_expected_pre_model_failure()
    status_path = safe_project_path(FAILED_R2_STATUS)
    first_dir = safe_project_path(FAILED_R2_FIRST_DIR)
    second_dir = safe_project_path(FAILED_R2_SECOND_DIR)
    status = json.loads(status_path.read_text(encoding="utf-8"))
    runs = status.get("runs", [])
    if (status.get("status") != "failed_infrastructure" or
            status.get("current_run_id") != "e4w7dev2r2" or len(runs) != 1 or
            runs[0].get("run_id") != "e4w7dev1r2" or
            "e4w7dev2r2" not in status.get("error", "")):
        raise RemoteLauncherError("r3 requires the finalized r2 infrastructure failure with case2 never diagnosed")
    result_path = first_dir / "dev/diagnostic/diagnostic_result.json"
    calls_path = first_dir / "dev/diagnostic/model_calls.jsonl"
    feedback_path = first_dir / "dev/m4_trusted_feedback/dev.jsonl"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    rows = [json.loads(line) for line in calls_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if (result.get("status") != "exhausted_or_failed" or result.get("accepted") is not False or
            result.get("case_id") != "bank-20210323T0630-utc8" or
            result.get("library_before_snapshot_id") != EXPECTED_INITIAL_SNAPSHOT or
            result.get("library_after_snapshot_id") != EXPECTED_INITIAL_SNAPSHOT or
            not rows or any(row.get("content") is not None or row.get("http_status") is not None or
                not isinstance(row.get("error"), dict) or
                row["error"].get("code") not in {"transport_error", "context_preflight_error"}
                for row in rows) or feedback_path.exists() or
            (second_dir / "dev/diagnostic/diagnostic_result.json").exists()):
        raise RemoteLauncherError("preserved r2 case1 is not a pure transport failure with no response, feedback, or library admission")
    from e4core.m4_library import load_library_snapshot
    before = load_library_snapshot(INITIAL_LIBRARY)
    after = load_library_snapshot(first_dir / "dev/diagnostic/library")
    if before.snapshot_id != after.snapshot_id or before.document != after.document:
        raise RemoteLauncherError("r2 changed the operator/playbook library; refusing r3")
    stdout_path = first_dir / "remote_controller.stdout.log"
    stderr_path = first_dir / "remote_controller.stderr.log"
    second_stdout = second_dir / "remote_controller.stdout.log"
    second_stderr = second_dir / "remote_controller.stderr.log"
    return {"attempt": PAIR_ATTEMPT,
        "supersedes_status_path": str(status_path),
        "supersedes_status_sha256": sha256_file(status_path),
        "superseded_run_ids": ["e4w7dev1r2", "e4w7dev2r2"],
        "r2_first_result_sha256": sha256_file(result_path),
        "r2_first_model_calls_sha256": sha256_file(calls_path),
        "r2_status_stdout_sha256": sha256_file(stdout_path),
        "r2_status_stderr_sha256": sha256_file(stderr_path),
        "r2_case2_stdout_sha256": sha256_file(second_stdout),
        "r2_case2_stderr_sha256": sha256_file(second_stderr),
        "classification": "infrastructure_failure_not_diagnostic_miss",
        "reason": "original server's SM70 flash-attention path crashed on SM75; all case1 completion attempts failed transport/context preflight without responses, case2 never entered diagnostic execution",
        "attempted_completion_requests": 1,
        "successful_model_responses": 0,
        "feedback_calls": 0,
        "operator_or_library_changes": 0,
        "r2_controller_attempt_count": len(rows),
        "initial_library_snapshot_id": EXPECTED_INITIAL_SNAPSHOT,
        "incident_trajectory_reroll": False}


def validate_neutral_smoke_preflight(status: dict[str, Any], *,
                                     remote_pins: dict[str, Any]) -> dict[str, Any]:
    """Require successful decode smoke from this exact attested server and port."""
    if (status.get("status") != "complete" or status.get("mode") != "neutral-gpu-smoke" or
            len(status.get("runs", [])) != 1 or
            status.get("remote_loopback_port") != remote_pins.get("remote_bind_port")):
        raise RemoteLauncherError("native SM75 neutral generation smoke status/port mismatch")
    attestation = status.get("remote_attestation", {})
    for key in ("remote_provenance_sha256", "remote_bind_port", "remote_llama_server_sha256",
                "remote_checkpoint_sha256", "remote_llama_cpp_revision"):
        if attestation.get(key) != remote_pins.get(key):
            raise RemoteLauncherError(f"neutral smoke used a different remote attestation ({key})")
    record = status["runs"][0]
    report_path = Path(record.get("neutral_report_path", ""))
    if (not report_path.is_file() or
            sha256_file(report_path) != record.get("neutral_report_sha256")):
        raise RemoteLauncherError("native SM75 neutral generation report is unavailable or changed")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if (report.get("status") != "complete" or
            report.get("data_classification") != "synthetic_neutral_only" or
            report.get("completion_requests_made") != 2 or
            report.get("short", {}).get("http_status") != 200 or
            report.get("near_context", {}).get("http_status") != 200 or
            int(report.get("short", {}).get("usage", {}).get("completion_tokens", 0) or 0) <= 0 or
            int(report.get("near_context", {}).get("usage", {}).get("completion_tokens", 0) or 0) <= 0):
        raise RemoteLauncherError("neutral smoke must show successful short and near-context GPU decoding")
    return report


def verify_expected_pre_model_failure() -> dict[str, Any]:
    """Only authorize r2 after the preserved first attempt's known parser-only failure."""
    status_path = safe_project_path(FAILED_FIRST_ATTEMPT_STATUS)
    run_dir = safe_project_path(FAILED_FIRST_ATTEMPT_DIR)
    status = json.loads(status_path.read_text(encoding="utf-8"))
    stdout_path = run_dir / "remote_controller.stdout.log"
    stderr_path = run_dir / "remote_controller.stderr.log"
    if (status.get("status") != "failed_infrastructure" or
            status.get("current_run_id") != "e4w7dev1" or status.get("runs") != [] or
            "isolated controller exited 2" not in status.get("error", "") or
            not stdout_path.is_file() or not stderr_path.is_file()):
        raise RemoteLauncherError("r2 requires the preserved parser-only failed e4w7dev1 attempt")
    stdout = json.loads(stdout_path.read_text(encoding="utf-8").splitlines()[-1])
    stderr = stderr_path.read_text(encoding="utf-8")
    if ("unrecognized arguments: scripts/run_m4_bank.py" not in stderr or
            stdout.get("command_exit_code") != 2 or
            stdout.get("health_probe", {}).get("endpoint") != "/health" or
            stdout.get("relay_metrics", {}).get("connection_count") != 1 or
            stdout.get("relay_metrics", {}).get("connections", [{}])[0].get("request_line") != "GET /health HTTP/1.1" or
            any(path.name not in {"remote_controller.stdout.log", "remote_controller.stderr.log"}
                for path in run_dir.iterdir())):
        raise RemoteLauncherError("preserved e4w7dev1 trajectory does not match the expected health-only parser failure")
    return {"attempt": PAIR_ATTEMPT,
        "supersedes_status_path": str(status_path),
        "supersedes_status_sha256": sha256_file(status_path),
        "superseded_run_id": "e4w7dev1",
        "superseded_run_dir": str(run_dir),
        "superseded_stdout_sha256": sha256_file(stdout_path),
        "superseded_stderr_sha256": sha256_file(stderr_path),
        "reason": "launcher passed the script filename as an M4 positional argument; parser exited before diagnostic execution",
        "observed_remote_requests": ["GET /health HTTP/1.1"],
        "completion_requests": 0,
        "incident_trajectory_reroll": False}


def diagnostic_summary(run: dict[str, Any], output_root: Path) -> dict[str, Any]:
    from e4core.m4_library import load_library_snapshot

    diag = output_root / "dev/diagnostic"
    result_path = diag / "diagnostic_result.json"
    if not result_path.is_file():
        raise RemoteLauncherError(f"{run['run_id']}: diagnostic result missing, incomplete infrastructure")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if result.get("case_id") != run["case_id"]:
        raise RemoteLauncherError(f"{run['run_id']}: diagnostic case identity mismatch")
    if result.get("status") not in {"accepted", "exhausted_or_failed"}:
        raise RemoteLauncherError(f"{run['run_id']}: diagnostic infrastructure status {result.get('status')!r}")
    if result.get("implementation_fingerprint") != EXPECTED_FINGERPRINT:
        raise RemoteLauncherError(f"{run['run_id']}: implementation fingerprint changed in controller")
    manifest_path = diag / "diagnostic_manifest.json"
    if not manifest_path.is_file():
        raise RemoteLauncherError(f"{run['run_id']}: controller diagnostic manifest missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("profile_sha256") != EXPECTED_PROFILE_SHA256 or
            manifest.get("m4_runtime_policy_sha256") != EXPECTED_POLICY_SHA256 or
            manifest.get("implementation_fingerprint") != EXPECTED_FINGERPRINT):
        raise RemoteLauncherError(f"{run['run_id']}: diagnostic manifest pins do not match V7")
    before = load_library_snapshot(run["library_root"])
    after = load_library_snapshot(diag / "library")
    if result.get("library_before_snapshot_id") != before.snapshot_id:
        raise RemoteLauncherError(f"{run['run_id']}: diagnostic library-before snapshot differs")
    if result.get("library_after_snapshot_id") != after.snapshot_id:
        raise RemoteLauncherError(f"{run['run_id']}: diagnostic library-after pointer differs")
    before_ops = {(row.get("name"), row.get("version")) for row in before.document.get("operators", [])
                  if row.get("kind") == "generated"}
    after_ops = {(row.get("name"), row.get("version")) for row in after.document.get("operators", [])
                 if row.get("kind") == "generated"}
    before_books = {row.get("name") for row in before.document.get("playbooks", [])
                    if row.get("kind") == "generated"}
    after_books = {row.get("name") for row in after.document.get("playbooks", [])
                   if row.get("kind") == "generated"}
    return {"run_id": run["run_id"], "case_id": run["case_id"],
        "status": result["status"], "accepted": result.get("accepted"),
        "accepted_stage": result.get("accepted_stage"),
        "diagnostic_result_path": str(result_path),
        "diagnostic_result_sha256": sha256_file(result_path),
        "diagnostic_wall_seconds": result.get("wall_seconds"),
        "model_cost": result.get("model_calls"),
        "library_before_snapshot_id": before.snapshot_id,
        "library_after_snapshot_id": after.snapshot_id,
        "admissions": {"generated_operators_added": sorted(f"{name}@{version}"
                for name, version in after_ops - before_ops),
            "generated_playbooks_added": sorted(after_books - before_books),
            "generated_operators_removed": sorted(f"{name}@{version}"
                for name, version in before_ops - after_ops),
            "generated_playbooks_removed": sorted(before_books - after_books)},
        "stages": [{key: stage.get(key) for key in
            ("stage", "status", "accepted", "evolve_status", "evolve_static_repairs",
             "stage3_serve_static_repairs") if key in stage} for stage in result.get("stages", [])]}


def atomic_json(path: Path, doc: dict[str, Any]) -> None:
    descriptor, name = tempfile.mkstemp(prefix=path.name + ".pending.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(doc, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def controller_phase_costs(run_root: Path) -> dict[str, Any]:
    path = run_root / "dev/diagnostic/model_calls.jsonl"
    phases: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        call = json.loads(line)
        phase = str(call.get("m4_phase", "unphased"))
        row = phases.setdefault(phase, {"calls": 0, "prompt_tokens": 0,
            "completion_tokens": 0, "elapsed_seconds": 0.0,
            "generation_elapsed_seconds": 0.0})
        row["calls"] += 1
        usage = call.get("usage", {})
        if isinstance(usage, dict):
            row["prompt_tokens"] += int(usage.get("prompt_tokens", usage.get("input_tokens", 0)) or 0)
            row["completion_tokens"] += int(usage.get("completion_tokens", usage.get("output_tokens", 0)) or 0)
        row["elapsed_seconds"] += float(call.get("elapsed_seconds") or 0)
        row["generation_elapsed_seconds"] += float(call.get("generation_elapsed_seconds") or 0)
    return phases


def verify_library_lineage(path: Path, ancestor_id: str) -> dict[str, Any]:
    from e4core.m4_library import load_library_snapshot
    current = load_library_snapshot(path)
    head = current
    seen = set()
    while current.snapshot_id != ancestor_id:
        if current.snapshot_id in seen or not current.parent_snapshot_id:
            raise RemoteLauncherError("development library did not descend from pinned V7 initial snapshot")
        seen.add(current.snapshot_id)
        current = load_library_snapshot(current.root / f"{current.parent_snapshot_id}.json")
    return {"head_snapshot_id": head.snapshot_id, "head_path": str(head.path)}


def compare_two_runs(results: list[dict[str, Any]], launch_docs: list[dict[str, Any]]) -> dict[str, Any]:
    prefixes = {f"accuracy_at_3_{i}": sum(any(stage.get("accepted") is True
        for stage in result["stages"][:i]) for result in results) for i in (1, 2, 3)}
    phase_totals: dict[str, dict[str, Any]] = {}
    for result in results:
        for phase, values in result.get("phase_costs", {}).items():
            total = phase_totals.setdefault(phase, {key: 0 for key in (
                "calls", "prompt_tokens", "completion_tokens", "elapsed_seconds",
                "generation_elapsed_seconds")})
            for key in total:
                total[key] += values.get(key, 0)
    return {"schema_version": "e4-remote-wingtip-development-pair/1",
        "split": "dev", "case_count": len(results), "runs": results,
        "accuracy_at_3_prefix_success_counts_of_2": prefixes,
        "phase_costs_cumulative": phase_totals,
        "admissions_by_case": {result["case_id"]: result["admissions"] for result in results},
        "transport": launch_docs,
        "limitations": ["Two fixed dev cases only; no scored accuracy claim.",
            "Remote Quadro RTX 8000/Turing results are not hardware-matched to local V100/Volta timings.",
            "Remote transport CPU/wall/health RTT are reported separately from M4 model costs."]}


def run_remote_mode(*, remote_port: int, remote_provenance_path: Path,
                    mode: str) -> dict[str, Any]:
    pins = verify_local_pins()
    remote_pins = verify_remote_attestation(remote_provenance_path, remote_port=remote_port)
    if mode == "run-development-pair":
        tokenizer_status_path = V7_ROOT / "remote_wingtip_tokenizer_status.json"
        if not tokenizer_status_path.is_file():
            raise RemoteLauncherError("remote tokenizer-only preflight must complete before development inference")
        tokenizer_status = json.loads(tokenizer_status_path.read_text(encoding="utf-8"))
        if (tokenizer_status.get("status") != "complete" or
                tokenizer_status.get("mode") != "tokenizer-only" or
                len(tokenizer_status.get("runs", [])) != 1):
            raise RemoteLauncherError("remote tokenizer-only status is not complete")
        tokenizer_record = tokenizer_status["runs"][0]
        report_path = Path(tokenizer_record.get("tokenizer_report_path", ""))
        if not report_path.is_file() or sha256_file(report_path) != tokenizer_record.get("tokenizer_report_sha256"):
            raise RemoteLauncherError("remote tokenizer report is unavailable or changed")
        token_report = json.loads(report_path.read_text(encoding="utf-8"))
        if (token_report.get("all_fit") is not True or
                token_report.get("completion_requests_made") != 0 or
                token_report.get("completion_endpoint_used") is not False or
                len(token_report.get("results", [])) != 38 or
                token_report.get("source_fingerprint") != EXPECTED_FINGERPRINT):
            raise RemoteLauncherError("all 38 exact typed-v7 payloads must pass tokenizer-only preflight before inference")
        neutral_path = V7_ROOT / "remote_wingtip_neutral_smoke_r3_status.json"
        if not neutral_path.is_file():
            raise RemoteLauncherError("native SM75 neutral generation smoke must complete before development inference")
        neutral_status = json.loads(neutral_path.read_text(encoding="utf-8"))
        neutral_report = validate_neutral_smoke_preflight(neutral_status, remote_pins=remote_pins)
        run_defs = development_run_definitions()
        engineering_retry = verify_expected_r2_infrastructure_failure()
    else:
        if mode == "tokenizer-only":
            run_defs = [{"run_id": TOKENIZER_RUN_ID, "case_id": None,
                "library_root": INITIAL_LIBRARY}]
        elif mode == "neutral-gpu-smoke":
            run_defs = [{"run_id": NEUTRAL_SMOKE_RUN_ID, "case_id": None,
                "library_root": INITIAL_LIBRARY}]
        else:
            raise RemoteLauncherError(f"unsupported remote mode: {mode}")
    output_roots = [ROOT / "artifacts/m3/model_runs" / row["run_id"] for row in run_defs]
    preexisting = [str(path) for path in output_roots if path.exists()]
    if preexisting:
        raise RemoteLauncherError(f"fixed remote launcher output already exists; refusing rerun: {preexisting}")
    suffix = {"tokenizer-only": "tokenizer", "neutral-gpu-smoke": "neutral_smoke_r3",
        "run-development-pair": "development_pair_r3"}[mode]
    status_path = V7_ROOT / f"remote_wingtip_{suffix}_status.json"
    if status_path.exists():
        raise RemoteLauncherError(f"{mode} status exists; refusing to repeat once-only run IDs")
    pair_path = V7_ROOT / ("remote_wingtip_development_pair_r3.json" if mode == "run-development-pair"
                           else "remote_wingtip_development_pair_unused.json")
    if mode == "run-development-pair" and pair_path.exists():
        raise RemoteLauncherError("remote development pair result exists; refusing to repeat fixed run IDs")
    status = {"schema_version": "e4-remote-wingtip-launch/1", "status": "starting",
        "mode": mode, "pins": pins, "remote_attestation": remote_pins,
        "remote_host": REMOTE_HOST, "remote_loopback_port": remote_port,
        "remote_hardware": remote_pins["remote_hardware"],
        **({"engineering_retry": engineering_retry} if mode == "run-development-pair" else {}),
        "local_comparison_hardware": LOCAL_COMPARISON_HARDWARE,
        "transport": {"type": "ssh-unix-domain-local-forward-plus-loopback-relay",
            "remote_target": f"127.0.0.1:{remote_port}", "bwrap_controller_tcp": False,
            "unix_socket_mounted_read_only": True},
        "runs": [], "started_utc": datetime.now(timezone.utc).isoformat()}
    atomic_json(status_path, status)
    socket_dir = None
    ssh_process = None
    try:
        ssh_log = status_path.parent / f"{status_path.stem}_ssh_forward.log"
        ssh_process, socket_dir, ssh_connect_seconds = connect_ssh(remote_port, ssh_log=ssh_log)
        status["ssh_forward"] = {"process_id": ssh_process.pid,
            "connect_seconds": ssh_connect_seconds,
            "ssh_log_path": str(ssh_log),
            "local_socket_path": str(socket_dir / "forward.sock"),
            "socket_path_bytes": len(os.fsencode(str(socket_dir / "forward.sock"))),
            "remote_target": f"127.0.0.1:{remote_port}"}
        atomic_json(status_path, status)
        for run_index, run in enumerate(run_defs):
            pins = verify_local_pins()
            status["pins_before_run"] = pins
            run_root = ROOT / "artifacts/m3/model_runs" / run["run_id"]
            run_root.mkdir(parents=True, exist_ok=False)
            if mode == "tokenizer-only":
                worker_command = [str(ROOT / ".venv/bin/python"), str(TOKENIZER_HELPER),
                    "--payload-dir", str(TOKENIZER_PAYLOAD_DIR)]
            elif mode == "neutral-gpu-smoke":
                worker_command = [str(ROOT / ".venv/bin/python"),
                    str(Path(__file__).resolve().parent / "neutral_model_smoke.py"),
                    "--target-tokens", "24000", "--timeout", "1800",
                    "--report", str(run_root / "neutral_model_smoke.json")]
            else:
                library = run["library_root"]
                if run_index == 1:
                    lineage = verify_library_lineage(library, EXPECTED_INITIAL_SNAPSHOT)
                else:
                    from e4core.m4_library import load_library_snapshot
                    first = load_library_snapshot(library)
                    if first.snapshot_id != EXPECTED_INITIAL_SNAPSHOT:
                        raise RemoteLauncherError("first run input library differs from pinned V7 initial snapshot")
                    lineage = {"head_snapshot_id": first.snapshot_id, "head_path": str(first.path)}
                m4_args = command_for_case(run, output_root=run_root)
                worker_command = [str(ROOT / ".venv/bin/python"), str(Path(__file__).resolve()),
                    "--m4-supervisor-child", "--socket-mount-source", "/run/e4remote-source",
                    "--", *m4_args[2:]]
            namespace_command = [str(ROOT / ".venv/bin/python"), str(Path(__file__).resolve()),
                "--namespace-child", "--socket-path", REMOTE_SOCKET_IN_NAMESPACE,
                "--run-dir", str(run_root), "--worker-mode",
                {"tokenizer-only": "tokenizer-only", "neutral-gpu-smoke": "neutral-smoke",
                 "run-development-pair": "diagnostic"}[mode], "--"] + worker_command
            stdout_path = run_root / "remote_controller.stdout.log"
            stderr_path = run_root / "remote_controller.stderr.log"
            status["status"] = "running"
            status["current_run_id"] = run["run_id"]
            status["updated_utc"] = datetime.now(timezone.utc).isoformat()
            atomic_json(status_path, status)
            wall_start = time.monotonic()
            namespace_result = run_bwrap(namespace_command, socket_dir=socket_dir,
                run_dir=run_root, stdout_path=stdout_path, stderr_path=stderr_path,
                timeout_seconds=7500 if mode == "run-development-pair" else 3600)
            if namespace_result["exit_code"] != 0:
                raise RemoteLauncherError(f"{run['run_id']} isolated controller exited {namespace_result['exit_code']}; no retries permitted")
            duration = time.monotonic() - wall_start
            namespace_record = namespace_result.get("namespace_record")
            if not isinstance(namespace_record, dict):
                raise RemoteLauncherError("isolated namespace exited without its relay/transport metrics record")
            if namespace_record.get("command_exit_code") != 0:
                raise RemoteLauncherError(f"{run['run_id']} controller child exited {namespace_record.get('command_exit_code')}")
            if not namespace_record.get("health_probe"):
                raise RemoteLauncherError(f"{run['run_id']} lacks a successful preflight health probe")
            if mode == "run-development-pair":
                summary = diagnostic_summary(run, run_root)
                summary["input_library_snapshot_id"] = lineage["head_snapshot_id"]
                summary["library_after"] = verify_library_lineage(
                    run_root / "dev/diagnostic/library", EXPECTED_INITIAL_SNAPSHOT)
                summary["phase_costs"] = controller_phase_costs(run_root)
                summary["remote_controller_wall_seconds"] = duration
            elif mode == "tokenizer-only":
                token_report_path = run_root / "prompt_tokenization_report.json"
                if not token_report_path.is_file():
                    raise RemoteLauncherError("tokenizer-only command returned without its exact-token report")
                token_report = json.loads(token_report_path.read_text(encoding="utf-8"))
                if (token_report.get("completion_requests_made") != 0 or
                        token_report.get("completion_endpoint_used") is not False or
                        len(token_report.get("results", [])) != 38 or
                        token_report.get("source_fingerprint") != EXPECTED_FINGERPRINT):
                    raise RemoteLauncherError("tokenizer-only helper unexpectedly used a completion endpoint")
                if token_report.get("all_fit") is not True:
                    raise RemoteLauncherError("remote tokenizer-only preflight found payloads exceeding the frozen context budget")
                summary = {"run_id": run["run_id"], "tokenizer_report_path": str(token_report_path),
                    "tokenizer_report_sha256": sha256_file(token_report_path),
                    "payload_count": len(token_report.get("results", [])),
                    "all_fit": token_report.get("all_fit"),
                    "remote_controller_wall_seconds": duration}
            else:
                neutral_report_path = run_root / "neutral_model_smoke.json"
                if not neutral_report_path.is_file():
                    raise RemoteLauncherError("neutral smoke returned without its generation report")
                neutral_report = json.loads(neutral_report_path.read_text(encoding="utf-8"))
                if (neutral_report.get("status") != "complete" or
                        neutral_report.get("data_classification") != "synthetic_neutral_only" or
                        neutral_report.get("completion_requests_made") != 2 or
                        neutral_report.get("short", {}).get("http_status") != 200 or
                        neutral_report.get("near_context", {}).get("http_status") != 200 or
                        int(neutral_report.get("short", {}).get("usage", {}).get("completion_tokens", 0) or 0) <= 0 or
                        int(neutral_report.get("near_context", {}).get("usage", {}).get("completion_tokens", 0) or 0) <= 0):
                    raise RemoteLauncherError("neutral smoke did not demonstrate successful GPU decoding")
                summary = {"run_id": run["run_id"],
                    "neutral_report_path": str(neutral_report_path),
                    "neutral_report_sha256": sha256_file(neutral_report_path),
                    "neutral_short_prompt_tokens": neutral_report["short"]["rendered_prompt_tokens"],
                    "neutral_near_context_prompt_tokens": neutral_report["near_context"]["rendered_prompt_tokens"],
                    "neutral_gpu_completion_tokens": [
                        neutral_report["short"]["usage"]["completion_tokens"],
                        neutral_report["near_context"]["usage"]["completion_tokens"]],
                    "remote_controller_wall_seconds": duration}
            summary["namespace_command_exit_code"] = namespace_result["exit_code"]
            summary["namespace_outer_network_inode"] = namespace_result["outer_network_namespace_inode"]
            summary["health_probe"] = namespace_record.get("health_probe")
            summary["relay_metrics"] = namespace_record.get("relay_metrics")
            summary["relay_parent_cpu_seconds"] = namespace_record.get("relay_parent_cpu_seconds")
            summary["controller_child_cpu_seconds"] = namespace_record.get("controller_child_cpu_seconds")
            summary["stdout_path"], summary["stderr_path"] = str(stdout_path), str(stderr_path)
            status["runs"].append(summary)
            status["updated_utc"] = datetime.now(timezone.utc).isoformat()
            atomic_json(status_path, status)
        if mode == "run-development-pair":
            pair = compare_two_runs(status["runs"], [row.get("relay_metrics") for row in status["runs"]])
            pair_path.write_text(json.dumps(pair, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                                 encoding="utf-8")
            status["pair_result_path"] = str(pair_path)
            status["pair_result_sha256"] = sha256_file(pair_path)
        status["status"] = "complete"
        status["finished_utc"] = datetime.now(timezone.utc).isoformat()
        atomic_json(status_path, status)
        return status
    except Exception as exc:
        status["status"] = "failed_infrastructure"
        status["error"] = f"{type(exc).__name__}: {str(exc)[:1000]}"
        status["finished_utc"] = datetime.now(timezone.utc).isoformat()
        atomic_json(status_path, status)
        raise
    finally:
        terminate_own_process(ssh_process)
        if socket_dir is not None:
            shutil.rmtree(socket_dir, ignore_errors=True)


def bwrap_smoke_command(*, socket_dir: Path, command: list[str], run_dir: Path) -> list[str]:
    return build_bwrap_command(run_dir=run_dir, socket_dir=socket_dir, command=command)


def namespace_child_main(*, socket_path: Path, run_dir: Path, command: list[str]) -> int:
    if not command:
        raise RemoteLauncherError("namespace child was not given a worker command")
    record = namespace_child(socket_path=socket_path, run_dir=run_dir, command=command)
    return int(record["command_exit_code"])


def m4_supervisor_child_main(*, socket_mount_source: Path, argv: list[str]) -> int:
    """Run unchanged M4 supervisor with only the trusted SSH socket added to its inner bwrap."""
    source = Path(socket_mount_source)
    if not source.is_dir() or not (source / "forward.sock").is_socket():
        raise RemoteLauncherError("M4 transport mount source must contain only the SSH forward socket")
    from scripts import run_m4_bank
    original = run_m4_bank._bwrap_command

    def bwrap_with_remote_transport(*args: Any, **kwargs: Any) -> list[str]:
        command = original(*args, **kwargs)
        separator = command.index("--")
        transport_mount = ["--dir", "/run", "--dir", "/run/e4remote",
            "--ro-bind", str(source), "/run/e4remote"]
        return command[:separator] + transport_mount + command[separator:]

    run_m4_bank._bwrap_command = bwrap_with_remote_transport
    return run_m4_bank.main(argv)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=False)
    modes.add_argument("--check-only", action="store_true",
        help="validate local V7 pins and isolation prerequisites; never SSH or launch")
    modes.add_argument("--tokenizer-only", action="store_true",
        help="run typed-v7 /apply-template and /tokenize checks through the SSH-forwarded model service")
    modes.add_argument("--run-development-pair", action="store_true",
        help="run the two fixed March dev cases once each, sequentially")
    modes.add_argument("--neutral-gpu-smoke", action="store_true",
        help="run neutral short and near-24k-token generations through the parent-started model server")
    parser.add_argument("--remote-port", type=int, default=DEFAULT_REMOTE_PORT)
    parser.add_argument("--remote-provenance", type=Path,
        help="parent-verified JSON attestation for remote GGUF, llama.cpp/server binary, hardware, and loopback bind")
    parser.add_argument("--namespace-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--socket-path", help=argparse.SUPPRESS)
    parser.add_argument("--run-dir", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--worker-mode", choices=("diagnostic", "tokenizer-only", "neutral-smoke"), help=argparse.SUPPRESS)
    parser.add_argument("--m4-supervisor-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--socket-mount-source", help=argparse.SUPPRESS)
    parser.add_argument("worker_command", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.namespace_child:
            if not args.socket_path or args.run_dir is None or args.worker_mode is None:
                parser.error("namespace child needs socket path, run directory, and worker mode")
            command = args.worker_command[1:] if args.worker_command[:1] == ["--"] else args.worker_command
            if not command:
                parser.error("namespace child needs a worker command after --")
            return namespace_child_main(socket_path=Path(args.socket_path),
                run_dir=args.run_dir, command=command)
        if args.m4_supervisor_child:
            if not args.socket_mount_source:
                parser.error("M4 supervisor transport wrapper needs a socket mount source")
            command = args.worker_command[1:] if args.worker_command[:1] == ["--"] else args.worker_command
            if not command:
                parser.error("M4 supervisor transport wrapper needs run_m4_bank arguments")
            return m4_supervisor_child_main(socket_mount_source=Path(args.socket_mount_source),
                                            argv=command)
        if not (args.check_only or args.tokenizer_only or args.run_development_pair or args.neutral_gpu_smoke):
            parser.error("choose --check-only, --tokenizer-only, --neutral-gpu-smoke, or --run-development-pair")
        pins = verify_local_pins()
        if args.check_only:
            if shutil.which("bwrap") is None or shutil.which("ssh") is None:
                raise RemoteLauncherError("check-only requires both bwrap and ssh executables")
            if socket.AF_UNIX is None:
                raise RemoteLauncherError("Unix-domain sockets are unavailable")
            print(json.dumps({"status": "local_pins_ready", "launches": 0,
                "pins": pins, "bwrap": shutil.which("bwrap"), "ssh": shutil.which("ssh"),
                "remote_host": REMOTE_HOST}, sort_keys=True))
            return 0
        if args.remote_provenance is None:
            parser.error("remote modes require --remote-provenance from the parent-verified Wingtip setup")
        status = run_remote_mode(remote_port=args.remote_port,
            remote_provenance_path=args.remote_provenance,
            mode=("tokenizer-only" if args.tokenizer_only else
                  "neutral-gpu-smoke" if args.neutral_gpu_smoke else "run-development-pair"))
        status_name = ("tokenizer" if args.tokenizer_only else
            "neutral_smoke_r3" if args.neutral_gpu_smoke else "development_pair_r3")
        print(json.dumps({"status": status["status"], "mode": status["mode"],
            "runs": [row.get("run_id") for row in status["runs"]],
            "status_path": str(V7_ROOT / f"remote_wingtip_{status_name}_status.json")}, sort_keys=True))
        return 0
    except RemoteLauncherError as exc:
        print(json.dumps({"status": "failed_or_blocked", "error": str(exc)}, sort_keys=True),
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
