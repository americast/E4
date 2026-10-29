#!/usr/bin/env python3
"""Run a command against one analysis model, in the run sandbox.

  .venv/bin/python scripts/model/run.py --model gemma_3090 --run-id my-run -- \
    .venv/bin/python scripts/cyber/diagnose.py

The model is config/models/<name>.json. A local model (one with a local_server block) is served
with a pinned llama-server first, and the server and command share a private network namespace
so they can use localhost but cannot reach anything else. A remote model gets no server, and the
command keeps the host network to reach the endpoint. Either way the project is read-only, the
command runs without a shell, and its output and launch provenance are kept in
artifacts/m3/model_runs/<run-id>, which the command sees as E4_RUN_DIR; E4_MODEL names the model.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.launch.llama_server import (DEFAULT_SERVER, MIN_FREE_MIB, PINNED_LLAMA_CPP_REVISION,
                                    GpuSampler, LaunchError, build_server_command, inside_root,
                                    preflight_server, read_manifest, repo_revision,
                                    resolve_server_libraries, server_healthy, sha256_file,
                                    validate_server_library_dirs, verify_model)
from e4.launch.network import remote_network_binds
from e4.models.analysis_model import load_model

DEFAULT_RUN_TIMEOUT_SECONDS = 4 * 60 * 60
_received_signal: int | None = None


def _tree_hashes(extra_paths: Sequence[Path]) -> dict[str, str]:
    files: set[Path] = {Path(__file__).resolve()}
    for directory in (ROOT / "src", ROOT / "scripts", ROOT / "tests"):
        files.update(path.resolve() for path in directory.rglob("*.py") if path.is_file())
    baseline = ROOT / "src/e4/baselines/terminus"
    files.update(path.resolve() for path in baseline.rglob("*")
                 if path.is_file() and (path.suffix in {".yaml", ".lock"} or
                                        path.name == "Dockerfile"))
    files.update(path.resolve() for path in (ROOT / "config").rglob("*.json") if path.is_file())
    for path in extra_paths:
        if path.is_file():
            files.add(path.resolve())
    result = {}
    for path in sorted(files):
        try:
            rel = path.relative_to(ROOT.resolve()).as_posix()
        except ValueError:
            rel = str(path)
        result[rel] = sha256_file(path)
    return result


def _network_snapshot() -> dict[str, Any]:
    interfaces = sorted(name for _, name in socket.if_nameindex())
    route_path = Path("/proc/net/route")
    routes = route_path.read_text().splitlines()[1:] if route_path.exists() else []
    default_routes = [line for line in routes if line.split()[1:2] == ["00000000"]]
    snapshot = {"interfaces": interfaces, "routes": routes,
                "default_routes": default_routes}
    if interfaces != ["lo"] or default_routes:
        raise LaunchError(f"network namespace is not offline: {snapshot}")
    return snapshot


def _client_environment(base: Mapping[str, str], *, docker_control: bool,
                        run_dir: Path | None = None) -> dict[str, str]:
    """Build the isolated client environment, adding Docker config only when opted in."""
    environment = dict(base)
    environment["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
    if docker_control:
        if run_dir is None:
            raise LaunchError("Docker control client environment requires a writable run directory")
        environment["DOCKER_CONFIG"] = str(ROOT / "artifacts/m5/runtime/docker-config")
        environment["BUILDX_CONFIG"] = str(Path(run_dir) / "docker-buildx-config")
        environment["E4_DOCKER_CONTROL_ENABLED"] = "1"
    return environment


def _docker_control_preflight() -> dict[str, Any]:
    """Verify the optional trusted Docker control path before starting the model server."""
    socket_path = Path("/var/run/docker.sock")
    if not socket_path.is_socket():
        raise LaunchError("Docker control was enabled but the daemon socket is not mounted")
    runtime = ROOT / "artifacts/m5/runtime"
    pin_path = runtime / "docker_compose_plugin_pin.json"
    image_pin_path = runtime / "terminus_task_image_pin.json"
    try:
        plugin_pin = json.loads(pin_path.read_text(encoding="utf-8"))
        image_pin = json.loads(image_pin_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LaunchError("Docker control requires pinned Harbor Compose and task-image artifacts") from exc
    plugin_path = ROOT / str(plugin_pin.get("path", ""))
    buildx_path = ROOT / str(plugin_pin.get("buildx_path", ""))
    docker_config_path = ROOT / str(plugin_pin.get("docker_config_path", ""))
    docker_config_dir = docker_config_path.parent
    if (not plugin_path.is_file() or sha256_file(plugin_path) != plugin_pin.get("sha256") or
            not buildx_path.is_file() or
            sha256_file(buildx_path) != plugin_pin.get("buildx_sha256") or
            not docker_config_path.is_file() or
            sha256_file(docker_config_path) != plugin_pin.get("docker_config_sha256") or
            os.environ.get("DOCKER_CONFIG") != str(docker_config_dir)):
        raise LaunchError("Docker Compose plugin or DOCKER_CONFIG differs from the pinned runtime")
    compose = subprocess.run(["docker", "compose", "version"], capture_output=True,
                             text=True, timeout=20)
    if compose.returncode or plugin_pin.get("version") not in compose.stdout:
        raise LaunchError(f"pinned Docker Compose preflight failed: {compose.stderr.strip()}")
    buildx = subprocess.run(["docker", "buildx", "version"], capture_output=True,
                            text=True, timeout=20)
    if buildx.returncode or plugin_pin.get("buildx_version") not in buildx.stdout:
        raise LaunchError(f"pinned Docker Buildx preflight failed: {buildx.stderr.strip()}")
    info = subprocess.run(["docker", "info", "--format", "{{.ServerVersion}}"],
                          capture_output=True, text=True, timeout=20)
    if info.returncode or not info.stdout.strip():
        raise LaunchError(f"Docker daemon preflight failed: {info.stderr.strip()}")
    image_id = image_pin.get("terminus_task_image_id")
    if (not image_id or image_pin.get("terminus_task_image_digest") != image_id or
            not image_pin.get("image_archive_sha256")):
        raise LaunchError("Terminus task image pin is incomplete")
    image = subprocess.run(["docker", "image", "inspect", image_id,
                            "--format", "{{.Id}}"], capture_output=True,
                           text=True, timeout=30)
    if image.returncode or image.stdout.strip() != image_id:
        raise LaunchError("pinned Terminus task image is unavailable in Docker")
    return {
        "docker_socket_path": str(socket_path),
        "docker_server_version": info.stdout.strip(),
        "docker_compose_version": compose.stdout.strip(),
        "docker_compose_plugin_path": str(plugin_path),
        "docker_compose_plugin_sha256": plugin_pin["sha256"],
        "docker_buildx_version": buildx.stdout.strip(),
        "docker_buildx_plugin_path": str(buildx_path),
        "docker_buildx_plugin_sha256": plugin_pin["buildx_sha256"],
        "task_image_id": image_id,
        "task_image_archive_sha256": image_pin["image_archive_sha256"],
        "task_image_source_dockerfile_sha256": image_pin.get("dockerfile_sha256"),
    }


def _terminate_group(proc: subprocess.Popen[Any], grace: float = 10.0) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        if proc.poll() is None:
            proc.wait()
        return
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        proc.poll()  # Reap the session leader while checking any remaining group members.
        try:
            os.killpg(proc.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if proc.poll() is None:
        proc.wait()


def _set_signal_handlers() -> None:
    global _received_signal
    _received_signal = None
    def mark(signum, _frame):
        global _received_signal
        _received_signal = signum
    signal.signal(signal.SIGTERM, mark)
    signal.signal(signal.SIGINT, mark)


def _command_files(client_args: Sequence[str]) -> list[Path]:
    """The command's executable and any project files named in its arguments, for provenance."""
    command_executable = shutil.which(client_args[0], path=f"{ROOT}/.venv/bin:/usr/bin:/bin")
    if command_executable is None:
        candidate = Path(client_args[0])
        if not candidate.is_absolute():
            candidate = ROOT / candidate
        if candidate.is_file():
            command_executable = str(candidate.resolve())
    files = [Path(command_executable)] if command_executable else []
    for arg in client_args[1:]:
        candidate = Path(arg)
        if not candidate.is_absolute():
            candidate = ROOT / candidate
        if candidate.is_file():
            resolved_candidate = candidate.resolve()
            try:
                resolved_candidate.relative_to(ROOT.resolve())
            except ValueError:
                continue
            files.append(resolved_candidate)
    return files


def _inner_run(model_name: str, server_arg: str, run_dir: Path, run_id: str,
               init_timeout: int | None, run_timeout: int | None,
               server_library_dirs: Sequence[str], command: Sequence[str]) -> int:
    started = datetime.now(timezone.utc).isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)
    sampler: GpuSampler | None = None
    server_proc: subprocess.Popen[Any] | None = None
    server_log = None
    client_proc: subprocess.Popen[Any] | None = None
    server_load_seconds: float | None = None
    exit_code = 1
    error: str | None = None
    _set_signal_handlers()
    try:
        model = load_model(model_name)
        client_args = list(command)
        if not client_args or not all(isinstance(arg, str) for arg in client_args):
            raise LaunchError("supply a command after `--`")
        command_timeout = run_timeout if run_timeout is not None else DEFAULT_RUN_TIMEOUT_SECONDS
        if command_timeout <= 0:
            raise LaunchError("run timeout must be positive")
        docker_control_enabled = os.environ.get("E4_DOCKER_CONTROL_ENABLED") == "1"
        docker_preflight = _docker_control_preflight() if docker_control_enabled else None
        clean_env = {"PATH": f"{ROOT}/.venv/bin:/usr/bin:/bin", "HOME": "/tmp",
                     "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PYTHONIOENCODING": "utf-8",
                     "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
                     "TOKENIZERS_PARALLELISM": "false"}
        launch = {
            "event": "launch", "run_id": run_id, "started_utc": started,
            "model": model.name, "model_file": str(model.path),
            "model_file_sha256": sha256_file(model.path), "model_settings": model.document,
            "base_url": model.base_url,
            "docker_control_enabled": docker_control_enabled,
            "docker_socket_path": ("/var/run/docker.sock"
                                   if docker_control_enabled else None),
            "docker_control_preflight": docker_preflight,
            "benchmark_command": client_args,
            "source_hashes": _tree_hashes(_command_files(client_args)),
            "repository_revision": repo_revision(ROOT),
            "command_stdout": str(run_dir / "command.stdout.log"),
            "command_stderr": str(run_dir / "command.stderr.log"),
        }

        if model.is_local:
            settings = model.local_server
            manifest, manifest_path = read_manifest(model)
            model_file, model_hash = verify_model(settings.model_path, manifest)
            server = inside_root(ROOT / server_arg, "llama-server")
            if not server.is_file() or not os.access(server, os.X_OK):
                raise LaunchError(f"llama-server is missing or not executable: {server}")
            dependency_doc = json.loads((run_dir / "server-dependency-resolution.json").read_text())
            resolved_library_dirs = validate_server_library_dirs(server, server_library_dirs)
            if dependency_doc.get("server_path") != str(server):
                raise LaunchError("outer dependency resolution names a different server binary")
            server_hash = sha256_file(server)
            if dependency_doc.get("server_sha256") != server_hash:
                raise LaunchError("llama-server binary changed after dependency preflight")
            if dependency_doc.get("library_directories") != resolved_library_dirs:
                raise LaunchError("inner library path differs from validated outer dependency resolution")
            if settings.parallel_requests != 1:
                raise LaunchError("runs must use serial inference (parallel_requests=1)")
            reserve_mib = settings.minimum_free_vram_mib
            if reserve_mib < MIN_FREE_MIB:
                raise LaunchError(f"minimum_free_vram_mib must be at least {MIN_FREE_MIB}")
            server_cmd = build_server_command(server, model_file, model)
            startup_timeout = (init_timeout if init_timeout is not None else
                               settings.init_timeout_seconds)
            if startup_timeout <= 0:
                raise LaunchError("startup timeout must be positive")
            llama_revision = repo_revision(ROOT / "third_party/llama.cpp")
            if llama_revision != PINNED_LLAMA_CPP_REVISION:
                raise LaunchError(f"llama.cpp revision is not the pinned {PINNED_LLAMA_CPP_REVISION}: {llama_revision}")
            network_snapshot = _network_snapshot()
            server_env = dict(clean_env)
            server_env["LD_LIBRARY_PATH"] = os.pathsep.join(resolved_library_dirs)
            server_preflight = preflight_server(server, server_env)
            (run_dir / "server-preflight.json").write_text(json.dumps({
                **server_preflight,
                "ldd_output": dependency_doc["ldd_output"],
                "library_directories": resolved_library_dirs,
                "resolved_dependencies": dependency_doc["dependencies"],
            }, indent=2, sort_keys=True) + "\n")
            launch.update({
                "model_manifest_path": str(manifest_path), "manifest_sha256": sha256_file(manifest_path),
                "model_path": str(model_file), "model_repository": manifest.get("repository"),
                "model_filename": manifest["filename"], "model_revision": manifest["revision"],
                "model_sha256": model_hash, "model_size_bytes": model_file.stat().st_size,
                "llama_server": str(server), "llama_server_sha256": server_hash,
                "llama_cpp_revision": llama_revision,
                "server_library_directories": resolved_library_dirs,
                "server_dependency_count": len(dependency_doc["dependencies"]),
                "server_preflight_path": str(run_dir / "server-preflight.json"),
                "minimum_free_vram_mib": reserve_mib,
                "network_namespace": "unshared; server and command share localhost",
                "network_snapshot": network_snapshot,
                "server_command": server_cmd, "server_load_seconds": None,
                "server_log": str(run_dir / "llama-server.log"),
            })
            (run_dir / "launch.json").write_text(json.dumps(launch, indent=2, sort_keys=True) + "\n")

            sampler = GpuSampler(reserve_mib=reserve_mib)
            sampler.start()
            sampler.wait_first()
            if _received_signal:
                raise LaunchError(f"received signal {_received_signal}")

            server_log = (run_dir / "llama-server.log").open("w")
            server_launch_started = time.monotonic()
            server_proc = subprocess.Popen(server_cmd, cwd=ROOT, env=server_env,
                                           stdout=server_log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
            startup_deadline = time.monotonic() + startup_timeout
            while time.monotonic() < startup_deadline:
                if _received_signal:
                    raise LaunchError(f"received signal {_received_signal}")
                if sampler.errors:
                    raise LaunchError(f"GPU memory sampling failed during model startup: {sampler.errors[-1]}")
                if sampler.low_memory.is_set():
                    raise LaunchError(f"GPU free memory fell below {reserve_mib} MiB during model startup")
                if server_proc.poll() is not None:
                    raise LaunchError(f"llama-server exited during startup with status {server_proc.returncode}")
                if server_healthy(model.base_url):
                    server_load_seconds = time.monotonic() - server_launch_started
                    launch["server_load_seconds"] = server_load_seconds
                    launch["server_ready_utc"] = datetime.now(timezone.utc).isoformat()
                    (run_dir / "launch.json").write_text(
                        json.dumps(launch, indent=2, sort_keys=True) + "\n"
                    )
                    sampler.mark_model_loaded()
                    sampler.wait_post_load_sample()
                    break
                time.sleep(0.5)
            else:
                raise TimeoutError(f"llama-server failed health check after {startup_timeout}s")
        else:
            launch["network_namespace"] = "shared; the command reaches the remote model endpoint"
            (run_dir / "launch.json").write_text(json.dumps(launch, indent=2, sort_keys=True) + "\n")

        command_env = _client_environment(clean_env, docker_control=docker_control_enabled,
                                          run_dir=run_dir)
        if docker_control_enabled:
            Path(command_env["BUILDX_CONFIG"]).mkdir(parents=True, exist_ok=True)
        command_env.update({"E4_RUN_DIR": str(run_dir), "E4_MODEL": model.name})
        stdout_path, stderr_path = run_dir / "command.stdout.log", run_dir / "command.stderr.log"
        with stdout_path.open("w") as command_stdout, stderr_path.open("w") as command_stderr:
            client_proc = subprocess.Popen(client_args, cwd=ROOT, env=command_env,
                                           stdout=command_stdout, stderr=command_stderr,
                                           start_new_session=True)
            command_deadline = time.monotonic() + command_timeout
            while client_proc.poll() is None:
                if _received_signal:
                    raise LaunchError(f"received signal {_received_signal}")
                if sampler is not None and sampler.errors:
                    raise LaunchError(f"GPU memory sampling failed during benchmark: {sampler.errors[-1]}")
                if sampler is not None and sampler.low_memory.is_set():
                    raise LaunchError(f"GPU free memory fell below {sampler.reserve_mib} MiB during benchmark")
                if server_proc is not None and server_proc.poll() is not None:
                    raise LaunchError(f"llama-server exited during benchmark with status {server_proc.returncode}")
                if time.monotonic() >= command_deadline:
                    raise TimeoutError(f"benchmark command exceeded {command_timeout}s")
                time.sleep(0.2)
            exit_code = client_proc.returncode
        if exit_code:
            error = f"benchmark command exited with status {exit_code}"
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, TimeoutError):
            exit_code = 124
        elif _received_signal:
            exit_code = 128 + _received_signal
        else:
            exit_code = 1
    finally:
        if client_proc is not None:
            _terminate_group(client_proc, grace=5)
        if server_proc is not None:
            _terminate_group(server_proc, grace=15)
        if server_log is not None:
            server_log.close()
        gpu = sampler.stop() if sampler is not None else {"sample_count": 0, "samples": []}
        if sampler is not None and exit_code == 0 and not gpu.get("reserve_satisfied"):
            exit_code = 1
            error = error or "GPU memory reserve was not verified from post-load samples"
        gpu_summary = {key: value for key, value in gpu.items() if key != "samples"}
        summary = {"event": "run_complete", "run_id": run_id,
                   "finished_utc": datetime.now(timezone.utc).isoformat(),
                   "exit_code": exit_code, "ok": exit_code == 0 and error is None,
                   "error": error, "gpu_memory": gpu_summary,
                   "server_exit_code": server_proc.returncode if server_proc else None,
                   "server_load_seconds": server_load_seconds,
                   "command_exit_code": client_proc.returncode if client_proc else None}
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        if gpu.get("samples"):
            (run_dir / "gpu-memory.json").write_text(json.dumps(gpu, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, sort_keys=True))
    return exit_code


def _bwrap_command(model_name: str, server: str, run_dir: Path, run_id: str,
                   init_timeout: int | None, run_timeout: int | None,
                   server_library_dirs: Sequence[str], command: Sequence[str], *,
                   local: bool, docker_control: bool = False,
                   docker_socket: Path = Path("/var/run/docker.sock")) -> list[str]:
    """The sandbox: read-only project, writable run directory, one PID namespace. A local model's
    server and the command share a private network; a remote model's command keeps the host's."""
    bwrap = shutil.which("bwrap")
    if not bwrap:
        raise LaunchError("bubblewrap is required for the run sandbox")
    bindir = str(ROOT / ".venv/bin")
    # Keep every server/client descendant inside one PID namespace. If the
    # namespace supervisor is interrupted, the kernel reaps detached sessions
    # too; process-group cleanup alone cannot reach new-session descendants.
    network = ["--unshare-net"] if local else []
    args = [bwrap, *network, "--unshare-pid", "--die-with-parent", "--clearenv",
            "--setenv", "PATH", f"{bindir}:/usr/bin:/bin", "--setenv", "HOME", "/tmp",
            "--setenv", "LANG", "C.UTF-8", "--setenv", "LC_ALL", "C.UTF-8",
            "--setenv", "OMP_NUM_THREADS", "1", "--setenv", "OPENBLAS_NUM_THREADS", "1",
            "--setenv", "MKL_NUM_THREADS", "1", "--setenv", "TOKENIZERS_PARALLELISM", "false",
            "--ro-bind", "/usr", "/usr", "--ro-bind", "/lib", "/lib"]
    if Path("/lib64").exists():
        args += ["--ro-bind", "/lib64", "/lib64"]
    if Path("/etc/ld.so.cache").exists():
        args += ["--dir", "/etc", "--ro-bind", "/etc/ld.so.cache", "/etc/ld.so.cache"]
    if not local:
        args += remote_network_binds()
    if Path("/sys").exists():
        args += ["--ro-bind", "/sys", "/sys"]
    args += ["--dev-bind", "/dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp",
             "--symlink", "/usr/bin", "/bin", "--symlink", "/usr/sbin", "/sbin",
             "--dir", "/raid", "--dir", "/raid/ssinha97",
             "--ro-bind", str(ROOT), str(ROOT), "--bind", str(run_dir), str(run_dir),
             ]
    # data/cyber links to the attacker runs outside the project; mount its target read-only too
    cyber_runs = ROOT / "data/cyber"
    if cyber_runs.is_symlink():
        target = str(cyber_runs.resolve(strict=True))
        args += ["--ro-bind", target, target]
    if docker_control:
        socket_path = Path(docker_socket)
        if not socket_path.is_socket():
            raise LaunchError(f"Docker control requested but socket is unavailable: {socket_path}")
        docker_config = ROOT / "artifacts/m5/runtime/docker-config"
        if not (docker_config / "config.json").is_file():
            raise LaunchError("Docker control requires the pinned Harbor Compose plugin setup")
        args += ["--dir", "/var", "--dir", "/var/run",
                 "--bind", str(socket_path.resolve(strict=True)), "/var/run/docker.sock"]
    args += ["--chdir", str(ROOT), "--setenv", "E4_BWRAPPED", "1",
             "--setenv", "E4_DOCKER_CONTROL_ENABLED", "1" if docker_control else "0"]
    if docker_control:
        args += ["--setenv", "DOCKER_CONFIG", str(ROOT / "artifacts/m5/runtime/docker-config")]
    args += ["--",
             sys.executable, str(Path(__file__).resolve()), "--inside",
             "--model", model_name, "--server", server, "--run-dir", str(run_dir),
             "--run-id", run_id]
    for library_dir in server_library_dirs:
        args += ["--server-lib-dir", library_dir]
    if init_timeout is not None:
        args += ["--init-timeout", str(init_timeout)]
    if run_timeout is not None:
        args += ["--run-timeout", str(run_timeout)]
    args += ["--", *command]
    return args


def _resolve_local_server(server_arg: str, run_dir: Path) -> list[str]:
    """Check the pinned llama.cpp build and record its libraries; returns their directories."""
    server_path = inside_root(ROOT / server_arg, "llama-server")
    llama_revision = repo_revision(ROOT / "third_party/llama.cpp")
    if llama_revision != PINNED_LLAMA_CPP_REVISION:
        raise LaunchError(f"llama.cpp revision is not the pinned {PINNED_LLAMA_CPP_REVISION}: {llama_revision}")
    try:
        library_dirs, dependencies, ldd_output = resolve_server_libraries(server_path)
        server_hash = sha256_file(server_path)
        dependency_record = {
            "server_path": str(server_path), "server_sha256": server_hash,
            "llama_cpp_revision": llama_revision,
            "library_directories": library_dirs, "dependencies": dependencies,
            "ldd_output": ldd_output,
            "resolver_environment": "clean PATH plus validated allowlisted LD_LIBRARY_PATH entries",
            "resolved_utc": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        dependency_record = {"server_path": str(server_path), "error": f"{type(exc).__name__}: {exc}"}
        (run_dir / "server-dependency-resolution.json").write_text(
            json.dumps(dependency_record, indent=2, sort_keys=True) + "\n")
        raise
    (run_dir / "server-dependency-resolution.json").write_text(
        json.dumps(dependency_record, indent=2, sort_keys=True) + "\n")
    return library_dirs


def _outer_run(args: argparse.Namespace) -> int:
    model = load_model(args.model)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise LaunchError("provide a command after `--`")
    if not all(isinstance(arg, str) for arg in command):
        raise LaunchError("command arguments must be text")
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", run_id):
        raise LaunchError("run_id may contain only letters, digits, period, underscore, and hyphen")
    run_dir = ROOT / "artifacts/m3/model_runs" / run_id
    if run_dir.exists():
        raise LaunchError(f"run artifact directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    launch_request = {"run_id": run_id, "model": model.name,
                      "server": args.server if model.is_local else None,
                      "init_timeout": args.init_timeout, "run_timeout": args.run_timeout,
                      "command": command, "docker_control_enabled": args.docker_control,
                      "docker_socket_path": "/var/run/docker.sock" if args.docker_control else None,
                      "requested_utc": datetime.now(timezone.utc).isoformat()}
    (run_dir / "request.json").write_text(json.dumps(launch_request, indent=2) + "\n")
    library_dirs: list[str] = []
    startup_timeout = 0
    if model.is_local:
        library_dirs = _resolve_local_server(args.server, run_dir)
        startup_timeout = (args.init_timeout if args.init_timeout is not None else
                           model.local_server.init_timeout_seconds)
    _set_signal_handlers()
    command_timeout = args.run_timeout if args.run_timeout is not None else DEFAULT_RUN_TIMEOUT_SECONDS
    if startup_timeout < 0 or command_timeout <= 0:
        raise LaunchError("startup and run timeouts must be positive")
    outer_deadline = time.monotonic() + startup_timeout + command_timeout + 60
    child: subprocess.Popen[Any] | None = None
    exit_code = 1
    error: str | None = None
    try:
        wrapper = _bwrap_command(model.name, args.server, run_dir, run_id,
                                 args.init_timeout, args.run_timeout, library_dirs, command,
                                 local=model.is_local, docker_control=args.docker_control)
        child = subprocess.Popen(wrapper, cwd=ROOT, env={}, start_new_session=True)
        while child.poll() is None:
            if _received_signal:
                _terminate_group(child, grace=10)
                exit_code = 128 + _received_signal
                error = f"launcher received signal {_received_signal}"
                break
            if time.monotonic() > outer_deadline:
                _terminate_group(child, grace=10)
                exit_code = 124
                error = "outer launcher watchdog expired"
                break
            time.sleep(0.2)
        else:
            exit_code = child.returncode
            if exit_code:
                error = f"bubblewrap run exited with status {exit_code}"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        exit_code = 1
    finally:
        if child is not None:
            _terminate_group(child, grace=10)
        (run_dir / "outer-supervisor.json").write_text(json.dumps({
            "run_id": run_id, "finished_utc": datetime.now(timezone.utc).isoformat(),
            "exit_code": exit_code, "error": error,
            "signal": _received_signal,
            "bubblewrap_exit_code": child.returncode if child else None,
        }, indent=2, sort_keys=True) + "\n")
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True,
                        help="model name: config/models/<name>.json")
    parser.add_argument("--server", default=DEFAULT_SERVER,
                        help=f"llama-server executable for a local model (default: {DEFAULT_SERVER})")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--init-timeout", type=int, default=None)
    parser.add_argument("--run-timeout", type=int, default=None)
    parser.add_argument("--docker-control", action="store_true",
                        help="mount the host Docker socket into this trusted controller process only")
    parser.add_argument("--run-dir", type=Path, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--server-lib-dir", action="append", default=[], help=argparse.SUPPRESS)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER,
                        help="command and arguments, following `--`")
    args = parser.parse_args(argv)
    try:
        if args.inside:
            if not os.environ.get("E4_BWRAPPED") or args.run_dir is None or not args.run_id:
                raise LaunchError("internal mode requires launcher namespace metadata")
            return _inner_run(args.model, args.server, args.run_dir, args.run_id,
                              args.init_timeout, args.run_timeout, args.server_lib_dir,
                              args.command[1:] if args.command[:1] == ["--"] else args.command)
        return _outer_run(args)
    except Exception as exc:
        print(f"model run failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
