#!/usr/bin/env python3
"""Launch a pinned local model and benchmark command in one offline namespace.

Example:
  .venv/bin/python scripts/run_m3_model.py --profile config/gemma_m3_inference.json -- \
    .venv/bin/python scripts/run_m3_benchmark.py --suite artifacts/m3/suite.json

The server and benchmark command share a private network namespace so they can
use localhost while they cannot reach external services. The command runs
without a shell; its output and the complete launch provenance are retained in
an isolated per-run artifact directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SERVER = "third_party/llama.cpp/build-v100/bin/llama-server"
PINNED_LLAMA_CPP_REVISION = "d81235049384534c167caea52b85a694f6103d14"
MIN_FREE_MIB = 3072
_received_signal: int | None = None


class LaunchError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inside_root(path: Path, name: str) -> Path:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise LaunchError(f"{name} must resolve within the project root") from exc
    return resolved


def _read_profile(path: Path) -> tuple[dict[str, Any], Path, dict[str, Any], Path]:
    profile_path = _inside_root(path, "profile")
    profile = json.loads(profile_path.read_text())
    required = ("model_path", "model_alias", "context_tokens", "max_generation_tokens",
                "temperature", "top_p", "top_k", "seed")
    missing = [name for name in required if name not in profile]
    if missing:
        raise LaunchError(f"inference profile is missing fields: {', '.join(missing)}")
    manifest_value = profile.get("model_manifest_path")
    if not isinstance(manifest_value, str) or not manifest_value:
        raise LaunchError("profile must name model_manifest_path")
    manifest_path = _inside_root(ROOT / manifest_value, "model manifest")
    manifest = json.loads(manifest_path.read_text())
    return profile, profile_path, manifest, manifest_path


def verify_model(profile: dict[str, Any], manifest: dict[str, Any]) -> tuple[Path, str]:
    model_path = _inside_root(ROOT / profile["model_path"], "model")
    expected_sha = manifest.get("sha256")
    if (not isinstance(manifest.get("revision"), str) or not manifest["revision"] or
            not isinstance(manifest.get("filename"), str) or
            not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha)):
        raise LaunchError("model manifest must pin filename, revision, and SHA-256")
    if model_path.name != manifest["filename"]:
        raise LaunchError("profile model filename does not match its manifest")
    if not model_path.is_file():
        raise LaunchError(f"model file is missing: {model_path}")
    expected_size = manifest.get("size_bytes")
    if expected_size is not None and model_path.stat().st_size != expected_size:
        raise LaunchError("model size does not match its pinned manifest")
    actual_sha = sha256_file(model_path)
    if actual_sha.lower() != expected_sha.lower():
        raise LaunchError(f"model SHA-256 mismatch: got {actual_sha}, expected {expected_sha}")
    return model_path, actual_sha


def _url(profile: dict[str, Any], port_override: int | None) -> tuple[str, int]:
    value = profile.get("base_url", "http://127.0.0.1:8080")
    parsed = urlsplit(value)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise LaunchError("base_url must be a local HTTP URL on 127.0.0.1 or localhost")
    port = port_override or parsed.port or 8080
    if not 1 <= port <= 65535:
        raise LaunchError("server port must be between 1 and 65535")
    return f"http://127.0.0.1:{port}", port


def build_server_command(server: Path, model: Path, profile: dict[str, Any],
                         port: int, context_tokens: int) -> list[str]:
    command = [str(server), "--model", str(model), "--alias", str(profile["model_alias"]),
               "--host", "127.0.0.1", "--port", str(port),
               "--ctx-size", str(context_tokens), "--n-gpu-layers", "99", "--parallel", "1",
               "--offline"]
    reasoning_mode = profile.get("reasoning_mode")
    if reasoning_mode:
        command += ["--reasoning", str(reasoning_mode)]
    if profile.get("reasoning_format"):
        command += ["--reasoning-format", str(profile["reasoning_format"])]
    if profile.get("reasoning_budget") is not None:
        command += ["--reasoning-budget", str(profile["reasoning_budget"])]
    extra = profile.get("server_args", [])
    if not isinstance(extra, list) or not all(isinstance(arg, str) for arg in extra):
        raise LaunchError("server_args must be a list of strings")
    command.extend(extra)
    return command


def resolve_server_libraries(server: Path) -> tuple[list[str], list[dict[str, str]], str]:
    """Resolve shared objects and produce a narrow LD_LIBRARY_PATH for bwrap.

    The checked-in llama-server RUNPATH may refer to a host-side workspace alias
    that is absent inside the namespace. Resolve its dependencies on the host,
    canonicalize every path, then permit only the project and standard library
    trees that the launcher mounts.
    """
    server = _inside_root(server, "llama-server")
    base_dirs = [str(server.parent)]
    allowed_roots = (ROOT.resolve(), Path("/usr").resolve(), Path("/lib").resolve(),
                     Path("/lib64").resolve())
    # Host profiles sometimes set CUDA's library directory explicitly. Resolve
    # and allowlist each entry before using it; never forward the raw variable.
    for raw_dir in os.environ.get("LD_LIBRARY_PATH", "").split(os.pathsep):
        if not raw_dir:
            continue
        try:
            candidate = Path(raw_dir).resolve(strict=True)
        except OSError:
            continue
        if not candidate.is_dir():
            continue
        if any(_is_within(candidate, root) for root in allowed_roots) and str(candidate) not in base_dirs:
            base_dirs.append(str(candidate))
    result = subprocess.run(["ldd", str(server)], capture_output=True, text=True,
                            timeout=20, env={"PATH": "/usr/bin:/bin", "LC_ALL": "C",
                                            "LD_LIBRARY_PATH": os.pathsep.join(base_dirs)})
    if result.returncode != 0:
        raise LaunchError(f"ldd failed for llama-server: {result.stderr.strip() or result.stdout.strip()}")
    dependencies: list[dict[str, str]] = []
    directories = list(base_dirs)
    for line in result.stdout.splitlines():
        if "not found" in line:
            raise LaunchError(f"unresolved llama-server dependency: {line.strip()}")
        match = re.match(r"\s*(\S+)\s+=>\s+(/\S+)\s+\(", line)
        if match:
            soname, raw_path = match.groups()
        else:
            match = re.match(r"\s*(/\S+)\s+\(", line)
            if not match:
                continue
            raw_path, soname = match.group(1), Path(match.group(1)).name
        library = Path(raw_path).resolve(strict=True)
        allowed = any(_is_within(library, prefix) for prefix in allowed_roots)
        if not allowed or not library.is_file():
            raise LaunchError(f"llama-server dependency is outside mounted library roots: {library}")
        directory = str(library.parent)
        if directory not in directories:
            directories.append(directory)
        dependencies.append({"soname": soname, "path": str(library)})
    if not dependencies:
        raise LaunchError("ldd returned no dynamic llama-server dependencies")
    return directories, dependencies, result.stdout


def _is_within(path: Path, prefix: Path) -> bool:
    try:
        path.relative_to(prefix)
        return True
    except ValueError:
        return False


def validate_server_library_dirs(server: Path, directories: Sequence[str]) -> list[str]:
    allowed_roots = (ROOT.resolve(), Path("/usr").resolve(), Path("/lib").resolve(),
                     Path("/lib64").resolve())
    validated = []
    for raw in directories:
        path = Path(raw).resolve(strict=True)
        if not path.is_dir() or not any(_is_within(path, prefix) for prefix in allowed_roots):
            raise LaunchError(f"server library directory is outside mounted roots: {raw}")
        value = str(path)
        if value not in validated:
            validated.append(value)
    if str(server.parent.resolve(strict=True)) not in validated:
        raise LaunchError("curated library path is missing the llama-server directory")
    return validated


def preflight_server(server: Path, env: Mapping[str, str], timeout: float = 15.0) -> dict[str, Any]:
    """Execute the real server's version command before allocating model memory."""
    result = subprocess.run([str(server), "--version"], capture_output=True, text=True,
                            timeout=timeout, env=dict(env), cwd=ROOT)
    record = {"command": [str(server), "--version"], "exit_code": result.returncode,
              "stdout": result.stdout, "stderr": result.stderr}
    if result.returncode != 0:
        raise LaunchError(f"llama-server dependency preflight failed ({result.returncode}): "
                          f"{result.stderr[-1000:] or result.stdout[-1000:]}")
    return record


def _tree_hashes(extra_paths: Sequence[Path]) -> dict[str, str]:
    files: set[Path] = {Path(__file__).resolve()}
    for directory in (ROOT / "e4core", ROOT / "e4infra", ROOT / "scripts", ROOT / "tests"):
        files.update(path.resolve() for path in directory.rglob("*.py") if path.is_file())
    files.update(path.resolve() for path in (ROOT / "config").glob("*.json") if path.is_file())
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


class GpuSampler:
    def __init__(self, reserve_mib: int = MIN_FREE_MIB, interval: float = 1.0):
        self.interval = interval
        self.reserve_mib = reserve_mib
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.loaded_sample_index: int | None = None
        self.stop_event = threading.Event()
        self.low_memory = threading.Event()
        self.thread = threading.Thread(target=self._sample, name="m3-gpu-sampler", daemon=True)

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            try:
                result = subprocess.run(
                    ["nvidia-smi", "--query-gpu=index,memory.used,memory.free,utilization.gpu",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=4,
                    env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
                if result.returncode:
                    raise LaunchError(result.stderr.strip() or "nvidia-smi failed")
                now = datetime.now(timezone.utc).isoformat()
                parsed = 0
                for line in result.stdout.splitlines():
                    fields = [part.strip() for part in line.split(",")]
                    if len(fields) != 4:
                        continue
                    sample = {"time_utc": now, "gpu_index": int(fields[0]),
                              "used_mib": int(fields[1]), "free_mib": int(fields[2]),
                              "utilization_pct": int(fields[3])}
                    self.samples.append(sample)
                    parsed += 1
                    if sample["free_mib"] < self.reserve_mib:
                        self.low_memory.set()
                if parsed == 0:
                    raise LaunchError("nvidia-smi returned no GPU memory rows")
            except Exception as exc:
                self.errors.append(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(self.interval)

    def start(self) -> None:
        self.thread.start()

    def wait_first(self, timeout: float = 6.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.samples:
                if self.low_memory.is_set():
                    raise LaunchError(f"GPU free memory is below {self.reserve_mib} MiB")
                return
            if self.errors:
                raise LaunchError(self.errors[-1])
            time.sleep(0.05)
        raise LaunchError("no GPU memory sample arrived before timeout")

    def mark_model_loaded(self) -> None:
        self.loaded_sample_index = len(self.samples)

    def wait_post_load_sample(self, timeout: float = 6.0) -> None:
        if self.loaded_sample_index is None:
            raise LaunchError("model load was not marked before GPU sampling")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.errors:
                raise LaunchError(f"GPU sampling failed after model load: {self.errors[-1]}")
            if len(self.samples) > self.loaded_sample_index:
                if self.low_memory.is_set():
                    raise LaunchError(f"GPU free memory is below {self.reserve_mib} MiB after model load")
                return
            time.sleep(0.05)
        raise LaunchError("no GPU memory sample arrived after model load")

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        self.thread.join(timeout=5)
        free = [sample["free_mib"] for sample in self.samples]
        used = [sample["used_mib"] for sample in self.samples]
        post_load_count = (max(0, len(self.samples) - self.loaded_sample_index)
                           if self.loaded_sample_index is not None else 0)
        return {"sample_count": len(self.samples), "post_load_sample_count": post_load_count,
                "minimum_free_mib": min(free) if free else None,
                "peak_used_mib": max(used) if used else None,
                "reserve_mib": self.reserve_mib,
                "reserve_satisfied": (bool(free) and min(free) >= self.reserve_mib and
                                      post_load_count > 0 and not self.errors),
                "samples": self.samples, "errors": self.errors}


def _health(base_url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/health", timeout=timeout) as response:
            return 200 <= response.status < 300
    except (OSError, urllib.error.URLError):
        return False


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


def _repo_revision(path: Path = ROOT) -> str | None:
    try:
        result = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"],
                                text=True, capture_output=True, timeout=5)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None


def _inner_run(profile_path: Path, server_arg: str, run_dir: Path,
               run_id: str, port_override: int | None, context_override: int | None,
               init_timeout: int | None, run_timeout: int | None,
               server_library_dirs: Sequence[str],
               command: Sequence[str]) -> int:
    started = datetime.now(timezone.utc).isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)
    sampler: GpuSampler | None = None
    server_proc: subprocess.Popen[Any] | None = None
    client_proc: subprocess.Popen[Any] | None = None
    exit_code = 1
    error: str | None = None
    _set_signal_handlers()
    try:
        profile, resolved_profile, manifest, manifest_path = _read_profile(profile_path)
        model, model_hash = verify_model(profile, manifest)
        server = _inside_root(ROOT / server_arg, "llama-server")
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
        client_args = list(command) or profile.get("benchmark_command", [])
        if not client_args or not all(isinstance(arg, str) for arg in client_args):
            raise LaunchError("supply a command after `--` or benchmark_command in the profile")
        context_tokens = (context_override if context_override is not None else
                          int(profile["context_tokens"]))
        if context_tokens <= 0:
            raise LaunchError("context_tokens must be positive")
        max_generation_tokens = int(profile["max_generation_tokens"])
        if max_generation_tokens <= 0:
            raise LaunchError("max_generation_tokens must be positive")
        if int(profile.get("parallel_requests", 1)) != 1:
            raise LaunchError("M3 benchmark runs must use serial inference (parallel_requests=1)")
        reserve_mib = int(profile.get("minimum_free_vram_mib", MIN_FREE_MIB))
        if reserve_mib < MIN_FREE_MIB:
            raise LaunchError(f"minimum_free_vram_mib must be at least {MIN_FREE_MIB}")
        base_url, port = _url(profile, port_override)
        server_cmd = build_server_command(server, model, profile, port, context_tokens)
        startup_timeout = (init_timeout if init_timeout is not None else
                           int(profile.get("model_init_timeout_seconds", 1800)))
        command_timeout = (run_timeout if run_timeout is not None else
                           int(profile.get("run_timeout_seconds", 4 * 60 * 60)))
        if startup_timeout <= 0 or command_timeout <= 0:
            raise LaunchError("startup and run timeouts must be positive")

        command_executable = shutil.which(client_args[0], path=f"{ROOT}/.venv/bin:/usr/bin:/bin")
        if command_executable is None:
            candidate = Path(client_args[0])
            if not candidate.is_absolute():
                candidate = ROOT / candidate
            if candidate.is_file():
                command_executable = str(candidate.resolve())
        extra_hash_paths = [Path(command_executable)] if command_executable else []
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
                extra_hash_paths.append(resolved_candidate)
        source_hashes = _tree_hashes(extra_hash_paths)
        llama_revision = _repo_revision(ROOT / "third_party/llama.cpp")
        if llama_revision != PINNED_LLAMA_CPP_REVISION:
            raise LaunchError(f"llama.cpp revision is not the pinned {PINNED_LLAMA_CPP_REVISION}: {llama_revision}")
        network_snapshot = _network_snapshot()
        clean_env = {"PATH": f"{ROOT}/.venv/bin:/usr/bin:/bin", "HOME": "/tmp",
                     "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "PYTHONIOENCODING": "utf-8",
                     "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
                     "TOKENIZERS_PARALLELISM": "false"}
        server_env = dict(clean_env)
        server_env["LD_LIBRARY_PATH"] = os.pathsep.join(resolved_library_dirs)
        server_preflight = preflight_server(server, server_env)
        (run_dir / "server-preflight.json").write_text(json.dumps({
            **server_preflight,
            "ldd_output": dependency_doc["ldd_output"],
            "library_directories": resolved_library_dirs,
            "resolved_dependencies": dependency_doc["dependencies"],
        }, indent=2, sort_keys=True) + "\n")
        profile_hash = sha256_file(resolved_profile)
        manifest_hash = sha256_file(manifest_path)
        launch = {
            "event": "launch", "run_id": run_id, "started_utc": started,
            "profile_path": str(resolved_profile), "profile_sha256": profile_hash,
            "model_manifest_path": str(manifest_path), "manifest_sha256": manifest_hash,
            "model_path": str(model), "model_repository": manifest.get("repository"),
            "model_filename": manifest["filename"], "model_revision": manifest["revision"],
            "model_sha256": model_hash, "model_size_bytes": model.stat().st_size,
            "model_alias": profile["model_alias"], "llama_server": str(server),
            "llama_server_sha256": server_hash, "llama_cpp_revision": llama_revision,
            "server_library_directories": resolved_library_dirs,
            "server_dependency_count": len(dependency_doc["dependencies"]),
            "server_preflight_path": str(run_dir / "server-preflight.json"),
            "context_tokens": context_tokens,
            "max_generation_tokens": max_generation_tokens,
            "reasoning_mode": profile.get("reasoning_mode", "profile_default"),
            "reasoning_format": profile.get("reasoning_format"),
            "reasoning_budget": profile.get("reasoning_budget"),
            "temperature": profile["temperature"], "top_p": profile["top_p"],
            "top_k": profile["top_k"], "seed": profile["seed"],
            "parallel_requests": int(profile.get("parallel_requests", 1)),
            "minimum_free_vram_mib": reserve_mib,
            "request_timeout_seconds": profile.get("request_timeout_seconds"),
            "network_namespace": "unshared; server and command share localhost",
            "network_snapshot": network_snapshot,
            "base_url": base_url, "server_command": server_cmd,
            "benchmark_command": client_args, "source_hashes": source_hashes,
            "repository_revision": _repo_revision(ROOT),
            "server_log": str(run_dir / "llama-server.log"),
            "command_stdout": str(run_dir / "command.stdout.log"),
            "command_stderr": str(run_dir / "command.stderr.log"),
        }
        (run_dir / "launch.json").write_text(json.dumps(launch, indent=2, sort_keys=True) + "\n")

        sampler = GpuSampler(reserve_mib=reserve_mib)
        sampler.start()
        sampler.wait_first()
        if _received_signal:
            raise LaunchError(f"received signal {_received_signal}")

        server_log_path = run_dir / "llama-server.log"
        with server_log_path.open("w") as server_log:
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
                if _health(base_url):
                    sampler.mark_model_loaded()
                    sampler.wait_post_load_sample()
                    break
                time.sleep(0.5)
            else:
                raise TimeoutError(f"llama-server failed health check after {startup_timeout}s")

            command_env = dict(clean_env)
            command_env.update({
                "E4_BASE_URL": base_url,
                "E4_RUN_DIR": str(run_dir),
                "E4_MODEL_PROFILE": str(resolved_profile),
                "E4_MAX_GENERATION_TOKENS": str(max_generation_tokens),
                "E4_CONTEXT_TOKENS": str(context_tokens),
                "E4_REQUEST_TIMEOUT_SECONDS": str(profile.get("request_timeout_seconds", 600)),
                "E4_MODEL_SETTINGS_JSON": json.dumps({
                    key: profile[key] for key in ("model_alias", "temperature", "top_p", "top_k", "seed",
                                                   "reasoning_mode", "reasoning_format", "reasoning_budget",
                                                   "max_generation_tokens") if key in profile
                }, separators=(",", ":")),
            })
            stdout_path, stderr_path = run_dir / "command.stdout.log", run_dir / "command.stderr.log"
            with stdout_path.open("w") as command_stdout, stderr_path.open("w") as command_stderr:
                client_proc = subprocess.Popen(client_args, cwd=ROOT, env=command_env,
                                               stdout=command_stdout, stderr=command_stderr,
                                               start_new_session=True)
                command_deadline = time.monotonic() + command_timeout
                while client_proc.poll() is None:
                    if _received_signal:
                        raise LaunchError(f"received signal {_received_signal}")
                    if sampler.errors:
                        raise LaunchError(f"GPU memory sampling failed during benchmark: {sampler.errors[-1]}")
                    if sampler.low_memory.is_set():
                        raise LaunchError(f"GPU free memory fell below {reserve_mib} MiB during benchmark")
                    if server_proc.poll() is not None:
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
        gpu = sampler.stop() if sampler is not None else {"sample_count": 0, "samples": []}
        if exit_code == 0 and not gpu.get("reserve_satisfied"):
            exit_code = 1
            error = error or "GPU memory reserve was not verified from post-load samples"
        gpu_summary = {key: value for key, value in gpu.items() if key != "samples"}
        summary = {"event": "run_complete", "run_id": run_id,
                   "finished_utc": datetime.now(timezone.utc).isoformat(),
                   "exit_code": exit_code, "ok": exit_code == 0 and error is None,
                   "error": error, "gpu_memory": gpu_summary,
                   "server_exit_code": server_proc.returncode if server_proc else None,
                   "command_exit_code": client_proc.returncode if client_proc else None}
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        if gpu.get("samples"):
            (run_dir / "gpu-memory.json").write_text(json.dumps(gpu, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, sort_keys=True))
    return exit_code


def _bwrap_command(profile: Path, server: str, run_dir: Path, run_id: str,
                   port: int | None, context_tokens: int | None, init_timeout: int | None,
                   run_timeout: int | None, server_library_dirs: Sequence[str],
                   command: Sequence[str]) -> list[str]:
    bwrap = shutil.which("bwrap")
    if not bwrap:
        raise LaunchError("bubblewrap is required for an offline shared server/client namespace")
    bindir = str(ROOT / ".venv/bin")
    # Keep every server/client descendant inside one PID namespace. If the
    # namespace supervisor is interrupted, the kernel reaps detached sessions
    # too; process-group cleanup alone cannot reach new-session descendants.
    args = [bwrap, "--unshare-net", "--unshare-pid", "--die-with-parent", "--clearenv",
            "--setenv", "PATH", f"{bindir}:/usr/bin:/bin", "--setenv", "HOME", "/tmp",
            "--setenv", "LANG", "C.UTF-8", "--setenv", "LC_ALL", "C.UTF-8",
            "--setenv", "OMP_NUM_THREADS", "1", "--setenv", "OPENBLAS_NUM_THREADS", "1",
            "--setenv", "MKL_NUM_THREADS", "1", "--setenv", "TOKENIZERS_PARALLELISM", "false",
            "--ro-bind", "/usr", "/usr", "--ro-bind", "/lib", "/lib"]
    if Path("/lib64").exists():
        args += ["--ro-bind", "/lib64", "/lib64"]
    if Path("/etc/ld.so.cache").exists():
        args += ["--dir", "/etc", "--ro-bind", "/etc/ld.so.cache", "/etc/ld.so.cache"]
    if Path("/sys").exists():
        args += ["--ro-bind", "/sys", "/sys"]
    args += ["--dev-bind", "/dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp",
             "--symlink", "/usr/bin", "/bin", "--symlink", "/usr/sbin", "/sbin",
             "--dir", "/raid", "--dir", "/raid/ssinha97",
             "--ro-bind", str(ROOT), str(ROOT), "--bind", str(run_dir), str(run_dir),
             "--chdir", str(ROOT), "--setenv", "E4_BWRAPPED", "1", "--",
             sys.executable, str(Path(__file__).resolve()), "--inside",
             "--profile", str(profile), "--server", server, "--run-dir", str(run_dir),
             "--run-id", run_id]
    for library_dir in server_library_dirs:
        args += ["--server-lib-dir", library_dir]
    if port is not None:
        args += ["--port", str(port)]
    if context_tokens is not None:
        args += ["--context-tokens", str(context_tokens)]
    if init_timeout is not None:
        args += ["--init-timeout", str(init_timeout)]
    if run_timeout is not None:
        args += ["--run-timeout", str(run_timeout)]
    args += ["--", *command]
    return args


def _outer_run(args: argparse.Namespace) -> int:
    profile = _inside_root(ROOT / args.profile, "profile")
    try:
        loaded_profile, _, _, _ = _read_profile(profile)
    except Exception as exc:
        raise LaunchError(str(exc)) from exc
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        command = loaded_profile.get("benchmark_command", [])
    if not command:
        raise LaunchError("provide a benchmark command after `--`")
    if not all(isinstance(arg, str) for arg in command):
        raise LaunchError("benchmark command arguments must be text")
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", run_id):
        raise LaunchError("run_id may contain only letters, digits, period, underscore, and hyphen")
    run_dir = ROOT / "artifacts/m3/model_runs" / run_id
    if run_dir.exists():
        raise LaunchError(f"run artifact directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)
    launch_request = {"run_id": run_id, "profile": str(profile), "server": args.server,
                      "port": args.port, "context_tokens_override": args.context_tokens,
                      "init_timeout": args.init_timeout, "run_timeout": args.run_timeout,
                      "command": command, "requested_utc": datetime.now(timezone.utc).isoformat()}
    (run_dir / "request.json").write_text(json.dumps(launch_request, indent=2) + "\n")
    server_path = _inside_root(ROOT / args.server, "llama-server")
    llama_revision = _repo_revision(ROOT / "third_party/llama.cpp")
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
    _set_signal_handlers()
    startup_timeout = (args.init_timeout if args.init_timeout is not None else
                       int(loaded_profile.get("model_init_timeout_seconds", 1800)))
    command_timeout = (args.run_timeout if args.run_timeout is not None else
                       int(loaded_profile.get("run_timeout_seconds", 4 * 60 * 60)))
    if startup_timeout <= 0 or command_timeout <= 0:
        raise LaunchError("startup and run timeouts must be positive")
    outer_deadline = time.monotonic() + startup_timeout + command_timeout + 60
    child: subprocess.Popen[Any] | None = None
    exit_code = 1
    error: str | None = None
    try:
        wrapper = _bwrap_command(profile, args.server, run_dir, run_id, args.port,
                                 args.context_tokens, args.init_timeout, args.run_timeout,
                                 library_dirs, command)
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
    parser.add_argument("--profile", type=Path, required=True,
                        help="M3 inference profile with pinned model_manifest_path")
    parser.add_argument("--server", default=DEFAULT_SERVER,
                        help=f"llama-server executable (default: {DEFAULT_SERVER})")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--context-tokens", type=int, default=None)
    parser.add_argument("--init-timeout", type=int, default=None)
    parser.add_argument("--run-timeout", type=int, default=None)
    parser.add_argument("--run-dir", type=Path, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--server-lib-dir", action="append", default=[], help=argparse.SUPPRESS)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER,
                        help="benchmark command and arguments, following `--`")
    args = parser.parse_args(argv)
    try:
        if args.inside:
            if not os.environ.get("E4_BWRAPPED") or args.run_dir is None or not args.run_id:
                raise LaunchError("internal mode requires launcher namespace metadata")
            return _inner_run(args.profile, args.server, args.run_dir, args.run_id,
                              args.port, args.context_tokens, args.init_timeout,
                              args.run_timeout, args.server_lib_dir,
                              args.command[1:] if args.command[:1] == ["--"] else args.command)
        return _outer_run(args)
    except Exception as exc:
        print(f"M3 model launch failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
