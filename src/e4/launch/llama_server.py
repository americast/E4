"""Serve a local AnalysisModel with a pinned llama-server: verify the weights and server binary,
resolve its libraries, watch GPU memory, and check that it answers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from e4.models.analysis_model import AnalysisModel
from e4.paths import ROOT

DEFAULT_SERVER = "third_party/llama.cpp/build-v100/bin/llama-server"
PINNED_LLAMA_CPP_REVISION = "d81235049384534c167caea52b85a694f6103d14"
MIN_FREE_MIB = 3072


class LaunchError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inside_root(path: Path, name: str) -> Path:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise LaunchError(f"{name} must resolve within the project root") from exc
    return resolved


def read_manifest(model: AnalysisModel) -> tuple[dict[str, Any], Path]:
    """The pinned manifest a local model's weights are checked against, and its path."""
    manifest_path = inside_root(ROOT / model.local_server.model_manifest_path, "model manifest")
    return json.loads(manifest_path.read_text()), manifest_path


def verify_model(model_path: str, manifest: dict[str, Any]) -> tuple[Path, str]:
    model_file = inside_root(ROOT / model_path, "model")
    expected_sha = manifest.get("sha256")
    if (not isinstance(manifest.get("revision"), str) or not manifest["revision"] or
            not isinstance(manifest.get("filename"), str) or
            not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha)):
        raise LaunchError("model manifest must pin filename, revision, and SHA-256")
    if model_file.name != manifest["filename"]:
        raise LaunchError("model filename does not match its manifest")
    if not model_file.is_file():
        raise LaunchError(f"model file is missing: {model_file}")
    expected_size = manifest.get("size_bytes")
    if expected_size is not None and model_file.stat().st_size != expected_size:
        raise LaunchError("model size does not match its pinned manifest")
    actual_sha = sha256_file(model_file)
    if actual_sha.lower() != expected_sha.lower():
        raise LaunchError(f"model SHA-256 mismatch: got {actual_sha}, expected {expected_sha}")
    return model_file, actual_sha


def build_server_command(server: Path, model_file: Path, model: AnalysisModel) -> list[str]:
    """llama-server arguments: the model's port and context, and its local_server settings."""
    port = urlsplit(model.base_url).port
    command = [str(server), "--model", str(model_file), "--alias", str(model.model_alias),
               "--host", "127.0.0.1", "--port", str(port),
               "--ctx-size", str(model.context_tokens), "--n-gpu-layers", "99", "--parallel", "1",
               "--offline"]
    settings = model.local_server
    if model.reasoning_mode:
        command += ["--reasoning", str(model.reasoning_mode)]
    if settings.reasoning_format:
        command += ["--reasoning-format", str(settings.reasoning_format)]
    if settings.reasoning_budget is not None:
        command += ["--reasoning-budget", str(settings.reasoning_budget)]
    if not all(isinstance(arg, str) for arg in settings.server_args):
        raise LaunchError("server_args must be a list of strings")
    command.extend(settings.server_args)
    return command


def resolve_server_libraries(server: Path) -> tuple[list[str], list[dict[str, str]], str]:
    """Resolve shared objects and produce a narrow LD_LIBRARY_PATH for bwrap.

    The checked-in llama-server RUNPATH may refer to a host-side workspace alias
    that is absent inside the namespace. Resolve its dependencies on the host,
    canonicalize every path, then permit only the project and standard library
    trees that the launcher mounts.
    """
    server = inside_root(server, "llama-server")
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


def server_healthy(base_url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/health", timeout=timeout) as response:
            return 200 <= response.status < 300
    except (OSError, urllib.error.URLError):
        return False


def repo_revision(path: Path = ROOT) -> str | None:
    try:
        result = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"],
                                text=True, capture_output=True, timeout=5)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None
