#!/usr/bin/env python3
"""Set up pinned Harbor Terminus-2 runtime and offline Bank task image."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "artifacts/m5/runtime"
HARBOR_REVISION = "26106c6d9c66848a1440faf5724d336affac52aa"
BASE_IMAGE = (
    "python:3.10-slim-bookworm@sha256:"
    "5be5aaecb962a41567bc8041f1e9e3432cb085b037fe057f9a562630b2e4b370"
)
IMAGE_TAG = "e4-terminus2-bank:py310-20261016"
DOCKER_COMPOSE_VERSION = "v2.32.4"
DOCKER_COMPOSE_SHA256 = "ed1917fb54db184192ea9d0717bcd59e3662ea79db48bff36d3475516c480a6b"
DOCKER_BUILDX_VERSION = "v0.20.1"
DOCKER_BUILDX_SHA256 = "8c38f60308a895fa570f1410e453c5de11aafd65a99fa99965d96d24b6225a78"


class SetupError(RuntimeError):
    pass


def _run(command: list[str], *, cwd: Path = ROOT, timeout: int = 3600,
         env: dict[str, str] | None = None) -> str:
    try:
        result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True,
                                timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupError(f"command failed to start or timed out: {command[0]}: {exc}") from exc
    if result.returncode:
        detail = (result.stderr or result.stdout)[-4000:]
        raise SetupError(f"command exited {result.returncode}: {command!r}\n{detail}")
    return result.stdout.strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def harbor_source_pin(source: Path, runtime: Path = RUNTIME,
                      python: Path | None = None) -> dict[str, Any]:
    source = source.resolve(strict=True)
    revision = _run(["git", "rev-parse", "HEAD"], cwd=source, timeout=30)
    git_tree = _run(["git", "rev-parse", "HEAD^{tree}"], cwd=source, timeout=30)
    dirty = _run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=source, timeout=30)
    if dirty:
        raise SetupError("Harbor checkout contains modified or untracked files")
    inventory: dict[str, str] = {}
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if not path.is_file() or ".git" in relative.parts:
            continue
        if "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        inventory[relative.as_posix()] = sha256_file(path)
    canonical = json.dumps(inventory, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":")).encode("utf-8")
    requirements = ROOT / "e4baseline/harbor_requirements.lock"
    build_requirements = ROOT / "e4baseline/harbor_build_requirements.lock"
    agent_source = source / "src/harbor/agents/terminus_2/terminus_2.py"
    pin: dict[str, Any] = {
        "schema_version": "e4-terminus2-harbor-source-pin/1",
        "harbor_repository": "https://github.com/harbor-framework/harbor.git",
        "harbor_revision": revision,
        "harbor_git_tree": git_tree,
        "harbor_source_tree_sha256": hashlib.sha256(canonical).hexdigest(),
        "harbor_source_path": source.relative_to(ROOT).as_posix(),
        "harbor_agent_path": "src/harbor/agents/terminus_2/terminus_2.py",
        "harbor_agent_sha256": sha256_file(agent_source),
        "dependency_lock_version": "harbor-0.23.0-requirements-lock/1",
        "dependency_lock_sha256": sha256_file(requirements),
        "build_dependency_lock_sha256": sha256_file(build_requirements),
        "python_version": None,
        "harbor_version": None,
        "pip_freeze_path": None,
        "pip_freeze_sha256": None,
    }
    if python is not None and Path(python).is_file():
        python = Path(python).resolve(strict=True)
        python_version = _run([str(python), "--version"], timeout=30)
        harbor_version = _run([str(python), "-c",
                               "from importlib.metadata import version; print(version('harbor'))"], timeout=30)
        if python_version != "Python 3.13.16" or harbor_version != "0.23.0":
            raise SetupError("Harbor interpreter/distribution does not match the pinned runtime")
        freeze = _run([str(python), "-m", "pip", "freeze", "--all"], timeout=60)
        freeze_path = runtime / "harbor-python-pip-freeze.txt"
        freeze_path.parent.mkdir(parents=True, exist_ok=True)
        freeze_path.write_text(freeze + "\n", encoding="utf-8")
        pin.update({"python_version": python_version, "harbor_version": harbor_version,
                    "pip_freeze_path": freeze_path.relative_to(ROOT).as_posix(),
                    "pip_freeze_sha256": sha256_file(freeze_path)})
    pin_path = runtime / "harbor_source_pin.json"
    pin_path.parent.mkdir(parents=True, exist_ok=True)
    pin_path.write_text(json.dumps(pin, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                        encoding="utf-8")
    return pin


def harbor_source(runtime: Path = RUNTIME) -> Path:
    source = runtime / "harbor-source"
    if source.exists():
        actual = _run(["git", "rev-parse", "HEAD"], cwd=source, timeout=30)
        if actual != HARBOR_REVISION:
            raise SetupError(f"existing Harbor checkout has unexpected revision {actual}")
        return source
    source.parent.mkdir(parents=True, exist_ok=True)
    _run(["git", "clone", "--no-checkout", "https://github.com/harbor-framework/harbor.git",
          str(source)], timeout=1200)
    _run(["git", "checkout", "--detach", HARBOR_REVISION], cwd=source, timeout=120)
    return source


def _conda_executable() -> str:
    configured = os.environ.get("CONDA_EXE")
    if configured and Path(configured).is_file():
        return configured
    executable = shutil.which("conda")
    if executable:
        return executable
    fallback = Path.home() / "anaconda3/bin/conda"
    if fallback.is_file():
        return str(fallback)
    raise SetupError("Conda is needed to create the isolated Python 3.13 Harbor runtime")


def _setup_environment(runtime: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update({
        "CONDA_PKGS_DIRS": str(runtime / "conda-pkgs"),
        "PIP_CACHE_DIR": str(runtime / "pip-cache"),
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PATH": str(runtime / "harbor-python/bin") + os.pathsep +
                env.get("PATH", "/usr/bin:/bin"),
    })
    Path(env["CONDA_PKGS_DIRS"]).mkdir(parents=True, exist_ok=True)
    Path(env["PIP_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    return env


def ensure_python_runtime(source: Path, runtime: Path = RUNTIME) -> Path:
    python_root = runtime / "harbor-python"
    python = python_root / "bin/python"
    setup_env = _setup_environment(runtime)
    if not python.is_file():
        _run([_conda_executable(), "create", "--prefix", str(python_root),
              "python=3.13.16", "pip=26.2.1", "-y"], timeout=3600, env=setup_env)
    build_requirements = ROOT / "e4baseline/harbor_build_requirements.lock"
    if not build_requirements.is_file():
        raise SetupError(f"pinned Harbor build dependency lock is missing: {build_requirements}")
    _run([str(python), "-m", "pip", "install", "-r", str(build_requirements)],
         timeout=600, env=setup_env)
    requirements = ROOT / "e4baseline/harbor_requirements.lock"
    if not requirements.is_file():
        raise SetupError(f"pinned Harbor dependency lock is missing: {requirements}")
    _run([str(python), "-m", "pip", "install", "-r", str(requirements)],
         timeout=3600, env=setup_env)
    _run([str(python), "-m", "pip", "install", "--no-deps", "--no-build-isolation", str(source)],
         timeout=600, env=setup_env)
    actual_version = _run([str(python), "--version"], timeout=30)
    if actual_version != "Python 3.13.16":
        raise SetupError(f"Harbor runtime must use pinned Python 3.13.16, got {actual_version}")
    harbor_version = _run([str(python), "-c",
                           "from importlib.metadata import version; print(version('harbor'))"], timeout=30)
    if harbor_version != "0.23.0":
        raise SetupError(f"unexpected Harbor distribution version: {harbor_version}")
    _run([str(python), "-m", "pip", "check"], timeout=120)
    cache = runtime / "tokenizer-cache"
    cache.mkdir(parents=True, exist_ok=True)
    controller_home = runtime / "controller-home"
    controller_home.mkdir(parents=True, exist_ok=True)
    tokenizer_env = dict(os.environ)
    tokenizer_env.update({
        "CONDA_PKGS_DIRS": setup_env["CONDA_PKGS_DIRS"],
        "PIP_CACHE_DIR": setup_env["PIP_CACHE_DIR"],
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "HOME": str(controller_home),
        "CUSTOM_TIKTOKEN_CACHE_DIR": str(cache),
        "TIKTOKEN_CACHE_DIR": str(cache),
        "LITELLM_LOCAL_MODEL_COST_MAP": "True",
    })
    _run([str(python), "-c",
          "import tiktoken; enc=tiktoken.get_encoding('cl100k_base'); print('cl100k_base', enc.n_vocab)"],
         timeout=600, env=tokenizer_env)
    return python


def build_task_image(runtime: Path = RUNTIME) -> dict[str, Any]:
    dockerfile = ROOT / "e4baseline/terminus_task/Dockerfile"
    _run(["docker", "build", "--pull=false", "--file", str(dockerfile),
          "--tag", IMAGE_TAG, str(ROOT / "e4baseline")], timeout=3600)
    raw = _run(["docker", "image", "inspect", IMAGE_TAG], timeout=120)
    info = json.loads(raw)[0]
    image_id = info.get("Id")
    if not isinstance(image_id, str) or not image_id.startswith("sha256:"):
        raise SetupError("Docker did not return an immutable task image ID")
    python_version = _run(["docker", "run", "--rm", "--network", "none", image_id,
                           "python", "--version"], timeout=120)
    tmux = _run(["docker", "run", "--rm", "--network", "none", image_id,
                 "tmux", "-V"], timeout=120)
    asciinema = _run(["docker", "run", "--rm", "--network", "none", image_id,
                      "asciinema", "--version"], timeout=120)
    scientific = _run(["docker", "run", "--rm", "--network", "none", image_id,
                       "python", "-c",
                       "import numpy,pandas,scipy,sklearn; print(numpy.__version__,pandas.__version__,scipy.__version__,sklearn.__version__)"],
                      timeout=120)
    if (not python_version.startswith("Python 3.10.") or not tmux.startswith("tmux ") or
            not asciinema.startswith("asciinema ")):
        raise SetupError("task image failed Python 3.10, tmux, or asciinema preflight")
    archive = runtime / "terminus_task_image.tar"
    archive.parent.mkdir(parents=True, exist_ok=True)
    _run(["docker", "save", "--output", str(archive), image_id], timeout=1200)
    pin = {
        "schema_version": "e4-terminus2-task-image-pin/1",
        "terminus_task_image_ref": IMAGE_TAG,
        "terminus_task_image_digest": image_id,
        "terminus_task_image_id": image_id,
        "image_archive": str(archive.relative_to(ROOT)),
        "image_archive_sha256": sha256_file(archive),
        "base_image": BASE_IMAGE,
        "dockerfile_sha256": sha256_file(dockerfile),
        "python_version": python_version,
        "tmux_version": tmux,
        "asciinema_version": asciinema,
        "scientific_python_versions": scientific,
        "network_preflight": "docker run --network none",
    }
    pin_path = runtime / "terminus_task_image_pin.json"
    pin_path.write_text(json.dumps(pin, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return pin


def ensure_docker_compose_plugin(runtime: Path = RUNTIME) -> dict[str, Any]:
    plugin_dir = runtime / "docker-cli-plugins"
    plugin_dir.mkdir(parents=True, exist_ok=True)
    plugin = plugin_dir / "docker-compose"
    url = ("https://github.com/docker/compose/releases/download/"
           f"{DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64")
    if not plugin.is_file() or sha256_file(plugin) != DOCKER_COMPOSE_SHA256:
        temporary = plugin.with_suffix(".download")
        try:
            with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            raise SetupError(f"cannot fetch the pinned official Docker Compose plugin: {exc}") from exc
        actual = sha256_file(temporary)
        if actual != DOCKER_COMPOSE_SHA256:
            temporary.unlink(missing_ok=True)
            raise SetupError(f"Docker Compose plugin SHA-256 mismatch: {actual}")
        temporary.chmod(0o755)
        temporary.replace(plugin)
    buildx = plugin_dir / "docker-buildx"
    buildx_url = ("https://github.com/docker/buildx/releases/download/"
                  f"{DOCKER_BUILDX_VERSION}/buildx-{DOCKER_BUILDX_VERSION}.linux-amd64")
    if not buildx.is_file() or sha256_file(buildx) != DOCKER_BUILDX_SHA256:
        temporary = buildx.with_suffix(".download")
        try:
            with urllib.request.urlopen(buildx_url, timeout=60) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            raise SetupError(f"cannot fetch the pinned official Docker Buildx plugin: {exc}") from exc
        actual = sha256_file(temporary)
        if actual != DOCKER_BUILDX_SHA256:
            temporary.unlink(missing_ok=True)
            raise SetupError(f"Docker Buildx plugin SHA-256 mismatch: {actual}")
        temporary.chmod(0o755)
        temporary.replace(buildx)
    config_dir = runtime / "docker-config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps({"cliPluginsExtraDirs": [str(plugin_dir.resolve())]},
                                      sort_keys=True, indent=2) + "\n", encoding="utf-8")
    pin = {
        "schema_version": "e4-terminus2-docker-compose-pin/1",
        "repository": "https://github.com/docker/compose",
        "version": DOCKER_COMPOSE_VERSION,
        "asset": "docker-compose-linux-x86_64",
        "url": url,
        "sha256": sha256_file(plugin),
        "path": plugin.relative_to(ROOT).as_posix(),
        "docker_config_path": config_path.relative_to(ROOT).as_posix(),
        "docker_config_sha256": sha256_file(config_path),
        "buildx_repository": "https://github.com/docker/buildx",
        "buildx_version": DOCKER_BUILDX_VERSION,
        "buildx_asset": f"buildx-{DOCKER_BUILDX_VERSION}.linux-amd64",
        "buildx_url": buildx_url,
        "buildx_sha256": sha256_file(buildx),
        "buildx_path": buildx.relative_to(ROOT).as_posix(),
    }
    output_path = runtime / "docker_compose_plugin_pin.json"
    output_path.write_text(json.dumps(pin, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return pin


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=RUNTIME)
    parser.add_argument("--skip-python", action="store_true",
                        help="verify source and build the task image only")
    parser.add_argument("--skip-image", action="store_true",
                        help="prepare only Harbor source and isolated controller runtime")
    parser.add_argument("--skip-compose-plugin", action="store_true",
                        help="do not install the pinned Docker CLI Compose and Buildx plugins")
    args = parser.parse_args(argv)
    runtime = args.runtime_dir.resolve()
    try:
        source = harbor_source(runtime)
        revision = _run(["git", "rev-parse", "HEAD"], cwd=source, timeout=30)
        tree = _run(["git", "rev-parse", "HEAD^{tree}"], cwd=source, timeout=30)
        print(json.dumps({"harbor_revision": revision, "harbor_git_tree": tree,
                          "harbor_source": str(source)}, sort_keys=True))
        python = runtime / "harbor-python/bin/python"
        if not args.skip_python:
            python = ensure_python_runtime(source, runtime)
            print(f"Harbor Python runtime: {python}")
        else:
            python = python if python.is_file() else None
        source_pin = harbor_source_pin(source, runtime, python)
        print(json.dumps(source_pin, sort_keys=True, indent=2))
        if not args.skip_compose_plugin:
            print(json.dumps(ensure_docker_compose_plugin(runtime), sort_keys=True, indent=2))
        if not args.skip_image:
            pin = build_task_image(runtime)
            print(json.dumps(pin, sort_keys=True, indent=2))
    except SetupError as exc:
        parser.exit(2, f"setup_terminus: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
