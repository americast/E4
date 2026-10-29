"""M5/M6 frozen experiment manifest construction and scored-run validation."""

from __future__ import annotations

import hashlib
import json
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

from e4.dsl.cache import implementation_fingerprint
from e4.agent.library import load_library_snapshot
from e4.comparison.tasks import load_bank_case_view
from e4.paths import ROOT

SCHEMA = "e4-m5-frozen-manifest/1"
DEVELOPMENT_SCHEMA = "e4-m5-development-manifest/1"
_RUNTIME_PIN_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


class FrozenManifestError(ValueError):
    """A scored run does not match the internally frozen experiment."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _e4_runtime_pin() -> dict[str, Any]:
    if sys.version_info[:2] != (3, 10):
        raise FrozenManifestError("freeze and scored guard must run under the pinned E4 Python 3.10")
    freeze = subprocess.run([sys.executable, "-m", "pip", "freeze", "--all"],
                            capture_output=True, text=True, check=True).stdout
    return {"python_version": platform.python_version(),
            "pip_freeze_sha256": hashlib.sha256(freeze.encode()).hexdigest(),
            "pip_freeze": freeze.splitlines()}


def _llama_runtime_pin(root: Path) -> dict[str, Any]:
    launcher = (root / "src/e4/launch/llama_server.py").read_text(encoding="utf-8")
    revision_match = re.search(r'PINNED_LLAMA_CPP_REVISION\s*=\s*["\']([^"\']+)', launcher)
    server_match = re.search(r'DEFAULT_SERVER\s*=\s*["\']([^"\']+)', launcher)
    if not revision_match or not server_match:
        raise FrozenManifestError("M3 launcher has no pinned llama.cpp/server declaration")
    expected_revision, relative_server = revision_match.group(1), server_match.group(1)
    source_root = root / "third_party/llama.cpp"
    revision = subprocess.run(["git", "-C", str(source_root), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(source_root), "status", "--porcelain",
                            "--untracked-files=no"], capture_output=True, text=True,
                           check=True).stdout.strip()
    server = (root / relative_server).resolve(strict=True)
    if (revision != expected_revision or dirty or root not in server.parents or not server.is_file()):
        raise FrozenManifestError("M3 launcher runtime differs from the pinned source or server")
    return {"llama_cpp_revision": revision, "server_path": relative_server,
            "server_sha256": sha256_file(server), "server_size_bytes": server.stat().st_size}


def _terminus_python_pin(root: Path, source_pin: Mapping[str, Any]) -> dict[str, Any]:
    python_path = (root / "artifacts/m5/runtime/harbor-python/bin/python").resolve(strict=True)
    if root not in python_path.parents or not python_path.is_file():
        raise FrozenManifestError("pinned Terminus Python runtime is outside project artifacts")
    python_version = subprocess.run([str(python_path), "--version"], capture_output=True,
                                    text=True, check=True).stdout.strip()
    freeze = subprocess.run([str(python_path), "-m", "pip", "freeze", "--all"],
                            capture_output=True, text=True, check=True).stdout.rstrip("\n") + "\n"
    freeze_hash = hashlib.sha256(freeze.encode()).hexdigest()
    freeze_path = (root / str(source_pin.get("pip_freeze_path", ""))).resolve()
    distribution = subprocess.run([str(python_path), "-c",
        "from importlib.metadata import version; print(version('harbor'))"],
        capture_output=True, text=True, check=True).stdout.strip()
    if (not python_version.startswith("Python 3.13.") or
            python_version != source_pin.get("python_version") or
            distribution != source_pin.get("harbor_version") or
            root not in freeze_path.parents or not freeze_path.is_file() or
            sha256_file(freeze_path) != source_pin.get("pip_freeze_sha256") or
            freeze_hash != source_pin.get("pip_freeze_sha256")):
        raise FrozenManifestError("Harbor Python/dependency runtime differs from setup pin")
    return {"python_path": str(python_path), "python_version": python_version,
            "harbor_version": distribution, "pip_freeze_path": str(freeze_path),
            "pip_freeze_sha256": freeze_hash,
            "pip_freeze": freeze.splitlines()}


def _docker_compose_runtime_pin(root: Path) -> dict[str, Any]:
    pin_path = root / "artifacts/m5/runtime/docker_compose_plugin_pin.json"
    if pin_path.is_symlink():
        raise FrozenManifestError("Docker Compose pin metadata cannot be a symlink")
    pin = _json(pin_path)
    expected = {"schema_version": "e4-terminus2-docker-compose-pin/1",
                "repository": "https://github.com/docker/compose", "version": "v2.32.4",
                "asset": "docker-compose-linux-x86_64",
                "sha256": "ed1917fb54db184192ea9d0717bcd59e3662ea79db48bff36d3475516c480a6b",
                "path": "artifacts/m5/runtime/docker-cli-plugins/docker-compose",
                "docker_config_path": "artifacts/m5/runtime/docker-config/config.json",
                "docker_config_sha256": "b68fc8a961c4c5fde3746300b824119142c1a787105a3f57b5ee8479ee78b5aa"}
    if any(pin.get(key) != value for key, value in expected.items()):
        raise FrozenManifestError("Docker Compose plugin/config pin differs from the approved setup")
    binary_path = (root / pin["path"]).resolve(strict=True)
    config_path = (root / pin["docker_config_path"]).resolve(strict=True)
    if ((root / pin["path"]).is_symlink() or (root / pin["docker_config_path"]).is_symlink() or
            root not in binary_path.parents or root not in config_path.parents or
            sha256_file(binary_path) != pin["sha256"] or
            sha256_file(config_path) != pin["docker_config_sha256"]):
        raise FrozenManifestError("Docker Compose plugin or trusted Docker config bytes differ from pin")
    return {"pin_path": str(pin_path), "pin_file_sha256": sha256_file(pin_path),
            "plugin_path": str(binary_path), "plugin_sha256": pin["sha256"],
            "docker_config_path": str(config_path),
            "docker_config_sha256": pin["docker_config_sha256"], "version": pin["version"]}


def _library_pin(root: Path, expected_id: str) -> tuple[Path, str]:
    snapshot = load_library_snapshot(root)
    if snapshot.snapshot_id != expected_id:
        raise FrozenManifestError("initial E4 library CURRENT pointer differs from requested snapshot")
    if snapshot.document.get("schema_version") is None or not isinstance(snapshot.document.get("provenance"), Mapping):
        raise FrozenManifestError("initial E4 library did not pass trusted snapshot validation")
    return snapshot.path, sha256_file(snapshot.path)


def _manifest_files(root: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    excluded_parts = {".git", ".venv", "__pycache__", "artifacts", "models"}
    for dirname in ("src", "scripts", "config", "tests", "templates"):
        base = root / dirname
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if (not path.is_file() or path.suffix in {".pyc", ".pyo"} or
                    excluded_parts.intersection(path.parts)):
                continue
            files[path.relative_to(root).as_posix()] = sha256_file(path)
    for pattern in ("pyproject.toml", "requirements*.txt", "Dockerfile*", "*.lock",
                    "setup.py", "setup.cfg", "pytest.ini", "tox.ini", "MANIFEST.in"):
        for path in root.glob(pattern):
            if path.is_file():
                files[path.relative_to(root).as_posix()] = sha256_file(path)
    return files


def _validate_local_file_inventory(root: Path, expected_files: Mapping[str, str]) -> None:
    current = _manifest_files(root)
    if set(current) != set(expected_files):
        raise FrozenManifestError("current code/config/dependency inventory differs from frozen inventory")
    for relative, actual_hash in current.items():
        if actual_hash != expected_files[relative]:
            raise FrozenManifestError(f"current source inventory hash differs: {relative}")


def _validate_manifest_header(manifest: Mapping[str, Any], sample: Mapping[str, Any], *,
                              case_id: str, split: str) -> list[str]:
    if manifest.get("schema_version") != SCHEMA or manifest.get("status") != "frozen":
        raise FrozenManifestError("experiment manifest is not a frozen M5 manifest")
    if manifest.get("split") != split or manifest.get("seed") != 42:
        raise FrozenManifestError("experiment split or seed differs from the frozen protocol")
    expected = list(sample.get("selection", {}).get("scored_ids", [])) if split == "scored" else [
        row["incident_id"] for row in sample.get("development_cases", [])]
    if manifest.get("case_ids") != expected or case_id not in expected:
        raise FrozenManifestError("case is outside the fixed ordered experiment allowlist")
    if manifest.get("systems") != ["E4", "Terminus-2"] or manifest.get("external_baseline") != "Terminus-2":
        raise FrozenManifestError("frozen system/baseline set differs from the selected experiment")
    expected_order = {value: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
                      for index, value in enumerate(expected)}
    if manifest.get("paired_execution_order") != expected_order:
        raise FrozenManifestError("paired order differs from the fixed alternating schedule")
    return expected


def _validate_resource_budget(manifest: Mapping[str, Any], experiment: Mapping[str, Any]) -> None:
    budgets = manifest.get("resource_budgets")
    if budgets != experiment.get("resource_budgets") or not isinstance(budgets, Mapping):
        raise FrozenManifestError("frozen resource budgets differ from active experiment configuration")
    if budgets.get("status") != "frozen":
        raise FrozenManifestError("development-measured resource budgets must be frozen before M6")
    if (type(budgets.get("case_wall_seconds")) is not int or budgets["case_wall_seconds"] <= 0 or
            type(budgets.get("terminus_max_turns")) is not int or budgets["terminus_max_turns"] <= 0 or
            type(budgets.get("aggregate_generated_tokens")) is not int or
            budgets["aggregate_generated_tokens"] <= 0):
        raise FrozenManifestError("frozen wall/turn/token budgets are malformed")


def _validate_development_budget(manifest: Mapping[str, Any], experiment: Mapping[str, Any]) -> None:
    budgets = manifest.get("resource_budgets")
    if budgets != experiment.get("resource_budgets") or not isinstance(budgets, Mapping):
        raise FrozenManifestError("development resource budgets differ from active experiment configuration")
    if budgets.get("status") not in {
            "development_pending_measurement", "development_pending_freeze",
            "must_be_measured_on_both_development_cases_before_scored_run"}:
        raise FrozenManifestError("development manifest must preserve the pending-measurement budget status")
    if (type(budgets.get("case_wall_seconds")) is not int or budgets["case_wall_seconds"] <= 0 or
            type(budgets.get("aggregate_generated_tokens")) is not int or
            budgets["aggregate_generated_tokens"] <= 0 or
            (budgets.get("terminus_max_turns") is not None and
             (type(budgets["terminus_max_turns"]) is not int or budgets["terminus_max_turns"] <= 0))):
        raise FrozenManifestError("development wall/token/turn budgets are malformed")


def _bootstrap_host_library_path(root: Path, bootstrap_result_path: Path,
                                bootstrap_result: Mapping[str, Any]) -> Path:
    """Resolve the preserved host snapshot beside a result that may name a bwrap path."""
    explicit_host = bootstrap_result.get("host_library_path")
    if isinstance(explicit_host, str) and explicit_host:
        candidate = Path(explicit_host)
        if not candidate.is_absolute():
            candidate = root / candidate
    else:
        reported = Path(str(bootstrap_result.get("library_path", "")))
        reported_candidate = reported if reported.is_absolute() else root / reported
        if root in reported_candidate.resolve().parents and reported_candidate.exists():
            candidate = reported_candidate
        else:
            # The M3 launcher preserves the controller-visible library adjacent to
            # bootstrap_result.json; its result may still contain the in-namespace
            # path, such as /m4run/bootstrap/library.
            candidate = Path(bootstrap_result_path).parent / "library"
    candidate = candidate.resolve(strict=True)
    if root not in candidate.parents or not candidate.is_dir():
        raise FrozenManifestError("bootstrap host library must be a project artifact directory")
    return candidate


def build_development_manifest(*, root: Path = ROOT, run_id: str,
                               initial_library_path: Path,
                               bootstrap_result_path: Path,
                               harbor_source_pin_path: Path,
                               terminus_image_pin_path: Path,
                               output_path: Path) -> dict[str, Any]:
    """Write a provisional two-case development pin without reading any gold labels."""
    root = Path(root).resolve()
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", run_id):
        raise FrozenManifestError("run_id contains invalid characters")
    sample = _json(root / "config/bank/sample.json")
    case_ids = list(sample.get("selection", {}).get("development_ids", []))
    if not case_ids:
        case_ids = [row["incident_id"] for row in sample.get("development_cases", [])]
    if len(case_ids) != 2 or len(set(case_ids)) != 2:
        raise FrozenManifestError("M5 development readiness requires exactly two fixed development cases")
    harbor_source_pin_path = Path(harbor_source_pin_path).resolve(strict=True)
    terminus_image_pin_path = Path(terminus_image_pin_path).resolve(strict=True)
    if root not in harbor_source_pin_path.parents or root not in terminus_image_pin_path.parents:
        raise FrozenManifestError("Terminus setup pin artifacts must be preserved project artifacts")
    source_pin = _json(harbor_source_pin_path)
    image_pin = _json(terminus_image_pin_path)
    if source_pin.get("harbor_revision") != "26106c6d9c66848a1440faf5724d336affac52aa":
        raise FrozenManifestError("development manifest requires the pinned Harbor source revision")
    source_root = (root / str(source_pin.get("harbor_source_path", ""))).resolve(strict=True)
    if root not in source_root.parents or not source_root.is_dir():
        raise FrozenManifestError("pinned Harbor source checkout is unavailable")
    revision = subprocess.run(["git", "-C", str(source_root), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    tree = subprocess.run(["git", "-C", str(source_root), "rev-parse", "HEAD^{tree}"],
                          capture_output=True, text=True, check=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(source_root), "status", "--porcelain",
                            "--untracked-files=all"], capture_output=True, text=True,
                           check=True).stdout.strip()
    if revision != source_pin.get("harbor_revision") or tree != source_pin.get("harbor_git_tree") or dirty:
        raise FrozenManifestError("Harbor checkout differs from the setup-generated source pin")
    image_archive = (root / str(image_pin.get("image_archive", ""))).resolve(strict=True)
    if (root not in image_archive.parents or
            sha256_file(image_archive) != image_pin.get("image_archive_sha256") or
            image_pin.get("terminus_task_image_id") != image_pin.get("terminus_task_image_digest")):
        raise FrozenManifestError("Terminus setup image pin or archive is unavailable/mismatched")
    initial_library_path = Path(initial_library_path).resolve(strict=True)
    if root not in initial_library_path.parents:
        raise FrozenManifestError("development E4 library must be a project artifact")
    bootstrap_result_path = Path(bootstrap_result_path).resolve(strict=True)
    if root not in bootstrap_result_path.parents or not bootstrap_result_path.is_file():
        raise FrozenManifestError("development bootstrap result must be a preserved project artifact")
    bootstrap_result = _json(bootstrap_result_path)
    if bootstrap_result.get("status") not in {"complete_seed_only", "complete_with_additions"}:
        raise FrozenManifestError("M5 development requires a completed fresh bootstrap result")
    snapshot = load_library_snapshot(initial_library_path)
    bootstrap_library = _bootstrap_host_library_path(root, bootstrap_result_path, bootstrap_result)
    if (bootstrap_library != initial_library_path or
            bootstrap_result.get("library_snapshot_id") != snapshot.snapshot_id or
            bootstrap_result.get("library_snapshot_sha256") != sha256_file(snapshot.path)):
        raise FrozenManifestError("development library does not match its fresh M5 bootstrap result")
    _validate_pristine_bootstrap(snapshot,
        implementation_fp=implementation_fingerprint(),
        profile_sha256=sha256_file(root / "config/models/gemma.json"),
        runtime_policy_sha256=sha256_file(root / "config/bank/controller_policy.json"),
        metric_policy_sha256=sha256_file(root / "config/bank/metric_kind_policy.json"),
        kpi_policy_sha256=sha256_file(root / "config/bank/kpi_policy.json"),
        seed_library_sha256=sha256_file(root / "config/bank/seed_playbooks.json"))
    library_sha = sha256_file(snapshot.path)
    local_files = _manifest_files(root)
    public_inputs: dict[str, Any] = {}
    for case_id in case_ids:
        view = load_bank_case_view(case_id, split="dev", root=root)
        public_inputs[case_id] = {
            "public_manifest_sha256": view.public_manifest_sha256,
            "input_manifest_sha256": view.input_manifest_sha256,
            "alert_sha256": view.alert_sha256,
            "clip_sha256": dict(view.telemetry_file_sha256),
        }
    experiment = _json(root / "config/comparison/experiment.json")
    _validate_development_budget({"resource_budgets": experiment.get("resource_budgets")}, experiment)
    paired_order = {case_id: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
                    for index, case_id in enumerate(case_ids)}
    manifest = {
        "schema_version": DEVELOPMENT_SCHEMA,
        "status": "development_pending_validation",
        "run_id": run_id,
        "dataset": "OpenRCA Bank",
        "split": "dev",
        "seed": 42,
        "case_ids": case_ids,
        "paired_execution_order": paired_order,
        "systems": ["E4", "Terminus-2"],
        "external_baseline": "Terminus-2",
        "local_pins": {"files": local_files,
            "experiment_config_sha256": sha256_file(root / "config/comparison/experiment.json"),
            "initial_library_path": str(initial_library_path),
            "initial_library_snapshot_id": snapshot.snapshot_id,
            "initial_library_manifest_sha256": library_sha,
            "bootstrap_result_path": str(bootstrap_result_path),
            "bootstrap_result_sha256": sha256_file(bootstrap_result_path),
            "bootstrap_reported_library_path": bootstrap_result.get("library_path"),
            "bootstrap_host_library_path": str(bootstrap_library)},
        "bootstrap_cost": bootstrap_result.get("cost", bootstrap_result.get("model_cost", {})),
        "terminus_setup_pins": {
            "harbor_source_pin_path": str(harbor_source_pin_path),
            "harbor_source_pin_sha256": sha256_file(harbor_source_pin_path),
            "terminus_image_pin_path": str(terminus_image_pin_path),
            "terminus_image_pin_sha256": sha256_file(terminus_image_pin_path),
            "terminus_task_image_id": image_pin["terminus_task_image_id"]},
        "resource_budgets": experiment["resource_budgets"],
        "public_inputs": public_inputs,
        "limitations": ["provisional development pin; resource budgets remain pending measurement",
                        "development outputs are readiness evidence, not scored results"]}
    output_path = Path(output_path)
    expected_root = root / "artifacts/m5/experiments" / run_id
    if output_path.resolve().parent != expected_root.resolve() or output_path.name != "development_manifest.json":
        raise FrozenManifestError("development manifest path must be artifacts/m5/experiments/<run_id>/development_manifest.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    return manifest


def validate_development_manifest(path: Path, *, root: Path = ROOT) -> dict[str, Any]:
    """Validate the provisional fixed-dev pin; this function never opens evaluator labels."""
    root = Path(root).resolve()
    raw_path = Path(path)
    if raw_path.is_symlink():
        raise FrozenManifestError("development manifest cannot be a symlink")
    manifest_path = raw_path.resolve(strict=True)
    expected_root = (root / "artifacts/m5/experiments").resolve()
    if not manifest_path.is_relative_to(expected_root) or manifest_path.name != "development_manifest.json":
        raise FrozenManifestError("development manifest must be under artifacts/m5/experiments")
    for parent in (manifest_path, *manifest_path.parents):
        if parent.is_symlink():
            raise FrozenManifestError("development manifest path cannot traverse symlinks")
        if parent == expected_root:
            break
    manifest = _json(manifest_path)
    sample = _json(root / "config/bank/sample.json")
    expected_ids = list(sample.get("selection", {}).get("development_ids", []))
    if not expected_ids:
        expected_ids = [row["incident_id"] for row in sample.get("development_cases", [])]
    order = {case_id: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
             for index, case_id in enumerate(expected_ids)}
    if (manifest.get("schema_version") != DEVELOPMENT_SCHEMA or
            manifest.get("status") != "development_pending_validation" or
            manifest.get("split") != "dev" or manifest.get("seed") != 42 or
            manifest.get("case_ids") != expected_ids or len(expected_ids) != 2 or
            manifest.get("systems") != ["E4", "Terminus-2"] or
            manifest.get("external_baseline") != "Terminus-2" or
            manifest.get("paired_execution_order") != order):
        raise FrozenManifestError("development manifest differs from the fixed two-case paired protocol")
    if set(manifest.get("public_inputs", {})) != set(expected_ids):
        raise FrozenManifestError("development public-input pins do not exactly match the fixed cases")
    pins = manifest.get("local_pins", {})
    files = pins.get("files")
    if not isinstance(files, Mapping):
        raise FrozenManifestError("development manifest has no local source pins")
    _validate_local_file_inventory(root, files)
    for relative, expected in files.items():
        target = (root / relative).resolve()
        if root not in target.parents or not target.is_file() or sha256_file(target) != expected:
            raise FrozenManifestError(f"development source pin mismatch: {relative}")
    experiment = _json(root / "config/comparison/experiment.json")
    if pins.get("experiment_config_sha256") != sha256_file(root / "config/comparison/experiment.json"):
        raise FrozenManifestError("development experiment settings changed since the provisional pin")
    _validate_development_budget(manifest, experiment)
    raw_initial_path = Path(pins.get("initial_library_path", ""))
    if raw_initial_path.is_symlink():
        raise FrozenManifestError("development initial library cannot be a symlink")
    initial_path = raw_initial_path.resolve(strict=True)
    if root not in initial_path.parents:
        raise FrozenManifestError("development initial library escaped project artifacts")
    snapshot = load_library_snapshot(initial_path)
    if (snapshot.snapshot_id != pins.get("initial_library_snapshot_id") or
            sha256_file(snapshot.path) != pins.get("initial_library_manifest_sha256")):
        raise FrozenManifestError("development initial E4 library differs from its provisional pin")
    bootstrap_path = Path(pins.get("bootstrap_result_path", ""))
    if bootstrap_path.is_symlink():
        raise FrozenManifestError("development bootstrap result cannot be a symlink")
    bootstrap_path = bootstrap_path.resolve(strict=True)
    if (root not in bootstrap_path.parents or not bootstrap_path.is_file() or
            sha256_file(bootstrap_path) != pins.get("bootstrap_result_sha256")):
        raise FrozenManifestError("development bootstrap result differs from its provisional pin")
    bootstrap_result = _json(bootstrap_path)
    bootstrap_library = _bootstrap_host_library_path(root, bootstrap_path, bootstrap_result)
    if (bootstrap_result.get("status") not in {"complete_seed_only", "complete_with_additions"} or
            bootstrap_library != initial_path or
            bootstrap_result.get("library_snapshot_id") != snapshot.snapshot_id or
            bootstrap_result.get("library_snapshot_sha256") != sha256_file(snapshot.path)):
        raise FrozenManifestError("development library is no longer the pinned bootstrap output")
    _validate_pristine_bootstrap(snapshot,
        implementation_fp=implementation_fingerprint(),
        profile_sha256=sha256_file(root / "config/models/gemma.json"),
        runtime_policy_sha256=sha256_file(root / "config/bank/controller_policy.json"),
        metric_policy_sha256=sha256_file(root / "config/bank/metric_kind_policy.json"),
        kpi_policy_sha256=sha256_file(root / "config/bank/kpi_policy.json"),
        seed_library_sha256=sha256_file(root / "config/bank/seed_playbooks.json"))
    setup = manifest.get("terminus_setup_pins", {})
    for key in ("harbor_source_pin_path", "terminus_image_pin_path"):
        pin_path = Path(setup.get(key, "")).resolve(strict=True)
        if root not in pin_path.parents:
            raise FrozenManifestError("Terminus setup pin escaped project artifacts")
        hash_key = "harbor_source_pin_sha256" if key == "harbor_source_pin_path" else "terminus_image_pin_sha256"
        if sha256_file(pin_path) != setup.get(hash_key):
            raise FrozenManifestError("Terminus setup pin changed since the provisional development freeze")
    source_pin = _json(Path(setup["harbor_source_pin_path"]))
    image_pin = _json(Path(setup["terminus_image_pin_path"]))
    if (source_pin.get("harbor_revision") != "26106c6d9c66848a1440faf5724d336affac52aa" or
            image_pin.get("terminus_task_image_id") != setup.get("terminus_task_image_id")):
        raise FrozenManifestError("Terminus setup pins do not match provisional development manifest")
    source_root = (root / str(source_pin.get("harbor_source_path", ""))).resolve(strict=True)
    if root not in source_root.parents:
        raise FrozenManifestError("pinned Harbor source path escaped project artifacts")
    upstream_files: dict[str, str] = {}
    for source_file in sorted(source_root.rglob("*")):
        if (not source_file.is_file() or ".git" in source_file.parts or
                "__pycache__" in source_file.parts or source_file.suffix in {".pyc", ".pyo"}):
            continue
        upstream_files[source_file.relative_to(source_root).as_posix()] = sha256_file(source_file)
    upstream_tree_hash = hashlib.sha256(json.dumps(upstream_files, sort_keys=True,
        separators=(",", ":")).encode()).hexdigest()
    revision = subprocess.run(["git", "-C", str(source_root), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    git_tree = subprocess.run(["git", "-C", str(source_root), "rev-parse", "HEAD^{tree}"],
                              capture_output=True, text=True, check=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(source_root), "status", "--porcelain",
                            "--untracked-files=all"], capture_output=True, text=True,
                           check=True).stdout.strip()
    if (revision != source_pin.get("harbor_revision") or
            git_tree != source_pin.get("harbor_git_tree") or dirty or
            upstream_tree_hash != source_pin.get("harbor_source_tree_sha256")):
        raise FrozenManifestError("Harbor source tree differs from its setup-generated pin")
    _terminus_python_pin(root, source_pin)
    _docker_compose_runtime_pin(root)
    archive_path = (root / str(image_pin.get("image_archive", ""))).resolve(strict=True)
    if (root not in archive_path.parents or
            sha256_file(archive_path) != image_pin.get("image_archive_sha256") or
            sha256_file(root / "src/e4/baselines/terminus/task/Dockerfile") != image_pin.get("dockerfile_sha256")):
        raise FrozenManifestError("Terminus task image archive or Dockerfile differs from its setup pin")
    image_id = image_pin.get("terminus_task_image_id")
    try:
        actual_image = subprocess.run(["docker", "image", "inspect", str(image_id),
            "--format", "{{.Id}}"], capture_output=True, text=True, timeout=30, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise FrozenManifestError("cannot verify the pinned Terminus task image before public-view loading") from exc
    if actual_image != image_id:
        raise FrozenManifestError("local Terminus task image differs from the provisional setup pin")
    for case_id in expected_ids:
        frozen = manifest.get("public_inputs", {}).get(case_id)
        view = load_bank_case_view(case_id, split="dev", root=root)
        _validate_public_view_pin(case_id, view, frozen)
    return manifest


def _validate_public_view_pin(case_id: str, view: Any,
                              frozen_input: Mapping[str, Any]) -> None:
    if (view.case_id != case_id or view.public_manifest_sha256 != frozen_input.get("public_manifest_sha256") or
            view.input_manifest_sha256 != frozen_input.get("input_manifest_sha256") or
            view.alert_sha256 != frozen_input.get("alert_sha256") or
            dict(view.telemetry_file_sha256) != frozen_input.get("clip_sha256")):
        raise FrozenManifestError("selected scored public input differs from freeze")


def _validate_pristine_bootstrap(snapshot: Any, *, implementation_fp: str,
                                 profile_sha256: str, runtime_policy_sha256: str,
                                 metric_policy_sha256: str, kpi_policy_sha256: str,
                                 seed_library_sha256: str) -> None:
    if snapshot.parent_snapshot_id is not None:
        raise FrozenManifestError("initial E4 library must be the pristine schema-bootstrap output")
    provenance = snapshot.document.get("provenance", {})
    expected = {
        "bank_input_manifest_used": False,
        "implementation_fingerprint": implementation_fp,
        "profile_sha256": profile_sha256,
        "m4_runtime_policy_sha256": runtime_policy_sha256,
        "bank_metric_policy_sha256": metric_policy_sha256,
        "bank_kpi_policy_sha256": kpi_policy_sha256,
        "bank_seed_library_sha256": seed_library_sha256,
    }
    if any(provenance.get(key) != value for key, value in expected.items()):
        raise FrozenManifestError("initial E4 bootstrap provenance differs from frozen source/config")
    lineage = snapshot.document.get("lineage", ())
    if (len(lineage) != 1 or lineage[0].get("event") != "snapshot_created" or
            lineage[0].get("parent_snapshot_id") is not None):
        raise FrozenManifestError("initial E4 library contains non-bootstrap lineage events")
    context_hash = provenance.get("schema_context_sha256")
    if not isinstance(context_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", context_hash):
        raise FrozenManifestError("initial E4 library lacks a valid schema-only context pin")
    for section in ("operators", "playbooks"):
        entries = snapshot.document.get(section, ())
        if any(entry.get("invoked_case_ids") for entry in entries):
            raise FrozenManifestError("initial E4 bootstrap library contains case-use history")


def build_frozen_manifest(*, root: Path = ROOT, run_id: str,
                          terminus_upstream_pin: Mapping[str, Any],
                          terminus_image_pin: Mapping[str, Any],
                          terminus_source_path: Path,
                          terminus_image_pin_path: Path,
                          initial_library_path: Path | None,
                          initial_library_snapshot_id: str | None,
                          bootstrap_result_path: Path,
                          output_path: Path) -> dict[str, Any]:
    """Capture the M6 freeze after development review and the primary's explicit M6 go.

    The function validates selected public clips and records a digest of the trusted truth
    file without parsing its labels. Only the later trusted evaluator opens the label rows.
    """
    root = Path(root).resolve()
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", run_id):
        raise FrozenManifestError("run_id contains invalid characters")
    sample_path = root / "config/bank/sample.json"
    sample = _json(sample_path)
    case_ids = list(sample.get("selection", {}).get("scored_ids", []))
    if len(case_ids) != 10 or len(set(case_ids)) != 10:
        raise FrozenManifestError("frozen Bank sample must contain its ten fixed scored IDs")
    bootstrap_result_path = Path(bootstrap_result_path).resolve(strict=True)
    if root not in bootstrap_result_path.parents or not bootstrap_result_path.is_file():
        raise FrozenManifestError("initial E4 bootstrap result must be a preserved project artifact")
    bootstrap_result = _json(bootstrap_result_path)
    if bootstrap_result.get("status") not in {"complete_seed_only", "complete_with_additions"}:
        raise FrozenManifestError("initial E4 bootstrap result is not complete")
    bootstrap_library = _bootstrap_host_library_path(root, bootstrap_result_path, bootstrap_result)
    bootstrap_snapshot_id = bootstrap_result.get("library_snapshot_id")
    if not initial_library_path:
        initial_library_path = bootstrap_library
    if not initial_library_snapshot_id:
        initial_library_snapshot_id = bootstrap_snapshot_id
    if (Path(initial_library_path).resolve() != bootstrap_library or
            initial_library_snapshot_id != bootstrap_snapshot_id):
        raise FrozenManifestError("requested initial library differs from bootstrap output")
    source_root = Path(initial_library_path).resolve()
    if root not in source_root.parents or not source_root.is_dir():
        raise FrozenManifestError("initial E4 library snapshot directory is missing")
    initial_snapshot = load_library_snapshot(source_root)
    _validate_pristine_bootstrap(initial_snapshot,
        implementation_fp=implementation_fingerprint(),
        profile_sha256=sha256_file(root / "config/models/gemma.json"),
        runtime_policy_sha256=sha256_file(root / "config/bank/controller_policy.json"),
        metric_policy_sha256=sha256_file(root / "config/bank/metric_kind_policy.json"),
        kpi_policy_sha256=sha256_file(root / "config/bank/kpi_policy.json"),
        seed_library_sha256=sha256_file(root / "config/bank/seed_playbooks.json"))
    library_manifest, library_hash = _library_pin(source_root, initial_library_snapshot_id)
    if bootstrap_result.get("library_snapshot_sha256") != library_hash:
        raise FrozenManifestError("bootstrap result snapshot hash differs from its immutable library snapshot")
    raw_bootstrap_cost = bootstrap_result.get("cost", bootstrap_result.get("model_cost", {}))
    if not isinstance(raw_bootstrap_cost, Mapping):
        raw_bootstrap_cost = {}
    bootstrap_cost = {
        "model_calls": raw_bootstrap_cost.get("model_calls", raw_bootstrap_cost.get("call_count",
                              bootstrap_result.get("model_call_count"))),
        "prompt_tokens": raw_bootstrap_cost.get("prompt_tokens"),
        "completion_tokens": raw_bootstrap_cost.get("completion_tokens"),
        "model_seconds": raw_bootstrap_cost.get("model_seconds", raw_bootstrap_cost.get("elapsed_seconds")),
        "cpu_seconds": raw_bootstrap_cost.get("cpu_seconds"),
        "load_seconds": raw_bootstrap_cost.get("load_seconds"),
        "startup_seconds": raw_bootstrap_cost.get("startup_seconds"),
        "wall_seconds": raw_bootstrap_cost.get("wall_seconds", bootstrap_result.get("wall_seconds")),
        "logical_bytes_scanned": raw_bootstrap_cost.get("logical_bytes_scanned"),
        "physical_bytes_read": raw_bootstrap_cost.get("physical_bytes_read"),
        "returned_bytes": raw_bootstrap_cost.get("returned_bytes"),
    }
    profile_path = root / "config/models/gemma.json"
    policy_path = root / "config/bank/controller_policy.json"
    experiment_path = root / "config/comparison/experiment.json"
    model_manifest_path = root / "config/models/gemma_manifest.json"
    pinned_paths = (profile_path, policy_path, experiment_path, model_manifest_path,
                    root / "config/bank/sample.json", root / "config/bank/input_manifest.json",
                    root / "config/bank/kpi_policy.json", root / "config/bank/metric_kind_policy.json",
                    library_manifest)
    missing = [str(path) for path in pinned_paths if not path.is_file()]
    if missing:
        raise FrozenManifestError(f"missing required freeze pin(s): {missing}")
    model_doc = _json(model_manifest_path)
    model_relative = Path("data/models") / str(model_doc.get("filename", ""))
    model_checkpoint = (root / model_relative).resolve(strict=True)
    profile_doc = _json(profile_path)
    declared_model = Path(str(profile_doc.get("local_server", {}).get("model_path", "")))
    if declared_model.is_absolute() or declared_model.parts[:2] != ("data", "models") or declared_model.name != model_doc.get("filename"):
        raise FrozenManifestError("Gemma profile checkpoint path differs from the pinned model manifest")
    if root not in model_checkpoint.parents or not model_checkpoint.is_file():
        raise FrozenManifestError("pinned Gemma checkpoint path is invalid")
    if (model_checkpoint.stat().st_size != model_doc.get("size_bytes") or
            sha256_file(model_checkpoint) != model_doc.get("sha256")):
        raise FrozenManifestError("Gemma checkpoint bytes differ from the frozen model manifest")
    local_files = _manifest_files(root)
    local_files["config/models/gemma.json"] = sha256_file(profile_path)
    local_files["config/bank/controller_policy.json"] = sha256_file(policy_path)
    local_files["config/comparison/experiment.json"] = sha256_file(experiment_path)
    local_files["config/models/gemma_manifest.json"] = sha256_file(model_manifest_path)
    local_files["config/bank/sample.json"] = sha256_file(sample_path)
    local_files["config/bank/input_manifest.json"] = sha256_file(root / "config/bank/input_manifest.json")
    local_files["config/bank/kpi_policy.json"] = sha256_file(root / "config/bank/kpi_policy.json")
    local_files["config/bank/metric_kind_policy.json"] = sha256_file(root / "config/bank/metric_kind_policy.json")
    current_source_inventory = _manifest_files(root)
    if set(local_files) != set(current_source_inventory):
        raise FrozenManifestError("source inventory changed while the freeze manifest was being assembled")
    terminus_source_path = Path(terminus_source_path).resolve(strict=True)
    upstream_files: dict[str, str] = {}
    for source_file in sorted(terminus_source_path.rglob("*")):
        if (not source_file.is_file() or ".git" in source_file.parts or
                "__pycache__" in source_file.parts or source_file.suffix in {".pyc", ".pyo"}):
            continue
        upstream_files[source_file.relative_to(terminus_source_path).as_posix()] = sha256_file(source_file)
    git_revision = subprocess.run(["git", "-C", str(terminus_source_path), "rev-parse", "HEAD"],
                                  capture_output=True, text=True, check=True).stdout.strip()
    git_tree = subprocess.run(["git", "-C", str(terminus_source_path), "rev-parse", "HEAD^{tree}"],
                              capture_output=True, text=True, check=True).stdout.strip()
    git_dirty = subprocess.run(["git", "-C", str(terminus_source_path), "status", "--porcelain"],
                               capture_output=True, text=True, check=True).stdout.strip()
    if git_revision != terminus_upstream_pin.get("harbor_revision") or git_dirty:
        raise FrozenManifestError("Harbor source checkout revision or cleanliness differs from its pin")
    if (terminus_upstream_pin.get("harbor_git_tree") != git_tree or
            str(Path(terminus_upstream_pin.get("harbor_source_path", terminus_source_path)).resolve()) !=
            str(terminus_source_path)):
        raise FrozenManifestError("Harbor source path/tree differs from setup-generated source pin")
    agent_relative = terminus_upstream_pin.get(
        "harbor_agent_path", "src/harbor/agents/terminus_2/terminus_2.py")
    if (not isinstance(agent_relative, str) or
            upstream_files.get(agent_relative) != terminus_upstream_pin.get("harbor_agent_sha256")):
        raise FrozenManifestError("Harbor Terminus-2 entry source hash differs from its pin")
    upstream_tree_hash = hashlib.sha256(json.dumps(upstream_files, sort_keys=True,
                                                   separators=(",", ":")).encode()).hexdigest()
    if terminus_upstream_pin.get("harbor_source_tree_sha256") != upstream_tree_hash:
        raise FrozenManifestError("Harbor full-source inventory differs from setup pin")
    upstream_pin = dict(terminus_upstream_pin)
    if sha256_file(root / "src/e4/baselines/terminus/requirements.lock") != upstream_pin.get("dependency_lock_sha256"):
        raise FrozenManifestError("Harbor dependency lock differs from setup-generated runtime pin")
    terminus_image_pin_path = Path(terminus_image_pin_path).resolve(strict=True)
    if root not in terminus_image_pin_path.parents:
        raise FrozenManifestError("Terminus image pin must be a preserved project artifact")
    image_pin_doc = _json(terminus_image_pin_path)
    if image_pin_doc != dict(terminus_image_pin):
        raise FrozenManifestError("Terminus image pin file differs from supplied image pin")
    archive_relative = image_pin_doc.get("image_archive")
    archive_path = (root / archive_relative).resolve() if isinstance(archive_relative, str) else None
    if (archive_path is None or root not in archive_path.parents or not archive_path.is_file() or
            sha256_file(archive_path) != image_pin_doc.get("image_archive_sha256")):
        raise FrozenManifestError("Terminus exported image archive does not match its hash pin")
    public_inputs: dict[str, Any] = {}
    for case_id in case_ids:
        view = load_bank_case_view(case_id, split="scored", root=root)
        public_inputs[case_id] = {
            "public_manifest_sha256": view.public_manifest_sha256,
            "input_manifest_sha256": view.input_manifest_sha256,
            "alert_sha256": view.alert_sha256,
            "clip_sha256": dict(view.telemetry_file_sha256),
            "alert_window_start": view.alert_window_start,
            "alert_window_end": view.alert_window_end,
        }
    profile = _json(profile_path)
    experiment = _json(experiment_path)
    if experiment.get("scored_seed") != 42 or experiment.get("scored_passes") != 1:
        raise FrozenManifestError("M6 freeze requires exactly one seed-42 scored pass")
    profile_contract = experiment.get("model_profile", {})
    profile_fields = {"context_tokens": "context_tokens_per_call",
                      "max_generation_tokens": "max_generation_tokens_per_call",
                      "reasoning_budget": "reasoning_budget_per_call",
                      "temperature": "temperature", "top_p": "top_p", "top_k": "top_k",
                      "seed": "seed", "reasoning_mode": "reasoning_mode"}
    # the reasoning budget is a server setting, kept in the model file's local_server block
    profile_values = {**profile, "reasoning_budget": profile.get("local_server", {}).get("reasoning_budget")}
    if any(profile_values.get(field) != profile_contract.get(contract_field)
           for field, contract_field in profile_fields.items()):
        raise FrozenManifestError("Gemma per-call profile differs from frozen M5 settings")
    if (experiment.get("max_diagnosis_submissions") != 3 or
            experiment.get("stop_on_acceptance") is not True or
            experiment.get("acceptance") != "exact component identity in ranked positions 1-3"):
        raise FrozenManifestError("M5 diagnosis opportunity or acceptance contract changed")
    paired_order = {case_id: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
                    for index, case_id in enumerate(case_ids)}
    truth_path = root / "data/prepared/bank/evaluator/scored_ground_truth.jsonl"
    if truth_path.is_symlink() or truth_path.resolve() != truth_path.absolute() or not truth_path.is_file():
        raise FrozenManifestError("fixed scored truth pin path must be a regular project file")
    manifest = {
        "schema_version": SCHEMA, "status": "frozen", "run_id": run_id,
        "dataset": "OpenRCA Bank", "split": "scored", "seed": 42,
        "case_ids": case_ids, "paired_execution_order": paired_order,
        "systems": ["E4", "Terminus-2"], "external_baseline": "Terminus-2",
        "local_pins": {
            "files": local_files,
            "e4_implementation_fingerprint": implementation_fingerprint(),
            "m5_e4_wrapper_sha256": local_files.get("src/e4/comparison/e4_runs.py"),
            "m5_e4_runner_sha256": local_files.get("scripts/comparison/e4_case.py"),
            "model_profile_sha256": sha256_file(profile_path),
            "runtime_policy_sha256": sha256_file(policy_path),
            "experiment_config_sha256": sha256_file(experiment_path),
            "sample_config_sha256": sha256_file(sample_path),
            "initial_library_snapshot_id": initial_library_snapshot_id,
            "initial_library_path": str(source_root),
            "initial_library_manifest_sha256": library_hash,
            "bootstrap_result_path": str(bootstrap_result_path),
            "bootstrap_result_sha256": sha256_file(bootstrap_result_path),
        },
        "bootstrap_cost": bootstrap_cost,
        "model": {"profile": profile, "model_manifest_sha256": sha256_file(model_manifest_path),
                  "checkpoint_path": model_relative.as_posix(),
                  "checkpoint_sha256": model_doc["sha256"],
                  "checkpoint_size_bytes": model_doc["size_bytes"]},
        "e4_runtime": _e4_runtime_pin(),
        "model_server_runtime": _llama_runtime_pin(root),
        "resource_budgets": experiment.get("resource_budgets"),
        "terminus_upstream_pin": upstream_pin,
        "terminus_runtime": _terminus_python_pin(root, upstream_pin),
        "docker_compose_runtime": _docker_compose_runtime_pin(root),
        "terminus_upstream_source": {"path": str(terminus_source_path),
                                     "files": upstream_files,
                                     "tree_sha256": upstream_tree_hash},
        "terminus_image_pin": {**dict(terminus_image_pin),
                                "pin_file": str(terminus_image_pin_path),
                                "pin_file_sha256": sha256_file(terminus_image_pin_path)},
        "public_inputs": public_inputs,
        "trusted_truth_pin": {
            "path": "data/prepared/bank/evaluator/scored_ground_truth.jsonl",
            "sha256": sha256_file(truth_path),
            "access": "trusted_evaluator_after_m6_go_only"
        },
        "limitations": ["Development resource budgets must be marked frozen before this manifest is used.",
                        "Terminus-2 turn limits are architecture-specific; count all model generations."]
    }
    if manifest["local_pins"]["m5_e4_wrapper_sha256"] is None or manifest["local_pins"]["m5_e4_runner_sha256"] is None:
        raise FrozenManifestError("M5 E4 wrapper and runner must exist before freeze")
    _validate_resource_budget(manifest, experiment)
    image_digest = terminus_image_pin.get("terminus_task_image_id")
    if (not terminus_upstream_pin.get("harbor_revision") or
            not terminus_upstream_pin.get("harbor_agent_sha256") or
            not isinstance(image_digest, str) or not image_digest.startswith("sha256:") or
            len(image_digest) != len("sha256:") + 64 or
            image_digest != terminus_image_pin.get("terminus_task_image_digest")):
        raise FrozenManifestError("Terminus upstream source and task image require exact immutable pins")
    output_path = Path(output_path)
    expected_output_root = root / "artifacts/m5/experiments" / run_id
    if output_path.resolve().parent != expected_output_root.resolve():
        raise FrozenManifestError("frozen manifest output must be artifacts/m5/experiments/<run_id>/frozen_manifest.json")
    if output_path.name != "frozen_manifest.json":
        raise FrozenManifestError("frozen manifest filename is fixed")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    return manifest


def validate_frozen_manifest(path: Path, *, root: Path = ROOT, case_id: str,
                             split: str = "scored",
                             initial_library_path: Path | None = None) -> dict[str, Any]:
    """Fail closed on stale or mismatched source/config pins before any scored task starts."""
    root = Path(root).resolve()
    raw_path = Path(path)
    if raw_path.is_symlink():
        raise FrozenManifestError("frozen experiment manifest cannot be a symlink")
    manifest_path = raw_path.resolve(strict=True)
    manifest_root = (root / "artifacts/m5/experiments").resolve()
    if not manifest_path.is_relative_to(manifest_root):
        raise FrozenManifestError("frozen experiment manifest must be under artifacts/m5/experiments")
    for parent in (manifest_path, *manifest_path.parents):
        if parent.is_symlink():
            raise FrozenManifestError("frozen experiment manifest path cannot traverse symlinks")
        if parent == manifest_root:
            break
    manifest = _json(manifest_path)
    sample = _json(root / "config/bank/sample.json")
    _validate_manifest_header(manifest, sample, case_id=case_id, split=split)
    pins = manifest.get("local_pins")
    if not isinstance(pins, dict) or not isinstance(pins.get("files"), dict):
        raise FrozenManifestError("experiment manifest has no local source pins")
    for relative, expected_hash in pins["files"].items():
        target = (root / relative).resolve()
        if root not in target.parents or not target.is_file() or sha256_file(target) != expected_hash:
            raise FrozenManifestError(f"frozen local source pin mismatch: {relative}")
    _validate_local_file_inventory(root, pins["files"])
    if pins.get("e4_implementation_fingerprint") != implementation_fingerprint():
        raise FrozenManifestError("E4 implementation fingerprint differs from frozen source")
    if manifest.get("e4_runtime") != _e4_runtime_pin():
        raise FrozenManifestError("E4 Python or installed dependency inventory differs from freeze")
    if manifest.get("model_server_runtime") != _llama_runtime_pin(root):
        raise FrozenManifestError("M3 llama.cpp source or executable differs from freeze")
    if manifest.get("terminus_runtime") != _terminus_python_pin(
            root, manifest.get("terminus_upstream_pin", {})):
        raise FrozenManifestError("Harbor Python/dependency runtime differs from freeze")
    if manifest.get("docker_compose_runtime") != _docker_compose_runtime_pin(root):
        raise FrozenManifestError("trusted Docker Compose plugin/config differs from freeze")
    profile_path = root / "config/models/gemma.json"
    policy_path = root / "config/bank/controller_policy.json"
    if pins.get("model_profile_sha256") != sha256_file(profile_path):
        raise FrozenManifestError("Gemma profile differs from frozen profile")
    if pins.get("runtime_policy_sha256") != sha256_file(policy_path):
        raise FrozenManifestError("M4 runtime policy differs from frozen policy")
    current_experiment = _json(root / "config/comparison/experiment.json")
    _validate_resource_budget(manifest, current_experiment)
    model = manifest.get("model", {})
    model_path = (root / str(model.get("checkpoint_path", ""))).resolve()
    if root not in model_path.parents or not model_path.is_file():
        raise FrozenManifestError("frozen Gemma checkpoint is unavailable")
    cache_key = (str(Path(path).resolve()), str(model_path))
    stat = model_path.stat()
    cached = _RUNTIME_PIN_CACHE.get(cache_key)
    stat_key = (stat.st_size, stat.st_mtime_ns, stat.st_ino)
    if cached is None or cached.get("model_stat") != stat_key:
        actual_sha = sha256_file(model_path)
        _RUNTIME_PIN_CACHE[cache_key] = {"model_stat": stat_key, "model_sha": actual_sha}
    else:
        actual_sha = cached["model_sha"]
    if (stat.st_size != model.get("checkpoint_size_bytes") or
            actual_sha != model.get("checkpoint_sha256")):
        raise FrozenManifestError("Gemma checkpoint differs from the frozen checkpoint pin")
    model_doc = _json(root / "config/models/gemma_manifest.json")
    if model_doc.get("sha256") != actual_sha or model_doc.get("size_bytes") != stat.st_size:
        raise FrozenManifestError("Gemma checkpoint no longer matches its model manifest")
    if initial_library_path is not None:
        library_root = Path(initial_library_path).resolve()
        try:
            library_manifest, current_hash = _library_pin(library_root, pins.get("initial_library_snapshot_id", ""))
        except (OSError, FrozenManifestError, json.JSONDecodeError):
            raise FrozenManifestError("initial E4 library pin differs from frozen manifest") from None
        if (str(library_root) != pins.get("initial_library_path") or
                current_hash != pins.get("initial_library_manifest_sha256")):
            raise FrozenManifestError("initial E4 library pin differs from frozen manifest")
    bootstrap_path = Path(pins.get("bootstrap_result_path", ""))
    bootstrap_path = bootstrap_path.resolve()
    if (root not in bootstrap_path.parents or not bootstrap_path.is_file() or
            sha256_file(bootstrap_path) != pins.get("bootstrap_result_sha256")):
        raise FrozenManifestError("initial E4 bootstrap result differs from frozen cost/library pin")
    bootstrap_result = _json(bootstrap_path)
    bootstrap_library = _bootstrap_host_library_path(root, bootstrap_path, bootstrap_result)
    bootstrap_snapshot = load_library_snapshot(bootstrap_library)
    if (bootstrap_library != Path(pins.get("initial_library_path", "")).resolve() or
            bootstrap_result.get("status") not in {"complete_seed_only", "complete_with_additions"} or
            bootstrap_result.get("library_snapshot_id") != pins.get("initial_library_snapshot_id") or
            sha256_file(bootstrap_snapshot.path) != pins.get("initial_library_manifest_sha256")):
        raise FrozenManifestError("bootstrap result does not resolve to the frozen initial library snapshot")
    upstream = manifest.get("terminus_upstream_source", {})
    upstream_root = Path(upstream.get("path", "")).resolve()
    upstream_files: dict[str, str] = {}
    if root not in upstream_root.parents or not upstream_root.is_dir():
        raise FrozenManifestError("frozen Harbor source checkout is unavailable")
    for source_file in sorted(upstream_root.rglob("*")):
        if (not source_file.is_file() or ".git" in source_file.parts or
                "__pycache__" in source_file.parts or source_file.suffix in {".pyc", ".pyo"}):
            continue
        upstream_files[source_file.relative_to(upstream_root).as_posix()] = sha256_file(source_file)
    if upstream_files != upstream.get("files"):
        raise FrozenManifestError("frozen Harbor source inventory differs from recorded full source")
    revision = subprocess.run(["git", "-C", str(upstream_root), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    if revision != manifest.get("terminus_upstream_pin", {}).get("harbor_revision"):
        raise FrozenManifestError("current Harbor revision differs from frozen upstream revision")
    git_tree = subprocess.run(["git", "-C", str(upstream_root), "rev-parse", "HEAD^{tree}"],
                              capture_output=True, text=True, check=True).stdout.strip()
    if git_tree != manifest.get("terminus_upstream_pin", {}).get("harbor_git_tree"):
        raise FrozenManifestError("current Harbor Git tree differs from frozen upstream source pin")
    pin_path = Path(manifest.get("terminus_image_pin", {}).get("pin_file", ""))
    pin_sha = manifest.get("terminus_image_pin", {}).get("pin_file_sha256")
    if (not pin_path.is_file() or sha256_file(pin_path) != pin_sha or
            _json(pin_path) != {key: value for key, value in manifest["terminus_image_pin"].items()
                                if key not in {"pin_file", "pin_file_sha256"}}):
        raise FrozenManifestError("Terminus image ID pin file differs from frozen runtime image")
    image_id = manifest["terminus_image_pin"].get("terminus_task_image_id")
    if not isinstance(image_id, str) or not image_id.startswith("sha256:"):
        raise FrozenManifestError("frozen Terminus task image ID is malformed")
    try:
        actual_image_id = subprocess.run(
            ["docker", "image", "inspect", image_id, "--format", "{{.Id}}"],
            capture_output=True, text=True, timeout=30, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise FrozenManifestError("cannot verify the pinned Terminus image in Docker") from exc
    if actual_image_id != image_id:
        raise FrozenManifestError("local Terminus image ID differs from the frozen image ID")
    pin_doc = _json(pin_path)
    archive_path = (root / pin_doc.get("image_archive", "")).resolve()
    if (root not in archive_path.parents or not archive_path.is_file() or
            sha256_file(archive_path) != pin_doc.get("image_archive_sha256")):
        raise FrozenManifestError("exported Terminus image archive differs from its frozen hash")
    if split == "scored":
        frozen_input = manifest.get("public_inputs", {}).get(case_id)
        if not isinstance(frozen_input, dict):
            raise FrozenManifestError("selected scored case has no frozen public input pin")
        view = load_bank_case_view(case_id, split="scored", root=root)
        _validate_public_view_pin(case_id, view, frozen_input)
    return manifest
