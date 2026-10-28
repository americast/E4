"""Native Harbor Terminus-2 execution on one selected public Bank case."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import inspect
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from e4.baselines.terminus.bank_task import build_bank_instruction
from e4.baselines.terminus.feedback_bridge import FeedbackBridge, three_attempt_slots
from e4.baselines.terminus.terminus2 import (
    DEFAULT_GENERATED_TOKEN_BUDGET,
    DEFAULT_MAX_TURNS,
    ModelCallBudget,
    TerminusSetupError,
    context_accounting,
    load_terminus2,
    sha256_file,
    verify_harbor_source,
)
from e4.paths import ROOT


def _runtime_image_pin(root: Path) -> dict[str, Any]:
    path = root / "artifacts/m5/runtime/terminus_task_image_pin.json"
    try:
        pin = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminusSetupError(
            "the Harbor terminal image is not set up; run scripts/comparison/setup_terminus.py"
        ) from exc
    if (not pin.get("terminus_task_image_id") or
            pin.get("terminus_task_image_digest") != pin.get("terminus_task_image_id")):
        raise TerminusSetupError("task image pin has no immutable Docker image ID")
    archive = root / str(pin.get("image_archive", ""))
    dockerfile = root / "src/e4/baselines/terminus/task/Dockerfile"
    if (not archive.is_file() or sha256_file(archive) != pin.get("image_archive_sha256") or
            sha256_file(dockerfile) != pin.get("dockerfile_sha256")):
        raise TerminusSetupError("Terminus task image archive or Dockerfile differs from its pin")
    return pin


def _runtime_compose_pin(root: Path) -> dict[str, Any]:
    pin_path = root / "artifacts/m5/runtime/docker_compose_plugin_pin.json"
    try:
        pin = json.loads(pin_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminusSetupError("pinned Docker Compose plugin artifact is missing") from exc
    plugin = root / str(pin.get("path", ""))
    buildx = root / str(pin.get("buildx_path", ""))
    config_path = root / str(pin.get("docker_config_path", ""))
    expected_config = config_path.parent.resolve()
    if (not plugin.is_file() or sha256_file(plugin) != pin.get("sha256") or
            not buildx.is_file() or sha256_file(buildx) != pin.get("buildx_sha256") or
            not config_path.is_file() or
            sha256_file(config_path) != pin.get("docker_config_sha256") or
            os.environ.get("DOCKER_CONFIG") != str(expected_config)):
        raise TerminusSetupError("Docker Compose binary/config differs from the pinned trusted runtime")
    version = subprocess.run(["docker", "compose", "version"], capture_output=True,
                             text=True, timeout=20)
    if version.returncode or pin.get("version") not in version.stdout:
        raise TerminusSetupError("pinned Docker Compose plugin is unavailable in the runner namespace")
    buildx_version = subprocess.run(["docker", "buildx", "version"], capture_output=True,
                                    text=True, timeout=20)
    if (buildx_version.returncode or
            pin.get("buildx_version") not in buildx_version.stdout):
        raise TerminusSetupError("pinned Docker Buildx plugin is unavailable in the runner namespace")
    return pin


def _runtime_source_pin(root: Path, actual: dict[str, str]) -> dict[str, Any]:
    path = root / "artifacts/m5/runtime/harbor_source_pin.json"
    try:
        pin = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminusSetupError("pinned Harbor source/dependency artifact is missing") from exc
    for key in ("harbor_revision", "harbor_git_tree", "harbor_source_tree_sha256",
                "harbor_source_path", "harbor_agent_sha256"):
        if pin.get(key) != actual.get(key):
            raise TerminusSetupError(f"Harbor runtime source pin differs for {key}")
    expected_python = (f"Python {sys.version_info.major}.{sys.version_info.minor}."
                       f"{sys.version_info.micro}")
    if pin.get("python_version") != expected_python:
        raise TerminusSetupError("Harbor controller Python differs from its runtime pin")
    if pin.get("harbor_version") != importlib.metadata.version("harbor"):
        raise TerminusSetupError("installed Harbor distribution differs from its runtime pin")
    freeze_path = root / pin.get("pip_freeze_path", "")
    if not freeze_path.is_file():
        raise TerminusSetupError("pinned Harbor pip freeze artifact is missing")
    freeze_result = subprocess.run([sys.executable, "-m", "pip", "freeze", "--all"],
                                   text=True, capture_output=True, timeout=60)
    if freeze_result.returncode:
        raise TerminusSetupError("cannot inspect active Harbor Python dependencies")
    actual_freeze = (freeze_result.stdout.strip() + "\n").encode("utf-8")
    freeze_hash = hashlib.sha256(actual_freeze).hexdigest()
    if (freeze_hash != pin.get("pip_freeze_sha256") or
            sha256_file(freeze_path) != pin.get("pip_freeze_sha256")):
        raise TerminusSetupError("active Harbor dependency set differs from the pinned pip freeze")
    for lock_name, key in (("requirements.lock", "dependency_lock_sha256"),
                            ("build_requirements.lock", "build_dependency_lock_sha256")):
        if sha256_file(root / "src/e4/baselines/terminus" / lock_name) != pin.get(key):
            raise TerminusSetupError(f"Harbor dependency lock differs: {lock_name}")
    return pin


def _docker_environment(*, image_id: str, run_dir: Path, ipc_dir: Path,
                        task_dir: Path, case_id: str, view: Any, root: Path):
    try:
        from harbor.environments.docker.docker import DockerEnvironment
        from harbor.models.task.config import EnvironmentConfig, NetworkMode, NetworkPolicy
        from harbor.models.trial.paths import TrialPaths
    except ImportError as exc:
        raise TerminusSetupError("Harbor runtime dependencies are not installed") from exc
    trial_dir = run_dir / "harbor_trial"
    trial_paths = TrialPaths(trial_dir=trial_dir)
    trial_paths.mkdir()
    return DockerEnvironment(
        environment_dir=task_dir,
        environment_name="bank-terminus-2",
        session_id=f"{case_id}-terminus-{uuid.uuid4().hex[:10]}",
        trial_paths=trial_paths,
        task_env_config=EnvironmentConfig(docker_image=image_id, workdir="/workspace",
                                          cpus=1, memory_mb=16384),
        network_policy=NetworkPolicy(network_mode=NetworkMode.NO_NETWORK),
        mounts=[{
            "type": "bind",
            "source": str((ipc_dir / "requests").resolve(strict=True)),
            "target": "/run/e4bank/requests",
        }, {
            "type": "bind",
            "source": str(view.telemetry_root.resolve(strict=True)),
            "target": "/data",
            "read_only": True,
        }, {
            "type": "bind",
            "source": str((ipc_dir / "responses").resolve(strict=True)),
            "target": "/run/e4bank/responses",
            "read_only": True,
        }],
        extra_docker_compose=[root / "src/e4/baselines/terminus/task/harbor_overlay.yaml"],
        persistent_env={"OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                        "MKL_NUM_THREADS": "1", "NUMEXPR_NUM_THREADS": "1"},
        keep_containers=False,
    )


def _inspect_task_container(task_env: Any, view: Any, *, image_id: str) -> dict[str, Any]:
    """Inspect Harbor's live container and fail closed on missing isolation limits."""
    project = re.sub(r"[^a-z0-9_-]", "-", task_env.session_id.lower())
    if not project or not project[0].isalnum():
        project = "0" + project
    listing = subprocess.run(
        ["docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
         "--filter", "label=com.docker.compose.service=main", "--format", "{{.ID}}"],
        text=True, capture_output=True, timeout=30,
    )
    if listing.returncode:
        raise TerminusSetupError(f"cannot inspect Harbor task container: {listing.stderr.strip()}")
    container_ids = [line.strip() for line in listing.stdout.splitlines() if line.strip()]
    if len(container_ids) != 1:
        raise TerminusSetupError("Harbor task must have exactly one running main container")
    inspected = subprocess.run(["docker", "inspect", container_ids[0]],
                                text=True, capture_output=True, timeout=30)
    if inspected.returncode:
        raise TerminusSetupError("Docker inspect failed for the Harbor task container")
    document = json.loads(inspected.stdout)[0]
    host = document.get("HostConfig") or {}
    mounts = {item.get("Destination"): item for item in document.get("Mounts", [])}
    expected_data = mounts.get("/data")
    expected_requests = mounts.get("/run/e4bank/requests")
    expected_responses = mounts.get("/run/e4bank/responses")
    if (not expected_data or expected_data.get("RW") is not False or
            Path(expected_data.get("Source", "")).resolve() != Path(view.telemetry_root).resolve()):
        raise TerminusSetupError("/data must be the selected public view mounted read-only")
    if not expected_requests or expected_requests.get("RW") is not True:
        raise TerminusSetupError("feedback requests must use the dedicated writable IPC mount")
    if not expected_responses or expected_responses.get("RW") is not False:
        raise TerminusSetupError("feedback responses must use the dedicated read-only IPC mount")
    if "/var/run/docker.sock" in mounts:
        raise TerminusSetupError("Docker control socket leaked into the task terminal")
    if any(Path(item.get("Source", "/nonexistent")).resolve() == ROOT.resolve()
           for item in document.get("Mounts", [])):
        raise TerminusSetupError("project root leaked into the task terminal")
    if host.get("NetworkMode") != "none":
        raise TerminusSetupError("Harbor task network is not disabled")
    if host.get("Privileged"):
        raise TerminusSetupError("Harbor task container must not be privileged")
    if host.get("Devices") or host.get("DeviceRequests"):
        raise TerminusSetupError("Harbor task container must not receive host devices or GPUs")
    if host.get("Memory") != 16 * 1024 * 1024 * 1024:
        raise TerminusSetupError("Harbor task memory cgroup differs from the frozen 16 GiB cap")
    if host.get("NanoCpus") != 1_000_000_000:
        raise TerminusSetupError("Harbor task CPU cgroup differs from the frozen 1 CPU cap")
    config = document.get("Config") or {}
    if any(str(entry).startswith("E4_BASE_URL=") for entry in config.get("Env", [])):
        raise TerminusSetupError("model endpoint leaked into the task terminal environment")
    tmpfs = host.get("Tmpfs") or {}
    workspace_tmpfs = tmpfs.get("/workspace", "")
    if "size=21474836480" not in workspace_tmpfs:
        raise TerminusSetupError("Harbor task workspace tmpfs differs from the frozen 20 GiB cap")
    if document.get("Image") != image_id and document.get("Config", {}).get("Image") != image_id:
        # Docker inspect's Image field is the resolved image ID; Config.Image can
        # be the immutable ID or its original local reference depending on Engine.
        if document.get("Image") != image_id:
            raise TerminusSetupError("running task image differs from the pinned image ID")
    return {
        "container_id": container_ids[0],
        "image_id": document.get("Image"),
        "network_mode": host.get("NetworkMode"),
        "privileged": host.get("Privileged"),
        "device_requests": host.get("DeviceRequests") or [],
        "devices": host.get("Devices") or [],
        "memory_bytes": host.get("Memory"),
        "nano_cpus": host.get("NanoCpus"),
        "workspace_tmpfs": workspace_tmpfs,
        "mounts": [{"source": item.get("Source"), "target": item.get("Destination"),
                    "read_write": item.get("RW"), "type": item.get("Type")}
                   for item in document.get("Mounts", [])],
        "has_docker_socket": False,
        "endpoint_in_task_environment": False,
    }


async def _run_async(*, view: Any, evaluation: Any, run_dir: Path,
                     harbor_source: Path, root: Path, api_base: str | None,
                     max_turns: int, generated_token_budget: int,
                     case_wall_seconds: int, enable_summarize: bool,
                     summarization_threshold_free_tokens: int) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    ipc_dir = run_dir / "feedback_ipc"
    trial_dir = run_dir / "harbor_trial"
    agent_logs = run_dir / "agent_logs"
    collisions = [path.name for path in (ipc_dir, trial_dir, agent_logs)
                  if path.exists() or path.is_symlink()]
    if collisions:
        raise TerminusSetupError(
            f"run output paths already exist; refusing to replace artifacts: {collisions}"
        )
    (ipc_dir / "requests").mkdir(parents=True, mode=0o700)
    (ipc_dir / "responses").mkdir(mode=0o755)
    os.chmod(ipc_dir / "requests", 0o733)
    os.chmod(ipc_dir / "responses", 0o755)
    agent_logs.mkdir(parents=True, mode=0o700)

    source_pin = verify_harbor_source(harbor_source)
    runtime_source_pin = _runtime_source_pin(root, source_pin)
    image_pin = _runtime_image_pin(root)
    Terminus2, agent = load_terminus2(
        harbor_source, root=root, logs_dir=run_dir / "agent_logs",
        max_turns=max_turns, api_base=api_base,
        enable_summarize=enable_summarize,
        proactive_summarization_threshold=summarization_threshold_free_tokens,
    )
    imported_path = Path(inspect.getfile(Terminus2)).resolve(strict=True)
    source_pin["imported_agent_path"] = imported_path.relative_to(Path(harbor_source).resolve()).as_posix()
    if source_pin["imported_agent_path"] != source_pin["harbor_agent_path"]:
        raise TerminusSetupError("imported Terminus-2 module differs from the pinned Harbor agent source")
    source_pin.update({key: value for key, value in runtime_source_pin.items()
                       if key not in source_pin})
    from harbor.models.agent.context import AgentContext

    task_env = _docker_environment(
        image_id=image_pin["terminus_task_image_id"], run_dir=run_dir,
        ipc_dir=ipc_dir, task_dir=root / "src/e4/baselines/terminus/task",
        case_id=view.case_id, view=view, root=root,
    )
    calls = ModelCallBudget(
        agent, generated_token_budget=generated_token_budget,
        terminal_predicate=lambda: evaluation.terminal,
        terminal_reason=lambda: (
            "accepted" if any(getattr(item, "accepted", False)
                              for item in getattr(evaluation, "records", [])) or
                              any(getattr(evaluation, "_accepted", []))
            else "submission_limit"
        ),
        http_journal_path=run_dir / "model_http_calls.jsonl",
    )
    def stop_native_summarization() -> None:
        # A terminal evaluation result closes diagnosis work. Terminus-2's own
        # completion parser then finalizes its trajectory without another generation.
        agent.options.enable_summarize = False

    feedback = FeedbackBridge(ipc_dir, evaluation, on_terminal=stop_native_summarization)
    context = AgentContext()
    started = time.monotonic()
    agent_started_seconds: float | None = None
    docker_inspection: dict[str, Any] | None = None
    setup_completed = False
    outcome = "failed"
    error: str | None = None
    try:
        instruction = build_bank_instruction(view)
        prompt_path = run_dir / "task_instruction.txt"
        with prompt_path.open("x", encoding="utf-8") as stream:
            stream.write(instruction)
        await task_env.start(force_build=False)
        docker_inspection = _inspect_task_container(
            task_env, view, image_id=image_pin["terminus_task_image_id"]
        )
        await task_env.exec("mkdir -p /workspace", timeout_sec=20)
        probe = await task_env.exec(
            "python -c 'import numpy,pandas,scipy,sklearn; print(numpy.__version__,pandas.__version__,scipy.__version__,sklearn.__version__)' "
            "&& tmux -V && test -z \"${E4_BASE_URL:-}\"",
            timeout_sec=30,
        )
        if probe.return_code != 0:
            raise TerminusSetupError("task terminal scientific-runtime or endpoint-isolation preflight failed")
        await agent.setup(task_env)
        setup_completed = True
        agent_started_seconds = time.monotonic() - started
        feedback.start()
        try:
            await asyncio.wait_for(
                agent.run(instruction, task_env, context),
                timeout=case_wall_seconds,
            )
            outcome = "complete"
        except asyncio.TimeoutError:
            outcome = "case_wall_timeout"
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            outcome = "model_or_agent_error"
    finally:
        feedback.stop()
        try:
            await task_env.stop(delete=True)
        except Exception as exc:
            if error is None:
                error = f"environment_cleanup: {type(exc).__name__}: {exc}"
            outcome = "environment_cleanup_error"
        calls.close()

    elapsed = time.monotonic() - started
    if feedback.error:
        outcome = "feedback_protocol_error"
        error = feedback.error
    accounting = context_accounting(context)
    call_budget = calls.as_dict()
    usage_complete = call_budget["usage_complete"]
    termination_reason = (
        "accepted" if any(slot["accepted"] for slot in three_attempt_slots(evaluation))
        else "submission_limit" if evaluation.terminal else None
    )
    result = {
        "schema_version": "e4-terminus2-case-result/1",
        "case_id": view.case_id,
        "split": view.split,
        "status": outcome,
        "error": error,
        "accepted": any(slot["accepted"] for slot in three_attempt_slots(evaluation)),
        "attempts": three_attempt_slots(evaluation),
        "submitted_attempt_count": evaluation.attempts,
        "terminal_after_submissions": evaluation.terminal,
        "wall_seconds": elapsed,
        "model_usage": accounting,
        "model_call_budget": call_budget,
        "cost": {
            "model_calls": call_budget["http_requests_started"],
            "prompt_tokens": call_budget["prompt_tokens_reported_by_http"] if usage_complete else None,
            "completion_tokens": call_budget["generated_tokens_reported"] if usage_complete else None,
            "model_seconds": call_budget["http_request_wall_seconds"],
            "cpu_seconds": None,
            "load_seconds": None,
            "startup_seconds": agent_started_seconds,
            "wall_seconds": elapsed,
            "logical_bytes_scanned": None,
            "physical_bytes_read": None,
            "returned_bytes": None,
        },
        "terminal_turn_limit": max_turns,
        "diagnosis_submission_limit": 3,
        "feedback_requests_served": feedback.request_count,
        "terminal_summaries_enabled": enable_summarize,
        "terminal_summary_threshold_free_tokens": summarization_threshold_free_tokens,
        "terminal_control_events": {
            "termination_reason": termination_reason,
            "synthetic_native_completion_responses": call_budget["terminal_finalization_responses"],
            "model_predictions_changed": False,
        },
        "agent_setup_completed": setup_completed,
        "native_agent": "Harbor Terminus-2",
        "upstream": source_pin,
        "task_image": image_pin,
        "docker_task_inspection": docker_inspection,
        "public_input_pins": {
            "input_manifest_sha256": view.input_manifest_sha256,
            "public_manifest_sha256": view.public_manifest_sha256,
            "alert_sha256": view.alert_sha256,
            "telemetry_file_sha256": dict(view.telemetry_file_sha256),
        },
        "task_instruction_sha256": hashlib.sha256(instruction.encode("utf-8")).hexdigest(),
        "trajectory_files": sorted(
            path.relative_to(run_dir).as_posix()
            for path in run_dir.rglob("trajectory*.json") if path.is_file()
        ),
        "agent_log_files": sorted(
            path.relative_to(run_dir).as_posix()
            for path in (run_dir / "agent_logs").rglob("*") if path.is_file()
        ) if (run_dir / "agent_logs").is_dir() else [],
    }
    return result


def run_terminus_case(*, view: Any, evaluation: Any, run_dir: Path,
                      harbor_source: Path | None = None, root: Path = ROOT,
                      api_base: str | None = None, max_turns: int = DEFAULT_MAX_TURNS,
                      generated_token_budget: int = DEFAULT_GENERATED_TOKEN_BUDGET,
                      case_wall_seconds: int = 7200, enable_summarize: bool = True,
                      summarization_threshold_free_tokens: int = 8000) -> dict[str, Any]:
    """Run native Terminus-2 for a selected public view and trusted evaluator session."""
    if view.case_id != evaluation.case_id:
        raise ValueError("public view and trusted evaluator case IDs differ")
    if view.staged_root is None or Path(view.staged_root).resolve() != Path(view.telemetry_root).resolve():
        raise ValueError("Terminus-2 requires the public-only case view staged under the run directory")
    if not Path(view.staged_root).is_dir():
        raise ValueError("staged public case view is missing")
    if max_turns <= 0 or generated_token_budget <= 0 or case_wall_seconds <= 0:
        raise ValueError("Terminus-2 resource limits must be positive")
    run_dir = Path(run_dir).resolve()
    runtime_root = Path(root).resolve() / "artifacts/m5/runtime"
    tokenizer_cache = runtime_root / "tokenizer-cache"
    if not tokenizer_cache.is_dir():
        raise TerminusSetupError("pinned local tokenizer cache is missing; rerun setup_terminus.py")
    os.environ["CUSTOM_TIKTOKEN_CACHE_DIR"] = str(tokenizer_cache)
    os.environ["TIKTOKEN_CACHE_DIR"] = str(tokenizer_cache)
    os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
    if os.environ.get("E4_DOCKER_CONTROL_ENABLED") != "1":
        raise TerminusSetupError("Terminus-2 requires the trusted --docker-control launcher mode")
    _runtime_compose_pin(Path(root).resolve())
    expected_parent = (Path(root).resolve() / "artifacts").resolve()
    if expected_parent not in run_dir.parents:
        raise ValueError("run directory must be under project artifacts for Docker host visibility")
    selected_source = harbor_source or (Path(root) / "artifacts/m5/runtime/harbor-source")
    try:
        result = asyncio.run(_run_async(
            view=view, evaluation=evaluation, run_dir=run_dir,
            harbor_source=Path(selected_source), root=Path(root), api_base=api_base,
            max_turns=max_turns, generated_token_budget=generated_token_budget,
            case_wall_seconds=case_wall_seconds, enable_summarize=enable_summarize,
            summarization_threshold_free_tokens=summarization_threshold_free_tokens,
        ))
    except RuntimeError as exc:
        # asyncio.run's nested-loop error is surfaced plainly to the orchestrator.
        raise TerminusSetupError(f"cannot start Harbor event loop: {exc}") from exc
    output = run_dir / "terminus_result.json"
    output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                      encoding="utf-8")
    return result
