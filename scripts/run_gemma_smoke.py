#!/usr/bin/env python3
"""Run one harmless native Terminus-2 turn in the pinned offline task image."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

from e4baseline.runner import (  # noqa: E402
    _docker_environment,
    _inspect_task_container,
    _runtime_compose_pin,
    _runtime_image_pin,
    _runtime_source_pin,
)
from e4baseline.terminus2 import (  # noqa: E402
    ModelCallBudget,
    context_accounting,
    load_terminus2,
    verify_harbor_source,
)


async def _run_smoke(run_dir: Path) -> dict[str, object]:
    source = ROOT / "artifacts/m5/runtime/harbor-source"
    verify_harbor_source(source)
    source_pin = _runtime_source_pin(ROOT, verify_harbor_source(source))
    image_pin = _runtime_image_pin(ROOT)
    _runtime_compose_pin(ROOT)

    run_dir.mkdir(parents=True, exist_ok=False)
    toy_root = run_dir / "toy_task"
    toy_root.mkdir()
    (toy_root / "task.txt").write_text(
        "Independent runtime smoke test. Respond with READY and finish.\n",
        encoding="utf-8",
    )
    ipc_dir = run_dir / "feedback_ipc"
    (ipc_dir / "requests").mkdir(parents=True, mode=0o700)
    (ipc_dir / "responses").mkdir(mode=0o755)
    os.chmod(ipc_dir / "requests", 0o733)
    os.chmod(ipc_dir / "responses", 0o755)
    logs_dir = run_dir / "agent_logs"
    logs_dir.mkdir(mode=0o700)

    view = SimpleNamespace(case_id="independent-smoke", telemetry_root=toy_root)
    _Terminus2, agent = load_terminus2(
        source, root=ROOT, logs_dir=logs_dir, max_turns=1,
        enable_summarize=False,
    )
    task_env = _docker_environment(
        image_id=image_pin["terminus_task_image_id"], run_dir=run_dir,
        ipc_dir=ipc_dir, task_dir=ROOT / "e4baseline/terminus_task",
        case_id=view.case_id, view=view, root=ROOT,
    )
    budget = ModelCallBudget(
        agent, generated_token_budget=1024, maximum_tokens_per_call=1024,
        http_journal_path=run_dir / "model_http_calls.jsonl",
    )
    started = True
    try:
        await task_env.start(force_build=False)
        inspection = _inspect_task_container(task_env, view,
                                             image_id=image_pin["terminus_task_image_id"])
        probe = await task_env.exec(
            "test -r /data/task.txt && test -z \"${E4_BASE_URL:-}\" && "
            "test ! -S /var/run/docker.sock",
            timeout_sec=20,
        )
        if probe.return_code:
            raise RuntimeError("native smoke task image preflight failed")
        await agent.setup(task_env)
        from harbor.models.agent.context import AgentContext

        context = AgentContext()
        await agent.run(
            "This is an independent no-data runtime smoke, not a diagnosis task. "
            "Read /data/task.txt, respond READY, and mark the task complete. "
            "Do not inspect any other files or use the network.",
            task_env,
            context,
        )
        usage = budget.as_dict()
        agent_steps = list(getattr(agent, "_trajectory_steps", []))
        output_text = "\n".join(
            "\n".join((step.message or "", step.reasoning_content or ""))
            for step in agent_steps
            if getattr(step, "source", None) == "agent"
        )
        completion_requested = any(
            call.function_name == "mark_task_complete"
            for step in agent_steps
            for call in (getattr(step, "tool_calls", None) or [])
        )
        ready_seen = "READY" in output_text.upper()
        return {
            "status": ("complete" if usage["http_requests_started"] == 1 and
                       ready_seen and completion_requested else "smoke_output_mismatch"),
            "model_http_requests": usage["http_requests_started"],
            "ready_seen": ready_seen,
            "task_complete_requested": completion_requested,
            "model_usage": context_accounting(context),
            "call_budget": usage,
            "container_inspection": inspection,
            "task_data": "synthetic task.txt only",
            "evaluator_connected": False,
            "harbor_revision": source_pin["harbor_revision"],
            "task_image_id": image_pin["terminus_task_image_id"],
        }
    finally:
        budget.close()
        await task_env.stop(delete=True)


def main() -> int:
    launcher_run_dir = os.environ.get("E4_RUN_DIR")
    if not launcher_run_dir:
        print("Gemma smoke must run inside scripts/run_m3_model.py", file=sys.stderr)
        return 2
    run_dir = Path(launcher_run_dir) / "terminus_native_smoke"
    try:
        result = asyncio.run(_run_smoke(run_dir))
    except Exception as exc:
        result = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    output = Path(launcher_run_dir) / "terminus_native_smoke_result.json"
    output.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                      encoding="utf-8")
    print(json.dumps({key: result.get(key) for key in (
        "status", "model_http_requests", "ready_seen", "task_complete_requested",
        "task_data", "evaluator_connected", "error")
        if key in result}, sort_keys=True))
    return 0 if result.get("status") == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
