#!/usr/bin/env python3
"""Run official Harbor Terminus-2 on one staged public Bank case."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from e4.baselines.terminus.feedback_bridge import EvaluationSocketProxy  # noqa: E402
from e4.baselines.terminus.runner import run_terminus_case  # noqa: E402
from e4.baselines.terminus.terminus2 import (  # noqa: E402
                                             DEFAULT_GENERATED_TOKEN_BUDGET,
                                             DEFAULT_MAX_TURNS,
                                             HARBOR_REVISION,
                                             TerminusSetupError,
)
from e4.comparison.tasks import load_bank_case_view  # noqa: E402
from e4.comparison.manifest import (  # noqa: E402
                                    _validate_local_file_inventory,
                                    _validate_manifest_header,
                                    _validate_resource_budget,
)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TerminusSetupError(f"cannot read run configuration {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise TerminusSetupError(f"run configuration must be a JSON object: {path}")
    return value


def _validate_development_manifest_in_e4_runtime(manifest_path: Path) -> None:
    """Run the shared M5 validator under E4's pinned Python/runtime fingerprint.

    Harbor itself runs on Python 3.13, while the E4 implementation fingerprint
    is frozen under the project's Python 3.10 environment. Recomputing that
    fingerprint in this process would reject a valid development manifest.
    """
    project_python = ROOT / ".venv/bin/python"
    if not project_python.is_file():
        raise TerminusSetupError(f"pinned E4 Python runtime is unavailable: {project_python}")
    snippet = (
        "import sys; from pathlib import Path; sys.path.insert(0, str(Path(sys.argv[2]) / 'src')); "
        "from e4.comparison.manifest import validate_development_manifest; "
        "validate_development_manifest(Path(sys.argv[1]), root=Path(sys.argv[2]))"
    )
    try:
        completed = subprocess.run(
            [str(project_python), "-c", snippet, str(manifest_path), str(ROOT)],
            cwd=ROOT, capture_output=True, text=True, timeout=120, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TerminusSetupError(f"pinned E4 manifest validation could not run: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()[-2000:]
        raise TerminusSetupError(
            f"provisional development manifest validation failed in pinned E4 runtime: {detail}")


def _freeze_guard(manifest_path: Path, *, case_id: str, split: str,
                  validated_sha256: str | None) -> dict[str, Any]:
    manifest = _read_json(manifest_path)
    if split == "scored":
        if manifest.get("schema_version") != "e4-m5-frozen-manifest/1" or manifest.get("status") != "frozen":
            raise TerminusSetupError("scored Terminus-2 requires a frozen M5 experiment manifest")
        sample = _read_json(ROOT / "config/bank/sample.json")
        try:
            _validate_manifest_header(manifest, sample, case_id=case_id, split="scored")
            pins = manifest.get("local_pins", {}).get("files", {})
            if not isinstance(pins, dict):
                raise ValueError("frozen manifest has no source pin inventory")
            _validate_local_file_inventory(ROOT, pins)
            for relative, expected in pins.items():
                path = (ROOT / relative).resolve(strict=True)
                if ROOT not in path.parents or _sha256(path) != expected:
                    raise ValueError(f"frozen source pin changed: {relative}")
            experiment = _read_json(ROOT / "config/comparison/experiment.json")
            _validate_resource_budget(manifest, experiment)
        except (OSError, ValueError, KeyError) as exc:
            raise TerminusSetupError(f"frozen manifest/source/budget validation failed: {exc}") from exc
        if validated_sha256 != _sha256(manifest_path):
            raise TerminusSetupError("trusted orchestrator's frozen-manifest validation marker is missing or stale")
        upstream = manifest.get("terminus_upstream_pin", {})
        if upstream.get("harbor_revision") != HARBOR_REVISION:
            raise TerminusSetupError("frozen manifest has a different Harbor revision")
    elif split == "dev":
        try:
            _validate_development_manifest_in_e4_runtime(manifest_path)
        except (OSError, ValueError, KeyError) as exc:
            raise TerminusSetupError(f"provisional development manifest validation failed: {exc}") from exc
        if validated_sha256 != _sha256(manifest_path):
            raise TerminusSetupError("trusted orchestrator's development-manifest marker is missing or stale")
    return manifest


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _use_staged_public_view(case_id: str, split: str, staged_root: Path):
    """Validate the orchestrator's host-visible selected-case view without rewriting it."""
    source_view = load_bank_case_view(case_id, split=split, root=ROOT)
    staged_root = Path(staged_root).resolve(strict=True)
    if not staged_root.is_dir():
        raise TerminusSetupError("staged public task view is not a directory")
    expected = {relative for relative in source_view.telemetry_files} | {"alert.json"}
    actual: set[str] = set()
    for path in staged_root.rglob("*"):
        if path.is_symlink():
            raise TerminusSetupError("staged public task view contains a symbolic link")
        if path.is_file():
            actual.add(path.relative_to(staged_root).as_posix())
        elif not path.is_dir():
            raise TerminusSetupError("staged public task view contains a special file")
    if actual != expected:
        raise TerminusSetupError("staged task files do not match the single selected public case")
    staged_files = {relative: staged_root / relative for relative in source_view.telemetry_files}
    for relative, path in staged_files.items():
        if _sha256(path) != source_view.telemetry_file_sha256[relative]:
            raise TerminusSetupError(f"staged public clip differs from pinned source: {relative}")
    if _sha256(staged_root / "alert.json") != source_view.alert_sha256:
        raise TerminusSetupError("staged public alert record differs from its pinned source")
    return replace(source_view, telemetry_root=staged_root,
                   telemetry_files=staged_files, staged_root=staged_root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-manifest", type=Path, required=True)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--split", choices=("dev", "scored"), required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--task-view-root", type=Path, required=True,
                        help="host-visible artifact directory staged by the trusted orchestrator")
    parser.add_argument("--trusted-evaluator-socket", type=Path, required=True,
                        help="trusted outer orchestrator's Unix socket; never mounted into task container")
    parser.add_argument("--harbor-source", type=Path,
                        default=ROOT / "artifacts/m5/runtime/harbor-source")
    parser.add_argument("--max-turns", type=int, default=DEFAULT_MAX_TURNS)
    parser.add_argument("--generated-token-budget", type=int,
                        default=DEFAULT_GENERATED_TOKEN_BUDGET)
    parser.add_argument("--case-wall-seconds", type=int, default=7200)
    parser.add_argument("--validated-manifest-sha256",
                        help="required by the trusted orchestrator for both development and scored runs")
    args = parser.parse_args(argv)
    run_dir = args.run_dir.resolve()
    # Keep the lexical path so `_use_staged_public_view` can reject a symlink at
    # the requested root before resolving it.
    task_view_root = Path(os.path.abspath(args.task_view_root))
    socket_path = args.trusted_evaluator_socket.resolve()
    if not socket_path.exists():
        parser.error("trusted evaluator socket is not available")
    try:
        manifest_path = args.experiment_manifest.resolve(strict=True)
        manifest = _freeze_guard(manifest_path, case_id=args.case_id, split=args.split,
                                  validated_sha256=args.validated_manifest_sha256)
        manifest_sha256 = _sha256(manifest_path)
        resource_budgets = manifest.get("resource_budgets") or {}
        terminus_config = _read_json(ROOT / "config/comparison/terminus.json")
        native_settings = terminus_config.get("native_terminus2_settings", {})
        terminus_budgets = terminus_config.get("resource_budgets", {})
        configured_turns = int(resource_budgets.get("terminus_max_turns") or
                                native_settings.get("max_turns") or args.max_turns)
        configured_tokens = int(resource_budgets.get(
            "aggregate_generated_tokens",
            resource_budgets.get("aggregate_generated_token_ceiling") or
            terminus_budgets.get("aggregate_generated_token_ceiling") or
            args.generated_token_budget))
        configured_wall = int(resource_budgets.get("case_wall_seconds") or
                              terminus_budgets.get("case_wall_seconds") or
                              args.case_wall_seconds)
        summarize = native_settings.get("enable_context_summarization")
        summary_threshold = native_settings.get("proactive_summarization_threshold_free_tokens")
        if type(summarize) is not bool or type(summary_threshold) is not int or summary_threshold < 0:
            raise TerminusSetupError("Terminus-2 context summarization settings are invalid")
        if (args.max_turns != configured_turns or
                args.generated_token_budget != configured_tokens or
                args.case_wall_seconds != configured_wall):
            raise TerminusSetupError("CLI resource limits differ from the validated experiment manifest")
        # Freeze/source/budget checks above precede reading the staged public case.
        view = _use_staged_public_view(args.case_id, args.split, task_view_root)
        evaluation = EvaluationSocketProxy(socket_path, args.case_id)
        result = run_terminus_case(
            view=view,
            evaluation=evaluation,
            run_dir=run_dir,
            harbor_source=args.harbor_source.resolve(),
            root=ROOT,
            max_turns=configured_turns,
            generated_token_budget=configured_tokens,
            case_wall_seconds=configured_wall,
            enable_summarize=summarize,
            summarization_threshold_free_tokens=summary_threshold,
        )
        result["validated_experiment_manifest_sha256"] = manifest_sha256
        result_path = run_dir / "terminus_result.json"
        result_path.write_text(json.dumps(result, ensure_ascii=False,
                                          sort_keys=True, indent=2) + "\n",
                               encoding="utf-8")
    except (TerminusSetupError, ValueError, OSError) as exc:
        parser.exit(2, f"run_terminus_bank: {exc}\n")
    print(json.dumps({"case_id": args.case_id, "status": result["status"],
                      "accepted": result["accepted"], "run_dir": str(run_dir)},
                     ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
