#!/usr/bin/env python3
"""Run the frozen 20-task synthetic M3 comparison suite or prompt calibration.

Run it under scripts/model/run.py, which supplies E4_RUN_DIR and E4_MODEL. The
`--calibrate` mode is a separate full-library prompt/token-budget measurement;
it is not one of the 20 scored synthetic tasks.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, default=ROOT / "config/synthetic/suite_v3.json")
    parser.add_argument("--calibrate", action="store_true",
                        help="send one representative full-library prompt and record token budget")
    args = parser.parse_args(argv)
    run_dir_value = os.environ.get("E4_RUN_DIR")
    if not run_dir_value or not os.environ.get("E4_MODEL"):
        parser.error("run through scripts/model/run.py; E4_RUN_DIR and E4_MODEL are required")
    run_dir = Path(run_dir_value).resolve()
    try:
        run_dir.relative_to((ROOT / "artifacts/m3/model_runs").resolve())
    except ValueError:
        parser.error("E4_RUN_DIR must be inside artifacts/m3/model_runs")
    try:
        from e4.models.analysis_model import model_from_environment
        from e4.synthetic.benchmark import run_calibration, run_suite
        model = model_from_environment()
        if args.calibrate:
            result = run_calibration(run_dir, model)
            ok = not result["model_call"].get("error") and result["within_context_budget"]
        else:
            suite_path = args.suite.resolve(strict=True)
            try:
                suite_path.relative_to(ROOT.resolve())
            except ValueError as exc:
                raise ValueError("suite file must be inside the project root") from exc
            result = run_suite(suite_path, run_dir, model)
            # A complete run is a successful launcher command even when model
            # correctness is low; correctness is reported in the summary.
            ok = True
        print(json.dumps({key: result.get(key) for key in
                          ("record_type", "task_count", "initial_correct_count",
                           "correct_after_static_repair_count", "final_correct_count",
                           "compile_pass_count", "semantic_pass_count",
                           "diagnosis_primary_top3_correct", "diagnosis_secondary_top1_correct",
                           "diagnosis_category_correct", "prompt_sha256", "prompt_token_count",
                           "context_tokens", "within_context_budget")}, sort_keys=True))
        return 0 if ok else 1
    except Exception as exc:
        print(f"M3 benchmark failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
