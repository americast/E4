#!/usr/bin/env python3
"""Re-score a completed M3 model capture offline against corrected v3 gold."""

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
    parser.add_argument("--capture", required=True, type=Path,
                        help="completed immutable source capture under artifacts/m3/model_runs")
    parser.add_argument("--suite", type=Path, default=ROOT / "config/synthetic/suite_v3.json")
    parser.add_argument("--output", type=Path,
                        help="new exclusive output directory (defaults to a name derived from capture)")
    args = parser.parse_args(argv)
    capture = args.capture if args.capture.is_absolute() else ROOT / args.capture
    output = args.output
    if output is None:
        output = ROOT / "artifacts/m3/offline_replays" / f"{capture.name}_corrected_v3"
    elif not output.is_absolute():
        output = ROOT / output
    suite = args.suite if args.suite.is_absolute() else ROOT / args.suite
    # Match the captured launcher’s numerical thread limits before importing
    # e4 or any optional numerical libraries.
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    try:
        from e4.synthetic.replay import replay_captured_run
        summary = replay_captured_run(capture, suite, output)
        shown = {key: summary.get(key) for key in (
            "task_count", "initial_correct_count", "correct_after_static_repair_count",
            "final_correct_count", "compile_pass_count", "semantic_pass_count",
            "diagnosis_primary_top3_correct", "diagnosis_secondary_top1_correct",
            "diagnosis_category_correct", "suite_id", "suite_sha256", "checker_sha256",
            "replay_validation_wall_seconds", "selection_costs")}
        print(json.dumps(shown, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"offline M3 replay failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
