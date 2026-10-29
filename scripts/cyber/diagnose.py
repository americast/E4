#!/usr/bin/env python3
"""Run the three-stage cyber diagnosis on the incalmo x enterprise_b pair, judged against the
failed-exfiltration ground truth. Run it under the model runner, which names the model:

    cd /home/marko/E4 && .venv/bin/python scripts/model/run.py --model gemma_3090 -- \
        .venv/bin/python scripts/cyber/diagnose.py
"""
from __future__ import annotations

import argparse
import dataclasses
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.adapters.cyber import load_run
from e4.agent.cyber.scope import scope_cases
from e4.judge.cyber.ground_truth import FAILED_EXFILTRATION
from e4.agent.cyber.stages import CyberModelClient, run_cyber_diagnostic
from e4.models.analysis_model import model_from_environment

RUNS = ROOT / "data/cyber"
SCOPE_RUNS = ["dissect_qwen38_i_entb_t2", "dissect_qwen38_i_entb_t1"]   # failing, successful


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=None,
                        help="sampling seed for this try; the model file's seed when omitted")
    args = parser.parse_args()

    model = model_from_environment()
    if args.seed is not None:
        model = dataclasses.replace(model, seed=args.seed)
    print(f"model {model.name}, seed {model.seed}", flush=True)
    client = CyberModelClient(model)

    print("loading runs in scope", flush=True)
    telemetries = {}
    for name in SCOPE_RUNS:
        telemetries[name] = load_run(RUNS / name)
    cases = scope_cases(telemetries)

    # the project is read-only under the launcher; its writable run directory is E4_RUN_DIR
    run_dir = Path(os.environ["E4_RUN_DIR"]) / "diagnosis"
    print(f"diagnosing; artifacts in {run_dir}", flush=True)
    result = run_cyber_diagnostic(cases, FAILED_EXFILTRATION, run_dir, client)
    for stage in result["stages"]:
        print(f"{stage['stage']}: {stage['status']}  static errors: {len(stage['static_errors'])}")
        for run, report in stage.get("judgement", {}).items():
            print(f"  {run}: passed {report['passed']}, behavior present {report['behavior_present']}")
            for lead in report["true_component_leads"]:
                if "recall_percent" in lead:
                    print(f"    rank {lead['rank']} {lead['component']}: recall {lead['recall_percent']}%"
                          f"  precision {lead['precision_percent']}%")
                else:
                    print(f"    rank {lead['rank']} {lead['component']}: flags time in a clean run: "
                          f"{lead['flags_time_in_clean_run']}")
            if not report["true_component_leads"]:
                print("    the true component is not in the top 3")
    print(f"accepted: {result['accepted']} at {result['accepted_stage']}  ({result['wall_seconds']} s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
