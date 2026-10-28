#!/usr/bin/env python3
"""Run the three-stage cyber diagnosis on the incalmo x enterprise_b pair, judged against the
failed-exfiltration ground truth. Run it under the model launcher so the model server is up:

    cd /home/marko/E4 && .venv/bin/python scripts/run_m3_model.py \
        --profile config/gemma_3090_inference.json -- .venv/bin/python scripts/run_cyber_m4.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.cyber_adapter import load_run
from e4core.cyber_controller import scope_cases
from e4core.cyber_ground_truth import FAILED_EXFILTRATION
from e4core.cyber_m4 import CyberModelClient, run_cyber_diagnostic

RUNS = ROOT / "data/cyber"
SCOPE_RUNS = ["dissect_qwen38_i_entb_t2", "dissect_qwen38_i_entb_t1"]   # failing, successful


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=None,
                        help="sampling seed for this try; the profile's seed when omitted")
    args = parser.parse_args()

    profile = json.loads(Path(os.environ["E4_MODEL_PROFILE"]).read_text())
    if args.seed is not None:
        profile["seed"] = args.seed
    print(f"model seed {profile['seed']}", flush=True)
    client = CyberModelClient(profile, base_url=os.environ["E4_BASE_URL"])

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
