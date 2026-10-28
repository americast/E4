#!/usr/bin/env python3
"""The cyber flow end to end on the incalmo x enterprise_b pair: load each run, build one Case
per run, run each seed playbook on every Case, and judge the answers against the ground truth.

    cd /home/marko/E4 && .venv/bin/python scripts/run_cyber_example.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.cyber_adapter import load_run
from e4core.cyber_controller import cyber_runtime, load_seed_playbooks, run_scope, scope_cases
from e4core.cyber_ground_truth import FAILED_EXFILTRATION
from e4core.cyber_judge import judge, recall_precision, same_component
from e4core.cyber_ops import cyber_registry

RUNS = ROOT / "data/cyber"
FAILING_RUN = "dissect_qwen38_i_entb_t2"
SUCCESSFUL_RUN = "dissect_qwen38_i_entb_t1"
# which ground truth each seed playbook is checked against
TRUTH_FOR_PLAYBOOK = {"actions_with_errors": FAILED_EXFILTRATION,
                      "high_level_actions": FAILED_EXFILTRATION}


def describe_answer(answer, truth, true_intervals) -> None:
    if not answer.leads:
        print("      no leads")
    for lead in answer.leads[:3]:
        intervals = lead.intervals or ()
        print(f"      lead {lead.entity.role}:{lead.entity.identifier}  score {lead.score:g}  "
              f"{len(intervals)} interval(s)")
        if same_component(lead.entity, truth.component) and true_intervals:
            recall, precision = recall_precision(intervals, true_intervals)
            print(f"        against {len(true_intervals)} true interval(s): "
                  f"recall {recall:.3f}  precision {precision:.3f}")
    if not true_intervals:
        print("      truth: the behavior is absent in this run")


def main() -> None:
    registry = cyber_registry()
    runtime = cyber_runtime()

    print("1. Load each run in scope into a Telemetry (timestamps in ms since the run's start)")
    telemetries = {}
    for name in (FAILING_RUN, SUCCESSFUL_RUN):
        telemetries[name] = load_run(RUNS / name)
        minutes = telemetries[name].end_ms / 60000
        print(f"   {name}: {len(telemetries[name].logs)} log records over {minutes:.1f} min")

    print("2. One Case per run: that run is the anomaly, the other run is the baseline")
    cases = scope_cases(telemetries)
    for case in cases.values():
        print(f"   {case.case_id}: {len(case.components)} components")

    for playbook_name, playbook in load_seed_playbooks().items():
        truth = TRUTH_FOR_PLAYBOOK[playbook_name]
        print(f"3. Playbook {playbook_name}: run on every Case")
        answers = run_scope(playbook, cases, registry, runtime)
        for name, answer in answers.items():
            print(f"   {name}")
            describe_answer(answer, truth, truth.runs[name])
        accepted = judge(truth, answers)
        print(f"4. Judge against the {truth.component.identifier} ground truth: accepted = {accepted}")


if __name__ == "__main__":
    main()
