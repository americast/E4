#!/usr/bin/env python3
"""Run one model-generated DAG on a frozen label-free Telecom dev case."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from e4core.development import replay_saved_development_dag, run_development_dag


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--split", choices=("dev",), default="dev")
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--replay-dag", type=Path,
                        help="Replay a previously saved model DAG without making a new model request")
    parser.add_argument("--verify-cache-parity", action="store_true",
                        help="For saved-DAG replay, compare cold-cache, warm-cache, and cache-disabled executions")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    if args.replay_dag is None and args.profile is None:
        parser.error("--profile is required unless --replay-dag is supplied")
    outer_run_dir = Path(os.environ["E4_RUN_DIR"]) if os.environ.get("E4_RUN_DIR") else None
    if args.replay_dag:
        output = (outer_run_dir / "development_replay" if outer_run_dir else
                  args.root / "artifacts/m3/dev" / args.run_id / "replay")
    else:
        output = (outer_run_dir / "development" if outer_run_dir else
                  args.root / "artifacts/m3/dev" / args.run_id / "development")
    if output.exists():
        parser.error(f"run directory already exists: {output}")
    if args.replay_dag:
        result = replay_saved_development_dag(args.case_id, args.split, args.replay_dag,
                                              run_dir=output, root=args.root,
                                              verify_cache_parity=args.verify_cache_parity)
        print(json.dumps({"run_dir": str(output), "case_id": args.case_id,
                          "dag_sha256": result["dag_sha256"], "cache_hits": result["cache_hits"]},
                         sort_keys=True))
        return 0
    result = run_development_dag(args.case_id, args.split, profile_path=args.profile,
                                 run_dir=output, root=args.root)
    print(json.dumps({"run_dir": str(output), "case_id": args.case_id,
                      "selected_kpi": result["selected_kpi"],
                      "cache_hits_warm": result["cache_hits_warm"],
                      "wall_seconds_including_load_and_model_call": result[
                          "wall_seconds_including_load_and_model_call"]},
                     ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
