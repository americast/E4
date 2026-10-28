#!/usr/bin/env python3
"""Prepare leak-separated OpenRCA Telecom metadata, audit metric_app coverage, and sample cases."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from e4.prep.telecom import audit_metric_app, prepare_metadata, run_sample
RAW = ROOT / "data/raw/openrca/Telecom"
PREPARED = ROOT / "data/prepared/telecom"
AUDIT = ROOT / "artifacts/data_audit"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    metadata = sub.add_parser("metadata", help="parse query windows and create label-separated candidates")
    metadata.add_argument("--raw-dir", type=Path, default=RAW)
    metadata.add_argument("--output-dir", type=Path, default=PREPARED)
    metadata.add_argument("--audit-dir", type=Path, default=AUDIT)
    metrics = sub.add_parser("audit-metric-app", help="audit duplicate keys and pre-alert coverage")
    metrics.add_argument("--telemetry-root", type=Path, default=RAW / "telemetry")
    metrics.add_argument("--candidates", type=Path, default=PREPARED / "candidate_alerts.jsonl")
    metrics.add_argument("--audit-dir", type=Path, default=AUDIT)
    metrics.add_argument("--minimum-timestamps", type=int, default=24,
                         help="minimum distinct minute timestamps in each 30-minute period (default 24)")
    sample = sub.add_parser("sample", help="freeze 10 scored plus 2 development cases after coverage audit")
    sample.add_argument("--candidates", type=Path, default=PREPARED / "candidate_alerts.jsonl")
    sample.add_argument("--candidate-gold", type=Path,
                        default=PREPARED / "evaluator/candidate_ground_truth.jsonl")
    sample.add_argument("--eligible-ids", type=Path, default=AUDIT / "eligible_incident_ids.json")
    sample.add_argument("--output-dir", type=Path, default=PREPARED)
    sample.add_argument("--audit-dir", type=Path, default=AUDIT)
    sample.add_argument("--seed", type=int, default=42)
    sample.add_argument("--freeze-config", type=Path, default=ROOT / "config/telecom/sample.json")
    sample.add_argument("--overwrite-freeze", action="store_true",
                        help="explicitly replace a previously frozen public sample declaration")
    args = parser.parse_args()
    if args.command == "metadata":
        report = prepare_metadata(args.raw_dir, args.output_dir, args.audit_dir)
    elif args.command == "audit-metric-app":
        report = audit_metric_app(args.telemetry_root, args.candidates, args.audit_dir,
                                  min_unique_timestamps=args.minimum_timestamps)
    else:
        report = run_sample(args.candidates, args.candidate_gold, args.eligible_ids,
                            args.output_dir, args.audit_dir, args.seed,
                            args.freeze_config, args.overwrite_freeze)
    print(json.dumps({key: report[key] for key in report
                      if key not in {"date_files", "per_incident", "strata"}}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
