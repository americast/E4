#!/usr/bin/env python3
"""Prepare Bank metadata candidates, audit metric_app coverage, and freeze sample."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from e4infra.bank_prep import (audit_metric_app, clip_selected_bank, finalize_staged_bank,
                               prepare_metadata, sample_bank_cases)


RAW = ROOT / "data/raw/openrca/Bank"
PREPARED = ROOT / "data/prepared/bank"
AUDIT = ROOT / "artifacts/data_audit"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    metadata = sub.add_parser("metadata", help="Build public candidate windows and private candidate gold")
    metadata.add_argument("--raw-dir", type=Path, default=RAW)
    metadata.add_argument("--output-dir", type=Path, default=PREPARED)
    metadata.add_argument("--audit-dir", type=Path, default=AUDIT)
    coverage = sub.add_parser("audit-metric-app", help="Audit coverage from all candidate-date app metric files")
    coverage.add_argument("--metric-root", type=Path, default=RAW / "telemetry")
    coverage.add_argument("--candidates", type=Path, default=PREPARED / "candidate_incidents.jsonl")
    coverage.add_argument("--audit-dir", type=Path, default=AUDIT)
    coverage.add_argument("--coverage-fraction", type=float, default=0.8)
    sample = sub.add_parser("sample", help="Freeze ten scored and two disjoint development cases")
    sample.add_argument("--candidates", type=Path, default=PREPARED / "candidate_incidents.jsonl")
    sample.add_argument("--candidate-gold", type=Path,
                        default=PREPARED / "evaluator/candidate_ground_truth.jsonl")
    sample.add_argument("--eligible-ids", type=Path, default=AUDIT / "bank_metric_app_eligible_ids.json")
    sample.add_argument("--output-dir", type=Path, default=PREPARED)
    sample.add_argument("--audit-dir", type=Path, default=AUDIT)
    sample.add_argument("--seed", type=int, default=42)
    sample.add_argument("--freeze-config", type=Path, default=ROOT / "config/bank_sample.json")
    sample.add_argument("--overwrite-freeze", action="store_true")
    clip = sub.add_parser("clip-selected", help="Integrity-check and clip all four selected Bank telemetry tables")
    clip.add_argument("--raw-dir", type=Path, default=RAW)
    clip.add_argument("--output-dir", type=Path, default=PREPARED)
    clip.add_argument("--audit-dir", type=Path, default=AUDIT)
    clip.add_argument("--freeze-config", type=Path, default=ROOT / "config/bank_sample.json")
    clip.add_argument("--overwrite-public", action="store_true")
    clip.add_argument("--recover-progress", type=Path,
                      help="finalize validated staged clips using a complete scan progress JSONL, without rescanning source CSVs")
    clip.add_argument("--expected-counts", type=Path,
                      help="optional independent case/table/reference+alert row-count JSON for staged recovery")
    args = parser.parse_args()
    if args.command == "metadata":
        result = prepare_metadata(args.raw_dir, args.output_dir, args.audit_dir)
    elif args.command == "audit-metric-app":
        result = audit_metric_app(args.metric_root, args.candidates, args.audit_dir,
                                  coverage_fraction=args.coverage_fraction)
    elif args.command == "sample":
        result = sample_bank_cases(args.candidates, args.candidate_gold, args.eligible_ids,
                                   args.output_dir, args.audit_dir, seed=args.seed,
                                   freeze_path=args.freeze_config,
                                   overwrite_freeze=args.overwrite_freeze)
    elif args.command == "clip-selected" and args.recover_progress:
        result = finalize_staged_bank(args.raw_dir, args.output_dir, args.audit_dir,
                                      sample_path=args.freeze_config, progress_path=args.recover_progress,
                                      expected_counts_path=args.expected_counts)
    else:
        result = clip_selected_bank(args.raw_dir, args.output_dir, args.audit_dir,
                                    sample_path=args.freeze_config,
                                    overwrite_public=args.overwrite_public)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
