#!/usr/bin/env python3
"""Create a provisional, label-free pin for the fixed two-case M5 readiness run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.comparison.manifest import ROOT as PROJECT_ROOT, build_development_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--library-root", type=Path, required=True,
                        help="project artifact directory from the fresh M5 bootstrap result")
    parser.add_argument("--bootstrap-result", type=Path, required=True,
                        help="preserved M5 bootstrap_result.json matching --library-root")
    parser.add_argument("--harbor-source-pin", type=Path,
                        default=PROJECT_ROOT / "artifacts/m5/runtime/harbor_source_pin.json")
    parser.add_argument("--terminus-image-pin", type=Path,
                        default=PROJECT_ROOT / "artifacts/m5/runtime/terminus_task_image_pin.json")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    args = parser.parse_args()
    target = args.output or (args.root / "artifacts/m5/experiments" / args.run_id /
                             "development_manifest.json")
    manifest = build_development_manifest(root=args.root, run_id=args.run_id,
        initial_library_path=args.library_root,
        bootstrap_result_path=args.bootstrap_result,
        harbor_source_pin_path=args.harbor_source_pin,
        terminus_image_pin_path=args.terminus_image_pin,
        output_path=target)
    print(json.dumps({"manifest": str(target), "status": manifest["status"],
                      "split": manifest["split"], "case_ids": manifest["case_ids"],
                      "terminus_task_image_id": manifest["terminus_setup_pins"]["terminus_task_image_id"]},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
