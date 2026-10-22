#!/usr/bin/env python3
"""Write the internal M5/M6 freeze manifest after both development runs are reviewed."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from e4core.m5_manifest import ROOT, build_frozen_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bootstrap-result", type=Path, required=True,
                        help="completed bootstrap_result.json; its snapshot/path/cost are pinned directly")
    parser.add_argument("--harbor-source-pin", type=Path,
                        default=ROOT / "artifacts/m5/runtime/harbor_source_pin.json")
    parser.add_argument("--terminus-image-pin", type=Path,
                        default=ROOT / "artifacts/m5/runtime/terminus_task_image_pin.json",
                        help="setup_terminus.py output containing the immutable Docker image ID")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    target = args.output or (args.root / "artifacts/m5/experiments" / args.run_id / "frozen_manifest.json")
    source_pin = json.loads(args.harbor_source_pin.read_text(encoding="utf-8"))
    image_pin = json.loads(args.terminus_image_pin.read_text(encoding="utf-8"))
    source_path = Path(source_pin["harbor_source_path"])
    if not source_path.is_absolute():
        source_path = args.root / source_path
    manifest = build_frozen_manifest(
        root=args.root, run_id=args.run_id,
        terminus_upstream_pin=source_pin,
        terminus_image_pin=image_pin,
        terminus_source_path=source_path,
        terminus_image_pin_path=args.terminus_image_pin,
        initial_library_path=None,
        initial_library_snapshot_id=None,
        bootstrap_result_path=args.bootstrap_result,
        output_path=target)
    print(json.dumps({"manifest": str(target), "status": manifest["status"],
                      "case_count": len(manifest["case_ids"]),
                      "local_pin_count": len(manifest["local_pins"]["files"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
