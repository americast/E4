#!/usr/bin/env python3
"""Convenience entrypoint delegating schema bootstrap to the restricted M4 supervisor."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_m4_bank import main as supervisor_main


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=ROOT / "config/gemma_m3_inference.json")
    parser.add_argument("--run-dir", type=Path, default=Path("m4_bootstrap"))
    args = parser.parse_args(argv)
    if not os.environ.get("E4_RUN_DIR") or not os.environ.get("E4_BASE_URL"):
        parser.error("invoke under scripts/run_m3_model.py with its E4_RUN_DIR and local E4_BASE_URL")
    configured_profile = os.environ.get("E4_MODEL_PROFILE")
    profile = args.profile.resolve(strict=True)
    if configured_profile and Path(configured_profile).resolve(strict=True) != profile:
        parser.error("profile must match E4_MODEL_PROFILE exported by the approved launcher")
    return supervisor_main(["--mode", "bootstrap", "--profile", str(profile),
                            "--run-dir", str(args.run_dir)])


if __name__ == "__main__":
    raise SystemExit(main())
