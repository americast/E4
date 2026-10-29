#!/usr/bin/env python3
"""Convenience entrypoint delegating schema bootstrap to the restricted M4 supervisor."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
for import_root in (ROOT / "src", ROOT):  # e4 lives in src; scripts.* at the repo root
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from scripts.bank.diagnose import main as supervisor_main


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, default=Path("m4_bootstrap"))
    args = parser.parse_args(argv)
    if not os.environ.get("E4_RUN_DIR") or not os.environ.get("E4_MODEL"):
        parser.error("invoke under scripts/model/run.py, which sets E4_RUN_DIR and E4_MODEL")
    return supervisor_main(["--mode", "bootstrap", "--run-dir", str(args.run_dir)])


if __name__ == "__main__":
    raise SystemExit(main())
