#!/usr/bin/env python3
"""The smoke suite (JSON answer, generated operator) against the run's model.

    .venv/bin/python scripts/model/run.py --model smoke -- .venv/bin/python scripts/model/smoke.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.launch.smoke import main

if __name__ == "__main__":
    raise SystemExit(main())
