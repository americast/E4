#!/usr/bin/env python3
"""One completion from the run's model, to check it answers.

    .venv/bin/python scripts/model/run.py --model gemma_3090 -- .venv/bin/python scripts/model/check.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from e4.models.analysis_model import model_from_environment
from e4.models.client import ModelClient


def main() -> int:
    client = ModelClient(model_from_environment())
    call = client.complete("Reply with exactly: OK", task_id="check_model", attempt=1, max_tokens=256)
    print(f"content: {call.content!r}")
    print(f"error: {call.error}")
    print(f"prompt tokens: {call.prompt_token_count}  usage: {call.usage}")
    return 0 if call.error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
