#!/usr/bin/env python3
"""One completion from the launched local model, to check it answers.

    .venv/bin/python scripts/run_m3_model.py --profile config/gemma_3090_inference.json -- \
        .venv/bin/python scripts/check_model.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.model_client import LocalModelClient


def main() -> int:
    profile = json.loads(Path(os.environ["E4_MODEL_PROFILE"]).read_text())
    client = LocalModelClient(profile, base_url=os.environ["E4_BASE_URL"])
    call = client.complete("Reply with exactly: OK", task_id="check_model", attempt=1, max_tokens=256)
    print(f"content: {call.content!r}")
    print(f"error: {call.error}")
    print(f"prompt tokens: {call.prompt_token_count}  usage: {call.usage}")
    return 0 if call.error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
