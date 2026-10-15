#!/usr/bin/env python3
"""Write a local provenance snapshot without exposing environment values or credentials."""

import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(args):
    try:
        return subprocess.run(args, text=True, capture_output=True, timeout=10).stdout.strip()
    except Exception as exc:
        return f"unavailable: {type(exc).__name__}"


def main():
    gpu = command(["nvidia-smi", "--query-gpu=index,name,pci.bus_id,memory.total,memory.free,utilization.gpu",
                   "--format=csv,noheader,nounits"])
    git_rev = command(["git", "-C", str(ROOT / "third_party/llama.cpp"), "rev-parse", "HEAD"])
    data = {
        "recorded_utc": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(), "platform": platform.platform(),
        "gpu_snapshot": gpu, "llama_cpp_revision": git_rev,
        "cuda_version": command(["nvcc", "--version"]),
    }
    path = ROOT / "artifacts/infrastructure/environment.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")
    print(path)


if __name__ == "__main__":
    main()
