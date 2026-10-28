#!/usr/bin/env python3
"""Audit selected OpenRCA telemetry and create blind per-case public clips."""
from __future__ import annotations

import argparse
import fcntl
import json
import sys
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from e4.prep.telecom_audit import audit_and_clip  # noqa: E402


@contextmanager
def output_lock(lock_path: Path):
    """Prevent concurrent clip writers from leaving mixed output generations."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another telemetry clip run holds {lock_path}") from exc
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def output_lock_path(output_root: Path) -> Path:
    resolved = output_root.resolve()
    return resolved.parent / f".{resolved.name}.clip.lock"


def with_output_lock(output_root: Path, audit_path: Path, action):
    """Run one generation under the destination lock; audit paths do not define ownership."""
    del audit_path
    with output_lock(output_lock_path(output_root)):
        return action()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scored", type=Path, default=ROOT / "data/prepared/telecom/scored_incidents.jsonl")
    parser.add_argument("--dev", type=Path, default=ROOT / "data/prepared/telecom/dev_incidents.jsonl")
    parser.add_argument("--telemetry-root", type=Path, default=ROOT / "data/raw/openrca/Telecom/telemetry")
    parser.add_argument("--mapping", type=Path, default=ROOT / "data/raw/downloads/AppTelecomDeploymentList.xlsx")
    parser.add_argument("--output", type=Path, default=ROOT / "data/prepared/telecom/public")
    parser.add_argument("--audit", type=Path, default=ROOT / "artifacts/data_audit/telecom_telemetry_audit.json")
    parser.add_argument("--scratch", type=Path, default=ROOT / "artifacts/data_audit/trace-index-scratch")
    args = parser.parse_args()
    cases = []
    for name, path in (("scored", args.scored), ("dev", args.dev)):
        for line in path.read_text().splitlines():
            if line.strip():
                case = json.loads(line)
                case.pop("split", None)
                cases.append(case)
    result = with_output_lock(args.output, args.audit,
        lambda: audit_and_clip(cases, args.telemetry_root, args.mapping, args.output,
                               args.audit, scratch_root=args.scratch))
    print(json.dumps({"selected_cases": result["selected_case_count"],
                      "selected_dates": result["selected_dates"],
                      "audited_files": len(result["file_audits"]),
                      "public_clips": result["clip_count"],
                      "audit": str(args.audit), "public_data_root": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
