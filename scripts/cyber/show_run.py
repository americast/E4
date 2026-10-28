"""Show the first rows of each table the cyber adapter makes from one run. The rows are the same
as old dissect's tables (AttackerActions, TokenUsage, AgentLog, AuditdMH, PasswdMH).

    .venv/bin/python scripts/cyber/show_run.py data/cyber/dissect_k3_i_eqs_t0
    .venv/bin/python scripts/cyber/show_run.py <run folder> --rows 10
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from e4.adapters.cyber import load_run, passwd_rows


def clock(timestamp_ms: int) -> str:
    """Milliseconds since the run's start -> +mm:ss."""
    seconds = timestamp_ms // 1000
    return f"+{seconds // 60:02d}:{seconds % 60:02d}"


def one_line(row, width: int = 160) -> str:
    """A row -> `key=value key=value ...` on one line, cut to `width` characters."""
    parts = []
    for key, value in row.items():
        if value is not None:
            parts.append(f"{key}={str(value)}".replace("\n", " "))
    return " ".join(parts)[:width]


parser = argparse.ArgumentParser()
parser.add_argument("run", type=Path)
parser.add_argument("--rows", type=int, default=5)
args = parser.parse_args()

telemetry = load_run(args.run)
logs = telemetry.logs
params = telemetry.params
print(f"run     {telemetry.telemetry_id}   {params['model']} / {params['harness']} / {params['environment']}")
print(f"span    {clock(telemetry.start_ms)} -> {clock(telemetry.end_ms)}   "
      f"(wall-clock start {params['wall_start']} UTC)")

tables = []
for record in logs:
    if record.table not in tables:
        tables.append(record.table)

for table in tables:
    in_table = [record for record in logs if record.table == table]
    columns = set()
    for record in in_table:
        columns.update(record.fields)
    print(f"\n{table}  ({len(in_table)} rows, {len(columns)} columns)")
    for record in in_table[:args.rows]:
        print(f"  {one_line(record.fields)}")

execve = [record for record in logs if record.kind == "EXECVE"]
print(f"\nfirst commands from audit EXECVE rows  ({len(execve)} total)")
for record in execve[:args.rows]:
    print(f"  {clock(record.timestamp_ms_utc)}  {record.entity.identifier:12} {' '.join(record.fields['argv'])[:120]}")

accounts = passwd_rows(args.run)
print(f"\nenvironment/passwd  ({len(accounts)} rows, kept in the deployment catalog)")
for row in accounts[:args.rows]:
    print(f"  {one_line(row)}")
