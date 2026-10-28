"""Attacker runs -> one Telemetry per run, and a Case comparing one anomalous run to baselines.

Audit and token rows are cut to the attacker's window, attacker.started_at..finished_at (both
ends included), the only two values read from experiment/. A run's Telemetry spans every record
it keeps; record timestamps (`timestamp_ms_utc`) are milliseconds since the earliest, so every run
starts at 0, and the wall-clock start is kept in `params["wall_start"]`.

Every source becomes the same rows, with the same columns and values, as old dissect's table for
it. Each row is one LogRecord: `fields` holds the row, `text` the raw source text.

    actions.json       -> AttackerActions: one row per action, nested JSON flattened
    token_usage*.json  -> TokenUsage: one row per model call
    llm.log, supervisor.log, *_realtime_context.txt -> AgentLog: one row per log message
    audit.log*         -> AuditdMH: one row per audit line, EXECVE argv assembled, hex decoded
    passwd             -> PasswdMH: one row per (host, account), kept in the deployment catalog
                          because it has no timestamp
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from dataclasses import replace
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from .contracts import Case, EntityRef, LogRecord, Telemetry, TelemetryWindow

ADAPTER_VERSION = "cyber-runs/0.6"

TOKEN_COLUMNS = ["subagent", "timestamp", "call_type", "model", "step", "response_id",
                 "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens",
                 "reasoning_tokens", "total_tokens", "high_level_action_id",
                 "high_level_action_name", "low_level_action_id", "low_level_action_name",
                 "cumulative_input_tokens", "cumulative_output_tokens", "cumulative_total_tokens"]

LOGGED = {"llm.log": re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) ([A-Z]+):(.*)$"),
          "supervisor.log": re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) - ([A-Z]+) - (.*)$")}
LOGGED_AGENT = {"llm.log": "main", "supervisor.log": "supervisor"}
SPOKEN = re.compile(r"^\[(\d\d:\d\d:\d\d)\] ([A-Z_]+): ?(.*)$")
STARTED = re.compile(r"^Started: (\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) UTC$")
TRANSCRIPT = "_realtime_context.txt"

AUDIT_HEADER = re.compile(r"^type=(\S+) msg=audit\(([\d.]+):(\d+)\):\s*(.*)$")
AUDIT_TOKEN = re.compile(r"(\w+(?:\[\d+\])?)=(\"[^\"]*\"|'[^']*'|\S+)")
HEX = re.compile(r"(?:[0-9A-Fa-f]{2})+")
HEX_FIELDS = ("name", "proctitle", "cwd", "exe", "comm", "cmd")
ARGUMENT = re.compile(r"a\d+")
FRAGMENT = re.compile(r"a(\d+)\[(\d+)\]")
FAMILY = {1: "unix", 2: "inet", 10: "inet6"}
NUMERIC = ("pid", "ppid", "auid", "uid", "gid", "euid", "suid", "fsuid", "egid", "sgid", "fsgid",
           "ses", "exit", "item", "items", "argc", "syscall", "port", "serial")

# The components a diagnosis may name, for incalmo against enterprise_b only (for now).
MODEL_COMPONENTS = ["context", "reasoning"]
# incalmo's high-level actions, spelled as its actions.json spells them
INCALMO_COMPONENTS = ["Scan", "FindInformationOnAHost", "LateralMoveToHost", "EscelatePrivledge",
                      "ExfiltrateData"]
# incalmo's low-level actions, as exported by Incalmo/incalmo/core/actions/LowLevel/__init__.py
INCALMO_LOW_LEVEL_COMPONENTS = [
    "ExploitStruts", "NCLateralMove", "SSHLateralMove", "ListFilesInDirectory", "ReadFile",
    "SCPFile", "wgetFile", "ScanHost", "ScanNetwork", "FindSSHConfig", "MD5SumAttackerData",
    "CopyFile", "AddSSHKey", "RunBashCommand", "WriteFile", "GetSudoVersion",
    "CheckPasswdPermissions", "SudoeditExploit", "WriteableSudoersExploit", "SudoBaronExploit",
    "WriteablePasswdExploit", "HTTPRequest", "HTTPRequestWithToken", "OAuthClientCredentials",
    "HTTPMethodFuzz", "APIEndpointProbe", "BrokenObjectLevelAuth", "MissingFunctionLevelAuth",
    "FuzzAPIParameter"]
# copied from dissect/src/dissect/ag/environments/enterprise_b.py
ENTERPRISE_B_COMPONENTS = {
    "host": (["attacker"] + [f"webserver{i}" for i in range(10)]
             + [f"employeea{i}" for i in range(10)] + [f"employeeb{i}" for i in range(10)]
             + [f"database{i}" for i in range(10)]),
    "user": ["root", "ubuntu", "tomcat"],
    "service": ["sshd", "http"],
    "subnet": ["attacker_subnet", "webserver_subnet", "employee_a_subnet", "employee_b_subnet",
               "database_subnet"],
    "vuln": ["CVE-2017-5638", "CVE-2021-3156"],
}


def naive_utc(text: str) -> datetime:
    """An ISO timestamp -> naive UTC. Naive input is already UTC; zoned input is converted."""
    moment = datetime.fromisoformat(text.replace(",", "."))
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return moment


def epoch_ms(moment: datetime) -> int:
    return int(moment.replace(tzinfo=timezone.utc).timestamp() * 1000)


def iso(moment: datetime) -> str:
    return moment.isoformat(timespec="microseconds")


def agent_entity(name: str) -> EntityRef:
    return EntityRef("agent", name)


# ---- actions.json -> AttackerActions ---------------------------------------------------------

def flat(record: dict, prefix: str = "") -> dict:
    """Nested JSON -> one flat dict with dotted keys, lists kept as their JSON text."""
    out = {}
    for key, value in record.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(flat(value, f"{name}."))
        elif isinstance(value, list):
            out[name] = json.dumps(value)
        else:
            out[name] = value
    return out


def action_records(run: Path) -> list[LogRecord]:
    """One row per action, every field it carried, plus its length in seconds and its event count."""
    records = []
    for path in sorted(run.glob("attacker/actions.json")):
        for number, line in enumerate(path.read_text().splitlines()):
            if not line.strip():
                continue
            row = flat(json.loads(line))
            start = naive_utc(row["ts_start"])
            finish = naive_utc(row["ts_finish"])
            row["ts_start"] = iso(start)
            row["ts_finish"] = iso(finish)
            row["seconds"] = (finish - start) / timedelta(microseconds=1) / 1e6
            if "events" in row:
                events = row["events"]
                if events is None or events == "[]":
                    row["event_count"] = 0
                else:
                    row["event_count"] = events.count('"class_name"')
            host = row.get("action_params.agent.hostname")
            records.append(LogRecord(
                table="attacker/actions.json", timestamp_ms_utc=epoch_ms(start),
                entity=EntityRef("host", host) if host else agent_entity("main"),
                kind=str(row["action_name"]), fields=row, text=line,
                row_key=f"{run.name}:action:{number}"))
    return records


# ---- token_usage*.json -> TokenUsage ---------------------------------------------------------

def token_records(run: Path, start: datetime | None, finish: datetime | None) -> list[LogRecord]:
    """One row per model call in [start, finish], in old dissect's fixed columns; a column a call
    lacks is null."""
    records = []
    for path in sorted(run.glob("attacker/token_usage*.json*")):
        subagent = path.stem.replace("token_usage", "").lstrip("_") or "main"
        for number, line in enumerate(path.read_text().splitlines()):
            if not line.strip():
                continue
            call = {"subagent": subagent, **json.loads(line)}
            row = {column: call.get(column) for column in TOKEN_COLUMNS}
            moment = naive_utc(str(row["timestamp"]))
            if start is not None and moment < start:
                continue
            if finish is not None and moment > finish:
                continue
            row["timestamp"] = iso(moment)
            records.append(LogRecord(
                table=f"attacker/{path.name}", timestamp_ms_utc=epoch_ms(moment),
                entity=agent_entity(subagent), kind=str(row["call_type"]), fields=row,
                text=line, row_key=f"{run.name}:{path.name}:{number}"))
    return records


# ---- llm.log, supervisor.log, subagent transcripts -> AgentLog --------------------------------

def logged(lines: list[str], header: re.Pattern) -> list:
    """A `logging` file's lines -> [timestamp, level, [message lines]] per record."""
    rows = []
    for line in lines:
        head = header.match(line)
        if head:
            rows.append([datetime.strptime(head[1], "%Y-%m-%d %H:%M:%S,%f"), head[2], [head[3]]])
        elif rows:
            rows[-1][2].append(line)
        elif line:
            rows.append([None, None, [line]])
    return rows


def spoken(lines: list[str]) -> list:
    """A subagent transcript's lines -> [timestamp, role, [message lines]] per record. A line holds
    only the time of day, so the date is the last `Started:` line's, rolled over at midnight."""
    rows, last = [], None
    for line in lines:
        begun = STARTED.match(line)
        if begun:
            last = datetime.strptime(begun[1], "%Y-%m-%d %H:%M:%S")
        head = SPOKEN.match(line)
        if head and last is not None:
            at = datetime.combine(last.date(), time.fromisoformat(head[1]))
            if at < last - timedelta(hours=12):
                at += timedelta(days=1)
            last = at
            rows.append([at, head[2], [head[3]]])
        elif rows:
            rows[-1][2].append(line)
        elif line:
            rows.append([None, None, [line]])
    return rows


def agent_log_records(run: Path) -> list[LogRecord]:
    """One row per log message: agent, timestamp, level, and every line up to the next message."""
    paths = [*run.glob("attacker/llm.log"), *run.glob("attacker/action_logs/supervisor.log"),
             *run.glob(f"attacker/action_logs/*{TRANSCRIPT}")]
    paths.sort(key=lambda p: (p.name not in LOGGED, p.name))
    records = []
    for path in paths:
        # "\n" only: splitlines() would also cut at the \r and \x1c inside captured output
        lines = path.read_text(errors="replace").removesuffix("\n").split("\n")
        if path.name in LOGGED:
            agent = LOGGED_AGENT[path.name]
            messages = logged(lines, LOGGED[path.name])
        else:
            agent = path.name.removesuffix(TRANSCRIPT)
            messages = spoken(lines)
        for number, (moment, level, text) in enumerate(messages):
            # text before the first timestamped message (an artemis transcript's header) has no time
            if moment is None:
                continue
            message = "\n".join(text)
            row = {"agent": agent, "timestamp": iso(moment), "level": level, "message": message}
            records.append(LogRecord(
                table=f"attacker/{path.name}", timestamp_ms_utc=epoch_ms(moment),
                entity=agent_entity(agent), kind=level, fields=row, text=message,
                row_key=f"{run.name}:{path.name}:{number}"))
    return records


# ---- audit.log* -> AuditdMH ------------------------------------------------------------------

def hex_text(value: str) -> str:
    return bytes.fromhex(value).decode("utf-8", "replace").replace("\0", " ")


def audit_age(path: Path) -> int:
    """audit.log.12 -> 12, audit.log -> 0. Higher is older."""
    tail = path.name.rsplit(".", 1)[-1]
    return int(tail) if tail.isdigit() else 0


def typed(row: dict) -> dict:
    """Numeric fields -> int (null when not a number); every other field stays text."""
    for name in NUMERIC:
        if name in row and row[name] is not None:
            try:
                row[name] = int(row[name])
            except ValueError:
                row[name] = None
    return row


def parse_audit_line(line: str, host: str):
    """One audit line -> (row, fragments) or None. Fragments are EXECVE `aN[M]` argument pieces."""
    match = AUDIT_HEADER.match(line)
    if not match:
        return None
    record_type, epoch, serial, rest = match.groups()
    row = {"host": host, "epoch": float(epoch), "serial": int(serial), "type": record_type}
    fragments = {}
    for key, value in AUDIT_TOKEN.findall(rest):
        quoted = value[:1] in "\"'"
        if quoted:
            value = value[1:-1]
        if key == "msg" and "=" in value:
            for inner_key, inner_value in AUDIT_TOKEN.findall(value):
                if inner_value[:1] in "\"'":
                    inner_value = inner_value[1:-1]
                row[inner_key] = inner_value
            continue
        if "[" in key:
            fragment = FRAGMENT.fullmatch(key)
            if fragment:
                fragments[(int(fragment[1]), int(fragment[2]))] = (value, quoted)
                continue
        is_argument = record_type == "EXECVE" and ARGUMENT.fullmatch(key)
        if not quoted and (key in HEX_FIELDS or is_argument) and HEX.fullmatch(value):
            value = hex_text(value)
        row[key] = value
    return row, fragments


def add_socket_address(row: dict) -> None:
    """SOCKADDR's hex `saddr` -> its family, and for inet its port and address."""
    raw = bytes.fromhex(row["saddr"])
    family = int.from_bytes(raw[:2], "little")
    row["fam"] = FAMILY.get(family, "other")
    if family == 2 and len(raw) >= 8:
        row["port"] = int.from_bytes(raw[2:4], "big")
        row["addr"] = str(ipaddress.IPv4Address(raw[4:8]))
    elif family == 10 and len(raw) >= 24:
        row["port"] = int.from_bytes(raw[2:4], "big")
        address = ipaddress.IPv6Address(raw[8:24])
        # IPv4-mapped addresses are written dotted (::ffff:192.168.0.1), as newer Pythons do
        if address.ipv4_mapped is not None:
            row["addr"] = f"::ffff:{address.ipv4_mapped}"
        else:
            row["addr"] = str(address)


def host_audit_rows(paths: list[Path], host: str, start: float,
                    finish: float) -> list[tuple[dict, str]]:
    """One host's audit.log*, oldest file first -> (row, raw text) per record whose epoch seconds
    lie in [start, finish].
    An EXECVE's argument pieces and continuation lines are joined into its one `argv` row."""
    rows = []
    execve = {}           # serial -> [row, raw lines]
    pieces = {}           # serial -> argument index -> piece index -> (value, quoted)
    for path in sorted(paths, key=audit_age, reverse=True):
        for line in path.read_text(errors="replace").splitlines():
            parsed = parse_audit_line(line, host)
            if parsed is None:
                continue
            row, fragments = parsed
            if not start <= row["epoch"] <= finish:
                continue
            serial = row["serial"]
            if row["type"] != "EXECVE":
                if row["type"] == "SOCKADDR" and HEX.fullmatch(row.get("saddr", "")):
                    add_socket_address(row)
                rows.append((row, line))
                continue
            for (argument, piece), value in fragments.items():
                pieces.setdefault(serial, {}).setdefault(argument, {})[piece] = value
            for key in [key for key in row if key.startswith("a") and key.endswith("_len")]:
                row.pop(key)
            if "argc" in row:
                row["argv"] = [row.pop(f"a{i}", None) for i in range(int(row["argc"]))]
                execve[serial] = [row, [line]]
            else:
                first, raw_lines = execve[serial]
                for key in [key for key in row if ARGUMENT.fullmatch(key)]:
                    first["argv"][int(key[1:])] = row[key]
                raw_lines.append(line)

    for serial, (row, raw_lines) in execve.items():
        for argument, parts in pieces.get(serial, {}).items():
            joined = "".join(value for _, (value, _) in sorted(parts.items()))
            if all(not quoted and HEX.fullmatch(value) for value, quoted in parts.values()):
                joined = hex_text(joined)
            row["argv"][argument] = joined
        rows.append((row, "\n".join(raw_lines)))
    return rows


def audit_records(run: Path, start: datetime | None, finish: datetime | None) -> list[LogRecord]:
    """Every host's audit.log* -> one row per audit record in [start, finish]; None is unbounded."""
    start_seconds = float("-inf") if start is None else start.replace(tzinfo=timezone.utc).timestamp()
    finish_seconds = float("inf") if finish is None else finish.replace(tzinfo=timezone.utc).timestamp()
    by_host = {}
    for path in sorted(run.glob("environment/*/audit.log*")):
        by_host.setdefault(path.parent.name, []).append(path)
    records = []
    for host in sorted(by_host):
        for row, raw in host_audit_rows(by_host[host], host, start_seconds, finish_seconds):
            moment = datetime.fromtimestamp(row.pop("epoch"), timezone.utc).replace(tzinfo=None)
            row = {"timestamp": iso(moment), **typed(row)}
            records.append(LogRecord(
                table="environment/audit.log", timestamp_ms_utc=epoch_ms(moment),
                entity=EntityRef("host", host), kind=row["type"], fields=row, text=raw,
                row_key=f"{run.name}:{host}:{row['serial']}:{len(records)}"))
    return records


# ---- passwd -> PasswdMH ----------------------------------------------------------------------

def passwd_rows(run: Path) -> list[dict]:
    """Every host's passwd -> one row per (host, account)."""
    rows = []
    for path in sorted(run.glob("environment/*/passwd")):
        host = path.parent.name
        for line in path.read_text(errors="replace").splitlines():
            field = line.split(":")
            if len(field) >= 7 and field[2].isdigit():
                rows.append({"host": host, "user": field[0], "uid": int(field[2]),
                             "gid": int(field[3]), "home": field[5], "shell": field[6]})
    return rows


# ---- one run -> Telemetry; runs -> Case --------------------------------------------------------

def attacker_window(run: Path) -> tuple[datetime | None, datetime | None]:
    """attacker.started_at and attacker.finished_at from the run's experiment config and result, as
    naive UTC; None where the run does not say. Nothing else in experiment/ is read."""
    held = {}
    for name in ("experiment_config.json", "experiment_result.json"):
        found = next(run.glob(f"**/{name}"), None)
        if found:
            attacker = json.loads(found.read_text()).get("attacker") or {}
            for key in ("started_at", "finished_at"):
                if key in attacker:
                    held[key] = attacker[key]
    window = []
    for key in ("started_at", "finished_at"):
        value = held.get(key)
        if not value:
            window.append(None)
        else:
            window.append(datetime.fromtimestamp(value, timezone.utc).replace(tzinfo=None))
    return window[0], window[1]


def run_params(run: Path, wall_start_ms: int) -> dict:
    """The run's name, model, harness and environment from attacker_config.json, plus its
    wall-clock start. incalmo and shell configs name a strategy; artemis configs name a model."""
    config = json.loads((run / "attacker/attacker_config.json").read_text())
    strategy = config.get("strategy", {})
    wall_start = datetime.fromtimestamp(wall_start_ms / 1000, timezone.utc).replace(tzinfo=None)
    return {"name": run.name,
            "model": strategy.get("planning_llm", config.get("model")),
            "harness": strategy.get("abstraction", "artemis" if "supervisor_model" in config else None),
            "environment": config.get("environment"),
            "wall_start": iso(wall_start),
            "attacker_config": config}


def files_read(run: Path) -> list[Path]:
    patterns = ("experiment/experiment_config.json", "experiment/experiment_result.json",
                "attacker/actions.json", "attacker/token_usage*.json*", "attacker/llm.log",
                "attacker/action_logs/supervisor.log", f"attacker/action_logs/*{TRANSCRIPT}",
                "attacker/attacker_config.json", "environment/*/audit.log*", "environment/*/passwd")
    files = []
    for pattern in patterns:
        files += sorted(run.glob(pattern))
    return files


def load_run(run: Path) -> Telemetry:
    """One run -> its Telemetry, spanning [0, duration) in milliseconds since its earliest record."""
    started, finished = attacker_window(run)
    actions = action_records(run)
    absolute = (actions + token_records(run, started, finished) + agent_log_records(run)
                + audit_records(run, started, finished))

    times = [record.timestamp_ms_utc for record in absolute]
    times += [epoch_ms(naive_utc(record.fields["ts_finish"])) for record in actions]
    wall_start = min(times)
    wall_end = max(times) + 1

    logs = []
    for record in absolute:
        logs.append(replace(record, timestamp_ms_utc=record.timestamp_ms_utc - wall_start))

    hosts = sorted(path.name for path in (run / "environment").iterdir() if path.is_dir())
    catalog = {"entities": {host: {"source_fields": ["environment/audit.log"],
                                   "relationship": "unknown"} for host in hosts},
               "passwd": passwd_rows(run)}

    file_sha256, file_bytes = {}, {}
    for path in files_read(run):
        key = str(path.relative_to(run))
        file_sha256[key] = hashlib.sha256(path.read_bytes()).hexdigest()
        file_bytes[key] = path.stat().st_size
    data_sha256 = hashlib.sha256(json.dumps(file_sha256, sort_keys=True).encode()).hexdigest()

    return Telemetry(
        case_id=run.name, reference_window=None, alert_window=None, metrics=(), spans=(),
        deployment_catalog=catalog, data_sha256=data_sha256, file_sha256=file_sha256,
        duplicate_counts={}, quality_counts={}, adapter_version=ADAPTER_VERSION,
        file_bytes=file_bytes, logs=logs, telemetry_id=run.name,
        start_ms=0, end_ms=wall_end - wall_start,
        params={**run_params(run, wall_start),
                "attacker_started_at": iso(started) if started else None,
                "attacker_finished_at": iso(finished) if finished else None})


def whole_run(telemetry: Telemetry) -> TelemetryWindow:
    """A window over the whole of a run."""
    return TelemetryWindow(f"{telemetry.telemetry_id}:whole", telemetry,
                           telemetry.start_ms, telemetry.end_ms)


def cyber_components() -> tuple[EntityRef, ...]:
    """Model, harness and environment components, the role naming the group."""
    components = [EntityRef("model", name) for name in MODEL_COMPONENTS]
    components += [EntityRef("harness", name) for name in INCALMO_COMPONENTS]
    components += [EntityRef("harness", name) for name in INCALMO_LOW_LEVEL_COMPONENTS]
    for kind, names in ENTERPRISE_B_COMPONENTS.items():
        components += [EntityRef(f"environment.{kind}", name) for name in names]
    return tuple(components)


def load_cyber_case(anomalous_run: Path, baseline_runs: list[Path]) -> Case:
    """One anomalous run and its baseline runs -> a Case of whole-run windows."""
    anomalous = whole_run(load_run(anomalous_run))
    baselines = [whole_run(load_run(run)) for run in baseline_runs]
    names = "+".join(run.name for run in baseline_runs)
    return Case(case_id=f"{anomalous_run.name}__vs__{names}",
                windows=(anomalous, *baselines), anomaly=anomalous.window_id,
                components=cyber_components())


if __name__ == "__main__":
    RUNS = Path(__file__).resolve().parents[1] / "data/cyber"
    # incalmo x enterprise_b, from old dissect's misc/findings/outcomes_all.csv: of its 15 runs
    # (glm-5.2, kimi-k3, qwen38 x 5 trials) only qwen38 t1 achieved every goal; the rest got 0%.
    # a failing run that shows the known failure: database-to-attacker exfiltration after infection
    FAILING = "dissect_qwen38_i_entb_t2"
    SUCCESSFUL = ["dissect_qwen38_i_entb_t1"]
    case = load_cyber_case(RUNS / FAILING, [RUNS / run for run in SUCCESSFUL])
    print(f"case {case.case_id}")
    for window in case.windows:
        role = "anomaly " if window.window_id == case.anomaly else "baseline"
        counts = {}
        for record in window.logs:
            counts[record.table] = counts.get(record.table, 0) + 1
        params = window.telemetry.params
        print(f"  {role} {window.window_id:28} {window.start_ms}..{window.end_ms} ms   "
              f"{params['model']} / {params['harness']} / {params['environment']}   "
              f"wall start {params['wall_start']}")
        print("           " + "   ".join(f"{table} {n}" for table, n in counts.items()))
        assert window.logs and min(r.timestamp_ms_utc for r in window.logs) == 0
    assert case.anomalous().window_id == case.anomaly and len(case.baselines()) == 1

    by_role = {}
    for component in case.components:
        by_role.setdefault(component.role, []).append(component.identifier)
    print(f"  components ({len(case.components)})")
    for role, names in by_role.items():
        print(f"    {role:20} {', '.join(names) if len(names) <= 6 else ', '.join(names[:6]) + f' ... ({len(names)})'}")
