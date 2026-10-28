#!/usr/bin/env python3
"""Run M4 bootstrap or one development-only Bank diagnosis in a restricted view."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import select
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
MAX_FEEDBACK_BYTES = 16 * 1024
MAX_FEEDBACK_COMPONENT_CHARS = 256
CASE_DIR_PREFIX = "bank-"
VIEW_CONFIG_FILES = (
    "bank/metric_kind_policy.json", "bank/kpi_policy.json", "bank/seed_playbooks.json",
    "bank/controller_policy.json", "runtime/schema.json", "runtime/values.json", "records/decisions.json",
    "telecom/metric_kind_policy.json", "telecom/kpi_policy.json",
)
VIEW_ROOT_FILES = ("pyproject.toml", "requirements.lock")
# package files the restricted view leaves out: the gold evaluator, and code no Bank run uses
VIEW_EXCLUDED_CODE = ("judge/bank.py", "baselines", "launch", "prep/telecom_audit.py")
NATIVE_TABLE_SCHEMAS = {
    "metric/metric_app.csv": ["timestamp", "rr", "sr", "cnt", "mrt", "tc"],
    "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
    "trace/trace_span.csv": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
    "log/log_service.csv": ["log_id", "timestamp", "cmdb_id", "log_name", "value"],
}


class SupervisorError(RuntimeError):
    pass


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(value), ensure_ascii=False, indent=2,
                               sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def _copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def _view_code_ignore(package_root: Path) -> Callable[[str, list[str]], list[str]]:
    """A copytree ignore that drops VIEW_EXCLUDED_CODE and compiled files from the package copy."""
    def ignore(directory: str, names: list[str]) -> list[str]:
        skipped = []
        for name in names:
            relative = (Path(directory) / name).relative_to(package_root).as_posix()
            if relative in VIEW_EXCLUDED_CODE or name == "__pycache__" or name.endswith(".pyc"):
                skipped.append(name)
        return skipped
    return ignore


def _safe_case_id(value: str) -> bool:
    return (isinstance(value, str) and value.startswith(CASE_DIR_PREFIX) and
            Path(value).name == value and value not in {".", ".."} and "/" not in value and "\\" not in value)


def _filtered_sample(sample: Mapping[str, Any], case_id: str, split: str = "dev") -> dict[str, Any]:
    split_key = "development_cases" if split == "dev" else "scored_cases"
    selected = [item for item in sample.get(split_key, [])
           if isinstance(item, Mapping) and item.get("incident_id") == case_id]
    if len(selected) != 1:
        raise SupervisorError(f"case is not a unique frozen Bank {split} case")
    return {"dataset": sample.get("dataset", "OpenRCA Bank"), split_key: [dict(selected[0])]}


def _prepare_public_manifest_view(root: Path, view_root: Path, case_id: str,
                                  split: str = "dev") -> dict[str, Any]:
    """Copy one split case and rewrite public manifests to disclose only that case."""
    if not _safe_case_id(case_id):
        raise SupervisorError("case ID is not a single frozen Bank case identifier")
    config_dir = view_root / "config"
    public_src = root / "data/prepared/bank/public"
    bank_src = root / "data/prepared/bank"
    original_sample = json.loads((root / "config/bank/sample.json").read_text(encoding="utf-8"))
    source_manifest_path = root / "config/bank/input_manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    original_sample_path = root / "config/bank/sample.json"
    split_filename = "dev_incidents.jsonl" if split == "dev" else "scored_incidents.jsonl"
    split_hash_key = f"{split}_manifest_sha256"
    original_dev_path = bank_src / split_filename
    original_clip_path = public_src / "clip_manifest.jsonl"
    original_catalog_path = public_src / "deployment_catalog.json"
    if _sha_file(original_sample_path) != source_manifest.get("sample_config_sha256"):
        raise SupervisorError("original sample config does not match the pinned input manifest")
    if _sha_file(original_dev_path) != source_manifest.get(split_hash_key):
        raise SupervisorError(f"original {split} manifest does not match the pinned input manifest")
    if _sha_file(original_clip_path) != source_manifest.get("clip_manifest_sha256"):
        raise SupervisorError("original clip manifest does not match the pinned input manifest")
    if _sha_file(original_catalog_path) != source_manifest.get("deployment_catalog_file_sha256"):
        raise SupervisorError("original deployment catalog does not match the pinned input manifest")
    prompt_path = root / "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py"
    if _sha_file(prompt_path) != source_manifest.get("deployment_catalog_schema_sha256"):
        raise SupervisorError("original Bank prompt schema does not match the input manifest")
    sample = _filtered_sample(original_sample, case_id, split)
    if case_id not in source_manifest.get("cases", {}):
        raise SupervisorError("input manifest does not contain the selected case")
    selected_manifest_rows = [json.loads(line) for line in
                original_dev_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    selected_dev_rows = [row for row in selected_manifest_rows if row.get("incident_id") == case_id]
    if len(selected_dev_rows) != 1:
        raise SupervisorError(f"public {split} manifest does not contain exactly one selected case")
    clip_rows = [json.loads(line) for line in
                 (public_src / "clip_manifest.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    selected_clips = [row for row in clip_rows if row.get("case_id") == case_id]
    case_manifest = source_manifest["cases"][case_id]
    selected_clip_map = {row.get("relative_path"): row for row in selected_clips}
    expected_clip_paths = set(case_manifest["files"])
    if set(selected_clip_map) != expected_clip_paths:
        raise SupervisorError("selected public case clips differ from the pinned manifest")
    case_src = (public_src / "cases" / case_id).resolve(strict=True)
    cases_root = (public_src / "cases").resolve(strict=True)
    if cases_root not in case_src.parents:
        raise SupervisorError("selected case clips escape the prepared public case directory")
    alert_path = (case_src / "alert.json").resolve(strict=True)
    if case_src not in alert_path.parents or _sha_file(alert_path) != case_manifest.get("alert_sha256"):
        raise SupervisorError("selected case alert does not match the original input manifest pin")
    observed_components: set[str] = set()
    for relative, file_doc in case_manifest["files"].items():
        path = (case_src / relative).resolve(strict=True)
        if case_src not in path.parents or not path.is_file():
            raise SupervisorError(f"selected case path escapes prepared clips: {relative}")
        if path.stat().st_size != file_doc.get("size_bytes") or _sha_file(path) != file_doc.get("sha256"):
            raise SupervisorError(f"selected case source does not match its original hash pin: {relative}")
        clip_doc = selected_clip_map[relative]
        if (clip_doc.get("clip_sha256") != file_doc.get("sha256") or
                clip_doc.get("clip_size_bytes") != file_doc.get("size_bytes") or
                clip_doc.get("clip_rows") != file_doc.get("rows") or
                clip_doc.get("header") != file_doc.get("header")):
            raise SupervisorError(f"selected clip metadata differs from the original manifest: {relative}")
        count = 0
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if list(reader.fieldnames or ()) != file_doc.get("header"):
                raise SupervisorError(f"selected clip header differs from its pinned schema: {relative}")
            for row in reader:
                count += 1
                component = row.get("cmdb_id")
                if component and component.strip().casefold() not in {"none", "null"}:
                    observed_components.add(component.strip())
        if count != file_doc.get("rows"):
            raise SupervisorError(f"selected clip row count differs from its original pin: {relative}")

    filtered_sample_path = config_dir / "bank/sample.json"
    filtered_dev_path = view_root / "data/prepared/bank" / split_filename
    filtered_clip_path = view_root / "data/prepared/bank/public/clip_manifest.jsonl"
    filtered_sample_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_dev_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_clip_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_sample_path.write_text(json.dumps(sample, ensure_ascii=False, sort_keys=True) + "\n",
                                    encoding="utf-8")
    filtered_dev_path.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                                    for row in selected_dev_rows), encoding="utf-8")
    filtered_clip_path.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                                     for row in selected_clips), encoding="utf-8")

    manifest = dict(source_manifest)
    manifest["sample_config_sha256"] = _sha_file(filtered_sample_path)
    manifest[split_hash_key] = _sha_file(filtered_dev_path)
    manifest["clip_manifest_sha256"] = _sha_file(filtered_clip_path)
    if "candidate_manifest_sha256" in manifest:
        manifest["candidate_manifest_sha256"] = None
    if "scored_manifest_sha256" in manifest and split != "scored":
        manifest["scored_manifest_sha256"] = None
    manifest["cases"] = {case_id: source_manifest["cases"][case_id]}
    filtered_manifest_path = config_dir / "bank/input_manifest.json"
    filtered_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
                                      encoding="utf-8")
    catalog_src = public_src / "deployment_catalog.json"
    catalog = json.loads(catalog_src.read_text(encoding="utf-8"))
    catalog_entities = catalog.get("entities", {})
    if not isinstance(catalog_entities, Mapping):
        raise SupervisorError("public deployment catalog entity table is malformed")
    catalog["entities"] = {identifier: info for identifier, info in catalog_entities.items()
                            if identifier in observed_components}
    filtered_catalog_path = view_root / "data/prepared/bank/public/deployment_catalog.json"
    filtered_catalog_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, sort_keys=True) + "\n",
                                     encoding="utf-8")
    manifest["deployment_catalog_file_sha256"] = _sha_file(filtered_catalog_path)
    filtered_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
                                      encoding="utf-8")
    shutil.copytree(case_src, view_root / "data/prepared/bank/public/cases" / case_id)
    return {"original_input_manifest_sha256": _sha_file(source_manifest_path),
            "original_sample_sha256": _sha_file(original_sample_path),
            "original_dev_manifest_sha256": _sha_file(original_dev_path),
            "original_clip_manifest_sha256": _sha_file(original_clip_path),
            "original_catalog_sha256": _sha_file(catalog_src),
            "selected_clip_hashes": {relative: doc["sha256"]
                                     for relative, doc in sorted(case_manifest["files"].items())},
            "bank_sample_sha256": _sha_file(filtered_sample_path),
            f"bank_{split}_manifest_sha256": _sha_file(filtered_dev_path),
            "bank_clip_manifest_sha256": _sha_file(filtered_clip_path),
            "bank_input_manifest_sha256": _sha_file(filtered_manifest_path),
            "bank_catalog_sha256": _sha_file(filtered_catalog_path),
            "catalog_observed_component_count": str(len(observed_components)),
            "selected_case_id": case_id,
            "selected_clip_count": str(len(selected_clips))}


def create_restricted_project_view(root: Path, view_root: Path, *,
                                   profile_path: Path, case_id: str | None,
                                   split: str = "dev",
                                   code_root: Path | None = None) -> dict[str, Any]:
    """Create a minimal project tree; no evaluator, scored, other-case, plan, or artifact paths."""
    root = root.resolve(strict=True)
    code_root = (code_root or root).resolve(strict=True)
    profile_path = profile_path.resolve(strict=True)
    if not profile_path.is_relative_to(root):
        raise SupervisorError("model profile must resolve inside the project root")
    if view_root.exists():
        raise FileExistsError(f"restricted view must be fresh: {view_root}")
    view_root.mkdir(parents=True)
    package_root = code_root / "src/e4"
    shutil.copytree(package_root, view_root / "src/e4", ignore=_view_code_ignore(package_root))
    (view_root / ".venv").mkdir()
    scripts_dir = view_root / "scripts"
    scripts_dir.mkdir()
    script_source = code_root / "scripts/bank/diagnose.py"
    _copy_file(script_source if script_source.is_file() else Path(__file__).resolve(),
               scripts_dir / "bank/diagnose.py")
    config_dir = view_root / "config"
    config_dir.mkdir()
    for name in VIEW_CONFIG_FILES:
        _copy_file(root / "config" / name, config_dir / name)
    for name in VIEW_ROOT_FILES:
        _copy_file(code_root / name, view_root / name)
    schema_path = root / "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py"
    schema_hash = _sha_file(schema_path)
    source_schema = {"schema_version": "openrca-bank-schema-only/1",
        "dataset": "OpenRCA Bank",
        "official_source": {"repository": "https://github.com/microsoft/OpenRCA",
            "revision": "c121a483a5a91dbae50708e5942c42a6609098b2",
            "schema_document": "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py",
            "schema_document_sha256": schema_hash},
        "telemetry_schema": NATIVE_TABLE_SCHEMAS,
        "upstream_declared_timestamp_units": {
            "metric/metric_app.csv": "seconds",
            "metric/metric_container.csv": "seconds",
            "trace/trace_span.csv": "milliseconds",
            "log/log_service.csv": "seconds"},
        "identity_policy": "preserve exact source IDs; infer no aliases or vertical topology",
        "trace_policy": "no success or call-type source field; duration unit undeclared",
        "log_policy": "value is raw text; no error/severity classifier is provided"}
    (config_dir / "bank/source.json").write_text(
        json.dumps(source_schema, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    profile_relative = profile_path.relative_to(root)
    _copy_file(profile_path, view_root / profile_relative)
    _copy_file(schema_path,
               view_root / "third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py")
    if case_id is None:
        pins = {"view_profile_sha256": _sha_file(view_root / profile_relative),
                "profile_relative_path": profile_relative.as_posix(),
                "view_kind": "schema_only_bootstrap"}
    else:
        public_pins = _prepare_public_manifest_view(root, view_root, case_id, split)
        pins = {**public_pins, "view_profile_sha256": _sha_file(view_root / profile_relative),
                "profile_relative_path": profile_relative.as_posix(), "view_kind": f"one_{split}_case"}
    inventory: dict[str, dict[str, Any]] = {}
    for path in sorted(view_root.rglob("*")):
        if not path.is_file() or ".venv" in path.relative_to(view_root).parts:
            continue
        inventory[path.relative_to(view_root).as_posix()] = {
            "sha256": _sha_file(path), "bytes": path.stat().st_size}
    inventory_bytes = json.dumps(inventory, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return {**pins, "restricted_view_sha256": _sha(inventory_bytes),
            "restricted_view_file_count": len(inventory), "view_file_inventory": inventory}


def _validate_local_endpoint(value: str | None) -> str:
    parsed = urlsplit(value or "")
    if (parsed.scheme not in {"http", "https"} or
            parsed.hostname not in {"localhost", "127.0.0.1", "::1"} or
            parsed.port is None):
        raise SupervisorError("E4_BASE_URL must be a local HTTP(S) endpoint with an explicit port")
    return value


def fresh_run_directory(path: Path, e4_run_dir: Path) -> tuple[Path, Path]:
    base = e4_run_dir.resolve(strict=True)
    target = path if path.is_absolute() else base / path
    target = target.resolve()
    if target == base or not target.is_relative_to(base):
        raise SupervisorError("M4 run output must be a child of E4_RUN_DIR")
    if target.exists():
        raise FileExistsError(f"M4 run directory must be fresh: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir(exist_ok=False)
    return target, base


def _bwrap_command(*, view_root: Path, run_dir: Path, library_root: Path | None,
                   profile_relative: str, mode: str, case_id: str | None,
                   split: str = "dev",
                   endpoint: str, feedback_socket: str | None,
                   implementation_fingerprint_value: str,
                   python_executable: str = "/m4project/.venv/bin/python",
                   command_override: Sequence[str] | None = None) -> list[str]:
    bwrap = shutil.which("bwrap")
    if not bwrap:
        raise SupervisorError("bubblewrap is required for M4 diagnostic isolation")
    if not re.fullmatch(r"[0-9a-f]{64}", implementation_fingerprint_value):
        raise SupervisorError("trusted source implementation fingerprint is malformed")
    args = [bwrap, "--unshare-all", "--share-net", "--die-with-parent", "--clearenv",
        "--setenv", "PATH", "/m4project/.venv/bin:/usr/bin:/bin",
        "--setenv", "LANG", "C.UTF-8", "--setenv", "LC_ALL", "C.UTF-8",
        "--setenv", "PYTHONNOUSERSITE", "1", "--setenv", "E4_M4_INNER", "1",
        "--setenv", "PYTHONDONTWRITEBYTECODE", "1",
        "--setenv", "E4_M4_IMPLEMENTATION_FINGERPRINT", implementation_fingerprint_value,
        "--setenv", "E4_BASE_URL", endpoint,
        "--setenv", "E4_RUN_DIR", "/m4run",
        "--setenv", "E4_MODEL_PROFILE", f"/m4project/{profile_relative}",
        "--ro-bind", "/usr", "/usr", "--ro-bind", "/lib", "/lib"]
    if Path("/lib64").exists():
        args += ["--ro-bind", "/lib64", "/lib64"]
    if Path("/etc/ld.so.cache").exists():
        args += ["--ro-bind", "/etc/ld.so.cache", "/etc/ld.so.cache"]
    args += ["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
             "--ro-bind", str(view_root), "/m4project",
             "--ro-bind", str(ROOT / ".venv"), "/m4project/.venv",
             "--bind", str(run_dir), "/m4run", "--chdir", "/m4project"]
    if library_root is not None:
        args += ["--ro-bind", str(library_root.resolve(strict=True)), "/m4library"]
    args += ["--"]
    if command_override is not None:
        return args + list(command_override)
    command = [python_executable, "/m4project/scripts/bank/diagnose.py", "--inside",
               "--mode", mode, "--root", "/m4project", "--run-dir",
               "/m4run/bootstrap" if mode == "bootstrap" else "/m4run/diagnostic",
               "--profile", f"/m4project/{profile_relative}"]
    if case_id is not None:
        command += ["--case-id", case_id]
    if mode == "m5-diagnostic":
        command += ["--split", split]
    if library_root is not None:
        command += ["--library-root", "/m4library"]
    if feedback_socket is not None:
        command += ["--feedback-socket", feedback_socket]
    return args + command


def _channel_fd(channel: socket.socket | int) -> int:
    return channel if type(channel) is int else channel.fileno()


def _send_frame(channel: socket.socket | int, value: Mapping[str, Any]) -> None:
    payload = json.dumps(dict(value), ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(payload) > MAX_FEEDBACK_BYTES:
        raise SupervisorError("feedback IPC message exceeds its fixed byte limit")
    frame = memoryview(struct.pack("!I", len(payload)) + payload)
    fd = _channel_fd(channel)
    sent_total = 0
    while sent_total < len(frame):
        sent_total += os.write(fd, frame[sent_total:])


def _receive_frame(channel: socket.socket | int) -> dict[str, Any]:
    fd = _channel_fd(channel)
    header = _read_exact_fd(fd, 4, timeout_seconds=5.0)
    size = struct.unpack("!I", header)[0]
    if size <= 0 or size > MAX_FEEDBACK_BYTES:
        raise SupervisorError("feedback IPC frame has an invalid size")
    value = json.loads(_read_exact_fd(fd, size, timeout_seconds=5.0))
    if not isinstance(value, dict):
        raise SupervisorError("feedback IPC message must be a JSON object")
    return value


def _read_exact_fd(fd: int, length: int, *, timeout_seconds: float) -> bytes:
    parts = bytearray()
    deadline = time.monotonic() + timeout_seconds
    while len(parts) < length:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
            raise TimeoutError("feedback IPC frame read timed out")
        chunk = os.read(fd, length - len(parts))
        if not chunk:
            raise EOFError("feedback IPC peer closed the channel")
        parts.extend(chunk)
    return bytes(parts)


class BinaryFeedbackProxy:
    """Controller-side IPC proxy: only accepted/rejected returns from the parent."""

    def __init__(self, address: str | socket.socket):
        if isinstance(address, socket.socket):
            self.sock = address
        else:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect("\0" + address)

    def evaluate(self, case_id: str, ranked_answer: Any) -> dict[str, bool]:
        from e4.contracts import RankedAnswer
        if not isinstance(ranked_answer, RankedAnswer):
            raise ValueError("feedback requires a RankedAnswer")
        components = [candidate.entity.identifier for candidate in ranked_answer.leads[:3]]
        if (not _safe_case_id(case_id) or len(components) > 3 or
                any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > MAX_FEEDBACK_COMPONENT_CHARS
                    for item in components)):
            raise ValueError("feedback request is outside the bounded component-only contract")
        request = {"case_id": case_id, "components": components}
        _send_frame(self.sock, request)
        response = _receive_frame(self.sock)
        if set(response) != {"accepted"} or type(response["accepted"]) is not bool:
            raise SupervisorError("trusted feedback service did not return one accepted boolean")
        return response

    def close(self) -> None:
        self.sock.close()


def _validate_feedback_request(request: Mapping[str, Any], expected_case_id: str) -> list[str]:
    if set(request) != {"case_id", "components"} or request.get("case_id") != expected_case_id:
        raise SupervisorError("feedback IPC request has unexpected fields or case identity")
    components = request.get("components")
    if not isinstance(components, list) or len(components) > 3:
        raise SupervisorError("feedback IPC request must include at most three components")
    if any(type(item) is not str or not item or
           len(item.encode("utf-8")) > MAX_FEEDBACK_COMPONENT_CHARS for item in components):
        raise SupervisorError("feedback IPC component identity is malformed")
    canonical = [item.strip().casefold() for item in components]
    if len(set(canonical)) != len(canonical):
        raise SupervisorError("feedback IPC component list contains duplicate identities")
    return components


def _ranked_answer_from_ids(components: Sequence[str]) -> Any:
    from e4.contracts import EntityRef, RankedAnswer, ScoredCandidate
    return RankedAnswer(tuple(ScoredCandidate(EntityRef("component", item),
                              float(len(components) - index), None, ())
                              for index, item in enumerate(components)))


def serve_feedback_request(sock: socket.socket, *, case_id: str,
                           evaluate_ids: Callable[[str, list[str]], bool],
                           audit_path: Path) -> None:
    """Evaluate one top-three request and return only one bool to the child."""
    request = _receive_frame(sock)
    components = _validate_feedback_request(request, case_id)
    accepted = evaluate_ids(case_id, components)
    if type(accepted) is not bool:
        raise SupervisorError("trusted evaluator returned a non-boolean result")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"case_id": case_id, "components": components,
            "accepted": accepted, "updated_utc": datetime.now(timezone.utc).isoformat()},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")
    _send_frame(sock, {"accepted": accepted})


def _run_with_feedback(command: Sequence[str] | Callable[[str], Sequence[str]], *, case_id: str,
                       evaluator: Any, audit_path: Path,
                       stdout_path: Path, stderr_path: Path,
                       timeout_seconds: float = 7200,
                       status_out: dict[str, Any] | None = None) -> int:
    address = "e4m4-" + secrets.token_hex(16)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind("\0" + address)
    listener.listen(1)
    listener.setblocking(False)
    launch_command = list(command(address) if callable(command) else command)

    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
        process = subprocess.Popen(launch_command, cwd=ROOT, env={}, stdout=stdout, stderr=stderr,
            start_new_session=True, close_fds=True)
        deadline = time.monotonic() + timeout_seconds
        feedback_count = 0
        feedback_failed = False
        conn: socket.socket | None = None
        try:
            while process.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if status_out is not None:
                        status_out["case_wall_timeout"] = True
                    process.terminate()
                    feedback_failed = True
                    break
                watch = conn if conn is not None else listener
                readable, _, _ = select.select([watch], [], [], min(remaining, 0.25))
                if not readable:
                    continue
                if conn is None:
                    conn, _peer = listener.accept()
                    conn.setblocking(True)
                    continue
                try:
                    request = _receive_frame(conn)
                except EOFError:
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        process.terminate()
                        feedback_failed = True
                    else:
                        feedback_failed = process.returncode != 0
                    break
                except Exception:
                    process.terminate()
                    feedback_failed = True
                    break
                try:
                    feedback_count += 1
                    if feedback_count > 3:
                        raise SupervisorError("feedback request budget exceeded")
                    components = _validate_feedback_request(request, case_id)
                    accepted = evaluator.evaluate(case_id, _ranked_answer_from_ids(components))["accepted"]
                    if type(accepted) is not bool:
                        raise SupervisorError("trusted evaluator response is not boolean")
                    audit_path.parent.mkdir(parents=True, exist_ok=True)
                    with audit_path.open("a", encoding="utf-8") as stream:
                        stream.write(json.dumps({"case_id": case_id, "components": components,
                            "accepted": accepted,
                            "updated_utc": datetime.now(timezone.utc).isoformat()},
                            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                            allow_nan=False) + "\n")
                    response = {"accepted": accepted}
                except Exception:
                    audit_path.parent.mkdir(parents=True, exist_ok=True)
                    with audit_path.open("a", encoding="utf-8") as stream:
                        stream.write(json.dumps({"case_id": case_id,
                            "status": "trusted_feedback_unavailable",
                            "updated_utc": datetime.now(timezone.utc).isoformat()},
                            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                            allow_nan=False) + "\n")
                    process.terminate()
                    feedback_failed = True
                    break
                _send_frame(conn, response)
            if feedback_failed:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                return 1
            return process.returncode
        finally:
            listener.close()
            if conn is not None:
                conn.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def _run_inner_feedback(sock: socket.socket, *, case_id: str,
                        run_diagnostic_fn: Callable[[], Any]) -> Any:
    return run_diagnostic_fn()


def _run_inner(args: argparse.Namespace) -> int:
    if os.environ.get("E4_M4_INNER") != "1":
        raise SupervisorError("internal mode must run inside the restricted M4 namespace")
    root = args.root.resolve(strict=True)
    profile = args.profile.resolve(strict=True)
    if not profile.is_relative_to(root):
        raise SupervisorError("inner model profile must stay inside the restricted project view")
    if args.mode == "bootstrap":
        from e4.agent.bank.bootstrap import run_bootstrap
        run_bootstrap(profile_path=profile, run_dir=args.run_dir, root=root)
    else:
        from e4.agent.bank.controller import run_diagnostic
        if args.feedback_socket is None or args.library_root is None or args.case_id is None:
            raise SupervisorError("diagnostic mode requires a library, case, and feedback IPC")
        feedback = BinaryFeedbackProxy(args.feedback_socket)
        split = args.split if args.mode == "m5-diagnostic" else "dev"
        run_diagnostic(args.case_id, library_root=args.library_root,
            run_dir=args.run_dir, feedback=feedback, profile_path=profile, root=root,
            split=split)
    return 0


def _run_outer(args: argparse.Namespace) -> int:
    supervisor_started = time.perf_counter()
    root = args.root.resolve(strict=True)
    if root != ROOT.resolve(strict=True):
        raise SupervisorError("outer M4 supervisor root must be this frozen project")
    endpoint = _validate_local_endpoint(os.environ.get("E4_BASE_URL"))
    configured_run_dir = os.environ.get("E4_RUN_DIR")
    if not configured_run_dir:
        raise SupervisorError("E4_RUN_DIR from the approved local model launcher is required")
    e4_run_dir = Path(configured_run_dir).resolve(strict=True)
    profile = args.profile.resolve(strict=True)
    if not profile.is_relative_to(root):
        raise SupervisorError("model profile must resolve inside the project")
    configured_profile = os.environ.get("E4_MODEL_PROFILE")
    if not configured_profile:
        raise SupervisorError("E4_MODEL_PROFILE from the approved local model launcher is required")
    if Path(configured_profile).resolve(strict=True) != profile:
        raise SupervisorError("selected profile must match E4_MODEL_PROFILE from the approved launcher")
    runtime_policy = json.loads((root / "config/bank/controller_policy.json").read_text(encoding="utf-8"))
    case_timeout = float(runtime_policy["serving"]["case_wall_seconds"])
    run_dir, base = fresh_run_directory(args.run_dir, e4_run_dir)
    run_id = run_dir.name
    feedback_path = base / "m4_trusted_feedback" / f"{run_id}.jsonl"
    library_root = args.library_root.resolve(strict=True) if args.library_root else None
    # Bootstrap runs do not stage a case view, but the supervisor manifest
    # shares the same schema with diagnostic runs.
    staged_task_view_sha: str | None = None
    if args.mode in {"diagnostic", "m5-diagnostic"}:
        if not args.case_id or not _safe_case_id(args.case_id) or library_root is None:
            raise SupervisorError("diagnostic mode requires a frozen Bank case and immutable library")
        sample = json.loads((root / "config/bank/sample.json").read_text(encoding="utf-8"))
        split = args.split if args.mode == "m5-diagnostic" else "dev"
        split_key = "development_cases" if split == "dev" else "scored_cases"
        split_ids = {row["incident_id"] for row in sample.get(split_key, [])}
        if args.case_id not in split_ids:
            raise SupervisorError(f"diagnostic mode case is outside the frozen {split} allowlist")
        if args.mode == "m5-diagnostic" and split == "scored":
            if not args.experiment_manifest:
                raise SupervisorError("scored M5 E4 requires a frozen experiment manifest")
            if not args.trusted_evaluator_socket:
                raise SupervisorError("scored M5 E4 requires the shared trusted evaluator socket")
            from e4.comparison.e4_runs import validate_scored_run
            validate_scored_run(args.experiment_manifest, root=root, case_id=args.case_id,
                                library_root=library_root, profile_path=profile)
        if args.mode == "m5-diagnostic" and split == "scored":
            if not args.task_view_root:
                raise SupervisorError("scored M5 E4 requires the orchestrator-staged public task view")
            if not Path(args.task_view_root).resolve(strict=True).is_relative_to(e4_run_dir):
                raise SupervisorError("staged M5 public task view must be inside E4_RUN_DIR")
            raw_socket_path = Path(args.trusted_evaluator_socket)
            if (raw_socket_path.is_symlink() or not raw_socket_path.is_socket() or
                    not raw_socket_path.resolve(strict=True).is_relative_to(e4_run_dir)):
                raise SupervisorError("trusted M5 evaluator socket must be inside E4_RUN_DIR")
            from e4.comparison.e4_runs import validate_staged_task_view
            staged_task_view_sha = validate_staged_task_view(args.task_view_root,
                root=root, case_id=args.case_id, split=split)
        else:
            staged_task_view_sha = None
        from e4.agent.library import snapshot_library
        library_snapshot = snapshot_library(library_root)
        library_id = library_snapshot.snapshot_id
    else:
        library_id = None
    from e4.dsl.cache import implementation_fingerprint
    host_implementation_fingerprint = implementation_fingerprint()
    view_root = Path(tempfile.mkdtemp(prefix="e4-m4-view-")) / "project"
    try:
        view_pins = create_restricted_project_view(root, view_root,
            profile_path=profile, case_id=args.case_id if args.mode in {"diagnostic", "m5-diagnostic"} else None,
            split=args.split if args.mode == "m5-diagnostic" else "dev")
        if implementation_fingerprint() != host_implementation_fingerprint:
            raise SupervisorError("project implementation changed while building the restricted view")
        # The view is copied from the exact frozen source before entering bwrap.
        view_digest = view_pins["restricted_view_sha256"]
        if args.mode == "bootstrap":
            command = _bwrap_command(view_root=view_root, run_dir=run_dir,
                library_root=None, profile_relative=view_pins["profile_relative_path"],
                mode=args.mode, case_id=None, endpoint=endpoint, feedback_socket=None,
                implementation_fingerprint_value=host_implementation_fingerprint)
            with (run_dir / "inner.stdout.log").open("w") as stdout, \
                    (run_dir / "inner.stderr.log").open("w") as stderr:
                completed = subprocess.run(command, cwd=ROOT, env={}, stdout=stdout,
                                           stderr=stderr, check=False)
            exit_code = completed.returncode
        else:
            if args.mode == "m5-diagnostic":
                from e4.comparison.e4_runs import M5TrustedFeedback
                evaluator = M5TrustedFeedback(root=root, case_id=args.case_id,
                    split=args.split, experiment_manifest=args.experiment_manifest,
                    trusted_evaluator_socket=args.trusted_evaluator_socket)
            else:
                from e4.judge.bank import DevOnlyFeedbackEvaluator
                evaluator = DevOnlyFeedbackEvaluator(root=root, allowed_case_ids=[args.case_id])
            def command_for_feedback(address: str) -> Sequence[str]:
                return _bwrap_command(view_root=view_root, run_dir=run_dir,
                    library_root=library_root, profile_relative=view_pins["profile_relative_path"],
                    mode=args.mode, case_id=args.case_id, endpoint=endpoint, feedback_socket=address,
                    split=args.split if args.mode == "m5-diagnostic" else "dev",
                    implementation_fingerprint_value=host_implementation_fingerprint)
            feedback_status: dict[str, Any] = {"case_wall_timeout": False}
            exit_code = _run_with_feedback(command_for_feedback, case_id=args.case_id,
                evaluator=evaluator, audit_path=feedback_path,
                stdout_path=run_dir / "inner.stdout.log",
                stderr_path=run_dir / "inner.stderr.log", timeout_seconds=case_timeout,
                status_out=feedback_status)
            if args.mode == "m5-diagnostic":
                attempt_slots = evaluator.write_attempt_slots(run_dir / "diagnostic",
                    case_timeout=feedback_status["case_wall_timeout"])
            else:
                attempt_slots = None
        if args.mode != "m5-diagnostic":
            feedback_status = {"case_wall_timeout": False}
        summary = {"schema_version": "e4-m4-supervisor-run/1", "mode": args.mode,
            "case_id": args.case_id if args.mode in {"diagnostic", "m5-diagnostic"} else None,
            "run_id": run_id, "exit_code": exit_code,
            "status": "complete" if exit_code == 0 else "failed",
            "profile_sha256": _sha_file(profile), "library_snapshot_id": library_id,
            "restricted_view_sha256": view_digest,
            "view_pins": view_pins,
            "host_implementation_fingerprint": host_implementation_fingerprint,
            "restricted_view_kind": view_pins["view_kind"],
            "feedback_audit_path": str(feedback_path) if args.mode in {"diagnostic", "m5-diagnostic"} else None,
            "attempt_slots": attempt_slots if args.mode == "m5-diagnostic" else None,
            "case_wall_timeout": feedback_status["case_wall_timeout"],
            "supervisor_wall_seconds": time.perf_counter() - supervisor_started,
            "staged_task_view_sha256": staged_task_view_sha,
            "finished_utc": datetime.now(timezone.utc).isoformat()}
        _write_json(run_dir / "supervisor_manifest.json", summary)
        print(json.dumps(summary, sort_keys=True))
        return exit_code
    finally:
        shutil.rmtree(view_root.parent, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("bootstrap", "diagnostic", "m5-diagnostic"), required=True)
    parser.add_argument("--profile", type=Path, default=ROOT / "config/models/gemma.json")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--case-id")
    parser.add_argument("--library-root", type=Path)
    parser.add_argument("--split", choices=("dev", "scored"), default="dev")
    parser.add_argument("--experiment-manifest", type=Path)
    parser.add_argument("--trusted-evaluator-socket", type=Path)
    parser.add_argument("--task-view-root", type=Path)
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--feedback-socket", help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.inside:
            return _run_inner(args)
        return _run_outer(args)
    except Exception as exc:
        print(f"M4 Bank run failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
