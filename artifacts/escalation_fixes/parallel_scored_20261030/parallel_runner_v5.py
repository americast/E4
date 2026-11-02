#!/usr/bin/env python3
"""Prepare and orchestrate the predeclared two-seed, four-chain Bank experiment.

This artifact does not launch model jobs during preparation. Each scored chain
uses the existing frozen audit driver's run_shard and combine APIs. Physical
model services are supplied by a pinned JSON service map; logical local/wingtip
labels continue to mean the first/last five cases in the frozen plan.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import select
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ARTIFACTS = ROOT / "artifacts/escalation_fixes/parallel_scored_20261030"
RUNNER_SCHEMA = "e4-parallel-scored-run/2"
CASE_COUNT = 10
SEEDS = (42, 43)
MODEL_SHA = "179cfb99212709597eae5929112cfca677e1bbf566178b479ae1da0c4772874b"
REMOTE_MARKER = "ssh-unix-forward-via-loopback-relay"
REMOTE_HOST = "ssinha97@wingtip-gpu2.cc.gatech.edu"
REMOTE_INFERENCE = ROOT / "artifacts/e4_improvement/remote_wingtip/remote_inference.py"
SEED_PREP = ROOT / "artifacts/escalation_fixes/implementation_20261026/launch/prepare_frozen_rerun.py"


class RunnerError(RuntimeError):
    pass


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def json_sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def atomic_json(path: Path, value: Any, *, replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or (path.exists() and not replace):
        raise RunnerError(f"preserving existing artifact: {path}")
    tmp = path.with_name(path.name + ".pending")
    tmp.write_text(json.dumps(value, sort_keys=True, ensure_ascii=False,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def project_path(value: str | Path, *, must_exist: bool = True) -> Path:
    raw = Path(value)
    if raw.is_absolute():
        path = raw
    else:
        path = ROOT / raw
    resolved = path.resolve(strict=must_exist)
    if not resolved.is_relative_to(ROOT):
        raise RunnerError(f"project artifact path escapes the project: {value}")
    cursor = ROOT
    for part in resolved.relative_to(ROOT).parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise RunnerError(f"project artifact path crosses a symlink: {cursor}")
    return resolved


def prepare(freeze_path: Path, run_root: Path) -> dict[str, Any]:
    """Create the sealed seed42/43 manifests and two independent audit roots."""
    freeze_path = project_path(freeze_path)
    run_root = project_path(run_root, must_exist=False)
    if not run_root.is_relative_to(ARTIFACTS):
        raise RunnerError("parallel run root must stay in this experiment artifact folder")
    if run_root.exists() or run_root.is_symlink():
        raise RunnerError("parallel run root already exists; preserving it")
    from artifacts.escalation_fixes.implementation_20261026.launch import prepare_frozen_rerun
    from scripts import run_paper_starter_audit as audit

    manifest = prepare_frozen_rerun._prepare(freeze_path, run_root, SEEDS)
    rows = {int(row["seed"]): row for row in manifest["seeds"]}
    audits: dict[str, dict[str, str]] = {}
    for seed in SEEDS:
        row = rows[seed]
        seed_root = project_path(row["seed_root"])
        seed_manifest_path = seed_root / "seed_manifest.json"
        seed_manifest = json.loads(seed_manifest_path.read_text(encoding="utf-8"))
        # The existing helper writes the seed seal before this driver adds the
        # standard audit root. The seal continues to pin the derived freeze.
        audit_dir = audit.prepare(ROOT / row["derived_freeze_path"],
            audit_root=seed_root / "audit")
        audit_doc = json.loads((audit_dir / "audit_manifest.json").read_text(encoding="utf-8"))
        if (audit_doc.get("case_ids") != manifest["case_order"] or
                audit_doc.get("shard_groups") != manifest["shard_groups"] or
                audit_doc.get("model_profile_sha256") != row["profile_sha256"] or
                audit_doc.get("initial_library_snapshot_id") != manifest["initial_library_snapshot_id"]):
            raise RunnerError(f"seed{seed} audit does not match the sealed profile/cases/library")
        audits[str(seed)] = {"audit_dir": str(audit_dir.relative_to(ROOT)),
            "audit_manifest_sha256": sha(audit_dir / "audit_manifest.json"),
            "seed_manifest_sha256": sha(seed_manifest_path)}
    root_manifest = {"schema_version": RUNNER_SCHEMA, "status": "prepared_no_model_calls",
        "runner_source_path": str(Path(__file__).resolve().relative_to(ROOT)),
        "runner_source_sha256": sha(Path(__file__).resolve()),
        "rerun_manifest_path": str((run_root / "rerun_manifest.json").relative_to(ROOT)),
        "rerun_manifest_sha256": sha(run_root / "rerun_manifest.json"),
        "freeze_path": str(freeze_path.relative_to(ROOT)), "freeze_sha256": sha(freeze_path),
        "seeds": [42, 43], "fixed_case_order": manifest["case_order"],
        "shard_groups": manifest["shard_groups"], "audits": audits,
        "service_assignment": {
            "seed42": {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"},
            "seed43": {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"}},
        "schedule": ["seed42.local and seed42.wingtip concurrently",
            "seed43.local starts only after seed42.local completes",
            "seed43.wingtip starts only after seed42.wingtip completes"],
        "fresh_initial_library_per_seed_and_shard": True,
        "all_cases_new_for_this_method": False,
        "case_reuse_note": "same ten frozen cases as paired_r5; not holdout and not new for this method",
        "official_m5_scored_claim": False, "fresh_holdout_claim": False,
        "model_calls_started": False}
    atomic_json(run_root / "parallel_manifest.json", root_manifest)
    return root_manifest


def _verify_runner_plan(run_root: Path, manifest: Mapping[str, Any]) -> None:
    if (manifest.get("schema_version") != RUNNER_SCHEMA or
            manifest.get("status") != "prepared_no_model_calls" or
            manifest.get("runner_source_path") != Path(__file__).resolve().relative_to(ROOT).as_posix() or
            manifest.get("runner_source_sha256") != sha(Path(__file__).resolve())):
        raise RunnerError("parallel manifest no longer matches this frozen runner source")
    rerun_path = project_path(manifest["rerun_manifest_path"])
    if sha(rerun_path) != manifest.get("rerun_manifest_sha256"):
        raise RunnerError("two-seed rerun manifest changed after preparation")
    for seed, row in manifest.get("audits", {}).items():
        audit_dir = project_path(row["audit_dir"])
        if sha(audit_dir / "audit_manifest.json") != row.get("audit_manifest_sha256"):
            raise RunnerError(f"seed{seed} audit manifest changed after preparation")


def _service_doc(service_map_path: Path, service_name: str) -> tuple[dict[str, Any], Path]:
    config_path = project_path(service_map_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if config.get("schema_version") != "e4-parallel-service-map/1":
        raise RunnerError("service map must use e4-parallel-service-map/1")
    services = config.get("services")
    if not isinstance(services, Mapping) or service_name not in services:
        raise RunnerError(f"service map has no {service_name!r} entry")
    row = services[service_name]
    if "service_role" in row and row["service_role"] != service_name:
        raise RunnerError(f"service map entry key and explicit service_role disagree for {service_name}")
    proof_path = project_path(row["proof_path"])
    if sha(proof_path) != row.get("proof_sha256"):
        raise RunnerError(f"{service_name} proof hash changed")
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    if (proof.get("schema_version") != "e4-parallel-model-service-proof/1" or
            proof.get("service_role") != service_name or
            proof.get("endpoint") != row.get("endpoint") or
            proof.get("actual_model_host") != row.get("actual_model_host") or
            proof.get("actual_model_fqdn") != row.get("actual_model_fqdn") or
            proof.get("checkpoint_sha256") != MODEL_SHA or
            proof.get("model_revision") != "59dde24573e7e61570dba08b18a2e1fe246955ed" or
            proof.get("model_alias") != "e4-gemma-m3" or
            proof.get("model_manifest_sha256") != sha(ROOT / "config/model_manifest.json") or
            proof.get("profile_sha256") != sha(ROOT / "config/gemma_m3_inference.json") or
            proof.get("weights_loaded") is not True or
            proof.get("health", {}).get("http_status") != 200 or
            proof.get("context_tokens") != 32768 or
            not isinstance(proof.get("actual_model_fqdn"), str) or not proof.get("actual_model_fqdn") or
            not isinstance(proof.get("startup_receipt_path"), str) or
            not isinstance(proof.get("startup_receipt_sha256"), str) or
            not isinstance(proof.get("context_config_sha256"), str) or
            len(proof["context_config_sha256"]) != 64 or
            not isinstance(proof.get("context_config"), Mapping) or
            json_sha(proof["context_config"]) != proof["context_config_sha256"]):
        raise RunnerError(f"{service_name} service proof failed identity/health/context checks")
    startup_receipt = project_path(proof["startup_receipt_path"])
    if sha(startup_receipt) != proof["startup_receipt_sha256"]:
        raise RunnerError(f"{service_name} raw startup receipt is missing or changed")
    startup = json.loads(startup_receipt.read_text(encoding="utf-8"))
    startup_model_sha = startup.get("model_sha256")
    startup_health = startup.get("health", startup.get("health_check", {}))
    startup_command = startup.get("server_command")
    startup_binary_sha = startup.get("server_sha256", startup.get("llama_server_sha256"))
    command_flags = ({startup_command[index]: startup_command[index + 1]
        for index in range(len(startup_command) - 1)
        if isinstance(startup_command[index], str) and startup_command[index].startswith("--")}
        if isinstance(startup_command, list) else {})
    expected_server_port = ({"dgx_v100": 19262, "local_p100_v100": 19250}[service_name]
        if service_name != "wingtip_rtx8000" else row.get("remote_port"))
    startup_ready = startup.get("state") == "ready" or startup.get("verified") is True
    if (startup_model_sha != MODEL_SHA or not startup_ready or
            startup.get("server_pid") != proof.get("server_pid") or
            startup.get("llama_cpp_revision") != proof.get("llama_cpp_revision") or
            startup_binary_sha != proof.get("server_binary_sha256") or
            startup_health.get("http_status") != 200 or
            command_flags.get("--host") != "127.0.0.1" or
            command_flags.get("--port") != str(expected_server_port) or
            command_flags.get("--alias") != "e4-gemma-m3" or
            command_flags.get("--ctx-size") != "32768" or
            command_flags.get("--parallel") != "1" or
            command_flags.get("--reasoning") != "on" or
            command_flags.get("--reasoning-budget") != "2048" or
            "--offline" not in command_flags):
        raise RunnerError(f"{service_name} raw startup receipt does not prove the pinned live server configuration")
    if service_name == "wingtip_rtx8000" and startup.get("bind_port") != expected_server_port:
        raise RunnerError("Wingtip startup bind_port differs from the configured remote port")
    if startup.get("model_alias") not in {None, "e4-gemma-m3"}:
        raise RunnerError(f"{service_name} startup receipt model alias differs")
    if startup.get("profile_sha256") not in {None, sha(ROOT / "config/gemma_m3_inference.json")}:
        raise RunnerError(f"{service_name} startup profile differs from the frozen base profile")
    if service_name == "dgx_v100":
        gpu = startup.get("gpu_baseline", {})
        if (gpu.get("uuid") != proof.get("gpu_uuid") or
                gpu.get("pci_bus_id") != proof.get("sampler_pci_bus_id")):
            raise RunnerError("DGX raw startup GPU identity differs from the service proof")
    elif service_name == "local_p100_v100":
        startup_gpus = startup.get("gpu_baseline", [])
        if {gpu.get("uuid") for gpu in startup_gpus} != set(proof.get("gpu_uuids", [])):
            raise RunnerError("local raw startup GPU identities differ from the service proof")
    elif startup.get("hardware") != proof.get("hardware"):
        raise RunnerError("Wingtip raw startup hardware differs from the service proof")
    expected_context = {"model_alias": "e4-gemma-m3", "context_tokens": 32768,
        "max_generation_tokens": 8192, "parallel": 1, "reasoning_mode": "on",
        "reasoning_budget": 2048, "offline": True}
    if proof["context_config"] != expected_context:
        raise RunnerError(f"{service_name} server context/sampling configuration differs from the frozen profile")
    if not isinstance(row.get("endpoint"), str) or not row["endpoint"].startswith("http://127.0.0.1:"):
        raise RunnerError("service endpoint must be explicit host loopback HTTP")
    port = urllib.parse.urlsplit(row["endpoint"]).port
    if service_name not in {"dgx_v100", "local_p100_v100", "wingtip_rtx8000"}:
        raise RunnerError(f"unsupported physical service role {service_name!r}")
    if service_name == "wingtip_rtx8000":
        expected_port, expected_bridge, expected_host = row.get("remote_port"), "ssh-unix-forward", REMOTE_HOST
        if type(expected_port) is not int or not 1 <= expected_port <= 65535:
            raise RunnerError("Wingtip remote_port must come from its fresh pinned startup proof")
    else:
        expected_port, expected_bridge, expected_host = (
            (19262, "dgx-loopback-proxy", "DGX V100") if service_name == "dgx_v100" else
            (19260, "ssh-reverse-tunnel", "new-local-P100-V100"))
    if (port != expected_port or row.get("bridge_kind") != expected_bridge or
            row.get("actual_model_host") != expected_host or
            row.get("actual_model_fqdn") != proof.get("actual_model_fqdn")):
        raise RunnerError(f"{service_name} service endpoint/host/bridge differs from predeclared mapping")
    if service_name == "dgx_v100":
        pci_parts = str(proof.get("sampler_pci_bus_id", "")).lower().split(":")
        pci_valid = (len(pci_parts) == 3 and len(pci_parts[0]) == 8 and
            all(char in "0123456789abcdef" for char in pci_parts[0]) and
            pci_parts[1] == "0f" and pci_parts[2] == "00.0")
        if (proof.get("gpu_uuid") != "GPU-b2f1e6f7-d4df-ab2f-ee27-8b4b76073539" or
                not pci_valid):
            raise RunnerError("DGX service proof does not identify the assigned V100 on PCI bus 0F")
    elif service_name == "local_p100_v100":
        if set(proof.get("gpu_uuids", [])) != {
                "GPU-6311f251-73be-4bd2-971d-63d6c7d0e947",
                "GPU-e43dc7e5-2eeb-ec4d-14bb-c0f1bec4efd2"}:
            raise RunnerError("local service proof does not identify both assigned P100 and V100 devices")
    else:
        if (proof.get("remote_host") != REMOTE_HOST or
                "Quadro RTX 8000" not in str(proof.get("hardware", {}).get("name", ""))):
            raise RunnerError("Wingtip service proof does not identify the pinned RTX 8000 host/port")
    return {**row, "service_role": service_name,
        "proof": proof, "proof_path": str(proof_path.relative_to(ROOT)),
        "proof_sha256": sha(proof_path),
        "startup_receipt_path": str(startup_receipt.relative_to(ROOT)),
        "startup_receipt_sha256": sha(startup_receipt)}, proof_path


def _http_probe(endpoint: str) -> dict[str, Any]:
    endpoint = endpoint.rstrip("/")
    results: dict[str, Any] = {}
    for path in ("/health", "/v1/models"):
        request = urllib.request.Request(endpoint + path, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                body = response.read(16384)
                status = response.status
        except Exception as exc:
            raise RunnerError(f"service probe {path} failed: {type(exc).__name__}: {exc}") from exc
        if status != 200:
            raise RunnerError(f"service probe {path} returned HTTP {status}")
        parsed = json.loads(body)
        if path == "/health" and parsed.get("status") not in {"ok", "ready"}:
            raise RunnerError("live service health body is not ready")
        if path == "/v1/models":
            models = [row.get("id") for row in parsed.get("data", []) if isinstance(row, Mapping)]
            if "e4-gemma-m3" not in models:
                raise RunnerError("live model listing does not include the pinned Gemma alias")
        results[path] = {"http_status": status, "body_sha256": hashlib.sha256(body).hexdigest(),
            "body": parsed}
    return results


def _probe_endpoint(service: Mapping[str, Any], *, log_path: Path | None = None) -> dict[str, Any]:
    """Re-probe live service before preflight and immediately before a shard."""
    endpoint = str(service["endpoint"]).rstrip("/")
    if service.get("bridge_kind") == "ssh-unix-forward":
        from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
        if log_path is None:
            raise RunnerError("Wingtip service probe requires an owned SSH log path")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ssh_process = None
        socket_dir = None
        relay = None
        try:
            ssh_process, socket_dir, _ = transport.connect_ssh(int(service["remote_port"]), ssh_log=log_path)
            relay = transport.TCPToUnixRelay(socket_dir / "forward.sock", port=0)
            relay.start()
            probed_endpoint = f"http://127.0.0.1:{relay.address[1]}"
            results = _http_probe(probed_endpoint)
        finally:
            if relay is not None:
                relay.close()
            transport.terminate_own_process(ssh_process)
            if socket_dir is not None:
                shutil.rmtree(socket_dir, ignore_errors=True)
    else:
        results = _http_probe(endpoint)
    return {"endpoint": endpoint, "checked_utc_epoch": time.time(), "probes": results,
        "service_proof_sha256": service["proof_sha256"],
        "context_tokens": service["proof"]["context_tokens"],
        "context_config_sha256": service["proof"]["context_config_sha256"]}


@contextmanager
def _service_tcp_access(service: Mapping[str, Any], log_path: Path):
    """Expose an actual service as host-loopback TCP for no-completion probes."""
    if service.get("bridge_kind") != "ssh-unix-forward":
        yield str(service["endpoint"])
        return
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ssh_process = None
    socket_dir = None
    relay = None
    try:
        ssh_process, socket_dir, _ = transport.connect_ssh(int(service["remote_port"]), ssh_log=log_path)
        relay = transport.TCPToUnixRelay(socket_dir / "forward.sock", port=0)
        relay.start()
        yield f"http://127.0.0.1:{relay.address[1]}"
    finally:
        if relay is not None:
            relay.close()
        transport.terminate_own_process(ssh_process)
        if socket_dir is not None:
            shutil.rmtree(socket_dir, ignore_errors=True)


class UnixToTCPBridge:
    """Owned Unix listener that forwards bytes to an attested host-loopback TCP endpoint."""
    def __init__(self, socket_path: Path, endpoint: str):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port is None:
            raise RunnerError("bridge target must be host loopback HTTP with port")
        self.path = socket_path
        self.host, self.port = parsed.hostname, parsed.port
        self.rows: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                started = time.monotonic()
                row = {"client_bytes": 0, "service_bytes": 0, "error": None}
                remote = socket.create_connection((owner.host, owner.port), timeout=8)
                remote.settimeout(None)
                try:
                    sockets = (self.request, remote)
                    done: set[socket.socket] = set()
                    while len(done) < 2:
                        readable, _, _ = select.select([sock for sock in sockets if sock not in done], [], [], 45)
                        if not readable:
                            continue
                        for source in readable:
                            data = source.recv(65536)
                            if not data:
                                done.add(source)
                                try:
                                    (remote if source is self.request else self.request).shutdown(socket.SHUT_WR)
                                except OSError:
                                    pass
                                continue
                            destination = remote if source is self.request else self.request
                            destination.sendall(data)
                            row["client_bytes" if source is self.request else "service_bytes"] += len(data)
                except OSError as exc:
                    row["error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
                finally:
                    remote.close()
                    row["wall_seconds"] = time.monotonic() - started
                    with owner.lock:
                        owner.rows.append(row)

        class Server(socketserver.ThreadingUnixStreamServer):
            daemon_threads = False
            block_on_close = True

        socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if socket_path.exists() or socket_path.is_symlink():
            raise RunnerError("forward socket path already exists")
        self.server = Server(str(socket_path), Handler)
        socket_path.chmod(0o600)
        self.thread = threading.Thread(target=self.server.serve_forever,
            kwargs={"poll_interval": 0.1}, daemon=True)

    def start(self):
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.path.unlink(missing_ok=True)

    def metrics(self):
        with self.lock:
            return {"connection_count": len(self.rows), "connections": list(self.rows),
                "client_bytes_total": sum(row["client_bytes"] for row in self.rows),
                "service_bytes_total": sum(row["service_bytes"] for row in self.rows)}


def _compat_receipt(audit_dir: Path, seed: int, shard: str, service: Mapping[str, Any],
                    probe: Mapping[str, Any], tokenizer_path: Path) -> Path:
    manifest = json.loads((audit_dir / "audit_manifest.json").read_text(encoding="utf-8"))
    tokenizer = json.loads(tokenizer_path.read_text(encoding="utf-8"))
    proof = service["proof"]
    receipt = {"schema_version": "e4-paper-starter-server-receipt/1",
        # The audit's endpoint pin is the client-visible loopback relay inside
        # the private namespace. The physical host endpoint is preserved below.
        "backend": shard, "endpoint": "http://127.0.0.1:8080",
        "model_manifest_sha256": manifest["model_manifest_sha256"],
        "checkpoint_sha256": manifest["model_checkpoint_sha256"],
        "model_revision": manifest["model_revision"],
        "profile_sha256": manifest["model_profile_sha256"], "weights_loaded": True,
        "provenance_path": service["proof_path"], "provenance_sha256": service["proof_sha256"],
        "tokenizer_preflight_path": str(tokenizer_path.relative_to(ROOT)),
        "tokenizer_preflight_sha256": sha(tokenizer_path),
        "tokenizer_prompt_archive_path": tokenizer["prompt_archive_path"],
        "tokenizer_prompt_archive_sha256": tokenizer["prompt_archive_sha256"],
        "physical_service": {"service_role": service["service_role"],
            "actual_model_host": service["actual_model_host"],
            "actual_model_fqdn": proof["actual_model_fqdn"],
            "bridge_kind": service["bridge_kind"],
            "endpoint": service["endpoint"], "service_proof_path": service["proof_path"],
            "service_proof_sha256": service["proof_sha256"],
            "startup_receipt_path": service["startup_receipt_path"],
            "startup_receipt_sha256": service["startup_receipt_sha256"],
            "live_probe": dict(probe), "context_config_sha256": proof["context_config_sha256"],
        "tokenizer_backend": tokenizer.get("backend"), "inference_seed": seed}}
    receipt_path = audit_dir / f"server_receipt_seed{seed}_{shard}.json"
    atomic_json(receipt_path, receipt)
    return receipt_path


def _patch_receipt_validator(audit: Any) -> None:
    """Retain original pins and extend their returned pin with physical service identity."""
    original = audit._validate_server_receipt
    if getattr(original, "_parallel_wrapper", False):
        return

    def validate(path, manifest, shard, *, require_endpoint=True):
        pinned = original(path, manifest, shard, require_endpoint=require_endpoint)
        doc = json.loads(Path(path).read_text(encoding="utf-8"))
        physical = doc.get("physical_service")
        if not isinstance(physical, Mapping) or not physical.get("service_role") or not physical.get("actual_model_host"):
            raise RunnerError("compatible server receipt lacks truthful physical service fields")
        # Preserve logical backend from the original validator and carry facts
        # the frozen combine API otherwise has no representation for.
        pinned["physical_service"] = dict(physical)
        return pinned

    validate._parallel_wrapper = True
    audit._validate_server_receipt = validate


def _binding_context(audit: Any, seed_row: Mapping[str, Any], seed: int):
    from artifacts.escalation_fixes.implementation_20261026.launch.worker_profile import bind_selected_profile
    return bind_selected_profile(audit, seed=seed,
        profile_relative=seed_row["profile_path"], profile_sha256=seed_row["profile_sha256"])


def _namespace_child_command(run_dir: Path, command: list[str]) -> list[str]:
    """Build the shared outer relay supervisor invocation for every execution path."""
    return [str(ROOT / ".venv/bin/python"), str(REMOTE_INFERENCE),
        "--namespace-child", "--socket-path", "/run/e4remote/forward.sock",
        "--run-dir", str(run_dir), "--worker-mode", "diagnostic", "--", *command]


def _worker_transport_preflight(probe_root: Path, *, profile_path: Path, profile_sha256: str,
        seed: int, case_id: str, shard: str) -> dict[str, Any]:
    """Use the real M4 client inside a nested isolated worker namespace, without completion."""
    from e4core.cache import implementation_fingerprint
    from scripts import run_m4_bank as supervisor

    probe_root = project_path(probe_root, must_exist=False)
    if probe_root.exists() or probe_root.is_symlink():
        raise RunnerError("worker transport preflight directory exists; preserving it")
    profile_path = project_path(profile_path)
    if sha(profile_path) != profile_sha256:
        raise RunnerError("worker transport profile hash changed")
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    if profile.get("seed") != seed:
        raise RunnerError("worker transport profile seed differs from the declared seed")
    if shard not in {"local", "wingtip"}:
        raise RunnerError("worker transport preflight logical shard is invalid")
    probe_root.mkdir(parents=True)
    view_root = probe_root / "view" / "project"
    run_dir = probe_root / "worker_run"
    run_dir.mkdir()
    supervisor.create_restricted_project_view(ROOT, view_root,
        profile_path=profile_path, case_id=case_id, split="scored")
    from scripts.run_paper_starter_audit import _install_paper_audit_config
    _install_paper_audit_config(view_root)
    profile_in_view = "/m4project/" + profile_path.relative_to(ROOT).as_posix()
    source = f'''import hashlib,json,os,urllib.request
from pathlib import Path
from e4core.m4_controller import M4ModelClient
from e4core.paper_starter import PROFILE
profile_path=Path({profile_in_view!r})
profile=json.loads(profile_path.read_text())
client=M4ModelClient(profile,base_url=os.environ["E4_BASE_URL"],harness_profile=PROFILE)
request=urllib.request.Request(os.environ["E4_BASE_URL"]+"/v1/models",headers={{"Accept":"application/json"}})
with urllib.request.urlopen(request,timeout=8) as response:
    model_raw=response.read(16384)
    models=json.loads(model_raw)
    model_status=response.status
model_ids=[item.get("id") for item in models.get("data",[]) if isinstance(item,dict)]
if model_status!=200 or profile.get("model_alias") not in model_ids:
    raise RuntimeError("M4 worker namespace cannot reach the pinned model listing")
prompt="Transport preflight only. Tokenize this fixed public string; make no model completion."
payload=client.payload_for_stage("select",prompt)
apply_status,apply_body,applied,apply_error=client._post_json("/apply-template",dict(payload))
if apply_error or not isinstance(applied,dict) or not isinstance(applied.get("prompt"),str):
    raise RuntimeError("M4 worker apply-template failed: "+str(apply_error or apply_status))
token_request={{"content":applied["prompt"],"add_special":True,"parse_special":True}}
token_status,token_body,token_response,token_error=client._post_json("/tokenize",token_request)
tokens=token_response.get("tokens") if isinstance(token_response,dict) else None
if token_error or token_status!=200 or not isinstance(tokens,list) or any(type(x) is not int for x in tokens):
    raise RuntimeError("M4 worker tokenize failed: "+str(token_error or token_status))
if len(tokens)+8192>client.context_tokens:
    raise RuntimeError("M4 worker transport prompt does not fit the frozen generation reserve")
result={{"schema_version":"e4-parallel-m4-worker-transport-preflight/1",
    "status":"complete","profile_seed":profile.get("seed"),
    "profile_sha256":{profile_sha256!r},"model_alias":profile.get("model_alias"),
    "model_list_http_status":model_status,"model_list_sha256":hashlib.sha256(model_raw).hexdigest(),
    "apply_template_http_status":apply_status,
    "apply_template_response_sha256":hashlib.sha256(apply_body.encode()).hexdigest(),
    "tokenize_http_status":token_status,"tokenize_response_sha256":hashlib.sha256(token_body.encode()).hexdigest(),
    "prompt_token_count":len(tokens),"context_tokens":client.context_tokens,
    "generation_reserve_tokens":8192,"completion_requests_made":0,
    "model_generation_endpoint_called":False}}
out=Path("/m4run/worker_transport_preflight.json")
fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
with os.fdopen(fd,"w") as f: f.write(json.dumps(result,sort_keys=True)+"\\n")
'''
    command = ["/m4project/.venv/bin/python", "-c", source]
    inner = supervisor._bwrap_command(view_root=view_root, run_dir=run_dir,
        library_root=None, profile_relative=profile_path.relative_to(ROOT).as_posix(),
        mode="diagnostic", case_id=case_id, split="scored",
        endpoint="http://127.0.0.1:8080", feedback_socket=None,
        implementation_fingerprint_value=implementation_fingerprint(),
        command_override=command)
    if shard == "wingtip":
        separator = inner.index("--")
        inner[separator:separator] = ["--dir", "/run", "--dir", "/run/e4remote",
            "--ro-bind", "/run/e4remote-source", "/run/e4remote"]
    stdout_path, stderr_path = probe_root / "worker.stdout.log", probe_root / "worker.stderr.log"
    with stdout_path.open("x") as stdout, stderr_path.open("x") as stderr:
        process = subprocess.Popen(inner, cwd=ROOT, env={}, stdout=stdout, stderr=stderr,
            start_new_session=True)
        try:
            code = process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise RunnerError("nested M4 worker transport preflight exceeded 120 seconds")
    result_path = run_dir / "worker_transport_preflight.json"
    if code != 0 or not result_path.is_file():
        raise RunnerError(f"nested M4 worker apply-template/tokenize preflight failed for {shard}")
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if (result.get("status") != "complete" or result.get("profile_seed") != seed or
            result.get("profile_sha256") != profile_sha256 or result.get("completion_requests_made") != 0 or
            result.get("model_generation_endpoint_called") is not False):
        raise RunnerError("nested M4 worker transport preflight receipt failed identity/no-completion checks")
    return {"result_path": str(result_path.relative_to(ROOT)),
        "result_sha256": sha(result_path), "worker_stdout_path": str(stdout_path.relative_to(ROOT)),
        "worker_stderr_path": str(stderr_path.relative_to(ROOT)), **result}


def verify_request_seed_proof(state: Mapping[str, Any], *, shard: str, seed: int,
        profile_relative: str, profile_sha256: str, expected_cases: list[str],
        initial_snapshot_id: str) -> dict[str, Any]:
    """Verify outgoing request seeds while retaining infrastructure failures in N."""
    from artifacts.escalation_fixes.implementation_20261026.launch.worker_profile import _json_sha
    cases = state.get("completed_cases")
    if (state.get("status") != "complete" or state.get("shard") != shard or
            not isinstance(cases, list) or [row.get("case_id") for row in cases] != expected_cases or
            len(cases) != 5):
        raise RunnerError("completed shard does not preserve the exact five-case fixed-order denominator")
    if cases[0].get("input_library_snapshot_id") != initial_snapshot_id:
        raise RunnerError("shard did not start from its fresh frozen initial library")
    proof_rows = []
    total_requests = 0
    unknown_usage = []
    for index, row in enumerate(cases, start=1):
        if row.get("model_calls") is None:
            unknown_usage.append(row["case_id"])
        if row.get("status") == "failed_infrastructure" and row.get("preserved_failed_trajectory"):
            case_root = project_path(row["preserved_failed_trajectory"])
            result_ref = None
        else:
            result_ref = project_path(row["diagnostic_result_path"])
            case_root = result_ref.parent.parent
        binding_path = case_root / "worker_profile_binding.json"
        if binding_path.is_symlink():
            raise RunnerError(f"case{index:02d} worker profile binding cannot be a symlink")
        requests = []
        if binding_path.is_file() and not binding_path.is_symlink():
            binding = json.loads(binding_path.read_text(encoding="utf-8"))
            if (binding.get("seed") != seed or binding.get("case_id") != row["case_id"] or
                    binding.get("profile_path") != profile_relative or
                    binding.get("profile_sha256") != profile_sha256 or
                    binding.get("remote_transport") != (shard == "wingtip")):
                raise RunnerError(f"case{index:02d} actual worker profile binding differs from the sealed seed")
            if result_ref is not None:
                journal = result_ref.parent / "model_calls.jsonl"
                if journal.is_file() and not journal.is_symlink():
                    for line in journal.read_text(encoding="utf-8").splitlines():
                        if not line.strip():
                            continue
                        call = json.loads(line)
                        payload = call.get("request_payload")
                        if not isinstance(payload, Mapping) or payload.get("seed") != seed:
                            raise RunnerError(f"case{index:02d} outgoing request seed differs from {seed}")
                        requests.append({"call_id": call.get("call_id"),
                            "request_seed": payload["seed"], "payload_sha256": _json_sha(payload),
                            "model": payload.get("model")})
        elif row.get("status") != "failed_infrastructure":
            raise RunnerError(f"case{index:02d} completed trajectory lacks its actual worker profile binding")
        if row.get("status") != "failed_infrastructure" and not requests:
            raise RunnerError(f"case{index:02d} has no outgoing model request journal")
        total_requests += len(requests)
        proof_rows.append({"case_index": index, "case_id": row["case_id"],
            "status": row.get("status"), "accepted": row.get("accepted"),
            "attempted_requests": len(requests),
            "worker_profile_binding_sha256": sha(binding_path) if binding_path.is_file() else None,
            "requests": requests,
            "usage": {key: row.get(key) for key in
                ("model_calls", "prompt_tokens", "completion_tokens", "charged_generated_tokens", "model_seconds")}})
    return {"schema_version": "e4-parallel-request-seed-proof/1",
        "status": "all_observed_request_seeds_verified", "seed": seed, "shard": shard,
        "case_count": len(cases), "request_count": total_requests,
        "profile_path": profile_relative, "profile_sha256": profile_sha256,
        "initial_library_snapshot_id": initial_snapshot_id,
        "all_observed_request_seeds_match": True,
        "failed_infrastructure_case_ids": [row["case_id"] for row in cases
            if row.get("status") == "failed_infrastructure"],
        "unknown_usage_case_ids": unknown_usage, "cases": proof_rows}


class InferenceTransportFailure(RunnerError):
    pass

def _install_failfast_case_guard(audit: Any) -> None:
    """Check service health before/after each case; preserve an interrupted slot."""
    original = audit._run_case
    def healthy() -> bool:
        try:
            with urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=5) as response:
                if response.status != 200:
                    return False
                body = json.loads(response.read().decode("utf-8"))
                return body.get("status") in {"ok", "ready"}
        except Exception:
            return False
    def telemetry_transport_evidence(case_root: Path) -> str | None:
        """Inspect only whitelisted failure code/status fields, never prompt or response text."""
        calls_path = case_root / "diagnostic" / "model_calls.jsonl"
        if not calls_path.is_file():
            return None
        try:
            with calls_path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    error = row.get("error") if isinstance(row, Mapping) else None
                    if isinstance(error, Mapping):
                        code = error.get("code")
                        status = error.get("http_status")
                        details = error.get("details")
                        if code == "transport_error":
                            return "model_calls.jsonl:transport_error"
                        if type(status) is int and status >= 500:
                            return f"model_calls.jsonl:http_{status}"
                        if code == "context_preflight_error" and isinstance(details, Mapping):
                            if details.get("code") == "transport_error":
                                return "model_calls.jsonl:context_preflight_transport_error"
                            detail_status = details.get("http_status", details.get("status"))
                            if type(detail_status) is int and detail_status >= 500:
                                return f"model_calls.jsonl:context_preflight_http_{detail_status}"
                    if isinstance(row, Mapping):
                        status = row.get("http_status")
                        if type(status) is int and status >= 500:
                            return f"model_calls.jsonl:http_{status}"
                        preflight = row.get("preflight_calls")
                        if isinstance(preflight, list):
                            for item in preflight:
                                if not isinstance(item, Mapping):
                                    continue
                                suberror = item.get("error")
                                if isinstance(suberror, Mapping):
                                    if suberror.get("code") == "transport_error":
                                        return "model_calls.jsonl:preflight_transport_error"
                                    substatus = suberror.get("http_status", suberror.get("status"))
                                    if type(substatus) is int and substatus >= 500:
                                        return f"model_calls.jsonl:preflight_http_{substatus}"
                                substatus = item.get("http_status")
                                if type(substatus) is int and substatus >= 500:
                                    return f"model_calls.jsonl:preflight_http_{substatus}"
        except (OSError, json.JSONDecodeError, TypeError):
            return None
        return None
    def transport_evidence(exc: BaseException, args: tuple[Any, ...]) -> str | None:
        patterns = ("connection refused", "connection reset", "connection closed",
            "remote disconnected", "failed to connect", "service unavailable",
            "inference server unavailable", "model server unavailable", "http 500",
            "http 502", "http 503", "http 504", "bad gateway", "gateway timeout")
        text = f"{type(exc).__name__}: {exc}".lower()
        try:
            index = int(args[1])
            shard_root = Path(args[3])
            case_root = shard_root / f"case{index:02d}"
            telemetry = telemetry_transport_evidence(case_root)
            if telemetry:
                return telemetry
            for name in ("outer.stdout.log", "outer.stderr.log"):
                log = case_root / name
                if log.is_file():
                    text += "\n" + log.read_text(encoding="utf-8", errors="replace")[-20000:].lower()
        except (IndexError, TypeError, ValueError, OSError):
            pass
        return next((pattern for pattern in patterns if pattern in text), None)
    def record_outage(args: tuple[Any, ...], *, reason: str, attempted: bool) -> None:
        try:
            case_id, index, _input_library, shard_root, _manifest, _truth, shard = args[:7]
            shard_root = Path(shard_root)
            receipt = {"schema_version": "e4-inference-transport-failure/1",
                "status": "failed_infrastructure", "case_id": str(case_id),
                "case_index": int(index), "logical_shard": str(shard),
                "request_attempted_or_inflight": bool(attempted),
                "usage_status": "unknown" if attempted else "no_case_request_started",
                "reported_usage": None, "reason": reason,
                "recorded_utc_epoch": time.time()}
            target = shard_root / "transport_failure.json"
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(receipt, stream, sort_keys=True)
                stream.write("\n")
        except FileExistsError:
            raise RunnerError("preserving existing transport failure receipt")
        except (IndexError, TypeError, ValueError, OSError):
            # The original failure remains primary if an incomplete call shape
            # prevents a supplemental receipt from being written.
            return
    def guarded(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if not healthy():
            record_outage(args, reason="health endpoint unavailable before case", attempted=False)
            raise InferenceTransportFailure("inference service unavailable before case; case not started")
        try:
            result = original(*args, **kwargs)
        except Exception as exc:
            case_id, index, input_library, shard_root = args[:4]
            case_root = Path(shard_root) / f"case{int(index):02d}"
            evidence = telemetry_transport_evidence(case_root)
            if evidence:
                record_outage(args, reason=evidence, attempted=True)
                raise InferenceTransportFailure(
                    f"inference transport outage during case ({evidence}); usage may be unknown") from exc
            summary_path = Path(shard_root) / "trusted" / f"case{int(index):02d}_summary.json"
            if summary_path.is_file():
                # The frozen audit driver raises on nonzero worker exit after
                # writing a fully verified result/ledger summary. Commit that
                # exact ordinary failed trajectory once; never rewrite it.
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                input_snapshot = audit.snapshot_library(Path(input_library))
                audit._verify_case_summary(summary, input_snapshot.snapshot_id)
                if (summary.get("case_id") != case_id or
                        type(summary.get("worker_exit_code")) is not int or
                        summary.get("worker_exit_code") == 0):
                    raise RunnerError("trusted completed failure summary does not match this nonzero-exit case") from exc
                return summary
            evidence = transport_evidence(exc, args)
            if not healthy() or evidence:
                reason = evidence or "health endpoint unavailable"
                record_outage(args, reason=reason, attempted=True)
                raise InferenceTransportFailure(
                    f"inference transport outage during case ({reason}); usage may be unknown") from exc
            raise
        index = int(args[1])
        shard_root = Path(args[3])
        case_root = shard_root / f"case{index:02d}"
        evidence = telemetry_transport_evidence(case_root)
        if evidence:
            record_outage(args, reason=evidence, attempted=True)
            raise InferenceTransportFailure(
                f"inference transport outage recorded for completed case ({evidence}); usage may be unknown")
        # A committed successful diagnostic must survive a later health drop.
        # The next case's preflight health check will stop the chain before a request.
        return result
    audit._run_case = guarded

def _remote_child_command(audit_dir: Path, shard: str, receipt_path: Path,
                          seed_row: Mapping[str, Any], seed: int, *,
                          preflight_only: bool = False) -> list[str]:
    # Executed in the isolated parent network namespace. The only communication
    # route available to its M4 workers is loopback:8080 through the mounted
    # owned Unix socket. Wingtip's legacy marker controls only its inner mount.
    remote_env = (f"os.environ['E4_REMOTE_TRANSPORT']={REMOTE_MARKER!r}"
        if shard == "wingtip" else "os.environ.pop('E4_REMOTE_TRANSPORT',None)")
    source = f'''import os, json
from pathlib import Path
{remote_env}
from scripts import run_paper_starter_audit as audit
from artifacts.escalation_fixes.implementation_20261026.launch.worker_profile import bind_selected_profile
from artifacts.escalation_fixes.parallel_scored_20261030.parallel_runner_v5 import _patch_receipt_validator, _worker_transport_preflight, _install_failfast_case_guard
_patch_receipt_validator(audit)
_install_failfast_case_guard(audit)
audit_dir = Path({str(audit_dir)!r})
receipt_path = Path({str(receipt_path)!r})
profile_context = bind_selected_profile(audit, seed={seed},
    profile_relative={seed_row['profile_path']!r}, profile_sha256={seed_row['profile_sha256']!r})
profile_context.__enter__()
recovery = None
try:
    manifest = json.loads((audit_dir / "audit_manifest.json").read_text(encoding="utf-8"))
    worker_preflight = _worker_transport_preflight(
        audit_dir / "worker_transport_preflight_seed{seed}_{shard}",
        profile_path=Path({str(ROOT / seed_row['profile_path'])!r}),
        profile_sha256={seed_row['profile_sha256']!r}, seed={seed},
        case_id=manifest["shard_groups"][{shard!r}][0], shard={shard!r})
    if {preflight_only!r}:
        print(json.dumps({{"worker_transport_preflight": worker_preflight,
            "case_shard_started": False, "completion_requests_made": 0}}, sort_keys=True))
        raise SystemExit(0)
    state = audit.run_shard(audit_dir, {shard!r}, server_receipt=receipt_path)
    print(json.dumps({{"state": state,
        "worker_transport_preflight": worker_preflight}}, sort_keys=True))
finally:
    profile_context.__exit__(None, None, None)
'''
    return [str(ROOT / ".venv/bin/python"), "-c", source]


def execute_task(run_root: Path, service_map_path: Path, *, seed: int, shard: str,
                 service_role: str, dry_process: bool = False) -> dict[str, Any]:
    """Preflight and run one fixed logical chain through a generic private relay."""
    run_root = project_path(run_root)
    parallel_manifest = json.loads((run_root / "parallel_manifest.json").read_text())
    _verify_runner_plan(run_root, parallel_manifest)
    if parallel_manifest.get("schema_version") != RUNNER_SCHEMA or seed not in SEEDS or shard not in {"local", "wingtip"}:
        raise RunnerError("task arguments do not match the prepared two-seed plan")
    expected_role = parallel_manifest["service_assignment"][f"seed{seed}"][shard]
    if service_role != expected_role:
        raise RunnerError("physical service role differs from the predeclared schedule")
    service, _proof_path = _service_doc(service_map_path, service_role)
    from scripts import run_paper_starter_audit as audit
    _patch_receipt_validator(audit)
    from artifacts.escalation_fixes.implementation_20261026.launch.prepare_frozen_rerun import validate_launch_bundle
    rerun_path = project_path(parallel_manifest["rerun_manifest_path"])
    rerun, seed_row, _ = validate_launch_bundle(rerun_path, seed)
    audit_dir = project_path(parallel_manifest["audits"][str(seed)]["audit_dir"])
    probe = _probe_endpoint(service, log_path=audit_dir / f"seed{seed}_{shard}_preflight_ssh.log")
    freeze_path = project_path(seed_row["derived_freeze_path"])
    preflight_dir = audit_dir / f"preflight_seed{seed}_{shard}"
    if dry_process:
        # This mode validates the actual relay/nested-worker path only. The
        # nested M4ModelClient test below exercises apply-template/tokenize and
        # exits before run_shard; no substitute server receipt is written.
        compat_receipt_path = audit_dir / f"unused_preflight_only_seed{seed}_{shard}.json"
    else:
        # Bind the selected seed at the actual tokenizer preflight as requested.
        if preflight_dir.exists():
            raise RunnerError("preflight output exists; preserving it")
        endpoint_env = os.environ.get("E4_BASE_URL")
        try:
            with _service_tcp_access(service, audit_dir / f"seed{seed}_{shard}_tokenizer_ssh.log") as tcp_endpoint:
                os.environ["E4_BASE_URL"] = tcp_endpoint
                with _binding_context(audit, seed_row, seed):
                    token = audit.preflight(freeze_path, preflight_dir, backend=shard)
        finally:
            if endpoint_env is None:
                os.environ.pop("E4_BASE_URL", None)
            else:
                os.environ["E4_BASE_URL"] = endpoint_env
        tokenizer_path = preflight_dir / "tokenizer_preflight.json"
        if (token.get("completion_requests_made") != 0 or token.get("all_fit") is not True or
                token.get("context_tokens") != service["proof"]["context_tokens"]):
            raise RunnerError("live tokenizer preflight does not match the service proof/context")
        compat_receipt_path = _compat_receipt(audit_dir, seed, shard, service, probe, tokenizer_path)
    _probe_endpoint(service, log_path=audit_dir / f"seed{seed}_{shard}_prelaunch_ssh.log")

    # The shared run_shard worker expects the legacy transport label only for
    # its inner socket mount. Physical host/role are carried in the receipt.
    outer = os.environ.get("E4_BASE_URL")
    os.environ["E4_BASE_URL"] = "http://127.0.0.1:8080"
    os.environ["E4_RUN_DIR"] = str(audit_dir)
    if shard == "wingtip":
        os.environ["E4_REMOTE_TRANSPORT"] = REMOTE_MARKER
    else:
        os.environ.pop("E4_REMOTE_TRANSPORT", None)
    try:
        import tempfile
        with tempfile.TemporaryDirectory(prefix="e4ps", dir="/tmp") as tmp:
            socket_dir = Path(tmp)
            bridge = None
            ssh_process = None
            from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
            if service.get("bridge_kind") == "ssh-unix-forward":
                ssh_log = audit_dir / f"seed{seed}_{shard}_model_ssh.log"
                ssh_process, socket_dir, _ = transport.connect_ssh(int(service["remote_port"]), ssh_log=ssh_log)
            else:
                sock = socket_dir / "forward.sock"
                bridge = UnixToTCPBridge(sock, service["endpoint"])
                bridge.start()
            try:
                command = _remote_child_command(audit_dir, shard, compat_receipt_path, seed_row, seed,
                    preflight_only=dry_process)
                namespace_command = _namespace_child_command(audit_dir, command)
                wrapper = transport.build_bwrap_command(run_dir=audit_dir,
                    socket_dir=socket_dir, command=namespace_command)
                stdout_path = audit_dir / f"seed{seed}_{shard}_outer.stdout.log"
                stderr_path = audit_dir / f"seed{seed}_{shard}_outer.stderr.log"
                with stdout_path.open("x") as stdout, stderr_path.open("x") as stderr:
                    proc = subprocess.Popen(wrapper, cwd=ROOT, env={}, stdout=stdout, stderr=stderr,
                        start_new_session=True)
                    try:
                        code = proc.wait(timeout=5 * 60 * 60)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGTERM)
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(proc.pid, signal.SIGKILL)
                            proc.wait()
                        raise RunnerError("isolated shard exceeded five-hour outer timeout")
            finally:
                if bridge is not None:
                    bridge.close()
                transport.terminate_own_process(ssh_process)
                if ssh_process is not None:
                    shutil.rmtree(socket_dir, ignore_errors=True)
            if code != 0:
                raise RunnerError(f"isolated run_shard exited {code}; preserving trajectory/logs")
            if dry_process:
                preflight_path = audit_dir / f"worker_transport_preflight_seed{seed}_{shard}" / "worker_run" / "worker_transport_preflight.json"
                preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
                task_receipt = {"schema_version": "e4-parallel-transport-preflight-task/1",
                    "status": "transport_ready_no_cases", "seed": seed, "logical_shard": shard,
                    "service_role": service_role, "actual_model_host": service["actual_model_host"],
                    "actual_model_fqdn": service["proof"]["actual_model_fqdn"],
                    "endpoint": service["endpoint"], "bridge_kind": service["bridge_kind"],
                    "worker_transport_preflight_path": str(preflight_path.relative_to(ROOT)),
                    "worker_transport_preflight_sha256": sha(preflight_path),
                    "worker_transport_preflight": preflight,
                    "case_shard_started": False, "completion_requests_made": 0,
                    "outer_stdout_path": str(stdout_path.relative_to(ROOT)),
                    "outer_stderr_path": str(stderr_path.relative_to(ROOT))}
                task_path = audit_dir / f"seed{seed}_{shard}_transport_preflight_task.json"
                atomic_json(task_path, task_receipt)
                return {"status": task_receipt["status"],
                    "task_receipt_path": str(task_path.relative_to(ROOT)),
                    "task_receipt_sha256": sha(task_path)}
            resumed = sorted(audit_dir.glob(f"bank10_{shard}_resume_*/bank10_status.json"))
            status_path = resumed[-1] if resumed else audit_dir / f"bank10_{shard}" / "bank10_status.json"
            status = json.loads(status_path.read_text(encoding="utf-8"))
            if status.get("status") != "complete" or len(status.get("completed_cases", [])) != 5:
                raise RunnerError("run_shard did not complete its fixed five-case chain")
            proof = verify_request_seed_proof(status, shard=shard, seed=seed,
                profile_relative=seed_row["profile_path"], profile_sha256=seed_row["profile_sha256"],
                expected_cases=rerun["shard_groups"][shard],
                initial_snapshot_id=rerun["initial_library_snapshot_id"])
            proof_path = audit_dir / f"seed{seed}_{shard}_request_seed_proof.json"
            atomic_json(proof_path, proof)
            task_receipt = {"schema_version": "e4-parallel-task-receipt/1",
                "seed": seed, "logical_shard": shard, "service_role": service_role,
                "actual_model_host": service["actual_model_host"],
                "actual_model_fqdn": service["proof"]["actual_model_fqdn"],
                "bridge_kind": service["bridge_kind"], "endpoint": service["endpoint"],
                "service_proof_path": service["proof_path"],
                "service_proof_sha256": service["proof_sha256"],
                "startup_receipt_path": service["startup_receipt_path"],
                "startup_receipt_sha256": service["startup_receipt_sha256"],
                "live_probe": probe, "compat_server_receipt_path": str(compat_receipt_path.relative_to(ROOT)),
                "status_path": str(status_path.relative_to(ROOT)), "status_sha256": sha(status_path),
                "request_seed_proof_path": str(proof_path.relative_to(ROOT)),
                "request_seed_proof_sha256": sha(proof_path),
                "bridge_metrics": bridge.metrics() if bridge is not None else {"kind": "ssh-unix-forward"},
                "outer_stdout_path": str(stdout_path.relative_to(ROOT)),
                "outer_stderr_path": str(stderr_path.relative_to(ROOT)), "exit_code": code}
            task_path = audit_dir / f"seed{seed}_{shard}_task_receipt.json"
            atomic_json(task_path, task_receipt)
            return {"status": "complete", "task_receipt_path": str(task_path.relative_to(ROOT)),
                "task_receipt_sha256": sha(task_path)}
    finally:
        if outer is None:
            os.environ.pop("E4_BASE_URL", None)
        else:
            os.environ["E4_BASE_URL"] = outer
        os.environ.pop("E4_REMOTE_TRANSPORT", None)


def _run_task_process(script: Path, run_root: Path, service_map: Path, *,
                      seed: int, shard: str, role: str) -> subprocess.Popen:
    command = [sys.executable, str(script), "--execute-task",
        "--run-root", str(run_root), "--service-map", str(service_map),
        "--seed", str(seed), "--shard", shard, "--service-role", role]
    audit_root = project_path(json.loads((run_root / "parallel_manifest.json").read_text())[
        "audits"][str(seed)]["audit_dir"])
    stdout_path = audit_root / f"seed{seed}_{shard}_task_process.stdout.log"
    stderr_path = audit_root / f"seed{seed}_{shard}_task_process.stderr.log"
    with stdout_path.open("x") as stdout, stderr_path.open("x") as stderr:
        process = subprocess.Popen(command, cwd=ROOT, env={}, stdout=stdout, stderr=stderr,
            start_new_session=True)
    return process


def check_services(service_map_path: Path, check_root: Path) -> dict[str, Any]:
    """Perform three no-completion live health/model checks and write their proofs."""
    service_map_path = project_path(service_map_path)
    check_root = project_path(check_root, must_exist=False)
    if not check_root.is_relative_to(ARTIFACTS):
        raise RunnerError("service check output must stay in this experiment artifact folder")
    if check_root.exists() or check_root.is_symlink():
        raise RunnerError("service check output exists; preserving prior evidence")
    check_root.mkdir(parents=True)
    checks = {}
    for role in ("local_p100_v100", "wingtip_rtx8000"):
        service, _ = _service_doc(service_map_path, role)
        probe = _probe_endpoint(service, log_path=check_root / f"{role}_ssh.log")
        checks[role] = {"service_role": role,
            "actual_model_host": service["actual_model_host"],
            "actual_model_fqdn": service["proof"]["actual_model_fqdn"],
            "endpoint": service["endpoint"], "bridge_kind": service["bridge_kind"],
            "service_proof_path": service["proof_path"],
            "service_proof_sha256": service["proof_sha256"],
            "startup_receipt_path": service["startup_receipt_path"],
            "startup_receipt_sha256": service["startup_receipt_sha256"],
            "live_probe": probe, "completion_requests_made": 0}
    receipt = {"schema_version": "e4-parallel-live-service-check/1",
        "status": "both_model_services_healthy", "service_map_path": str(service_map_path.relative_to(ROOT)),
        "service_map_sha256": sha(service_map_path), "services": checks,
        "completion_requests_made": 0, "model_generation_requests_made": 0}
    atomic_json(check_root / "service_checks.json", receipt)
    return receipt


def namespace_smoke(service_map_path: Path, smoke_root: Path) -> dict[str, Any]:
    """Exercise production outer relay plus nested M4 apply-template/tokenize on each service route."""
    service_map_path = project_path(service_map_path)
    smoke_root = project_path(smoke_root, must_exist=False)
    if not smoke_root.is_relative_to(ARTIFACTS):
        raise RunnerError("namespace smoke output must stay in this experiment artifact folder")
    if smoke_root.exists() or smoke_root.is_symlink():
        raise RunnerError("namespace smoke output exists; preserving prior evidence")
    smoke_root.mkdir(parents=True)
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    from artifacts.escalation_fixes.implementation_20261026.launch.prepare_frozen_rerun import CASE_ORDER
    results = {}
    task_routes = (("local_p100_v100", "local"), ("wingtip_rtx8000", "wingtip"))
    for role, shard in task_routes:
        service, _ = _service_doc(service_map_path, role)
        route_name = f"{role}_{shard}"
        probe = _probe_endpoint(service, log_path=smoke_root / f"{route_name}_probe_ssh.log")
        run_dir = smoke_root / route_name / "seed43_smoke"
        profile_path = run_dir / "profile.json"
        run_dir.mkdir(parents=True)
        profile = json.loads((ROOT / "config/gemma_m3_inference.json").read_text(encoding="utf-8"))
        profile["seed"] = 43
        atomic_json(profile_path, profile)
        profile_relative = profile_path.relative_to(ROOT).as_posix()
        profile_sha = sha(profile_path)
        smoke_target = run_dir / "namespace_smoke_result.json"
        source = f'''import json,os
from pathlib import Path
from scripts import run_paper_starter_audit as audit
from artifacts.escalation_fixes.implementation_20261026.launch.worker_profile import bind_selected_profile
before=audit._worker_command
with bind_selected_profile(audit,seed=43,profile_relative={profile_relative!r},profile_sha256={profile_sha!r}):
    bound=audit._worker_command is not before
    from artifacts.escalation_fixes.parallel_scored_20261030.parallel_runner_v5 import _worker_transport_preflight
    preflight=_worker_transport_preflight(Path(os.environ["E4_RUN_DIR"])/"m4_worker_preflight",
        profile_path=Path({str(profile_path)!r}),profile_sha256={profile_sha!r},
        seed=43,case_id={CASE_ORDER[0]!r},shard={shard!r})
    if not bound or json.loads(Path({str(profile_path)!r}).read_text()).get("seed")!=43:
        raise RuntimeError("seed43 selected profile was not bound")
    receipt={{"schema_version":"e4-parallel-namespace-smoke/2","status":"complete",
        "profile_path":{profile_relative!r},"profile_sha256":{profile_sha!r},
        "profile_seed":43,"profile_binding_active":bound,"logical_shard":{shard!r},
        "case_id":{CASE_ORDER[0]!r},"m4_worker_transport_preflight":preflight,
        "completion_requests_made":0}}
    out=Path(os.environ["E4_RUN_DIR"])/"namespace_smoke_result.json"
    fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o644)
    with os.fdopen(fd,"w") as f: f.write(json.dumps(receipt,sort_keys=True)+"\\n")
'''
        socket_dir = None
        ssh_process = None
        bridge = None
        with __import__("tempfile").TemporaryDirectory(prefix="e4ps", dir="/tmp") as temp:
            socket_dir = Path(temp)
            if service["bridge_kind"] == "ssh-unix-forward":
                ssh_process, socket_dir, _ = transport.connect_ssh(int(service["remote_port"]),
                    ssh_log=smoke_root / f"{route_name}_model_ssh.log")
            else:
                bridge = UnixToTCPBridge(socket_dir / "forward.sock", service["endpoint"])
                bridge.start()
            try:
                child = [str(ROOT / ".venv/bin/python"), "-c", source]
                namespace_command = _namespace_child_command(run_dir, child)
                wrapper = transport.build_bwrap_command(run_dir=run_dir,
                    socket_dir=socket_dir, command=namespace_command)
                stdout_path, stderr_path = run_dir / "outer.stdout.log", run_dir / "outer.stderr.log"
                with stdout_path.open("x") as stdout, stderr_path.open("x") as stderr:
                    proc = subprocess.Popen(wrapper, cwd=ROOT, env={}, stdout=stdout, stderr=stderr,
                        start_new_session=True)
                    try:
                        code = proc.wait(timeout=240)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGTERM)
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(proc.pid, signal.SIGKILL)
                            proc.wait()
                        raise RunnerError(f"{route_name} namespace smoke exceeded 240 seconds")
            finally:
                if bridge is not None:
                    bridge.close()
                transport.terminate_own_process(ssh_process)
                if ssh_process is not None:
                    shutil.rmtree(socket_dir, ignore_errors=True)
        if code != 0 or not smoke_target.is_file():
            raise RunnerError(f"{role} namespace smoke failed; preserving logs")
        result = json.loads(smoke_target.read_text(encoding="utf-8"))
        worker_check = result.get("m4_worker_transport_preflight", {})
        if (result.get("profile_seed") != 43 or result.get("completion_requests_made") != 0 or
                result.get("profile_binding_active") is not True or
                worker_check.get("status") != "complete" or
                worker_check.get("apply_template_http_status") != 200 or
                worker_check.get("tokenize_http_status") != 200 or
                worker_check.get("completion_requests_made") != 0):
            raise RunnerError(f"{route_name} smoke did not prove nested M4 profile43 apply-template/tokenize")
        results[route_name] = {"run_dir": str(run_dir.relative_to(ROOT)),
            "smoke_result_path": str(smoke_target.relative_to(ROOT)),
            "smoke_result_sha256": sha(smoke_target), "profile_path": profile_relative,
            "logical_shard": shard, "service_role": role,
            "profile_sha256": profile_sha, "service_proof_path": service["proof_path"],
            "service_proof_sha256": service["proof_sha256"], "live_probe": probe,
            "completion_requests_made": 0,
            "bridge_metrics": bridge.metrics() if bridge is not None else {"kind": "ssh-unix-forward"}}
    receipt = {"schema_version": "e4-parallel-namespace-smoke-suite/1", "status": "complete",
        "service_map_path": str(service_map_path.relative_to(ROOT)),
        "service_map_sha256": sha(service_map_path), "services": results,
        "completion_requests_made": 0}
    atomic_json(smoke_root / "namespace_smoke_suite.json", receipt)
    return receipt


def orchestrate(run_root: Path, service_map_path: Path) -> dict[str, Any]:
    """Run two independent service chains with seed43 gated by seed42 per service."""
    run_root = project_path(run_root)
    service_map_path = project_path(service_map_path)
    manifest = json.loads((run_root / "parallel_manifest.json").read_text())
    _verify_runner_plan(run_root, manifest)
    snapshot_path = run_root / "service_map_snapshot.json"
    if snapshot_path.exists() or snapshot_path.is_symlink():
        raise RunnerError("service map snapshot already exists; preserving prior orchestration")
    snapshot_path.write_bytes(service_map_path.read_bytes())
    service_map_path = snapshot_path
    roles = {42: {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"},
        43: {"local": "local_p100_v100", "wingtip": "wingtip_rtx8000"}}
    if manifest.get("service_assignment") != {"seed42": roles[42], "seed43": roles[43]}:
        raise RunnerError("predeclared service assignment changed")
    # Read and prove all service identities/health before starting any scored work.
    for role in sorted({value for plan in roles.values() for value in plan.values()}):
        row, _ = _service_doc(service_map_path, role)
        _probe_endpoint(row, log_path=run_root / "service_checks" / f"{role}_ssh.log")
    # Global gate: prove the full outer relay and nested M4 worker namespace
    # can perform model-list, apply-template, and tokenize on every task route.
    # This makes no completions and runs before any shard process is started.
    transport_smoke = namespace_smoke(service_map_path, run_root / "namespace_smoke_prelaunch")
    transport_smoke_path = run_root / "namespace_smoke_prelaunch" / "namespace_smoke_suite.json"
    launcher = Path(__file__).resolve()
    task_status = _drain_two_service_chains(
        lambda **kw: _run_task_process(launcher, run_root, service_map_path, **kw), roles)

    from scripts import run_paper_starter_audit as audit
    _patch_receipt_validator(audit)
    combined = {}
    for seed in SEEDS:
        audit_dir = project_path(manifest["audits"][str(seed)]["audit_dir"])
        if not all(task_status[f"{seed}.{shard}"]["state"] == "complete"
                for shard in ("local", "wingtip")):
            continue
        task_local = json.loads((audit_dir / f"seed{seed}_local_task_receipt.json").read_text())
        task_wingtip = json.loads((audit_dir / f"seed{seed}_wingtip_task_receipt.json").read_text())
        local_status = project_path(task_local["status_path"])
        wingtip_status = project_path(task_wingtip["status_path"])
        # Existing combine reruns all original checks and validates these
        # additions by reloading the immutable physical service receipt.
        combined[str(seed)] = audit.combine(audit_dir, local_status, wingtip_status)
    all_tasks_complete = all(task_status[task]["state"] == "complete"
        for task in ("42.local", "42.wingtip", "43.local", "43.wingtip"))
    record = {"schema_version": RUNNER_SCHEMA,
        "status": "complete" if all_tasks_complete else "partial_with_failures",
        "runner_manifest_sha256": sha(run_root / "parallel_manifest.json"),
        "service_map_path": str(service_map_path.relative_to(ROOT)),
        "service_map_sha256": sha(service_map_path),
        "transport_smoke_path": str(transport_smoke_path.relative_to(ROOT)),
        "transport_smoke_sha256": sha(transport_smoke_path),
        "transport_smoke_completion_requests_made": transport_smoke.get("completion_requests_made"),
        "scheduled_tasks": [task_status[task] for task in
            ("42.local", "42.wingtip", "43.local", "43.wingtip")],
        "seed_results": {seed: {"combined_result_path": str((project_path(manifest["audits"][seed]["audit_dir"]) / "combined_bank10_result.json").relative_to(ROOT)),
            "combined_result_sha256": sha(project_path(manifest["audits"][seed]["audit_dir"]) / "combined_bank10_result.json"),
            "case_count": combined[seed]["case_count"],
            "cumulative_top3_prefix_successes": combined[seed]["cumulative_top3_prefix_successes"]}
            for seed in combined},
        "partial_successful_shards": [task for task, status in task_status.items()
            if status.get("state") == "complete"],
        "total_fresh_cases": sum(5 for status in task_status.values() if status.get("state") == "complete"),
        "case_reuse_note": "paired_r5 case set reused; this is not a holdout or new-for-method evaluation", "official_m5_scored_claim": False,
        "fresh_holdout_claim": False}
    atomic_json(run_root / "orchestration_result.json", record)
    return record

def _drain_two_service_chains(launch_task: Any, roles: Mapping[int, Mapping[str, str]]) -> dict[str, dict[str, Any]]:
    """Run each service's seed42→seed43 chain independently; failures do not stop the peer."""
    active: dict[str, Any] = {}
    task_status: dict[str, dict[str, Any]] = {}
    for task in ("42.local", "42.wingtip"):
        seed_s, shard = task.split(".")
        active[task] = launch_task(seed=int(seed_s), shard=shard, role=roles[int(seed_s)][shard])
        task_status[task] = {"task": task, "service_role": roles[int(seed_s)][shard], "state": "started"}
    pending = set(active)
    while pending:
        finished = [task for task in pending if active[task].poll() is not None]
        if not finished:
            time.sleep(1)
            continue
        for task in finished:
            pending.remove(task)
            code = active[task].returncode
            task_status[task]["exit_code"] = code
            task_status[task]["state"] = "complete" if code == 0 else "failed"
            seed_s, shard = task.split(".")
            dependent = f"43.{shard}"
            if int(seed_s) == 42:
                if code == 0:
                    active[dependent] = launch_task(seed=43, shard=shard, role=roles[43][shard])
                    task_status[dependent] = {"task": dependent, "service_role": roles[43][shard], "state": "started"}
                    pending.add(dependent)
                else:
                    task_status[dependent] = {"task": dependent, "service_role": roles[43][shard],
                        "state": "skipped_dependency_failed", "dependency": task}
    return task_status



def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--prepare", action="store_true")
    modes.add_argument("--orchestrate", action="store_true")
    modes.add_argument("--check-services", action="store_true")
    modes.add_argument("--namespace-smoke", action="store_true")
    modes.add_argument("--execute-task", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--freeze-manifest", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--service-map", type=Path)
    parser.add_argument("--check-root", type=Path)
    parser.add_argument("--smoke-root", type=Path)
    parser.add_argument("--seed", type=int, choices=SEEDS)
    parser.add_argument("--shard", choices=("local", "wingtip"))
    parser.add_argument("--service-role")
    parser.add_argument("--transport-preflight-only", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.prepare:
            if args.freeze_manifest is None or args.run_root is None:
                parser.error("--prepare requires --freeze-manifest and --run-root")
            result = prepare(args.freeze_manifest, args.run_root)
        elif args.execute_task:
            if args.run_root is None or args.service_map is None or args.seed is None or args.shard is None or args.service_role is None:
                parser.error("task requires --run-root, --service-map, --seed, --shard, and --service-role")
            result = execute_task(args.run_root, args.service_map, seed=args.seed,
                shard=args.shard, service_role=args.service_role,
                dry_process=args.transport_preflight_only)
        elif args.check_services:
            if args.service_map is None or args.check_root is None:
                parser.error("--check-services requires --service-map and --check-root")
            result = check_services(args.service_map, args.check_root)
        elif args.namespace_smoke:
            if args.service_map is None or args.smoke_root is None:
                parser.error("--namespace-smoke requires --service-map and --smoke-root")
            result = namespace_smoke(args.service_map, args.smoke_root)
        else:
            if args.run_root is None or args.service_map is None:
                parser.error("--orchestrate requires --run-root and --service-map")
            result = orchestrate(args.run_root, args.service_map)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed_or_blocked", "error": f"{type(exc).__name__}: {str(exc)[:1000]}"}, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
