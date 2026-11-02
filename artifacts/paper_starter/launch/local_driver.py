#!/usr/bin/env python3
"""Local V100 child for the paper starter audit, run by scripts/run_m3_model.py."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_paper_starter_audit import prepare, preflight, run_shard


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"preserving existing artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + ".pending")
    pending.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(pending, path)


def _health(endpoint: str) -> dict[str, Any]:
    with urllib.request.urlopen(endpoint.rstrip("/") + "/health", timeout=5) as response:
        body = response.read(4096)
        if response.status != 200:
            raise RuntimeError(f"model health returned HTTP {response.status}")
        return {"endpoint": "/health", "http_status": response.status,
                "response_bytes": len(body), "checked_utc": datetime.now(timezone.utc).isoformat()}


def run(freeze_manifest: Path) -> dict[str, Any]:
    run_dir_value = os.environ.get("E4_RUN_DIR")
    endpoint = os.environ.get("E4_BASE_URL")
    if not run_dir_value or not endpoint:
        raise RuntimeError("local driver must run inside scripts/run_m3_model.py")
    run_dir = Path(run_dir_value).resolve(strict=True)
    launch_path = run_dir / "launch.json"
    if launch_path.is_symlink() or not launch_path.is_file():
        raise RuntimeError("approved local launcher did not write launch.json")
    launch = json.loads(launch_path.read_text(encoding="utf-8"))
    freeze_path = freeze_manifest.resolve(strict=True)
    frozen = json.loads(freeze_path.read_text(encoding="utf-8"))
    profile_path = ROOT / frozen["model_profile_path"]
    model_manifest_path = ROOT / frozen["model_manifest_path"]
    model_manifest = json.loads(model_manifest_path.read_text(encoding="utf-8"))
    if (launch.get("server_ready_utc") is None or launch.get("base_url") != endpoint or
            launch.get("profile_sha256") != frozen["model_profile_sha256"] or
            launch.get("manifest_sha256") != frozen["model_manifest_sha256"] or
            launch.get("model_sha256") != frozen["model_checkpoint_sha256"] or
            launch.get("model_revision") != frozen["model_revision"] or
            launch.get("context_tokens") != 32768 or
            launch.get("max_generation_tokens") != 8192 or
            model_manifest.get("sha256") != launch.get("model_sha256")):
        raise RuntimeError("live local server launch receipt differs from the frozen Gemma profile/checkpoint")
    server_cmd = launch.get("server_command")
    if not isinstance(server_cmd, list) or not all(isinstance(arg, str) for arg in server_cmd):
        raise RuntimeError("launch receipt lacks the exact llama-server argument list")
    server_path = Path(launch.get("llama_server", "")).resolve(strict=True)
    if (not server_path.is_file() or _sha(server_path) != launch.get("llama_server_sha256") or
            server_cmd[0] != str(server_path)):
        raise RuntimeError("live llama-server binary differs from the approved launch receipt")
    required = {"--host": "127.0.0.1", "--port": str(url_port(endpoint)),
        "--alias": "e4-gemma-m3", "--ctx-size": "32768", "--n-gpu-layers": "99",
        "--parallel": "1", "--offline": None, "--reasoning": "on",
        "--reasoning-budget": "2048"}
    for flag, value in required.items():
        if flag not in server_cmd or (value is not None and
                server_cmd[server_cmd.index(flag) + 1] != value):
            raise RuntimeError(f"live local server command differs at {flag}")
    if ("--model" not in server_cmd or
            Path(server_cmd[server_cmd.index("--model") + 1]).resolve(strict=True) !=
            Path(launch.get("model_path", "")).resolve(strict=True) or
            Path(launch["model_path"]).name != model_manifest.get("filename")):
        raise RuntimeError("live local server command does not use the checkpoint recorded in launch.json")
    if _health(endpoint)["http_status"] != 200:
        raise RuntimeError("live local model health check failed")
    _atomic_json(run_dir / "local_health_probe.json", _health(endpoint))

    token = preflight(freeze_path, run_dir / "local_preflight", backend="local")
    tokenizer_path = run_dir / "local_preflight" / "tokenizer_preflight.json"
    receipt = {"schema_version": "e4-paper-starter-server-receipt/1",
        "backend": "local", "endpoint": endpoint,
        "model_manifest_sha256": _sha(model_manifest_path),
        "checkpoint_sha256": launch["model_sha256"], "model_revision": launch["model_revision"],
        "profile_sha256": launch["profile_sha256"],
        "weights_loaded": bool(launch.get("server_ready_utc") and launch.get("server_load_seconds") is not None),
        "server_command_sha256": hashlib.sha256(json.dumps(server_cmd, separators=(",", ":")).encode()).hexdigest(),
        "health_probe_path": "local_health_probe.json",
        "health_probe_sha256": _sha(run_dir / "local_health_probe.json"),
        "provenance_path": str(launch_path.relative_to(ROOT)), "provenance_sha256": _sha(launch_path),
        "tokenizer_preflight_path": str(tokenizer_path.relative_to(ROOT)),
        "tokenizer_preflight_sha256": _sha(tokenizer_path),
        "tokenizer_prompt_archive_path": token["prompt_archive_path"],
        "tokenizer_prompt_archive_sha256": token["prompt_archive_sha256"]}
    if receipt["weights_loaded"] is not True:
        raise RuntimeError("local launch receipt does not prove model load completed")
    receipt_path = run_dir / "local_server_receipt.json"
    _atomic_json(receipt_path, receipt)
    audit_dir = prepare(freeze_path, audit_root=run_dir / "audits")
    handoff = {"schema_version": "e4-paper-starter-local-handoff/1",
        "status": "local_shard_starting", "audit_dir": str(audit_dir.relative_to(ROOT)),
        "run_root": str(run_dir.relative_to(ROOT)),
        "freeze_manifest_sha256": _sha(freeze_path),
        "local_server_receipt_path": str(receipt_path.relative_to(ROOT)),
        "local_server_receipt_sha256": _sha(receipt_path),
        "created_utc": datetime.now(timezone.utc).isoformat()}
    handoff_path = run_dir / "launch_handoff.json"
    _atomic_json(handoff_path, handoff)
    status = run_shard(audit_dir, "local", server_receipt=receipt_path)
    handoff.update({"status": status["status"], "local_status_path": str(
        (audit_dir / "bank10_local" / "bank10_status.json").relative_to(ROOT)),
        "local_status_sha256": _sha(audit_dir / "bank10_local" / "bank10_status.json"),
        "finished_utc": datetime.now(timezone.utc).isoformat()})
    completion_path = run_dir / "local_shard_handoff.json"
    _atomic_json(completion_path, handoff)
    return handoff


def url_port(endpoint: str) -> int:
    from urllib.parse import urlsplit
    parsed = urlsplit(endpoint)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port is None:
        raise RuntimeError("local launcher endpoint must be plain HTTP on 127.0.0.1")
    return parsed.port


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze-manifest", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.freeze_manifest), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
