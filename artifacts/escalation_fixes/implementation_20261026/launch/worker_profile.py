"""Artifact-local profile-bound worker factory and request-seed verifier."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class WorkerProfileError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_sha(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _worker_command(case_id: str, case_root: Path, library_root: Path,
        view_root: Path, feedback_address: str, *, remote: bool,
        profile_relative: str, seed: int, profile_sha256: str) -> list[str]:
    from e4core.cache import implementation_fingerprint
    from scripts import run_m4_bank as supervisor

    parts = Path(profile_relative).parts
    if (not parts or parts[0] != "artifacts" or ".." in parts or
            not any(part.startswith("seed") for part in parts)):
        raise WorkerProfileError("selected worker profile must be a sealed project-relative seed copy")
    source_profile = (ROOT / profile_relative).resolve(strict=True)
    if _sha(source_profile) != profile_sha256:
        raise WorkerProfileError("selected worker profile hash differs from the seal")
    profile = json.loads(source_profile.read_text(encoding="utf-8"))
    if profile.get("seed") != seed:
        raise WorkerProfileError("selected worker profile seed differs from the declared run seed")
    worker_profile = "/m4project/" + Path(profile_relative).as_posix()
    profile_literal = json.dumps(worker_profile)
    source = (
        "import sys; from pathlib import Path; "
        "from scripts.run_m4_bank import BinaryFeedbackProxy; "
        "from e4core.m4_controller import run_diagnostic; "
        "proxy=BinaryFeedbackProxy(sys.argv[1]); "
        "run_diagnostic(sys.argv[2], library_root=Path('/m4library'), "
        "run_dir=Path('/m4run/diagnostic'), feedback=proxy, "
        f"profile_path=Path({profile_literal}), "
        "root=Path('/m4project'), split='scored'); proxy.close()"
    )
    command = ["/m4project/.venv/bin/python", "-c", source, feedback_address, case_id]
    built = supervisor._bwrap_command(view_root=view_root, run_dir=case_root,
        library_root=library_root, profile_relative=profile_relative,
        mode="diagnostic", case_id=case_id, split="scored",
        endpoint=os.environ.get("E4_BASE_URL", "http://127.0.0.1:8080"),
        feedback_socket=feedback_address,
        implementation_fingerprint_value=implementation_fingerprint(), command_override=command)
    if remote:
        separator = built.index("--")
        built[separator:separator] = ["--dir", "/run", "--dir", "/run/e4remote",
            "--ro-bind", "/run/e4remote-source", "/run/e4remote"]
    binding = {"schema_version": "e4-rerun-worker-profile-binding/1",
        "seed": seed, "case_id": case_id,
        "profile_path": profile_relative, "profile_sha256": profile_sha256,
        "profile_path_in_view": worker_profile,
        "worker_command_sha256": _json_sha(built),
        "remote_transport": remote}
    binding_path = case_root / "worker_profile_binding.json"
    fd = os.open(binding_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(binding, ensure_ascii=False, sort_keys=True,
            indent=2, allow_nan=False) + "\n")
    return built


@contextmanager
def bind_selected_profile(audit_module: Any, *, seed: int,
        profile_relative: str, profile_sha256: str) -> Iterator[None]:
    """Temporarily bind only the audit worker factory; always restore it."""
    if seed not in {42, 43, 44} or not re.fullmatch(r"[0-9a-f]{64}", profile_sha256):
        raise WorkerProfileError("invalid selected seed or profile digest")
    original = audit_module._worker_command

    def selected_worker(case_id, case_root, library_root, view_root, feedback_address, *, remote):
        return _worker_command(case_id, case_root, library_root, view_root,
            feedback_address, remote=remote, profile_relative=profile_relative,
            seed=seed, profile_sha256=profile_sha256)

    audit_module._worker_command = selected_worker
    try:
        yield
    finally:
        audit_module._worker_command = original


def verify_shard_request_seeds(state: Mapping[str, Any], shard_root: Path, *,
        shard: str, seed: int, profile_relative: str, profile_sha256: str,
        expected_cases: list[str], initial_snapshot_id: str) -> dict[str, Any]:
    if shard not in {"local", "wingtip"} or seed not in {42, 43, 44}:
        raise WorkerProfileError("invalid shard or inference seed")
    cases = state.get("completed_cases")
    if not isinstance(cases, list) or [row.get("case_id") for row in cases] != expected_cases:
        raise WorkerProfileError("completed shard does not match the sealed case order")
    if not cases or cases[0].get("input_library_snapshot_id") != initial_snapshot_id:
        raise WorkerProfileError("shard did not start from its fresh frozen starter library")
    proof_cases = []
    total_requests = 0
    for index, row in enumerate(cases, start=1):
        case_root = shard_root / f"case{index:02d}"
        binding_path = case_root / "worker_profile_binding.json"
        if not binding_path.is_file() or binding_path.is_symlink():
            raise WorkerProfileError(f"case{index:02d} lacks its actual worker profile binding record")
        binding = json.loads(binding_path.read_text(encoding="utf-8"))
        if (binding.get("seed") != seed or binding.get("case_id") != row.get("case_id") or
                binding.get("profile_path") != profile_relative or
                binding.get("profile_sha256") != profile_sha256 or
                binding.get("remote_transport") != (shard == "wingtip")):
            raise WorkerProfileError(f"case{index:02d} worker command profile differs from the sealed seed")
        result_path = ROOT / row["diagnostic_result_path"]
        call_path = result_path.parent / "model_calls.jsonl"
        if not call_path.is_file() or call_path.is_symlink():
            raise WorkerProfileError(f"case{index:02d} has no immutable actual request journal")
        requests = []
        for line in call_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            call = json.loads(line)
            payload = call.get("request_payload")
            if not isinstance(payload, Mapping) or payload.get("seed") != seed:
                raise WorkerProfileError(f"case{index:02d} outgoing request seed differs from {seed}")
            requests.append({"call_id": call.get("call_id"),
                "payload_sha256": _json_sha(payload), "request_seed": payload["seed"],
                "model": payload.get("model")})
        if not requests:
            raise WorkerProfileError(f"case{index:02d} produced no model request payloads")
        total_requests += len(requests)
        proof_cases.append({"case_index": index, "request_count": len(requests),
            "worker_profile_binding_sha256": _sha(binding_path), "requests": requests})
    return {"schema_version": "e4-frozen-rerun-request-seed-proof/1",
        "status": "all_outgoing_request_seeds_verified", "seed": seed,
        "shard": shard, "case_count": len(cases), "request_count": total_requests,
        "profile_path": profile_relative, "profile_sha256": profile_sha256,
        "initial_library_snapshot_id": initial_snapshot_id,
        "all_request_seeds_match": True, "cases": proof_cases}
