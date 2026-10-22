"""M5-specific scored guard and trusted feedback ledger for the E4 controller."""

from __future__ import annotations

import json
import hashlib
import socket
import struct
from pathlib import Path
from typing import Any, Sequence

from .m5_evaluation import (EvaluationSession, load_development_truth_ids,
                            load_scored_truth_ids)
from .m5_manifest import ROOT, validate_frozen_manifest
from .m5_tasks import load_bank_case_view


def validate_scored_run(manifest_path: Path, *, root: Path = ROOT, case_id: str,
                        library_root: Path, profile_path: Path) -> dict[str, Any]:
    """Check frozen source, profile, public-input and pristine-library pins before scored setup."""
    # The scored manifest pins the pristine initial snapshot. Later cases may
    # use descendants that contain only sequential usage/admission updates.
    root = Path(root).resolve(strict=True)
    raw_manifest_path = Path(manifest_path)
    if raw_manifest_path.is_symlink():
        raise ValueError("scored M5 manifest cannot be a symlink")
    manifest_path = raw_manifest_path.resolve(strict=True)
    manifest_root = (root / "artifacts/m5/experiments").resolve()
    if not manifest_path.is_relative_to(manifest_root):
        raise ValueError("scored M5 manifest must be under artifacts/m5/experiments")
    manifest_doc = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    raw_initial_path = Path(manifest_doc["local_pins"]["initial_library_path"])
    if raw_initial_path.is_symlink():
        raise ValueError("frozen initial E4 library cannot be a symlink")
    initial_path = raw_initial_path.resolve(strict=True)
    if (not initial_path.is_dir() or
            not initial_path.is_relative_to(root / "artifacts")):
        raise ValueError("frozen initial E4 library must be a real snapshot directory under project artifacts")
    manifest = validate_frozen_manifest(manifest_path, root=root, case_id=case_id,
                                        split="scored", initial_library_path=initial_path)
    from .m4_library import load_library_snapshot, snapshot_library
    current = snapshot_library(library_root)
    initial_id = manifest["local_pins"]["initial_library_snapshot_id"]
    expected_provenance = {"implementation_fingerprint":
        manifest["local_pins"]["e4_implementation_fingerprint"],
        "m4_runtime_policy_sha256": manifest["local_pins"]["runtime_policy_sha256"]}
    seen = set()
    while True:
        provenance = current.document.get("provenance", {})
        if any(provenance.get(key) != value for key, value in expected_provenance.items()):
            raise ValueError("evolving E4 library contains stale source or policy provenance")
        if current.snapshot_id == initial_id:
            break
        if current.snapshot_id in seen or current.parent_snapshot_id is None:
            raise ValueError("evolving E4 library is not descended from the frozen initial snapshot")
        seen.add(current.snapshot_id)
        current = load_library_snapshot(current.root / f"{current.parent_snapshot_id}.json")
    if profile_path.resolve() != (root / "config/gemma_m3_inference.json").resolve():
        raise ValueError("scored M5 E4 profile must be the frozen Gemma profile")
    return manifest


def validate_staged_task_view(task_view_root: Path, *, root: Path = ROOT,
                              case_id: str, split: str) -> str:
    """Verify the orchestrator-staged public telemetry matches the frozen case view."""
    view = load_bank_case_view(case_id, split=split, root=root)
    raw_staged = Path(task_view_root)
    if raw_staged.is_symlink():
        raise ValueError("staged M5 public task view cannot be a symlink")
    staged = raw_staged.resolve(strict=True)
    if not staged.is_dir():
        raise ValueError("staged M5 public task view must be a directory")
    expected = dict(view.telemetry_file_sha256)
    input_manifest = json.loads((Path(root) / "config/bank_input_manifest.json").read_text(encoding="utf-8"))
    case_pin = input_manifest.get("cases", {}).get(case_id, {})
    alert_sha256 = case_pin.get("alert_sha256")
    if not isinstance(alert_sha256, str):
        raise ValueError("selected Bank alert has no frozen public hash")
    expected["alert.json"] = alert_sha256
    observed: dict[str, str] = {}
    for path in staged.rglob("*"):
        if path.is_symlink():
            raise ValueError("staged M5 task view cannot contain symlinks")
        if path.is_file():
            relative = path.relative_to(staged).as_posix()
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            observed[relative] = digest.hexdigest()
    if observed != expected:
        raise ValueError("staged M5 public task view differs from the selected pinned Bank clips")
    return hashlib.sha256(json.dumps(observed, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class M5TrustedFeedback:
    """Host-side evaluator; only its one accepted boolean crosses the IPC boundary."""

    def __init__(self, *, root: Path, case_id: str, split: str,
                 experiment_manifest: Path | None = None,
                 trusted_evaluator_socket: Path | None = None):
        self.root = Path(root).resolve(strict=True)
        self.experiment_manifest = experiment_manifest
        self.trusted_evaluator_socket = (Path(trusted_evaluator_socket).resolve()
                                         if trusted_evaluator_socket is not None else None)
        self.case_view = load_bank_case_view(case_id, split=split, root=self.root)
        self.session = None
        self.remote_submissions: list[dict[str, Any]] = []
        if self.trusted_evaluator_socket is None:
            if split == "scored" and self.experiment_manifest is None:
                raise ValueError("scored M5 evaluator requires the frozen experiment manifest")
            truth = (load_scored_truth_ids(root=self.root, case_id=case_id,
                experiment_manifest=self.experiment_manifest) if split == "scored" else
                load_development_truth_ids(root=self.root, case_id=case_id))
            self.session = EvaluationSession(case_id=case_id,
                allowed_component_ids=self.case_view.component_ids, truth_ids=truth)

    def evaluate_ids(self, case_id: str, components: Sequence[str]) -> bool:
        if self.trusted_evaluator_socket is not None:
            result = self._submit_remote(case_id, {"ranked_components": list(components)})
            self.remote_submissions.append(result)
            return result["accepted"]
        if self.session is None or case_id != self.session.case_id:
            raise ValueError("M5 evaluator case identity mismatch")
        result = self.session.submit({"ranked_components": list(components)})
        return result["accepted"]

    def _submit_remote(self, case_id: str, raw_answer: Any) -> dict[str, bool]:
        payload = json.dumps({"case_id": case_id, "raw_answer": raw_answer},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(payload) > 16 * 1024:
            raise ValueError("trusted evaluator request exceeds its fixed frame limit")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.connect(str(self.trusted_evaluator_socket))
            stream.sendall(struct.pack(">I", len(payload)) + payload)
            header = _recv_exact(stream, 4)
            length = struct.unpack(">I", header)[0]
            if not 0 < length <= 16 * 1024:
                raise ValueError("trusted evaluator returned an invalid frame length")
            response = json.loads(_recv_exact(stream, length).decode("utf-8"))
        if type(response) is not dict or set(response) != {"accepted"} or type(response["accepted"]) is not bool:
            raise ValueError("trusted evaluator must return only one accepted boolean")
        return response

    @property
    def case_id(self) -> str:
        return self.session.case_id if self.session is not None else self.case_view.case_id

    def evaluate(self, case_id: str, ranked_answer: Any) -> dict[str, bool]:
        from .contracts import RankedAnswer
        if not isinstance(ranked_answer, RankedAnswer):
            raise ValueError("M5 feedback requires a RankedAnswer")
        components = [lead.entity.identifier for lead in ranked_answer.leads[:3]]
        return {"accepted": self.evaluate_ids(case_id, components)}

    def write_attempt_slots(self, run_dir: Path, *, case_timeout: bool = False) -> list[dict[str, Any]]:
        """Preserve the controller's three stage positions, including failed and unrun slots."""
        run_dir = Path(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        result_path = run_dir / "diagnostic_result.json"
        result_doc = json.loads(result_path.read_text(encoding="utf-8")) if result_path.is_file() else {}
        stages = result_doc.get("stages", [])
        stage_names = ("stage1_select", "stage2_compose", "stage3_synthesize")
        if not stages:
            stages = []
            for stage_name in stage_names:
                record_path = run_dir / "attempts" / stage_name / "stage_record.json"
                if record_path.is_file():
                    stages.append(json.loads(record_path.read_text(encoding="utf-8")))
                else:
                    stages.append(None)
        submitted_rows = (self.session.trusted_ledger() if self.session is not None else
            [{"accepted": row["accepted"], "valid": None, "correct_rank": None}
             for row in self.remote_submissions])
        submitted_index = 0
        slots = []
        for index in range(3):
            stage = stages[index] if index < len(stages) else None
            stage_started = (run_dir / "attempts" / stage_names[index]).is_dir()
            has_feedback = submitted_index < len(submitted_rows)
            if stage is not None and stage.get("status") in {"accepted", "rejected"}:
                if not has_feedback:
                    raise ValueError("controller feedback stage has no matching trusted evaluator record")
                row = submitted_rows[submitted_index]
                submitted_index += 1
                slot = {"attempt": index + 1, "status": "submitted",
                        "accepted": bool(row["accepted"]), "valid": row.get("valid"),
                        "correct_rank": row.get("correct_rank")}
            elif stage is None and case_timeout and stage_started and has_feedback:
                # A hard outer wall stop can land after trusted feedback records the
                # boolean but before the controller persists this stage record.
                row = submitted_rows[submitted_index]
                submitted_index += 1
                slot = {"attempt": index + 1, "status": "submitted",
                        "accepted": bool(row["accepted"]), "valid": row.get("valid"),
                        "correct_rank": row.get("correct_rank")}
            elif stage is None and case_timeout and stage_started:
                slot = {"attempt": index + 1, "status": "failed", "accepted": False,
                        "valid": False, "correct_rank": None,
                        "validation_error": "case_wall_timeout"}
            elif stage is None:
                slot = {"attempt": index + 1, "status": "not_run", "accepted": False,
                        "valid": None, "correct_rank": None}
            else:
                slot = {"attempt": index + 1, "status": "failed", "accepted": False,
                        "valid": False, "correct_rank": None,
                        "validation_error": stage.get("status")}
            slots.append(slot)
        if submitted_index != len(submitted_rows):
            raise ValueError("trusted evaluator records exceed controller diagnosis stages")
        (run_dir / "m5_attempt_slots.json").write_text(
            json.dumps(slots, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return slots


def _recv_exact(stream: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = stream.recv(length - len(chunks))
        if not chunk:
            raise EOFError("trusted evaluator closed an incomplete frame")
        chunks.extend(chunk)
    return bytes(chunks)

