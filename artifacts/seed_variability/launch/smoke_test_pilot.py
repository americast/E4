#!/usr/bin/env python3
"""No-model E4 seed propagation and fresh-starter smoke test."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pilot

from e4core.m4_library import snapshot_library
from e4core.m4_controller import M4ModelClient
from scripts import run_m4_bank
from scripts import run_paper_starter_audit
from artifacts.seed_variability.launch import remote_driver
from artifacts.seed_variability.launch import pilot as packaged_pilot


ROOT = pilot.ROOT


class SeedVariabilityPilotSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        manifest_path = pilot.LAUNCH_ROOT / "pilot_manifest.json"
        if not manifest_path.is_file():
            raise AssertionError("run pilot.py prepare before the smoke test")
        cls.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    def test_seed_profile_delta_and_model_payload_use_requested_seed(self):
        base_path = ROOT / self.manifest["base_model_profile_path"]
        base = json.loads(base_path.read_text(encoding="utf-8"))
        self.assertEqual(base["seed"], 42)
        base_sha = hashlib.sha256(base_path.read_bytes()).hexdigest()
        self.assertEqual(base_sha, self.manifest["base_model_profile_sha256"])
        for seed in pilot.SEEDS:
            profile_row = self.manifest["seed_profiles"][str(seed)]
            profile_path = ROOT / profile_row["path"]
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
            self.assertEqual(profile["seed"], seed)
            self.assertEqual({k: v for k, v in profile.items() if k != "seed"},
                             {k: v for k, v in base.items() if k != "seed"})
            self.assertEqual(hashlib.sha256(profile_path.read_bytes()).hexdigest(),
                             profile_row["sha256"])
            client = M4ModelClient(profile)
            payload = client.payload_for_stage("compose", "no model call in this test")
            self.assertEqual(payload["seed"], seed)
            self.assertEqual(payload["messages"][1]["content"], "no model call in this test")

    def test_e4_helper_seals_bind_each_profile_and_current_helper_hashes(self):
        for seed in pilot.SEEDS:
            seal = pilot._verify_e4_seal(seed)
            self.assertEqual(seal["seed"], seed)
            self.assertEqual(seal["expected_request_seed"], seed)
            self.assertFalse(seal["model_requests_started"])

    def test_worker_bwrap_and_run_diagnostic_both_receive_seed_profile_path(self):
        seed = 43
        seed_manifest = json.loads((pilot.LAUNCH_ROOT / "seed43/seed_manifest.json").read_text())
        relative = seed_manifest["profile_path"]
        starter = ROOT / self.manifest["initial_library_path"]
        with tempfile.TemporaryDirectory(prefix="seedpilot_view_") as view_tmp, \
             tempfile.TemporaryDirectory(prefix="seedpilot_run_") as run_tmp:
            view_root, run_dir = Path(view_tmp), Path(run_tmp)
            with patch.object(run_m4_bank.shutil, "which", return_value="/usr/bin/bwrap"):
                command = pilot._worker_command_for_profile(
                    "bank-20210304T0100-utc8", run_dir, starter, view_root,
                    "/run/e4remote/feedback.sock", remote=False, profile_relative=relative)
        expected_view_path = "/m4project/" + relative
        self.assertIn("E4_MODEL_PROFILE", command)
        env_index = command.index("E4_MODEL_PROFILE")
        self.assertEqual(command[env_index + 1], expected_view_path)
        separator = command.index("--")
        worker_source = command[separator + 1:]
        source_text = " ".join(worker_source)
        self.assertIn(f"profile_path=Path({json.dumps(expected_view_path)})", source_text)
        self.assertIn("run_diagnostic", source_text)

    def test_restricted_namespace_reads_seed43_and44_without_model_calls(self):
        for seed in pilot.SEEDS:
            proof = pilot.namespace_profile_smoke(seed)
            self.assertEqual(proof["seed"], seed)
            self.assertEqual(proof["profile_seed"], seed)
            self.assertEqual(proof["payload_seed"], seed)
            self.assertEqual(proof["completion_requests_made"], 0)

    def test_each_seed_audit_starts_from_same_original_run06_snapshot(self):
        starter = snapshot_library(ROOT / self.manifest["initial_library_path"])
        self.assertEqual(starter.snapshot_id, pilot.EXPECTED_STARTER_ID)
        starter_path = Path(starter.path)
        self.assertEqual(hashlib.sha256(starter_path.read_bytes()).hexdigest(),
                         self.manifest["initial_library_snapshot_sha256"])
        for seed in pilot.SEEDS:
            seed_manifest = json.loads((pilot.LAUNCH_ROOT / f"seed{seed}/seed_manifest.json").read_text())
            audit_path = ROOT / seed_manifest["audit_dir"] / "audit_manifest.json"
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            self.assertEqual(audit["initial_library_snapshot_id"], pilot.EXPECTED_STARTER_ID)
            self.assertEqual(audit["case_ids"], list(pilot.CASE_ORDER))
            self.assertEqual(audit["shard_groups"], {
                "local": list(pilot.CASE_ORDER[:5]), "wingtip": list(pilot.CASE_ORDER[5:])})
            self.assertEqual(audit["model_profile_sha256"], seed_manifest["profile_sha256"])

    def test_remote_receipt_separates_server_profile_from_seeded_client_profile(self):
        seed = 43
        seed_manifest = json.loads((pilot.LAUNCH_ROOT / "seed43/seed_manifest.json").read_text())
        freeze_path = ROOT / seed_manifest["derived_freeze_path"]
        freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
        profile_path = ROOT / freeze["model_profile_path"]
        base_profile_path = ROOT / "config/gemma_m3_inference.json"
        provenance_path = ROOT / "artifacts/m3/model_runs/paperstarter_bank10_20261024_06/wingtip_server_provenance.json"
        with tempfile.TemporaryDirectory(prefix="seedpilot_remote_receipt_", dir=pilot.LAUNCH_ROOT / "seed43") as temp:
            run_root = Path(temp)
            attestation = {"remote_checkpoint_sha256": freeze["model_checkpoint_sha256"],
                "remote_server_command": ["llama-server"], "server_pid": 123,
                "process_group_id": 456, "server_ready_utc": "2026-10-24T00:00:00Z",
                "health_check": {"http_status": 200, "body": {"status": "ok"}}}
            def fake_preflight(freeze_file, output_dir, *, backend):
                self.assertEqual(backend, "wingtip")
                output_dir.mkdir(parents=True, exist_ok=False)
                report = output_dir / "tokenizer_preflight.json"
                report.write_text(json.dumps({"profile_sha256": freeze["model_profile_sha256"]}))
                archive = output_dir / "prompt_tokenizer_archive.json"
                archive.write_text("{}\n")
                return {"prompt_archive_path": str(archive.relative_to(ROOT)),
                    "prompt_archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
            with patch.object(remote_driver, "_validate_server_provenance", return_value=attestation), \
                 patch("artifacts.e4_improvement.remote_wingtip.remote_inference.local_health_probe",
                       return_value={"http_status": 200}), \
                 patch.object(run_paper_starter_audit, "preflight", side_effect=fake_preflight):
                path, receipt = remote_driver._write_remote_receipt(run_root=run_root,
                    audit_dir=ROOT / seed_manifest["audit_dir"], freeze_path=freeze_path,
                    provenance_path=provenance_path, remote_port=8080)
            self.assertTrue(path.is_file())
            self.assertEqual(receipt["profile_sha256"], hashlib.sha256(profile_path.read_bytes()).hexdigest())
            self.assertEqual(receipt["client_profile_sha256"], receipt["profile_sha256"])
            self.assertEqual(receipt["client_profile_seed"], seed)
            self.assertEqual(receipt["server_base_profile_sha256"],
                             hashlib.sha256(base_profile_path.read_bytes()).hexdigest())
            self.assertEqual(receipt["server_base_profile_seed"], 42)

    def test_remote_inner_uses_seed_worker_wrapper(self):
        seed = 43
        seed_manifest = json.loads((pilot.LAUNCH_ROOT / "seed43/seed_manifest.json").read_text())
        original_audit = ROOT / seed_manifest["audit_dir"]
        with tempfile.TemporaryDirectory(prefix="seedpilot_remote_inner_", dir=pilot.LAUNCH_ROOT / "seed43") as temp:
            run_root = Path(temp)
            audit_dir = run_root / "audit"
            audit_dir.mkdir()
            (audit_dir / "audit_manifest.json").write_bytes(
                (original_audit / "audit_manifest.json").read_bytes())
            local_receipt = run_root / "local_receipt.json"
            local_receipt.write_text("{}\n")
            (run_root / "launch_handoff.json").write_text(json.dumps({
                "status": "complete", "audit_dir": str(audit_dir.relative_to(ROOT)),
                "local_server_receipt_path": str(local_receipt.relative_to(ROOT)),
                "local_server_receipt_sha256": hashlib.sha256(local_receipt.read_bytes()).hexdigest()}))
            remote_receipt = run_root / "remote_receipt.json"
            remote_receipt.write_text("{}\n")
            (audit_dir / "bank10_wingtip").mkdir(exist_ok=True)
            (audit_dir / "bank10_wingtip/bank10_status.json").write_text("{}\n")
            receipt = {"remote_server_pid": 10, "forwarded_health_probe": {"http_status": 200}}
            calls = []
            def fake_seed_run(seed_value, shard, audit_value, receipt_value, *, proof_path=None):
                calls.append((seed_value, shard, audit_value, receipt_value, proof_path))
                return {"status": "complete", "shard": "wingtip"}
            with patch.object(remote_driver, "_write_remote_receipt",
                    return_value=(remote_receipt, receipt)), \
                 patch.object(packaged_pilot, "run_shard", side_effect=fake_seed_run):
                handoff = remote_driver._inner(run_root=run_root, audit_dir=audit_dir,
                    provenance_path=ROOT / "artifacts/m3/model_runs/paperstarter_bank10_20261024_06/wingtip_server_provenance.json",
                    remote_port=8080)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][0:2], (seed, "wingtip"))
            self.assertEqual(calls[0][2], audit_dir)
            self.assertEqual(calls[0][3], remote_receipt)
            self.assertEqual(calls[0][4], run_root / "wingtip_request_seed_proof.json")
            self.assertEqual(handoff["status"], "complete")


if __name__ == "__main__":
    unittest.main()
