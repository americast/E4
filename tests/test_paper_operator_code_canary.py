from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import run_paper_operator_code_canary as canary
from scripts import run_paper_starter_audit as paper_audit


class PaperOperatorCodeCanaryTests(unittest.TestCase):
    def _inputs(self, root: Path, *, profile: str = "typed-operator-code-v1"):
        freeze_path = root / "freeze.json"
        freeze_path.write_text("{}\n", encoding="utf-8")
        manifest_path = root / "audit_manifest.json"
        manifest_path.write_text("{}\n", encoding="utf-8")
        freeze = {"run_id": "synthetic-freeze", "scored_case_ids": [
                "case-first", *(f"case-{index:02d}" for index in range(2, 11))],
            "initial_library_path": "artifacts/library", "initial_library_snapshot_id": "input-snapshot",
            "e4_implementation_fingerprint": "impl"}
        manifest = {"initial_library_path": "artifacts/library",
            "initial_library_snapshot_id": "input-snapshot", "source_fingerprint": "impl",
            "model_profile_path": "config/gemma_m3_inference.json"}
        return freeze_path, manifest_path, freeze, manifest, profile

    def test_main_success_runs_only_first_frozen_case_and_writes_hash_receipt(self):
        with tempfile.TemporaryDirectory(dir=canary.ROOT) as temporary:
            root = Path(temporary)
            freeze_path, manifest_path, freeze, manifest, profile = self._inputs(root)
            output_dir = root / "canary"
            fake_snapshot = object()
            captured = {}
            def fake_run_case(case_id, index, input_library, shard_root, run_manifest, truth, shard):
                captured.update({"case_id": case_id, "index": index, "truth": truth,
                    "shard": shard, "input_library": input_library})
                return {"case_id": case_id, "status": "rejected", "accepted": False,
                    "stage_slots": [{"stage": "stage1_select", "status": "submitted",
                        "accepted": False}], "model_calls": 4, "prompt_tokens": 500,
                    "completion_tokens": 200, "charged_generated_tokens": 200,
                    "model_seconds": 1.0, "wall_seconds": 2.0,
                    "input_library_snapshot_id": "input-snapshot",
                    "output_library_snapshot_id": "output-snapshot",
                    "library_path": "artifacts/output-library",
                    "diagnostic_result_path": "artifacts/result.json",
                    "diagnostic_result_sha256": "a" * 64,
                    "ledger_path": "artifacts/ledger.json", "ledger_sha256": "b" * 64}

            output = io.StringIO()
            with patch.object(paper_audit, "_load_freeze", return_value=(freeze, freeze_path)), \
                    patch.object(canary, "snapshot_library", return_value=fake_snapshot), \
                    patch.object(canary, "stage3_authoring_profile", return_value=profile), \
                    patch.object(paper_audit, "prepare", return_value=root / "audit"), \
                    patch.object(paper_audit, "_verify_audit", return_value=(manifest, manifest_path)), \
                    patch.object(canary.supervisor, "_validate_local_endpoint"), \
                    patch.object(paper_audit, "_validate_server_receipt", return_value={
                        "backend": "local", "endpoint": "http://127.0.0.1:19405",
                        "checkpoint_sha256": "c" * 64}), \
                    patch.object(paper_audit, "_truth_rows", return_value={
                        "case-first": ("host-only-target",), "case-second": ("other-target",)}), \
                    patch.object(paper_audit, "_run_case", side_effect=fake_run_case), \
                    contextlib.redirect_stdout(output):
                code = canary.main(["--freeze", str(freeze_path), "--output-dir", str(output_dir),
                    "--endpoint", "http://127.0.0.1:19405", "--server-receipt", str(root / "receipt.json")])
            self.assertEqual(code, 0)
            self.assertEqual(captured["case_id"], "case-first")
            self.assertEqual(captured["index"], 1)
            self.assertEqual(captured["truth"], {"case-first": ("host-only-target",)})
            self.assertEqual(captured["shard"], "local")
            result = json.loads((output_dir / "canary_result.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "complete")
            self.assertIn("not an accuracy estimate", result["claim"])
            self.assertNotIn("host-only-target", json.dumps(result))
            digest = (output_dir / "canary_result.sha256").read_text(encoding="ascii").strip()
            self.assertEqual(digest, paper_audit._sha(output_dir / "canary_result.json"))
            self.assertEqual(json.loads(output.getvalue())["case_id"], "case-first")

    def test_profile_and_receipt_gates_precede_worker_launch(self):
        with tempfile.TemporaryDirectory(dir=canary.ROOT) as temporary:
            root = Path(temporary)
            freeze_path, manifest_path, freeze, manifest, _profile = self._inputs(root,
                profile="typed-operator-ir-v2")
            with patch.object(paper_audit, "_load_freeze", return_value=(freeze, freeze_path)), \
                    patch.object(canary, "snapshot_library", return_value=object()), \
                    patch.object(canary, "stage3_authoring_profile", return_value="typed-operator-ir-v2"), \
                    patch.object(paper_audit, "prepare") as prepare, \
                    patch.object(paper_audit, "_run_case") as run_case:
                with self.assertRaises(SystemExit):
                    canary.main(["--freeze", str(freeze_path), "--output-dir", str(root / "profile-fail"),
                        "--endpoint", "http://127.0.0.1:19405", "--server-receipt", str(root / "receipt.json")])
                prepare.assert_not_called()
                run_case.assert_not_called()

            freeze_path, manifest_path, freeze, manifest, _profile = self._inputs(root)
            with patch.object(paper_audit, "_load_freeze", return_value=(freeze, freeze_path)), \
                    patch.object(canary, "snapshot_library", return_value=object()), \
                    patch.object(canary, "stage3_authoring_profile", return_value="typed-operator-code-v1"), \
                    patch.object(paper_audit, "prepare", return_value=root / "audit"), \
                    patch.object(paper_audit, "_verify_audit", return_value=(manifest, manifest_path)), \
                    patch.object(canary.supervisor, "_validate_local_endpoint"), \
                    patch.object(paper_audit, "_validate_server_receipt", side_effect=paper_audit.AuditError("receipt mismatch")), \
                    patch.object(paper_audit, "_run_case") as run_case:
                with self.assertRaises(SystemExit):
                    canary.main(["--freeze", str(freeze_path), "--output-dir", str(root / "receipt-fail"),
                        "--endpoint", "http://127.0.0.1:19405", "--server-receipt", str(root / "receipt.json")])
                run_case.assert_not_called()

    def test_worker_failure_keeps_a_hashed_failure_record(self):
        with tempfile.TemporaryDirectory(dir=canary.ROOT) as temporary:
            root = Path(temporary)
            freeze_path, manifest_path, freeze, manifest, profile = self._inputs(root)
            output_dir = root / "worker-fail"
            with patch.object(paper_audit, "_load_freeze", return_value=(freeze, freeze_path)), \
                    patch.object(canary, "snapshot_library", return_value=object()), \
                    patch.object(canary, "stage3_authoring_profile", return_value=profile), \
                    patch.object(paper_audit, "prepare", return_value=root / "audit"), \
                    patch.object(paper_audit, "_verify_audit", return_value=(manifest, manifest_path)), \
                    patch.object(canary.supervisor, "_validate_local_endpoint"), \
                    patch.object(paper_audit, "_validate_server_receipt", return_value={"backend": "local"}), \
                    patch.object(paper_audit, "_truth_rows", return_value={"case-first": ("target",)}), \
                    patch.object(paper_audit, "_run_case", side_effect=RuntimeError("worker failure")):
                with self.assertRaises(SystemExit):
                    canary.main(["--freeze", str(freeze_path), "--output-dir", str(output_dir),
                        "--endpoint", "http://127.0.0.1:19405", "--server-receipt", str(root / "receipt.json")])
            result_path = output_dir / "canary_result.json"
            record = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["case_id"], "case-first")
            self.assertEqual(record["error"]["message"], "worker failure")
            self.assertTrue((output_dir / "single_case").is_dir())
            self.assertEqual((output_dir / "canary_result.sha256").read_text(encoding="ascii").strip(),
                paper_audit._sha(result_path))


if __name__ == "__main__":
    unittest.main()
