import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e4core.m5_evaluation import EvaluationSession
from scripts import run_paper_starter_audit as audit


class PaperStarterAuditTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("E4_RUN_SANDBOX_STRESS") == "1",
        "run in the approved restricted-worker namespace with E4_RUN_SANDBOX_STRESS=1")
    def test_restricted_paper_view_contains_v2_policy_and_runs_host_capability_provider(self):
        from scripts import run_m4_bank as supervisor
        from e4core.cache import implementation_fingerprint

        with tempfile.TemporaryDirectory(prefix="paper-v2-view-") as temporary:
            temp = Path(temporary)
            view_root = temp / "project"
            run_dir = temp / "run"
            run_dir.mkdir()
            profile = audit.ROOT / "config/gemma_m3_inference.json"
            supervisor.create_restricted_project_view(audit.ROOT, view_root,
                profile_path=profile, case_id=None, split="scored")
            audit._install_paper_audit_config(view_root)
            self.assertTrue((view_root / "config/typed_operator_ir_v2_policy.json").is_file())
            worker = (
                "import json,os; from e4core.m4_controller import _implementation_fingerprint; "
                "from e4core.contracts import PaperStarterInput,Telemetry,Window; "
                "from e4core.m4_operator_ir import typed_operator_ir_v2_schema,operator_ir_capability_report; "
                "from e4core.registry import OperatorRegistry; "
                "ref=Window('reference',0,1000); alt=Window('alert',1000,2000); "
                "telemetry=Telemetry('restricted-smoke',ref,alt,(),(),{},'a'*64,{}, {}, {},'view-smoke/1'); "
                "starter=PaperStarterInput(telemetry,None,None,{'sessions':'unsupported','urls':'unsupported',"
                "'native_logs':'unsupported','native_log_patterns':'unsupported'},60000); "
                "schema=typed_operator_ir_v2_schema(); report=operator_ir_capability_report(starter,"
                "OperatorRegistry(),runtime_values={'minimum_support':12,'minimum_independent_blocks':6}); "
                "fp=_implementation_fingerprint(); result={'fingerprint':fp,"
                "'expected_fingerprint':os.environ['E4_M4_IMPLEMENTATION_FINGERPRINT'],"
                "'has_selector_kernel':'series.select_metric_rule@1' in schema['kernels'],"
                "'selector_count':len(report['metric_selector_catalog'])}; "
                "print(json.dumps(result)); "
                "assert fp==result['expected_fingerprint'] and result['has_selector_kernel'] "
                "and result['selector_count']>0, result"
            )
            command = supervisor._bwrap_command(view_root=view_root, run_dir=run_dir,
                library_root=None, profile_relative="config/gemma_m3_inference.json",
                mode="bootstrap", case_id=None, split="scored",
                endpoint="http://127.0.0.1:8080", feedback_socket=None,
                implementation_fingerprint_value=implementation_fingerprint(),
                command_override=["/m4project/.venv/bin/python", "-c", worker])
            result = subprocess.run(command, cwd=audit.ROOT, env={}, capture_output=True,
                text=True, timeout=180, check=False)
            self.assertEqual(result.returncode, 0, result.stderr[-2000:])
            output = json.loads(result.stdout.strip().splitlines()[-1])
            self.assertEqual(output["fingerprint"], implementation_fingerprint())
            self.assertGreater(output["selector_count"], 0)

    def test_prepare_emits_all_fields_required_by_real_server_receipt_validation(self):
        case_ids = [f"bank-20210304T{hour:02d}00-utc8" for hour in range(10)]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "config").mkdir()
            (root / "artifacts").mkdir()
            profile_path = root / "config/gemma.json"
            model_manifest_path = root / "config/model.json"
            profile_path.write_text("{}\n")
            model_manifest_path.write_text(json.dumps({"sha256": "checkpoint", "revision": "revision"}))
            sample_path = root / "config/bank_sample.json"
            sample_path.write_text("{}\n")
            baseline_path = root / "artifacts/baseline.json"
            baseline_path.write_text("{}\n")
            freeze_path = root / "artifacts/freeze.json"
            freeze_path.write_text("{}\n")
            freeze = {"run_id": "freeze-test", "initial_library_path": "artifacts/bank_library",
                "initial_library_snapshot_id": "library", "canonical_origin": {"snapshot_id": "origin"},
                "e4_implementation_fingerprint": "source", "repository_source_inventory_sha256": "inventory",
                "model_profile_path": "config/gemma.json", "model_profile_sha256": audit._sha(profile_path),
                "model_manifest_path": "config/model.json", "model_manifest_sha256": audit._sha(model_manifest_path),
                "model_checkpoint_sha256": "checkpoint", "model_revision": "revision",
                "baseline_manifest_path": "artifacts/baseline.json",
                "baseline_manifest_sha256": audit._sha(baseline_path),
                "trusted_truth_pin": {"path": "data/truth.jsonl", "sha256": "truth"},
                "bank_sample_sha256": audit._sha(sample_path), "scored_case_ids": case_ids,
                "shard_groups": {"local": case_ids[:5], "wingtip": case_ids[5:]}}
            with patch.object(audit, "ROOT", root), patch.object(audit, "_load_freeze",
                    return_value=(freeze, freeze_path)):
                audit_dir = audit.prepare(freeze_path, audit_root=root / "audits")
            manifest = json.loads((audit_dir / "audit_manifest.json").read_text())
            self.assertEqual(manifest["model_checkpoint_sha256"], "checkpoint")
            self.assertEqual(manifest["model_revision"], "revision")

            provenance = root / "launch.json"
            archive = root / "prompt_archive.json"
            tokenizer_path = root / "tokenizer.json"
            provenance.write_text("{}\n")
            archive.write_text("{}\n")
            tokenizer = {"profile_sha256": manifest["model_profile_sha256"], "all_fit": True,
                "completion_requests_made": 0, "context_reserve_tokens": 8192,
                "prompt_count": 40, "source_fingerprint": "source", "initial_library_snapshot_id": "library",
                "backend": "local", "freeze_manifest_sha256": audit._sha(freeze_path),
                "prompt_phases": ["select", "compose", "evolve", "stage3_serve"],
                "case_ids": case_ids, "prompt_archive_path": "prompt_archive.json",
                "prompt_archive_sha256": audit._sha(archive)}
            tokenizer_path.write_text(json.dumps(tokenizer))
            receipt = {"schema_version": "e4-paper-starter-server-receipt/1", "backend": "local",
                "endpoint": "http://127.0.0.1:8080", "model_manifest_sha256": manifest["model_manifest_sha256"],
                "checkpoint_sha256": "checkpoint", "model_revision": "revision",
                "profile_sha256": manifest["model_profile_sha256"], "weights_loaded": True,
                "provenance_path": "launch.json", "provenance_sha256": audit._sha(provenance),
                "tokenizer_preflight_path": "tokenizer.json",
                "tokenizer_preflight_sha256": audit._sha(tokenizer_path)}
            receipt_path = root / "receipt.json"
            receipt_path.write_text(json.dumps(receipt))
            with patch.object(audit, "ROOT", root), patch.dict("os.environ",
                    {"E4_BASE_URL": "http://127.0.0.1:8080"}):
                validated = audit._validate_server_receipt(receipt_path, manifest, "local")
            self.assertEqual(validated["checkpoint_sha256"], "checkpoint")
            self.assertEqual(validated["model_revision"], "revision")

    def test_truth_loader_pins_exact_ten_scored_rows(self):
        ids = [f"bank-20210304T{hour:02d}00-utc8" for hour in range(10)]
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "truth.jsonl"
            rows = [{"incident_id": case_id, "split": "scored",
                     "ground_truth": [{"component": f"component-{index}"}]}
                    for index, case_id in enumerate(ids)]
            raw = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
            path.write_text(raw)
            manifest = {"trusted_truth_pin": {"path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}, "case_ids": ids}
            with patch.object(audit, "ROOT", Path("/")):
                actual = audit._truth_rows(manifest)
            self.assertEqual(list(actual), ids)
            self.assertEqual(actual[ids[3]], ("component-3",))
            manifest["trusted_truth_pin"]["sha256"] = "0" * 64
            with patch.object(audit, "ROOT", Path("/")):
                with self.assertRaisesRegex(audit.AuditError, "metadata pin"):
                    audit._truth_rows(manifest)

    def test_attempt_slots_preserve_fixed_three_attempt_denominator(self):
        session = EvaluationSession(case_id="bank-20210304T0100-utc8",
            allowed_component_ids=("component-a", "component-b"),
            truth_ids=("component-b",))
        result = {"stages": [{"stage": "stage1_select", "status": "static_failure"},
                             {"stage": "stage2_compose", "status": "accepted"}]}
        # Stage 1 failed before feedback; the only submitted feedback belongs to
        # stage 2. This catches a rank shift between stage slots and the ledger.
        result["stages"][1]["status"] = "accepted"
        session.submit({"ranked_components": ["component-b"]})
        slots = audit._slots(result, session)
        self.assertEqual(len(slots), 3)
        self.assertEqual([row["status"] for row in slots], ["failed", "submitted", "not_run_after_acceptance"])
        self.assertEqual([row["accepted"] for row in slots], [False, True, None])
        self.assertEqual([any(row["accepted"] is True for row in slots[:n]) for n in (1, 2, 3)],
                         [False, True, True])

    def test_truth_loader_rejects_partial_denominator_and_duplicate_targets(self):
        ids = [f"bank-20210304T{hour:02d}00-utc8" for hour in range(10)]
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "truth.jsonl"
            rows = [{"incident_id": case_id, "split": "scored",
                "ground_truth": [{"component": "component-a"}]} for case_id in ids[:-1]]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            manifest = {"trusted_truth_pin": {"path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}, "case_ids": ids}
            with patch.object(audit, "ROOT", Path("/")):
                with self.assertRaisesRegex(audit.AuditError, "fixed ten-case denominator"):
                    audit._truth_rows(manifest)

    def test_server_receipt_requires_checkpoint_profile_and_tokenizer_provenance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            provenance = root / "launch.json"
            tokenizer = root / "tokenizer.json"
            prompt_archive = root / "prompts.json"
            provenance.write_text("{}\n")
            prompt_archive.write_text("{}\n")
            tokenizer.write_text(json.dumps({"profile_sha256": "profile", "all_fit": True,
                "completion_requests_made": 0, "context_reserve_tokens": 8192,
                "prompt_count": 40, "source_fingerprint": "source", "initial_library_snapshot_id": "library",
                "backend": "local", "freeze_manifest_sha256": "freeze",
                "prompt_phases": ["select", "compose", "evolve", "stage3_serve"],
                "case_ids": ["case"] * 10,
                "prompt_archive_path": "prompts.json",
                "prompt_archive_sha256": hashlib.sha256(prompt_archive.read_bytes()).hexdigest()}))
            receipt = root / "server.json"
            doc = {"schema_version": "e4-paper-starter-server-receipt/1", "backend": "local",
                "endpoint": "http://127.0.0.1:8080", "model_manifest_sha256": "manifest",
                "checkpoint_sha256": "checkpoint", "model_revision": "revision",
                "profile_sha256": "profile", "weights_loaded": True,
                "provenance_path": "launch.json",
                "provenance_sha256": hashlib.sha256(provenance.read_bytes()).hexdigest(),
                "tokenizer_preflight_path": "tokenizer.json",
                "tokenizer_preflight_sha256": hashlib.sha256(tokenizer.read_bytes()).hexdigest()}
            receipt.write_text(json.dumps(doc))
            manifest = {"model_manifest_sha256": "manifest", "model_checkpoint_sha256": "checkpoint",
                "model_revision": "revision", "model_profile_sha256": "profile",
                "source_fingerprint": "source", "initial_library_snapshot_id": "library",
                "freeze_manifest_sha256": "freeze", "case_ids": ["case"] * 10}
            with patch.object(audit, "ROOT", root), patch.dict("os.environ",
                    {"E4_BASE_URL": "http://127.0.0.1:8080"}):
                pin = audit._validate_server_receipt(receipt, manifest, "local")
            self.assertEqual(pin["backend"], "local")
            self.assertEqual(pin["tokenizer_preflight_sha256"], doc["tokenizer_preflight_sha256"])
            doc["checkpoint_sha256"] = "wrong"
            receipt.write_text(json.dumps(doc))
            with patch.object(audit, "ROOT", root), patch.dict("os.environ",
                    {"E4_BASE_URL": "http://127.0.0.1:8080"}):
                with self.assertRaisesRegex(audit.AuditError, "frozen checkpoint"):
                    audit._validate_server_receipt(receipt, manifest, "local")


if __name__ == "__main__":
    unittest.main()
