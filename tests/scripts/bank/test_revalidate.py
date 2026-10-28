from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e4.adapters.bank import sha256_file
from e4.agent.bank.controller import _materialize_seeds_for_registry
from e4.operators.bank.query import make_m4_registry
from e4.agent.library import create_library_snapshot, snapshot_library
from scripts.bank.revalidate_library import (
    RevalidationError, _policy_pins, revalidate_seed_only_library,
)


ROOT = Path(__file__).resolve().parents[3]


class RevalidateM4LibraryTests(unittest.TestCase):
    def _fixture(self, base: Path):
        bootstrap = base / "bootstrap"
        bootstrap.mkdir()
        library = bootstrap / "library"
        policy = ROOT / "config/bank/controller_policy.json"
        profile = ROOT / "config/models/gemma.json"
        registry = make_m4_registry(None, [], root=ROOT, audit_dir=None)
        seed_config = json.loads((ROOT / "config/bank/seed_playbooks.json").read_text())
        seeds = _materialize_seeds_for_registry(seed_config, registry)
        pins = _policy_pins(ROOT, policy, profile)
        provenance = {**pins, "implementation_fingerprint": "a" * 64,
                      "bootstrap_calls": 2, "model_alias": "test-capture"}
        original = create_library_snapshot(seeds, [], [], provenance, library,
            builtin_specs=registry.signatures())
        result = {"status": "complete_seed_only", "library_snapshot_id": original.snapshot_id,
                  "library_snapshot_sha256": sha256_file(original.path),
                  "model_call_count": 2}
        (bootstrap / "bootstrap_result.json").write_text(json.dumps(result), encoding="utf-8")
        return bootstrap, library, original, profile, policy

    @staticmethod
    def _pass_fixture(document, generated_specs, scenario, fixture_root, **kwargs):
        return True, {"status": "passed", "scenario": scenario}

    def test_revalidation_adds_metadata_only_child_and_preserves_original_store(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._fixture(base)
            before = {p.relative_to(library).as_posix(): p.read_bytes()
                      for p in library.rglob("*") if p.is_file()}
            with patch("scripts.bank.revalidate_library._execute_synthetic_playbook",
                       side_effect=self._pass_fixture) as execute:
                result = revalidate_seed_only_library(source_library=library,
                    output_dir=base / "revalidated", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=original.snapshot_id,
                    reason="implementation-only repair; no model or case-data changes",
                    root=ROOT, policy_path=policy)
            self.assertEqual(execute.call_count, 20)
            self.assertEqual(result["status"], "revalidated_seed_only")
            self.assertEqual(result["source_snapshot_id"], original.snapshot_id)
            self.assertEqual(result["parent_snapshot_id"], original.snapshot_id)
            self.assertTrue(result["operator_content_unchanged"])
            self.assertTrue(result["playbook_content_unchanged"])
            self.assertFalse(result["gold_accessed"])
            old_after = snapshot_library(library)
            new_after = snapshot_library(base / "revalidated/library")
            self.assertEqual(old_after.snapshot_id, original.snapshot_id)
            self.assertEqual(old_after.document["operators"], new_after.document["operators"])
            self.assertEqual(old_after.document["playbooks"], new_after.document["playbooks"])
            self.assertEqual(old_after.document["provenance"]["bootstrap_calls"], 2)
            self.assertEqual(new_after.document["provenance"]["original_bootstrap_provenance"]["bootstrap_calls"], 2)
            after = {p.relative_to(library).as_posix(): p.read_bytes()
                     for p in library.rglob("*") if p.is_file()}
            self.assertEqual(before, after)

    def test_rejects_wrong_snapshot_and_does_not_create_output(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._fixture(base)
            out = base / "out"
            with self.assertRaises(RevalidationError):
                revalidate_seed_only_library(source_library=library, output_dir=out,
                    profile_path=profile, bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id="0" * 64, reason="test", root=ROOT,
                    policy_path=policy)
            self.assertFalse(out.exists())
            self.assertEqual(snapshot_library(library).snapshot_id, original.snapshot_id)

    def test_fails_closed_when_any_synthetic_seed_fixture_fails(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._fixture(base)
            outcomes = [(True, {"status": "passed"})] * 19 + [(False, {"status": "failed"})]
            with patch("scripts.bank.revalidate_library._execute_synthetic_playbook",
                       side_effect=outcomes):
                with self.assertRaisesRegex(RevalidationError, "synthetic validations failed"):
                    revalidate_seed_only_library(source_library=library,
                        output_dir=base / "out", profile_path=profile,
                        bootstrap_artifact_dir=bootstrap,
                        expected_snapshot_id=original.snapshot_id, reason="test",
                        root=ROOT, policy_path=policy)
            self.assertEqual(snapshot_library(library).snapshot_id, original.snapshot_id)
            self.assertEqual(snapshot_library(base / "out/library").snapshot_id,
                             original.snapshot_id)
            failure = json.loads((base / "out/revalidation_result.json").read_text())
            self.assertEqual(failure["status"], "failed")
            self.assertTrue(failure["source_inventory_unchanged"])

    def test_requires_original_bootstrap_artifact_hash(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._fixture(base)
            result_path = bootstrap / "bootstrap_result.json"
            result = json.loads(result_path.read_text())
            result["library_snapshot_sha256"] = "f" * 64
            result_path.write_text(json.dumps(result), encoding="utf-8")
            with self.assertRaisesRegex(RevalidationError, "snapshot digest"):
                revalidate_seed_only_library(source_library=library,
                    output_dir=base / "out", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=original.snapshot_id, reason="test",
                    root=ROOT, policy_path=policy)


if __name__ == "__main__":
    unittest.main()
