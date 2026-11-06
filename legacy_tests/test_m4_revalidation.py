from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e4core.bank_adapter import sha256_file
from e4core.m4_controller import _materialize_seeds_for_registry
from e4core.m4_controller import _validate_graph, build_m4_runtime
from e4core.m4_data import make_m4_registry
from e4core.m4_fixtures import write_synthetic_public_case
from e4core.m4_generated import validate_generated_spec
from e4core.m4_library import (create_library_snapshot, operator_key,
                               playbook_graph_hash, snapshot_library, _thaw)
from scripts.revalidate_m4_library import (
    FROZEN_SEED_CASES, RevalidationError, _policy_pins, revalidate_generated_bootstrap_library,
    revalidate_seed_only_library, _protocol_policy_migration,
    PROTOCOL_SOURCE_POLICY_SHA256, PROTOCOL_MIGRATION_FIELDS, main,
)
from e4core.m4_bootstrap import _copy_fixture_policy


ROOT = Path(__file__).resolve().parents[1]


class RevalidateM4LibraryTests(unittest.TestCase):
    def _fixture(self, base: Path):
        bootstrap = base / "bootstrap"
        bootstrap.mkdir(parents=True)
        library = bootstrap / "library"
        policy = ROOT / "config/m4_runtime.json"
        profile = ROOT / "config/gemma_m3_inference.json"
        registry = make_m4_registry(None, [], root=ROOT, audit_dir=None)
        seed_config = json.loads((ROOT / "config/bank_seed_library.json").read_text())
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

    def _generated_fixture(self, base: Path, *, builtin_drift=False,
                           seed_drift=False, provenance_drift=False,
                           source_policy_migration=False):
        bootstrap = base / "bootstrap_generated"
        bootstrap.mkdir(parents=True)
        library = bootstrap / "library"
        policy = ROOT / "config/m4_runtime.json"
        profile = ROOT / "config/gemma_m3_inference.json"
        base_registry = make_m4_registry(None, [], root=ROOT, audit_dir=None)
        generated_spec = validate_generated_spec({
            "name": "evolved.revalidation_fixture",
            "version": "1",
            "description": "Synthetic generated scorer retained for revalidation tests.",
            "family": "synthetic_revalidation",
            "input_ports": [{"name": "candidates", "type": "CandidateSet"}],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "source": ("def operator(inputs, params, runtime):\n"
                       "    return {'$type':'ScoredCandidates','$fields':"
                       "{'candidates':{'$tuple':[]},'scorer':'fixture'}}\n")})
        generated_registry = make_m4_registry(None, [generated_spec], root=ROOT, audit_dir=None)
        signatures = {row["name"]: row for row in generated_registry.signatures()}
        dag = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "case.telemetry", "version": signatures["case.telemetry"]["version"],
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate",
             "version": signatures["bank.candidates.generate"]["version"],
             "inputs": {"data": "data"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": generated_spec["name"], "version": generated_spec["version"],
             "inputs": {"candidates": "candidates"}, "params": {}, "bindings": {}},
            {"id": "answer", "operator": "bank.answers.combine",
             "version": signatures["bank.answers.combine"]["version"],
             "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {}}],
            "return": {"node": "answer"}}
        outcomes = [{"scenario": scenario, "status": "passed",
                     "top_components": [alpha_id] if scenario == "positive" else [],
                     "answer_digest": "b" * 64}
                    for scenario, alpha_id, _beta_id, _fixture in FROZEN_SEED_CASES]
        generated_book = {"name": "generated_test_playbook", "document": dag,
            "provenance": {"source": "schema_only_bootstrap", "synthetic_outcomes": outcomes}}
        seed_config = json.loads((ROOT / "config/bank_seed_library.json").read_text())
        seed_docs = _materialize_seeds_for_registry(seed_config, base_registry)
        if seed_drift:
            seed_docs.pop(next(iter(seed_docs)))
        builtin_specs = base_registry.signatures()
        if builtin_drift:
            builtin_specs = builtin_specs[:-1]
        pins = _policy_pins(ROOT, policy, profile)
        if source_policy_migration:
            pins["m4_runtime_policy_sha256"] = PROTOCOL_SOURCE_POLICY_SHA256
        if provenance_drift:
            pins["bank_metric_policy_sha256"] = "f" * 64
        provenance = {**pins, "implementation_fingerprint": "a" * 64,
            "bootstrap_calls": 2, "model_alias": "test-capture",
            "bank_input_manifest_used": False,
            "bootstrap_model_response_obtained": True}
        snapshot = create_library_snapshot(seed_docs, [generated_spec], [generated_book],
            provenance, library, builtin_specs=builtin_specs,
            verified_playbook_graph_hashes={playbook_graph_hash(dag)})
        operators = [row for row in snapshot.document["operators"] if row["kind"] == "generated"]
        books = [row for row in snapshot.document["playbooks"] if row["kind"] == "generated"]
        result = {"status": "complete_with_additions", "library_snapshot_id": snapshot.snapshot_id,
            "library_snapshot_sha256": sha256_file(snapshot.path),
            "library_operator_count": len(snapshot.operators),
            "library_playbook_count": len(snapshot.playbooks),
            "accepted_generated_operators": sorted(operator_key(row) for row in operators),
            "accepted_generated_playbooks": sorted(row["name"] for row in books),
            "verified_seed_playbooks": [{"name": name, "passed": True,
                                         "outcomes": list(outcomes)}
                                        for name in sorted(seed_docs)],
            "implementation_fingerprint": "a" * 64, "model_call_count": 2}
        (bootstrap / "bootstrap_result.json").write_text(json.dumps(result), encoding="utf-8")
        return bootstrap, library, snapshot, profile, policy

    @staticmethod
    def _rewrite_snapshot(library: Path, payload_mutator):
        snapshot = snapshot_library(library)
        payload = _thaw(snapshot.document)
        payload_mutator(payload)
        serialized_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode("utf-8")
        snapshot_id = hashlib.sha256(serialized_payload).hexdigest()
        envelope = {"snapshot_id": snapshot_id, "payload": payload}
        target = library / f"{snapshot_id}.json"
        target.write_bytes(json.dumps(envelope, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n")
        (library / "CURRENT").write_text(snapshot_id + "\n", encoding="ascii")
        result_path = library.parent / "bootstrap_result.json"
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["library_snapshot_id"] = snapshot_id
        result["library_snapshot_sha256"] = sha256_file(target)
        result["library_operator_count"] = len(payload["operators"])
        result["library_playbook_count"] = len(payload["playbooks"])
        result_path.write_text(json.dumps(result), encoding="utf-8")
        return snapshot_id, sha256_file(target)

    @staticmethod
    def _pass_fixture(document, generated_specs, scenario, fixture_root, **kwargs):
        return True, {"status": "passed", "scenario": scenario}

    def test_revalidation_adds_metadata_only_child_and_preserves_original_store(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._fixture(base)
            before = {p.relative_to(library).as_posix(): p.read_bytes()
                      for p in library.rglob("*") if p.is_file()}
            with patch("scripts.revalidate_m4_library._execute_synthetic_playbook",
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
            with patch("scripts.revalidate_m4_library._execute_synthetic_playbook",
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

    def test_generated_bootstrap_mode_preserves_operators_books_and_original_artifacts(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-generated-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._generated_fixture(base)
            source_before = {p.relative_to(library).as_posix(): p.read_bytes()
                             for p in library.rglob("*") if p.is_file()}
            artifact_before = {p.relative_to(bootstrap).as_posix(): p.read_bytes()
                               for p in bootstrap.rglob("*") if p.is_file()}
            passed_calls = []

            def run_playbook(document, generated_specs, scenario, fixture_root, **kwargs):
                passed_calls.append((document, generated_specs, scenario))
                self.assertEqual(len(generated_specs), 1)
                self.assertEqual(generated_specs[0]["name"], "evolved.revalidation_fixture")
                self.assertIsInstance(document, dict)
                self.assertIsInstance(document["nodes"], list)
                _copy_fixture_policy(ROOT, fixture_root)
                case = write_synthetic_public_case(fixture_root, scenario=scenario,
                    alpha_id=kwargs["alpha_id"], beta_id=kwargs["beta_id"])
                registry = make_m4_registry(case, generated_specs, root=fixture_root,
                                            audit_dir=None)
                runtime = build_m4_runtime(case.alert, root=ROOT)
                verified = _validate_graph(document, registry, runtime, stage="bootstrap")
                self.assertEqual(verified.return_node, document["return"]["node"])
                return True, {"status": "passed", "scenario": scenario}

            with patch("scripts.revalidate_m4_library._execute_synthetic_playbook",
                       side_effect=run_playbook):
                result = revalidate_generated_bootstrap_library(source_library=library,
                    output_dir=base / "preserved", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=original.snapshot_id,
                    expected_snapshot_sha256=sha256_file(original.path),
                    reason="preserve generated bootstrap across implementation-only revalidation",
                    root=ROOT, policy_path=policy)

            self.assertEqual(len(passed_calls), 25)
            self.assertEqual(result["status"], "revalidated_generated_bootstrap")
            self.assertEqual(result["builtin_count"], 10)
            self.assertEqual(result["seed_playbook_count"], 4)
            self.assertEqual(result["generated_operator_count"], 1)
            self.assertEqual(result["generated_playbook_count"], 1)
            self.assertEqual(result["library_operator_count"], 11)
            self.assertEqual(result["library_playbook_count"], 5)
            self.assertTrue(result["operator_content_unchanged"])
            self.assertTrue(result["playbook_content_unchanged"])
            self.assertTrue(result["source_inventory_unchanged"])
            self.assertTrue(result["bootstrap_artifact_inventory_unchanged"])
            self.assertEqual(result["model_calls"], 0)
            self.assertFalse(result["gold_accessed"])
            preserved = snapshot_library(base / "preserved/library")
            self.assertEqual(preserved.document["operators"], original.document["operators"])
            self.assertEqual(preserved.document["playbooks"], original.document["playbooks"])
            self.assertEqual(preserved.document["provenance"]["original_bootstrap_provenance"],
                             original.document["provenance"])
            self.assertEqual({p.relative_to(library).as_posix(): p.read_bytes()
                              for p in library.rglob("*") if p.is_file()}, source_before)
            self.assertEqual({p.relative_to(bootstrap).as_posix(): p.read_bytes()
                              for p in bootstrap.rglob("*") if p.is_file()}, artifact_before)

    def test_generated_bootstrap_protocol_migration_replays_and_preserves_25_fixtures(self):
        source_policy = ROOT / "tests/fixtures/protocol_v6/source_policy.json"
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-protocol-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._generated_fixture(
                base, source_policy_migration=True)
            with patch("scripts.revalidate_m4_library._execute_synthetic_playbook",
                       side_effect=self._pass_fixture) as execute:
                result = revalidate_generated_bootstrap_library(source_library=library,
                    output_dir=base / "preserved", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=original.snapshot_id,
                    expected_snapshot_sha256=sha256_file(original.path),
                    reason="approved V4 to separated evolve/serve policy migration",
                    root=ROOT, policy_path=policy, source_policy_path=source_policy)
            self.assertEqual(execute.call_count, 25)
            migration = result["protocol_policy_migration"]
            self.assertEqual(migration["source_policy_sha256"], PROTOCOL_SOURCE_POLICY_SHA256)
            self.assertEqual(migration["current_policy_sha256"], sha256_file(policy))
            self.assertEqual({row["path"] for row in migration["approved_differences"]},
                             set(PROTOCOL_MIGRATION_FIELDS))
            self.assertEqual(result["fixture_count"], 25)
            self.assertTrue(result["operator_content_unchanged"])
            self.assertTrue(result["playbook_content_unchanged"])
            self.assertTrue(result["source_inventory_unchanged"])
            self.assertTrue(result["bootstrap_artifact_inventory_unchanged"])
            self.assertEqual(result["model_calls"], 0)
            self.assertFalse(result["gold_accessed"])
            child = snapshot_library(base / "preserved/library")
            self.assertEqual(child.document["provenance"]["original_bootstrap_provenance"],
                             original.document["provenance"])
            self.assertEqual(child.document["provenance"]["m4_runtime_policy_sha256"],
                             sha256_file(policy))

    def _migrated_policy_fixture(self, base: Path) -> tuple[Path, Path]:
        source = ROOT / "tests/fixtures/protocol_v6/source_policy.json"
        policy = json.loads(source.read_text(encoding="utf-8"))
        for dotted, value in PROTOCOL_MIGRATION_FIELDS.items():
            policy["serving"][dotted.split(".", 1)[1]] = value
        target = base / "m4_runtime.json"
        target.write_text(json.dumps(policy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return source, target

    def test_protocol_migration_rejects_threshold_or_removed_source_keys(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-protocol-test-") as td:
            base = Path(td)
            source, policy_path = self._migrated_policy_fixture(base)
            provenance = {"m4_runtime_policy_sha256": PROTOCOL_SOURCE_POLICY_SHA256}
            policy = json.loads(policy_path.read_text(encoding="utf-8"))
            policy["model_runtime_overrides"]["minimum_support"]["minimum"] += 1
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            with self.assertRaisesRegex(RevalidationError, "unapproved"):
                _protocol_policy_migration(source_policy_path=source, policy_path=policy_path,
                                           root=ROOT, provenance=provenance)

            source, policy_path = self._migrated_policy_fixture(base)
            policy = json.loads(policy_path.read_text(encoding="utf-8"))
            policy["query_and_generation"]["maximum_public_query_rows_per_case"] += 1
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            with self.assertRaisesRegex(RevalidationError, "unapproved"):
                _protocol_policy_migration(source_policy_path=source, policy_path=policy_path,
                                           root=ROOT, provenance=provenance)

            source, policy_path = self._migrated_policy_fixture(base)
            policy = json.loads(policy_path.read_text(encoding="utf-8"))
            del policy["serving"]["maximum_diagnostic_attempts"]
            policy_path.write_text(json.dumps(policy), encoding="utf-8")
            with self.assertRaisesRegex(RevalidationError, "unapproved"):
                _protocol_policy_migration(source_policy_path=source, policy_path=policy_path,
                                           root=ROOT, provenance=provenance)

    def test_protocol_migration_rejects_unmatched_source_hash_and_cli_option(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-protocol-test-") as td:
            base = Path(td)
            source, policy_path = self._migrated_policy_fixture(base)
            tampered = base / "source_policy.json"
            tampered.write_bytes(source.read_bytes() + b" ")
            provenance = {"m4_runtime_policy_sha256": PROTOCOL_SOURCE_POLICY_SHA256}
            with self.assertRaisesRegex(RevalidationError, "source policy SHA-256"):
                _protocol_policy_migration(source_policy_path=tampered, policy_path=policy_path,
                                           root=ROOT, provenance=provenance)
            with self.assertRaisesRegex(RevalidationError, "does not match original snapshot provenance"):
                _protocol_policy_migration(source_policy_path=source, policy_path=policy_path,
                    root=ROOT, provenance={"m4_runtime_policy_sha256": "0" * 64})
        with self.assertRaises(SystemExit) as caught:
            main(["--source-library", "source", "--bootstrap-artifact-dir", "bootstrap",
                  "--expected-snapshot-id", "a" * 64, "--output-dir", "out",
                  "--reason", "test", "--source-policy", "source-policy.json"])
        self.assertEqual(caught.exception.code, 2)

    def test_generated_bootstrap_mode_rejects_wrong_snapshot_sha_and_tampered_provenance(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-generated-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._generated_fixture(base)
            with self.assertRaisesRegex(RevalidationError, "snapshot SHA-256"):
                revalidate_generated_bootstrap_library(source_library=library,
                    output_dir=base / "wrong_sha", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=original.snapshot_id,
                    expected_snapshot_sha256="0" * 64, reason="test", root=ROOT,
                    policy_path=policy)
            bootstrap2, library2, original2, profile2, policy2 = self._generated_fixture(
                base / "provenance", provenance_drift=True)
            with self.assertRaisesRegex(RevalidationError, "frozen bank_metric_policy_sha256"):
                revalidate_generated_bootstrap_library(source_library=library2,
                    output_dir=base / "bad_provenance", profile_path=profile2,
                    bootstrap_artifact_dir=bootstrap2,
                    expected_snapshot_id=original2.snapshot_id,
                    expected_snapshot_sha256=sha256_file(original2.path), reason="test",
                    root=ROOT, policy_path=policy2)

    def test_generated_bootstrap_mode_rejects_existing_usage_and_generated_source_tampering(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-generated-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._generated_fixture(base)
            self._rewrite_snapshot(library, lambda payload: next(
                row for row in payload["operators"] if row["kind"] == "generated"
            )["invoked_case_ids"].append("prior-case"))
            current = snapshot_library(library)
            with self.assertRaisesRegex(RevalidationError, "operator usage"):
                revalidate_generated_bootstrap_library(source_library=library,
                    output_dir=base / "usage", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=current.snapshot_id,
                    expected_snapshot_sha256=sha256_file(current.path), reason="test",
                    root=ROOT, policy_path=policy)

        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-generated-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._generated_fixture(base)
            self._rewrite_snapshot(library, lambda payload: payload["operators"][-1].__setitem__(
                "source", payload["operators"][-1]["source"] + "\n# tampered source"))
            current_id = (library / "CURRENT").read_text(encoding="ascii").strip()
            current_path = library / f"{current_id}.json"
            with self.assertRaisesRegex(RevalidationError, "failed integrity"):
                revalidate_generated_bootstrap_library(source_library=library,
                    output_dir=base / "tampered_source", profile_path=profile,
                    bootstrap_artifact_dir=bootstrap,
                    expected_snapshot_id=current_id,
                    expected_snapshot_sha256=sha256_file(current_path), reason="test",
                    root=ROOT, policy_path=policy)

    def test_generated_bootstrap_mode_rejects_builtin_and_seed_drift(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-generated-test-") as td:
            base = Path(td)
            for name, kwargs, message in (
                    ("builtin", {"builtin_drift": True}, "ten immutable built-ins"),
                    ("seed", {"seed_drift": True}, "four protected seed playbooks")):
                bootstrap, library, original, profile, policy = self._generated_fixture(
                    base / name, **kwargs)
                with self.assertRaisesRegex(RevalidationError, message):
                    revalidate_generated_bootstrap_library(source_library=library,
                        output_dir=base / f"out_{name}", profile_path=profile,
                        bootstrap_artifact_dir=bootstrap,
                        expected_snapshot_id=original.snapshot_id,
                        expected_snapshot_sha256=sha256_file(original.path), reason="test",
                        root=ROOT, policy_path=policy)

    def test_generated_bootstrap_mode_fails_closed_on_any_retained_fixture_failure(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix="m4-revalidate-generated-test-") as td:
            base = Path(td)
            bootstrap, library, original, profile, policy = self._generated_fixture(base)
            outcomes = [(True, {"status": "passed"})] * 24 + [(False, {"status": "failed"})]
            with patch("scripts.revalidate_m4_library._execute_synthetic_playbook",
                       side_effect=outcomes) as execute:
                with self.assertRaisesRegex(RevalidationError, "retained bootstrap playbook fixtures failed"):
                    revalidate_generated_bootstrap_library(source_library=library,
                        output_dir=base / "failed_fixture", profile_path=profile,
                        bootstrap_artifact_dir=bootstrap,
                        expected_snapshot_id=original.snapshot_id,
                        expected_snapshot_sha256=sha256_file(original.path), reason="test",
                        root=ROOT, policy_path=policy)
            self.assertEqual(execute.call_count, 25)
            self.assertFalse((base / "failed_fixture/library").exists())
            failure = json.loads((base / "failed_fixture/revalidation_result.json").read_text())
            self.assertEqual(failure["status"], "failed")
            self.assertEqual(failure["operators_removed"], 0)
            self.assertEqual(failure["playbooks_removed"], 0)
            self.assertEqual(snapshot_library(library).snapshot_id, original.snapshot_id)


if __name__ == "__main__":
    unittest.main()
