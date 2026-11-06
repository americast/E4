import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from e4core.cache import canonical_json
from e4core.m4_controller import _implementation_fingerprint
from e4core.m4_library import (_thaw, _write_snapshot, generated_specs_from_snapshot,
    load_library_snapshot, operator_key)
from scripts.repair_m4_library import (NEW_SOURCE_SHA256, OLD_SOURCE_SHA256, RepairError,
    repair_library)


ROOT = Path(__file__).resolve().parents[1]
BASE_LIBRARY = ROOT / "tests/fixtures/legacy_library"
PROPOSED_SOURCE = BASE_LIBRARY / "proposed_source.py"
OLD_KEY = "evolved.log_pattern_scorer@1+23690222713563f3"
NEW_KEY = "evolved.log_pattern_scorer@1+5ae68443b341841b"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class RepairM4LibraryTests(unittest.TestCase):
    def _fresh_v11_base(self, root: Path):
        source_library = root / "base_revalidated/library"
        shutil.copytree(BASE_LIBRARY, source_library)
        old = load_library_snapshot(source_library)
        payload = _thaw(old.document)
        payload["parent_snapshot_id"] = old.snapshot_id
        payload["provenance"]["implementation_fingerprint"] = _implementation_fingerprint()
        fresh = _write_snapshot(payload, source_library, expected_current_id=old.snapshot_id)
        return source_library, fresh

    def test_exact_repair_creates_pinned_child_and_mechanically_updates_playbook_refs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source_library, source_snapshot = self._fresh_v11_base(root)
            source_inventory_before = {p.relative_to(source_library).as_posix(): _sha(p)
                for p in source_library.rglob("*") if p.is_file()}
            original = {operator_key(row): row for row in source_snapshot.document["operators"]}
            original_books = {row["name"]: row for row in source_snapshot.document["playbooks"]}
            report = repair_library(source_library=source_library,
                expected_source_snapshot_id=source_snapshot.snapshot_id,
                expected_source_snapshot_sha256=_sha(source_snapshot.path),
                expected_old_source_sha256=OLD_SOURCE_SHA256,
                proposed_source=PROPOSED_SOURCE,
                expected_new_source_sha256=NEW_SOURCE_SHA256,
                output_dir=root / "typed_v11/legacy_operator_repair",
                reason="repair the reviewed starter scorer's typed envelope contract",
                allowed_output_root=root / "typed_v11")
            repaired = load_library_snapshot(Path(report["new_library"]))
            self.assertEqual(report["status"], "repaired")
            self.assertEqual(report["source_snapshot_id"], source_snapshot.snapshot_id)
            self.assertEqual(report["old_operator_key"], OLD_KEY)
            self.assertEqual(report["new_operator_key"], NEW_KEY)
            self.assertEqual(report["repair_fields"], ["ScoredCandidate.entity",
                "Evidence.contributing_entities", "Evidence.source"])
            self.assertTrue(report["exact_three_field_patch"])
            self.assertTrue(report["counting_logic_unchanged"])
            self.assertTrue(report["other_operator_content_unchanged"])
            self.assertTrue(report["playbook_changes_only_operator_version_references"])
            self.assertTrue(report["source_library_inventory_unchanged"])
            self.assertEqual(report["builtin_count"], 10)
            self.assertEqual(report["library_operator_count"], 11)
            self.assertEqual(report["library_playbook_count"], 5)
            self.assertEqual(report["starter_usage_count"], 0)
            self.assertEqual(report["model_calls"], 0)
            self.assertFalse(report["gold_accessed"])
            self.assertEqual(report["migration_lineage_event"],
                "authorized_legacy_operator_contract_repair")
            self.assertIn("profile_sha256", report["policy_and_profile_pins"])
            self.assertEqual(repaired.document["lineage"][-1]["event"],
                "authorized_legacy_operator_contract_repair")
            self.assertEqual(repaired.parent_snapshot_id, source_snapshot.snapshot_id)
            self.assertIsNone(repaired.operators.get(OLD_KEY))
            self.assertEqual(repaired.operators[NEW_KEY]["source_sha256"], NEW_SOURCE_SHA256)
            self.assertEqual(tuple(repaired.operators[NEW_KEY]["invoked_case_ids"]), ())
            self.assertEqual(len(repaired.operators), 11)
            self.assertEqual(len(repaired.playbooks), 5)
            for key, row in original.items():
                if key != OLD_KEY:
                    self.assertEqual(canonical_json(repaired.operators[key]), canonical_json(row))
            old_references = [name for name, book in original_books.items()
                if any(node.get("operator") == OLD_KEY.split("@", 1)[0] and
                       node.get("version") == OLD_KEY.split("@", 1)[1]
                       for node in book["document"]["nodes"])]
            self.assertEqual(old_references, report["changed_playbook_names"])
            self.assertEqual(old_references, ["log_depth_analysis"])
            for name, book in repaired.playbooks.items():
                if name not in old_references:
                    self.assertEqual(book["graph_sha256"], original_books[name]["graph_sha256"])
                    self.assertEqual(canonical_json(book["document"]),
                        canonical_json(original_books[name]["document"]))
                else:
                    old_doc = _thaw(book["document"])
                    expected_doc = _thaw(original_books[name]["document"])
                    for node in expected_doc["nodes"]:
                        if (node.get("operator") == OLD_KEY.split("@", 1)[0] and
                                node.get("version") == OLD_KEY.rsplit("@", 1)[1]):
                            node["version"] = NEW_KEY.rsplit("@", 1)[1]
                    self.assertEqual(canonical_json(old_doc), canonical_json(expected_doc))
                    self.assertNotEqual(book["graph_sha256"],
                        original_books[name]["graph_sha256"])
                self.assertFalse(any(node.get("operator") == OLD_KEY.split("@", 1)[0] and
                    node.get("version") == OLD_KEY.split("@", 1)[1]
                    for node in book["document"]["nodes"]))
            source_inventory_after = {p.relative_to(source_library).as_posix(): _sha(p)
                for p in source_library.rglob("*") if p.is_file()}
            self.assertEqual(source_inventory_before, source_inventory_after)
            self.assertEqual(json.loads((Path(report["new_library"]).parent /
                "repair_result.json").read_text())["new_snapshot_id"], repaired.snapshot_id)

    def test_repair_rejects_wrong_hash_and_any_unreviewed_source_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source_library, source_snapshot = self._fresh_v11_base(root)
            common = {"source_library": source_library,
                "expected_source_snapshot_id": source_snapshot.snapshot_id,
                "expected_source_snapshot_sha256": _sha(source_snapshot.path),
                "proposed_source": PROPOSED_SOURCE,
                "expected_new_source_sha256": NEW_SOURCE_SHA256,
                "output_dir": root / "typed_v11/rejected",
                "reason": "negative test", "allowed_output_root": root / "typed_v11"}
            with self.assertRaises(RepairError):
                repair_library(**{**common, "expected_old_source_sha256": "0" * 64})
            self.assertFalse((root / "typed_v11/rejected").exists())
            altered_source = root / "altered.py"
            altered_source.write_bytes(PROPOSED_SOURCE.read_bytes() + b"\n# unreviewed\n")
            with self.assertRaises(RepairError):
                repair_library(**{**common, "proposed_source": altered_source,
                    "expected_old_source_sha256": OLD_SOURCE_SHA256})
            self.assertFalse((root / "typed_v11/rejected").exists())


if __name__ == "__main__":
    unittest.main()
