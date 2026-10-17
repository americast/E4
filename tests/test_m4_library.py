import json
import hashlib
import tempfile
import unittest
from pathlib import Path

from e4core.m4_generated import validate_generated_spec
from e4core.m4_library import (
    LibraryConflictError, LibraryIntegrityError, admit_after_accepted,
    assert_library_registry_compatible,
    canonicalize_playbook_graph, create_library_snapshot, generated_specs_from_snapshot,
    load_library_snapshot, playbook_graph_hash, record_library_invocation,
    rollback_to_snapshot, snapshot_library,
)


def generated(name, source=None, version="v1"):
    return validate_generated_spec({
        "name": name, "version": version, "description": "fixture transform",
        "family": "fixture", "input_ports": [{"name": "data", "type": "Aggregate"}],
        "output_type": "Aggregate", "parameters": {},
        "source": source or "def operator(inputs, params, runtime):\n    return inputs['data']\n",
    })


def builtins():
    names = ["case.telemetry", "bank.public.query", "bank.answers.combine"] + [f"bank.test{i}" for i in range(6)]
    return [{"name": name, "version": "1", "inputs": [], "output_type": "Aggregate",
             "parameters": {}, "family": "builtin", "external_input": False,
             "description": "test signature"} for name in names]


def seed_dag():
    return {"schema_version": "e4-dag/1", "nodes": [
        {"id": "input", "operator": "case.telemetry", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine", "version": "1",
         "inputs": {"scores": ["input"]}, "params": {}, "bindings": {}}],
        "return": {"node": "answer"}}


def book_with_ops(first_key, second_key):
    first_name, first_version = first_key.split("@", 1)
    second_name, second_version = second_key.split("@", 1)
    return {"schema_version": "e4-dag/1", "nodes": [
        {"id": "data", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
        {"id": "first", "operator": first_name, "version": first_version,
         "inputs": {"data": "data"}, "params": {}, "bindings": {}},
        {"id": "second", "operator": second_name, "version": second_version,
         "inputs": {"data": "data"}, "params": {}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine", "version": "1",
         "inputs": {"scores": ["first", "second"]}, "params": {}, "bindings": {}}],
        "return": {"node": "answer"}}


class LibraryCanonicalizationTests(unittest.TestCase):
    def test_node_id_renaming_and_topological_permutation_are_isomorphic(self):
        first = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "x", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {"table": "metric"}, "bindings": {}},
            {"id": "y", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {"table": "trace"}, "bindings": {}},
            {"id": "a", "operator": "evolved.a", "version": "v1", "inputs": {"data": "x"}, "params": {"window": "alert"}, "bindings": {"window": "runtime.window"}},
            {"id": "b", "operator": "evolved.b", "version": "v2", "inputs": {"data": "y"}, "params": {"window": "reference"}, "bindings": {}},
            {"id": "out", "operator": "bank.answers.combine", "version": "1", "inputs": {"scores": ["a", "b"]}, "params": {"limit": 5}, "bindings": {}}],
            "return": {"node": "out"}}
        second = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "Y", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {"table": "trace"}, "bindings": {}},
            {"id": "X", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {"table": "metric"}, "bindings": {}},
            {"id": "B", "operator": "evolved.b", "version": "v2", "inputs": {"data": "Y"}, "params": {"window": "reference"}, "bindings": {}},
            {"id": "A", "operator": "evolved.a", "version": "v1", "inputs": {"data": "X"}, "params": {"window": "alert"}, "bindings": {"window": "runtime.window"}},
            {"id": "O", "operator": "bank.answers.combine", "version": "1", "inputs": {"scores": ["A", "B"]}, "params": {"limit": 5}, "bindings": {}}],
            "return": {"node": "O"}}
        self.assertEqual(playbook_graph_hash(first), playbook_graph_hash(second))

    def test_graph_identity_preserves_parameters_bindings_variadic_order_and_sharing(self):
        shared = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "data", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
            {"id": "a", "operator": "evolved.a", "version": "v1", "inputs": {"data": "data"}, "params": {"x": 1}, "bindings": {}},
            {"id": "b", "operator": "evolved.b", "version": "v1", "inputs": {"data": "data"}, "params": {}, "bindings": {}},
            {"id": "out", "operator": "bank.answers.combine", "version": "1", "inputs": {"scores": ["a", "b"]}, "params": {}, "bindings": {}}],
            "return": {"node": "out"}}
        baseline = playbook_graph_hash(shared)
        params = json.loads(json.dumps(shared)); params["nodes"][1]["params"]["x"] = 2
        bindings = json.loads(json.dumps(shared)); bindings["nodes"][1]["bindings"] = {"x": "runtime.x"}
        order = json.loads(json.dumps(shared)); order["nodes"][-1]["inputs"]["scores"].reverse()
        duplicate = json.loads(json.dumps(shared)); duplicate["nodes"].insert(2,
            {"id": "data2", "operator": "bank.public.query", "version": "1", "inputs": {}, "params": {}, "bindings": {}})
        duplicate["nodes"][3]["inputs"]["data"] = "data2"
        self.assertNotEqual(baseline, playbook_graph_hash(params))
        self.assertNotEqual(baseline, playbook_graph_hash(bindings))
        self.assertNotEqual(baseline, playbook_graph_hash(order))
        self.assertNotEqual(baseline, playbook_graph_hash(duplicate))

    def test_graph_canonicalizer_rejects_unreachable_nodes_and_unknown_fields(self):
        dag = seed_dag()
        dag["nodes"].append({"id": "unused", "operator": "bank.test0", "version": "1", "inputs": {}, "params": {}, "bindings": {}})
        with self.assertRaisesRegex(ValueError, "reachable"):
            canonicalize_playbook_graph(dag)
        dag = seed_dag(); dag["unexpected"] = True
        with self.assertRaises(ValueError):
            canonicalize_playbook_graph(dag)


class LibrarySnapshotTests(unittest.TestCase):
    def _initial(self, root, generated=None):
        return create_library_snapshot({"examples": {"seed_combined": seed_dag()}}, generated or [], [],
            {"bootstrap": "synthetic-test", "model": "fixture"}, root,
            builtin_specs=builtins())

    def test_content_hash_roundtrip_generated_specs_and_atomic_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initial = self._initial(root, [generated("evolved.bootstrap")])
            loaded = snapshot_library(root)
            self.assertEqual(initial.snapshot_id, loaded.snapshot_id)
            self.assertEqual(load_library_snapshot(initial.path).snapshot_id, initial.snapshot_id)
            thawed = generated_specs_from_snapshot(loaded)
            self.assertEqual(thawed[0]["name"], "evolved.bootstrap")
            self.assertIsInstance(thawed[0]["input_ports"], list)
            sha_before = initial.path.read_bytes()
            updated = record_library_invocation(initial, invocation_case_id="bank-fixture-a",
                used_operator_keys={"evolved.bootstrap@" + thawed[0]["version"]},
                used_playbook_names={"seed_combined"})
            self.assertNotEqual(initial.snapshot_id, updated.snapshot_id)
            self.assertEqual(initial.path.read_bytes(), sha_before)
            rollback = rollback_to_snapshot(root, initial.snapshot_id, expected_current_id=updated.snapshot_id)
            self.assertEqual(rollback.snapshot_id, initial.snapshot_id)
            self.assertEqual(snapshot_library(root).snapshot_id, initial.snapshot_id)
            self.assertEqual(initial.path.read_bytes(), sha_before)

    def test_current_m4_registry_must_match_snapshot_builtin_signatures(self):
        from e4core.m4_data import make_m4_registry
        project = Path(__file__).resolve().parents[1]
        registry = make_m4_registry(None, [], root=project)
        with tempfile.TemporaryDirectory() as directory:
            snapshot = create_library_snapshot({"examples": {}}, [], [],
                {"bootstrap": "synthetic-registry-test"}, Path(directory),
                builtin_specs=registry.signatures())
            assert_library_registry_compatible(snapshot, registry)

    def test_rejected_stage_three_is_noop_and_cas_rejects_stale_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initial = self._initial(root)
            self.assertIs(admit_after_accepted(initial, [generated("evolved.new")], [], {}, accepted=False,
                verified_reachable_operator_keys=set()), initial)
            newer = record_library_invocation(initial, invocation_case_id="bank-fixture-a",
                used_operator_keys={"case.telemetry@1"}, used_playbook_names={"seed_combined"})
            with self.assertRaises(LibraryConflictError):
                admit_after_accepted(initial, [generated("evolved.new")], [], {}, accepted=True,
                    verified_reachable_operator_keys={"evolved.new@v1"},
                    invocation_case_id="bank-fixture-b")
            self.assertEqual(snapshot_library(root).snapshot_id, newer.snapshot_id)

    def test_admission_requires_literal_boolean_and_generated_names_are_collision_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initial = self._initial(root)
            for value in (1, "true", None):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, "literal bool"):
                    admit_after_accepted(initial, [], [], {}, accepted=value,
                        verified_reachable_operator_keys=set())
            first = generated("evolved.identity")
            second = generated("evolved.identity",
                "def operator(inputs, params, runtime):\n    return inputs['data'] if 'data' in inputs else None\n")
            self.assertEqual(first["name"], second["name"])
            self.assertNotEqual(first["version"], second["version"])
            longest_declared = generated("evolved.long_version", version="v" * 64)
            self.assertLessEqual(len(longest_declared["version"]), 128)
            with self.assertRaises(ValueError):
                generated("../escape")

    def test_cap_evicts_least_used_generated_and_drops_dependent_playbook(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_specs = [generated(f"evolved.{letter}") for letter in "abcdefghij"]
            initial = self._initial(root, old_specs)
            by_name = {spec["name"]: spec for spec in generated_specs_from_snapshot(initial)}
            initial = record_library_invocation(initial, invocation_case_id="bank-case-1",
                used_operator_keys={"evolved.a@" + by_name["evolved.a"]["version"]},
                used_playbook_names={"seed_combined"})
            new = generated("evolved.znew")
            new_key = f"{new['name']}@{new['version']}"
            a_key = f"evolved.a@{by_name['evolved.a']['version']}"
            b_key = f"evolved.b@{by_name['evolved.b']['version']}"
            book = book_with_ops(b_key, new_key)
            book_hash = playbook_graph_hash(book)
            used = {f"{row['name']}@{row['version']}" for row in generated_specs_from_snapshot(initial)} | {new_key}
            updated = admit_after_accepted(initial, [new], [{"name": "evolved_playbook", "document": book}],
                {"stage": 3, "verified": True}, accepted=True,
                verified_reachable_operator_keys=used,
                invocation_case_id="bank-case-2", used_operator_keys=used,
                used_playbook_names={"seed_combined"},
                verified_playbook_graph_hashes={book_hash})
            self.assertEqual(len(updated.operators), 19)
            self.assertIn(a_key, updated.operators)
            self.assertNotIn(b_key, updated.operators)  # least-use tie uses qualified-name tiebreak
            self.assertIn("seed_combined", updated.playbooks)
            self.assertNotIn("evolved_playbook", updated.playbooks)
            lineage = updated.document["lineage"][-1]
            self.assertEqual(list(lineage["evicted_operator_keys"]), [b_key])
            self.assertIn({"name": "evolved_playbook", "reason": "operator_evicted"},
                          lineage["dropped_invalid_playbooks"])

    def test_bootstrap_generated_operators_compete_for_unreserved_cap_slots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            specs = [generated(f"evolved.{letter}") for letter in "abcdefghijk"]
            snapshot = self._initial(root, specs)
            self.assertEqual(len(snapshot.operators), 19)
            self.assertNotIn(f"evolved.a@{specs[0]['version']}", snapshot.operators)
            self.assertIn(f"evolved.k@{specs[-1]['version']}", snapshot.operators)
            event = snapshot.document["lineage"][0]
            self.assertEqual(list(event["bootstrap_evicted_operator_keys"]),
                             [f"evolved.a@{specs[0]['version']}"])

    def test_snapshot_corruption_is_detected_on_load(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = self._initial(root)
            path = snapshot.path
            path.chmod(0o644)
            envelope = json.loads(path.read_text())
            envelope["payload"]["provenance"]["model"] = "tampered"
            path.write_text(json.dumps(envelope))
            with self.assertRaises(LibraryIntegrityError):
                load_library_snapshot(path)

    def test_self_rehashed_snapshot_cannot_hide_generated_version_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = self._initial(root, [generated("evolved.bootstrap")])
            envelope = json.loads(snapshot.path.read_text())
            entry = next(row for row in envelope["payload"]["operators"]
                         if row["name"] == "evolved.bootstrap")
            entry["version"] = "v99+0000000000000000"
            entry["signature"]["version"] = entry["version"]
            canonical = json.dumps(envelope["payload"], ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":"), allow_nan=False).encode()
            envelope["snapshot_id"] = hashlib.sha256(canonical).hexdigest()
            path = root / f"{envelope['snapshot_id']}.json"
            path.write_text(json.dumps(envelope, ensure_ascii=False, sort_keys=True,
                                       separators=(",", ":")) + "\n")
            with self.assertRaisesRegex(LibraryIntegrityError, "identity mismatch"):
                load_library_snapshot(path)


if __name__ == "__main__":
    unittest.main()
