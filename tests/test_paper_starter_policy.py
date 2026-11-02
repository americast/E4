import tempfile
import unittest
from pathlib import Path

from e4core.m4_generated import validate_generated_spec
from e4core.m4_library import (
    LibraryError, PAPER_STARTER_CAPACITY_POLICY, admit_after_accepted,
    create_library_snapshot, load_library_snapshot,
)


def builtin(name, family="paper_diagnostic", output_type="Aggregate"):
    return {"name": name, "version": "1", "inputs": [], "output_type": output_type,
            "parameters": {}, "family": family, "external_input": name == "case.telemetry",
            "description": "policy fixture"}


def proposal(name, *, family="fixture", output_type="Aggregate"):
    return validate_generated_spec({
        "name": name, "version": "v1", "description": "policy proposal",
        "family": family, "input_ports": [{"name": "data", "type": "Aggregate"}],
        "output_type": output_type, "parameters": {},
        "source": "def operator(inputs, params, runtime):\n    return inputs['data']\n",
    })


class PaperStarterCapacityPolicyTests(unittest.TestCase):
    def _initial(self, root):
        rows = [builtin(f"paper.diag{i:02d}") for i in range(19)]
        rows.extend([builtin("case.telemetry", "utility", "Telemetry"),
                     builtin("paper.input.prepare", "paper_data_access", "PaperStarterInput")])
        return create_library_snapshot({"examples": {}}, [], [], {"fixture": True}, root,
            builtin_specs=rows, capacity_policy=PAPER_STARTER_CAPACITY_POLICY)

    def test_utilities_are_reported_protected_and_excluded_from_diagnostic_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = self._initial(Path(directory))
            loaded = load_library_snapshot(snapshot.path)
            policy = loaded.document["policy"]
            self.assertEqual(policy["paper_counts"], {
                "diagnostic_operator_count": 19,
                "utility_operator_count": 2,
                "utility_operator_families": {"paper_data_access": 1, "utility": 1},
            })
            self.assertEqual(set(loaded.document["immutable_operator_keys"]),
                             {"case.telemetry@1", "paper.input.prepare@1"})

    def test_builtin_candidate_score_answer_and_generated_utility_family_still_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [builtin(f"paper.diag{i:02d}") for i in range(19)]
            rows.extend([builtin("case.telemetry", "utility", "Telemetry"),
                builtin("paper.input.prepare", "paper_data_access", "PaperStarterInput"),
                builtin("paper.fake_candidate", "utility", "CandidateSet"),
                builtin("paper.fake_score", "utility", "ScoredCandidates"),
                builtin("paper.fake_answer", "utility", "RankedAnswer")])
            initial = create_library_snapshot({"examples": {}}, [], [], {"fixture": True}, root,
                builtin_specs=rows, capacity_policy=PAPER_STARTER_CAPACITY_POLICY)
            self.assertEqual(initial.document["policy"]["paper_counts"]["diagnostic_operator_count"], 19)
            self.assertEqual(initial.document["policy"]["paper_counts"]["utility_operator_count"], 2)
            self.assertFalse({"paper.fake_candidate@1", "paper.fake_score@1", "paper.fake_answer@1"} &
                set(initial.document["immutable_operator_keys"]))

    def test_generated_operator_cannot_spoof_the_utility_family_to_escape_eviction(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = self._initial(Path(directory))
            new = proposal("evolved.utility_spoof", family="utility", output_type="ScoredCandidates")
            key = f"{new['name']}@{new['version']}"
            admitted = admit_after_accepted(initial, [new], [], {"fixture": True}, accepted=True,
                verified_reachable_operator_keys={key}, invocation_case_id="paper-utility-spoof")
            self.assertIn(key, admitted.operators)
            self.assertNotIn("paper.diag00@1", admitted.operators)
            counts = admitted.document["policy"]["paper_counts"]
            self.assertEqual(counts["diagnostic_operator_count"], 19)
            self.assertEqual(counts["utility_operator_count"], 2)

    def test_admission_evicts_exact_lfu_name_tie_and_keeps_accepted_operator(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = self._initial(Path(directory))
            new = proposal("evolved.new_feature")
            new_key = f"{new['name']}@{new['version']}"
            admitted = admit_after_accepted(initial, [new], [], {"fixture": True}, accepted=True,
                verified_reachable_operator_keys={new_key},
                invocation_case_id="paper-case-1")
            self.assertNotIn("paper.diag00@1", admitted.operators)
            self.assertIn(new_key, admitted.operators)
            event = admitted.document["lineage"][-1]
            self.assertEqual(list(event["evicted_operator_keys"]), ["paper.diag00@1"])
            self.assertEqual(event["eviction_reason"], "least_distinct_case_usage_then_qualified_name")
            self.assertEqual(admitted.document["policy"]["paper_counts"]["diagnostic_operator_count"], 19)

    def test_accepted_dependency_closure_overflow_rejects_without_writing_child(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = self._initial(Path(directory))
            new = proposal("evolved.new_feature")
            reachable = {f"paper.diag{i:02d}@1" for i in range(19)}
            reachable.add(f"{new['name']}@{new['version']}")
            with self.assertRaisesRegex(LibraryError, "dependency closure"):
                admit_after_accepted(initial, [new], [], {"fixture": True},
                    accepted=True, verified_reachable_operator_keys=reachable,
                    invocation_case_id="paper-case-overflow")
            self.assertEqual(load_library_snapshot(initial.root).snapshot_id, initial.snapshot_id)
            self.assertEqual(len(list(initial.root.glob("*.json"))), 1)

    def test_accepted_composite_dependency_is_protected_from_lfu_eviction(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = self._initial(Path(directory))
            composite = {"name": "evolved.wrap_diagnostic", "version": "v1",
                "description": "composite fixture", "family": "fixture",
                "input_ports": [{"name": "data", "type": "Aggregate"}],
                "output_type": "Aggregate", "parameters": {},
                "authoring_profile": "operator-dag-v1",
                "program": {"schema_version": "e4-operator-dag/1", "nodes": [{
                    "id": "call", "operator": "paper.diag00", "version": "1",
                    "inputs": {}, "params": {}, "bindings": {}}],
                    "return": {"node": "call"}}}
            from e4core.m4_composite import normalize_composite_spec
            from e4core.m4_library import _operator_registry_from_rows
            normalized = normalize_composite_spec(composite,
                _operator_registry_from_rows(initial.document["operators"]))
            normalized_version = normalized["version"]
            admitted = admit_after_accepted(initial, [composite], [], {"fixture": True},
                accepted=True,
                verified_reachable_operator_keys={f"{composite['name']}@{normalized_version}"},
                invocation_case_id="paper-composite-case")
            self.assertIn("paper.diag00@1", admitted.operators)
            self.assertNotIn("paper.diag01@1", admitted.operators)
            self.assertEqual(list(admitted.document["lineage"][-1]["accepted_dependency_closure"]),
                ["evolved.wrap_diagnostic@" + normalized_version, "paper.diag00@1"])

    def test_paper_admission_rejects_more_than_three_transient_proposals(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = self._initial(Path(directory))
            proposals = [proposal(f"evolved.proposal_{index}") for index in range(4)]
            with self.assertRaisesRegex(LibraryError, "at most three"):
                admit_after_accepted(initial, proposals, [], {"fixture": True}, accepted=True,
                    verified_reachable_operator_keys={f"{row['name']}@{row['version']}" for row in proposals},
                    invocation_case_id="paper-too-many")
            self.assertEqual(load_library_snapshot(initial.root).snapshot_id, initial.snapshot_id)

    def test_legacy_profile_does_not_gain_paper_capacity_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            initial = create_library_snapshot({"examples": {}}, [], [], {}, Path(directory),
                builtin_specs=[builtin("legacy.one")])
            self.assertNotIn("capacity_policy", initial.document["policy"])
            self.assertNotIn("paper_counts", initial.document["policy"])
            self.assertEqual(initial.document["policy"]["total_operator_cap"], 19)


if __name__ == "__main__":
    unittest.main()
