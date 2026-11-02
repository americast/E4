import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_e4_revision import (
    ALLOWED_RUNTIME_DELTA_PATHS, BASELINE_COMPARISON_RELATIVE,
    BASELINE_COMPARISON_SHA256, BASELINE_MANIFEST_RELATIVE,
    BASELINE_MANIFEST_SHA256, ROOT, _load_baseline_terminus_cases,
    _validate_revision_contract, _validate_revision_metadata,
    _validate_runtime_delta,
)


def runtime_policy(*, revision=False):
    return {
        "schema_version": "e4-m4-runtime-policy/1",
        "policy_name": "bank-m4-controller-input16m-cpu10-wall15-v3" if revision else "old",
        **({"policy_rationale": "E4 sandbox moved from 2 MiB to 16 MiB, with 10 CPU seconds and 15 wall seconds; unchanged other contracts"}
           if revision else {}),
        "serving": {"acceptance_top_k": 3,
                    "acceptance_contract": "casefolded exact component identity in the frozen top three",
                    "case_wall_seconds": 7200},
        "protected_runtime_values": {"seed": 42, "lead_count": 5},
        "query_and_generation": {
            "maximum_operator_input_bytes": 16 * 1024 * 1024 if revision else 2 * 1024 * 1024,
            **({"generated_operator_cpu_seconds": 10} if revision else {}),
            "generated_operator_wall_seconds": 15 if revision else 5,
            "generated_operator_memory_bytes": 512 * 1024 * 1024,
            "maximum_public_query_rows_per_case": 2048,
        },
        "model_runtime_overrides": {"minimum_support": {"minimum": 4, "maximum": 128}},
        "library": {"total_operator_cap": 19},
    }


def manifest_pair():
    case_ids = [f"case-{index}" for index in range(10)]
    order = {case_id: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
             for index, case_id in enumerate(case_ids)}
    model = {"checkpoint_sha256": "c" * 64,
             "profile": {"context_tokens": 32768, "max_generation_tokens": 8192}}
    public_inputs = {case_id: {"clip_sha256": {"metrics.csv": f"{index:064x}"}}
                     for index, case_id in enumerate(case_ids)}
    budgets = {"case_wall_seconds": 7200, "aggregate_generated_tokens": 73728,
               "terminus_max_turns": 32}
    baseline = {"case_ids": case_ids, "split": "scored", "seed": 42,
        "systems": ["E4", "Terminus-2"], "paired_execution_order": order,
        "model": model, "public_inputs": public_inputs, "resource_budgets": budgets}
    baseline_result = {"case_ids": case_ids,
        "experiment_manifest_sha256": BASELINE_MANIFEST_SHA256,
        "case_jobs": {case_id: {"Terminus-2": {"status": "complete", "complete": True,
            "result_path": f"/tmp/{case_id}.json", "result_sha256": "a" * 64}}
            for case_id in case_ids}}
    revision = {**baseline, "model": dict(model), "public_inputs": dict(public_inputs),
                "resource_budgets": dict(budgets)}
    return revision, baseline, baseline_result


class E4RevisionTests(unittest.TestCase):
    def test_runtime_policy_allows_only_documented_generated_worker_resources(self):
        summary = _validate_runtime_delta(runtime_policy(), runtime_policy(revision=True))
        self.assertEqual(set(summary["allowed_changed_paths"]), ALLOWED_RUNTIME_DELTA_PATHS)

        changed_acceptance = runtime_policy(revision=True)
        changed_acceptance["serving"]["acceptance_top_k"] = 4
        with self.assertRaisesRegex(ValueError, "outside the documented sandbox"):
            _validate_runtime_delta(runtime_policy(), changed_acceptance)

        changed_memory = runtime_policy(revision=True)
        changed_memory["query_and_generation"]["generated_operator_memory_bytes"] = 1024
        with self.assertRaisesRegex(ValueError, "outside the documented sandbox"):
            _validate_runtime_delta(runtime_policy(), changed_memory)

    def test_revision_contract_rejects_profile_and_public_input_drift(self):
        revision, baseline, baseline_result = manifest_pair()
        kwargs = {"baseline_runtime": runtime_policy(),
                  "revision_runtime": runtime_policy(revision=True)}
        changed_profile = {**revision, "model": {"checkpoint_sha256": "d" * 64}}
        with self.assertRaisesRegex(ValueError, "public inputs or the frozen model"):
            _validate_revision_contract(changed_profile, baseline, baseline_result, **kwargs)
        changed_data = {**revision, "public_inputs": {**revision["public_inputs"],
                                                       "case-0": {"alert_sha256": "f" * 64}}}
        with self.assertRaisesRegex(ValueError, "public inputs or the frozen model"):
            _validate_revision_contract(changed_data, baseline, baseline_result, **kwargs)

    def test_revision_contract_rejects_incomplete_terminus_baseline(self):
        revision, baseline, baseline_result = manifest_pair()
        baseline_result["case_jobs"]["case-4"]["Terminus-2"]["status"] = "infrastructure_incomplete"
        with self.assertRaisesRegex(ValueError, "baseline is incomplete for case-4"):
            _validate_revision_contract(revision, baseline, baseline_result,
                baseline_runtime=runtime_policy(), revision_runtime=runtime_policy(revision=True))

    def test_revision_metadata_rejects_wrong_baseline_hash(self):
        revision, _, _ = manifest_pair()
        revision["revision_metadata"] = {
            "schema_version": "e4-exploratory-revision/1",
            "mode": "exploratory_post_analysis_revision",
            "terminus_reused": True, "contemporaneous": False,
            "baseline_comparison": {"path": BASELINE_COMPARISON_RELATIVE.as_posix(),
                                    "sha256": "0" * 64},
            "baseline_manifest": {"path": BASELINE_MANIFEST_RELATIVE.as_posix(),
                                  "sha256": BASELINE_MANIFEST_SHA256},
            "sandbox_delta_note": "E4 sandbox changed; Terminus unchanged"}
        with self.assertRaisesRegex(ValueError, "does not pin the approved immutable Terminus"):
            _validate_revision_metadata(revision,
                baseline_result_path=ROOT / BASELINE_COMPARISON_RELATIVE,
                baseline_manifest_path=ROOT / BASELINE_MANIFEST_RELATIVE)

    def test_recorded_terminus_source_hashes_are_checked_before_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case_id = "case-a"
            source_path = root / "source.json"
            source_path.write_text("{}\n", encoding="utf-8")
            source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
            paired_path = root / "paired.json"
            paired = {"case_id": case_id, "system": "Terminus-2",
                "source_result_path": str(source_path), "source_result_sha256": source_hash,
                "attempts": [{"attempt": index, "status": "not_run", "accepted": False}
                             for index in (1, 2, 3)], "cost": {"model_calls": 0}}
            paired_path.write_text(json.dumps(paired), encoding="utf-8")
            baseline = {"case_jobs": {case_id: {"Terminus-2": {
                "result_path": str(paired_path),
                "result_sha256": hashlib.sha256(paired_path.read_bytes()).hexdigest()}}}}
            cases, hashes = _load_baseline_terminus_cases(baseline, case_ids=[case_id])
            self.assertEqual(cases[case_id]["attempts"], paired["attempts"])
            self.assertEqual(hashes[case_id]["source_result_sha256"], source_hash)
            paired["source_result_sha256"] = "f" * 64
            paired_path.write_text(json.dumps(paired), encoding="utf-8")
            baseline["case_jobs"][case_id]["Terminus-2"]["result_sha256"] = hashlib.sha256(
                paired_path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "source-result hash changed"):
                _load_baseline_terminus_cases(baseline, case_ids=[case_id])


if __name__ == "__main__":
    unittest.main()
