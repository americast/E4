import tempfile
import unittest
from pathlib import Path

from e4core.m5_manifest import (FrozenManifestError, SCHEMA,
    _validate_local_file_inventory, _validate_manifest_header, _validate_pristine_bootstrap,
    _validate_public_view_pin, _validate_resource_budget, _validate_development_budget,
    _bootstrap_host_library_path, _manifest_files)


class FrozenManifestGateTests(unittest.TestCase):
    def test_bootstrap_result_namespace_path_resolves_to_preserved_host_library(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_dir = root / "artifacts" / "bootstrap"
            library = result_dir / "library"
            library.mkdir(parents=True)
            result_path = result_dir / "bootstrap_result.json"
            result_path.write_text("{}", encoding="utf-8")
            resolved = _bootstrap_host_library_path(root, result_path,
                {"library_path": "/m4run/bootstrap/library"})
            self.assertEqual(resolved, library)

    def test_frozen_header_rejects_unknown_case_and_wrong_alternating_order(self):
        ids = [f"bank-20261016T{index:04d}-utc8" for index in range(10)]
        sample = {"selection": {"scored_ids": ids}}
        order = {case: (["e4", "terminus"] if index % 2 == 0 else ["terminus", "e4"])
                 for index, case in enumerate(ids)}
        manifest = {"schema_version": SCHEMA, "status": "frozen", "split": "scored",
                    "seed": 42, "case_ids": ids, "systems": ["E4", "Terminus-2"],
                    "external_baseline": "Terminus-2", "paired_execution_order": order}
        self.assertEqual(_validate_manifest_header(manifest, sample, case_id=ids[0], split="scored"), ids)
        with self.assertRaises(FrozenManifestError):
            _validate_manifest_header(manifest, sample, case_id="bank-20261016T9999-utc8",
                                      split="scored")
        changed = dict(manifest)
        changed["paired_execution_order"] = dict(order)
        changed["paired_execution_order"][ids[0]] = ["terminus", "e4"]
        with self.assertRaises(FrozenManifestError):
            _validate_manifest_header(changed, sample, case_id=ids[0], split="scored")

    def test_resource_budget_must_match_active_frozen_settings(self):
        budgets = {"status": "frozen", "case_wall_seconds": 100,
                   "terminus_max_turns": 20, "aggregate_generated_tokens": 73728}
        manifest = {"resource_budgets": budgets}
        _validate_resource_budget(manifest, {"resource_budgets": dict(budgets)})
        pending = {"resource_budgets": {**budgets, "status": "pending"}}
        with self.assertRaises(FrozenManifestError):
            _validate_resource_budget(pending, {"resource_budgets": pending["resource_budgets"]})
        stale = {"resource_budgets": {**budgets, "terminus_max_turns": 21}}
        with self.assertRaises(FrozenManifestError):
            _validate_resource_budget(manifest, stale)

    def test_development_budget_allows_unmeasured_native_turn_limit_only(self):
        budgets = {"status": "development_pending_measurement", "case_wall_seconds": 7200,
                   "terminus_max_turns": None, "aggregate_generated_tokens": 73728}
        _validate_development_budget({"resource_budgets": budgets}, {"resource_budgets": budgets})
        requirements_pending = {"resource_budgets": {**budgets,
            "status": "must_be_measured_on_both_development_cases_before_scored_run"}}
        _validate_development_budget(requirements_pending, requirements_pending)
        malformed = {"resource_budgets": {**budgets, "aggregate_generated_tokens": 0}}
        with self.assertRaises(FrozenManifestError):
            _validate_development_budget(malformed, malformed)

    def test_source_inventory_rejects_mutated_and_added_code_before_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            code = root / "e4core" / "adapter.py"
            code.parent.mkdir()
            code.write_text("frozen", encoding="utf-8")
            pins = _manifest_files(root)
            _validate_local_file_inventory(root, pins)
            code.write_text("mutated", encoding="utf-8")
            with self.assertRaises(FrozenManifestError):
                _validate_local_file_inventory(root, pins)
            code.write_text("frozen", encoding="utf-8")
            (code.parent / "new_adapter.py").write_text("added", encoding="utf-8")
            with self.assertRaises(FrozenManifestError):
                _validate_local_file_inventory(root, pins)

    def test_public_case_clip_or_alert_mutation_fails_frozen_pin(self):
        class View:
            case_id = "case-a"
            public_manifest_sha256 = "manifest"
            input_manifest_sha256 = "input"
            alert_sha256 = "alert"
            telemetry_file_sha256 = {"clip.csv": "clip"}

        frozen = {"public_manifest_sha256": "manifest", "input_manifest_sha256": "input",
                  "alert_sha256": "alert", "clip_sha256": {"clip.csv": "clip"}}
        _validate_public_view_pin("case-a", View(), frozen)
        frozen["clip_sha256"] = {"clip.csv": "mutated"}
        with self.assertRaises(FrozenManifestError):
            _validate_public_view_pin("case-a", View(), frozen)

    def test_initial_library_must_be_parentless_schema_only_and_unused(self):
        provenance = {"bank_input_manifest_used": False, "implementation_fingerprint": "fp",
            "profile_sha256": "profile", "m4_runtime_policy_sha256": "runtime",
            "bank_metric_policy_sha256": "metric", "bank_kpi_policy_sha256": "kpi",
            "bank_seed_library_sha256": "seeds", "schema_context_sha256": "a" * 64}
        snapshot = type("Snapshot", (), {"parent_snapshot_id": None,
            "document": {"provenance": provenance,
                         "lineage": [{"event": "snapshot_created", "parent_snapshot_id": None}],
                         "operators": [{"invoked_case_ids": []}],
                         "playbooks": [{"invoked_case_ids": []}]}})()
        _validate_pristine_bootstrap(snapshot, implementation_fp="fp", profile_sha256="profile",
            runtime_policy_sha256="runtime", metric_policy_sha256="metric",
            kpi_policy_sha256="kpi", seed_library_sha256="seeds")
        snapshot.parent_snapshot_id = "development-child"
        with self.assertRaises(FrozenManifestError):
            _validate_pristine_bootstrap(snapshot, implementation_fp="fp", profile_sha256="profile",
                runtime_policy_sha256="runtime", metric_policy_sha256="metric",
                kpi_policy_sha256="kpi", seed_library_sha256="seeds")
        snapshot.parent_snapshot_id = None
        snapshot.document["operators"][0]["invoked_case_ids"] = ["bank-dev"]
        with self.assertRaises(FrozenManifestError):
            _validate_pristine_bootstrap(snapshot, implementation_fp="fp", profile_sha256="profile",
                runtime_policy_sha256="runtime", metric_policy_sha256="metric",
                kpi_policy_sha256="kpi", seed_library_sha256="seeds")


if __name__ == "__main__":
    unittest.main()
