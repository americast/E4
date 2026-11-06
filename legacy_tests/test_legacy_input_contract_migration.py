"""Static Bank producer/consumer contract tests for legacy generated operators."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from e4core.dag import VerifiedDAG, VerifiedNode
from e4core.bank_adapter import sha256_file
from e4core.cache import implementation_fingerprint
from e4core.m4_capabilities import InputCapabilityError, check_typed_input_capabilities
from e4core.m4_data import make_m4_registry
from e4core.m4_library import generated_specs_from_snapshot, load_library_snapshot
from e4core.m4_controller import (_materialize_seeds_for_registry, _plain, _policy,
    _stage_prompt, build_m4_runtime, run_diagnostic)
from e4core.m4_library import create_library_snapshot
from e4core.m4_library import playbook_graph_hash
from e4core.m4_generated import register_generated_specs, validate_generated_spec
from tests.test_m4_controller import FakeClient, FakeFeedback, _fixture_root
from tests.test_legacy_log_contracts import REPAIRED_SOURCE
from scripts.migrate_log_scorer_input_contract import (LOG_INPUT_REQUIREMENTS,
    TARGET_NAME, migrate)


ROOT = Path(__file__).resolve().parents[1]
APPROVED_SOURCE_SHA256 = "6434d4312587714d9146019b1c3961fde47602c5a04f85f86646eac31e60a679"


def _legacy_raw_spec():
    return {"name": TARGET_NAME, "version": "1",
        "description": "Scores candidates based on the frequency of a specific text pattern within raw log values for the alert window.",
        "family": "bank_log_content",
        "input_ports": [{"name": "candidates", "type": "CandidateSet"},
            {"name": "logs", "type": "Aggregate"}],
        "output_type": "ScoredCandidates",
        "parameters": {"pattern": {"type": "string", "required": True,
            "description": "The substring pattern to search for in the log value column."}},
        "source": REPAIRED_SOURCE}


def _source_library(root: Path, directory: Path):
    raw = _legacy_raw_spec()
    registry = make_m4_registry(None, [], root=root)
    normalized = validate_generated_spec(raw)
    versions = {row["name"]: row["version"] for row in registry.signatures()}
    document = {"schema_version": "e4-dag/1", "nodes": [
        {"id": "data", "operator": "case.telemetry", "version": versions["case.telemetry"],
            "inputs": {}, "params": {}, "bindings": {}},
        {"id": "logs", "operator": "bank.public.query", "version": versions["bank.public.query"],
            "inputs": {"data": "data"}, "params": {"table": "log/log_service.csv",
                "window": "alert", "fields": ["cmdb_id", "value"], "offset": 0, "limit": 64}, "bindings": {}},
        {"id": "candidates", "operator": "bank.candidates.generate", "version": versions["bank.candidates.generate"],
            "inputs": {"data": "data"}, "params": {}, "bindings": {}},
        {"id": "scores", "operator": TARGET_NAME, "version": normalized["version"],
            "inputs": {"logs": "logs", "candidates": "candidates"},
            "params": {"pattern": "error"}, "bindings": {}},
        {"id": "answer", "operator": "bank.answers.combine", "version": versions["bank.answers.combine"],
            "inputs": {"scores": ["scores"]}, "params": {"limit": 5}, "bindings": {}}],
        "return": {"node": "answer"}}
    seed_library = json.loads((root / "config/bank_seed_library.json").read_text(encoding="utf-8"))
    seed_docs = _materialize_seeds_for_registry(seed_library, registry)
    provenance = {"implementation_fingerprint": implementation_fingerprint(),
        "m4_runtime_policy_sha256": sha256_file(root / "config/m4_runtime.json"),
        "bank_metric_policy_sha256": sha256_file(root / "config/bank_metric_kind_policy.json"),
        "bank_kpi_policy_sha256": sha256_file(root / "config/bank_dev_kpi_policy.json"),
        "bank_seed_library_sha256": sha256_file(root / "config/bank_seed_library.json")}
    return create_library_snapshot(seed_docs, [raw], [{"name": "legacy_log_analysis", "document": document}],
        provenance, directory, builtin_specs=registry.signatures(),
        verified_playbook_graph_hashes=[playbook_graph_hash(document)])


def _migrated_fixture_library(root: Path, source_dir: Path, output_dir: Path):
    source = _source_library(root, source_dir)
    report = migrate(source_library=source.root, output_library=output_dir,
        expected_source_snapshot_id=source.snapshot_id,
        expected_source_sha256=sha256_file(source.path))
    return source, load_library_snapshot(output_dir), report


def _raw_with_requirements(spec):
    raw = dict(spec)
    raw["input_requirements"] = LOG_INPUT_REQUIREMENTS
    return raw


def _verified_log_graph(registry, operator, *, producer="query", table="log/log_service.csv",
                        fields=("cmdb_id", "value"), projected=False):
    data = registry.require("case.telemetry", "1")
    nodes = [VerifiedNode("data", data, {}, {}, {}, 1)]
    if producer == "query":
        op = registry.require("bank.public.query", "1")
        nodes.append(VerifiedNode("logs", op, {"data": ("data",)},
            {"table": table, "window": "alert", "fields": list(fields), "offset": 0, "limit": 64}, {}, 2))
        logs_id = "logs"
    elif producer == "aggregate":
        op = next(registry.require(row["name"], row["version"]) for row in registry.signatures()
            if row["name"] == "bank.metrics.aggregate")
        nodes.append(VerifiedNode("logs", op, {"data": ("data",)}, {"window": "alert"}, {}, 2))
        logs_id = "logs"
    else:
        raise AssertionError(producer)
    if projected:
        op = registry.require("bank.public.project", next(row["version"] for row in registry.signatures()
            if row["name"] == "bank.public.project"))
        nodes.append(VerifiedNode("projected", op, {"data": (logs_id,)},
            {"source_fields": [], "statistics": [], "entities": [], "offset": 0, "limit": 64}, {}, 3))
        logs_id = "projected"
    candidate_op = registry.require("bank.candidates.generate", "1")
    nodes.append(VerifiedNode("candidates", candidate_op, {"data": ("data",)}, {}, {}, len(nodes) + 1))
    scorer = registry.require(operator["name"], operator["version"])
    nodes.append(VerifiedNode("scorer", scorer, {"logs": (logs_id,), "candidates": ("candidates",)},
        {"pattern": "error"}, {}, len(nodes) + 1))
    return VerifiedDAG(tuple(nodes), "scorer", max(row.depth for row in nodes), len(nodes))


class LegacyInputContractMigrationTests(unittest.TestCase):
    def test_migration_is_explicit_content_versioned_and_keeps_old_snapshot_readable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _fixture_root(root)
            old_snapshot, new_snapshot, report = _migrated_fixture_library(root,
                root / "source", root / "migrated")
            old_spec = next(row for row in generated_specs_from_snapshot(old_snapshot)
                if row["name"] == TARGET_NAME)
            old_key = f"{old_spec['name']}@{old_spec['version']}"
            new_specs = generated_specs_from_snapshot(new_snapshot)
            new_spec = next(row for row in new_specs if row["name"] == TARGET_NAME)
            self.assertEqual(report["parent_snapshot_id"], old_snapshot.snapshot_id)
            self.assertEqual(report["old_version"], old_spec["version"])
            self.assertEqual(report["new_version"], new_spec["version"])
            self.assertNotEqual(report["old_version"], report["new_version"])
            self.assertEqual(report["old_source_sha256"], APPROVED_SOURCE_SHA256)
            self.assertEqual(report["new_source_sha256"], APPROVED_SOURCE_SHA256)
            self.assertTrue(report["source_bytes_unchanged"])
            self.assertEqual(report["revalidation_status"], "not_revalidated_after_contract_migration")
            self.assertIsNone(new_spec.get("authoring_profile"))
            self.assertEqual(new_spec["input_requirements"], LOG_INPUT_REQUIREMENTS)
            self.assertIn(old_key, old_snapshot.operators)
            self.assertNotIn(old_key, new_snapshot.operators)
            self.assertIn(f"{TARGET_NAME}@{new_spec['version']}", new_snapshot.operators)
            self.assertEqual(load_library_snapshot(old_snapshot.path).snapshot_id, old_snapshot.snapshot_id)
            rewritten = 0
            for book in new_snapshot.playbooks.values():
                for node in book["document"]["nodes"]:
                    if node.get("operator") == TARGET_NAME:
                        self.assertEqual(node["version"], new_spec["version"])
                        rewritten += 1
            self.assertEqual(rewritten, report["rewritten_playbook_node_references"])
            self.assertGreater(rewritten, 0)
            self.assertEqual(report["rewritten_playbooks"], ["legacy_log_analysis"])
            self.assertTrue(report["unmodified_playbooks_preserved"])
            self.assertEqual(report["unrelated_operator_count"], len(old_snapshot.operators) - 1)

    def test_raw_query_contract_passes_and_wrong_producers_fail_statically(self):
        raw = _raw_with_requirements(_legacy_raw_spec())
        registry = make_m4_registry(None, [], root=ROOT)
        normalized = register_generated_specs(registry, [raw])[0]
        candidates = [("query", "log/log_service.csv", ("cmdb_id", "value"), False, None),
            ("aggregate", "log/log_service.csv", ("cmdb_id", "value"), False,
             "input_source_column_unavailable"),
            ("query", "metric/metric_app.csv", ("cmdb_id", "value"), False,
             "input_source_column_unavailable"),
            ("query", "log/log_service.csv", ("cmdb_id",), False,
             "input_source_column_unavailable"),
            ("query", "log/log_service.csv", ("cmdb_id", "value"), True,
             "input_projection_unsupported")]
        for producer, table, fields, projected, expected in candidates:
            graph = _verified_log_graph(registry, normalized, producer=producer,
                table=table, fields=fields, projected=projected)
            with self.subTest(producer=producer, table=table, fields=fields, projected=projected):
                if expected is None:
                    report = check_typed_input_capabilities(graph, [normalized])
                    logs_report = next(row for row in report if row["port"] == "logs")
                    self.assertEqual(logs_report["status"], "verified")
                    self.assertEqual(logs_report["coverage"], "page")
                else:
                    with self.assertRaises(InputCapabilityError) as raised:
                        check_typed_input_capabilities(graph, [normalized])
                    self.assertEqual(raised.exception.code, expected)

    def test_controller_rejects_wrong_legacy_log_producer_before_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, profile = _fixture_root(root)
            _source, library, _report = _migrated_fixture_library(root,
                root / "source_library", root / "migrated_library")
            specs = generated_specs_from_snapshot(library)
            migrated = next(row for row in specs if row["name"] == TARGET_NAME)
            registry = make_m4_registry(None, specs, root=root)
            versions = {(row["name"], row["version"]): row for row in registry.signatures()}
            selection = {"playbook": "combined_shared_candidates", "runtime_overrides": {},
                "node_parameters": {}, "node_bindings": {}}
            nodes = [
                {"id": "data", "operator": "case.telemetry", "version": "1",
                    "inputs": {}, "params": {}, "bindings": {}},
                {"id": "alert", "operator": "bank.metrics.aggregate",
                    "version": next(k[1] for k in versions if k[0] == "bank.metrics.aggregate"),
                    "inputs": {"data": "data"}, "params": {"window": "alert"}, "bindings": {}},
                {"id": "candidates", "operator": "bank.candidates.generate", "version": "1",
                    "inputs": {"data": "data"}, "params": {}, "bindings": {}},
                {"id": "scores", "operator": migrated["name"], "version": migrated["version"],
                    "inputs": {"logs": "alert", "candidates": "candidates"},
                    "params": {"pattern": "error"}, "bindings": {}},
                {"id": "answer", "operator": "bank.answers.combine",
                    "version": next(k[1] for k in versions if k[0] == "bank.answers.combine"),
                    "inputs": {"scores": ["scores"]}, "params": {"limit": 5}, "bindings": {}}]
            wrong = {"schema_version": "e4-dag/1", "nodes": nodes,
                "return": {"node": "answer"}}
            fallback = _plain(library.playbooks["combined_shared_candidates"]["document"])
            client = FakeClient([json.dumps(selection),
                json.dumps({"dag": wrong, "runtime_overrides": {}}),
                json.dumps({"dag": fallback, "runtime_overrides": {}})])
            result = run_diagnostic(case_id, library_root=library.root,
                run_dir=root / "legacy_contract_wrong_producer", feedback=FakeFeedback([False, True]),
                profile_path=profile, client=client, root=root)
            self.assertTrue(result["result"]["accepted"])
            stage2 = result["result"]["stages"][1]
            self.assertEqual(stage2["static_errors"][0]["code"], "input_source_column_unavailable")
            self.assertEqual(result["result"]["query_budget"]["requests"], 0)
            executed_stage2 = json.loads((root / "legacy_contract_wrong_producer/attempts/stage2_compose/execution.json").read_text())
            executed_operator_names = {row.get("operator") for row in executed_stage2.get("records", [])}
            self.assertNotIn(TARGET_NAME, executed_operator_names)

    def test_prompt_publishes_legacy_requirements_without_typed_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _case_id, _profile = _fixture_root(root)
            _source, snapshot, _report = _migrated_fixture_library(root,
                root / "source_library", root / "migrated_library")
            specs = generated_specs_from_snapshot(snapshot)
            registry = make_m4_registry(None, specs, root=root)
            runtime = build_m4_runtime({"analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
                "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
                "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=root)
            from e4core.m4_data import M4QueryBudget
            prompt = _stage_prompt("compose", digest={}, registry=registry,
                snapshot=snapshot, runtime=runtime, history=[], policy=_policy(root),
                query_budget=M4QueryBudget(None))
            self.assertIn('"input_requirements"', prompt)
            self.assertIn('"log/log_service.csv":["cmdb_id","value"]', prompt)
            self.assertIn('"support_unit":"raw_row"', prompt)
            self.assertIn('"coverage":"page"', prompt)
            self.assertNotIn('"authoring_profile":"typed-records-v1"', prompt)


if __name__ == "__main__":
    unittest.main()
