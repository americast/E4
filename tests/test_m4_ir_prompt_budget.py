"""Prompt-size regression checks for the source-free Stage 3 evolve call."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from e4core.m4_capabilities import input_producer_capability_schema
from e4core.m4_composite import trusted_aggregate_semantics
from e4core.m4_controller import (_compact_evolution_case_digest, _policy,
    _stage_prompt, _typed_ir_input_capabilities, build_m4_runtime)
from e4core.m4_data import M4QueryBudget, make_m4_registry
from e4core.m4_library import generated_specs_from_snapshot
from tests.test_m4_controller import _fixture_root, _initial_library


class TypedIREvolutionPromptBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        _fixture_root(self.root)
        self.snapshot = _initial_library(self.root, self.root / "source_library")
        self.registry = make_m4_registry(None,
            generated_specs_from_snapshot(self.snapshot), root=self.root)
        self.policy = _policy(self.root)
        self.runtime = build_m4_runtime({
            "analysis_window_start_epoch_ms_utc": 1_700_000_000_000,
            "alert_window_start_epoch_ms_utc": 1_700_001_800_000,
            "alert_window_end_epoch_ms_utc": 1_700_003_600_000}, root=self.root)
        self.query_budget = M4QueryBudget(None)

    def _args(self, history):
        return {"digest": {}, "registry": self.registry, "snapshot": self.snapshot,
            "runtime": self.runtime, "history": history, "policy": self.policy,
            "query_budget": self.query_budget}

    def test_evolve_keeps_necessary_context_once_and_drops_legacy_payloads(self):
        history = [
            {"stage": "stage2_compose", "status": "invalid_answer", "accepted": False,
             "ranked_answer": {"leads": [{"component_id": "case-local-cause-a", "score": 0.82}]},
             "plan_summary": {"dag_sha256": "a" * 64, "full_summary_sha256": "b" * 64,
                 "nodes": [{"id": "score_a", "operator": "bank.metrics.contrast",
                     "version": "1", "inputs": {"reference": "ref_a", "alert": "alt_a"},
                     "params": {}, "bindings": {}}], "return": {"node": "answer_a"},
                 "runtime_overrides": {}}},
            {"stage": "stage2_synthesize", "status": "invalid_answer", "accepted": False,
             "ranked_answer": {"leads": [{"component_id": "case-local-cause-b", "score": 0.73}]},
             "plan_summary": {"dag_sha256": "c" * 64, "full_summary_sha256": "d" * 64,
                 "nodes": [{"id": "score_b", "operator": "bank.traces.latency_contrast",
                     "version": "1", "inputs": {"reference": "ref_b", "alert": "alt_b"},
                     "params": {}, "bindings": {}}], "return": {"node": "answer_b"},
                 "runtime_overrides": {}}},
        ]
        prompt = _stage_prompt("evolve", **self._args(history))

        # The common section is a lossless default-compressed catalog, so the
        # duplicated full JSON signature dump is unnecessary.
        self.assertEqual(prompt.count("REGISTERED OPERATOR SIGNATURES"), 1)
        self.assertNotIn("CURRENT REGISTERED SIGNATURES", prompt)
        for signature in self.registry.signatures():
            self.assertIn(signature["name"], prompt)
            self.assertIn(signature["version"], prompt)
            if signature.get("description"):
                self.assertIn(signature["description"], prompt)

        from e4core.m4_operator_ir import typed_operator_ir_schema
        ir_schema = typed_operator_ir_schema()
        self.assertIn("PRIVATE TYPED-OPERATOR-IR SCHEMA AND KERNEL CATALOG", prompt)
        self.assertNotIn("input_ports", ir_schema["spec_fields"])
        self.assertNotIn("input_requirements", ir_schema["spec_fields"])
        self.assertIn("input_ports", ir_schema["fixed_host_abi"])
        self.assertIn("input_requirements", ir_schema["fixed_host_abi"])
        self.assertIn("do not emit outer-spec `inputs`, `input_ports`, or `input_requirements`", prompt)
        for kernel in ir_schema["kernels"]:
            self.assertIn(kernel, prompt)
        trusted_inputs = _typed_ir_input_capabilities(self.registry)
        for field in trusted_inputs["component_metrics"]["approved_source_fields"]:
            self.assertIn(field, prompt)
        self.assertIn("bucket_samples", prompt)
        self.assertIn("case_data_sha256", prompt)
        self.assertIn("Aggregate.rows is empty", prompt)
        self.assertIn("row_requirements", prompt)
        self.assertIn("exactly two sibling fields", prompt)
        self.assertIn("`operators` MUST be a sibling of `gap_record`, never a field inside it", prompt)
        self.assertIn("`kernel` is the bare kernel name without `@version`", prompt)
        self.assertIn("kernel:'series.primary_kpi',version:'1'", prompt)
        self.assertIn('"kernel":"series.correlate","version":"1"', prompt)
        self.assertIn('"minimum_pairs":"runtime.minimum_support"', prompt)
        self.assertIn("omit it from `params`", prompt)
        # This is the legacy V1 prompt. The V2 gap-draft wording stays scoped
        # to the paper-starter profile.
        self.assertNotIn("MODEL-FACING GAP DRAFT", prompt)

        # Both rejected plans, their rankings, and binary feedback stay visible.
        self.assertIn("stage2_compose", prompt)
        self.assertIn("stage2_synthesize", prompt)
        self.assertIn("case-local-cause-a", prompt)
        self.assertIn("case-local-cause-b", prompt)
        self.assertIn('"accepted":false', prompt)
        self.assertIn("bank.metrics.contrast", prompt)
        self.assertIn("bank.traces.latency_contrast", prompt)

        # Stage-3's JSON contract and policy budgets remain in full.
        self.assertIn("maximum_public_query_requests_per_case", prompt)
        self.assertIn("maximum_operator_input_bytes", prompt)
        self.assertIn("generated_operator_cpu_seconds", prompt)
        self.assertIn("CURRENT ALLOWLISTED RUNTIME VALUES", prompt)
        self.assertIn("RUNTIME OVERRIDE POLICY", prompt)
        self.assertNotIn('"contract_fields"', prompt)
        self.assertNotIn('"native_tables"', prompt)
        self.assertNotIn('"coverage_metadata"', prompt)

    def test_compaction_saves_legacy_operator_catalog_and_wire_schema_payload(self):
        compact = _typed_ir_input_capabilities(self.registry)
        old_payloads = (
            self.registry.signatures(),
            trusted_aggregate_semantics(self.registry),
            input_producer_capability_schema(self.registry),
        )
        dump = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"))
        old_bytes = sum(len(dump(value).encode("utf-8")) for value in old_payloads)
        new_bytes = len(dump(compact).encode("utf-8"))
        # This is the removed stage-specific payload. The shared catalog still
        # appears once in _stage_prompt's defaults-compressed common section.
        self.assertGreater(old_bytes - new_bytes, 9_000)

    def test_evolve_digest_compaction_preserves_context_and_omission_provenance(self):
        digest = {
            "case_id": "case-local-synthetic", "timezone": "America/New_York",
            "reference_window_utc_ms": [1000, 2000], "alert_window_utc_ms": [2000, 3000],
            "source_timestamp_units_by_date": {"2021_03_04": "seconds"},
            "exact_observed_component_ids": [f"opaque-component-{i}" for i in range(18)],
            "available_metric_fields": [f"approved-field-{i}" for i in range(342)],
            "primary_app_kpi_context": {"kpi_id": "metric_app.mrt", "tc_id": "opaque-tc",
                "policy_sha256": "a" * 64, "selection_reason": "support-first",
                "paired_support": 30},
            "primary_app_kpi_policy_sha256": "a" * 64,
            "component_identity_coverage": {"truncated": False},
            "quality_counts": {"invalid_metric_rows": 2, "duplicate_rows": 3},
            "log_summary": {"total": 17, "truncated": False},
            "metric_digest_selection_policy": "fixed deterministic selection",
            "metric_series_limit": 4, "metric_series_omitted": 38,
            "app_kpi_context_series_omitted": 1, "trace_series_limit": 4,
            "trace_series_omitted": 8, "log_volume_series_limit": 4,
            "log_volume_series_omitted": 10, "digest_sha256": "b" * 64,
            "digest_bytes": 32_000,
            "file_sha256": {f"source-{i}": "c" * 64 for i in range(10)},
            "file_bytes": {f"source-{i}": 1_000_000 for i in range(10)},
            "metric_series": [{"samples": [[stamp, float(stamp)] for stamp in range(300)],
                               "source_field": f"metric-{i}"} for i in range(4)],
            "app_kpi_context_series": [{"samples": [[stamp, float(stamp)] for stamp in range(300)]}],
            "log_volume_series": [{"samples": [[stamp, 1] for stamp in range(300)]}],
            "trace_series": [{"trace_values": [[f"trace-{stamp}", 1] for stamp in range(300)]}],
            "log_text_excerpts": [{"text": "raw excerpt " * 1000}],
        }
        compact = _compact_evolution_case_digest(digest)
        dump = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"))
        self.assertGreater(len(dump(digest)) - len(dump(compact)), 15_000)
        for key in ("case_id", "timezone", "reference_window_utc_ms", "alert_window_utc_ms",
                    "source_timestamp_units_by_date", "exact_observed_component_ids",
                    "available_metric_fields", "primary_app_kpi_context",
                    "primary_app_kpi_policy_sha256", "component_identity_coverage",
                    "quality_counts", "log_summary", "metric_series_omitted",
                    "trace_series_omitted", "log_volume_series_omitted", "digest_sha256",
                    "digest_bytes", "metric_series_limit", "trace_series_limit"):
            self.assertEqual(compact[key], digest[key])
        self.assertEqual(compact["omitted_for_evolution"], sorted({
            "file_sha256", "file_bytes", "metric_series", "app_kpi_context_series",
            "log_volume_series", "trace_series", "log_text_excerpts"}))
        self.assertIn("later serving phase receives the full current case digest",
                      compact["evolution_digest_semantics"])

    def test_only_evolve_uses_compact_digest(self):
        full_digest = {"case_id": "case-local", "metric_series": [{"sentinel": "full-detail"}]}
        evolve = _stage_prompt("evolve", **{**self._args([]), "digest": full_digest})
        serve_args = {**self._args([]), "digest": full_digest,
                      "gap_record": {"schema_version": "e4-capability-gaps/1"}}
        serve = _stage_prompt("stage3_serve", **serve_args)
        self.assertIn("full-detail", serve)
        self.assertNotIn("full-detail", evolve)
        self.assertIn("omitted_for_evolution", evolve)

    def test_select_compose_and_stage3_serve_keep_their_existing_context(self):
        args = self._args([])
        select = _stage_prompt("select", **args)
        compose = _stage_prompt("compose", **args)
        serve = _stage_prompt("stage3_serve", gap_record={
            "schema_version": "e4-capability-gaps/1"}, **args)
        self.assertIn("CURRENT STORED PLAYBOOKS", select)
        self.assertIn("TRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS", compose)
        self.assertIn("bank_public_raw_page", compose)
        self.assertIn("TRUSTED AGGREGATE PRODUCER/CONSUMER SEMANTICS", serve)
        self.assertIn("TRUSTED INPUT PRODUCER CAPABILITIES", serve)

