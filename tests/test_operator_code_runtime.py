from __future__ import annotations

import json
import math
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from e4core.contracts import Candidate, CandidateSet, EntityRef, MetricPoint
from e4core.m4_fixtures import FIXTURE_CAUSE_ID, FIXTURE_CONTROL_ID, synthetic_bank_telemetry
from e4core.m4_typecheck import (OPERATOR_CODE_PROFILE, TypedAuthoringError,
    check_generated_spec_types)
from e4core.operator_code_api import (prepare_operator_code_inputs,
    typed_operator_code_api)
from e4core.sandbox import run_generated_operator


def _spec(source: str, ports: list[dict]) -> dict:
    return {"authoring_profile": OPERATOR_CODE_PROFILE, "source": source,
        "input_ports": ports, "parameters": {}, "output_type": "ScoredCandidates"}


class TypedOperatorCodeRuntimeTests(unittest.TestCase):
    def test_registry_has_closed_scalar_fields_and_full_window_aggregation(self):
        api = typed_operator_code_api()
        names = {row["name"] for row in api["helpers"]}
        self.assertIn("aggregate_records", names)
        self.assertIn("log_records", names)
        self.assertEqual(api["source_limit_bytes"], 8192)
        self.assertNotIn("success", api["record_fields"]["SpanView"])
        self.assertIn("duration_value", api["record_fields"]["SpanView"])
        self.assertIn("origin_row_id", api["record_fields"]["MetricPointView"])

    def test_input_transport_handles_same_case_aliases_variadics_and_zero_telemetry(self):
        telemetry = synthetic_bank_telemetry()
        candidates = CandidateSet((Candidate(EntityRef("component", FIXTURE_CAUSE_ID)),), "fixture")
        ports = [{"name": "data", "variadic": False},
            {"name": "data_alias", "variadic": False},
            {"name": "candidates", "variadic": True}]
        with prepare_operator_code_inputs({"data": telemetry, "data_alias": telemetry,
                "candidates": (candidates,)}, input_ports=ports) as prepared:
            self.assertEqual(prepared.wire_inputs["data"], prepared.wire_inputs["data_alias"])
            self.assertEqual(prepared.wire_inputs["data"]["$type"], "TelemetryHandle")
            self.assertEqual([row["$type"] for row in prepared.wire_inputs["candidates"]], ["CandidateSet"])
            self.assertEqual(sorted(path.name for path in prepared.case_dir.iterdir()),
                ["logs.jsonl", "metrics.jsonl", "operator_code_manifest.json", "spans.jsonl"])
        with prepare_operator_code_inputs({"candidates": (candidates,)},
                input_ports=[{"name": "candidates", "variadic": True}]) as prepared:
            self.assertIsNone(prepared.case_dir)
            self.assertEqual(prepared.wire_inputs["candidates"][0]["$type"], "CandidateSet")

    def test_hash_verified_bank_log_rows_are_present_with_exact_source_fields(self):
        from e4core.paper_starter_fixtures import _write_bank_log_fixture
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            telemetry = _write_bank_log_fixture(root, scenario="positive")
            public_root = root / "data/prepared/bank/public"
            with prepare_operator_code_inputs({"data": telemetry}, public_data_root=public_root) as prepared:
                manifest = json.loads((prepared.case_dir / "operator_code_manifest.json").read_text())
                rows = [json.loads(line) for line in (prepared.case_dir / "logs.jsonl").read_text().splitlines()]
            self.assertEqual(manifest["record_sources"]["log"], ["bank/log/log_service.csv"])
            self.assertEqual(len(rows), manifest["log_count"])
            self.assertGreater(len(rows), 0)
            self.assertEqual(set(rows[0]), {"source_table", "timestamp_ms_utc", "source_ref",
                "log_id", "entity_role", "entity_identifier", "log_name", "value"})
            self.assertEqual(rows[0]["source_table"], "bank/log/log_service.csv")
            self.assertIsInstance(rows[0]["source_ref"], str)

    def test_metric_app_field_points_keep_an_opaque_shared_origin_row_id(self):
        telemetry = synthetic_bank_telemetry()
        timestamp = telemetry.reference_window.start_ms_utc
        entity = EntityRef("app_kpi", "same-kpi")
        points = (
            MetricPoint("bank/metric_app.csv", timestamp, entity, "rr", "bank.metric_app", "rr", 0.9, "0.9", "source:10:rr"),
            MetricPoint("bank/metric_app.csv", timestamp, entity, "cnt", "bank.metric_app", "cnt", 10.0, "10", "source:10:cnt"),
            MetricPoint("bank/metric_app.csv", timestamp, entity, "rr", "bank.metric_app", "rr", 0.7, "0.7", "source:11:rr"),
        )
        telemetry = replace(telemetry, metrics=telemetry.metrics + points)
        with prepare_operator_code_inputs({"data": telemetry}) as prepared:
            rows = [json.loads(line) for line in (prepared.case_dir / "metrics.jsonl").read_text().splitlines()]
        selected = [row for row in rows if row["source_table"] == "bank/metric_app.csv"]
        self.assertEqual(len(selected), 3)
        self.assertEqual(selected[0]["origin_row_id"], selected[1]["origin_row_id"])
        self.assertNotEqual(selected[0]["origin_row_id"], selected[2]["origin_row_id"])
        self.assertNotIn("source:10", json.dumps(selected))

    def test_container_source_field_and_metric_key_are_distinct_selectors(self):
        import e4core.operator_code_api as api_module
        import e4core.sandbox_worker as worker
        telemetry = synthetic_bank_telemetry()
        timestamp = telemetry.alert_window.start_ms_utc
        point = MetricPoint("bank/metric_container.csv", timestamp,
            EntityRef("component", "fixture_pod_alpha"), "CPU_util_pct",
            "bank.metric_container", "value", 91.0, "91", "source:42")
        telemetry = replace(telemetry, metrics=telemetry.metrics + (point,))
        with prepare_operator_code_inputs({"data": telemetry}) as prepared:
            old_root, old_module = worker.OPERATOR_CODE_STORE_ROOT, worker._OPERATOR_CODE_API_MODULE
            worker.OPERATOR_CODE_STORE_ROOT = prepared.case_dir
            worker._OPERATOR_CODE_API_MODULE = api_module
            try:
                view = worker.telemetry_view(prepared.wire_inputs["data"])
                result = worker.metric_points(view, "bank/metric_container.csv",
                    view.alert_window, source_field="value", metric_key="CPU_util_pct")
                self.assertEqual(len(result.rows), 1)
                self.assertEqual(result.rows[0].source_field, "value")
                self.assertEqual(result.rows[0].metric_key, "CPU_util_pct")
            finally:
                worker.OPERATOR_CODE_STORE_ROOT = old_root
                worker._OPERATOR_CODE_API_MODULE = old_module

    def test_worker_generic_full_window_quantiles_and_group_pagination_match_fixture(self):
        import e4core.operator_code_api as api_module
        import e4core.sandbox_worker as worker
        telemetry = synthetic_bank_telemetry()
        with prepare_operator_code_inputs({"data": telemetry}) as prepared:
            old_root, old_module = worker.OPERATOR_CODE_STORE_ROOT, worker._OPERATOR_CODE_API_MODULE
            worker.OPERATOR_CODE_STORE_ROOT = prepared.case_dir
            worker._OPERATOR_CODE_API_MODULE = api_module
            try:
                view = worker.telemetry_view(prepared.wire_inputs["data"])
                query = worker.aggregate_records(view, "bank/metric_container.csv",
                    view.alert_window, "metric", "value", "quantile",
                    group_by=("entity_identifier",), quantile=0.5)
                self.assertTrue(query.coverage.complete)
                self.assertEqual(query.coverage.support, 60)
                self.assertEqual(query.coverage.total_matches, 2)
                for group in query.rows:
                    expected = sorted(point.value for point in telemetry.metrics
                        if point.table == "bank/metric_container.csv" and
                        point.entity.identifier == group.key[0] and
                        telemetry.alert_window.contains(point.timestamp_ms_utc))
                    position = 0.5 * (len(expected) - 1)
                    lower, upper = math.floor(position), math.ceil(position)
                    expected_q = expected[lower] + (expected[upper] - expected[lower]) * (position - lower)
                    self.assertEqual(group.value, expected_q)
                    self.assertEqual(group.support, len(expected))
                mean_query = worker.aggregate_records(view, "bank/metric_container.csv",
                    view.alert_window, "metric", "value", "mean",
                    group_by=("entity_identifier",))
                count_query = worker.aggregate_records(view, "bank/metric_container.csv",
                    view.alert_window, "metric", "value", "count",
                    group_by=("entity_identifier",))
                for mean_group, count_group in zip(mean_query.rows, count_query.rows):
                    values = [point.value for point in telemetry.metrics
                        if point.table == "bank/metric_container.csv" and
                        point.entity.identifier == mean_group.key[0] and
                        telemetry.alert_window.contains(point.timestamp_ms_utc)]
                    self.assertEqual(mean_group.value, sum(values) / len(values))
                    self.assertEqual(count_group.value, len(values))
                buckets = worker.aggregate_records(view, "bank/metric_container.csv",
                    view.alert_window, "metric", "value", "mean",
                    group_by=("entity_identifier",), time_bucket_ms=600_000)
                self.assertEqual(buckets.coverage.total_matches, 6)
                self.assertEqual(buckets.coverage.support, 60)
                missing_telemetry = synthetic_bank_telemetry(scenario="missing")
                with prepare_operator_code_inputs({"data": missing_telemetry}) as missing_prepared:
                    worker.OPERATOR_CODE_STORE_ROOT = missing_prepared.case_dir
                    missing_view = worker.telemetry_view(missing_prepared.wire_inputs["data"])
                    missing_query = worker.aggregate_records(missing_view,
                        "bank/metric_container.csv", missing_view.alert_window, "metric",
                        "value", "mean", group_by=("entity_identifier",))
                    self.assertTrue(missing_query.coverage.complete)
                    for group in missing_query.rows:
                        self.assertIsNone(group.value)
                        self.assertEqual(group.support, 30)
                        self.assertEqual(group.missing_count, 30)
                        self.assertEqual(group.nonfinite_count, 0)
                worker.OPERATOR_CODE_STORE_ROOT = prepared.case_dir
                paged = worker.aggregate_records(view, "bank/metric_container.csv",
                    view.alert_window, "metric", "value", "mean",
                    group_by=("entity_identifier",), limit=1)
                self.assertFalse(paged.coverage.complete)
                self.assertTrue(paged.coverage.truncated)
                self.assertEqual(paged.coverage.next_offset, 1)
                self.assertEqual(paged.coverage.support, 60)
                raw = worker.metric_points(view, "bank/metric_container.csv",
                    view.alert_window, limit=1)
                self.assertFalse(raw.coverage.complete)
                self.assertTrue(raw.coverage.truncated)
                self.assertEqual(raw.coverage.next_offset, 1)
            finally:
                worker.OPERATOR_CODE_STORE_ROOT = old_root
                worker._OPERATOR_CODE_API_MODULE = old_module

    def test_empty_supported_log_source_is_distinct_from_unavailable_log_source(self):
        import hashlib
        import e4core.operator_code_api as api_module
        import e4core.sandbox_worker as worker
        base = synthetic_bank_telemetry()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public_root = root / "data/prepared/bank/public"
            relative = "source_dates/2099_01_01/log/log_service.csv"
            log_path = public_root / "cases" / base.case_id / relative
            log_path.parent.mkdir(parents=True)
            log_path.write_text("log_id,timestamp,cmdb_id,log_name,value\n")
            digest = hashlib.sha256(log_path.read_bytes()).hexdigest()
            supported = replace(base, file_sha256={relative: digest})
            with prepare_operator_code_inputs({"data": supported}, public_data_root=public_root) as prepared:
                old_root, old_module = worker.OPERATOR_CODE_STORE_ROOT, worker._OPERATOR_CODE_API_MODULE
                worker.OPERATOR_CODE_STORE_ROOT = prepared.case_dir
                worker._OPERATOR_CODE_API_MODULE = api_module
                try:
                    view = worker.telemetry_view(prepared.wire_inputs["data"])
                    empty = worker.log_records(view, "bank/log/log_service.csv", view.alert_window)
                    self.assertTrue(empty.coverage.complete)
                    self.assertEqual(empty.coverage.support, 0)
                    self.assertEqual(empty.rows, ())
                finally:
                    worker.OPERATOR_CODE_STORE_ROOT = old_root
                    worker._OPERATOR_CODE_API_MODULE = old_module
        with prepare_operator_code_inputs({"data": base}) as prepared:
            old_root, old_module = worker.OPERATOR_CODE_STORE_ROOT, worker._OPERATOR_CODE_API_MODULE
            worker.OPERATOR_CODE_STORE_ROOT = prepared.case_dir
            worker._OPERATOR_CODE_API_MODULE = api_module
            try:
                view = worker.telemetry_view(prepared.wire_inputs["data"])
                with self.assertRaisesRegex(ValueError, "record type is unavailable"):
                    worker.log_records(view, "bank/log/log_service.csv", view.alert_window)
            finally:
                worker.OPERATOR_CODE_STORE_ROOT = old_root
                worker._OPERATOR_CODE_API_MODULE = old_module

    def test_static_api_rejects_success_unknown_columns_sources_and_statistics(self):
        ports = [{"name": "data", "type": "Telemetry", "required": True, "variadic": False}]
        prefix = '''def operator(inputs, params, runtime):
    telemetry = telemetry_view(inputs["data"])
'''
        suffix = '\n    return make_scored_candidates([], "typed-operator-code-v1", status="abstained")\n'
        rejected = [
            prefix + '    rows = span_records(telemetry, "bank/trace_span.csv", telemetry.alert_window)\n    value = rows.rows[0].success' + suffix,
            prefix + '    rows = aggregate_records(telemetry, "bad.csv", telemetry.alert_window, "span", "duration_value", "mean")' + suffix,
            prefix + '    rows = aggregate_records(telemetry, "bank/trace_span.csv", telemetry.alert_window, "span", "unknown_column", "mean")' + suffix,
            prefix + '    rows = aggregate_records(telemetry, "bank/trace_span.csv", telemetry.alert_window, "span", "duration_value", "cusum")' + suffix,
            prefix + '    rows = metric_points(telemetry, "bank/metric_container.csv", telemetry.alert_window, source_field="CPU_util_pct")' + suffix,
            prefix + '    return Output(**inputs["data"])\n',
            'import os\n' + prefix + '    value = open("/tmp/x")' + suffix,
            'import os\n' + prefix + '    value = eval("1+1")' + suffix,
            prefix + '    return "not a ScoredCandidates value"\n',
        ]
        for source in rejected:
            with self.subTest(source=source):
                with self.assertRaises(TypedAuthoringError):
                    check_generated_spec_types(_spec(source, ports))
        from e4core.sandbox_worker import validate_source
        with self.assertRaisesRegex(ValueError, "only as a type annotation"):
            validate_source('def operator(inputs, params, runtime):\n    return TelemetryHandle\n',
                OPERATOR_CODE_PROFILE)

    def test_code_source_cap_is_profile_specific(self):
        source = "def operator(inputs, params, runtime):\n    return make_scored_candidates([], 'typed-operator-code-v1')\n" + ("#" + "x" * 8192)
        with self.assertRaises(TypedAuthoringError) as raised:
            check_generated_spec_types(_spec(source, []))
        self.assertEqual(raised.exception.code, "typed_source_error")

    def test_full_window_aggregate_fixture_executes_as_authored_python_in_bwrap(self):
        source = '''def operator(inputs, params, runtime):
    telemetry = telemetry_view(inputs["data"])
    reference = metric_points(telemetry, "bank/metric_container.csv", telemetry.reference_window)
    alert = metric_points(telemetry, "bank/metric_container.csv", telemetry.alert_window)
    candidates = candidate_views(inputs["candidates"])
    window = make_window(telemetry.alert_window.name,
        telemetry.alert_window.start_ms_utc, telemetry.alert_window.end_ms_utc)
    scored = []
    for candidate in candidates:
        reference_sum = 0.0
        reference_count = 0
        for row in reference.rows:
            if row.entity_identifier == candidate.entity.identifier and row.value is not None:
                reference_sum += float(row.value)
                reference_count += 1
        if reference_count == 0:
            continue
        baseline = reference_sum / reference_count
        cumulative = 0.0
        peak = 0.0
        support = 0
        for row in alert.rows:
            if row.entity_identifier == candidate.entity.identifier and row.value is not None:
                cumulative = max(0.0, cumulative + float(row.value) - baseline)
                peak = max(peak, cumulative)
                support += 1
        if support:
            entity = make_entity_ref(candidate.entity.role, candidate.entity.identifier)
            evidence = make_evidence("bank/metric_container.csv", window, "cusum_peak",
                peak, [entity], support, True,
                {"ranking_eligible": True, "source_tables": ["bank/metric_container.csv"],
                 "support_unit": "records"})
            scored.append(make_scored_candidate(entity, peak, None, [evidence]))
    return make_scored_candidates(scored, "typed-operator-code-v1",
        status="ready" if scored else "abstained")
'''
        ports = [{"name": "data", "type": "Telemetry", "required": True, "variadic": False},
            {"name": "candidates", "type": "CandidateSet", "required": True, "variadic": False}]
        spec = _spec(source, ports)
        check_generated_spec_types(spec)
        telemetry = synthetic_bank_telemetry()
        candidates = CandidateSet(tuple(Candidate(EntityRef("component", identifier))
            for identifier in (FIXTURE_CAUSE_ID, FIXTURE_CONTROL_ID)), "fixture")
        with prepare_operator_code_inputs({"data": telemetry, "candidates": candidates},
                input_ports=ports) as prepared:
            result = run_generated_operator(source, prepared.wire_inputs, {}, {}, "ScoredCandidates",
                authoring_profile=OPERATOR_CODE_PROFILE, typecheck_spec=spec,
                case_dir=prepared.case_dir)
        if not result.get("ok") and "RTM_NEWADDR" in result.get("stderr", ""):
            self.skipTest("the local bubblewrap namespace cannot create its network namespace")
        if not result.get("ok") and result.get("error", {}).get("code") == "sandbox_unavailable":
            self.skipTest("the local container lacks the Python 3.10 bubblewrap runtime")
        self.assertTrue(result.get("ok"), result)
        envelope = result["result"]
        self.assertEqual(envelope["$type"], "ScoredCandidates")
        self.assertGreater(len(envelope["$fields"]["candidates"]["$tuple"]), 0)


if __name__ == "__main__":
    unittest.main()
