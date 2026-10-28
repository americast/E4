import shutil
import json
import unittest
from pathlib import Path

from e4.dsl.cache import canonical_json, digest_value
from e4.contracts import Aggregate, AggregateRow, EntityRef, Window
from e4.dsl.executor import NodeContext
from e4.operators.generated import register_generated_specs, validate_generated_spec
from e4.agent.bank.controller import data_operator_contract_schema
from e4.operators.bank.projection import (MAX_OUTPUT_BYTES, BankProjectionError,
                                          PROJECTION_VERSION, project_bank_aggregate,
                                          register_m4_projection)
from e4.dsl.registry import OperatorRegistry


ALERT = Window("alert", 1_000, 2_000)
REFERENCE = Window("reference", 0, 1_000)


def projection_context():
    return NodeContext("fixture", "project", f"bank.public.project@{PROJECTION_VERSION}",
                       {}, {}, REFERENCE, ALERT, "a" * 64)


def summary_row(entity="Mysql02", source_field="cpu-host", statistic="median", *, details=None,
                value=42.5, support=30, source="bank/metric_container.csv"):
    return AggregateRow(entity=EntityRef("component", entity),
        metric_key=f"{source}|{source_field}|value", window=ALERT,
        statistic=statistic, value=value, support=support,
        source_tables=(source,),
        contributing_entities=(EntityRef("component", entity),),
        details={"source_field": source_field, "unit": "undeclared",
                 "metric_kind": "unknown", "ranking_eligible": False,
                 **(details or {})})


def project(value, params=None):
    return project_bank_aggregate({"data": value}, params or {}, projection_context()).value


class BankAggregateProjectionTests(unittest.TestCase):
    def test_published_projection_layout_matches_actual_projected_aggregate(self):
        source = Aggregate(ALERT, (summary_row(details={"samples": (1, 2, 3)}),),
            ("bank/metric_container.csv",), {"source": "fixture", "large": {"drop": True}})
        output = project(source)
        contract = data_operator_contract_schema()["bank.public.project"]["output"]
        self.assertEqual(len(output.rows), 1)
        self.assertEqual(set(contract["metadata_projection_fields"]),
                         set(output.metadata["projection"]))
        self.assertIn("projection_omitted_detail_keys", output.rows[0].details)
        self.assertEqual(output.rows[0].details["projection_omitted_detail_keys"], ("samples",))
        self.assertEqual(output.metadata["projection_omitted_metadata_keys"], ("large",))
        self.assertEqual(output.rows[0].value, source.rows[0].value)
        self.assertEqual(output.rows[0].support, source.rows[0].support)

    def test_large_full_window_buffers_are_omitted_but_summary_contract_and_coverage_survive(self):
        samples = tuple((1_600_000_000_000 + i * 1000, float(i % 17)) for i in range(120_000))
        raw_counts = tuple((timestamp, 3) for timestamp, _ in samples)
        source = Aggregate(ALERT, (
            summary_row(details={"bucket_samples": samples,
                                "raw_rows_by_timestamp": raw_counts,
                                "distinct_time_buckets": len(samples)}),
            summary_row("Tomcat01", "mrt", "tail_quantile", value=15.0, support=24,
                        source="bank/metric_app.csv",
                        details={"source_field": "mrt", "direction": "higher_is_worse"}),
        ), ("bank/metric_container.csv", "bank/metric_app.csv"),
            {"source": "Bank", "window": "alert", "large_metadata": {"private_to_projection": "x" * 4096}})
        source_bytes = len(canonical_json(source))
        self.assertGreater(source_bytes, 2 * 1024 * 1024)

        result = project_bank_aggregate({"data": source}, {"limit": 64}, projection_context())
        output = result.value
        output_bytes = len(canonical_json(output))
        self.assertLess(output_bytes, MAX_OUTPUT_BYTES)
        self.assertEqual(result.rows_processed, 2)
        self.assertEqual(result.bytes_processed, source_bytes + output_bytes)
        self.assertEqual(output.metadata["projection"]["source_aggregate_digest"], digest_value(source))
        self.assertEqual(output.metadata["projection"]["source_serialized_bytes"], source_bytes)
        self.assertEqual(output.metadata["projection"]["projection_serialized_bytes"], output_bytes)
        self.assertTrue(output.metadata["projection"]["complete"])
        self.assertEqual(output.metadata["projection"]["total_matching_row_count"], 2)
        self.assertEqual(output.metadata["projection"]["selected_row_count"], 2)
        self.assertEqual(output.metadata["projection_omitted_metadata_keys"], ("large_metadata",))
        first = output.rows[0]
        self.assertEqual(first.value, 42.5)
        self.assertEqual(first.support, 30)
        self.assertEqual(first.metric_key, source.rows[0].metric_key)
        self.assertEqual(first.source_tables, source.rows[0].source_tables)
        self.assertEqual(first.contributing_entities, source.rows[0].contributing_entities)
        self.assertEqual(first.details["source_field"], "cpu-host")
        self.assertEqual(first.details["unit"], "undeclared")
        self.assertEqual(first.details["distinct_time_buckets"], 120_000)
        self.assertEqual(first.details["projection_omitted_detail_keys"],
                         ("bucket_samples", "raw_rows_by_timestamp"))
        self.assertNotIn("bucket_samples", first.details)

    def test_filters_are_exact_for_native_fields_and_stats_and_casefold_only_for_entities(self):
        source = Aggregate(ALERT, (
            summary_row("Mysql02", "CPU-KPI", "median", value=1),
            summary_row("mysql02", "cpu-kpi", "tail_quantile", value=2),
            summary_row("PodZ", "duration", "per_trace_median", source="bank/trace_span.csv", value=3,
                        details={"duration_unit": "undeclared"}),
        ), ("bank/metric_container.csv", "bank/trace_span.csv"),
            {"duration_unit": "undeclared"})
        selected = project(source, {"source_fields": ["cpu-kpi"], "entities": ["MYSQL02"],
                                    "statistics": ["tail_quantile"], "offset": 0, "limit": 1})
        self.assertEqual(len(selected.rows), 1)
        self.assertEqual(selected.rows[0].entity.identifier, "mysql02")
        self.assertEqual(selected.rows[0].value, 2)
        self.assertEqual(selected.metadata["projection"]["total_matching_row_count"], 1)
        trace = project(source, {"source_fields": ["duration"], "entities": ["podz"],
                                 "statistics": ["per_trace_median"]})
        self.assertEqual([row.metric_key for row in trace.rows], [source.rows[2].metric_key])
        self.assertEqual(trace.rows[0].details["source_field"], "duration")
        self.assertEqual(trace.metadata["duration_unit"], "undeclared")
        self.assertEqual(project(source, {"source_fields": ["CPU-KPI"]}).rows[0].value, 1)
        self.assertEqual(project(source, {"source_fields": ["Cpu-Kpi"]}).rows, ())

    def test_paging_reports_total_before_after_next_offset_and_incomplete_default(self):
        source = Aggregate(ALERT, tuple(summary_row(f"pod-{i:03d}", "mrt", "median", value=float(i))
                                         for i in range(300)), (), {"window": "alert"})
        first = project(source)
        coverage = first.metadata["projection"]
        self.assertEqual(coverage["total_matching_row_count"], 300)
        self.assertEqual(coverage["selected_row_count"], 64)
        self.assertEqual(coverage["omitted_before"], 0)
        self.assertEqual(coverage["remaining_after"], 236)
        self.assertFalse(coverage["complete"])
        self.assertEqual(coverage["next_offset"], 64)
        last = project(source, {"offset": 290, "limit": 64})
        coverage = last.metadata["projection"]
        self.assertEqual(coverage["selected_row_count"], 10)
        self.assertEqual(coverage["omitted_before"], 290)
        self.assertEqual(coverage["remaining_after"], 0)
        self.assertFalse(coverage["complete"])
        self.assertIsNone(coverage["next_offset"])

    def test_invalid_bounds_strings_and_oversized_result_fail_without_clipping(self):
        source = Aggregate(ALERT, (summary_row(),), (), {})
        for params in ({"offset": -1}, {"limit": 257}, {"limit": True},
                       {"entities": ["Mysql02", "mysql02"]}):
            with self.subTest(params=params), self.assertRaises(BankProjectionError):
                project(source, params)
        self.assertEqual(project(source, {"source_fields": ["CPU-HOST"]}).rows, ())
        long_detail = summary_row(details={"explanation": "x" * 4097})
        with self.assertRaisesRegex(BankProjectionError, "exceeds"):
            project(Aggregate(ALERT, (long_detail,), (), {}))
        large_rows = tuple(summary_row(f"entity-{i:03d}", f"field-{i:03d}", "median",
            details={"description": "x" * 3900}) for i in range(150))
        with self.assertRaisesRegex(BankProjectionError, "serialized projection exceeds"):
            project(Aggregate(ALERT, large_rows, (), {}), {"limit": 256})

    def test_native_query_pages_and_prior_projections_cannot_launder_coverage(self):
        query_page = Aggregate(ALERT, (), (), {"public_query": {"rows_data": [{"value": "raw"}]}})
        with self.assertRaisesRegex(BankProjectionError, "native public-query"):
            project(query_page)
        source = Aggregate(ALERT, (summary_row(),), (), {})
        projected = project(source)
        with self.assertRaisesRegex(BankProjectionError, "re-projecting"):
            project(projected)

    def test_registration_is_versioned_cacheable_and_typed(self):
        registry = OperatorRegistry()
        register_m4_projection(registry)
        spec = registry.require("bank.public.project", PROJECTION_VERSION)
        self.assertEqual(spec.output_type, "Aggregate")
        self.assertEqual(spec.inputs[0].value_type, "Aggregate")
        self.assertTrue(spec.cacheable)
        self.assertEqual(spec.family, "bank_public_projection")

    @unittest.skipUnless(shutil.which("bwrap") and Path("/usr/bin/python3.10").is_file(),
                         "requires bubblewrap and Python 3.10 worker runtime")
    def test_generated_operator_can_consume_projected_aggregate_over_worker_boundary(self):
        from e4.sandbox.runner import run_generated_operator
        # Validate output size first, then pass the exact typed Aggregate through
        # the production generated-code worker and strict contract decoder.
        source = Aggregate(ALERT, (summary_row(details={"bucket_samples": tuple((i, float(i))
            for i in range(200_000))}),), ("bank/metric_container.csv",), {})
        projected = project(source, {"limit": 1})
        spec = validate_generated_spec({
            "name": "evolved.projection_reader", "version": "v1",
            "description": "Read one projected aggregate summary", "family": "bank_metric",
            "input_ports": [{"name": "data", "type": "Aggregate"}],
            "output_type": "Aggregate", "parameters": {},
            "source": "def operator(inputs, params, runtime):\n    return inputs['data']\n"})
        wire_aggregate = json.loads(canonical_json(projected))
        result = run_generated_operator(spec["source"], {"data": wire_aggregate}, {}, {}, "Aggregate")
        if not result.get("ok") and "bwrap: loopback: Failed to create NETLINK_ROUTE socket" in result.get("stderr", ""):
            self.skipTest("bubblewrap namespace setup is unavailable")
        self.assertTrue(result.get("ok"), result.get("error"))
        self.assertEqual(result["result"]["$type"], "Aggregate")
        self.assertEqual(result["result"]["$fields"]["rows"]["$tuple"][0]["$fields"]["value"], 42.5)


if __name__ == "__main__":
    unittest.main()
