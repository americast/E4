"""Focused regressions for schema and authored evidence contracts."""

from __future__ import annotations

import json
import unittest

from e4.dag import DAGValidationError, GROUP_BY_BY_RECORD_TYPE, verify_dag
from e4.registry import OperatorRegistry, OperatorSpec, ParameterSpec, PortSpec
from e4.data.record_store import TelemetryRecordStore
from e4.controller.controller import _to_ranked_answer
from e4.controller.repair import StaticRepairController
from e4.prompts.examples import METRIC_ANOMALY_EXAMPLE, typed_api_reference
from e4.sandbox.runner import BubblewrapRunner, ExecutionError, SubprocessRunner
from e4.sandbox.typechecker import check_operator_types
from e4.sandbox.views import (
    CoverageView,
    TelemetryView,
    WindowView,
    aggregate_records,
    make_evidence,
    make_entity_ref,
)
from e4.types import (
    ColumnSchema,
    DatasetSchema,
    EntityRef,
    Evidence,
    LogRecord,
    MetricRecord,
    RankedAnswer,
    SpanRecord,
    TableSchema,
    Window,
)


def _schema() -> DatasetSchema:
    return DatasetSchema(
        tables=(
            TableSchema(
                name="metrics.csv",
                record_type="metric",
                columns=(
                    ColumnSchema("timestamp", "integer"),
                    ColumnSchema("value", "number"),
                    ColumnSchema("custom_dimension", "string"),
                ),
                entity_roles=("service",),
            ),
        )
    )


class RequirementContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = OperatorRegistry()
        self.registry.register(
            OperatorSpec(
                name="test.source",
                version="1",
                inputs=(),
                output_type="Aggregate",
                parameters={
                    "table": ParameterSpec("table", "string", default="metrics.csv"),
                    "statistic": ParameterSpec("statistic", "string", default="mean"),
                },
                implementation=lambda i, p, r: None,
            )
        )

    def _consumer(self, name: str, requirements: dict[str, object]) -> None:
        self.registry.register(
            OperatorSpec(
                name=name,
                version="1",
                inputs=(PortSpec("data", "Aggregate"),),
                output_type="ScoredCandidates",
                input_requirements={"data": requirements},
                implementation=lambda i, p, r: None,
            )
        )

    def _dag(self, consumer: str, params: dict[str, object] | None = None) -> dict[str, object]:
        return {
            "nodes": [
                {"id": "src", "operator": "test.source", "version": "1", "inputs": {}, "params": params or {}},
                {"id": "out", "operator": consumer, "version": "1", "inputs": {"data": "src"}},
            ],
            "return_node": "out",
        }

    def test_plural_contract_and_schema_derived_grouping_dimension(self) -> None:
        self._consumer(
            "test.consumer_valid",
            {
                "source_columns": {"metrics.csv": ["timestamp", "value"]},
                "statistics": ["mean", "quantile"],
                "group_by": ["service", "custom_dimension"],
            },
        )
        verified = verify_dag(self._dag("test.consumer_valid"), self.registry, dataset_schema=_schema())
        self.assertEqual(verified.return_node, "out")

    def test_malformed_plural_contract_is_not_silently_ignored(self) -> None:
        cases = (
            {"source_columns": {"metrics.csv": "value"}},
            {"statistics": "mean"},
            {"group_by": ["service", None]},
        )
        for index, requirement in enumerate(cases):
            with self.subTest(requirement=requirement):
                name = f"test.consumer_malformed_{index}"
                self._consumer(name, requirement)
                with self.assertRaises(DAGValidationError) as raised:
                    verify_dag(self._dag(name), self.registry, dataset_schema=_schema())
                self.assertEqual(raised.exception.code, "input_requirement_mismatch")

    def test_schema_columns_and_producer_table_and_statistic_are_checked(self) -> None:
        self._consumer("test.consumer_bad_column", {"source_columns": {"metrics.csv": ["missing"]}})
        with self.assertRaises(DAGValidationError):
            verify_dag(self._dag("test.consumer_bad_column"), self.registry, dataset_schema=_schema())

        self._consumer("test.consumer_table", {"source_columns": {"other.csv": ["value"]}})
        with self.assertRaises(DAGValidationError):
            verify_dag(self._dag("test.consumer_table"), self.registry, dataset_schema=_schema())

        self._consumer("test.consumer_stat", {"statistics": ["sum"]})
        with self.assertRaises(DAGValidationError):
            verify_dag(
                self._dag("test.consumer_stat", {"table": "metrics.csv", "statistic": "mean"}),
                self.registry,
                dataset_schema=_schema(),
            )

        self._consumer(
            "test.consumer_wrong_table_dimension",
            {"source_columns": {"metrics.csv": ["value"]}, "group_by": ["executing_component"]},
        )
        with self.assertRaises(DAGValidationError):
            verify_dag(
                self._dag("test.consumer_wrong_table_dimension"),
                self.registry,
                dataset_schema=_schema(),
            )


class EvidenceContractTests(unittest.TestCase):
    def test_repair_prompt_preserves_metadata_schema_api_source_and_two_call_cap(self) -> None:
        original_source = (
            "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n"
            "    return 1\n"
        )
        bundle = {
            "gap_record": {
                "gap_description": "compare the declared metric table across windows",
                "proposed_operator_names": ["evolved.repair_context"],
            },
            "proposed_operators": [{
                "name": "evolved.repair_context",
                "version": "1",
                "authoring_profile": "typed-operator-code-v1",
                "description": "Original operator description",
                "family": "custom_metric",
                "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
                "output_type": "Aggregate",
                "parameters": {"source_table": {"type": "string", "required": True}},
                "input_requirements": {
                    "telemetry": {
                        "source_columns": {"metrics.csv": ["timestamp", "value"]},
                        "statistics": ["mean"],
                        "group_by": ["entity_identifier"],
                    }
                },
                "source": original_source,
            }],
        }
        prompts: list[str] = []

        def repeat_invalid_bundle(prompt: str) -> str:
            prompts.append(prompt)
            return json.dumps(bundle)

        controller = StaticRepairController(max_repairs=2)
        ok, specs, errors, repairs_used = controller.attempt_repairs(
            bundle, repeat_invalid_bundle, dataset_schema=_schema()
        )
        self.assertFalse(ok)
        self.assertEqual(specs, [])
        self.assertEqual(repairs_used, 2)
        self.assertEqual(len(prompts), 2)
        self.assertEqual(bundle["proposed_operators"][0]["source"], original_source)
        for prompt in prompts:
            self.assertIn("compare the declared metric table across windows", prompt)
            self.assertIn("evolved.repair_context", prompt)
            self.assertIn("source_table", prompt)
            self.assertIn("source_columns", prompt)
            self.assertIn('"record_type": "metric"', prompt)
            self.assertIn("make_evidence(", prompt)
            self.assertIn(original_source, prompt)
            self.assertEqual(prompt.count(original_source), 1)

    def test_aggregate_grouping_supports_all_scalar_typed_view_fields(self) -> None:
        alert = Window("alert", 10, 20)
        ref = Window("reference", 0, 10)
        metric = MetricRecord(
            "metrics.csv", 11, EntityRef("service", "svc-a"), "cpu", 2.5, "percent",
            {"origin_row_id": "metric-row-1"},
        )
        span = SpanRecord(
            "traces.csv", 12, "trace-a", "span-a", "parent-a", 4.5, True, "rpc",
            EntityRef("service", "svc-a"), EntityRef("service", "svc-b"),
            EntityRef("database", "db-a"),
        )
        log = LogRecord(
            "logs.csv", 13, EntityRef("service", "svc-a"), "app", "failed", "error",
        )
        view = TelemetryView(
            "case", ref, alert, (metric, span, log),
            source_tables=("metrics.csv", "traces.csv", "logs.csv"),
        )
        scalar_fields = {
            "metric": (
                "source_table", "timestamp_ms_utc", "entity_role", "entity_identifier",
                "origin_row_id", "metric_name", "metric_key", "value", "unit",
            ),
            "span": (
                "source_table", "timestamp_ms_utc", "trace_id", "span_id", "parent_id",
                "duration_ms", "duration_value", "success", "call_type",
                "executing_component_role", "executing_component_identifier",
                "logical_service_role", "logical_service_identifier", "database_target_role",
                "database_target_identifier",
            ),
            "log": (
                "source_table", "timestamp_ms_utc", "log_name", "entity_role",
                "entity_identifier", "value", "severity",
            ),
        }
        for record_type, table, field, fields in (
            ("metric", "metrics.csv", "value", scalar_fields["metric"]),
            ("span", "traces.csv", "duration_value", scalar_fields["span"]),
            ("log", "logs.csv", "count", scalar_fields["log"]),
        ):
            for group_field in fields:
                with self.subTest(record_type=record_type, group_field=group_field):
                    self.assertIn(group_field, GROUP_BY_BY_RECORD_TYPE[record_type])
                    result = aggregate_records(
                        view, table, view.alert_window, record_type, field, "mean",
                        group_by=[group_field],
                    )
                    self.assertEqual(len(result.rows), 1)
                    self.assertEqual(result.rows[0].group_fields, (group_field,))

    def test_evolve_bundle_over_three_operators_is_rejected_before_typechecking(self) -> None:
        bundle = {
            "gap_record": {
                "gap_description": "test proposal cap",
                "proposed_operator_names": [f"evolved.op_{i}" for i in range(4)],
            },
            "proposed_operators": [
                {"name": f"evolved.op_{i}", "source": "this is not valid Python"}
                for i in range(4)
            ],
        }
        errors, specs = StaticRepairController().validate_and_typecheck(bundle)
        self.assertEqual(specs, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("maximum for one EVOLVE proposal is 3", errors[0])

    def test_prompt_api_reference_uses_lines_and_publishes_constructor_contracts(self) -> None:
        reference = typed_api_reference()
        self.assertIn("\n- make_scored_candidate(", reference)
        self.assertIn("\n- make_scored_candidates(", reference)
        self.assertIn("\n- make_aggregate_row(", reference)
        self.assertIn("EvidenceView fields:", reference)
        self.assertIn("ScoredCandidateView fields:", reference)
        self.assertIn("SpanView fields:", reference)
        self.assertNotIn("\\n- make_scored_candidate(", reference)

    def test_aggregate_records_accepts_documented_duration_value_alias(self) -> None:
        ref = Window("reference", 0, 10)
        alert = Window("alert", 10, 20)
        record = SpanRecord("traces.csv", 11, "trace", "span", None, 4.5, True)
        view = TelemetryView("case", ref, alert, records=(record,), source_tables=("traces.csv",))
        result = aggregate_records(view, "traces.csv", view.alert_window, "span", "duration_value", "mean")
        self.assertEqual(result.rows[0].value, 4.5)

    def test_answer_boundary_requires_supported_eligible_known_candidates(self) -> None:
        allowed = EntityRef("service", "svc-a")
        invented = EntityRef("service", "made-up")
        window = Window("alert", 10, 20)
        partial = Evidence("probe", window, "mean", 1.0, (allowed,), 1, ranking_eligible=False)
        valid = Evidence("probe", window, "mean", 1.0, (allowed,), 1, ranking_eligible=True)
        self.assertIsNone(
            _to_ranked_answer(
                RankedAnswer((allowed,), (1.0,), evidence=(partial,)),
                "serve",
                (allowed,),
            )
        )
        self.assertIsNone(
            _to_ranked_answer(
                RankedAnswer((invented,), (1.0,), evidence=(valid,)),
                "serve",
                (allowed,),
            )
        )
        answer = _to_ranked_answer(
            RankedAnswer((allowed,), (1.0,), evidence=(valid,)),
            "serve",
            (allowed,),
        )
        self.assertEqual(answer.candidates, (allowed,))

    def test_incomplete_coverage_disables_ranking_eligibility(self) -> None:
        evidence = make_evidence(
            source="authored",
            window=WindowView("alert", 10, 20),
            statistic="mean",
            value=1.0,
            contributing_entities=(make_entity_ref("service", "svc-a"),),
            support=1,
            coverage=CoverageView(False, False, 0, None, 3, 1),
        )
        self.assertFalse(evidence.ranking_eligible)

    def test_typechecker_stub_accepts_coverage_keyword(self) -> None:
        spec = {
            "name": "evolved.coverage_contract",
            "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "source": (
                "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> ScoredCandidates:\n"
                "    tel = telemetry_view(inputs['telemetry'])\n"
                "    result = metric_points(tel, 'metrics.csv', tel.alert_window)\n"
                "    evidence = make_evidence('probe', result.window, 'mean', 1.0, [], 1, coverage=result.coverage)\n"
                "    return make_scored_candidates([], 'probe')\n"
            ),
        }
        # The return is deliberately simple; this checks that the public typed
        # constructor declaration accepts the same keyword as its runtime API.
        check_operator_types(spec, _schema())

    def test_subprocess_worker_reads_file_backed_store_without_inline_records(self) -> None:
        class Adapter:
            def get_schema(self) -> DatasetSchema:
                return _schema()

            def get_windows(self) -> tuple[Window, Window]:
                return Window("reference", 0, 10), Window("alert", 10, 20)

            def stream_records(self, table_name: str, window: Window):
                if table_name == "metrics.csv" and window.name == "alert":
                    yield MetricRecord("metrics.csv", 11, make_entity_ref("service", "svc-a"), "cpu", 7.0)

        adapter = Adapter()
        store = TelemetryRecordStore.from_adapter(adapter)
        try:
            ref, alt = adapter.get_windows()
            telemetry = TelemetryView(
                "case",
                ref,
                alt,
                records=(),
                source_tables=("metrics.csv",),
                record_store=store,
            )
            payload_view = telemetry.as_dict()
            self.assertNotIn("records", payload_view)
            self.assertIn("record_store", payload_view)
            source = '''
def operator(inputs, params, runtime):
    tel = telemetry_view(inputs["telemetry"])
    result = metric_points(tel, "metrics.csv", tel.alert_window, limit=10)
    if not result.coverage.complete:
        return make_aggregate(tel.alert_window, [], source_tables=["metrics.csv"])
    rows = []
    for point in result.rows:
        rows.append(make_aggregate_row(
            make_entity_ref(point.entity_role, point.entity_identifier),
            point.metric_key,
            tel.alert_window,
            "mean",
            point.value,
            1,
            source_tables=["metrics.csv"],
        ))
    return make_aggregate(tel.alert_window, rows, source_tables=["metrics.csv"])
'''
            operator = OperatorSpec(
                "test.file_backed",
                "1",
                (PortSpec("telemetry", "Telemetry"),),
                "Aggregate",
                source=source,
            )
            result = SubprocessRunner(timeout_seconds=30, memory_limit_mb=512).run(
                operator, {"telemetry": telemetry}, {}, {}
            )
            self.assertEqual(len(result.rows), 1)
            self.assertEqual(result.rows[0].value, 7.0)
            with self.assertRaisesRegex(ExecutionError, "does not yet mount file-backed telemetry"):
                BubblewrapRunner(bwrap_path="/bin/true").run(operator, {"telemetry": telemetry}, {}, {})
        finally:
            store.close()

    def test_evolve_reference_example_typechecks_against_incident_schema(self) -> None:
        spec = {
            "name": "evolved.reference_example",
            "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
            "output_type": "Aggregate",
            "parameters": {"source_table": {"type": "string", "required": True}},
            "source": METRIC_ANOMALY_EXAMPLE,
        }
        check_operator_types(spec, _schema())


if __name__ == "__main__":
    unittest.main()
