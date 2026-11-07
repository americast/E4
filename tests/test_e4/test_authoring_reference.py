from __future__ import annotations

import unittest
from dataclasses import fields

from e4.controller.controller import _to_ranked_answer
from e4.controller.schemas import REQUIRED_SPEC_FIELDS, validate_operator_proposal
from e4.prompts.authoring import authoring_contract_reference
from e4.prompts.examples import (
    LOG_ANALYSIS_EXAMPLE,
    METRIC_ANOMALY_EXAMPLE,
    SCORED_CANDIDATE_EXAMPLE,
    typed_api_reference,
)
from e4.registry import OperatorSpec, PortSpec
from e4.sandbox.ast_validator import SecurityValidationError, validate_source
from e4.sandbox.runner import ExecutionError, InProcessRunner, SubprocessRunner
from e4.sandbox.typechecker import check_operator_types
from e4.sandbox.views import (
    AggregateView,
    AggregateGroupView,
    CoverageView,
    LogRecordView,
    MetricPointView,
    ScoredCandidateView,
    ScoredCandidatesView,
    SpanView,
    SummaryResult,
    TelemetryView,
    EntityRefView,
    WindowView,
    aggregate_view,
)
from e4.types import (
    Aggregate,
    AggregateRow,
    Evidence,
    EntityRef,
    LogRecord,
    MetricRecord,
    ScoredCandidate,
    ScoredCandidates,
    Window,
)


class AuthoringReferenceTests(unittest.TestCase):
    def _proposal(self, source: str, output_type: str, *, metric: bool = False) -> dict:
        needs_table = 'params["source_table"]' in source
        params = {"source_table": {"type": "string", "required": True}} if needs_table else {}
        ports = [{"name": "telemetry", "type": "Telemetry", "required": True}]
        if output_type == "ScoredCandidates":
            ports = [{"name": "aggregate", "type": "Aggregate", "required": True}]
        reqs = {port["name"]: {} for port in ports}
        if metric:
            reqs["telemetry"] = {
                "source_columns": {"metrics.csv": ["timestamp", "entity", "metric", "value"]}
            }
        elif needs_table:
            reqs["telemetry"] = {
                "source_columns": {"logs.csv": ["timestamp", "value", "entity"]}
            }
        return {
            "name": "evolved.authoring_example",
            "version": "1",
            "authoring_profile": "typed-operator-code-v1",
            "description": "Example contract integration test",
            "family": "example",
            "input_ports": ports,
            "output_type": output_type,
            "parameters": params,
            "input_requirements": reqs,
            "source": source,
        }

    def _runtime_spec(self, proposal: dict) -> OperatorSpec:
        return OperatorSpec(
            name=proposal["name"],
            version=proposal["version"],
            inputs=tuple(
                PortSpec(name=port["name"], value_type=port["type"])
                for port in proposal["input_ports"]
            ),
            output_type=proposal["output_type"],
            authoring_profile=proposal["authoring_profile"],
            source=proposal["source"],
            input_requirements=proposal["input_requirements"],
        )

    def _validate_and_run(self, source: str, output_type: str, inputs: dict,
                          params: dict | None = None, *, metric: bool = False):
        proposal = self._proposal(source, output_type, metric=metric)
        self.assertEqual(set(proposal), REQUIRED_SPEC_FIELDS)
        self.assertEqual(validate_operator_proposal(proposal), [])
        self.assertEqual(check_operator_types(proposal)["status"], "passed")
        return SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), inputs, params or {}, {}
        )

    def test_reference_is_runtime_grounded_and_has_complete_result_fields(self) -> None:
        rendered = authoring_contract_reference()
        self.assertIn("collections, itertools, math, statistics", rendered)
        self.assertIn("source_columns", rendered)
        self.assertIn("synchronous entry point", rendered)
        self.assertIn("8192 UTF-8 bytes", rendered)
        self.assertIn("AggregateGroupView fields:", rendered)
        self.assertIn("SummaryResult fields:", rendered)
        self.assertIn("QueryResult[T] fields:", rendered)
        self.assertIn("TelemetryView fields:", rendered)
        self.assertIn("CoverageView fields:", rendered)
        self.assertIn("not an invented `.group` field", rendered)
        self.assertIn("details:", typed_api_reference())
        aliases = next(line for line in rendered.splitlines() if "available annotation names" in line)
        self.assertNotIn("Any", aliases)
        # The runtime records that callers may inspect are rendered with every
        # actual dataclass field, including the previously omitted result types.
        for view_type in (
            CoverageView,
            MetricPointView,
            SpanView,
            LogRecordView,
            AggregateGroupView,
            SummaryResult,
            ScoredCandidateView,
            ScoredCandidatesView,
        ):
            for field in fields(view_type):
                self.assertIn(f"{field.name}:", rendered)

    def test_metric_example_typechecks_runs_full_windows_and_abstains_on_truncation(self) -> None:
        proposal = self._proposal(METRIC_ANOMALY_EXAMPLE, "Aggregate", metric=True)
        self.assertEqual(validate_operator_proposal(proposal), [])
        self.assertEqual(check_operator_types(proposal)["status"], "passed")

        entity = EntityRef("service", "checkout")
        telemetry = _telemetry(
            (
                MetricRecord("metrics.csv", 10, entity, "latency", 10.0),
                MetricRecord("metrics.csv", 110, entity, "latency", 30.0),
            )
        )
        result = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"telemetry": telemetry}, {"source_table": "metrics.csv"}, {}
        )
        self.assertIsInstance(result, Aggregate)
        self.assertEqual(result.rows[0].value, 20.0)
        self.assertEqual(result.rows[0].contributing_entities, (entity,))

        incomplete = _telemetry(
            (
                MetricRecord("metrics.csv", 10, entity, "latency", 10.0),
                MetricRecord("metrics.csv", 110, entity, "latency", 30.0),
            ),
            truncated=True,
        )
        abstained = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"telemetry": incomplete}, {"source_table": "metrics.csv"}, {}
        )
        self.assertEqual(abstained.rows, ())
        self.assertIn("abstained", abstained.metadata)

    def test_log_example_typechecks_handles_nullable_identity_and_incomplete_coverage(self) -> None:
        proposal = self._proposal(LOG_ANALYSIS_EXAMPLE, "Aggregate")
        self.assertEqual(validate_operator_proposal(proposal), [])
        self.assertEqual(check_operator_types(proposal)["status"], "passed")

        telemetry = _telemetry(
            (
                LogRecord("logs.csv", 110, EntityRef("service", "checkout"), value="timeout"),
                LogRecord("logs.csv", 120, None, value="unattributed"),
            ),
            source_tables=("logs.csv",),
        )
        result = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"telemetry": telemetry}, {"source_table": "logs.csv"}, {}
        )
        self.assertIsInstance(result, Aggregate)
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0].entity.identifier, "checkout")
        self.assertEqual(result.rows[0].value, 1.0)

        incomplete = _telemetry(
            (LogRecord("logs.csv", 110, EntityRef("service", "checkout"), value="timeout"),),
            source_tables=("logs.csv",),
            truncated=True,
        )
        abstained = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"telemetry": incomplete}, {"source_table": "logs.csv"}, {}
        )
        self.assertEqual(abstained.rows, ())
        self.assertIn("abstained", abstained.metadata)

    def test_scored_example_passes_answer_guard_and_abstains_without_supported_rows(self) -> None:
        proposal = self._proposal(SCORED_CANDIDATE_EXAMPLE, "ScoredCandidates")
        self.assertEqual(validate_operator_proposal(proposal), [])
        self.assertEqual(check_operator_types(proposal)["status"], "passed")
        entity = EntityRef("service", "checkout")
        window = Window("alert", 100, 200)
        aggregate = Aggregate(
            window=window,
            rows=(AggregateRow(
                entity=entity,
                metric_key="latency.delta",
                window=window,
                statistic="mean_delta",
                value=2.5,
                support=3,
                contributing_entities=(entity,),
            ),),
        )
        result = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"aggregate": aggregate}, {}, {}
        )
        self.assertIsInstance(result, ScoredCandidates)
        answer = _to_ranked_answer(result, "serve", (entity,))
        self.assertIsNotNone(answer)
        self.assertEqual(answer.candidates, (entity,))
        self.assertEqual(answer.evidence[0].contributing_entities, (entity,))

        empty = Aggregate(window=window, rows=())
        abstained = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"aggregate": empty}, {}, {}
        )
        self.assertEqual(abstained.status, "abstained")
        self.assertIsNone(_to_ranked_answer(abstained, "serve", (entity,)))

        incomplete_with_valid_looking_row = Aggregate(
            window=window,
            rows=(AggregateRow(
                entity=entity,
                metric_key="latency.delta",
                window=window,
                statistic="mean_delta",
                value=2.5,
                support=3,
                contributing_entities=(entity,),
            ),),
            metadata={"truncated": True},
        )
        guarded = SubprocessRunner(timeout_seconds=20).run(
            self._runtime_spec(proposal), {"aggregate": incomplete_with_valid_looking_row}, {}, {}
        )
        self.assertEqual(guarded.status, "abstained")
        self.assertIsNone(_to_ranked_answer(guarded, "serve", (entity,)))

    def test_json_value_alias_is_annotation_only_and_metadata_is_native(self) -> None:
        source = '''def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    summary: JSONValue = "typed metadata"
    metadata: JSONMapping = {"summary": summary, "count": 2}
    return make_aggregate(make_window("alert", 0, 1), [], metadata=metadata)
'''
        proposal = {
            "name": "evolved.json_metadata",
            "authoring_profile": "typed-operator-code-v1",
            "input_ports": [],
            "output_type": "Aggregate",
            "parameters": {},
            "source": source,
        }
        validate_source(source)
        self.assertEqual(check_operator_types(proposal)["status"], "passed")
        operator = OperatorSpec(
            name="evolved.json_metadata",
            version="1",
            inputs=(),
            output_type="Aggregate",
            source=source,
        )
        result = SubprocessRunner(timeout_seconds=20).run(operator, {}, {}, {})
        self.assertIsInstance(result, Aggregate)
        self.assertEqual(result.metadata["summary"], "typed metadata")
        self.assertEqual(result.metadata["count"], 2)

        invalid_source = '''def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    value = JSONValue("not a runtime constructor")
    return make_aggregate(make_window("alert", 0, 1), [], metadata={"value": value})
'''
        with self.assertRaisesRegex(SecurityValidationError, "type annotation only"):
            validate_source(invalid_source)

    def test_native_record_aliases_are_valid_annotations_but_not_runtime_constructors(self) -> None:
        source = '''def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    entity: EntityRef = make_entity_ref("service", "typed-alias")
    window: Window = make_window("alert", 0, 1)
    row: AggregateRow = make_aggregate_row(
        entity, "latency", window, "mean", 2.0, 1,
        contributing_entities=[entity],
    )
    evidence = make_evidence(
        source="typed-alias",
        window=row.window,
        statistic=row.statistic,
        value=row.value,
        contributing_entities=[row.entity],
        support=row.support,
    )
    candidate: ScoredCandidate = make_scored_candidate(
        row.entity, 2.0, evidence=[evidence],
    )
    return make_scored_candidates([candidate], scorer="typed_aliases")
'''
        self.assertEqual(validate_source(source), None)
        spec = {
            "name": "evolved.native_alias_annotations",
            "authoring_profile": "typed-operator-code-v1",
            "input_ports": [],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "source": source,
        }
        self.assertEqual(check_operator_types(spec)["status"], "passed")
        operator = OperatorSpec(
            name="evolved.native_alias_annotations",
            version="1",
            inputs=(),
            output_type="ScoredCandidates",
            source=source,
        )
        result = SubprocessRunner(timeout_seconds=20).run(operator, {}, {}, {})
        self.assertIsInstance(result, ScoredCandidates)
        self.assertEqual(result.candidates[0].entity.identifier, "typed-alias")

        for alias, constructor_args in (
            ("Window", '"alert", 0, 1'),
            ("EntityRef", '"service", "x"'),
            ("AggregateRow", 'None, "metric", None, "stat", 1.0, 1'),
            ("ScoredCandidate", 'None, 1.0'),
        ):
            invalid_source = (
                "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n"
                f"    value = {alias}({constructor_args})\n"
                "    return make_scored_candidates([], scorer=\"invalid\")\n"
            )
            with self.subTest(alias=alias), self.assertRaisesRegex(SecurityValidationError, "type annotation only"):
                validate_source(invalid_source)

    def test_object_and_nullable_array_parameters_use_json_aliases_and_execute(self) -> None:
        source = '''def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    settings = params.get("settings")
    threshold = 1.0
    if settings is not None:
        raw_threshold = settings.get("threshold")
        if isinstance(raw_threshold, (int, float)):
            threshold = float(raw_threshold)

    labels = params.get("labels")
    label_count = 0
    if labels is not None:
        for label in labels:
            if isinstance(label, str):
                label_count += 1

    entity = make_entity_ref("service", "typed-config")
    value = threshold + float(label_count)
    evidence = make_evidence(
        source="typed-config",
        window=make_window("alert", 0, 1),
        statistic="typed_parameter_summary",
        value=value,
        contributing_entities=[entity],
        support=1,
    )
    candidate = make_scored_candidate(entity, value, evidence=[evidence])
    return make_scored_candidates([candidate], scorer="typed_config")
'''
        profile_spec = {
            "name": "evolved.typed_config",
            "authoring_profile": "typed-operator-code-v1",
            "input_ports": [],
            "output_type": "ScoredCandidates",
            "parameters": {
                "settings": {"type": "object"},
                "labels": {"type": "array", "nullable": True},
            },
            "source": source,
        }
        self.assertEqual(check_operator_types(profile_spec)["status"], "passed")
        operator = OperatorSpec(
            name="evolved.typed_config",
            version="1",
            inputs=(),
            output_type="ScoredCandidates",
            source=source,
        )
        result = SubprocessRunner(timeout_seconds=20).run(
            operator,
            {},
            {"settings": {"threshold": 3.5}, "labels": ["east", 7, None, "west"]},
            {},
        )
        self.assertEqual(result.candidates[0].score, 5.5)
        nullable = SubprocessRunner(timeout_seconds=20).run(operator, {}, {"labels": None}, {})
        self.assertEqual(nullable.candidates[0].score, 1.0)

    def test_variadic_input_is_read_as_sequence_and_runs(self) -> None:
        source = '''def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    score_sets: Sequence[ScoredCandidatesView] = inputs["scores"]
    candidates: list[ScoredCandidateView] = []
    for score_set in score_sets:
        if score_set.status == "ready":
            candidates.extend(score_set.candidates)
    if not candidates:
        return make_scored_candidates(candidates, scorer="sequence_merge", status="abstained")
    return make_scored_candidates(candidates, scorer="sequence_merge")
'''
        proposal = {
            "name": "evolved.sequence_merge",
            "authoring_profile": "typed-operator-code-v1",
            "input_ports": [{"name": "scores", "type": "ScoredCandidates", "variadic": True}],
            "output_type": "ScoredCandidates",
            "parameters": {},
            "source": source,
        }
        self.assertEqual(check_operator_types(proposal)["status"], "passed")
        operator = OperatorSpec(
            name="evolved.sequence_merge",
            version="1",
            inputs=(PortSpec("scores", "ScoredCandidates", variadic=True),),
            output_type="ScoredCandidates",
            source=source,
        )
        entity = EntityRef("service", "sequence-input")
        evidence = Evidence("fixture", Window("alert", 0, 1), "mean", 1.0, (entity,), 1)
        candidate = ScoredCandidate(entity, 1.0, evidence=(evidence,), analysis_family="fixture")
        score_set = ScoredCandidates((candidate,), scorer="fixture")
        result = SubprocessRunner(timeout_seconds=20).run(
            operator,
            {"scores": (score_set,)},
            {},
            {},
        )
        self.assertEqual(result.candidates[0].entity.identifier, "sequence-input")

    def test_allowed_imports_and_local_helpers_work_but_other_imports_remain_blocked(self) -> None:
        source = '''import math

def add_one(value: float) -> float:
    return value + 1.0

def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    amount = add_one(math.sqrt(4.0))
    return make_aggregate(
        make_window("alert", 0, 1),
        [make_aggregate_row(
            make_entity_ref("service", "local"), "probe", make_window("alert", 0, 1),
            "value", amount, 1,
        )],
    )
'''
        spec = OperatorSpec(
            name="evolved.namespace_probe",
            version="1",
            inputs=(),
            output_type="Aggregate",
            source=source,
        )
        self.assertEqual(
            check_operator_types({
                "name": "evolved.namespace_probe",
                "authoring_profile": "typed-operator-code-v1",
                "input_ports": [],
                "output_type": "Aggregate",
                "parameters": {},
                "source": source,
            })["status"],
            "passed",
        )
        for runner in (SubprocessRunner(timeout_seconds=20), InProcessRunner()):
            with self.subTest(runner=type(runner).__name__):
                result = runner.run(spec, {}, {}, {})
                self.assertEqual(result.rows[0].value, 3.0)
                view = aggregate_view(result)
                self.assertIsInstance(view, AggregateView)
                self.assertIsInstance(view.rows[0].entity, EntityRefView)

        denied = OperatorSpec(
            name="evolved.import_probe",
            version="1",
            inputs=(),
            output_type="Aggregate",
            source=(
                "import os\n"
                "def operator(inputs, params, runtime):\n"
                "    return make_aggregate(make_window('alert', 0, 1), [])\n"
            ),
        )
        for runner in (SubprocessRunner(timeout_seconds=20), InProcessRunner()):
            with self.subTest(runner=type(runner).__name__):
                with self.assertRaisesRegex((ExecutionError, ImportError), "unauthorized module"):
                    runner.run(denied, {}, {}, {})


def _telemetry(records: tuple, source_tables: tuple[str, ...] = ("metrics.csv",),
               truncated: bool = False):
    return TelemetryView(
        "authoring-test",
        Window("reference", 0, 100),
        Window("alert", 100, 200),
        records=records,
        source_tables=source_tables,
        metadata={"truncated_tables": list(source_tables)} if truncated else {},
    )


if __name__ == "__main__":
    unittest.main()
