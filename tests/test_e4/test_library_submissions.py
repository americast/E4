from __future__ import annotations

import json
import tempfile
import unittest

from e4.bootstrap import (
    LibrarySubmissionError,
    apply_library_submission,
    parse_library_submission,
)
from e4.controller.library import LibraryManager
from e4.dag import SerialExecutor, verify_dag
from e4.onboarding import prepare_library
from e4.sandbox.runner import SubprocessRunner
from e4.sandbox.views import TelemetryView
from e4.types import ColumnSchema, DatasetSchema, EntityRef, MetricRecord, ScoredCandidates, TableSchema, Window


SCHEMA = DatasetSchema((
    TableSchema(
        "metrics.csv",
        "metric",
        (ColumnSchema("timestamp", "timestamp"), ColumnSchema("value", "number")),
        entity_roles=("service",),
    ),
))

SCORER_SOURCE = '''def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    telemetry = telemetry_view(inputs["telemetry"])
    source_table = params["source_table"]
    result = metric_points(telemetry, source_table, telemetry.alert_window)
    if not result.coverage.complete:
        return make_scored_candidates([], scorer="evolved.observed_scorer", status="abstained")
    candidates: list[ScoredCandidateView] = []
    for point in result.rows:
        if point.value is None:
            continue
        entity = make_entity_ref(point.entity_role, point.entity_identifier)
        evidence = make_evidence(
            source="evolved.observed_scorer",
            window=telemetry.alert_window,
            statistic="observed_metric_value",
            value=point.value,
            contributing_entities=[entity],
            support=1,
            coverage=result.coverage,
        )
        candidates.append(make_scored_candidate(
            entity=entity,
            score=abs(float(point.value)),
            evidence=[evidence],
            analysis_family="generated_metric",
        ))
    if not candidates:
        return make_scored_candidates(candidates, scorer="evolved.observed_scorer", status="abstained")
    return make_scored_candidates(candidates, scorer="evolved.observed_scorer")
'''


def _operator() -> dict:
    return {
        "name": "evolved.observed_scorer",
        "version": "1",
        "authoring_profile": "typed-operator-code-v1",
        "description": "Score observed typed metric rows with coverage-bound evidence.",
        "family": "observed_metric",
        "input_ports": [{"name": "telemetry", "type": "Telemetry", "required": True}],
        "output_type": "ScoredCandidates",
        "parameters": {"source_table": {"type": "string", "required": True}},
        "input_requirements": {
            "telemetry": {"source_columns": {"metrics.csv": ["timestamp", "value"]}},
        },
        "source": SCORER_SOURCE,
    }


def _book(name: str = "observed_metric_book", operator_name: str = "evolved.observed_scorer") -> dict:
    return {
        "name": name,
        "description": "Run the observed metric scorer over the adapter's telemetry.",
        "dag": {
            "schema_version": "e4-dag/1",
            "nodes": [{
                "id": "score",
                "operator": operator_name,
                "version": "1",
                "inputs": {"telemetry": "telemetry"},
                "params": {"source_table": "metrics.csv"},
            }],
            "return_node": "score",
        },
    }


def _source_submission(*, books: list[dict] | None = None) -> dict:
    return {
        "status": "additions",
        "gap_record": {
            "gap_description": "No active scorer preserves typed observations and query coverage.",
            "proposed_operator_names": ["evolved.observed_scorer"],
        },
        "proposed_operators": [_operator()],
        "playbooks": books if books is not None else [_book()],
    }


def _telemetry() -> TelemetryView:
    ref = Window("reference", 0, 100)
    alert = Window("alert", 100, 200)
    entity = EntityRef("service", "checkout")
    return TelemetryView(
        "library-submission",
        ref,
        alert,
        records=(MetricRecord("metrics.csv", 120, entity, "latency", 12.5, unit="ms"),),
        source_tables=("metrics.csv",),
    )


class LibrarySubmissionTests(unittest.TestCase):
    def test_exact_status_envelopes_and_strict_json(self) -> None:
        addition = _source_submission()
        self.assertEqual(parse_library_submission(json.dumps(addition)), addition)
        unchanged = {"status": "unchanged", "reason": "Existing books cover the declared schema."}
        blocked = {"status": "blocked", "reason": "An essential identity field is absent."}
        self.assertEqual(parse_library_submission(unchanged), unchanged)
        self.assertEqual(parse_library_submission(blocked), blocked)
        for raw in (
            json.dumps(unchanged) + " prose",
            '{"status":"unchanged","status":"blocked","reason":"x"}',
            json.dumps({**unchanged, "extra": True}),
            '{"status":"unchanged","reason":NaN}',
            {"status": [], "reason": "bad type"},
        ):
            with self.subTest(raw=repr(raw)[:35]), self.assertRaises(LibrarySubmissionError):
                parse_library_submission(raw)

    def test_default_unchanged_and_playbook_only_additions_preserve_defaults(self) -> None:
        base = prepare_library(SCHEMA, mode="paper")
        defaults_ops = set(base._default_operator_names)
        defaults_books = set(base._default_playbook_names)
        unchanged = apply_library_submission(
            {"status": "unchanged", "reason": "Paper starters cover this simple task."}, base, SCHEMA,
        )
        self.assertIsNot(unchanged, base)
        self.assertEqual(unchanged.mode, "paper")
        self.assertEqual(unchanged._default_operator_names, defaults_ops)
        self.assertEqual(unchanged._default_playbook_names, defaults_books)

        unsupported_schema = DatasetSchema((TableSchema(
            "app_metrics.csv",
            "metric",
            (ColumnSchema("timestamp", "timestamp"), ColumnSchema("value", "number")),
            entity_roles=("application",),
        ),))
        unprepared = prepare_library(unsupported_schema, mode="paper")
        with self.assertRaisesRegex(LibrarySubmissionError, "empty or unprepared"):
            apply_library_submission(
                {"status": "unchanged", "reason": "no changes"},
                unprepared,
                unsupported_schema,
            )

        existing = base.get_playbook("backend_mean_shift")
        self.assertIsNotNone(existing)
        playbook_only = {
            "status": "additions",
            "proposed_operators": [],
            "playbooks": [{
                "name": "copied_backend_book",
                "description": "A new composition reusing active shipped operators.",
                "dag": existing.dag,
            }],
        }
        updated = apply_library_submission(playbook_only, base, SCHEMA)
        self.assertEqual(updated.mode, "paper")
        self.assertTrue(updated.registry.has("paper.backend.metric_conditions", "1"))
        self.assertIsNotNone(updated.get_playbook("copied_backend_book"))
        self.assertEqual(updated._default_operator_names, defaults_ops)
        self.assertEqual(updated._default_playbook_names, defaults_books)
        self.assertIsNone(base.get_playbook("copied_backend_book"))

    def test_clean_slate_requires_source_and_book_and_excludes_paper(self) -> None:
        clean = prepare_library(SCHEMA, mode="clean_slate")
        for response in (
            {"status": "unchanged", "reason": "no change"},
            {"status": "additions", "proposed_operators": [], "playbooks": [_book("empty") ]},
        ):
            with self.subTest(response=response), self.assertRaises(LibrarySubmissionError):
                apply_library_submission(response, clean, SCHEMA)

        updated = apply_library_submission(_source_submission(), clean, SCHEMA)
        self.assertEqual(updated.mode, "clean_slate")
        self.assertEqual([op.name for op in updated.registry.all_operators()], ["evolved.observed_scorer"])
        self.assertEqual([book.name for book in updated.list_playbooks()], ["observed_metric_book"])

        contaminated = LibraryManager(
            registry=prepare_library(SCHEMA, mode="paper").registry,
            mode="clean_slate",
            origin="user-clean-slate",
        )
        with self.assertRaisesRegex(LibrarySubmissionError, "contains shipped paper"):
            apply_library_submission(_source_submission(), contaminated, SCHEMA)

    def test_atomic_operator_book_validation_and_unreferenced_source_rejection(self) -> None:
        base = prepare_library(SCHEMA, mode="paper")
        broken = _source_submission(books=[_book(operator_name="evolved.missing")])
        with self.assertRaises(LibrarySubmissionError) as caught:
            apply_library_submission(broken, base, SCHEMA)
        self.assertEqual(caught.exception.stage, "playbooks")
        self.assertFalse(base.registry.has("evolved.observed_scorer", "1"))
        self.assertIsNone(base.get_playbook("observed_metric_book"))

        unreferenced = _source_submission(books=[{
            "name": "existing_only",
            "description": "Uses only shipped analysis.",
            "dag": base.get_playbook("backend_mean_shift").dag,
        }])
        with self.assertRaisesRegex(LibrarySubmissionError, "must be reachable"):
            apply_library_submission(unreferenced, base, SCHEMA)
        self.assertFalse(base.registry.has("evolved.observed_scorer", "1"))

    def test_new_source_executes_verifies_and_reloads_in_both_modes(self) -> None:
        for mode in ("paper", "clean_slate"):
            with self.subTest(mode=mode):
                base = prepare_library(SCHEMA, mode=mode)
                prepared = apply_library_submission(_source_submission(), base, SCHEMA)
                book = prepared.get_playbook("observed_metric_book")
                verified = verify_dag(book.dag, prepared.registry, dataset_schema=SCHEMA)
                result = SerialExecutor(prepared.registry).execute_verified(
                    verified,
                    runtime=None,
                    external_values={"telemetry": _telemetry()},
                    runner=SubprocessRunner(timeout_seconds=20),
                )
                self.assertIsInstance(result.return_value, ScoredCandidates)
                self.assertEqual(result.return_value.candidates[0].entity.identifier, "checkout")

                with tempfile.TemporaryDirectory() as directory:
                    prepared.save_to_dir(directory)
                    loaded = LibraryManager.load_from_dir(directory, mode=mode)
                    loaded_dag = verify_dag(loaded.get_playbook("observed_metric_book").dag, loaded.registry, dataset_schema=SCHEMA)
                    loaded_result = SerialExecutor(loaded.registry).execute_verified(
                        loaded_dag,
                        runtime=None,
                        external_values={"telemetry": _telemetry()},
                        runner=SubprocessRunner(timeout_seconds=20),
                    )
                    self.assertEqual(loaded_result.return_value.candidates[0].entity.identifier, "checkout")
                    if mode == "clean_slate":
                        self.assertFalse(set(op.name for op in loaded.registry.all_operators()) & {"paper.answers.weighted_consensus"})


if __name__ == "__main__":
    unittest.main()
