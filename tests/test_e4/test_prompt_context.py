"""Controller history and public schema are carried into EVOLVE and SERVE."""

from __future__ import annotations

import json
import unittest

from e4.controller.controller import DiagnosisController, _bounded_answer_summary
from e4.controller.library import LibraryManager, Playbook
from e4.oracle import Decision
from e4.registry import OperatorRegistry, OperatorSpec
from e4.sandbox.runner import InProcessRunner
from e4.types import (
    ColumnSchema,
    DatasetSchema,
    EntityRef,
    Evidence,
    RankedAnswer,
    ScoredCandidate,
    ScoredCandidates,
    TableSchema,
    Window,
)


class _Telemetry:
    def get_schema(self) -> DatasetSchema:
        return DatasetSchema(
            tables=(
                TableSchema(
                    "metrics.csv",
                    "metric",
                    (ColumnSchema("timestamp", "integer"), ColumnSchema("value", "number")),
                    entity_roles=("service",),
                ),
            )
        )

    def get_windows(self):
        return Window("reference", 0, 10), Window("alert", 10, 20)

    def extract_suspect_entities(self):
        return [EntityRef("service", "svc-a"), EntityRef("service", "svc-b")]

    def stream_records(self, table_name, window):
        return iter(())


class _ReviseOracle:
    max_attempts = 3

    def evaluate(self, answer, context):
        return Decision("revise", f"continue after {context.stage}")


class _ReturnSupported(InProcessRunner):
    def __init__(self, output):
        self.output = output

    def run(self, operator, inputs, params, runtime):
        return self.output


class PromptContextTests(unittest.TestCase):
    def test_answer_summary_is_count_and_text_bounded(self) -> None:
        entities = tuple(EntityRef("service", f"svc-{index}") for index in range(5))
        long_evidence = tuple(
            Evidence(
                source="s" * 200,
                window=Window("alert", 10, 20),
                statistic="mean",
                value="v" * 400,
                contributing_entities=(entities[index % len(entities)],),
                support=1,
            )
            for index in range(8)
        )
        summary = _bounded_answer_summary(
            RankedAnswer(entities, tuple(float(index) for index in range(5)), evidence=long_evidence)
        )

        self.assertEqual(len(summary["candidates"]), 3)
        self.assertEqual(len(summary["evidence"]), 6)
        self.assertEqual(len(summary["evidence"][0]["source"]), 128)
        self.assertTrue(summary["evidence"][0]["source_truncated"])
        self.assertEqual(len(summary["evidence"][0]["value"]), 256)
        self.assertTrue(summary["evidence"][0]["value_truncated"])

    def test_evolve_and_serve_receive_failed_dag_schema_and_bounded_evidence(self) -> None:
        entity = EntityRef("service", "svc-a")
        alert = Window("alert", 10, 20)
        output = ScoredCandidates(
            candidates=(
                ScoredCandidate(
                    entity,
                    0.75,
                    evidence=(Evidence("fixture_metric", alert, "mean", 2.5, (entity,), 4),),
                ),
            ),
            scorer="fixture_scorer",
        )

        registry = OperatorRegistry()
        registry.register(
            OperatorSpec(
                name="baseline.scorer",
                version="1",
                inputs=(),
                output_type="ScoredCandidates",
                implementation=lambda inputs, params, runtime: output,
            )
        )
        library = LibraryManager(registry=registry)
        original_dag = {
            "nodes": [{"id": "selected", "operator": "baseline.scorer", "version": "1", "inputs": {}}],
            "return_node": "selected",
        }
        library.add_playbook(Playbook("baseline.pb", "fixture playbook", original_dag))

        source = (
            "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> ScoredCandidates:\n"
            "    tel = telemetry_view(inputs['telemetry'])\n"
            "    ent = make_entity_ref('service', 'svc-a')\n"
            "    ev = make_evidence('evolved_metric', tel.alert_window, 'mean', 1.0, [ent], 3)\n"
            "    c = make_scored_candidate(ent, 0.8, evidence=[ev])\n"
            "    return make_scored_candidates([c], scorer='evolved_metric')\n"
        )
        proposal = {
            "gap_record": {
                "gap_description": "summarize metric behavior by service",
                "proposed_operator_names": ["evolved.metric_probe"],
            },
            "proposed_operators": [
                {
                    "name": "evolved.metric_probe",
                    "version": "1",
                    "authoring_profile": "typed-operator-code-v1",
                    "description": "Produces a supported metric score.",
                    "family": "metric_probe",
                    "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
                    "output_type": "ScoredCandidates",
                    "parameters": {},
                    "input_requirements": {"telemetry": {}},
                    "source": source,
                },
            ],
        }
        failed_dag = {
            "nodes": [{"id": "missing", "operator": "unknown.operator", "version": "1", "inputs": {}}],
            "return_node": "missing",
        }
        prompts: list[str] = []

        def model(prompt: str) -> str:
            prompts.append(prompt)
            if "Stage 1: Playbook Retrieval" in prompt:
                return json.dumps({"selected_playbook": "baseline.pb", "parameter_bindings": {}})
            if "Stage 2: DAG Adaptation" in prompt:
                return json.dumps({"dag": failed_dag})
            if "Stage 3A: Operator Authoring" in prompt:
                return json.dumps(proposal)
            if "Stage 3B: Pipeline Serving" in prompt:
                return json.dumps({
                    "dag": {
                        "nodes": [{
                            "id": "serve",
                            "operator": "evolved.metric_probe",
                            "version": "1",
                            "inputs": {"telemetry": "telemetry"},
                        }],
                        "return_node": "serve",
                    }
                })
            raise AssertionError(f"Unexpected model prompt: {prompt[:80]}")

        controller = DiagnosisController(
            library=library,
            oracle=_ReviseOracle(),
            runner=_ReturnSupported(output),
            max_attempts=3,
        )
        result = controller.run_diagnostic(_Telemetry(), model)

        self.assertEqual(result.status, "unverified")
        evolve_prompt = next(prompt for prompt in prompts if "Stage 3A: Operator Authoring" in prompt)
        serve_prompt = next(prompt for prompt in prompts if "Stage 3B: Pipeline Serving" in prompt)
        for prompt in (evolve_prompt, serve_prompt):
            self.assertIn('"status": "failed_execution"', prompt)
            self.assertIn('"operator": "unknown.operator"', prompt)
            self.assertIn('"name": "metrics.csv"', prompt)
            self.assertIn('"source": "fixture_metric"', prompt)
            self.assertIn('"score": 0.75', prompt)
            self.assertIn('"support": 4', prompt)
            self.assertIn('"identifier": "svc-a"', prompt)

        failed_compose = next(
            item for item in result.history if item.get("stage") == "compose" and item.get("status") == "failed_execution"
        )
        self.assertEqual(failed_compose["dag"], failed_dag)
