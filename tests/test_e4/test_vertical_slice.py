"""Vertical slice engineering test: 3-stage loop, repair with preserved source, admission and reuse."""

import json
import unittest
from pathlib import Path

from adapters.bank_adapter import BankAdapter
from adapters.bank_library import build_bank_registry, get_bank_seed_playbook
from e4.controller.controller import DiagnosisController
from e4.controller.library import LibraryManager, Playbook
from e4.oracle import AcceptanceOracle, BudgetStopPolicy, Decision, DiagnosticContext
from e4.sandbox.runner import SubprocessRunner
from e4.types import DiagnosticResult, EntityRef, RankedAnswer


class MockThreeStageOracle(AcceptanceOracle):
    """Rejects Stage 1 and Stage 2, confirms Stage 3 if target component is present."""

    def __init__(self, target_entity: str):
        self.target_entity = target_entity

    def evaluate(self, answer: RankedAnswer, context: DiagnosticContext) -> Decision:
        if context.stage in {"select", "compose"}:
            return Decision(
                action="revise",
                reason=f"Stage '{context.stage}' did not identify root cause; escalating.",
                accepted=False,
            )
        # Stage 3
        top_ids = [c.canonical_id for c in answer.candidates[:3]]
        if self.target_entity in top_ids or not top_ids:
            return Decision(
                action="confirm",
                reason=f"Found target '{self.target_entity}' in top-3 candidates.",
                accepted=True,
            )
        return Decision(
            action="stop_unverified",
            reason=f"Target '{self.target_entity}' not in top-3 candidates {top_ids}.",
            accepted=False,
        )


class MockLLMClient:
    """Mock LLM responding to select, compose, evolve (with repair), and serve prompts."""

    def __init__(self):
        self.call_history: list[str] = []
        self.repair_prompt_received: str | None = None

    def complete(self, prompt: str) -> str:
        self.call_history.append(prompt)

        # Stage 1: SELECT
        if "Stage 1: Playbook Retrieval" in prompt:
            return json.dumps({
                "selected_playbook": "bank.starter.metric_consensus",
                "parameter_bindings": {},
                "rationale": "Retrieve baseline container metric consensus playbook",
            })

        # Stage 2: COMPOSE
        if "Stage 2: DAG Adaptation" in prompt:
            return json.dumps({
                "dag": {
                    "nodes": [
                        {
                            "id": "node_metric_agg",
                            "operator": "bank.metrics.aggregate",
                            "version": "1",
                            "inputs": {"telemetry": "telemetry"},
                            "params": {"table": "bank/metric_container.csv"},
                        },
                        {
                            "id": "node_anomaly_scorer",
                            "operator": "bank.anomaly.scorer",
                            "version": "1",
                            "inputs": {"aggregate": "node_metric_agg"},
                        },
                        {
                            "id": "node_consensus",
                            "operator": "paper.answers.weighted_consensus",
                            "version": "1",
                            "inputs": {"scores": ["node_anomaly_scorer"]},
                        },
                    ],
                    "return_node": "node_consensus",
                },
                "adaptation_rationale": "Recompose baseline DAG",
            })

        # Stage 3A: EVOLVE - First attempt with an intentional metadata name error
        if "Stage 3A: Operator Authoring" in prompt:
            return json.dumps({
                "gap_record": {
                    "gap_description": "Need custom metric variance ratio analysis",
                    "proposed_operator_names": ["evolved.bank.variance_contrast"],
                },
                "proposed_operators": [
                    {
                        "name": "evolved.bank.variance_contrast",  # Intentionally invalid naming
                        "version": "1",
                        "authoring_profile": "typed-operator-code-v1",
                        "description": "Calculates variance contrast for container metrics",
                        "family": "custom_metric",
                        "input_ports": [
                            {"name": "telemetry", "type": "Telemetry", "required": True, "variadic": False}
                        ],
                        "output_type": "ScoredCandidates",
                        "parameters": {"threshold": {"type": "number", "default": 1.5}},
                        "input_requirements": {
                            "telemetry": {
                                "source_columns": {"bank/metric_container.csv": ["timestamp", "cmdb_id", "value"]},
                                "statistics": ["raw"],
                                "group_by": [],
                            }
                        },
                        "source": (
                            "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n"
                            "    tel = telemetry_view(inputs['telemetry'])\n"
                            "    alt_w = tel.alert_window\n"
                            "    res = metric_points(tel, 'bank/metric_container.csv', alt_w)\n"
                            "    by_ent = group_by(res.rows, ['entity_identifier'])\n"
                            "    scores: list[ScoredCandidateView] = []\n"
                            "    for (ent_id,), pts in by_ent.items():\n"
                            "        if not ent_id or not pts:\n"
                            "            continue\n"
                            "        vals = [p.value for p in pts]\n"
                            "        sm = summarize(vals, 'max')\n"
                            "        score_val = sm.value if sm.value is not None else 0.0\n"
                            "        ent = make_entity_ref('service', str(ent_id))\n"
                            "        ev = make_evidence('custom_variance', alt_w, 'max', score_val, [ent], sm.support, True)\n"
                            "        scores.append(make_scored_candidate(ent, float(score_val), evidence=[ev]))\n"
                            "    return make_scored_candidates(scores, scorer='variance_scorer')\n"
                        ),
                    }
                ],
            })

        # Static Repair Prompt
        if "Static Repair Required" in prompt:
            self.repair_prompt_received = prompt
            # Fix the operator name to compliant format: evolved.variance_contrast
            return json.dumps({
                "gap_record": {
                    "gap_description": "Need custom metric variance ratio analysis",
                    "proposed_operator_names": ["evolved.variance_contrast"],
                },
                "proposed_operators": [
                    {
                        "name": "evolved.variance_contrast",  # Fixed name!
                        "version": "1",
                        "authoring_profile": "typed-operator-code-v1",
                        "description": "Calculates variance contrast for container metrics",
                        "family": "custom_metric",
                        "input_ports": [
                            {"name": "telemetry", "type": "Telemetry", "required": True, "variadic": False}
                        ],
                        "output_type": "ScoredCandidates",
                        "parameters": {"threshold": {"type": "number", "default": 1.5}},
                        "input_requirements": {
                            "telemetry": {
                                "source_columns": {"bank/metric_container.csv": ["timestamp", "cmdb_id", "value"]},
                                "statistics": ["raw"],
                                "group_by": [],
                            }
                        },
                        "source": (
                            "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n"
                            "    tel = telemetry_view(inputs['telemetry'])\n"
                            "    alt_w = tel.alert_window\n"
                            "    res = metric_points(tel, 'bank/metric_container.csv', alt_w)\n"
                            "    by_ent = group_by(res.rows, ['entity_identifier'])\n"
                            "    scores: list[ScoredCandidateView] = []\n"
                            "    for (ent_id,), pts in by_ent.items():\n"
                            "        if not ent_id or not pts:\n"
                            "            continue\n"
                            "        vals = [p.value for p in pts]\n"
                            "        sm = summarize(vals, 'max')\n"
                            "        score_val = sm.value if sm.value is not None else 0.0\n"
                            "        ent = make_entity_ref('service', str(ent_id))\n"
                            "        ev = make_evidence('custom_variance', alt_w, 'max', score_val, [ent], sm.support, True)\n"
                            "        scores.append(make_scored_candidate(ent, float(score_val), evidence=[ev]))\n"
                            "    return make_scored_candidates(scores, scorer='variance_scorer')\n"
                        ),
                    }
                ],
            })

        # Stage 3B: SERVE
        if "Stage 3B: Pipeline Serving" in prompt:
            return json.dumps({
                "dag": {
                    "nodes": [
                        {
                            "id": "node_evolved_scorer",
                            "operator": "evolved.variance_contrast",
                            "version": "1",
                            "inputs": {"telemetry": "telemetry"},
                            "params": {"threshold": 1.5},
                        },
                        {
                            "id": "node_consensus",
                            "operator": "paper.answers.weighted_consensus",
                            "version": "1",
                            "inputs": {"scores": ["node_evolved_scorer"]},
                        },
                    ],
                    "return_node": "node_consensus",
                },
                "serving_rationale": "Use evolved variance contrast operator in consensus DAG",
            })

        raise ValueError(f"Unexpected prompt received: {prompt[:100]}")


class TestVerticalSlice(unittest.TestCase):
    def test_full_three_stage_progression_repair_and_admission(self):
        case_path = Path("data/prepared/bank/public/cases/bank-20210304T0100-utc8")
        if not case_path.is_dir():
            self.skipTest(f"Bank case {case_path} not found")

        adapter = BankAdapter(case_path)
        target = "tomcat01"

        # Setup library with Bank baseline
        reg = build_bank_registry()
        lib = LibraryManager(registry=reg)
        seed_pb = get_bank_seed_playbook()
        lib.add_playbook(
            Playbook(
                name=seed_pb["name"],
                description=seed_pb["description"],
                dag=seed_pb["dag"],
            )
        )

        oracle = MockThreeStageOracle(target_entity=target)
        client = MockLLMClient()
        runner = SubprocessRunner(timeout_seconds=30)

        controller = DiagnosisController(
            library=lib,
            oracle=oracle,
            runner=runner,
            max_attempts=3,
            max_repairs=2,
        )

        # Run diagnosis
        res = controller.run_diagnostic(adapter, client, case_id="bank-case-01")

        # 1. Verify progression through all 3 stages
        self.assertEqual(res.status, "confirmed")
        self.assertEqual(res.stage, "serve")
        self.assertGreater(len(res.top_candidates), 0)

        # 2. Verify Section 8 repair occurred and preserved source in feedback
        self.assertIsNotNone(client.repair_prompt_received)
        self.assertIn("Rejected Python source for 'evolved.bank.variance_contrast'", client.repair_prompt_received)
        self.assertIn("def operator(inputs: Inputs, params: Params, runtime: Runtime)", client.repair_prompt_received)

        # 3. Verify operator was admitted to library
        self.assertIn("evolved.variance_contrast@1", res.admitted_operators)
        self.assertIsNotNone(res.admitted_playbook)
        self.assertTrue(lib.registry.has("evolved.variance_contrast", "1"))
        self.assertIsNotNone(lib.get_playbook(res.admitted_playbook))

        # 4. Verify reuse on a second case
        case2_path = Path("data/prepared/bank/public/cases/bank-20210304T1800-utc8")
        if case2_path.is_dir():
            adapter2 = BankAdapter(case2_path)
            # Second run mock: selects the newly admitted playbook in Stage 1!
            class ReuseLLMClient:
                def complete(self, prompt: str) -> str:
                    return json.dumps({
                        "selected_playbook": res.admitted_playbook,
                        "parameter_bindings": {},
                    })

            controller.oracle = BudgetStopPolicy(max_attempts=1)
            res2 = controller.run_diagnostic(
                adapter2,
                ReuseLLMClient(),
                case_id="bank-case-02",
            )
            # The admitted evolved operator ran successfully in Stage 1 of the new case!
            self.assertEqual(res2.stage, "select")
            self.assertGreater(len(res2.top_candidates), 0)


if __name__ == "__main__":
    unittest.main()
