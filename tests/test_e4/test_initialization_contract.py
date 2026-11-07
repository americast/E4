"""Proposal-boundary regressions for evolved operator requirements."""

from __future__ import annotations

import json
import unittest

from e4.controller.repair import StaticRepairController
from e4.prompts.examples import METRIC_ANOMALY_EXAMPLE
from e4.types import ColumnSchema, DatasetSchema, TableSchema


def _dataset_schema() -> DatasetSchema:
    return DatasetSchema(
        tables=(
            TableSchema(
                name="metrics.csv",
                record_type="metric",
                columns=(ColumnSchema("timestamp", "integer"), ColumnSchema("value", "number")),
                entity_roles=("service",),
            ),
        )
    )


def _proposal(source_columns: object, *, port_requirements: dict[str, object] | None = None) -> dict[str, object]:
    requirements = (
        port_requirements
        if port_requirements is not None
        else {"source_columns": source_columns}
    )
    return {
        "gap_record": {
            "gap_description": "summarize metric behavior by service",
            "proposed_operator_names": ["evolved.metric_summary"],
        },
        "proposed_operators": [
            {
                "name": "evolved.metric_summary",
                "version": "1",
                "authoring_profile": "typed-operator-code-v1",
                "description": "Summarize metric values by entity.",
                "family": "metric_summary",
                "input_ports": [{"name": "telemetry", "type": "Telemetry"}],
                "output_type": "Aggregate",
                "parameters": {"source_table": {"type": "string", "required": True}},
                "input_requirements": {"telemetry": requirements},
                "source": METRIC_ANOMALY_EXAMPLE,
            }
        ],
    }


class ProposalRequirementTests(unittest.TestCase):
    def test_malformed_source_columns_reaches_repair_and_valid_source_reference_passes(self) -> None:
        invalid = _proposal(
            "metrics.csv",
            port_requirements={
                "source_table": "metrics.csv",
                "source_columns": "metrics.csv",
                "statistics": ["mean"],
                "group_by": ["entity_identifier"],
            },
        )
        repaired = _proposal(
            "unused",
            port_requirements={
                "source_table": "metrics.csv",
                "source_columns": {"metrics.csv": ["timestamp", "value"]},
                "statistics": ["mean"],
                "group_by": ["entity_identifier"],
            },
        )
        prompts: list[str] = []

        def repair(prompt: str) -> str:
            prompts.append(prompt)
            return json.dumps(repaired)

        ok, specs, errors, repairs_used = StaticRepairController(max_repairs=2).attempt_repairs(
            invalid, repair, dataset_schema=_dataset_schema()
        )

        self.assertTrue(ok, errors)
        self.assertEqual(len(specs), 1)
        self.assertEqual(repairs_used, 1)
        self.assertEqual(len(prompts), 1)
        prompt = prompts[0]
        self.assertIn("source_columns must map table names to column sequences", prompt)
        self.assertIn("summarize metric behavior by service", prompt)
        self.assertIn('"source_table": "metrics.csv"', prompt)
        self.assertIn(METRIC_ANOMALY_EXAMPLE, prompt)

    def test_known_table_columns_and_canonical_grouping_are_checked_before_repair(self) -> None:
        cases = (
            ({"source_columns": {"missing.csv": ["value"]}}, "not in dataset schema"),
            ({"source_columns": {"metrics.csv": ["missing"]}}, "is not in table"),
            ({"source_columns": {"metrics.csv": ["value"]}, "group_by": ["imaginary"]}, "unsupported grouping dimension"),
            ({"statistics": "mean"}, "statistics must be a sequence"),
            ({"group_by": ["service", None]}, "group_by must contain non-empty"),
        )
        controller = StaticRepairController()
        for requirements, message in cases:
            with self.subTest(requirements=requirements):
                errors, specs = controller.validate_and_typecheck(
                    _proposal("unused", port_requirements=requirements),
                    dataset_schema=_dataset_schema(),
                )
                self.assertEqual(specs, [])
                self.assertTrue(any(message in error for error in errors), errors)

    def test_empty_requirements_and_non_mapping_proposals_are_handled(self) -> None:
        valid = _proposal("unused", port_requirements={})
        errors, specs = StaticRepairController().validate_and_typecheck(valid, dataset_schema=_dataset_schema())
        self.assertEqual(errors, [])
        self.assertEqual(len(specs), 1)

        malformed = {"gap_record": {}, "proposed_operators": [None]}
        errors, specs = StaticRepairController().validate_and_typecheck(malformed)
        self.assertEqual(specs, [])
        self.assertTrue(any("proposal must be a mapping" in error for error in errors), errors)

    def test_three_operator_proposal_limit_remains_enforced(self) -> None:
        bundle = {
            "gap_record": {
                "gap_description": "bounded proposal",
                "proposed_operator_names": [f"evolved.op_{index}" for index in range(4)],
            },
            "proposed_operators": [{"name": f"evolved.op_{index}"} for index in range(4)],
        }
        errors, specs = StaticRepairController().validate_and_typecheck(bundle)
        self.assertEqual(specs, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("maximum for one EVOLVE proposal is 3", errors[0])
