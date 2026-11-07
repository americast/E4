from __future__ import annotations

import unittest

from e4.controller.controller import _select_available_playbook
from e4.controller.library import Playbook
from e4.onboarding import (
    normalize_dataset_schema,
    paper_availability,
    paper_catalog,
    paper_reference_examples,
    prepare_library,
)
from e4.prompts.templates import format_compose_prompt, format_evolve_prompt, format_serve_prompt
from e4.registry import OperatorSpec, PortSpec


class OnboardingPromptIntegrationTests(unittest.TestCase):
    def test_paper_catalog_and_active_availability_are_separate_from_examples(self):
        catalog = paper_catalog()
        self.assertEqual(len(catalog["operators"]), 19)
        self.assertEqual(len(catalog["playbooks"]), 11)
        availability = paper_availability({"tables": []})
        self.assertTrue(availability["operators"])
        self.assertTrue(availability["playbooks"])
        examples = paper_reference_examples()
        self.assertTrue(examples)
        self.assertTrue(all(item.get("reference_only") is True for item in examples))

    def test_clean_slate_inventory_is_empty_and_paper_examples_do_not_register(self):
        library = prepare_library({"tables": []}, mode="clean_slate")
        inventory = library.as_dict()
        self.assertEqual(inventory["mode"], "clean_slate")
        self.assertEqual(inventory["operators"], [])
        self.assertEqual(inventory["playbooks"], [])
        self.assertTrue(paper_reference_examples())
        self.assertEqual(library.as_dict()["operators"], [])

    def test_default_library_inventory_omits_source_and_records_mode(self):
        library = prepare_library({"tables": []}, mode="paper")
        inventory = library.as_dict()
        self.assertEqual(inventory["mode"], "paper")
        self.assertTrue(inventory["origin"])
        self.assertTrue(inventory["operators"])
        for operator in inventory["operators"]:
            self.assertNotIn("source", operator)

    def test_select_cannot_resolve_a_schema_filtered_default(self):
        compatible = Playbook("compatible", "usable for this schema", {"nodes": []})
        schema_filtered = [compatible]

        chosen = _select_available_playbook(schema_filtered, "unavailable-cloud-default")

        self.assertIs(chosen, compatible)
        self.assertNotEqual(chosen.name, "unavailable-cloud-default")

    def test_select_empty_active_library_returns_none(self):
        self.assertIsNone(_select_available_playbook([], "paper.default"))

    def test_metric_only_schema_filters_composition_but_keeps_new_source_for_serve(self):
        schema_doc = {
            "tables": [{
                "name": "metrics.csv",
                "record_type": "metric",
                "columns": [
                    {"name": "timestamp", "data_type": "timestamp"},
                    {"name": "entity", "data_type": "string"},
                    {"name": "metric_name", "data_type": "string"},
                    {"name": "value", "data_type": "number"},
                ],
                "entity_roles": ["service"],
            }],
            "time_unit": "ms_utc",
        }
        schema = normalize_dataset_schema(schema_doc)
        library = prepare_library(schema, mode="paper")
        active = library.signatures_for_schema(schema)
        active_names = {item["name"] for item in active}
        self.assertIn("paper.backend.metric_conditions", active_names)
        self.assertNotIn("paper.scores.dependency_adjust", active_names)

        compose = format_compose_prompt("case", [], active, {}, dataset_schema=schema.as_dict())
        evolve = format_evolve_prompt("case", [], {}, schema.as_dict(), available_operators=active)
        self.assertNotIn("paper.scores.dependency_adjust", compose)
        self.assertNotIn("paper.scores.dependency_adjust", evolve)

        candidate_registry = library.registry.clone()
        candidate_registry.register(OperatorSpec(
            name="evolved.new_signal",
            version="1",
            inputs=(PortSpec("data", "Telemetry"),),
            output_type="ScoredCandidates",
            family="custom_metric",
            description="Newly validated task-specific signal.",
        ))
        serve_catalog = library.signatures_for_schema(schema, registry=candidate_registry)
        serve_names = {item["name"] for item in serve_catalog}
        serve = format_serve_prompt("case", [{"name": "evolved.new_signal"}], serve_catalog, {})
        self.assertIn("evolved.new_signal", serve_names)
        self.assertIn("evolved.new_signal", serve)
        self.assertNotIn("paper.scores.dependency_adjust", serve)


if __name__ == "__main__":
    unittest.main()
