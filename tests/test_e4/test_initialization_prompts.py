from __future__ import annotations

import ast
import contextlib
import io
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

from e4.cli import main
from e4.data.agent_prompt import get_adapter_generation_prompt
from e4.data.submissions import adapter_code_template, adapter_submission_schema
from e4.prompts import load_initialization_resource, render_initialization_prompt
from e4.prompts.templates import (
    format_compose_prompt,
    format_evolve_prompt,
    format_repair_prompt,
    format_select_prompt,
    format_serve_prompt,
)


class InitializationPromptTests(unittest.TestCase):
    def test_default_is_pre_run_paper_bootstrap_and_discloses_pending_schema(self):
        rendered = render_initialization_prompt()
        self.assertIn("PRE-RUN BOOTSTRAP (PAPER DEFAULT)", rendered)
        self.assertIn("PENDING: provide validated adapter schema JSON", rendered)
        self.assertIn("active compatibility and inventory are not asserted", rendered)
        self.assertIn("reference-only", rendered.lower())
        self.assertIn("one generation response", rendered.lower())
        self.assertIn("at most **3 new operators**", rendered)
        self.assertIn("reported 19 operators and 11 playbooks describe catalog coverage", rendered)
        self.assertIn("actual implemented, schema-compatible capabilities", rendered)

    def test_clean_slate_is_distinct_and_keeps_examples_reference_only(self):
        rendered = render_initialization_prompt(clean_slate=True)
        self.assertIn("CLEAN-SLATE GENERATION", rendered)
        self.assertIn("Mode: `clean_slate`", rendered)
        self.assertIn("automatic bootstrap is skipped", rendered)
        self.assertIn("inventory is **empty**", rendered)
        self.assertIn("reference-only", rendered.lower())
        self.assertNotIn("PRE-RUN BOOTSTRAP", rendered)

    def test_bank_essentials_and_priors_remain_separate_and_attributed(self):
        with self.assertRaisesRegex(ValueError, "requires include_bank=True"):
            render_initialization_prompt("some other domain", include_domain_priors=True)
        essential = render_initialization_prompt(include_bank=True)
        self.assertIn("bank/metric_container.csv", essential)
        self.assertNotIn("evaluation-informed variant", essential)
        informed = render_initialization_prompt(include_bank=True, include_domain_priors=True)
        self.assertIn("prior inspection of scored evaluation failures", informed)
        self.assertNotIn("generated_use", informed)
        self.assertNotIn("benefit_evidence", informed)

    def test_schema_input_is_rendered_with_real_active_and_reference_context(self):
        schema = {"tables": [], "time_unit": "ms_utc", "description": "test public schema"}
        paper = render_initialization_prompt(schema=schema)
        clean = render_initialization_prompt(schema=schema, clean_slate=True)
        self.assertIn("test public schema", paper)
        self.assertIn('"mode":"paper"', paper)
        self.assertIn('"mode":"clean_slate"', clean)
        self.assertIn('"operators":[]', clean)
        self.assertIn("Reference-only paper examples", clean)

        active_text = (
            paper.split("### Active compatible callable inventory", 1)[1]
            .split("### Paper catalog compatibility", 1)[0]
            .strip()
        )
        active = json.loads(active_text)
        self.assertIsInstance(active, dict)
        self.assertIsInstance(active["operators"], list)
        self.assertTrue(all(isinstance(operator, dict) for operator in active["operators"]))
        consensus = next(
            operator for operator in active["operators"]
            if operator["name"] == "paper.answers.weighted_consensus"
        )
        self.assertIsInstance(consensus["parameters"]["family_weights"], dict)

    def test_domain_brief_and_fillable_template(self):
        custom = "Rank only identities backed by deployment mappings."
        self.assertIn(custom, render_initialization_prompt(custom))
        self.assertIn("Domain brief (fillable template)", render_initialization_prompt())

    def test_bundled_prompt_resources_are_installed_and_readable(self):
        for name in (
            "initialization_system.md",
            "domain_brief_template.md",
            "bank_domain_brief.md",
            "bank_domain_priors.md",
            "adapter_system.md",
            "bootstrap_system.md",
            "clean_slate_system.md",
        ):
            with self.subTest(name=name):
                self.assertTrue(load_initialization_resource(name).strip())
        with self.assertRaises(ValueError):
            load_initialization_resource("../pyproject.toml")

    def test_adapter_prompt_is_strict_json_and_embeds_the_package_contract(self):
        prompt = get_adapter_generation_prompt(
            "timestamp, host, latency\n...",
            domain_brief="Rank hosts based on observable failure evidence.",
            public_metadata_text="timestamps are seconds; host_id is documented identity",
        )
        self.assertIn("Return exactly one JSON object", prompt)
        self.assertIn(json.dumps(adapter_submission_schema(), separators=(",", ":")), prompt)
        self.assertIn(adapter_code_template(), prompt)
        self.assertIn("Rank hosts based on observable failure evidence.", prompt)
        self.assertIn("timestamps are seconds", prompt)
        self.assertIn("timestamp, host, latency", prompt)
        self.assertIn("status: \"ready\"", prompt)
        self.assertIn("status: \"needs_input\"", prompt)
        self.assertNotIn("Return only the Python module", prompt)

    def test_cli_preserves_adapter_prompt_and_supports_metadata_domain_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            sample = f"{directory}/sample.csv"
            brief = f"{directory}/domain.md"
            metadata = f"{directory}/metadata.md"
            with open(sample, "w", encoding="utf-8") as target:
                target.write("timestamp,host,latency")
            with open(brief, "w", encoding="utf-8") as target:
                target.write("Diagnose host latency failures.")
            with open(metadata, "w", encoding="utf-8") as target:
                target.write("host is the documented identity field.")
            with patch.object(sys, "argv", ["e4", "adapter-prompt", sample, "--domain-file", brief, "--metadata-file", metadata]):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    main()
        self.assertIn("Diagnose host latency failures.", output.getvalue())
        self.assertIn("host is the documented identity field.", output.getvalue())
        self.assertIn("timestamp,host,latency", output.getvalue())

    def test_cli_initialization_default_and_clean_slate_are_offline(self):
        with tempfile.TemporaryDirectory() as directory:
            brief = f"{directory}/domain.md"
            schema = f"{directory}/schema.json"
            with open(brief, "w", encoding="utf-8") as target:
                target.write("Only explicit identity links are valid.")
            with open(schema, "w", encoding="utf-8") as target:
                json.dump({"tables": [], "description": "public schema"}, target)
            for args, expected in (([], "PRE-RUN BOOTSTRAP"), (["--clean-slate"], "CLEAN-SLATE GENERATION")):
                with patch.object(sys, "argv", ["e4", "initialization-prompt", brief, "--schema", schema, *args]):
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        main()
                self.assertIn("no model is called", output.getvalue().lower())
                self.assertIn("Only explicit identity links are valid.", output.getvalue())
                self.assertIn("public schema", output.getvalue())
                self.assertIn(expected, output.getvalue())

    def test_runtime_prompts_receive_actual_bindings_schema_and_shared_authoring(self):
        select = format_select_prompt(
            "case",
            [{"name": "pb", "dag": {"nodes": [{"id": "n1", "operator": "x", "params": {"table": "t"}}]}}],
            {},
            operators=[{"name": "x", "parameters": {"table": {"required": True}}}],
        )
        self.assertIn('"id": "n1"', select)
        self.assertIn('"table"', select)
        self.assertIn("Do not invent", select)
        self.assertNotIn('"rationale"', select)
        compose = format_compose_prompt("case", [{"stage": "select"}], [], {}, {"tables": [{"name": "t"}]})
        self.assertIn('"name": "t"', compose)
        self.assertNotIn('"adaptation_rationale"', compose)
        evolve = format_evolve_prompt("case", [], {}, {"tables": []})
        repair = format_repair_prompt(["bad type"], {"evolved.x": "def operator(): pass"})
        for prompt in (evolve, repair):
            self.assertIn("AggregateGroupView", prompt)
            self.assertIn("Complete log summary with coverage", prompt)

    def test_serve_is_separate_dag_composition_and_accepts_both_core_answers(self):
        old = format_serve_prompt("case", [], [], {})
        self.assertIn("Prior diagnosis attempts and diagnostics:\n[]", old)
        self.assertIn("Available public dataset schema:\n{}", old)
        self.assertIn("ScoredCandidates", old)
        self.assertIn("RankedAnswer", old)
        self.assertNotIn('"serving_rationale"', old)
        enriched = format_serve_prompt(
            "case", [], [], {}, prior_attempts=[{"error": "bad binding"}], dataset_schema={"tables": []}
        )
        self.assertIn("bad binding", enriched)
        self.assertIn('"tables": []', enriched)

    def test_source_code_survives_json_newline_serialization(self):
        rendered = render_initialization_prompt()
        bundle_block = (
            rendered.split("## Current proposal and playbook JSON shapes", 1)[1]
            .split("```json", 1)[1]
            .split("```", 1)[0]
        )
        bundle = json.loads(bundle_block)
        self.assertEqual(set(bundle), {"status", "proposed_operators", "gap_record", "playbooks"})
        required_operator_fields = {
            "name", "version", "authoring_profile", "description", "family",
            "input_ports", "output_type", "parameters", "input_requirements", "source",
        }
        self.assertTrue(required_operator_fields.issubset(bundle["proposed_operators"][0]))
        self.assertEqual(bundle["proposed_operators"][0]["input_ports"][0]["name"], "data")
        self.assertEqual(bundle["playbooks"][0]["dag"]["nodes"][0]["inputs"]["data"], "telemetry")
        self.assertNotIn("capability_summary", bundle)
        self.assertNotIn("assumptions", bundle)
        self.assertNotIn("knowledge_ledger", bundle)
        self.assertNotIn("initialization_provenance", bundle)
        dag_block = (
            rendered.split("A DAG object on its own uses this same node and parameter contract:", 1)[1]
            .split("```json", 1)[1]
            .split("```", 1)[0]
        )
        dag = json.loads(dag_block)
        self.assertEqual(dag["return_node"], "score")
        self.assertEqual(dag["nodes"][0]["params"]["source_table"], "<exact declared table>")
        block = (
            rendered.split("When a JSON response contains multiline Python", 1)[1]
            .split("```json", 1)[1]
            .split("```", 1)[0]
        )
        proposal = json.loads(block)
        self.assertIn("\n    ...", proposal["source"])
        self.assertNotIn("\\n", proposal["source"])
        ast.parse(proposal["source"])


if __name__ == "__main__":
    unittest.main()
