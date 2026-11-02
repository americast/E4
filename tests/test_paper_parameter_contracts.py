"""Declared semantic and numeric parameter contracts for paper operators."""

from __future__ import annotations

import copy
import unittest

from e4core.dag import DAGValidationError, verify_dag
from e4core.executor import SerialExecutor
from e4core.m4_fixtures import synthetic_case_runtime
from e4core.paper_starter import (intern_paper_parameter_choices,
    make_paper_starter_registry, paper_parameter_catalog)
from e4core.runtime import (ParameterSpec, RuntimeContext, RuntimeField, RuntimeSchema,
    RuntimeResolutionError, _validate_parameter)


class PaperParameterContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = make_paper_starter_registry(bank_only=True)
        cls.full_registry = make_paper_starter_registry(bank_only=False)
        cls.runtime = synthetic_case_runtime()
        cls.base = {"schema_version": "e4-dag/1", "nodes": [
            {"id": "telemetry", "operator": "case.telemetry", "version": "1",
             "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter", "operator": "paper.input.prepare", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate", "version": "1",
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "score", "operator": "paper.backend.tail_permutation", "version": "1",
             "inputs": {"data": "starter", "candidates": "candidates"},
             "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus", "version": "1",
             "inputs": {"scores": ["score"]}, "params": {}, "bindings": {}}],
            "return": {"node": "answer"}}

    def _doc(self, parameter, value, *, binding=False, operator="paper.backend.tail_permutation"):
        document = copy.deepcopy(self.base)
        score = document["nodes"][3]
        score["operator"] = operator
        if operator == "paper.urls.flow_contribution":
            document["nodes"][2] = {"id": "candidates", "operator": "paper.urls.delay_conditions",
                "version": "1", "inputs": {"data": "starter"}, "params": {}, "bindings": {}}
        if binding:
            score["bindings"][parameter] = value
        else:
            score["params"][parameter] = value
        return document

    def test_native_metric_and_app_kpi_enums_reject_wrong_field(self):
        metric_doc = self._doc("metric", "mrt", operator="paper.backend.mean_welch")
        with self.assertRaises(DAGValidationError):
            verify_dag(metric_doc, self.registry, runtime=self.runtime)
        app_doc = self._doc("app_kpi_field", "CPUUtil", operator="paper.bank.metric_tree_likelihood")
        with self.assertRaises(DAGValidationError):
            verify_dag(app_doc, self.registry, runtime=self.runtime)

    def test_enum_and_numeric_math_bounds_fail_static_and_direct(self):
        cases = (
            ("quantile", 1.0, "paper.backend.tail_permutation"),
            ("alpha", 0.0, "paper.backend.tail_permutation"),
            ("minimum_samples", 1, "paper.backend.mean_welch"),
            ("fit_mode", "future", "paper.bank.trace_root_flow"),
            ("validation_fraction", 1.0, "paper.bank.trace_root_flow"),
            ("residual_scale_floor", 0.0, "paper.bank.trace_root_flow"),
            ("ridge", -1.0, "paper.urls.flow_contribution"),
        )
        for parameter, value, operator in cases:
            with self.subTest(parameter=parameter, operator=operator):
                with self.assertRaises(DAGValidationError):
                    verify_dag(self._doc(parameter, value, operator=operator),
                        self.registry, runtime=self.runtime)
        tail = self.registry.require("paper.backend.tail_permutation", "1")
        with self.assertRaises(ValueError):
            tail.implementation({}, {"quantile": 1.0}, None)
        scorer = self.registry.require("paper.backend.mean_welch", "1")
        with self.assertRaises(ValueError):
            scorer.implementation({}, {"metric": "mrt"}, None)

    def test_runtime_bound_selector_is_checked_before_external_loader(self):
        document = self._doc("metric", "runtime.component_metric", binding=True,
            operator="paper.backend.mean_welch")
        schema = RuntimeSchema({"component_metric": RuntimeField("component_metric", "string",
            "component metric selector", "run")}, "paper-selector-test/1")
        runtime = RuntimeContext.build(schema, {"component_metric": "mrt"})
        loader_called = False

        def external_loader():
            nonlocal loader_called
            loader_called = True
            return {}

        with self.assertRaises(DAGValidationError):
            SerialExecutor(self.registry).execute_document(document, runtime, external_loader)
        self.assertFalse(loader_called)

        fraction_document = self._doc("validation_fraction", "runtime.validation_fraction",
            binding=True, operator="paper.bank.trace_root_flow")
        fraction_schema = RuntimeSchema({"validation_fraction": RuntimeField(
            "validation_fraction", "number", "heldout fraction", "run")}, "fraction-test/1")
        fraction_runtime = RuntimeContext.build(fraction_schema, {"validation_fraction": 1.0})
        with self.assertRaises(DAGValidationError):
            SerialExecutor(self.registry).execute_document(fraction_document, fraction_runtime, external_loader)
        self.assertFalse(loader_called)

    def test_helper_supported_edge_values_and_valid_wider_search_breadth_remain_accepted(self):
        for quantile in (0.0, 1.0):
            with self.subTest(url_delay_quantile=quantile):
                url_delay = copy.deepcopy(self.base)
                url_delay["nodes"][2].update({"operator": "paper.urls.delay_conditions",
                    "inputs": {"data": "starter"}, "params": {"quantile": quantile}})
                url_delay["nodes"][3].update({"operator": "paper.urls.welch",
                    "inputs": {"data": "starter", "candidates": "candidates"}})
                verify_dag(url_delay, self.full_registry, runtime=self.runtime)
        # A large nonnegative consensus tolerance is permitted by its helper.
        tie_doc = copy.deepcopy(self.base)
        tie_doc["nodes"][4]["params"]["tie_tolerance"] = 2.0
        verify_dag(tie_doc, self.registry, runtime=self.runtime)

        breadth = copy.deepcopy(self.base)
        candidate = breadth["nodes"][2]
        candidate.update({"operator": "paper.cohorts.conjunction",
            "inputs": {"data": "starter"},
            "params": {"maximum_order": 8, "minimum_support": 1}})
        score = breadth["nodes"][3]
        score.update({"operator": "paper.cohorts.permutation",
            "inputs": {"data": "starter", "candidates": "candidates"}})
        answer = breadth["nodes"][4]
        answer["inputs"] = {"scores": ["score"]}
        verify_dag(breadth, self.full_registry,
            runtime=self.runtime, max_nodes=40, max_depth=12)

    def test_long_enum_is_interned_once_in_prompt_display_not_registry(self):
        signatures = self.registry.signatures()
        metric_parameters = [row["parameters"]["metric"]["choices"] for row in signatures
            if row["name"] in {"paper.backend.mean_welch", "paper.backend.location_permutation",
                               "paper.backend.tail_permutation"}]
        self.assertGreater(len(metric_parameters), 1)
        self.assertTrue(all(len(values) > 1 for values in metric_parameters))
        displayed = intern_paper_parameter_choices(signatures)
        refs = [row["parameters"]["metric"].get("choices_ref") for row in displayed
            if row["name"] in {"paper.backend.mean_welch", "paper.backend.location_permutation",
                               "paper.backend.tail_permutation"}]
        self.assertEqual(refs, ["paper.bank_component_metric_fields"] * len(refs))
        self.assertEqual(len(paper_parameter_catalog()["enums"]["paper.bank_component_metric_fields"]["values"]),
                         len(metric_parameters[0]))
        legacy_window = self.registry.require("bank.metrics.aggregate",
            next(row["version"] for row in self.registry.signatures()
                 if row["name"] == "bank.metrics.aggregate")).parameters["window"]
        self.assertFalse(legacy_window.constraints)
        legacy_signature = next(row for row in self.registry.signatures()
            if row["name"] == "bank.metrics.aggregate")
        self.assertNotIn("constraints", legacy_signature["parameters"]["window"])

    def test_constraint_metadata_is_finite_typed_supported_and_immutable(self):
        bad_constraints = (
            {"allow_empty": True},
            {"minimum": float("nan")},
            {"maximum": "1"},
            {"exclusive_maximum": 1, "maximum": 1.0},
            {"exclusive_minimum": True},
            {"minimum": 2.0, "maximum": 1.0},
        )
        for value in bad_constraints:
            with self.subTest(constraints=value), self.assertRaises(ValueError):
                ParameterSpec("bounded", "number", constraints=value)
        spec = ParameterSpec("bounded", "number", constraints={"minimum": 0.0})
        with self.assertRaises(TypeError):
            spec.constraints["minimum"] = -1.0

    def test_empty_legacy_constraints_do_not_float_convert_integers(self):
        legacy = ParameterSpec("count", "integer")
        huge = 10 ** 10000
        _validate_parameter(legacy, huge)
        bounded = ParameterSpec("count", "integer", constraints={
            "minimum": 2 ** 60, "maximum": 2 ** 60 + 1})
        _validate_parameter(bounded, 2 ** 60 + 1)
        with self.assertRaises(RuntimeResolutionError):
            _validate_parameter(bounded, 2 ** 60 + 2)


if __name__ == "__main__":
    unittest.main()
