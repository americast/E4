"""Lossless compact columnar prompt view of registered serving signatures."""
from __future__ import annotations

import copy
import hashlib
import json
import unittest

from e4core.cache import canonical_json
from e4core.m4_controller import (_paper_evolve_operator_catalog,
    _paper_registered_signature_catalog, _expand_paper_registered_signature_catalog)
from e4core.m4_prompt_catalog import (PromptCatalogError,
    expand_registered_signature_catalog, pack_registered_signature_catalog)
from e4core.paper_starter import make_paper_starter_registry


class PromptCatalogPackingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = make_paper_starter_registry(bank_only=True)
        cls.catalog = _paper_registered_signature_catalog(cls.registry.signatures())

    def test_real_serving_catalog_roundtrips_exactly_and_compacts_repeated_contract_keys(self):
        packed = pack_registered_signature_catalog(self.catalog)
        expanded = expand_registered_signature_catalog(packed)
        self.assertEqual(expanded, self.catalog)
        self.assertEqual(expanded["operators"], self.catalog["operators"])
        self.assertEqual(list(expanded["parameter_contracts"]),
            list(self.catalog["parameter_contracts"]))
        self.assertEqual(packed["operator_table"]["column_order"], [
            "name", "version", "output_type", "family", "description"])
        self.assertEqual(packed["operator_table"]["input_row_columns"], ["name", "contract_ref"])
        self.assertLess(len(canonical_json(packed)), len(canonical_json(self.catalog)) * 0.75)
        self.assertEqual(len(packed["parameter_contract_table"]["choice_lists"]), 2)

    def test_mixed_v2_table_group_contract_remains_explicit_and_roundtrips_losslessly(self):
        requirements = {"reference": {"source_columns": {
                "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
                "metric/metric_app.csv": ["timestamp", "tc", "mrt"]},
            "statistics": ["median"], "group_by": [], "support_unit": "timestamp",
            "coverage": "full_window", "group_by_by_table": {
                "metric/metric_container.csv": ["component"],
                "metric/metric_app.csv": ["app_kpi"]}}}
        signature = {"name": "evolved.mixed_group_fixture", "version": "v2+fixture",
            "output_type": "ScoredCandidates", "family": "metric",
            "description": "", "inputs": [], "parameters": {},
            "input_requirements": requirements}
        catalog = _paper_registered_signature_catalog([signature])
        row = catalog["operators"][0]
        contract = catalog["input_requirement_contracts"][row["input_requirements_ref"]]
        reference = contract["reference"]
        self.assertEqual(reference["group_by_by_table"], {
            "metric/metric_container.csv": ["component"],
            "metric/metric_app.csv": ["app_kpi"]})
        expanded = _expand_paper_registered_signature_catalog(catalog)
        self.assertEqual(expanded["operators"][0]["input_requirements"], requirements)

        evolve = _paper_evolve_operator_catalog([signature])
        evolve_ref = evolve["operators"][0]["input_requirements_ref"]
        self.assertEqual(evolve["input_requirement_contracts"][evolve_ref]["reference"]["group_by_by_table"], {
            "metric/metric_container.csv": ["component"],
            "metric/metric_app.csv": ["app_kpi"]})

    def test_model_facing_catalog_omits_only_parameter_description_prose(self):
        packed = pack_registered_signature_catalog(self.catalog,
            omit_parameter_descriptions=True)
        expanded = expand_registered_signature_catalog(packed)
        metadata = packed["parameter_description_omission"]
        descriptions = {ref: contract["description"] for ref, contract in
            self.catalog["parameter_contracts"].items()
            if isinstance(contract.get("description"), str) and contract["description"]}
        expected_hash = hashlib.sha256(json.dumps(descriptions, ensure_ascii=False,
            sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
        self.assertEqual(metadata["schema_version"], "e4-paper-parameter-description-omission/1")
        self.assertEqual(metadata["omitted_parameter_description_count"], len(descriptions))
        self.assertEqual(metadata["omitted_parameter_description_map_sha256"], expected_hash)
        self.assertEqual(metadata["full_host_catalog_ref"],
            "trusted_registered_operator_signatures.parameter_contracts")
        self.assertGreater(metadata["omitted_parameter_description_count"], 0)
        self.assertEqual(expanded["operators"], self.catalog["operators"])
        self.assertEqual(expanded["input_contracts"], self.catalog["input_contracts"])
        self.assertEqual(expanded["input_requirement_contracts"],
            self.catalog["input_requirement_contracts"])
        for ref, host_contract in self.catalog["parameter_contracts"].items():
            model_contract = expanded["parameter_contracts"][ref]
            omitted_description = isinstance(host_contract.get("description"), str) and bool(
                host_contract["description"])
            expected = ({key: value for key, value in host_contract.items()
                if key != "description"} if omitted_description else host_contract)
            self.assertEqual(model_contract, expected)
            if omitted_description:
                self.assertNotIn("description", model_contract)

    def test_sparse_optional_fields_and_null_zero_contract_values_are_lossless(self):
        source = copy.deepcopy(self.catalog)
        source["operators"][0]["input_requirements_ref"] = "req-exact"
        source["input_requirement_contracts"]["req-exact"] = {
            "minimum": 0, "maximum": None, "enabled": False}
        source["operators"][0]["input_requirements_ref"] = "req-exact"
        source["operators"][1]["external_input"] = False
        source["parameter_contracts"]["contract-missing"] = {"type": "integer"}
        source["parameter_contracts"]["contract-null"] = {
            "type": "integer", "default": None, "has_default": True}
        source["parameter_contracts"]["contract-zero"] = {
            "type": "integer", "default": 0, "has_default": True}
        source["operators"][0]["parameters"].update({
            "missing_default": "contract-missing", "null_default": "contract-null",
            "zero_default": "contract-zero"})
        packed = pack_registered_signature_catalog(source)
        self.assertEqual(expand_registered_signature_catalog(packed), source)
        extras = packed["operator_table"]["sparse_extras_by_operator"]
        self.assertEqual(extras["0"]["input_requirements_ref"], "req-exact")
        self.assertIs(extras["1"]["external_input"], False)
        table = packed["parameter_contract_table"]
        self.assertEqual(len(table["row_order"]), len(source["parameter_contracts"]))
        self.assertNotIn("input_requirement_contracts", packed)
        self.assertEqual(packed["input_requirement_contract_table"]["row_order"], ["req-exact"])

    def test_contract_shapes_and_shared_choice_ids_are_stable_under_operator_reordering(self):
        source = copy.deepcopy(self.catalog)
        packed = pack_registered_signature_catalog(source)
        reordered = copy.deepcopy(source)
        reordered["operators"] = list(reversed(reordered["operators"]))
        packed_reordered = pack_registered_signature_catalog(reordered)
        self.assertEqual(packed["parameter_contract_table"]["choice_lists"],
            packed_reordered["parameter_contract_table"]["choice_lists"])
        self.assertEqual(packed["parameter_contract_table"]["shape_tables"],
            packed_reordered["parameter_contract_table"]["shape_tables"])
        self.assertEqual(expand_registered_signature_catalog(packed_reordered), reordered)

    def test_tampered_choice_reference_fails_closed(self):
        packed = pack_registered_signature_catalog(self.catalog)
        self.assertEqual(expand_registered_signature_catalog(packed), self.catalog)
        bad_choice = copy.deepcopy(packed)
        table = bad_choice["parameter_contract_table"]
        for shape in table["shape_tables"]:
            for row in shape["rows"]:
                for index, key in enumerate(shape["column_order"], start=1):
                    if key == "choices" and isinstance(row[index], dict):
                        row[index]["$choice_list_ref"] = "choices_unknown"
                        with self.assertRaisesRegex(PromptCatalogError, "unknown choice"):
                            expand_registered_signature_catalog(bad_choice)
                        return
        self.fail("real catalog did not contain a shared choice-list reference to tamper")

    def test_evolve_catalog_roundtrips_identity_requirements_and_optional_fields(self):
        source = _paper_evolve_operator_catalog(self.registry.signatures())
        source["operators"][0].pop("external_input", None)
        if len(source["operators"]) > 1:
            source["operators"][1]["external_input"] = None
        packed = pack_registered_signature_catalog(source)
        self.assertEqual(expand_registered_signature_catalog(packed), source)
        self.assertEqual(packed["source_schema"], "e4-paper-evolve-operator-catalog/2")
        self.assertEqual(packed["operator_table"]["shared_fields"][
            "omitted_serving_dag_fields"], ["inputs", "parameters"])
        self.assertNotIn("omitted_serving_dag_fields",
            packed["operator_table"]["column_order"])
        self.assertIn("shared_fields apply to every row", packed["packing_guide"])
        self.assertEqual([row[0] for row in packed["operator_table"]["rows"]],
            [row["name"] for row in source["operators"]])
        optional = packed["operator_table"]["sparse_extras_by_operator"]
        self.assertNotIn("external_input", optional.get("0", {}))
        if len(source["operators"]) > 1:
            self.assertIsNone(optional["1"]["external_input"])
        self.assertEqual(list(expand_registered_signature_catalog(packed)[
            "input_requirement_contracts"]), list(source["input_requirement_contracts"]))


if __name__ == "__main__":
    unittest.main()
