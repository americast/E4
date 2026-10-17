import hashlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e4core.bank_ops import bank_registry
from e4core.cache import canonical_json
from e4core.contracts import (Aggregate, EntityRef, Evidence, ScoredCandidate,
                              ScoredCandidates, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.m4_data import make_m4_registry, query_bank_public
from e4core.m4_generated import (GeneratedSpecError, PARAMETER_TYPES, PARAM_FIELDS,
                                 PORT_FIELDS, PORT_TYPES, SPEC_FIELDS,
                                 assert_snapshot_registry_compatible,
                                 generated_spec_contract_schema, register_generated_specs,
                                 validate_generated_spec)
from e4core.registry import OperatorRegistry


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = Window("reference", 1_700_000_000_000, 1_700_001_800_000)
ALERT = Window("alert", 1_700_001_800_000, 1_700_003_600_000)
SOURCE = '''def operator(inputs, params, runtime):
    aggregate = contract_fields(inputs["data"], "Aggregate")
    query = aggregate["metadata"]["public_query"]
    grouped = {}
    for row in query["rows_data"]:
        raw = row.get("value", "")
        if raw:
            entity_id = row["cmdb_id"]
            signal = max(0, float(raw) - params["threshold"])
            current = grouped.get(entity_id, {"score": 0, "support": 0, "value": 0})
            grouped[entity_id] = {"score": max(current["score"], signal),
                                  "support": current["support"] + 1,
                                  "value": max(current["value"], float(raw))}
    window = aggregate["window"]
    candidates = []
    for entity_id, item in grouped.items():
        if item["support"] >= params["minimum_support"] and item["score"] > 0:
            entity = {"$type": "EntityRef", "$fields":
                      {"role": "component", "identifier": entity_id}}
            evidence = {"$type": "Evidence", "$fields": {
                "source": "bank/metric_container.csv", "window": window,
                "statistic": "synthetic_parameterized_signal", "value": item["value"],
                "contributing_entities": {"$tuple": [entity]}, "support": item["support"],
                "details": {"$map": {"ranking_eligible": True}}}}
            candidates.append({"$type": "ScoredCandidate", "$fields": {
                "entity": entity, "score": item["score"], "category": None,
                "evidence": {"$tuple": [evidence]}, "analysis_family": "bank_metric"}})
    status = "ready" if candidates else "abstained"
    return {"$type": "ScoredCandidates", "$fields": {
        "candidates": {"$tuple": candidates}, "scorer": "evolved.synthetic_signal",
        "status": status, "status_details": {"$map": {}}}}
'''


def spec(source=SOURCE):
    return {"name": "evolved.synthetic_signal", "version": "1", "description": "Synthetic bounded signal scorer",
        "family": "bank_metric", "input_ports": [{"name": "data", "type": "Aggregate"}],
        "output_type": "ScoredCandidates", "parameters": {
            "threshold": {"type": "number", "required": True,
                          "description": "Frozen synthetic signal threshold"},
            "minimum_support": {"type": "positive_integer", "runtime_field": "minimum_support"}},
        "source": source}


class M4GeneratedOperatorTests(unittest.TestCase):
    def test_public_schema_matches_validator_and_rejects_port_mapping(self):
        schema = generated_spec_contract_schema()
        self.assertEqual(schema["top_level"]["required_fields"], sorted(SPEC_FIELDS))
        self.assertEqual(schema["input_ports"]["allowed_fields"], sorted(PORT_FIELDS))
        self.assertEqual(schema["input_ports"]["fields"]["type"]["enum"], sorted(PORT_TYPES))
        self.assertEqual(schema["parameters"]["entry_allowed_fields"], sorted(PARAM_FIELDS))
        self.assertEqual(schema["parameters"]["entry_fields"]["type"]["enum"],
                         sorted(PARAMETER_TYPES))
        self.assertEqual(schema["input_ports"]["min_items"], 1)
        self.assertEqual(schema["input_ports"]["max_items"], 8)
        self.assertEqual(validate_generated_spec(spec())["input_ports"][0]["name"], "data")

        mapped_ports = spec()
        mapped_ports["input_ports"] = {"data": "Aggregate"}
        with self.assertRaisesRegex(GeneratedSpecError,
                "JSON array.*name/type.*mappings are not accepted"):
            validate_generated_spec(mapped_ports)

    def test_normalized_spec_is_idempotent_and_source_bound(self):
        normalized = validate_generated_spec(spec())
        self.assertEqual(validate_generated_spec(normalized), normalized)
        self.assertRegex(normalized["version"], r"^1\+[0-9a-f]{16}$")
        changed = validate_generated_spec(spec(SOURCE.replace("max(0,", "max(1,")))
        self.assertNotEqual(normalized["version"], changed["version"])
        self.assertNotEqual(normalized["source_sha256"], changed["source_sha256"])

    def test_spec_rejects_unregistered_numbers_bad_signature_and_non_namespaced_name(self):
        hardcoded = SOURCE.replace('params["threshold"]', '2')
        with self.assertRaisesRegex(GeneratedSpecError, "numeric constants"):
            validate_generated_spec(spec(hardcoded))
        bad_signature = SOURCE.replace("def operator(inputs, params, runtime):",
                                       "def operator(inputs, params):")
        with self.assertRaisesRegex(GeneratedSpecError, "sandbox syntax contract"):
            validate_generated_spec(spec(bad_signature))
        bad_name = spec()
        bad_name["name"] = "bank.synthetic_signal"
        with self.assertRaisesRegex(GeneratedSpecError, "evolved"):
            validate_generated_spec(bad_name)
        bad_default = spec()
        bad_default["parameters"]["minimum_support"]["default"] = 0
        with self.assertRaisesRegex(GeneratedSpecError, "invalid default"):
            validate_generated_spec(bad_default)
        bad_choice = spec()
        bad_choice["parameters"]["threshold"]["choices"] = ["high"]
        with self.assertRaisesRegex(GeneratedSpecError, "choice with the wrong type"):
            validate_generated_spec(bad_choice)

    def test_snapshot_compatibility_checks_frozen_builtin_signatures(self):
        registry = bank_registry(root=ROOT)
        rows = registry.signatures()
        entries = []
        policy_versions = {}
        for signature in rows:
            identity = signature["name"] + "@" + signature["version"]
            encoded = json.dumps(signature, ensure_ascii=False, sort_keys=True,
                                  separators=(",", ":"), allow_nan=False).encode("utf-8")
            entries.append({"name": signature["name"], "version": signature["version"],
                "kind": "builtin", "signature": signature,
                "signature_sha256": hashlib.sha256(encoded).hexdigest()})
            policy_versions[identity] = signature["version"]
        snapshot = {"operators": tuple(entries),
                    "policy": {"builtin_policy_versions": policy_versions}}
        evolved_registry = bank_registry(root=ROOT)
        register_generated_specs(evolved_registry, [spec()])
        assert_snapshot_registry_compatible(snapshot, evolved_registry)
        changed = json.loads(json.dumps(snapshot))
        changed["operators"][0]["signature"]["description"] += " changed"
        with self.assertRaisesRegex(ValueError, "signature mismatch"):
            assert_snapshot_registry_compatible(changed, evolved_registry)

    @unittest.skipUnless(os.environ.get("E4_TEST_BWRAP") == "1",
                         "requires the approved bubblewrap namespace test environment")
    def test_query_generated_scorer_and_combiner_cover_positive_null_missing_and_confounded_fixtures(self):
        scenarios = {
            "known_positive": [
                {"timestamp": "1700001801000", "cmdb_id": "fixture-cause",
                 "kpi_name": "CPUUtil", "value": "10"}],
            "null_signal": [
                {"timestamp": "1700001801000", "cmdb_id": "fixture-null",
                 "kpi_name": "CPUUtil", "value": ""}],
            "missing_signal": [],
            "confounded": [
                {"timestamp": "1700001801000", "cmdb_id": "fixture-cause",
                 "kpi_name": "CPUUtil", "value": "10"},
                {"timestamp": "1700001802000", "cmdb_id": "fixture-confounder",
                 "kpi_name": "CPUUtil", "value": "6"}],
        }
        expected = {"known_positive": ["fixture-cause"], "null_signal": [],
                    "missing_signal": [], "confounded": ["fixture-cause", "fixture-confounder"]}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config"
            config.mkdir()
            for name in ("bank_metric_kind_policy.json", "bank_dev_kpi_policy.json"):
                shutil.copyfile(ROOT / "config" / name, config / name)
            reference = Window("reference", 1_700_000_000_000, 1_700_001_800_000)
            alert_window = Window("alert", 1_700_001_800_000, 1_700_003_600_000)
            for scenario_name, rows in scenarios.items():
                case_id = "synthetic-" + scenario_name
                registry = make_m4_registry(None, [spec()], root=root)
                query_operator = registry.require("bank.public.query", "1")
                scorer = next(item for item in registry.signatures()
                              if item["name"] == "evolved.synthetic_signal")
                scorer_operator = registry.require(scorer["name"], scorer["version"])
                answer_operator = registry.require("bank.answers.combine",
                    next(item["version"] for item in registry.signatures()
                         if item["name"] == "bank.answers.combine"))
                case_dir = root / "data/prepared/bank/public/cases" / case_id
                file_path = case_dir / "source_dates/2021_01_01/metric/metric_container.csv"
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with file_path.open("w", encoding="utf-8") as stream:
                    stream.write("timestamp,cmdb_id,kpi_name,value\n")
                    for row in rows:
                        stream.write(",".join(row[key] for key in
                            ("timestamp", "cmdb_id", "kpi_name", "value")) + "\n")
                alert_path = case_dir / "alert.json"
                alert_path.write_text(json.dumps({"incident_id": case_id,
                    "telemetry_dates": ["2021_01_01"],
                    "source_date_timestamp_units": {"2021_01_01":
                        {"metric/metric_container.csv": "milliseconds"}}}), encoding="utf-8")
                relative = "source_dates/2021_01_01/metric/metric_container.csv"
                hashes = {"alert.json": hashlib.sha256(alert_path.read_bytes()).hexdigest(),
                          relative: hashlib.sha256(file_path.read_bytes()).hexdigest()}
                telemetry = Telemetry(case_id, reference, alert_window, (), (), {},
                    hashlib.sha256(case_id.encode()).hexdigest(), hashes, {}, {},
                    "synthetic-bank/1", {"alert.json": alert_path.stat().st_size,
                                          relative: file_path.stat().st_size})
                query_context = NodeContext(case_id, "query", "bank.public.query", {}, {},
                                            reference, alert_window, telemetry.data_sha256)
                queried = query_operator.implementation({"data": telemetry}, {
                    "table": "metric/metric_container.csv", "window": "alert",
                    "entity": None, "metric": None, "offset": 0, "limit": 64,
                    "fields": ("timestamp", "cmdb_id", "kpi_name", "value")}, query_context)
                scorer_context = NodeContext(case_id, "score", scorer_operator.qualified_name,
                    {"minimum_support": 1}, {"minimum_support": "synthetic_fixture"},
                    reference, alert_window, telemetry.data_sha256)
                scored_result = scorer_operator.implementation({"data": queried.value},
                    {"threshold": 5.0, "minimum_support": 1}, scorer_context)
                self.assertEqual(scored_result.status,
                    "ready" if expected[scenario_name] else "abstained", scenario_name)
                answer_context = NodeContext(case_id, "answer", answer_operator.qualified_name,
                    {"lead_count": 3}, {"lead_count": "synthetic_fixture"},
                    reference, alert_window, telemetry.data_sha256)
                answer = answer_operator.implementation({"scores": (scored_result,)},
                    {"limit": 3}, answer_context)
                self.assertEqual([lead.entity.identifier for lead in answer.leads],
                                 expected[scenario_name], scenario_name)
                for lead in answer.leads:
                    self.assertTrue(lead.evidence)
                    self.assertEqual(lead.entity.role, "component")

    @unittest.skipUnless(os.environ.get("E4_TEST_BWRAP") == "1",
                         "requires the approved bubblewrap namespace test environment")
    def test_typed_output_is_executed_in_worker_and_decoded_to_core_contract(self):
        normalized = validate_generated_spec({**spec(), "source": '''def operator(inputs, params, runtime):
    return {"$type": "ScoredCandidates", "$fields": {
        "candidates": {"$tuple": []}, "scorer": "evolved.synthetic_signal",
        "status": "abstained", "status_details": {"$map": {}}}}
'''})
        registry = OperatorRegistry()
        register_generated_specs(registry, [normalized])
        operator = registry.require(normalized["name"], normalized["version"])
        aggregate = Aggregate(Window("alert", 1, 2), (), ("bank/fixture.csv",), {})
        context = NodeContext("fixture", "node", operator.qualified_name, {}, {},
                              None, aggregate.window, "a" * 64)
        result = operator.implementation({"data": aggregate}, {"threshold": 1.0,
            "minimum_support": 1}, context)
        self.assertEqual(result.status, "abstained")
        self.assertEqual(result.candidates, ())

    def test_variadic_inputs_are_plain_lists_of_typed_wire_envelopes(self):
        raw = spec()
        raw["input_ports"] = [{"name": "scores", "type": "ScoredCandidates",
                               "variadic": True, "minimum_items": 1}]
        raw["source"] = "def operator(inputs, params, runtime):\n    return inputs['scores'][0]\n"
        normalized = validate_generated_spec(raw)
        registry = OperatorRegistry()
        register_generated_specs(registry, [normalized])
        operator = registry.require(normalized["name"], normalized["version"])
        value = ScoredCandidates((), "fixture", "abstained")
        context = NodeContext("fixture", "node", operator.qualified_name, {}, {},
                              None, None, "a" * 64)
        captured = {}

        def fake_runner(source, inputs, params, runtime, output_type, **kwargs):
            captured["inputs"] = inputs
            return {"ok": True,
                    "result": json.loads(canonical_json(value).decode("utf-8")),
                    "stdout": "", "stderr": "", "isolation": {"active": True}}

        with patch("e4core.m4_generated.run_generated_operator", side_effect=fake_runner):
            result = operator.implementation({"scores": (value, value)}, {}, context)
        self.assertEqual(result, value)
        self.assertIs(type(captured["inputs"]["scores"]), list)
        self.assertEqual(len(captured["inputs"]["scores"]), 2)
        self.assertTrue(all(item["$type"] == "ScoredCandidates"
                            for item in captured["inputs"]["scores"]))

    def test_bank_m4_generated_scorer_rejects_non_null_category(self):
        normalized = validate_generated_spec(spec())
        registry = OperatorRegistry()
        register_generated_specs(registry, [normalized])
        operator = registry.require(normalized["name"], normalized["version"])
        entity = EntityRef("component", "fixture-component")
        evidence = Evidence("bank/synthetic.csv", ALERT, "fixture", 1.0,
                            (entity,), 1, {"ranking_eligible": True})
        candidate = ScoredCandidate(entity, 1.0, "CPU fault", (evidence,),
                                     analysis_family="bank_metric")
        invalid = ScoredCandidates((candidate,), "evolved.synthetic_signal", "ready")
        context = NodeContext("fixture", "node", operator.qualified_name, {}, {},
                              REFERENCE, ALERT, "a" * 64)

        def fake_runner(*args, **kwargs):
            return {"ok": True, "result": json.loads(canonical_json(invalid).decode("utf-8")),
                    "stdout": "", "stderr": "", "isolation": {"active": True}}

        with patch("e4core.m4_generated.run_generated_operator", side_effect=fake_runner):
            with self.assertRaisesRegex(ValueError, "category null"):
                operator.implementation({"data": Aggregate(ALERT, (), (), {})},
                    {"threshold": 1.0, "minimum_support": 1}, context)


if __name__ == "__main__":
    unittest.main()
