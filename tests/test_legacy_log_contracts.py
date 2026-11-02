"""Strict nested-contract regression for the repaired legacy log scorer.

All inputs here are synthetic. The source fixture is self-contained and does
not depend on historical run artifacts or a mutable operator library.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from types import MappingProxyType

from e4core.bank_adapter import BankCase
from e4core.cache import CacheStore, canonical_json, deserialize_contract_json
from e4core.contracts import (Aggregate, Candidate, CandidateSet, EntityRef, Evidence,
    RankedAnswer, ScoredCandidate, Telemetry, Window)
from e4core.executor import NodeContext
from e4core.m4_controller import M4ControllerError, validate_bank_answer
from e4core.m4_data import make_m4_registry
from e4core.m4_generated import validate_generated_spec
from e4core.sandbox import run_generated_operator


# The reviewed proposal changes only both emitted EntityRef fields and the
# evidence source path. Its operator name and every match/counting operation
# remain as in the provided legacy scorer.
REPAIRED_SOURCE = '''\
def operator(inputs, params, runtime):
    candidates_env = contract_fields(inputs['candidates'], 'CandidateSet')
    logs_env = contract_fields(inputs['logs'], 'Aggregate')
    
    # Raw rows from bank.public.query are located in metadata.public_query.rows_data
    rows = logs_env['metadata']['public_query']['rows_data']
    pattern = params['pattern']
    
    scored_candidates = []
    
    for cand_env in candidates_env['candidates']:
        cand = contract_fields(cand_env, 'Candidate')
        entity_ref = contract_fields(cand['entity'], 'EntityRef')
        cid = entity_ref['identifier']
        
        count = 0
        for row in rows:
            # Row keys are native column names from log/log_service.csv
            if row.get('cmdb_id') == cid:
                val = row.get('value', '')
                if val and pattern in val:
                    count += 1
        
        if count > 0:
            evidence = {
                '$type': 'Evidence',
                '$fields': {
                    'contributing_entities': {'$tuple': [cand['entity']]},
                    'details': {'ranking_eligible': True},
                    'source': 'bank/log/log_service.csv',
                    'statistic': 'pattern_occurrence_count',
                    'support': count,
                    'value': float(count),
                    'window': logs_env['window']
                }
            }
            
            scored_candidates.append({
                '$type': 'ScoredCandidate',
                '$fields': {
                    'analysis_family': 'bank_log_content',
                    'category': None,
                    'entity': cand['entity'],
                    'evidence': {'$tuple': [evidence]},
                    'score': float(count)
                }
            })
            
    return {
        '$type': 'ScoredCandidates',
        '$fields': {
            'candidates': {'$tuple': scored_candidates},
            'scorer': 'evolved.log_pattern_scorer',
            'status': 'ready',
            'status_details': {}
        }
    }
'''.rstrip("\n")
REPAIRED_SOURCE_SHA256 = "6434d4312587714d9146019b1c3961fde47602c5a04f85f86646eac31e60a679"


def _spec():
    return validate_generated_spec({
        "name": "evolved.log_pattern_scorer",
        "version": "1",
        "description": "Counts matching log values for candidates in supplied public query rows.",
        "family": "bank_log_content",
        "input_ports": [
            {"name": "candidates", "type": "CandidateSet"},
            {"name": "logs", "type": "Aggregate"},
        ],
        "output_type": "ScoredCandidates",
        "parameters": {"pattern": {"type": "string", "required": True}},
        "source": REPAIRED_SOURCE,
    })


def _synthetic_inputs(rows):
    reference = Window("reference", 0, 30_000)
    alert = Window("alert", 30_000, 60_000)
    entities = (EntityRef("component", "synthetic-a"), EntityRef("component", "synthetic-b"))
    candidates = CandidateSet(tuple(Candidate(entity, "synthetic") for entity in entities), "synthetic")
    aggregate = Aggregate(alert, (), ("log/log_service.csv",), {
        "public_query": {"table": "log/log_service.csv", "window": "alert",
            "rows_data": list(rows), "coverage": {"truncated": False, "next_offset": None}}})
    return {"candidates": candidates, "logs": aggregate}, reference, alert, entities


def _wire_inputs(inputs):
    return {key: json.loads(canonical_json(value).decode("utf-8"))
            for key, value in inputs.items()}


class RepairedLegacyLogContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = _spec()
        cls.registry = make_m4_registry(None, [cls.spec], root=Path(__file__).resolve().parents[1])
        cls.scorer = cls.registry.require(cls.spec["name"], cls.spec["version"])
        answer_signature = next(row for row in cls.registry.signatures()
                                if row["name"] == "bank.answers.combine")
        cls.combiner = cls.registry.require(answer_signature["name"], answer_signature["version"])

    def _score(self, rows):
        inputs, reference, alert, entities = _synthetic_inputs(rows)
        params = {"pattern": "FINDME"}
        runtime = {"alert_window": {"name": "alert", "start_ms_utc": 30_000,
                                     "end_ms_utc": 60_000}}
        wire = _wire_inputs(inputs)
        raw_result = run_generated_operator(REPAIRED_SOURCE, wire, params, runtime, "ScoredCandidates")
        self.assertTrue(raw_result["ok"], raw_result.get("error"))
        scored = self.scorer.implementation(inputs, params, NodeContext(
            "synthetic-legacy-contract", "scorer", self.scorer.qualified_name,
            MappingProxyType({}), MappingProxyType({}), reference, alert, "a" * 64))
        return raw_result["result"], scored, reference, alert, entities

    def _answer_and_case(self, scores, reference, alert, observed_ids):
        context = NodeContext("synthetic-legacy-contract", "answer", self.combiner.qualified_name,
            MappingProxyType({}), MappingProxyType({"lead_count": 3}), reference, alert, "b" * 64)
        answer = self.combiner.implementation({"scores": (scores,)}, {"limit": 5}, context)
        telemetry = Telemetry("synthetic-legacy-contract", reference, alert, (), (),
            {identifier: {"role": "component"} for identifier in observed_ids},
            "c" * 64, {}, {}, {}, "synthetic/1")
        case = BankCase(telemetry, {}, {}, {}, {}, {})
        return answer, case

    def test_reviewed_source_hash_and_positive_nested_output_roundtrip(self):
        self.assertEqual(hashlib.sha256(REPAIRED_SOURCE.encode()).hexdigest(), REPAIRED_SOURCE_SHA256)
        raw, scored, reference, alert, entities = self._score([
            {"cmdb_id": "synthetic-a", "value": "FINDME one", "timestamp_ms_utc": 40_000},
            {"cmdb_id": "synthetic-a", "value": "FINDME two", "timestamp_ms_utc": 50_000},
            {"cmdb_id": "synthetic-a", "value": "findme lower-case", "timestamp_ms_utc": 55_000},
        ])
        decoded = deserialize_contract_json(raw, "ScoredCandidates")
        self.assertEqual(decoded, scored)
        self.assertEqual(len(scored.candidates), 1)
        candidate = scored.candidates[0]
        self.assertEqual(candidate.entity, entities[0])
        self.assertEqual(candidate.score, 2.0)
        self.assertEqual(candidate.evidence[0].contributing_entities, (entities[0],))
        self.assertIsInstance(candidate.evidence[0].window, Window)
        self.assertEqual(candidate.evidence[0].window, alert)
        self.assertEqual(candidate.evidence[0].source, "bank/log/log_service.csv")
        self.assertTrue(candidate.evidence[0].details["ranking_eligible"])
        answer, case = self._answer_and_case(scored, reference, alert, [entities[0].identifier])
        validate_bank_answer(answer, case)
        self.assertEqual(len(answer.leads), 1)
        self.assertEqual(answer.leads[0].entity, entities[0])

    def test_no_match_is_empty_and_answer_guard_rejects_unobserved_positive(self):
        _, empty_scores, reference, alert, entities = self._score([
            {"cmdb_id": "synthetic-a", "value": "no marker", "timestamp_ms_utc": 40_000},
        ])
        self.assertEqual(empty_scores.candidates, ())
        empty_answer, case = self._answer_and_case(empty_scores, reference, alert,
                                                    [entity.identifier for entity in entities])
        validate_bank_answer(empty_answer, case)
        self.assertEqual(empty_answer.leads, ())

        _, positive_scores, reference, alert, entities = self._score([
            {"cmdb_id": "synthetic-a", "value": "FINDME", "timestamp_ms_utc": 40_000},
        ])
        _, unobserved_case = self._answer_and_case(positive_scores, reference, alert,
                                                   [entities[1].identifier])
        answer, _ = self._answer_and_case(positive_scores, reference, alert,
                                           [entities[1].identifier])
        with self.assertRaises(M4ControllerError):
            validate_bank_answer(answer, unobserved_case)

    def test_every_nested_contract_position_rejects_plain_or_missing_envelopes(self):
        raw, _scored, _reference, _alert, _entities = self._score([
            {"cmdb_id": "synthetic-a", "value": "FINDME", "timestamp_ms_utc": 40_000},
        ])
        original = raw["$fields"]["candidates"]["$tuple"][0]
        candidate_fields = original["$fields"]
        evidence = candidate_fields["evidence"]["$tuple"][0]
        evidence_fields = evidence["$fields"]

        corruptions = []
        plain_candidate_entity = copy.deepcopy(raw)
        plain_candidate_entity["$fields"]["candidates"]["$tuple"][0]["$fields"]["entity"] = {
            "role": "component", "identifier": "synthetic-a"}
        corruptions.append(plain_candidate_entity)

        plain_contributor = copy.deepcopy(raw)
        plain_contributor["$fields"]["candidates"]["$tuple"][0]["$fields"]["evidence"]["$tuple"][0]["$fields"]["contributing_entities"]["$tuple"][0] = {
            "role": "component", "identifier": "synthetic-a"}
        corruptions.append(plain_contributor)

        plain_window = copy.deepcopy(raw)
        plain_window["$fields"]["candidates"]["$tuple"][0]["$fields"]["evidence"]["$tuple"][0]["$fields"]["window"] = {
            "name": "alert", "start_ms_utc": 30_000, "end_ms_utc": 60_000}
        corruptions.append(plain_window)

        missing_evidence_type = copy.deepcopy(raw)
        del missing_evidence_type["$fields"]["candidates"]["$tuple"][0]["$fields"]["evidence"]["$tuple"][0]["$type"]
        corruptions.append(missing_evidence_type)

        missing_entity_fields = copy.deepcopy(raw)
        missing_entity_fields["$fields"]["candidates"]["$tuple"][0]["$fields"]["entity"]["$fields"].pop("role")
        corruptions.append(missing_entity_fields)

        for damaged in corruptions:
            with self.subTest(damaged=damaged):
                with self.assertRaises((TypeError, ValueError)):
                    deserialize_contract_json(damaged, "ScoredCandidates")

    def test_cache_roundtrip_preserves_nested_types_and_rejects_rechecksummed_plain_entity(self):
        _raw, scored, _reference, _alert, _entities = self._score([
            {"cmdb_id": "synthetic-a", "value": "FINDME", "timestamp_ms_utc": 40_000},
        ])
        with tempfile.TemporaryDirectory() as temporary:
            cache = CacheStore(Path(temporary), enabled=True)
            key = "synthetic-typed-scorer-output"
            cache.put(key, "ScoredCandidates", scored)
            restored, digest, status = cache.get(key, "ScoredCandidates")
            self.assertEqual(status, "hit")
            self.assertTrue(digest)
            self.assertEqual(restored, scored)
            self.assertIsInstance(restored.candidates[0].entity, EntityRef)
            self.assertIsInstance(restored.candidates[0].evidence[0], Evidence)
            self.assertIsInstance(restored.candidates[0].evidence[0].window, Window)
            self.assertIsInstance(restored.candidates[0].evidence[0].contributing_entities[0], EntityRef)

            cache_path = Path(temporary) / f"{key}.json"
            envelope = json.loads(cache_path.read_text(encoding="utf-8"))
            envelope["payload"]["$fields"]["candidates"]["$tuple"][0]["$fields"]["entity"] = {
                "role": "component", "identifier": "synthetic-a"}
            payload_bytes = json.dumps(envelope["payload"], sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False).encode("utf-8")
            envelope["payload_sha256"] = hashlib.sha256(payload_bytes).hexdigest()
            cache_path.write_text(json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n",
                                  encoding="utf-8")
            corrupted, digest, status = cache.get(key, "ScoredCandidates")
            self.assertIsNone(corrupted)
            self.assertIsNone(digest)
            self.assertEqual(status, "corrupt_recomputed")


if __name__ == "__main__":
    unittest.main()
