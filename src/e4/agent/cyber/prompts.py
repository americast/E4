"""Prompts for the three cyber stages: select a stored playbook, compose a new DAG, or synthesize
new operators and a DAG that reaches them. The ground truth never appears in a prompt."""
from __future__ import annotations

import json
from typing import Any, Mapping

from e4.agent.bank.controller import generated_spec_shape_example
from e4.operators.generated import generated_spec_contract_schema
from e4.dsl.registry import OperatorRegistry
from e4.dsl.runtime import RuntimeContext

SYSTEM_PROMPT = ("Analyze the supplied attacker-run telemetry using only the registered E4 operators. "
                 "Return only the requested JSON artifact. You have no external tools or label access.")

TASK = (
    "You are explaining why an autonomous attacker run failed. The scope is a set of runs of the same "
    "model, harness and environment; the anomalous run failed its goals, and the baseline runs are other "
    "runs in the same scope. Write a playbook: a typed DAG of registered operators. The playbook is run "
    "once per run in scope, each time on a Case where that run is the anomaly and the others are "
    "baselines, so it must not depend on which run is which. Its RankedAnswer names, for the anomalous "
    "run, the components (from the Case's component list) behind the behavior, and for each one the "
    "time intervals [start_ms, end_ms) in that run when the behavior happens. Times are milliseconds "
    "since the run's start. A lead is judged correct only when its component is right AND its intervals "
    "cover the behavior's true times closely (both recall and precision count). The DAG starts at one "
    "'case' node and must return cyber.answers.combine.")


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def wire_schema() -> dict[str, Any]:
    """Exact cache-wire shapes a generated operator reads and returns."""
    return {
        "envelope": "{$type: contract_name, $fields: {...}}; fields with defaults may be omitted",
        "collections": {"tuple": "{'$tuple': [items...]}", "mapping": "{'$map': {key: value}}"},
        "contract_fields": "contract_fields(value, 'Aggregate') returns the $fields as read-only "
                           "mappings and tuples",
        "contracts": {
            "EntityRef": {"role": "component role, e.g. harness or environment.host",
                          "identifier": "component name, e.g. ExfiltrateData or database3"},
            "TelemetryWindowRef": {"window_id": "string", "telemetry_id": "string",
                                   "start_ms": "integer", "end_ms": "integer"},
            "AggregateRow": {"entity": "EntityRef", "metric_key": "string",
                             "window": "TelemetryWindowRef", "statistic": "string",
                             "value": "finite number or null", "support": "nonnegative integer",
                             "source_tables": "tuple of strings",
                             "contributing_entities": "tuple of EntityRef (default empty)",
                             "details": "mapping of JSON data (default empty)"},
            "Aggregate": {"window": "TelemetryWindowRef", "rows": "tuple of AggregateRow",
                          "source_tables": "tuple of strings", "metadata": "mapping of JSON data"},
            "Candidate": {"entity": "EntityRef", "rationale": "string"},
            "CandidateSet": {"candidates": "tuple of Candidate", "source": "string"},
            "ScoredCandidate": {"entity": "EntityRef", "score": "finite number",
                                "category": "null", "evidence": "tuple (may be empty)",
                                "analysis_family": "string or null",
                                "intervals": "tuple of [start_ms, end_ms] integer pairs, end > start"},
            "ScoredCandidates": {"candidates": "tuple of ScoredCandidate", "scorer": "string",
                                 "status": "ready or abstained", "status_details": "mapping"}}}


def data_layout() -> dict[str, Any]:
    return {
        "cyber.logs.count": "Aggregate rows, one per (entity, table, kind): metric_key 'table|kind', "
                            "statistic 'count', value = count, details {table, kind, first_ms, last_ms}",
        "cyber.logs.query": "Aggregate with no rows; metadata.log_query = {request, rows_data, "
                            "total_matching_rows, returned_rows, next_offset}; each row is "
                            "{timestamp_ms, table, entity 'role:identifier', kind, fields}; at most 256 "
                            "rows; string values longer than 1024 characters are cut",
        "action records": "table attacker/actions.json, kind = action name; fields include type "
                          "(HighLevelAction or LowLevelAction), ts_start, ts_finish, seconds, "
                          "high_level_action_id, action_params.*, stdout, stderr, event"}


def common_prompt(digest: Mapping[str, Any], registry: OperatorRegistry, runtime: RuntimeContext,
                  history: list[Mapping[str, Any]]) -> str:
    return (TASK +
            "\n\nCASE DIGEST (the first scope Case; counts only, no raw records)\n" + json_text(digest) +
            "\n\nREGISTERED OPERATOR SIGNATURES\n" + json_text(registry.signatures()) +
            "\n\nRUNTIME VALUES\n" + json_text(runtime.as_dict()["values"]) +
            "\n\nPRIOR ATTEMPTS (answers per run and the binary accepted flag only)\n" + json_text(history))


def select_prompt(common: str, playbooks: Mapping[str, Any]) -> str:
    return (common + "\n\nSTAGE 1: select one stored playbook. Return exactly "
            "{playbook: name, node_parameters: {node_id: {param: value}}}. You may only change "
            "parameters the stored nodes declare; nodes, edges and operators stay as stored. Use {} "
            "when no change is needed.\n\nSTORED PLAYBOOKS\n" + json_text(playbooks))


def compose_prompt(common: str) -> str:
    return (common + "\n\nSTAGE 2: compose a new DAG from the registered operators. Return exactly "
            "{dag: {schema_version: 'e4-dag/1', nodes: [{id, operator, version, inputs, params, "
            "bindings}], return: {node}}}. inputs, params and bindings are JSON objects ({} when empty).")


def synthesize_prompt(common: str) -> str:
    return (common + "\n\nSTAGE 3: propose one focused new operator (at most three) and a DAG that "
            "reaches it. Return exactly {operators: [spec], dag: {...}}. Each spec is exactly "
            "{name, version, description, family, input_ports, output_type, parameters, source}; names "
            "use evolved.<lower_snake>; source defines operator(inputs, params, runtime). Generated "
            "operators take Aggregate, CandidateSet or ScoredCandidates inputs, never a Case or "
            "TelemetryWindows: feed them cyber.logs.count or cyber.logs.query pages. Return exact wire "
            "envelopes. Only math, statistics, itertools and collections may be imported; no files, "
            "network, classes, lambdas, try/except or recursion. Numbers other than 0 and 1 must be "
            "declared parameters. Do not hard-code run names."
            "\n\nWIRE SCHEMA\n" + json_text(wire_schema()) +
            "\n\nDATA LAYOUT\n" + json_text(data_layout()) +
            "\n\nGENERATED OPERATOR SPEC SCHEMA\n" + json_text(generated_spec_contract_schema()) +
            "\n\nSPEC SHAPE EXAMPLE (shape only)\n" + json_text(generated_spec_shape_example()))


def repair_prompt(stage_prompt: str, previous: str, error: Mapping[str, Any]) -> str:
    return (stage_prompt + "\n\nYOUR PREVIOUS RESPONSE FAILED A STATIC CHECK. Fix it and return the "
            "corrected JSON object only.\nError: " + json_text(error) +
            "\nPrevious response:\n" + previous)
