"""Clean, structured prompt templates for all diagnosis stages and static repairs."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from .authoring import authoring_contract_reference
from .examples import SCORED_CANDIDATE_EXAMPLE

SYSTEM_PROMPT = """You are E4, an expert Root-Cause Analysis (RCA) engine for distributed microservice systems.
Your goal is to identify the root-cause faulty component for the given incident.
Output all answers strictly as JSON conforming to the requested schema. Do not include markdown code block backticks (like ```json) unless instructed.
"""

SELECT_TEMPLATE = """# Stage 1: Playbook Retrieval (SELECT)

You are analyzing incident '{case_id}'.
Available active playbooks. Each record includes its actual DAG, node IDs, and parameter defaults:
{playbooks_summary}

Registered operator signatures and parameter declarations:
{operators_catalog}

Public Telemetry Digest:
{public_digest}

Task:
Select the most relevant playbook from the active list. Return parameter bindings only for node IDs and parameter names that appear in that playbook and the registered operator declarations. Do not invent, rename, or replace nodes, operators, inputs, or parameters. Bind only values supported by this incident's public context. If defaults are adequate, return an empty binding map.
Respond strictly with a JSON object:
{{
  "selected_playbook": "<playbook_name>",
  "parameter_bindings": {{
    "<node_id>": {{
      "<param_name>": <value>
    }}
  }}
}}
"""

COMPOSE_TEMPLATE = """# Stage 2: DAG Adaptation & Composition (COMPOSE)

Incident '{case_id}' was not resolved by initial playbook retrieval.
Prior Attempt Summary:
{prior_attempts}

Available Registered Operators:
{operators_catalog}

Public Telemetry Digest:
{public_digest}

Public dataset schema:
{dataset_schema}

Task:
Compose an adapted diagnosis DAG using only the available registered operators. Reuse legal port and parameter names from their signatures. Ground table/column choices in the public schema. The return_node must produce a 'ScoredCandidates' or 'RankedAnswer' ranking root-cause components. Return a DAG only; do not author source code.

Respond strictly with a JSON object:
{{
  "dag": {{
    "nodes": [
      {{
        "id": "node_1",
        "operator": "<operator_name>",
        "version": "1",
        "inputs": {{ ... }},
        "params": {{ ... }}
      }}
    ],
    "return_node": "<final_node_id>"
  }}
}}
"""

EVOLVE_TEMPLATE = """# Stage 3A: Operator Authoring & Evolution (EVOLVE)

Incident '{case_id}' requires novel analytical capabilities not present in the existing library.
Prior Attempts:
{prior_attempts}

Available Baseline Operators in Library:
{available_operators}

Public Telemetry Digest:
{public_digest}

Available Tables & Schema:
{dataset_schema}

## Task:
1. Identify the structural analysis gap (what telemetry relationship or statistic is missing).
2. Author one or more genuine, self-contained Python operators to fill this gap.
   - Operator function signature MUST be:
     `def operator(inputs, params, runtime):`
   - Operator name MUST match: `evolved.<lowercase_name>` (e.g., `evolved.metric_ratio`, `evolved.latency_spike_detector`).
   - `authoring_profile` MUST be `"typed-operator-code-v1"`.
   - Output type must be 'Aggregate', 'CandidateSet', or 'ScoredCandidates'.
   - For an operator that queries a source table, select `source_table` from the incident's declared `dataset_schema.tables` and declare it as a required string parameter. Do not require `source_table` for analyses that do not query telemetry. Never invent a table or column name.
   - Use typed helpers and their exact signatures in the shared authoring contract below. Query rows expose typed view fields, and query results expose `coverage`; do not build ranking evidence from incomplete coverage.
   - All helper iteration should use `.rows` (e.g. `for row in res.rows:`).
   - Any helper functions defined inside your operator MUST have full type annotations so Mypy does not reject them.

## Shared Typed Authoring Contract and Reference Examples:
{api_reference}

Respond strictly with a JSON object:
{{
  "gap_record": {{
    "gap_description": "<existing capabilities considered; precise missing relationship or statistic; expected evidence; how proposed analysis and serving use that evidence>",
    "proposed_operator_names": ["evolved.<name>"]
  }},
  "proposed_operators": [
    {{
      "name": "evolved.<lowercase_name>",
      "version": "1",
      "authoring_profile": "typed-operator-code-v1",
      "description": "<detailed purpose>",
      "family": "custom_metric",
      "input_ports": [
        {{
          "name": "data",
          "type": "Telemetry",
          "required": true,
          "variadic": false
        }}
      ],
      "output_type": "Aggregate",
      "parameters": {{
        "source_table": {{"type": "string", "required": true}}
      }},
      "input_requirements": {{
        "data": {{
          "source_columns": {{"<exact declared table>": ["<exact declared column>"]}},
          "statistics": ["raw"],
          "group_by": []
        }}
      }},
      "source": "<complete, valid Python implementation for the exact typed operator(inputs, params, runtime) entry point>"
    }}
  ]
}}
"""

SERVE_TEMPLATE = """# Stage 3B: Pipeline Serving (SERVE)

Your proposed operator(s) have been successfully type-checked and registered in the engine:
{registered_evolved_operators}

All Available Operators (Baseline + Evolved):
{all_operators_catalog}

Public Telemetry Digest:
{public_digest}

Prior diagnosis attempts and diagnostics:
{prior_attempts}

Available public dataset schema:
{dataset_schema}

Diagnosis task for incident '{case_id}'.

Task:
Compose a serving diagnosis DAG that incorporates your newly registered operator(s) to produce the final component ranking. The return_node must produce a supported 'ScoredCandidates' or 'RankedAnswer'.

Respond strictly with a JSON object:
{{
  "dag": {{
    "nodes": [
      {{
        "id": "node_1",
        "operator": "<operator_name>",
        "version": "1",
        "inputs": {{ ... }},
        "params": {{ ... }}
      }}
    ],
    "return_node": "<final_node_id>"
  }}
}}
"""

REPAIR_TEMPLATE = """# Static Repair Required for Operator Proposal

Your proposed operator(s) failed static validation or type-checking.
Below are the exact, batched errors detected:

Errors Encountered:
{error_list}

Original proposal metadata (Python source is preserved separately below):
{proposal_metadata}

Available public dataset schema:
{dataset_schema}

Typed runtime API reference:
{api_reference}

{quoted_source_context}

Task:
Fix the reported errors. Return the complete corrected JSON object with all fields and Python source code.
Ensure:
1. Operator names strictly match `evolved.<lowercase_name>`.
2. All nested helpers have complete type annotations.
3. No forbidden AST constructs or unauthorized imports.
4. Correct use of `.rows` on query results.
5. All required fields in `input_requirements` and `authoring_profile` are present.

Respond strictly with the corrected JSON object conforming to the EVOLVE schema.
"""


def format_select_prompt(
    case_id: str,
    playbooks: Sequence[Mapping[str, Any]],
    public_digest: Mapping[str, Any],
    operators: Sequence[Mapping[str, Any]] = (),
) -> str:
    return SELECT_TEMPLATE.format(
        case_id=case_id,
        playbooks_summary=json.dumps(list(playbooks), indent=2),
        operators_catalog=json.dumps(list(operators), indent=2),
        public_digest=json.dumps(public_digest, indent=2),
    )


def format_compose_prompt(
    case_id: str,
    prior_attempts: Sequence[Mapping[str, Any]],
    operators: Sequence[Mapping[str, Any]],
    public_digest: Mapping[str, Any],
    dataset_schema: Mapping[str, Any] | None = None,
) -> str:
    return COMPOSE_TEMPLATE.format(
        case_id=case_id,
        prior_attempts=json.dumps(prior_attempts, indent=2),
        operators_catalog=json.dumps(operators, indent=2),
        public_digest=json.dumps(public_digest, indent=2),
        dataset_schema=json.dumps(dataset_schema or {}, indent=2),
    )


def format_evolve_prompt(
    case_id: str,
    prior_attempts: Sequence[Mapping[str, Any]],
    public_digest: Mapping[str, Any],
    dataset_schema: Mapping[str, Any],
    available_operators: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    return EVOLVE_TEMPLATE.format(
        case_id=case_id,
        prior_attempts=json.dumps(prior_attempts, indent=2),
        available_operators=json.dumps(available_operators or [], indent=2),
        public_digest=json.dumps(public_digest, indent=2),
        dataset_schema=json.dumps(dataset_schema, indent=2),
        api_reference=authoring_contract_reference(),
    )


def format_serve_prompt(
    case_id: str,
    registered_evolved: Sequence[Mapping[str, Any]],
    all_operators: Sequence[Mapping[str, Any]],
    public_digest: Mapping[str, Any],
    *,
    prior_attempts: Sequence[Mapping[str, Any]] | None = None,
    dataset_schema: Mapping[str, Any] | None = None,
) -> str:
    return SERVE_TEMPLATE.format(
        case_id=case_id,
        registered_evolved_operators=json.dumps(registered_evolved, indent=2),
        all_operators_catalog=json.dumps(all_operators, indent=2),
        public_digest=json.dumps(public_digest, indent=2),
        prior_attempts=json.dumps(prior_attempts or [], indent=2),
        dataset_schema=json.dumps(dataset_schema or {}, indent=2),
    )


def format_repair_prompt(
    errors: Sequence[str],
    rejected_sources: Mapping[str, str],
    proposal_metadata: Mapping[str, Any] | None = None,
    dataset_schema: Any = None,
    api_reference: str | None = None,
) -> str:
    err_text = "\n".join(f"- {err}" for err in errors)
    source_context_parts = []
    for name, src in rejected_sources.items():
        source_context_parts.append(
            f"### Rejected Python source for '{name}':\n```python\n{src}\n```\n"
        )
    quoted_context = (
        "Preserved Source Code from Rejected Proposal:\n"
        + "\n".join(source_context_parts)
        if source_context_parts
        else ""
    )
    schema = dataset_schema.as_dict() if hasattr(dataset_schema, "as_dict") else dataset_schema
    return REPAIR_TEMPLATE.format(
        error_list=err_text,
        proposal_metadata=json.dumps(proposal_metadata or {}, indent=2),
        dataset_schema=json.dumps(schema, indent=2) if schema is not None else "Not provided",
        api_reference=api_reference or authoring_contract_reference(),
        quoted_source_context=quoted_context,
    )
