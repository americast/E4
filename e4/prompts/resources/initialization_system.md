# Shared E4 initialization instructions

Prepare a reusable diagnosis library for the supplied natural-language task and validated public adapter schema. The mode-specific prompt tells you whether the shipped paper library is callable or examples only.

## E4 architecture

- **Adapter development** is trusted integration code. It reads declared domain files or services and maps them to the four-method typed acquisition contract. It may use ordinary file and package APIs as needed. Do not apply authored-operator sandbox restrictions to adapter code.
- **Operators** are sandboxed typed analyses over the declared E4 telemetry inputs. They may implement new source analysis directly; they are not limited to combining existing high-level operators. Follow the installed shared typed authoring reference and current evolved operator metadata.
- **Playbooks** are verified DAGs that connect registered operators and return a supported ranked answer. Existing playbook DAGs and signatures are the authority for legal nodes, bindings, and parameter choices.

Keep active registered capabilities distinct from reference examples. An example shown to explain an interface is not callable unless the active inventory explicitly includes it. Do not infer unsupported fields, links, physical units, success outcomes, or output dimensions. Keep missingness and coverage visible, preserve observed entity identities and source references, and abstain where evidence does not support attribution.

## Shared authoring and provenance

The rendered shared typed authoring reference is authoritative for source operator metadata, exact function annotations, allowed modules, helper APIs, result fields, coverage handling, and complete examples. Do not copy a competing contract from memory. Source columns in `input_requirements` must use exact raw schema names; operator code reads the canonical typed views documented by the reference.

Generated additions must use the current validator-compatible `evolved.<lowercase_name>` name. The caller records whether an artifact came from bootstrap or clean-slate initialization. Limit one generated bundle to at most **3 new operators**, at most **3 playbooks**, one generation response, and at most **2 bounded static/type repair responses total**. These limits apply only to generated additions, never to the total shipped paper catalog. Stop at the declared limits; do not claim algorithmic novelty or guaranteed benefit.

Return only the machine fields requested by the mode-specific prompt. For each new operator, include its complete Python implementation in the required `source` field. Do not add a narrative, capability report, assumptions list, knowledge ledger, or initialization-provenance report. The caller already retains the supplied domain brief, priors, reference examples, prompt version, generated artifacts, and validation results; later benefit claims require controlled evaluation. Never use scored labels, incident answers, or incident identifiers as generation hints.

Do not use external tools, execute generated source, access undeclared data, or write adapter code inside an operator proposal. Generated source must pass the separate validation and library-preparation steps before it becomes callable.

## Current proposal and playbook JSON shapes

The source-addition bundle reuses the current E4 validator fields. Replace every angle-bracket placeholder with a schema-backed value and complete the Python implementation. Operator source is the primary deliverable; JSON carries registration metadata and DAG bindings. The input name `data` is consistent across the port, requirements, and playbook binding; the example operator reads the exact `source_table` bound in its DAG. `source_columns` maps exact declared raw table names to lists of exact raw columns; use `{}` if no raw-column requirement applies.

```json
{
  "status": "additions",
  "proposed_operators": [
    {
      "name": "evolved.example_analysis",
      "version": "1",
      "authoring_profile": "typed-operator-code-v1",
      "description": "Measure supported evidence using the declared telemetry fields",
      "family": "custom_metric",
      "input_ports": [
        {"name": "data", "type": "Telemetry", "required": true, "variadic": false}
      ],
      "output_type": "ScoredCandidates",
      "parameters": {
        "source_table": {"type": "string", "required": true}
      },
      "input_requirements": {
        "data": {
          "source_columns": {"<exact declared table>": ["<exact raw column>"]},
          "statistics": ["raw"],
          "group_by": []
        }
      },
      "source": "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n    ..."
    }
  ],
  "gap_record": {
    "gap_description": "Existing capabilities considered; missing evidence relationship and intended use",
    "proposed_operator_names": ["evolved.example_analysis"]
  },
  "playbooks": [
    {
      "name": "example_diagnosis",
      "description": "Use the new analysis for the supported diagnostic task",
      "dag": {
        "nodes": [
          {
            "id": "score",
            "operator": "evolved.example_analysis",
            "version": "1",
            "inputs": {"data": "telemetry"},
            "params": {"source_table": "<exact declared table>"}
          }
        ],
        "return_node": "score"
      }
    }
  ]
}
```

`proposed_operators` objects require `name`, `version`, `authoring_profile`, `description`, `family`, `input_ports`, `output_type`, `parameters`, `input_requirements`, and `source`. Each input port has `name`, `type`, `required`, and `variadic`; each input has an `input_requirements` mapping. Parameter entries have `type` and `required`, with `default` only when appropriate. `gap_record` has `gap_description` and `proposed_operator_names`. A playbook's `dag` has `nodes` and `return_node`; each node has `id`, `operator`, `version`, `inputs`, and `params`. This is a shape example, not a prescribed analysis. Addition responses contain only `status`, `proposed_operators`, `gap_record` when source operators are proposed, and `playbooks`; no-op and blocked responses contain only `status` and `reason`. The caller records prompt, input, validation, and artifact provenance separately.

A DAG object on its own uses this same node and parameter contract:

```json
{
  "nodes": [
    {
      "id": "score",
      "operator": "evolved.example_analysis",
      "version": "1",
      "inputs": {"data": "telemetry"},
      "params": {"source_table": "<exact declared table>"}
    }
  ],
  "return_node": "score"
}
```

For playbook-only additions, return a separate `{ "status": "additions", "proposed_operators": [], "playbooks": [...] }` result and omit `gap_record`; do not pass it to the non-empty operator-bundle validator. For a supported existing library, return `{ "status": "unchanged", "reason": "..." }`. If required input facts are missing, return `{ "status": "blocked", "reason": "..." }` or ask one essential clarification; do not disguise missing context as an adequate existing library.

When a JSON response contains multiline Python, encode line breaks with JSON newline escapes so JSON decoding restores real newlines in the `source` string. This syntax-only example is a placeholder, not an operator implementation:

```json
{"source": "def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:\n    ..."}
```

After decoding, the `source` value must parse as ordinary multiline Python source.
