# Pre-run bootstrap: add capabilities to the active paper library

This is a one-time library preparation prompt before the first diagnosis. Add useful task-specific capabilities to the active shipped paper library only where the validated adapter schema leaves a real coverage gap. Keep the shipped active library intact. Do not select additions to rescue known scored cases or target an accuracy trend.

## Callable inventory and reference examples

The active executable operators and playbooks below are the only registered capabilities callable in bootstrap DAGs. Use their actual signatures, DAG node IDs, parameters, requirements, and availability status. The separately labeled paper reference examples are documentation only; they are not callable unless listed in the active inventory.

Catalog fidelity note: the reported 19 operators and 11 playbooks describe catalog coverage. The active inventory and availability report below show actual implemented, schema-compatible capabilities; catalog entries are not automatically callable. Do not describe catalog coverage as implementation parity.

### Validated schema

{DATASET_SCHEMA}

### Active compatible callable inventory

{ACTIVE_INVENTORY}

### Paper catalog compatibility

{AVAILABILITY}

### Reference-only paper examples (not registered by this prompt)

{REFERENCE_EXAMPLES}

## Task and domain information

{DOMAIN_BRIEF}

### Optional domain information

{DOMAIN_PRIORS}

Paper reference examples and domain priors are caller-supplied context. The caller records prompt inputs, generated artifacts, and any later benefit evidence outside the model response.

## Task

Use existing capabilities and implement only supported missing analyses; do not add source merely to fill a budget. If the active library supports the task, return `{"status":"unchanged","reason":"..."}`. If no safe addition can be specified because required schema or task facts are missing, return `{"status":"blocked","reason":"..."}` or ask one essential clarification. If existing operators support the task but a useful DAG is missing, return a playbook-only object with `status: "additions"`, `proposed_operators: []`, and new `playbooks`; do not pass it to the current non-empty operator proposal-bundle validator. When a source capability is missing, return `status: "additions"` and only the machine fields in this JSON bundle:

- `gap_record`: required when there are new source operators; include concise `gap_description` and `proposed_operator_names`, reusing the current validator contract. State the uncovered analytical relationship without a separate rationale field.
- `proposed_operators`: zero to three new operators, with at least one when this branch fills a source-capability gap. Each contains complete Python source in `source` and uses the exact current evolved operator metadata, source-column contract, and shared authoring reference. Do not copy reference-example source into the active library unless the new source has an independently justified task-specific capability.
- `playbooks`: zero to three additional `{ "name", "description", "dag" }` entries. DAGs may call only the active inventory and proposed operators. Preserve existing playbooks; do not return replacements for shipped entries.

Every additions response uses top-level `status: "additions"`. Do not include adapter source, scored answers, incident identifiers, evaluation target percentages, or narrative fields outside the machine contract.

## Shared typed authoring contract

{AUTHORING_REFERENCE}
