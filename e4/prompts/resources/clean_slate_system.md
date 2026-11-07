# Clean-slate domain library generation

Prepare a task-specific E4 library from the validated adapter metadata without any shipped paper capabilities in the executable registry. This is an alternative to the default pre-run bootstrap. The subsequent automatic bootstrap pass is skipped in this mode.

## Callable inventory and reference examples

The executable inventory is **empty**. No paper operator or playbook shown below may be called in a DAG unless it is independently generated and validated in this clean-slate bundle. Paper catalog material is reference-only interface/analysis documentation, not registered capability.

Catalog fidelity note: the reported 19 operators and 11 playbooks describe catalog coverage. The availability report below distinguishes actual implementations and schema compatibility; do not state or imply that every paper catalog entry is executable.

### Validated schema

{DATASET_SCHEMA}

### Active executable inventory

{ACTIVE_INVENTORY}

### Paper catalog compatibility (informational only)

{AVAILABILITY}

### Reference-only paper examples (not callable)

{REFERENCE_EXAMPLES}

## Task and domain information

{DOMAIN_BRIEF}

### Optional domain information

{DOMAIN_PRIORS}

Paper reference examples and domain priors are caller-supplied context. The caller records prompt inputs, generated artifacts, and any later benefit evidence outside the model response.

## Task

From the validated schema and task brief, propose a small connected library with at least one and at most three new source operators and at least one and at most three playbooks. Generate only capabilities supported by the available fields and identity links. Return only `status: "additions"`, `proposed_operators`, `gap_record`, and `playbooks`. Each operator contains complete Python source in `source` and uses validator-compatible `evolved.<lowercase_name>` names. The caller records clean-slate origin separately.

The generated additions budget is at most three operators and three playbooks, one generation response, and at most two bounded static/type repair responses total. This budget limits newly generated work; it does not define or truncate the shipped paper catalog shown as reference. If a required schema or task fact is missing, return `{"status":"blocked","reason":"..."}` or ask one essential clarification rather than presenting an incomplete library. Do not invent fields, identity links, units, or outcomes. Do not include executable adapter code or scored incident answers.

## Shared typed authoring contract

{AUTHORING_REFERENCE}
