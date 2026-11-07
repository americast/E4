# Domain brief (fillable template)

Replace the bracketed guidance with facts you can support. Write `unknown` when a value, relationship, or behavior has not been established. Do not use scored answers or incident-specific hints.

## Task and accepted output

- Diagnostic question: [What should be diagnosed?]
- Unit of analysis: [One incident, service, component, deployment, ...]
- Candidate ontology: [Allowed component identities and their source. Explain whether non-component IDs exist.]
- Required output: [Ranking, top-k, component-plus-time, abstention, ...]
- Acceptance and abstention: [What supports a candidate? When should the system decline to attribute?]
- Scope limit: [Which output dimensions are not supported by the current E4 contract?]

## Data and acquisition

For each input source, provide its actual table/file name, record kind, exact columns, identity key and relationship evidence, timestamp origin/unit/timezone, physical units, missing-value conventions, and representative sanitized development rows if allowed.

| Source/table | Kind | Exact fields | Identity and evidence | Time/unit | Missingness/limits |
| --- | --- | --- | --- | --- | --- |
| [name] | [metric/span/log/other] | [columns] | [documented relation] | [explicit or unknown] | [details] |

- Reference/normal and alert/incident window semantics: [interval bounds, overlap, and provenance]
- Can reference data contain the mechanism under investigation? [yes/no/unknown, with source]
- Schema available to an operator and schema version/source: [details]
- Adapter/mapping constraints: [details; do not place executable adapter source in the library JSON]
- Identity/linking rules: [how rows map to rankable components; do not infer links from similar names]

## Domain knowledge and observability

- Supported mechanisms or hypotheses: [include source and evidence; otherwise `none supplied`]
- Useful relationships to investigate: [optional analytical priors, with source/provenance]
- Known failure modes and observability gaps: [details]
- Unknown values or semantic ambiguities the system must preserve: [details]
- Evidence quality, coverage, or censoring concerns: [details]

## Allowed inputs, budgets, and provenance

- Allowed initialization inputs: [public schema, documentation, sanitized development examples, ...]
- Explicitly excluded information: [labels, incident answers, other information]
- Model/generation/repair/resource limits: [use the generic prompt's declared defaults unless changed in a separately reviewed version]
- Human or expert additions and source for each: [list additions]
- Input-information ledger: for each supplied fact or prior, record its exact text, source, reason, expected effect, and exposure class (`task/schema fact`, `development-derived prior`, or `evaluation-informed`). The caller retains this input record and tracks generated-artifact use or later benefit evidence separately.

Do not imply that this brief supplies a data adapter, validates the supplied schema, or guarantees that a particular diagnosis is observable.
