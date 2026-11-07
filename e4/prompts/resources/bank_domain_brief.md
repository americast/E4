# Bank service root-cause task brief — task/schema essentials

## Provenance and scope

This brief is the essentials-only Bank prompt variant. It contains task and public adapter/schema facts only; it does not contain scored component labels, incident identifiers, failed-case details, or answer hints. Table names and source headers below describe the current E4 Bank adapter contract. Confirm the public input schema supplied at generation time before using columns: table availability and population can vary by run.

The requested diagnosis is a **top-three ranking of task-defined rankable components** for an alert, using its reference and alert windows. Preserve the actual observed adapter entity role and identity; do not assume every component ID has a particular role label. This existing task contract ranks components only. It does not define component-plus-time localization or cyber-style time intervals. Return only identities supported by declared evidence and use the supported abstention behavior when attribution is unsupported; do not fabricate a link.

## Task and ontology facts

- The adapter exposes two windows named `reference` and `alert`, represented as UTC epoch-millisecond intervals. Source timestamp units are declared per source date/file in public metadata; do not assume all files share units.
- Container source `bank/metric_container.csv` has fields `timestamp`, `cmdb_id`, `kpi_name`, and `value`. Physical units and individual metric semantics are available only when explicitly declared by the accompanying schema/catalog; otherwise treat them as unknown.
- Application KPI source `bank/metric_app.csv` has fields `timestamp`, `tc`, `rr`, `sr`, `cnt`, and `mrt`. `tc`/application KPI entities are opaque `app_kpi` entities, not rankable service/container components. No mapping from transaction codes to components is implied.
- Trace source `bank/trace_span.csv` has fields `timestamp`, `cmdb_id`, `trace_id`, `span_id`, `parent_id`, and `duration`. The duration's physical unit is unknown unless the supplied public schema declares it. Canonical span views include nullable `success`; source rows in this contract do not provide an outcome value, so it is unknown (`None`), not a field to infer. Trace IDs and span relationships do not by themselves prove causal ownership beyond the declared component identity.
- Log source `bank/log/log_service.csv` has fields `timestamp`, `log_id`, `cmdb_id`, `log_name`, and `value`. Use only the actual columns in the supplied schema. Do not assume a severity, error, outcome, or message field exists.
- Timestamp conversion and source-row identity are handled by the adapter. Preserve the adapter's observed entity roles and source references; do not derive new identity links from names or codes.

### Raw source columns and canonical typed rows

`input_requirements.source_columns` names the raw source headers listed above. Query helpers return canonical typed views; authored operators should read those view fields, not raw names such as `row.cmdb_id`:

- For container metrics, `cmdb_id` becomes `MetricPointView.entity_identifier`; `kpi_name` becomes the canonical `metric_name`/`metric_key`.
- For application KPIs, each of `rr`, `sr`, `cnt`, and `mrt` becomes a separate metric observation. `tc` is the opaque `app_kpi` entity identifier.
- For spans, `cmdb_id` becomes `SpanView.executing_component_identifier`. Canonical `duration_ms`/`duration_value` are normalized field names, but the duration's physical unit remains unknown unless public metadata declares it. `success` remains nullable and is `None` when the source has no outcome.
- For logs, `cmdb_id` becomes `LogRecordView.entity_identifier`; `log_name` and `value` remain the canonical log name and value fields.

## Analytical priors

No extra domain-specific analytical prior is included in this essentials-only brief. Optional, explicitly evaluation-informed priors are available separately and must be requested using the opt-in prompt rendering option. Their provenance and status appear in that resource.

## Supplied task/schema facts

| Addition | Source | Reason | Expected effect | Exposure |
| --- | --- | --- | --- | --- |
| Component top-three ranking task | Public task specification | Defines target and output | Keep ranking focused on rankable components | Task/schema fact |
| Reference/alert windows and per-date source timestamp declarations | Public adapter metadata contract | Avoid cross-window/unit errors | Enable valid comparisons where fields support them | Task/schema fact |
| Container fields (`timestamp`, `cmdb_id`, `kpi_name`, `value`) | Public adapter/schema contract | Define component-level metric observations | Enable supported component metric analyses | Task/schema fact |
| App KPI fields and opaque `app_kpi` entity role | Public adapter/schema contract | Preserve application signals without false component mapping | Use cautiously as contextual evidence; never rank transaction IDs as components | Task/schema fact |
| Trace identity/duration fields, unknown physical duration unit, and nullable canonical success with no source outcome | Public adapter/schema contract | Prevent unsupported unit/outcome inference | Enable duration/structure analysis only if schema and coverage support it | Task/schema fact |
| Log fields (`timestamp`, `log_id`, `cmdb_id`, `log_name`, `value`) | Public adapter/schema contract | Avoid invented log semantics | Permit only analyses on declared columns | Task/schema fact |
| Raw Bank headers map to the canonical typed row fields described above | Bank adapter `stream_records` mapping | Separate metadata requirements from fields read by helper results | Use exact raw headers in `source_columns` and canonical view names in code | Task/schema fact |
| Unknown physical units where not declared | Public schema contract | Prevent unsupported interpretation | Preserve magnitude comparisons only when defensible | Task/schema fact |

The caller retains these supplied facts as input provenance and records generated artifacts separately; the model response contains only the requested code/DAG fields.
