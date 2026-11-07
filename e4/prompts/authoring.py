"""Shared, runtime-grounded authoring contract for EVOLVE and initialization."""

from __future__ import annotations

from e4.controller.schemas import REQUIRED_SPEC_FIELDS, VALID_OUTPUT_TYPES
from e4.prompts.examples import (
    LOG_ANALYSIS_EXAMPLE,
    METRIC_ANOMALY_EXAMPLE,
    SCORED_CANDIDATE_EXAMPLE,
    typed_api_reference,
)
from e4.sandbox.ast_validator import (
    ALLOWED_MODULES,
    ANNOTATION_ONLY_NAMES,
    MAX_SOURCE_BYTES,
)
from e4.sandbox.typechecker import PROFILE


def authoring_contract_reference() -> str:
    """Render the shared authoring rules and examples from installed E4 code."""
    modules = ", ".join(sorted(ALLOWED_MODULES))
    # `Any` is admitted by name analysis because generated stubs need it, but
    # strict Mypy rejects it in authored expressions/annotations.
    aliases = ", ".join(sorted(ANNOTATION_ONLY_NAMES - {"Any"}))
    fields = ", ".join(sorted(REQUIRED_SPEC_FIELDS))
    output_types = ", ".join(sorted(VALID_OUTPUT_TYPES))
    return "\n\n".join(
        (
            "E4 typed operator authoring contract (shared by initialization and evolution)",
            f"Authoring profile: `{PROFILE}`. Proposal metadata must include: {fields}. "
            f"The declared output_type must be one of: {output_types}. "
            f"Source is limited to {MAX_SOURCE_BYTES} UTF-8 bytes.",
            """Each source defines exactly one synchronous entry point with this signature:

```python
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    ...
```

`inputs`, `params`, and `runtime` are the exact parameter names. Do not rename the
entry point, make it asynchronous, or replace it with a local helper. Additional
ordinary local functions are allowed when useful. `Inputs`, `Params`, `Runtime`,
`Output`, and the profile's other annotation names are supplied by the type-check
profile; do not import `typing` or `e4`. Use the annotation names only to type
code. Construct values through the `make_*` helpers rather than constructing view
classes. The available annotation names are: """ + aliases + ".",
            f"Source imports are limited to these runtime modules: {modules}. "
            "The worker already provides the typed helper functions described below. "
            "No other imports, `Any`, type-checker suppression comments, or unsafe "
            "casts are permitted. Fully parameterize collections, for example "
            "`rows: list[AggregateRowView] = []` and "
            "`by_key: dict[tuple[Scalar, ...], AggregateGroupView] = {}`. "
            "Narrow nullable fields with checks such as `if point.value is None: continue`; "
            "never silently turn missing values into zero.",
            """Runtime objects and output kinds:

- `telemetry_view(inputs[...])` produces a read-only telemetry view. Record query
  helpers return `QueryResult[T]`; their rows are typed views.
- `aggregate_records` returns `QueryResult[AggregateGroupView]`. A group has
  `key`, `group_fields`, `value`, `support`, `missing_count`, `nonfinite_count`,
  and `invalid_count`; use `key` (not an invented `.group` field).
- `summarize` returns `SummaryResult` with `value`, `support`, `missing_count`,
  `nonfinite_count`, and `invalid_count`.
- The `make_*` constructors return the native typed records at runtime. The
  static profile models those values as corresponding `*View` interfaces.
  Query helpers and wrappers such as `aggregate_view` return views. Worker output
  serialization accepts the native record constructors and corresponding views;
  the declared output type still has to match the returned object.
- A `CandidateSet` is a discovery set, not a scored terminal answer. A scored
  answer needs candidates with finite scores and eligible evidence that names
  the candidate among its contributors. The answer boundary also restricts
  candidates to observed entities. Empty or unsupported output should abstain.

`Inputs` keys are generated from declared input port names. `Params` fields are
generated from declared parameter metadata; mark a parameter required or provide
a default before indexing it directly. Optional and nullable parameters need a
`.get(...)` default or a `None` check. The static `Runtime` protocol currently
exposes only `dag_depth: int`; do not assume other runtime attributes.
""",
            """Coverage and source requirements:

`QueryResult` has `rows`, `window`, `source_tables`, and `coverage`. Coverage has
`complete`, `truncated`, `offset`, `next_offset`, `total_matches`, and `support`.
`metric_points`, `span_records`, and `log_records` return at most their requested
row limit; `aggregate_records` returns at most its requested number of groups.
Check `result.coverage.complete` before using a result to establish absence,
compute a full-window ranking, or claim complete support. Raw-query pages may be
combined only when every page is accounted for using `next_offset`; otherwise
abstain. Pass `coverage=result.coverage` to `make_evidence` when evidence comes
from a query, which makes incomplete coverage ineligible for ranking.

When declaring `input_requirements`, `source_columns` is a mapping from available
table names to sequences of required column names, for example
`{"<declared table>": ["<declared column>"]}`. Replace both placeholders with
names from the supplied schema. It is not a flat list or a table-name-to-single-
string mapping. Do not invent table or column names.
""",
            "Runtime API signatures and actual typed-view fields:",
            typed_api_reference(),
            "The signatures and field annotations above are rendered from runtime "
            "implementations. The strict static profile may expose narrower types "
            "such as `Scalar` and `Mapping[str, JSONValue]`; runtime implementation "
            "annotations containing `Any` do not authorize authored `Any` expressions "
            "or annotations. Follow the typed profile and keep authored code fully typed.",
            """Complete examples (API demonstrations, not prescribed diagnostic algorithms):

Metric aggregation with reference/alert coverage checks:

```python
"""
            + METRIC_ANOMALY_EXAMPLE
            + """
```

Complete log summary with coverage and nullable identity handling:

```python
"""
            + LOG_ANALYSIS_EXAMPLE
            + """
```

Scored-output conversion and abstention on unusable rows:

```python
"""
            + SCORED_CANDIDATE_EXAMPLE
            + "\n```",
        )
    )
