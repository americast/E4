# E4 adapter submission

Prepare a dataset adapter for this natural-language task and the explicitly supplied public source metadata and sanitized samples. Adapters are trusted integration code: source code may read the declared dataset through ordinary Python and documented clients. Operator sandbox restrictions do not apply. Do not use hidden repository data or scored labels, answers, or incident identifiers.

## Required response

Return exactly one JSON object matching the machine response schema below. Do not use Markdown fences or add prose before or after the object.

- When the supplied facts are sufficient, return `status: "ready"`, a complete runnable Python module in `source`, and a JSON Schema object in `config_schema`. The source must fill the supplied module template, define `DatasetAdapter(BaseDatasetAdapter)`, use its fixed `__init__(config: Mapping[str, object]) -> None`, implement all four methods, and expose `create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter`. `config_schema` must be an inline Draft 2020-12 object schema with `type: "object"`, `properties`, `required` (use `[]` if none), and `additionalProperties: false`; declare only runtime values the adapter genuinely needs. Inline nested definitions and do not use `$ref` or remote resources.
- When an essential fact is missing, return `status: "needs_input"` and a `required_fields` array with each missing value's name, JSON type, and a concise reason. Ask for only facts required to implement a correct adapter; do not return partial code or guess.

The response schema and Python starter template supplied below are authoritative. Fill the template completely; do not return a schema report, tests, a separate mapping proposal, TODOs, ellipses, or `NotImplementedError`.

## Domain task

{DOMAIN_BRIEF}

## Explicit public source metadata

{PUBLIC_METADATA}

## Sanitized sample rows

{SAMPLE_DATA}

## Runtime canonical constructor signatures

{CANONICAL_TYPE_REFERENCE}

Use these actual constructor fields when creating canonical E4 records. Raw source headers are not constructor keywords: normalize timestamps to `timestamp_ms_utc` and always provide `source_table`. Keep unknown units/outcomes and missing values `None`; do not guess them.

## Strict JSON response schema

{RESPONSE_SCHEMA}

## Required Python module template

{ADAPTER_TEMPLATE}
