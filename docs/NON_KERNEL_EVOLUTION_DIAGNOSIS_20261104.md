# Why non-kernel evolution failed

The completed seed42 Bank run scored **2/10 after retrieval, 5/10 after adaptation, and 5/10 after evolution**. Evolution was reached on five incidents. All five stopped during proposal validation, before Stage 3 serving or execution of a newly authored operator. This run therefore does not establish that a kernel catalog improves diagnostic reasoning.

No additional inference runs were started for this investigation. The checks below inspect saved proposals, actual prompts, and static validation. They do not change the recorded experiment.

## What actually failed

Across the five incidents there were 15 EVOLVE responses, including the permitted repairs:

| First reported blocker | Responses |
| --- | ---: |
| Invalid JSON | 4 |
| Invalid operator name | 5 |
| Incomplete input requirements | 6 |

The 11 parseable responses contained 22 operator proposals. Twenty-one source bodies had valid Python syntax. The observed zero evolution gain was dominated by the surrounding interface, rather than operators executing and producing unsuccessful diagnoses.

One repeated example is the name `evolved.bank.metric_volatility_contrast`. The validator permits a single lowercase suffix after `evolved.`, such as `evolved.metric_volatility_contrast`. Registered starter operators use longer namespaces, so copying that naming style is an understandable model mistake. The regex is published, but the resulting repair consumes the same small repair budget needed for substantive code errors.

Every parseable proposal's Telemetry requirement omitted `group_by`. Some CandidateSet requirements also omitted the empty source/statistic/support declarations that our validator requires even for this port type.

## Confirmed problems in our interface

### 1. The actual EVOLVE prompt contains a conflicting instruction

The common prompt includes:

> No generated source is written in this stage.

Later text correctly asks for new Python source. Both instructions were present in the actual `stage3_evolve_call1.txt` and repair prompts sent during the run. The conflict comes from the generic DAG-wiring fallback in [`_stage_prompt`](../e4core/m4_controller.py).

The code-profile branch needs phase-specific common instructions, including Python wording during serving rather than leftover IR wording.

### 2. The published contract is ambiguous about required fields

[`typed_operator_code_contract_schema`](../e4core/m4_generated.py) inherits a legacy schema where `authoring_profile` and `input_requirements` appear under `optional_fields`, although they are mandatory for this profile. The nested requirement object describes fields but does not explicitly list its required field names.

There is also a direct contradiction: the description says to use `group_by_by_table` **instead of** flat `group_by`, while [`validate_input_requirements`](../e4core/m4_capabilities.py) still requires the flat key. An empty `group_by: []` must accompany table-specific grouping under the current validator.

A complete raw-input declaration currently looks like this:

```json
{
  "data": {
    "source_columns": {
      "bank/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"]
    },
    "statistics": ["raw"],
    "group_by": [],
    "support_unit": "raw_row",
    "coverage": "full_window"
  }
}
```

The prompt should publish an exact, machine-checkable schema and small contract-only examples for each supported input type. Examples should teach API usage without supplying diagnostic algorithms.

### 3. Repairs discard the source being repaired

[`_safe_repair_artifact`](../e4core/m4_controller.py) replaces every source field with `[rejected executable source omitted]`. [`_static_repair_prompt`](../e4core/m4_controller.py) then asks for a corrected complete JSON object.

Thus a repair for an operator name or missing metadata also requires regenerating the function. The model can introduce new code errors while fixing metadata. The validator reports the first encountered error; with two repairs, several cases used the entire budget on JSON/name/requirement errors and never received Python feedback.

Source should remain available as quoted data for source-mode repair, with the same validation and sandbox checks applied to the result. Repair context still needs an actual token-budget check; silently truncating code would recreate the problem.

### 4. Python is embedded in unconstrained JSON output

The client asks for JSON in prose but sends no `response_format` in [`LocalModelClient.make_payload`](../e4core/model_client.py). Escaping multiline Python makes that response harder to serialize than a small kernel program. Four responses were not valid JSON.

The pinned local server documents schema-constrained JSON in [`llama.cpp's server README`](../third_party/llama.cpp/tools/server/README.md), under `/v1/chat/completions`. This should be tested for the exact pinned server and reasoning configuration before adoption. Constraining the response envelope can enforce valid JSON, names and required metadata while leaving the Python analysis body model-authored. It does not prove Python correctness or prevent output truncation.

### 5. The typing/API requirements need clearer authoring guidance

Fixing only metadata would not be sufficient. I ran a separate **offline diagnostic** over the 22 saved proposals: canonicalize name punctuation, supply hypothetical empty/default metadata fields, and leave every source body unchanged. This was not a serving run and did not alter the original proposals or scores.

| Next static-validation result | Proposals |
| --- | ---: |
| Passed spec/type checking | 3 |
| Mypy errors | 13 |
| Missing imports | 2 |
| Restricted import | 1 |
| Annotation-only alias used as a runtime value | 1 |
| Forbidden lambda | 1 |
| Python syntax error | 1 |

Common substantive mistakes included:

- Iterating a `QueryResult` directly instead of its `.rows` sequence.
- Calling `summarize(..., "median")` when the published helper accepts `quantile` with a quantile argument.
- Using a nullable summary value without a missing-value check.
- Defining nested helpers without annotations, causing strict Mypy checks to infer and reject `Any` throughout the downstream code.
- Using an allowed module without importing it, or using a lambda despite the declared AST policy.

The guidance currently says ordinary Python loops/math/statistics are available but does not make the strict nested-function annotation requirements sufficiently explicit. Its final API pointer also names `sandbox.typed_operator_code_api`, while that object is actually a top-level schema field.

These are reasons to improve the published API and compiler feedback, rather than remove typing. The three static passes also do not demonstrate correct evidence, producer compatibility, runtime behavior or diagnostic utility.

## Why the kernel version had an easier path

The previous compact V2 path has a host-supplied ABI: [`model_facing_operator_ir_v2_schema`](../e4core/m4_operator_ir.py) publishes fixed input ports and requirements that the normalizer supplies. The model primarily emits a small typed program over known operations.

The Python path currently adds responsibility for full port metadata, imports, nested typing, API usage, and escaping source inside JSON. Comparing the two runs therefore mixes analysis expressiveness with a different amount of interface and compiler work. Our new path needs reliable authoring support before interpreting the accuracy difference.

Retrieval and adaptation results also changed before evolution: this run's cumulative prefixes are 20% and 50%. Code-profile wording is added to common prompts across stages, so the overall accuracy change is not an isolated evolution ablation.

There is also direct evidence that a fixed seed does not make this setup fully repeatable. The engineering canary and the full evaluation's first incident used identical initial prompt hashes and identical complete request-payload hashes, including seed 42 and sampling settings, but produced different accepted outcomes. This is an observation; the precise backend cause has not been established. The separate canary must not replace the full run's failed first-case trajectory.

## Fix order before another evaluation

1. Remove contradictory phase instructions and publish one exact code-profile schema with explicit required fields, valid names, accurate grouping semantics and correct API pointers.
2. Preserve source during repairs and report actionable errors together across the proposal bundle, within the existing repair and context budgets.
3. Test schema-constrained JSON against the pinned server using a neutral transport fixture, then apply it to the code-authoring envelope if it works correctly with reasoning enabled. Record this protocol change in the next freeze.
4. Add small, statically checked API examples covering `.rows`, coverage, nullable summaries, quantiles, imports and annotated nested helpers. Keep diagnostic algorithms model-authored.
5. Revalidate the saved failures offline as regression fixtures. Then run a fresh source-bound real-model canary that actually reaches authoring and serving before another ten-case run.

Keep strict types, data capability checks, source policy, isolation, evidence validation and acceptance rules. Do not change the starter analyses or thresholds to produce a trend.

## Evidence locations

DGX candidate root:

```text
/raid/ssinha97/E4/artifacts/operator_code_fix_20261103/execution_candidate_20261104
```

Paths relative to that root:

```text
artifacts/operator_code_evaluation/seed42/audits/20261104T060301Z-bc016eb2/combined_bank10_result.json
artifacts/operator_code_canary/evolution_metadata_diagnosis.json
artifacts/operator_code_canary/offline_metadata_peel_diagnosis.json
```

Per-case `diagnostic/prompts/stage3_evolve_call*.txt` files contain the actual prompts. `diagnostic_result.json` contains the recorded first-error sequence. The investigation reads final structured proposal content and whitelisted metadata; model reasoning is excluded.
