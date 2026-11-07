# Shipped paper-family library

The package now has native implementations for six reconstructed paper operator families, three backend playbooks, and one dependency playbook that additionally needs span telemetry. This is a partial implementation of the reported starter inventory, not all 19 operators and 11 playbooks.

The [paper, §4.1, §5 and Table 2](https://arxiv.org/pdf/2610.08622) supplies the family descriptions and reported counts. It does not publish a complete original source/contract/playbook artifact. Names, exact formulas, parameter defaults and DAGs in this package are reconstructions. The complete reconstructed inventory and explicit gaps are in [paper_catalog.json](../e4/starters/resources/paper_catalog.json).

## Runnable operators

| Operator | What this implementation does |
| --- | --- |
| `paper.backend.metric_conditions` | Generate observed component candidates from supported reference/alert metric changes. Preserve role, identifier, metric name and unit separately. |
| `paper.backend.mean_welch` | Compare fixed-width time-block means with a two-sided Welch test. |
| `paper.backend.location_permutation` | Compare means of the time-block observations with a seeded two-sided permutation test. |
| `paper.backend.tail_permutation` | Compare a declared quantile of time-block means with a seeded permutation test. This is a tail of block summaries, not the raw-record tail. |
| `paper.scores.dependency_adjust` | Reduce a caller's score when an observed same-trace parent/child edge and eligible callee anomaly support the adjustment. Missing or ambiguous links do not create edges. |
| `paper.answers.weighted_consensus` | Combine supported normalized scores, deduplicate repeated scorer inputs, aggregate within families and preserve independent-family corroboration and original evidence. |

Code: [paper.py](../e4/starters/paper.py), with the statistical routines in [statistics.py](../e4/starters/statistics.py). Source selectors and thresholds are declared parameters; playbooks bind actual adapter table names. Native implementations stream the canonical record store rather than repeatedly fetching bounded raw pages. They do not import the legacy controller or Bank-specific preparation code.

The default books are `backend_mean_shift`, `backend_location_shift` and `backend_tail_degradation`. `dependency_adjusted_backend_consensus` is available with compatible span records; its adjustment remains conditional on observing usable parent links at runtime.

## Availability and fidelity gaps

The catalog includes 19 reconstructed operator entries and 11 reconstructed playbook entries. **Thirteen operator entries and seven playbook entries remain unimplemented.** Client/cohort/session, URL and event-sequence capabilities need explicit supported data mappings and validated implementations. Listing them is documentation, not registration of placeholders.

`paper_availability(schema)` distinguishes unimplemented families from implemented families unavailable for a schema. Schema checks use canonical record kinds and declared entity roles; they do not demand Bank's raw header names. Backend role assumptions are explicit. Opaque app transaction identities are not promoted into component candidates.

The statistical reconstruction assumes fixed-width blocks are suitable independent units; adapter conformance does not establish that assumption. Tests are per metric stream, not a claim of global multiple-testing significance. Metric units and semantics must be comparable across windows; counter-to-rate conversion and causal attribution are not inferred automatically. Scores are ranking evidence, not calibrated root-cause probabilities.

## Package API

```python
from e4 import prepare_library, paper_catalog, paper_availability

library = prepare_library(adapter.get_schema(), mode="paper")
library.save_to_dir("prepared_library")
```

Saved paper libraries restore their native implementations from the installed package while checking operator contracts. Explicit custom libraries remain caller-controlled. `prepare_library(schema, mode="clean_slate")` returns an empty executable library; domain generation must populate and validate it before diagnosis. The mode persists in `library.json`.

`paper_reference_examples()` supplies six operator contracts and two illustrative playbook DAGs. All examples are explicitly reference-only and do not register capabilities. The complete catalog, active inventory and examples are separate concepts.

This library and the revised bootstrap prompts have **no benchmark accuracy result yet**. The completed 70% Bank campaign used an explicit earlier Bank library. Model generation and experiment reruns remain paused for prompt review.
