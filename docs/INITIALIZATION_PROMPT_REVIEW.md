# Onboarding prompts to review

This is the corrected adapter-first review checkpoint. Prompt export is offline. No domain library generation or experiment rerun has been performed.

## Read these templates in order

| Step | Purpose | Template |
| --- | --- | --- |
| 1 | Develop the dataset adapter from task prose, public metadata and samples; validate identities, time, schema and canonical records | [Adapter development](../e4/prompts/resources/adapter_system.md) |
| 2 | Load the shipped compatible library by default; this is deterministic, not an LLM regeneration step | [Library implementation and gaps](PAPER_DEFAULT_LIBRARY.md) |
| 2a | Optionally generate a task-specific library from an empty executable inventory | [Clean-slate generation](../e4/prompts/resources/clean_slate_system.md) |
| 3 | Extend the shipped library before the first incident; allow playbook-only additions or an explicit no-change result | [Pre-run bootstrap](../e4/prompts/resources/bootstrap_system.md) |
| 4 | Retrieve a valid playbook, then compose registered operators if needed | [SELECT and COMPOSE templates](../e4/prompts/templates.py) |
| 5 | Author new source capabilities after failed diagnosis, then compose their use in a separate call | [EVOLVE and SERVE templates](../e4/prompts/templates.py) |

## Expected deliverables

| Stage | What the model returns |
| --- | --- |
| Adapter | Strict JSON containing a completed Python scaffold in `source`, its `config_schema`, and `status: ready`; or typed `needs_input` fields. |
| Bootstrap or clean slate | Complete operator Python source with registration metadata and playbook DAGs in the machine-readable JSON envelope. Bootstrap can also return no changes or playbooks only. |
| Adaptation / serving | The executable DAG JSON, using registered operators. |
| Evolution | Complete new operator Python source and the required registration/gap metadata; serving remains a separate call. |

Schema declarations, identity/time handling, table availability, missing values and source references are correctness requirements implemented in code. Conformance checks are acceptance checks for that code. The response does not need a separate policy summary, table inventory, assumptions report or knowledge ledger. Missing authoring facts use structured `needs_input` fields; runtime values are declared in `config_schema`. The package consumes both forms without an orchestrating LLM.

Clean-slate generation replaces the default-library preparation path; it does not trigger another automatic bootstrap pass. Existing paper operators/playbooks can be shown as reference examples while remaining absent from the clean-slate registry.

Every library-authoring prompt uses [shared generic instructions](../e4/prompts/resources/initialization_system.md), a domain brief and the installed [typed authoring reference](../e4/prompts/authoring.py). The [domain brief template](../e4/prompts/resources/domain_brief_template.md) is guidance; ordinary natural-language input is accepted.

The adapter coding contract and deterministic parser/loader workflow are documented in [ADAPTER_SUBMISSIONS.md](ADAPTER_SUBMISSIONS.md).

## Bank information and human additions

The [Bank brief](../e4/prompts/resources/bank_domain_brief.md) supplies task/schema facts, including exact raw headers, canonical field mappings, timestamp handling, opaque app identities, unknown units and missing source outcomes. The [optional analytical priors](../e4/prompts/resources/bank_domain_priors.md) remain opt-in and explicitly evaluation-informed.

Shipped examples and their reconstructed algorithms are also human-supplied information. The caller retains supplied prompts/domain inputs and generated code/DAG artifacts; any later assessment of their use and benefit is separate from the model response. Benefit remains unmeasured. The [fidelity report](PAPER_DEFAULT_LIBRARY.md) distinguishes six runnable families from the complete 19-entry catalog. Catalog entries and examples are not automatically callable.

## Export the prompts

```bash
e4 adapter-prompt samples.txt --domain-file domain.md --metadata-file metadata.json

e4 initialization-prompt domain.md --schema schema.json

e4 initialization-prompt domain.md --schema schema.json --clean-slate
```

`schema.json` is the validated adapter's `DatasetSchema.as_dict()` output. Without it, the export marks active compatibility as pending rather than inventing capabilities. `--bank` selects the packaged Bank brief; `--include-domain-priors` adds the separately attributed priors.

## Runtime and persistence checks

- New engines without an explicit library load compatible paper defaults when the adapter schema becomes available. Explicit custom libraries retain their own behavior.
- `library_mode="clean_slate"` and saved clean-slate libraries exclude shipped operators/playbooks. An empty clean-slate library stops before model calls until initial operators/playbooks are prepared.
- Saved paper libraries restore native functions and check stable operator contracts. Per-case default table bindings refresh without resetting learned additions.
- Runtime prompts show schema-filtered active signatures. SERVE additionally sees validated temporary operators; source authoring and DAG composition remain separate calls.
- Reused playbook metadata must remain structured JSON rather than frozen-mapping representations. Prompt sizes must leave the declared generation reserve available.

Generation/assembly of task-specific additions, their validation/freeze and benchmark evaluation remain the next phase after this review. The last completed Bank campaign used the explicit old Bank library; it does not evaluate these defaults or prompts.


## Complete local exports

These are review snapshots, not another maintained set of templates. Runtime values remain symbolic; Bank schema metadata comes from a public development clip, with no scored labels or incident answers.

| Prompt | Complete export |
| --- | --- |
| Adapter development | [Adapter](../artifacts/structured_adapter_review_20261107/adapter_prompt.md) |
| Default pre-run bootstrap | [Bootstrap](../artifacts/code_first_prompt_review_20261106/bootstrap_bank.md) |
| Optional Bank priors | [Bootstrap with priors](../artifacts/code_first_prompt_review_20261106/bootstrap_bank_with_priors.md) |
| Clean-slate generation | [Clean slate](../artifacts/code_first_prompt_review_20261106/clean_slate_bank.md) |
| Retrieval | [SELECT](../artifacts/code_first_prompt_review_20261106/retrieval.md) |
| Adaptation | [COMPOSE](../artifacts/code_first_prompt_review_20261106/adaptation.md) |
| Evolution | [EVOLVE](../artifacts/code_first_prompt_review_20261106/evolution.md) |
| Separate composition after evolution | [SERVE](../artifacts/code_first_prompt_review_20261106/serving.md) |

## Validation at this stop

The structured coding-submission revision passed **147 maintained tests in 157.026 seconds**. A fresh wheel installed under a temporary target parsed and loaded an adapter through the fixed factory, checked typed records in both windows, handled `needs_input`, and exercised the CLI from `/tmp` with imports verified outside this checkout. CLI validation was tested with source that would raise if executed; it remained static.

The current adapter [scaffold](../e4/data/resources/adapter_template.py.txt), [response schema](../e4/data/resources/adapter_response.schema.json), and [package workflow](ADAPTER_SUBMISSIONS.md) define the machine contract. The caller uses ordinary Python, not another LLM, to decode, validate configuration and instantiate the adapter. Unknown facts are structured input requests, not free-text clarifications.

Latest adapter exports, wheel and source hashes are in `artifacts/structured_adapter_review_20261107/`. Earlier code-first and onboarding validation records remain preserved separately. Other library/runtime prompt exports above are unchanged.

**Stop here for user review.** Validation used manually authored engineering fixtures. No live domain-adapter/library generation or incident benchmark experiment ran. Changes remain unstaged and uncommitted.
