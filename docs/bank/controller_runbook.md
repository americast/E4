# M4 Bank runbook

M4 is a development-only three-stage controller built on the pinned Bank adapter and Gemma profile. The final v4 source passed release verification, and the single schema-only v4 bootstrap completed seed-only. Case one exhausted its stages without an accepted diagnosis; case two continued from case one's resulting library and was accepted at stage 1. Earlier v1–v3 artifacts remain preserved as engineering history. See [M4 results](controller_results.md).

## Requirements and frozen settings

Prepare the Bank public inputs using [Bank runbook](runbook.md). Use the existing `.venv`, `config/models/gemma.json`, and the M4 settings in `config/bank/controller_policy.json`. The controller enforces three diagnosis attempts: select a stored playbook, compose a DAG using existing operators, then synthesize at most three operators. Each attempt allows two static repairs, up to 40 DAG nodes and depth 12, and five retained leads. Feedback checks component-only top-three after each executed attempt and returns only `{"accepted": true|false}` to the diagnostic process.

The initial library holds the ten current M4 built-ins, including `bank.public.project`, the four verified Bank seed playbooks, and any schema-bootstrap additions that pass validation and synthetic fixtures. The total operator cap is 19. Built-ins are immutable; bootstrap and later generated operators share the remaining slots. Eviction uses least distinct development-case use, then qualified operator name. Snapshot files are content addressed and immutable; diagnostic updates create child snapshots, and each run saves its resulting library under its own output directory.

Use `bank.public.project` after a built-in aggregate when full-window summary details such as bucket buffers or trace-value arrays exceed the generated worker input limit. It filters already-computed rows by exact `source_fields`, exact `statistics`, and casefolded component IDs, then pages up to 256 rows with explicit omitted-row counts and a digest of the full source Aggregate. It preserves summary values/support/units and reports omitted non-scalar detail and metadata keys; it does not sample or recompute statistics. Native `bank.public.query` pages are already bounded and should remain unprojected when an analysis needs raw rows or log text.

## Bootstrap

### Current frozen-source bootstrap

Run schema-only bootstrap once with a fresh outer run ID. The v4 bootstrap completed as `m4_schema_bootstrap_v4`; its command and artifact layout are:

```bash
.venv/bin/python scripts/model/launch.py \
  --profile config/models/gemma.json \
  --run-id m4_schema_bootstrap_v4 -- \
  .venv/bin/python scripts/bank/bootstrap.py \
  --profile config/models/gemma.json \
  --run-dir m4_bootstrap
```

The bootstrap prompt includes pinned table schemas and units, static official candidate IDs, runtime/operator contracts, the four seed examples, and the public-query/projection output contracts. It does not load case telemetry or evaluator labels. Proposed operators and playbooks must pass static contracts and independent synthetic behavior checks before the immutable library snapshot is written. Use the first resulting `library/` snapshot to initialize case one, then pass case one's resulting diagnostic library to case two. Do not regenerate or select another bootstrap based on usefulness or diagnosis performance.

The snapshot is at:

```text
artifacts/m3/model_runs/m4_schema_bootstrap_v4/m4_bootstrap/bootstrap/library/
```

The bootstrap status, prompts, full model calls, proposed bundle, rejection log, synthetic outcomes, and `bootstrap_result.json` are under `artifacts/m3/model_runs/m4_schema_bootstrap_v4/m4_bootstrap/bootstrap/`. It completed `complete_seed_only` with snapshot `1862834dc9bc244f81aed5fa3aea6e531beaa54a9453f0def20d19fd9d8e58f4` (10 built-ins, four seeds, zero generated additions). Its proposed log-pattern operator passed static validation but failed all five synthetic executions with a metadata-cache wire-envelope runtime error, so it was not admitted. This was the sole bootstrap sample; do not regenerate or change the prompts based on this synthetic failure. Case one starts from the recorded snapshot; case two follows its evolving child lineage.

### Historical bootstrap and revalidation

The v2 bootstrap was created under an earlier implementation fingerprint, and the v3 revalidation created a valid metadata-only child for the serving-only correction. This is retained as history and is not the v4 workflow. A fresh bootstrap produced by the current frozen source uses its own library directly; no old snapshot ID or revalidation is involved.

For the recorded v2 store only, do not point diagnostics directly at its old-fingerprint snapshot. The v3 run revalidated it into a fresh output directory after the corrected source and configuration were frozen:

```bash
.venv/bin/python scripts/bank/revalidate_library.py \
  --source-library artifacts/m3/model_runs/m4_schema_bootstrap_v2/m4_bootstrap/bootstrap/library \
  --bootstrap-artifact-dir artifacts/m3/model_runs/m4_schema_bootstrap_v2/m4_bootstrap/bootstrap \
  --expected-snapshot-id 3e194f3e816a9446b70fe59042ce688a5afdb4e860d709117d0604091dcc8c45 \
  --output-dir artifacts/m4/verification/revalidated_initial_v3 \
  --profile config/models/gemma.json \
  --policy config/bank/controller_policy.json \
  --reason "Revalidate the pristine seed-only bootstrap after the frozen implementation-only repair"
```

The frozen v3 run passed this check: all 20 fixed synthetic seed checks passed, producing child snapshot `82d88893533f6ad021aa890490e4f8aab97fadbddbdb7674f57b144e7ca03e9a` at `artifacts/m4/verification/revalidated_initial_v3/library/`. The original bootstrap store remains unchanged. For a future source freeze, use a fresh output directory and its matching recorded source fingerprint.

## Development diagnostics

Use a fresh launcher run ID and output directory for each case. The two frozen development cases are `bank-20210323T0630-utc8` and `bank-20210325T1530-utc8`.

```bash
.venv/bin/python scripts/model/launch.py \
  --profile config/models/gemma.json \
  --run-id m4_firstdev_v4 -- \
  .venv/bin/python scripts/bank/diagnose.py \
  --mode diagnostic \
  --profile config/models/gemma.json \
  --run-dir m4/firstdev \
  --case-id bank-20210323T0630-utc8 \
  --library-root artifacts/m3/model_runs/m4_schema_bootstrap_v4/m4_bootstrap/bootstrap/library
```

The controller runs inside a restricted project view containing only the selected development case, its public manifest/catalog, code/configuration, and the chosen snapshot. It has no evaluator, scored labels, candidate truth, other case clip trees, plans, or previous run artifacts. Generated operators run in a separate bubblewrap worker with typed bounded inputs and no evaluator or host data mounts. The supervisor evaluates the top-three component IDs in a trusted process and sends back one boolean over a private socket.

The diagnostic run is under:

```text
artifacts/m3/model_runs/m4_firstdev_v4/m4/firstdev/diagnostic/
```

`diagnostic_result.json` records every stage, accepted flag, DAG hash, execution record, runtime, query budget, static repair, and final status. A process exit code of zero alone does not mean the trusted evaluator accepted the diagnosis; inspect the recorded stage and accepted fields. `model_calls.jsonl` retains every full request/response. `library/` contains the immutable snapshot returned by the run, including accepted additions or metadata-only use counts. The completed case-one v4 run exhausted its stages without acceptance and changed the library through usage accounting only; case two used that child snapshot and was accepted at stage 1.

The outer `supervisor_manifest.json` is alongside the `diagnostic/` directory. The trusted feedback audit is written outside the restricted project view at `artifacts/m3/model_runs/m4_firstdev_v4/m4_trusted_feedback/firstdev.jsonl`.

For the second frozen development case, use a new launcher run ID and case one's resulting library:

```bash
.venv/bin/python scripts/model/launch.py \
  --profile config/models/gemma.json \
  --run-id m4_seconddev_v4 -- \
  .venv/bin/python scripts/bank/diagnose.py \
  --mode diagnostic \
  --profile config/models/gemma.json \
  --run-dir m4/seconddev \
  --case-id bank-20210325T1530-utc8 \
  --library-root artifacts/m3/model_runs/m4_firstdev_v4/m4/firstdev/diagnostic/library
```

Preserve all bootstrap and diagnostic output directories. The two final development cases made no queries and admitted no real generated operators. Admission and retention tests use scripted synthetic clients and fixture operator source; they are not evidence that Gemma discovered or admitted an operator. Do not run this harness on scored cases.

## Failure handling and reports

A failed bootstrap does not create an active library snapshot. A rejected diagnosis can advance to the next stage, while a trusted-evaluator process failure aborts the run without revealing label details. Preserve invalid model responses, static repairs, execution failures, rejected proposals, and library lineage. Do not regenerate a prompt, DAG, or operator because a development feedback bit was negative.

Record the snapshot IDs, all stage outcomes, proposal acceptance/rejection reasons, query requests/returned rows/bytes, cache decisions, model calls/tokens/latency, repairs, and final outputs in [M4 results](controller_results.md). These two development cases are engineering checks; they do not estimate scored-set accuracy.
