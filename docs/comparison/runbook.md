# Bank M5 development and initial M6 comparison

This runbook records the completed M5 readiness work and the paired initial M6 pass. The result is descriptive: the frozen sample contains ten scored Bank incidents at seed 42. Do not alter source, configuration, model profile, case allowlist, or initial library and then resume this checkpoint. A changed setup requires a new frozen manifest and run ID.

The final report is [M5/M6 results](results.md). Rebuild its tracked CSV and SVG from the preserved result artifacts with:

```sh
.venv/bin/python scripts/comparison/make_report.py
```

The scored run used the frozen manifest below (SHA-256 `a4bb8489736cd9b01b72128b974db7e975334c6dd06f407b7e0f252bf96a68c7`):

```text
artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json
```

Its frozen source snapshot is `artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_source.tar.gz` (SHA-256 `14b00ba6c7f028d91ede80abe8b01bb255de626e9ba2a372b1532be5319040ae`). The completed launch used run ID `bankm6i2`; the exact launcher request is preserved in `artifacts/m3/model_runs/bankm6i2/request.json`.

The finished artifacts are preserved at:

```text
artifacts/m3/model_runs/bankm6i2/m6_comparison/comparison_result.json
artifacts/m3/model_runs/bankm6i2/m6_comparison/checkpoint.json
artifacts/m3/model_runs/m5dev05/m5_development/development_comparison_result.json
artifacts/m3/model_runs/m5-schema-bootstrap-20261017-01/m5_bootstrap/bootstrap/bootstrap_result.json
```

For a new development-approved freeze, build a manifest from the completed schema-only bootstrap and pinned Terminus source/image records. Use a short run ID to keep runtime socket paths below the Unix socket length limit:

```sh
.venv/bin/python scripts/comparison/freeze.py \
  --run-id bankm6n01 \
  --bootstrap-result artifacts/m3/model_runs/m5-schema-bootstrap-20261017-01/m5_bootstrap/bootstrap/bootstrap_result.json
```

This writes `artifacts/m5/experiments/bankm6n01/frozen_manifest.json` by default. Preserve its hashes and run a new scored preflight before starting any model requests.

The suite of 270 source tests (5 environment skips) passed before the frozen pass. E4 uses the project Python 3.10 environment; Harbor Terminus-2 uses the pinned Python 3.13 environment and native Docker task image.

The completed scored invocation was:

```sh
.venv/bin/python scripts/model/run.py \
  --model gemma \
  --run-id bankm6i2 --run-timeout 86400 --docker-control -- \
  .venv/bin/python scripts/comparison/run.py --split scored --execute \
    --experiment-manifest artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json \
    --preflight-receipt artifacts/m5/experiments/bank_m6_initial_20261017_02/preflight.json
```

That run ID is already occupied by the completed result; the command is retained as a record and must not be launched again. Use the templates below only for new frozen runs.

## Scored preflight

For a future, newly frozen run, first create a new preflight receipt. The receipt path must not already exist:

```sh
.venv/bin/python scripts/comparison/run.py --split scored --preflight-only \
  --experiment-manifest artifacts/m5/experiments/<frozen-run>/frozen_manifest.json \
  --preflight-receipt artifacts/m5/experiments/<frozen-run>/scored_preflight.json
```

The scorer verifies source/configuration pins and the fixed scored inputs before model startup. It reads evaluator labels only inside trusted evaluation after this preflight. Preserve the receipt alongside the run.

## Scored execution

Run the paired comparison under the pinned local model launcher, using a new run ID and a fresh child run directory:

```sh
.venv/bin/python scripts/model/run.py \
  --model gemma \
  --run-id <new-run-id> --run-timeout 86400 --docker-control -- \
  .venv/bin/python scripts/comparison/run.py --split scored --execute \
    --experiment-manifest artifacts/m5/experiments/<frozen-run>/frozen_manifest.json \
    --preflight-receipt artifacts/m5/experiments/<frozen-run>/scored_preflight.json
```

The launcher allocates the model server run directory; the comparison defaults to its `m6_comparison` child. The Harbor interpreter is taken from the pinned runtime in the manifest unless `--harbor-python` is provided. Keep each case's checkpoint, predictions, source revisions, feedback, and costs. A complete diagnostic miss remains part of the pass. Retry only infrastructure failures, in a new attempt directory, as allowed by the result contract.

## Resume an incomplete paired run

Resume only when the checkpoint and new run use the same frozen manifest, code, model profile, task image, and library lineage. Give the launcher a fresh run ID so it preserves prior server logs; point `--resume-from` to the prior comparison directory:

```sh
.venv/bin/python scripts/model/run.py \
  --model gemma \
  --run-id <resume-run-id> --run-timeout 86400 --docker-control -- \
  .venv/bin/python scripts/comparison/run.py --split scored --execute \
    --experiment-manifest artifacts/m5/experiments/<frozen-run>/frozen_manifest.json \
    --preflight-receipt artifacts/m5/experiments/<frozen-run>/scored_preflight.json \
    --resume-from artifacts/m3/model_runs/<prior-run-id>/m6_comparison
```

The resume path validates the frozen manifest against the checkpoint and continues pending infrastructure work in new attempt directories. It does not rerun completed cases. Do not use resume to repair diagnostic failures or to change a prediction after seeing scored outcomes. A failed feedback protocol or result-contract error requires fixing the implementation, freezing a new experiment, and starting a new run.

## Development readiness

The completed two-case development run is preserved at `artifacts/m3/model_runs/m5dev05/m5_development/`. Its result informed the resource budget and validated the task path; its accuracy is not part of the ten-case M6 result. The fresh schema-only bootstrap is separately preserved under `artifacts/m3/model_runs/m5-schema-bootstrap-20261017-01/`; report its cost separately and amortized over the scored cases. It is not a Terminus-2 bootstrap phase.
