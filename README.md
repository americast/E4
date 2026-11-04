# E4

## Setup

Use Python 3.10. Create a virtual environment and install the pinned runtime and test dependencies:

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -r requirements-m3.lock
.venv/bin/python -m pip check
```

Run the test suite with Python's built-in unittest discovery:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Worker tests expect `/usr/bin/python3.10` and a Linux host where bubblewrap user and network namespaces work.

## Bank data

Prepare and verify the Bank dataset using the commands in the [Bank runbook](docs/BANK_RUNBOOK.md) before running the commands below.

To run the four CPU-only seed controls, set `E4_RUN_DIR` to a fresh, existing, writable directory. For example:

```bash
mkdir -p artifacts/bank_controls/local_firstdev
E4_RUN_DIR=artifacts/bank_controls/local_firstdev \
  .venv/bin/python scripts/run_bank_development.py \
  --case-id bank-20210323T0630-utc8 --controls
```

The controls read the prepared public clips and make no model requests. The case output is written beneath `$E4_RUN_DIR/development/<case-id>/controls`. Use a fresh `E4_RUN_DIR` for each repeated run.

## Starter library

With the prepared Bank experiment manifest at `artifacts/m5/experiments/bank_m6_initial_20261017_02/frozen_manifest.json`, create a validated starter-library snapshot:

```bash
.venv/bin/python scripts/freeze_paper_starter.py
```

The command prints the new artifact directory containing `bank_library/` and `freeze_manifest.json`.

## Local Gemma inference

The selected model is Gemma; see `config/experimental_model.json`. Its inference settings are in `config/gemma_m3_inference.json`, which uses the pinned `config/model_manifest.json`.

First download the manifest-pinned model and build the pinned llama.cpp server. The downloader verifies the configured commit, file size, and SHA-256. Running it with no arguments uses the existing pin; `--resolve` changes that pin.

```bash
.venv/bin/python scripts/fetch_model.py

git clone --no-checkout https://github.com/ggml-org/llama.cpp third_party/llama.cpp
git -C third_party/llama.cpp checkout d81235049384534c167caea52b85a694f6103d14
export PATH=/usr/local/cuda-12.3/bin:$PATH
export CUDACXX=/usr/local/cuda-12.3/bin/nvcc
cmake -S third_party/llama.cpp -B third_party/llama.cpp/build-v100 \
  -DGGML_CUDA=ON -DCMAKE_CUDA_ARCHITECTURES=70 -DCMAKE_BUILD_TYPE=Release
cmake --build third_party/llama.cpp/build-v100 \
  --target llama-server llama-cli -j 12
```

Inference requires a CUDA-capable GPU, CUDA 12.3 and CMake 3.22 or newer for the pinned server build, and Linux bubblewrap with working user/network namespaces. The launcher checks the model checksum, llama.cpp revision, GPU memory reserve, and offline namespace before starting the run.

Use a new run ID for each model run:

```bash
.venv/bin/python scripts/run_m3_model.py \
  --profile config/gemma_m3_inference.json \
  --run-id bank_firstdev_local_01 -- \
  .venv/bin/python scripts/run_bank_development.py \
  --case-id bank-20210323T0630-utc8
```

The launcher saves its logs and provenance under `artifacts/m3/model_runs/<run-id>/`; the Bank DAG, calls, and execution records are saved under `development/<case-id>/` within that run directory. To bootstrap the Bank library or run the three-stage harness, follow the [three-stage harness runbook](docs/M4_RUNBOOK.md).

## Current results

The historical E4 rows below are from the **kernel-constrained variant** in commit `7de48fe`. The new typed operator-code variant has one completed seed42 evaluation; its authoring-interface fixes and planned simplification are pending.

Accuracy measures ground-truth inclusion in the top three on ten Bank incidents. Historical rows average decoding seeds 42 and 43; the operator-code row is seed42 only. Historical E4 rows are cumulative stage prefixes. Token cost includes input and output tokens for all model calls and repairs, relative to the same-seed T2 baseline (100%).

| Method | Accuracy | Token cost (% of T2; seed42) |
|---|---:|---:|
| E4: retrieval | 30.0% | 4.2% |
| E4: retrieval + adaptation | 55.0% | 8.1% |
| E4: retrieval + adaptation + evolution | 70.0% | 11.4% |
| E4: typed operator-code variant (seed42 only) | 50.0% | 14.7% |
| T2: up to three diagnoses | 40.0% | 100.0% |

Historical final E4 accuracy is 80% for seed42 and 60% for seed43. The operator-code run scored 20% after retrieval and 50% after adaptation; all five evolution attempts failed proposal validation, adding no successes. Cost covers seed42 only because T2 seed43 token usage is incomplete. These exploratory runs reuse ten distinct incidents and do not establish statistical significance.

See the [verified results](docs/BANK_CURRENT_RESULTS.md) for per-seed counts, token totals and result hashes.
