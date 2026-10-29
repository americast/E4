# E4

## Setup

Use Python 3.10. Create a virtual environment and install the pinned runtime and test dependencies:

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock
.venv/bin/python -m pip check
```

Run the test suite with Python's built-in unittest discovery:

```bash
.venv/bin/python -m unittest discover -s tests -t . -v
```

Worker tests expect `/usr/bin/python3.10` and a Linux host where bubblewrap user and network namespaces work.

## Bank data

Prepare and verify the Bank dataset using the commands in the [Bank runbook](docs/bank/runbook.md) before running the commands below.

To run the four CPU-only seed controls, set `E4_RUN_DIR` to a fresh, existing, writable directory. For example:

```bash
mkdir -p artifacts/bank_controls/local_firstdev
E4_RUN_DIR=artifacts/bank_controls/local_firstdev \
  .venv/bin/python scripts/bank/develop.py \
  --case-id bank-20210323T0630-utc8 --controls
```

The controls read the prepared public clips and make no model requests. The case output is written beneath `$E4_RUN_DIR/development/<case-id>/controls`. Use a fresh `E4_RUN_DIR` for each repeated run.

## Starter library

Create a validated starter-library snapshot:

```bash
.venv/bin/python scripts/freeze_paper_starter.py
```

The command prints the new artifact directory containing `bank_library/` and `freeze_manifest.json`.

## Local Gemma inference

The selected model is Gemma; see `config/records/model_selection.json`. Its inference settings are in `config/models/gemma.json`, which uses the pinned `config/models/gemma_manifest.json`.

First download the manifest-pinned model and build the pinned llama.cpp server. The downloader verifies the configured commit, file size, and SHA-256. Running it with no arguments uses the existing pin; `--resolve` changes that pin.

```bash
.venv/bin/python scripts/model/fetch.py

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
.venv/bin/python scripts/model/run.py \
  --model gemma \
  --run-id bank_firstdev_local_01 -- \
  .venv/bin/python scripts/bank/develop.py \
  --case-id bank-20210323T0630-utc8
```

The launcher saves its logs and provenance under `artifacts/m3/model_runs/<run-id>/`; the Bank DAG, calls, and execution records are saved under `development/<case-id>/` within that run directory. To bootstrap the Bank library or run the three-stage harness, follow the [three-stage harness runbook](docs/bank/controller_runbook.md).

## Current results

Accuracy measures ground-truth inclusion in the top three, averaged across decoding seeds 42, 43 and 44 on the same ten Bank incidents. E4 rows are cumulative stage prefixes. Token cost is the mean same-seed input-plus-output token ratio relative to T2 (100%), including repairs and recovery calls.

| Method | Mean accuracy | Mean token cost (% of T2) |
|---|---:|---:|
| E4: retrieval | 30.0% | 5.6% |
| E4: retrieval + adaptation | 43.3% | 9.3% |
| E4: retrieval + adaptation + evolution | 46.7% | 15.5% |
| T2: up to three diagnoses | 36.7% | 100.0% |

Accuracy covers all three seeds. Cost covers seeds 42 and 44 only; T2 seed43 token usage is incomplete. These exploratory results use ten distinct incidents and do not establish statistical significance.

The local report is `artifacts/seed_variability/reports/pilot_20261025_s43_s44_v2/seed_variability_report.md`.
