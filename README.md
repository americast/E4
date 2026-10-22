# E4

E4 lets you define, validate, execute, and replay telemetry-analysis DAGs using typed operators, cached computations, and isolated local model runs.

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

Prepare and verify the Bank dataset before running the Bank commands below. The [Bank runbook](docs/BANK_RUNBOOK.md) documents source provenance, timestamp units, preparation, clip verification, and supported operator families. The [Bank results](docs/BANK_RESULTS.md) records the current development observations.

To run the four CPU-only seed controls, set `E4_RUN_DIR` to a fresh, existing, writable directory. For example:

```bash
mkdir -p artifacts/bank_controls/local_firstdev
E4_RUN_DIR=artifacts/bank_controls/local_firstdev \
  .venv/bin/python scripts/run_bank_development.py \
  --case-id bank-20210323T0630-utc8 --controls
```

The controls read the prepared public clips and make no model requests. The case output is written beneath `$E4_RUN_DIR/development/<case-id>/controls`. Use a fresh `E4_RUN_DIR` for each repeated run.

## Current Bank results

The R5 ten-incident revision audit is in progress on the V100 and Wingtip GPUs. Across its first two incidents, retrieval is **0/2**, and adaptation is **0/2**. Both evolution proposals authored typed operators, but their gap records failed static schema validation after the repair allowance, so no third diagnosis was submitted. The other eight incidents remain to be evaluated. These interim counts are not the final ten-case score. Terminus-2's historical result on the same ten incidents is **4/10** after up to three diagnoses. R5 artifacts are under `artifacts/m3/model_runs/e4v13bank10localr5/` and `artifacts/m3/model_runs/e4v13bank10wingr5/`; the frozen audit manifest is `artifacts/e4_improvement/genuine_ir_v13_bank10_r5/audit_manifest.json`.

The R5 starter library contains eleven registered operators:

| Operator | Purpose |
|---|---|
| `case.telemetry` | Supplies the hash-pinned public telemetry for the selected Bank case. |
| `bank.candidates.generate` | Lists exact component IDs observed in that case; it does not infer aliases or topology. |
| `bank.metrics.aggregate` | Builds per-window native metric summaries and derived per-minute log counts. |
| `bank.metrics.contrast` | Scores supported, explicitly allowlisted metric changes between reference and alert windows. |
| `bank.traces.aggregate` | Summarizes native trace durations per trace and component. |
| `bank.traces.latency_contrast` | Scores per-trace duration changes; the source duration unit is undeclared. |
| `bank.logs.volume_contrast` | Scores per-minute log-volume changes; this is not an error-rate measure. |
| `bank.public.query` | Reads a bounded, hash-checked page of native columns from one case window. |
| `bank.public.project` | Selects rows from existing summaries while reporting omitted detail and coverage. |
| `evolved.log_pattern_scorer` | Counts a selected text pattern in alert-window log values. |
| `bank.answers.combine` | Combines eligible evidence families with the configured weights and normalization. |

The five starter playbooks are `combined_shared_candidates`, `log_depth_analysis`, `native_metric_contrast`, `per_minute_log_volume`, and `per_trace_native_duration`.

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
