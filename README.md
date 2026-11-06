# E4: Root-Cause Analysis Engine

E4 is a lightweight, domain-agnostic Root-Cause Analysis (RCA) engine for microservice and distributed system telemetry. It executes a 3-stage progression loop (**Retrieve $\to$ Adapt $\to$ Evolve/Serve**) using typed Directed Acyclic Graphs (DAGs) and sandboxed Python operator synthesis.

---

## 1. Installation & Environment Setup

Use Python 3.10:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e .
```

---

## 2. Running Verification Tests

```bash
# Run all maintained engine and regression tests (37 tests):
.venv/bin/python -m unittest discover -s tests/test_e4 -v

# Run entire discoverable test suite:
.venv/bin/python -m unittest discover -s tests -v
```

Execution is powered by `SubprocessRunner` by default, running portably without requiring unprivileged user namespaces or Bubblewrap. An optional `BubblewrapRunner` is available for environments requiring strict kernel namespace isolation.

---

## 3. Quickstart: Programmatic Usage (10 Lines of Python)

```python
from adapters.bank_adapter import BankAdapter
from adapters.bank_library import build_bank_registry, get_bank_seed_playbook
from e4 import DiagnosisEngine, GroundTruthOracle
from e4.controller.library import LibraryManager

# 1. Load telemetry adapter
adapter = BankAdapter("data/prepared/bank/public/cases/bank-20210304T1800-utc8")

# 2. Configure starter operator library and seed playbooks
lib = LibraryManager(registry=build_bank_registry())
lib.add_playbook(get_bank_seed_playbook())

# 3. Initialize engine with LLM client and acceptance oracle
engine = DiagnosisEngine(
    model=model_client,
    library=lib,
    oracle=GroundTruthOracle(ground_truth_entities=["cartservice"]),
)

# 4. Run diagnosis
result = engine.diagnose(adapter)
print(f"Status: {result.status}, Stage: {result.stage}, Top Cause: {result.top_candidates[:3]}")
```

---

## 4. Benchmark Evaluation

To evaluate on the 10 scored Bank cases using the frozen evaluation settings (temperature 1.0, 8192 max tokens, top-p 0.95, top-k 64 from pinned `config/gemma_m3_inference.json`):

```bash
.venv/bin/python -m benchmarks.bank_benchmark \
    --cases-dir data/prepared/bank/public/cases \
    --split scored \
    --seed 42 \
    --temperature 1.0 \
    --max-tokens 8192 \
    --top-k 64 \
    --model-url http://127.0.0.1:19250
```

---

## 5. Dataset Adapters & Zero-Code Telemetry Ingestion

For new telemetry datasets, implement the 4-method `TelemetrySource` protocol or use declarative JSON mappings with `FolderTelemetrySource`:

```bash
# Initialize boilerplate mapping
e4 setup --output-dir ./my_project

# Print AI prompt to generate custom adapter
e4 adapter-prompt --schema ./my_project/mapping.sample.json
```

---

## 6. Current Results Reference

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
