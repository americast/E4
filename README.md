# E4

## Install

Use Python 3.10 or newer:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Prepare a dataset and library

Export the adapter-development prompt using your task description, public source metadata and sample rows:

```bash
e4 setup ./my_project
e4 adapter-prompt samples.txt \
  --domain-file domain.md \
  --metadata-file metadata.json > adapter_prompt.md
```

The coding model returns `adapter_response.json` with the filled Python template and its configuration schema, or typed missing-input fields. Inspect the starter and validate the response without executing it:

```bash
e4 adapter-template > adapter_template.py
e4 adapter-validate adapter_response.json --config adapter_config.json
```

Load a ready response through `load_adapter_submission(response, config)` from `e4`. After implementing and validating the adapter, save `adapter.get_schema().as_dict()` as `schema.json`. Export the pre-run bootstrap prompt to extend the compatible shipped library:

```bash
e4 initialization-prompt domain.md --schema schema.json > bootstrap_prompt.md
```

To generate a domain library from an empty executable inventory instead:

```bash
e4 initialization-prompt domain.md --schema schema.json \
  --clean-slate > clean_slate_prompt.md
```

These commands export prompts; they do not call a model or generate a library. The prompts are awaiting review before generation or further experiments. Reference examples remain visible in clean-slate mode without being registered.

## Run the prepared Bank benchmark

Requires prepared Bank data and a running model endpoint:

```bash
.venv/bin/python -m benchmarks.bank_benchmark \
  --cases-dir data/prepared/bank/public/cases \
  --split scored --seed 42 \
  --temperature 1.0 --max-tokens 8192 --top-k 64 \
  --model-url http://127.0.0.1:19250
```

This benchmark explicitly selects its Bank library. It does not evaluate the new shipped paper defaults or pre-run bootstrapping.

## Run checks

```bash
.venv/bin/python -m unittest discover -s tests/test_e4 -v
```

## Recorded results

Accuracy is ground-truth inclusion in the top three on the same ten Bank incidents. Cost is reported input/output tokens, including repairs, as a percentage of the historical T2 seed42 cost (5,314,218 tokens). Cost percentages use seed42 only; T2 seed43 usage is incomplete.

| Variant | Accuracy | Token cost (% of T2; seed42) |
| --- | ---: | ---: |
| Historical kernel E4: retrieval | 30.0% | 4.2% |
| Historical kernel E4: + adaptation | 55.0% | 8.1% |
| Historical kernel E4: + evolution | 70.0% | 11.4% |
| Earlier Python operator-code E4 | 50.0% | 14.7% |
| Simplified E4 with explicit Bank starters | 70.0% | 2.2% |
| New paper defaults and bootstrap workflow | TBD | TBD |
| Historical T2 | 40.0% | 100.0% |

Kernel accuracies average seeds 42/43; the earlier Python row is seed42 only. The simplified Bank row is 70% in each of seeds 42/43/44, with all successes at retrieval and no additional adaptation/evolution successes. These are separate implementations and repeat the same incidents; they do not establish statistical significance or an isolated causal improvement. The new default library and prompts have no accuracy result yet.

Recorded evidence: [historical results](docs/BANK_CURRENT_RESULTS.md) and [corrected simplified campaign](docs/SIMPLIFICATION_PROPOSAL.md#177-completed-corrected-campaign).
