# E4

E4 is a Python framework for diagnosing root causes from normalized telemetry. For a new dataset, begin with an adapter and a domain brief. Then prepare either the shipped compatible paper library or a clean-slate library, and use an explicit model client for diagnosis. Prompt export, validation, and library preparation are deterministic; they do not call a model.

## Install

Requires Python 3.10 or newer.

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

## 1. Describe the task and data

Start from the fillable [domain brief template](e4/prompts/resources/domain_brief_template.md):

```bash
cp e4/prompts/resources/domain_brief_template.md domain.md
```

Fill in the diagnostic goal and accepted output, rankable entity identities, and the exact tables, columns, links, windows, timestamp origins/units, physical units, availability, and missing-value behavior. Include sanitized public or development examples where useful. Mark unknown facts as unknown; do not guess.

Keep information sources separate:

- `domain.md`: task meaning, candidate ontology, observability limits, and any human-supplied analytical priors with provenance.
- `metadata.json`: public schema or source facts that the adapter author may rely on.
- `samples.txt`: small sanitized example rows, if available.

The packaged system prompts supply the generic E4 contracts. Your `domain.md`, `metadata.json`, and samples supply the domain-specific facts appended to them.

Do not put scored labels, known answers, or incident-specific hints in these inputs.
For a filled domain-brief example, see [bank_domain_brief.md](e4/prompts/resources/bank_domain_brief.md); generic prompts do not load it unless Bank is selected explicitly.

## 2. Develop and validate the adapter

Export the adapter prompt with your task, public metadata, and sanitized samples:

```bash
e4 adapter-prompt samples.txt \
  --domain-file domain.md \
  --metadata-file metadata.json > adapter_prompt.md

e4 adapter-template > adapter_template.py
```

Give `adapter_prompt.md` and `adapter_template.py` to your chosen coding model outside E4. The response must be strict JSON with either:

- `status: "ready"`, a complete Python module in `source`, and a `config_schema`; or
- `status: "needs_input"`, with typed `required_fields` and reasons.

Save the complete model response as `adapter_response.json`. Fill `adapter_config.json` with runtime values matching its generated `config_schema`, then validate without executing the submitted source:

```bash
e4 adapter-validate adapter_response.json --config adapter_config.json
```

`adapter-validate` checks the JSON envelope, source syntax and fixed adapter interface, config schema, and config values. It does not execute Python. A `needs_input` result means the missing information must be supplied before loading.

Load a ready adapter explicitly in Python. This step executes trusted integration code, so review the module before calling it:

```python
import json
from pathlib import Path

from e4 import load_adapter_submission

response = Path("adapter_response.json").read_text(encoding="utf-8")
config = json.loads(Path("adapter_config.json").read_text(encoding="utf-8"))
adapter = load_adapter_submission(response, config)  # conformance check is on by default
schema = adapter.get_schema()
Path("schema.json").write_text(json.dumps(schema.as_dict(), indent=2), encoding="utf-8")
```

The four required adapter methods are `get_schema()`, `get_windows()`, `extract_suspect_entities()`, and `stream_records(table_name, window)`. `check_adapter_conformance()` verifies normalized records and identity/window behavior. Adapter source is trusted integration code; it does not run in the authored-operator sandbox.

## 3. Prepare a library

Export the default paper-bootstrap prompt:

```bash
e4 initialization-prompt domain.md --schema schema.json > bootstrap_prompt.md
```

Or choose clean-slate generation, which excludes shipped operators and books from the callable inventory:

```bash
e4 initialization-prompt domain.md --schema schema.json \
  --clean-slate > clean_slate_prompt.md
```

These are prompt exports only. Send the selected prompt to your model client and save its JSON response as `library_submission.json`. Continue with the `schema` obtained from the adapter above to validate and apply it:

```python
from pathlib import Path

from e4 import apply_library_submission, prepare_library

mode = "paper"  # choose "clean_slate" for an empty executable starting point
base = prepare_library(schema, mode=mode)
response = Path("library_submission.json").read_text(encoding="utf-8")
prepared = apply_library_submission(response, base, schema)
prepared.save_to_dir("prepared_library")
```

In paper mode, a response can say `unchanged`, `blocked`, or `additions`; `unchanged` is valid only when the prepared paper library has usable playbooks for the schema. Clean-slate mode must return `additions` with at least one new operator and one playbook, or `blocked`; it cannot finish with `unchanged`. Additions are checked for source metadata, AST rules, strict Mypy types, schema requirements, valid connected DAGs, and a supported ranking return type before a new library is returned. The operation is atomic: on failure the original library remains unchanged. Reference examples in the prompt never register themselves.

`unchanged` returns a cloned prepared library. `blocked` stops preparation with a structured `LibrarySubmissionError`.

In Python, `prepare_library()` and `render_initialization_prompt()` make no model calls. The package does not run adapter generation or pre-run bootstrap automatically. The application supplies the model client, if needed.

Paper mode uses a partial reconstruction of the reported starter families; see [PAPER_DEFAULT_LIBRARY.md](docs/PAPER_DEFAULT_LIBRARY.md) for implemented capabilities and explicit gaps.

## Where prompts and examples live

| Purpose | Source |
| --- | --- |
| Adapter instructions | [`e4/prompts/resources/adapter_system.md`](e4/prompts/resources/adapter_system.md), rendered by [`e4/data/agent_prompt.py`](e4/data/agent_prompt.py) |
| Adapter response schema and source scaffold | [`e4/data/resources/adapter_response.schema.json`](e4/data/resources/adapter_response.schema.json), [`e4/data/resources/adapter_template.py.txt`](e4/data/resources/adapter_template.py.txt), and [`e4/data/submissions.py`](e4/data/submissions.py) |
| Fillable task brief | [`e4/prompts/resources/domain_brief_template.md`](e4/prompts/resources/domain_brief_template.md) |
| Shared initialization instructions | [`e4/prompts/resources/initialization_system.md`](e4/prompts/resources/initialization_system.md) |
| Paper-default bootstrap | [`e4/prompts/resources/bootstrap_system.md`](e4/prompts/resources/bootstrap_system.md) |
| Clean-slate generation | [`e4/prompts/resources/clean_slate_system.md`](e4/prompts/resources/clean_slate_system.md) |
| Typed source contract and examples | [`e4/prompts/authoring.py`](e4/prompts/authoring.py), [`e4/prompts/examples.py`](e4/prompts/examples.py) |
| Runtime retrieval/composition/authoring | [`e4/prompts/templates.py`](e4/prompts/templates.py): `format_select_prompt`, `format_compose_prompt`, `format_evolve_prompt`, and `format_serve_prompt` |
| Structured library submission | [`e4/bootstrap.py`](e4/bootstrap.py): `parse_library_submission` and `apply_library_submission` |

The adapter prompt is first. Initialization then selects paper defaults or an empty clean-slate library. Runtime SELECT, COMPOSE, EVOLVE, and the separate SERVE composition are defined by the current controller and templates.

## 4. Run diagnosis

A prepared `LibraryManager` can be passed to `DiagnosisEngine`. The model object must provide `complete(prompt: str) -> str` for the runtime diagnosis stages.

```python
from e4 import BudgetStopPolicy, DiagnosisEngine

engine = DiagnosisEngine(
    model=model_client,       # supplied by your application
    library=prepared,
    oracle=BudgetStopPolicy(max_attempts=3),
)
result = engine.diagnose(adapter, case_id="case-001")
print(result.status, result.stage, result.answer)
```

`BudgetStopPolicy` limits attempts and returns supported candidates as unverified when it stops; it does not certify correctness. `GroundTruthOracle` is for host-side evaluation only. Do not include its labels in prompts.

## Focused checks

```bash
.venv/bin/python -m unittest \
  tests.test_e4.test_adapter_submissions \
  tests.test_e4.test_library_submissions \
  tests.test_e4.test_paper_defaults
```

For a code-reading route through the adapter, library, controller, and replay boundaries, see [CODE_REVIEW_GUIDE.md](docs/CODE_REVIEW_GUIDE.md).
