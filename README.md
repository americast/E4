# E4

E4 is a Python framework for diagnosing root causes from normalized telemetry. For a new dataset, begin with an adapter and a domain brief. Then prepare either the shipped compatible paper library or a clean-slate library, and use an explicit model client for diagnosis. Prompt export, validation, and library preparation are deterministic; model requests happen only when your code calls the selected client.

## Install

Requires Python 3.10 or newer.

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

## Choose a model client

E4 includes lightweight clients for OpenAI's Responses API and a local Ollama server. Both implement `complete(prompt: str) -> str`; no provider SDK is required by these clients. Choose a JSON-capable model that fits your budget and hardware.

### OpenAI API

Create an API key in the [OpenAI API dashboard](https://platform.openai.com/api-keys), then keep it in an environment variable. Replace the model placeholder with a Responses API model available to your account:

```bash
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL="your-model-id"
```

```python
import os
from e4 import OpenAIClient

model_client = OpenAIClient(model=os.environ["OPENAI_MODEL"])
```

The client reads `OPENAI_API_KEY` if `api_key` is omitted. It uses JSON mode by default; E4 still validates each response against its own contract. See the [OpenAI API quickstart](https://developers.openai.com/api/docs/quickstart), [Responses API reference](https://developers.openai.com/api/reference/resources/responses/methods/create), and [JSON mode/Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses).

### Local Ollama

On Linux, follow [Ollama's install instructions](https://docs.ollama.com/linux). On macOS or Windows, use the [Ollama download page](https://ollama.com/download). Start `ollama serve` only if the local service is not already running. Choose a model that fits the machine's memory; this example may be too large for some systems:

```bash
# Linux install (see official instructions for other platforms)
curl -fsSL https://ollama.com/install.sh | sh
```

If the Ollama service is not already running, start it in a separate terminal and leave it running there:

```bash
ollama serve
```

Then prepare a local model:

```bash
export OLLAMA_MODEL="gemma4:31b"
ollama pull "$OLLAMA_MODEL"
ollama list
ollama ps
curl -fsS http://localhost:11434/api/tags
```

```python
import os
from e4 import OllamaClient

model_client = OllamaClient(model=os.environ["OLLAMA_MODEL"])
```

The default host is `http://localhost:11434`; set `OLLAMA_HOST` to use another endpoint. Defaults are `context_tokens=32768` and `max_output_tokens=8192`. Override those constructor arguments to match available memory and the prompt/output size; `context_tokens` must exceed `max_output_tokens`. The local API requires no key; see [Ollama authentication](https://docs.ollama.com/api/authentication).

### Smoke-check

This calls the selected model; an OpenAI request may incur charges. The repository tests use mock clients.

```python
print(model_client.complete('Return exactly one JSON object: {"ok":true}'))
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

The packaged system prompts supply the generic E4 contracts; `domain.md`, `metadata.json`, and samples provide your domain-specific facts. Do not include scored labels, known answers, or incident-specific hints. For a filled brief example, see [bank_domain_brief.md](e4/prompts/resources/bank_domain_brief.md); generic prompts do not load it unless Bank is selected explicitly.

## 2. Develop and validate the adapter

Export the adapter prompt with your task, public metadata, and sanitized samples:

```bash
e4 adapter-prompt samples.txt \
  --domain-file domain.md \
  --metadata-file metadata.json > adapter_prompt.md

e4 adapter-template > adapter_template.py
```

Use the selected `model_client` to generate the adapter response. It must be strict JSON with either:

- `status: "ready"`, a complete Python module in `source`, and a `config_schema`; or
- `status: "needs_input"`, with typed `required_fields` and reasons.

For example, in the same Python session as `model_client`:

```python
from pathlib import Path

adapter_response = model_client.complete(Path("adapter_prompt.md").read_text(encoding="utf-8"))
Path("adapter_response.json").write_text(adapter_response, encoding="utf-8")
```

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

These commands export prompts; they do not call a model. Use the same `model_client` to generate the selected submission and pass its JSON response to the deterministic validator:

```python
from pathlib import Path

from e4 import apply_library_submission, prepare_library

mode = "paper"  # choose "clean_slate" for an empty executable starting point
base = prepare_library(schema, mode=mode)
prompt_path = "bootstrap_prompt.md" if mode == "paper" else "clean_slate_prompt.md"
submission = model_client.complete(Path(prompt_path).read_text(encoding="utf-8"))
Path("library_submission.json").write_text(submission, encoding="utf-8")
prepared = apply_library_submission(submission, base, schema)
prepared.save_to_dir("prepared_library")
```

In paper mode, a response can say `unchanged`, `blocked`, or `additions`; `unchanged` is valid only when the prepared paper library has usable playbooks for the schema. Clean-slate mode must return `additions` with at least one new operator and one playbook, or `blocked`; it cannot finish with `unchanged`. Additions are checked for source metadata, AST rules, strict Mypy types, schema requirements, valid connected DAGs, and a supported ranking return type before a new library is returned. The operation is atomic: on failure the original library remains unchanged. Reference examples in the prompt never register themselves.

`unchanged` returns a cloned prepared library. `blocked` stops preparation with a structured `LibrarySubmissionError`.

In Python, `prepare_library()` and `render_initialization_prompt()` make no model calls. The package does not run adapter generation or pre-run bootstrap automatically. Your code calls `model_client.complete()` for adapter, bootstrap, and runtime prompts; reuse the same client if appropriate.

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

## 5. Citing our work

If you use E4 in your research, please cite [Agentic RCA for Internet-Scale Services Using Constrained Creativity](https://arxiv.org/abs/2610.08622).

```bibtex
@misc{sinha2026e4,
  title         = {Agentic {RCA} for Internet-Scale Services Using Constrained Creativity},
  author        = {Sayan Sinha and Vipul Harsh and B. Aditya Prakash
                   and Vyas Sekar and Hui Zhang},
  year          = {2026},
  eprint        = {2610.08622},
  archivePrefix = {arXiv},
  primaryClass  = {cs.NI},
  doi           = {10.48550/arXiv.2610.08622},
  url           = {https://arxiv.org/abs/2610.08622}
}
```
