# Structured adapter coding submissions

The coding model fills a supplied Python template and returns one strict JSON object. Ordinary Python in the package parses, validates and loads it; no orchestrating LLM is required.

The authoritative resources are the [response schema](../e4/data/resources/adapter_response.schema.json), [Python scaffold](../e4/data/resources/adapter_template.py.txt) and [adapter prompt](../e4/prompts/resources/adapter_system.md).

## Response forms

A completed submission contains exactly these fields:

```json
{
  "status": "ready",
  "source": "<complete filled Python module>",
  "config_schema": {
    "type": "object",
    "properties": {"dataset_root": {"type": "string"}},
    "required": ["dataset_root"],
    "additionalProperties": false
  }
}
```

If facts needed to write the adapter are missing, the model returns structured input requests instead of partial code or a prose question:

```json
{
  "status": "needs_input",
  "required_fields": [
    {"name": "timestamp_unit", "type": "string", "reason": "Source unit is not declared"}
  ]
}
```

The class is always `DatasetAdapter(BaseDatasetAdapter)`, its constructor takes `config: Mapping[str, object]`, and its four method signatures are fixed by the scaffold. The factory is always:

```python
def create_adapter(config: Mapping[str, object]) -> BaseDatasetAdapter:
    return DatasetAdapter(config)
```

The model completes the method bodies and may add needed imports/helpers. Unfilled template markers, missing methods, changed signatures and changed factory wiring are rejected. Operator registration and playbook generation remain separate tasks.

## Package workflow

```python
import json
from pathlib import Path
from e4 import parse_adapter_submission, load_adapter_submission

response = Path("adapter_response.json").read_text()
submission = parse_adapter_submission(response)
if submission["status"] == "needs_input":
    print(submission["required_fields"])
else:
    config = json.loads(Path("adapter_config.json").read_text())
    adapter = load_adapter_submission(response, config)
    schema = adapter.get_schema()
```

`parse_adapter_submission` checks strict JSON, the envelope, configuration-schema definition and source interface without executing the submitted module. It rejects prose/Markdown wrappers, duplicate keys, nonfinite JSON numbers and unknown response fields.

`validate_adapter_config` validates runtime configuration separately. Configuration uses inline Draft 2020-12 object schemas; references are disabled so validation stays offline. Root properties, required fields and `additionalProperties: false` must be explicit.

`load_adapter_submission` validates configuration before executing the trusted integration module, invokes the fixed factory, checks the returned adapter type, and runs conformance by default. Conformance checks canonical record kinds, integer timestamps, source tables and bounds in both reference and alert windows, including valid empty iterators. This verifies the adapter interface, not diagnostic accuracy.

`AdapterSubmissionError` provides `stage`, `details` and `as_dict()`. A `needs_input` response raises `AdapterInputRequired` when passed to the loader; callers can inspect its `required_fields` without interpreting prose.

## CLI

```bash
e4 adapter-template > adapter_template.py
e4 adapter-prompt samples.txt --domain-file domain.md --metadata-file metadata.json

e4 adapter-validate adapter_response.json --config adapter_config.json
```

`adapter-validate` never executes submitted code. It emits machine-readable `ready`, `needs_input`, or error results; invalid submissions exit nonzero. Only an explicit loader call executes the trusted adapter source. The prompt also includes runtime-derived constructor signatures for the canonical return types.


## Validation checkpoint

147 maintained tests passed. An installed-wheel check from outside the repository verified ready-response parsing, the fixed factory, reference/alert conformance, typed missing-input handling, non-executing CLI validation, structured CLI failures and packaged schema/template access. These checks use manually authored fixtures; no live coding-model or incident experiment is included. Review artifacts are under `artifacts/structured_adapter_review_20261107/`.
