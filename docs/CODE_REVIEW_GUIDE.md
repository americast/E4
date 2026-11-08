# Code review guide

Review baseline: **`a6b823d` — adapter-first onboarding with scoped experiment replay.** This guide follows current code symbols and boundaries; older Bank starter runs are not the package default.

## Fast route (about 30 minutes)

1. [Adapter source prompt](../e4/data/agent_prompt.py) and [submission loader](../e4/data/submissions.py): see what the adapter author is asked to return, how strict JSON/config validation works, and where trusted source is explicitly executed.
2. [Onboarding selection](../e4/onboarding.py): trace `prepare_library`, `paper_availability`, and `normalize_dataset_schema`. These are deterministic library helpers; they do not call a model or perform pre-run bootstrap.
3. [Paper operators](../e4/starters/paper.py) and [fidelity notes](PAPER_DEFAULT_LIBRARY.md): distinguish the partial shipped library from the full reconstructed catalog and the older Bank-specific library.
4. [Library submission](../e4/bootstrap.py): follow `parse_library_submission` and `apply_library_submission` for response parsing, validation, atomic cloning, and mode preservation.
5. [Runtime controller](../e4/controller/controller.py): read `DiagnosisController.run_diagnostic`, `_to_ranked_answer`, and the SELECT → COMPOSE → EVOLVE/SERVE transitions.
6. [Current Bank orchestration](../scripts/run_bank_onboarding.py): inspect `_prepare_mode`, `_run_mode_seed`, and `run_campaign` only after understanding the package path.

The current public API is designed to keep each step reviewable:

```text
adapter prompt → strict submission parse → config validation → explicit trusted load/conformance
                ↓
          choose paper defaults OR empty clean slate
                ↓
       optional model-generated library submission
                ↓
             per-incident SELECT → COMPOSE → EVOLVE → separate SERVE
```

The package does **not** automatically run adapter generation or library bootstrap. `DiagnosisEngine` lazily loads a schema-compatible paper library when diagnosis begins. An application or the Bank benchmark runner must explicitly invoke model prompts and `apply_library_submission` for generation/bootstrap.

## Adapter boundary: integration code is trusted

Read [adapter.py](../e4/data/adapter.py) for `BaseDatasetAdapter`, `TelemetrySource`, and `check_adapter_conformance`. The interface is `get_schema()`, `get_windows()`, `extract_suspect_entities()`, and `stream_records(table_name, window)`.

Read [adapter_system.md](../e4/prompts/resources/adapter_system.md) alongside [submissions.py](../e4/data/submissions.py). `adapter_submission_schema()` describes exactly two response shapes: `ready` with a complete source module and inline config schema, or `needs_input` with named typed fields. `parse_adapter_submission()` rejects prose/fences, duplicate JSON keys, extra envelope fields, and incomplete source scaffolds. `validate_adapter_config()` applies Draft 2020-12 JSON Schema without remote references. `load_adapter_submission()` validates before running the module and then checks adapter conformance.

`load_adapter_submission()` executes ordinary trusted Python. It is not the authored-operator sandbox. Review the fixed `DatasetAdapter` methods and `create_adapter(config)` factory in `adapter_template.py.txt`; the scaffold's `NotImplementedError` markers must be replaced. The static source check verifies this interface but does not prove arbitrary adapter code safe.

The Bank-specific integration gate is separate: [bank_onboarding.py](../benchmarks/bank_onboarding.py), especially `validate_bank_adapter_submission()`, compares the adapter to the canonical Bank adapter on public development cases and timestamp-unit probes. The production runner does not send scored labels to adapter or library prompts. Label loading and `GroundTruthOracle` evaluation remain host-side in [bank_benchmark.py](../benchmarks/bank_benchmark.py); `_load_plan()` also checks configured IDs on the host.

## Initial library: paper default and clean slate

`prepare_library(schema, mode="paper")` filters the shipped package library to the schema. `prepare_library(schema, mode="clean_slate")` returns an empty callable library. Both paths are deterministic. The [paper catalog](../e4/starters/resources/paper_catalog.json) records 19 reconstructed operator families and 11 books, but only six operator families are implemented. The schema-compatible default books are the three backend metric books; the dependency book is conditional on runtime trace links. See [PAPER_DEFAULT_LIBRARY.md](PAPER_DEFAULT_LIBRARY.md) for equations, assumptions, and unimplemented gaps. Do not call this full paper fidelity.

The historical [Bank library](../adapters/bank_library.py) is a separate six-operator/two-book implementation. It is not the current paper default. Do not use [run_bank_three_seed.py](../scripts/run_bank_three_seed.py) as the current onboarding entry point; it is the older explicit-Bank runner.

`paper_reference_examples()` returns reference-only operator contracts and playbook DAGs. In clean-slate mode those examples remain informational; they are not registered capabilities. Compare [initialization.py](../e4/prompts/initialization.py), [bootstrap_system.md](../e4/prompts/resources/bootstrap_system.md), and [clean_slate_system.md](../e4/prompts/resources/clean_slate_system.md). The prompt exporter `render_initialization_prompt()` is offline and does not generate or apply a library.

The deterministic submission boundary is [bootstrap.py](../e4/bootstrap.py): `parse_library_submission()` accepts only `additions`, `unchanged`, or `blocked` JSON; `apply_library_submission()` clones the input library, validates new source with `StaticRepairController.validate_and_typecheck()`, verifies each DAG, rejects disconnected nodes/operators and non-ranking return types, then returns the prepared clone. It does not call a model. Clean slate must receive at least one new operator and one playbook; paper mode can add a playbook that composes existing operators.

## Runtime diagnosis and validation boundaries

The controller's single `run_diagnostic()` method performs SELECT, COMPOSE, then EVOLVE and separate SERVE after earlier attempts do not confirm. SELECT binds a stored book. COMPOSE builds a DAG from existing operators. EVOLVE proposes operator source; it enters a temporary registry only after source metadata, AST, and Mypy checks. SERVE separately composes a DAG against that registry. Only a confirmed answer admits reachable new operators and a playbook.

The supported authored entry point is:

```python
def operator(inputs: Inputs, params: Params, runtime: Runtime) -> Output:
    ...
```

This is the annotated form in `METRIC_ANOMALY_EXAMPLE` in [examples.py](../e4/prompts/examples.py), not a separate runtime interface. [authoring.py](../e4/prompts/authoring.py) renders the shared contract; [typechecker.py](../e4/sandbox/typechecker.py) generates the Mypy stub. Variadic input ports are read-only `Sequence[DeclaredViewType]`, and nullable parameters/record values need narrowing. [ast_validator.py](../e4/sandbox/ast_validator.py) enforces imports and names; [repair.py](../e4/controller/repair.py) runs the bounded structural/AST/Mypy check.

The latest typing fixes align `Window`, `EntityRef`, `AggregateRow`, and `ScoredCandidate` annotation aliases across the AST validator, Mypy profile and worker. They remain annotation-only names in authored source; construct results with the `make_*` helpers. `_type_for()` now maps object/array parameters to `JSONMapping` / `Sequence[JSONValue]`, so authored code must narrow JSON values instead of receiving `Any`.

Read [views.py](../e4/sandbox/views.py) for `metric_points`, `span_records`, `log_records`, and `aggregate_records`. The latter takes a `field: str`; Mypy cannot prove a dynamically selected field exists on a canonical record. For example, metric `MetricRecord` exposes `value`, not raw source headers such as `rr`, `sr`, `cnt`, or `mrt`. `aggregate_records` rejects an unsupported field at runtime. A successful AST/Mypy pass therefore does not guarantee a valid query or serving DAG.

In [dag.py](../e4/dag.py), `verify_dag()` checks graph/port types and bindings before `SerialExecutor` runs the graph. A terminal answer must be `ScoredCandidates` or `RankedAnswer`; `_to_ranked_answer()` in the controller also requires an observed identity and positive, eligible supporting evidence. Inspect `CoverageView` propagation before treating incomplete data as evidence of absence. A concrete review case is an `Aggregate` output wired into a `ScoredCandidates` input: the connection is invalid, even when both operator sources type-check individually. `RankedAnswer` is also a distinct port type. These serving-composition failures remain separate from the replay fix.

The two execution paths are different: trusted built-in `OperatorSpec.implementation` functions execute directly in `SerialExecutor`; authored `OperatorSpec.source` runs through [runner.py](../e4/sandbox/runner.py) and [worker.py](../e4/sandbox/worker.py), with AST restrictions, resource limits, serialization, and subprocess execution. Those restrictions are not an OS security boundary for the separate adapter loader.

## Current Bank experiment runner and replay

The current end-to-end model orchestration is [run_bank_onboarding.py](../scripts/run_bank_onboarding.py), not package initialization. It imports the external adapter and its public development gate, freezes the shared adapter, prepares paper/clean-slate libraries, and calls `run_benchmark()` on per-seed shards. [bank_benchmark.py](../benchmarks/bank_benchmark.py) provides the benchmark and host-side oracle. The orchestration is experimental Bank code outside the pip package API.

Review replay/accounting in these symbols:

- `_JournalClient.complete()` marks each fresh physical call consumed immediately (`self.used.add(self.next_index)`). Pending responses may replay once after interruption; `_context_matches()` restricts that reuse to the same endpoint request context (mode, seed, shard, stage, case). Review `test_fresh_repeated_prompt_is_consumed_and_causes_two_physical_calls` and `test_pending_response_replay_requires_exact_seed_shard_case_and_is_once` in the runner tests.
- `LocalLlamaClient._record_call()` in `bank_benchmark.py` appends and `fsync`s each call journal record.
- `_mark_calls()` uses first-owner semantics so replay cannot reassign a physical call's cost; `_recover_accounted_calls()` reconstructs accounted indices after restart.
- `_prepare_mode()` writes the frozen library; `_run_mode_seed()` restores the latest snapshot, writes a case result, saves an immutable library snapshot, then advances its checkpoint. `run_campaign(..., resume=True)` checks the frozen profile and recovers journal accounting.

Current recovery status and integrity limits are in the optional local [integrity review](../artifacts/onboarding_e2e_20261107/INTEGRITY_REVIEW.md) and [valid-seed failure audit](../artifacts/onboarding_e2e_20261107/FAILURE_AUDIT_VALID_SEEDS.md). Seed 43 is being recovered after an exact-prompt replay defect. Do not cite the invalid revision-2 three-seed aggregate; check the live campaign artifacts before reporting results. Seed 42/44 summaries repeat the same ten incidents and are not a held-out sample.

## Review checks and tests

For a focused code route, inspect [adapter submission tests](../tests/test_e4/test_adapter_submissions.py), [library submission tests](../tests/test_e4/test_library_submissions.py), [onboarding prompt tests](../tests/test_e4/test_onboarding_prompts.py), [bank profile tests](../tests/test_e4/test_bank_onboarding_profile.py), and [runner replay tests](../tests/test_e4/test_bank_onboarding_runner.py). Paper runtime contracts and actual default-book execution are covered by [paper defaults tests](../tests/test_e4/test_paper_defaults.py); the source/runtime type profile is covered by [authoring reference tests](../tests/test_e4/test_authoring_reference.py).

Useful targeted command:

```bash
.venv/bin/python -m unittest \
  tests.test_e4.test_adapter_submissions \
  tests.test_e4.test_library_submissions \
  tests.test_e4.test_onboarding_prompts \
  tests.test_e4.test_bank_onboarding_profile \
  tests.test_e4.test_bank_onboarding_runner -v
```

At the `a6b823d` review checkpoint, 38 targeted frozen-runtime tests passed. An earlier complete baseline was 161 tests. They describe different snapshots; do not add them or repeat the old 75-test claim as a current full-suite result. Review outcomes in the integrity artifacts separately from source/test validation.

## Two reading routes

**Fast core review:** adapter submission/resource → `prepare_library()` → `apply_library_submission()` → paper/clean-slate prompt → `DiagnosisEngine.diagnose()` → controller answer guard. Ask at each boundary: who owns the model call, what information crosses it, which validators run, and what state can change on failure?

**Experiment review:** add the Bank public gate, `_prepare_mode()`, `_run_mode_seed()`, journal replay/ownership, checkpoint and snapshot writes, then `GroundTruthOracle` and campaign aggregation. Confirm adapter/library prompts use public development facts only; verify the evaluator is host-side; distinguish generation costs from diagnosis costs; and inspect whether each purported addition reaches the return node before attributing any outcome to evolution.
