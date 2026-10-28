# M3 results — M3 complete; development-only assessment recorded

**Status as of 2026-10-15:** Gemma and Devstral completed the same fixed 20-task synthetic suite and checker. Their raw stored scores were **19/20** and **12/20**. Symmetric offline replay on the corrected suite gives **20/20** for Gemma and **13/20** for Devstral. Gemma is selected as the experimental local model. Its first development DAG, final-source cache-parity replay, and separate trusted development-only assessment are complete. M3 acceptance work is complete. This one-case result is not a scored-case accuracy trend or the paper's three-attempt metric.

No scored Telecom incidents, M4 bootstrap/admission, or three-attempt serving controller have run. The synthetic diagnoses below are not scored-case predictions.

## Frozen comparison method

The captures used suite `m3-synthetic-v2` (`e4-m3-synthetic-suite/2`, SHA-256 `1e0b7d91e6a8d8e7f7dca673b8361f8fe776d91a178a4b3f2651a0f2ba291cac`) and checker SHA-256 `aa88d8e49be2bdf7f7f9bdbee5feccf4c393e69271d24245494b57112481c023`. The authoritative offline replay used suite `m3-synthetic-v3` (`e4-m3-synthetic-suite/3`, SHA-256 `c38eb6bdfb2d3bce3b18c3faf3d86bcb69831830d04951fdbd95e2c919567183`) and checker SHA-256 `c6eaf3c7ae00a139a7a1457a52d882a79b77cc712c300c0eb2f59f66dadb7cef`. Apart from suite version metadata, v3 changes only the audited expected delta from 1.0 to 2.0. The suite contains eight executable DAG compositions, six generated-operator tasks, four synthetic diagnoses, and two schema tasks. Prompts, API contracts, task order, seed examples, and request budgets were frozen before either full v3 capture.

The launcher ran each model serially on the V100 in a network-isolated namespace with 32,768 context tokens, an 8,192-token generation cap, seed 42, one request at a time, and a 3,072 MiB free-VRAM reserve. Gemma used its declared thinking mode and 2,048-token reasoning budget; Devstral used its native nonthinking profile. Each task allowed at most two static syntax/schema repairs. Semantic outputs were never repaired from checker feedback. Gemma needed zero repairs.

The operator output checker follows the published schemas: finite numeric fields accept finite integer or float values within tolerance; integer and Boolean fields require their exact types (so a Boolean does not count as an integer); arrays declared as integer arrays require integer elements, while finite-number arrays accept either numeric type. Diagnosis category format is checked against the public enum, while category correctness is secondary and `null` is allowed.

Luna authored the M3 implementation, test, and harness changes. The primary agent independently reviewed the frozen comparison inputs, ran the local models and release checks, and reviewed the replay evidence.

The separate full-library prompt calibrations used the same 18,228-character prompt (SHA-256 `c5b8596c6f362950cdc2bf80191dd0e5184f3ebca6d37eabadea77e2d4251887`). Gemma rendered 4,427 prompt tokens and generated 2,030 tokens; Devstral rendered 4,491 prompt tokens and generated 855 tokens. Both fit the common context and generation budgets. Calibration is a context/hardware preflight, not task correctness evidence, and its outputs do not contribute to model selection.

## Gemma full capture — raw suite v2

The complete record is under `artifacts/m3/model_runs/gemma_m3_suite_v3/`. The model is Google Gemma 4 31B Instruct QAT Q4_0, repository revision `59dde24573e7e61570dba08b18a2e1fe246955ed`, SHA-256 `179cfb99212709597eae5929112cfca677e1bbf566178b479ae1da0c4772874b`.

| Measure | Captured result |
| --- | ---: |
| Tasks completed | 20/20 |
| Initial checker passes | 19/20 |
| Passes after repairs | 19/20 |
| Repairs | 0 |
| DAG compositions | 8/8 |
| Generated operators | 5/6 |
| Synthetic diagnosis component in top three | 4/4 |
| Synthetic diagnosis component at rank one | 2/4 |
| Synthetic diagnosis category match | 3/4 |
| Prompt tokens | 65,609 |
| Completion tokens | 33,568 |
| Model-run elapsed time | 1,205.034 seconds |
| Peak GPU memory used | 21,403 MiB |
| Minimum free GPU memory | 11,090 MiB |
| 3,072 MiB reserve | Passed |

The sole raw-v2 failure was `operator_04_median_window_contrast`. Its second check stored an incorrect expected delta. For reference `[10, 10, 12]`, the median is 10; for alert `[11, 12, 13]`, the median is 12. The correct delta is 2.0. Since the threshold is 2 and the contract is strict `delta > threshold`, `exceeds` is correctly false. Gemma's saved implementation returned `{ "delta": 2, "exceeds": false }`; only the stored expected value `1.0` caused the raw mismatch. NumPy and independent arithmetic both confirmed the medians and delta.

The diagnosis numbers are separate measures: top-three component matching is the primary synthetic diagnosis check; top-one and category are secondary. A category may be null when evidence does not support one. These four synthetic examples do not establish expected performance on Telecom incidents.

## Devstral full capture

The complete record is under `artifacts/m3/model_runs/devstral_m3_suite_v3/`. The model is Mistral Devstral Small 2 24B Instruct Q5_K_M, repository revision `027695770ae1de77c2f6fb19f8e1ba9d65fcd15d`, SHA-256 `e1a2d4b849f12a0efe6a92bb93d33e27ef5fddf859da7bfdbc7cc5bc58cf42ff`.

| Measure | Captured result |
| --- | ---: |
| Tasks completed | 20/20 |
| Initial checker passes | 12/20 |
| Passes after repairs | 12/20 |
| Repairs | 2 |
| DAG compositions | 2/8 |
| Generated operators | 4/6 |
| Synthetic diagnosis component in top three | 4/4 |
| Synthetic diagnosis component at rank one | 2/4 |
| Synthetic diagnosis category match | 3/4 |
| Prompt tokens | 75,194 |
| Completion tokens | 9,410 |
| Model-run elapsed time | 339.949 seconds |
| Peak GPU memory used | 21,651 MiB |
| Minimum free GPU memory | 10,842 MiB |
| 3,072 MiB reserve | Passed |

Devstral's raw 12/20 includes the same false failure on `operator_04_median_window_contrast` caused by the bad delta expectation described above. One other operator failure, `operator_05_fixed_time_blocks`, is a model error: the saved code calls `statistics.median` without importing `statistics`. Six DAG tasks did not pass: five returned a valid graph with a composition different from the requested task; `dag_06_flow_and_dependency` used both allowed static repairs but still returned a graph with unused scorer nodes. No task passed only after repair. Those six DAG failures and the operator 5 failure remain in the corrected score; only the confirmed operator 4 oracle mismatch is corrected. Diagnosis measures match Gemma's raw totals: all four components were in the top three, two were rank one, and three categories matched.

## Symmetric offline correction and selection

The oracle audit checked all 19 generated-operator cases and found that one v2 expected value was inconsistent with the public median definition. The v3 check corrects only that delta from 1.0 to 2.0; all other prompts, cases, telemetry observations, and expected outputs are unchanged. Both complete v2 captures were replayed offline against v3 using the saved model calls. The final replay verified every rendered prompt and request payload (Gemma 20/20 calls; Devstral 22/22, including its two static repair calls), with zero network requests. For each capture, all 14 source artifact hashes before and after replay match. The saved request payloads and call content remain byte-equivalent to the original captures. Original run artifacts were not overwritten.

| Model | Raw suite v2 | Corrected suite v3 | Original elapsed | Original input/output tokens | Original GPU peak/min-free | Offline replay wall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Gemma 31B Q4_0 | 19/20 | **20/20** | 1,205.034 s | 65,609 / 33,568 | 21,403 / 11,090 MiB | 12.556 s |
| Devstral 24B Q5_K_M | 12/20 | **13/20** | 339.949 s | 75,194 / 9,410 | 21,651 / 10,842 MiB | 9.311 s |

The final replay summaries retain the original launch costs as `selection_costs`; offline wall time is separate and excluded from model cost comparison. Gemma was selected for the first development integration because it has the higher corrected score, 20/20 versus 13/20, under identical task prompts/checks and token budgets. That configuration was used for the completed development integration and replay described below. No new HTTP calls, model generations, or GPU runs were needed for the oracle correction.

An earlier offline replay pair used checker SHA-256 `16d54254fd32c54d5550b226a2eb78f3795b784551fd56cd41449d92830f2cb8` and measured 9.732/9.483 seconds. Those directories are retained as historical intermediate records. The final replay directories below use checker `c6eaf3...f2cb8` and are authoritative for corrected counts and replay timing. Reproduce the authoritative replays with:

```sh
.venv/bin/python scripts/synthetic/replay.py \
  --capture artifacts/m3/model_runs/gemma_m3_suite_v3 \
  --suite config/synthetic/suite_v3.json \
  --output artifacts/m3/offline_replays/gemma_m3_suite_v3_final
.venv/bin/python scripts/synthetic/replay.py \
  --capture artifacts/m3/model_runs/devstral_m3_suite_v3 \
  --suite config/synthetic/suite_v3.json \
  --output artifacts/m3/offline_replays/devstral_m3_suite_v3_final
```

Before replay, the frozen capture inputs/source were archived at `artifacts/m3/capture_source_v2.tar.gz`, SHA-256 `dc101d028c0d27b7ece5d669cb4fb9477cf85e6e5ec969c3080d637dbd65184b`.

## Capture history and exclusions

Two earlier Gemma runs are retained but excluded from comparison. `gemma_m3_suite_v1` was interrupted after the composition tasks and early operator work exposed a harness problem: runtime output/type failures were incorrectly eligible for compilation repair, creating a risk that checker feedback could affect subsequent model calls. Its partial pass count is not a comparison result. `gemma_m3_suite_v2` was also interrupted after the eight DAG tasks; operator 1 revealed that the prompt had not specified the row object fields (`entity`, `metric`, `value`). The model's plausible alternate row interpretation failed the checker. That prompt contract was corrected before v3. These runs remain available for audit, and neither is used for model selection.

The measured capture and replay records are preserved at:

- `artifacts/m3/model_runs/gemma_m3_suite_v3/m3_benchmark_summary.json`
- `artifacts/m3/model_runs/gemma_m3_suite_v3/m3_task_results.jsonl`
- `artifacts/m3/model_runs/gemma_m3_suite_v3/m3_model_calls.jsonl`
- `artifacts/m3/model_runs/gemma_m3_suite_v3/m3_benchmark_manifest.json`
- `artifacts/m3/model_runs/gemma_m3_suite_v3/launch.json`
- `artifacts/m3/model_runs/gemma_m3_suite_v3/gpu-memory.json`
- `artifacts/m3/model_runs/devstral_m3_suite_v3/m3_benchmark_summary.json`
- `artifacts/m3/model_runs/devstral_m3_suite_v3/m3_task_results.jsonl`
- `artifacts/m3/model_runs/devstral_m3_suite_v3/m3_model_calls.jsonl`
- `artifacts/m3/model_runs/devstral_m3_suite_v3/m3_benchmark_manifest.json`
- `artifacts/m3/model_runs/devstral_m3_suite_v3/launch.json`
- `artifacts/m3/model_runs/devstral_m3_suite_v3/gpu-memory.json`
- `artifacts/m3/offline_replays/gemma_m3_suite_v3_final/m3_replay_summary.json`
- `artifacts/m3/offline_replays/gemma_m3_suite_v3_final/m3_replay_manifest.json`
- `artifacts/m3/offline_replays/devstral_m3_suite_v3_final/m3_replay_summary.json`
- `artifacts/m3/offline_replays/devstral_m3_suite_v3_final/m3_replay_manifest.json`

## Development integration and verification

The primary's earlier full implementation checkpoint passed **97/97 tests** (`artifacts/m3/final_contract_tests.log`). The post-replay release suite passed **102/102 tests** (`artifacts/m3/release_tests.log`); after canonical ordering and its regression test, the latest release suite passed **104/104 tests** (`artifacts/m3/final_determinism_tests.log`).

The first actual development integration ran Gemma on public case `telecom-20200523T0430-utc8`, saved under `artifacts/m3/model_runs/gemma_m3_first_development_v1/development/`. The input was hash-verified: data SHA-256 `32a41a1af1cc663e68ebb19322be0fe6e4831e4c6081d4445c61d15de450149d`, 200,394,219 bytes. The selected label-blind KPI was root OSB `elapsedTime` mean for service `osb_001` (13,851 reference and 14,429 alert traces). The model produced a valid 12-node DAG after two static repairs for parameter type/binding errors. It ranked five component leads: `os_021`, `docker_006`, `os_018`, `docker_003`, `docker_004`. The saved artifacts are `development/dev_manifest.json`, `development/model_calls.jsonl`, `development/model_dag.json`, `development/development_run.json`, and `development/phase_status.json` under that run directory. Gold labels were not available to DAG generation, ranking, or replay; one gold row was read only by a separate trusted evaluator after source and predictions were frozen.

Within that run, cache-cold, cache-warm, and cache-disabled answers deserialized as exactly equal. The executor recorded 0/11/0 cache hits and execution times of 582.069/13.461/566.522 seconds. Loading took 30.377 seconds and end-to-end model/load wall time was 1,405.378 seconds. The run retained three model calls and its status record says the run passed. Its source snapshot is `artifacts/m3/development_source_v3.tar.gz`, SHA-256 `7b87bc0cff1ba49c92e8b622fd8f8a7c01e88907b2f24908a39caad139f5d2f2`.

The initial within-process equality did not establish cross-process determinism. Final review found the dependency scorer's permutation input order came from an unordered trace-ID set. With the same numerical seed, scores varied across `PYTHONHASHSEED` values. Luna added canonical trace-ID ordering and a regression check: the same synthetic fixture now produces `323957751.6576788` identically under ten hash seeds; the previous outputs ranged from `288739499.8465` to `370774392.8644`. The latest release suite after this change passed **104/104 tests** (`artifacts/m3/final_determinism_tests.log`). A separate-process replay of the saved development DAG then passed under `PYTHONHASHSEED=1`, with OMP/OpenBLAS/MKL thread counts fixed to one and `--verify-cache-parity` enabled. All three full serialized `RankedAnswer` objects were exactly equal. The replay used 0/11/0 cache hits and took 614.926/12.905/584.223 seconds for cold/warm/disabled execution; loading took 30.889 seconds. The original saved DAG SHA-256 is `0a82b361e923299d16e9b22ddf4cbfff8ae97d2c482769a8598e3cd85d7c023a`; the replay confirmed the case data SHA and 200,394,219-byte input were unchanged. The run's implementation fingerprint is `51c31be79d38b89e520797e416da357b411f99d34f82abb9330ffeef3ebaa668`. These results establish deterministic replay, not root-cause correctness; the single-case trusted evaluation is reported below.

The final development replay record is `artifacts/m3/dev/gemma_firstdev_fixedsource_replay/replay/development_replay.json`, SHA-256 `22a8f4977b3c68463e523f9569760e2faeb16384c17dee14d4b1e463bd4959eb`. The initial, pre-fix code snapshot is retained as `artifacts/m3/development_source_v3.tar.gz` (SHA-256 `7b87bc0cff1ba49c92e8b622fd8f8a7c01e88907b2f24908a39caad139f5d2f2`); the final implementation snapshot is `artifacts/m3/final_source_v3.tar.gz` (SHA-256 `38b3e29d98d9df0a6cc1c45c9a6a0fb5834049d08a0dc6cf0dad71e80e265eae`).

The trusted development-only evaluation record is `artifacts/m3/trusted_dev_evaluation.json`, SHA-256 `f56ff9864367a12e930905b9249857288ae9402ede853b60a982d1f764373436`. It reads only the single development gold row for this case, after all model inputs, source, and saved predictions were frozen. Gold is component `docker_006`, level `pod`, category `CPU fault`, timestamp 2020-05-23 04:47 UTC+8. The original capture and final replay answers are identical. `os_021` ranks first and `docker_006` ranks second: component top one misses, component top three hits. All predicted categories are null, so category top one/top three and component-plus-category top one/top three miss. This is one development case, not an aggregate accuracy estimate. No scored gold was read. No source/config/test changes, prompt edits, answer modifications, or model calls followed this evaluation.

## Scope and fidelity limits

The original E4 implementation and its eleven seed playbooks were unavailable. M3 uses six reconstructed generic seed playbooks and backend Telecom operators; it does not claim code-level or full-library fidelity to the paper. MoCE-inspired operations are backend adaptations, not reproductions of its client/session experts. Absolute units for trace `elapsedTime` and app `avg_time` remain unresolved, so comparisons stay within fields and do not convert them across telemetry types. The flow operator abstains unless eligible matched traces have exactly one logical root OSB service, and other support/fit gates can also make it unsupported. Category classification is not implemented in the core answer path; unknown category remains null unless an evidence-backed classifier is added.

M3 scope includes synthetic validation, isolated operator execution, the local-model comparison, one development integration with deterministic cross-process replay, and a separate trusted development-only assessment of its saved output. M3 is complete. It does not include the M4 three-attempt controller, schema bootstrap/admission, evaluator feedback loop, ten scored Telecom runs, baselines, or the final Accuracy@3,3 report. No synthetic or single-development-case result should be presented as scored Telecom accuracy.
