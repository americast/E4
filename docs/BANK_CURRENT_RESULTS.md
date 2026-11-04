# Verified Bank results

**Scope correction, 3 November:** the historical results below measure the kernel-constrained implementation in commit `7de48fe`. Its evolved operator bodies were composed from a fixed numerical-kernel catalog. The paper describes newly authored operator implementations behind typed data-access interfaces. The new typed operator-code path has now been evaluated separately; see the November 4 update at the end. The historical numbers remain valid for the constrained variant and do not validate full paper-style operator synthesis.

The `paired_r6_final` evaluation completed on 1 November 2026 at approximately 21:15 UTC. All four five-case chains exited successfully. The host combiner verified both ten-case results, and the independent watcher verified their recorded hashes. There were no infrastructure-aborted chains in this run.

| Seed | Retrieval | Through adaptation | Through evolution | Historical T2 |
|---|---:|---:|---:|---:|
| 42 | 4/10 | 7/10 | 8/10 | 4/10 |
| 43 | 2/10 | 4/10 | 6/10 | 4/10 |
| Mean accuracy | 30% | 55% | 70% | 40% |

Success means the ground-truth component appears within the first three ranked candidates in any of the three diagnosis stages. Adaptation rescued five runs after retrieval missed; evolution rescued another three. Evolution authors typed operators separately from the serving call that composes the final pipeline.

These are two decoding seeds on the same ten frozen OpenRCA Bank incidents, not 20 independent incidents or a new holdout. Statistical significance has not been established. The T2 numbers are the existing same-seed baseline, not a new baseline run.

## Recorded token cost

Costs sum API-reported input and output tokens, including static repairs, evolution authoring and separate serving calls. The stage-prefix sums below reconcile exactly with each final combined result and all 75 recorded model calls.

| Seed | Retrieval prefix | Adaptation prefix | Evolution prefix | Model calls |
|---|---:|---:|---:|---:|
| 42 | 223,831 | 427,813 | 608,313 | 30 |
| 43 | 268,313 | 483,545 | 936,314 | 45 |

The verified T2 seed42 denominator is 5,314,218 input/output tokens. Thus E4 seed42 costs 4.2%, 8.1% and 11.4% of T2 across the three prefixes. T2 seed43 usage is incomplete, so no seed43 ratio or two-seed mean ratio is reported. Token ratios are not dollar prices or wall-clock comparisons.

The historical baseline report is `artifacts/seed_variability/reports/pilot_20261025_s43_s44_v2/seed_variability_report.json`, SHA256 `202f2f3f59859f7da8a4635f36298884e3da6cbb4c18f5ecc5d2884879cd04a8`. It records T2 accuracy 4/10 for each seed and complete seed42 token usage.

## Method and validation

The evaluated implementation fingerprint on DGX Python 3.10.12 is `e0c9d77cb02d3f719748c5bdb210c7dcbd27612cd50cdd4cb0ab9451997415e3`. The model is the pinned Gemma checkpoint, with the same decoding profiles, dataset, evaluator and starter semantics as the preceding comparison. The source-bound starter snapshot is `438657f9289e2f2fbd3b44472085a4a8cff3d6b97bdbe937cc0d61b20395f6e5`.

The main correction gives component metrics and application KPIs their own grouping contracts (`component` and `app_kpi`). Previously the host injected a flat requirement across both tables, preventing valid generated operators from being served. See [the contract diagnosis](TABLE_GROUPING_CONTRACT_DIAGNOSIS_20261031.md).

Before scoring, validation passed 577 tests with zero skips, all eight retained-context tokenizer checks with the unchanged 8192-token generation reserve, and a mixed-table V2 restricted-worker-to-consensus smoke. The latter used synthetic inputs and is not part of the accuracy counts. Thirteen additional operations tests covered the actual orchestration entrypoint, seed binding, inference outage handling and preservation of completed failures.

Inference used the coordinator P100/V100 pair and Wingtip RTX8000, with independent fresh five-case library chains per seed. All data, operator and evaluator workers remained isolated on DGX CPU. A failed earlier trial was preserved after the DGX GPU became inaccessible; it is not included in this completed run's results. Prompt wording and physical assignment also changed during the repair work, so this is not an isolated causal ablation of the grouping correction.

## Evidence

The artifact root is `artifacts/escalation_fixes/parallel_scored_20261030/` on the experiment host; immutable final JSON files and the watcher summary are also copied to the coordinator. Large data, model files and raw model journals remain local artifacts.

| Evidence | Path below artifact root | SHA256 |
|---|---|---|
| Seed42 combined result | `runs/paired_r6_final/seed42/audit/20261101T165814Z-8698d59c/combined_bank10_result.json` | `65cbdcdf5a5ee0b3120f704be4cf600d0ae290b4dcc0ad80134a1fed0c14b287` |
| Seed43 combined result | `runs/paired_r6_final/seed43/audit/20261101T165814Z-b4ec1f94/combined_bank10_result.json` | `6fab8152dbdedff99b2bd38eb8c28efb6cc02139ba60e068c0b81a6f87f143ff` |
| Frozen operations runner | `parallel_runner_v5.py` | `8ccb901658a35033cb07363c2c1abd004282b4e9ca4653ce910f67ebe175ccf3` |

The watcher summary is `monitor_r6_final/final_summary.json`; its verification errors array is empty and `accuracy_valid` is true. Earlier failed launches and partial trials remain separate and are not silently replaced by these results.

## November 4: Python operator-code variant, seed42

The separate `typed-operator-code-v1` evaluation completed both fixed five-case chains with no worker timeouts or nonzero worker exit codes. The official combiner verified the frozen case order, library lineages, server/tokenizer receipts and trusted feedback ledgers.

| Seed / variant | Retrieval | Through adaptation | Through evolution |
| --- | ---: | ---: | ---: |
| 42 / typed Python operator code | 2/10 | 5/10 | 5/10 |

Adaptation rescued three of eight prior misses. All five cases reaching evolution failed proposal validation before serving/executing new Python operators, so evolution added no successes. Across the 15 EVOLVE responses, first reported blockers were four malformed JSON responses, five invalid names and six incomplete input requirement declarations. These failures expose an authoring-interface problem; they do not establish diagnostic superiority of the kernel catalog. See [the diagnosis](NON_KERNEL_EVOLUTION_DIAGNOSIS_20261104.md) and [the simplification proposal and handoff](SIMPLIFICATION_PROPOSAL.md).

The run used **38 model calls**, **654,559 prompt tokens** and **127,453 completion tokens**, totaling **782,012 tokens**, or **14.7%** of the recorded T2 seed42 denominator of 5,314,218 tokens. The separate engineering canary is excluded from these figures. Seed43 has not run for this variant, so there is no new two-seed mean or significance claim.

The evaluated candidate is on `ssinha97@dgxstation.cc.gatech.edu` at `/raid/ssinha97/E4/artifacts/operator_code_fix_20261103/execution_candidate_20261104`. Paths below are relative to that candidate:

```text
artifacts/operator_code_canary/final_freezes/20261104T054005Z-cd185022/freeze_manifest.json
artifacts/operator_code_evaluation/seed42/audits/20261104T060301Z-bc016eb2/combined_bank10_result.json
```

Combined-result SHA256: `c39242c269383c777b926ef434b8ad8d3705cbc6796058cd0f46f11fd8eab9e1`.

The simplification and diagnosed interface fixes are pending. Preserve this completed trajectory when evaluating a future source revision.
