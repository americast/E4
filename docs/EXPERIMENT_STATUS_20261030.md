# Experiment status, 30 October 2026

## Completed corrected evaluation, verified 2 November

The clean two-host `paired_r6_final` evaluation completed all 20 scheduled slots on 1 November at 21:15 UTC. Both combined result hashes verify. Mean stage-prefix accuracy is **30% → 55% → 70%**, compared with historical same-seed T2 at **40%**. Seed42 reached 80%; seed43 reached 60%. Adaptation rescued five runs and evolution rescued three.

The verified token totals are 608,313 for seed42 and 936,314 for seed43. Seed42's cumulative token costs are 4.2%, 8.1% and 11.4% of its recorded T2 baseline. Seed43's T2 usage is incomplete, so no cost ratio is claimed for it. See [the current results report](BANK_CURRENT_RESULTS.md) for exact counts and evidence hashes. Earlier checkpoints below are historical; the hardware-interrupted R5 run is not the final performance result.

## Final paired results, verified 1 November

The corrected-transport `paired_r3` experiment completed both ten-incident Bank evaluations. All four tasks exited successfully, and the host combiner verified both results.

| Seed | Retrieval | Through adaptation | Through evolution | Historical T2 |
|---|---:|---:|---:|---:|
| 42 | 40% | 40% | 50% | 40% |
| 43 | 30% | 50% | 50% | 40% |
| Mean | 35% | 45% | 50% | 40% |

This shows one evolution rescue and two adaptation rescues across the repeated runs. The same ten incidents were reused; statistical significance is not established. Seed42 used 914,150 recorded input/output tokens; seed43 used 956,853. The final seed43 case was preserved as a failed slot after a static-validation failure and nonzero worker exit; its chain closed through the existing recovery path without rerunning the case.

Results are under `artifacts/escalation_fixes/parallel_scored_20261030/runs/paired_r3/`. Root verified the orchestration and both combined-result hashes and copied them to the coordinator.

The mixed-table grouping bug still affected this frozen method. Its isolated fix has passed focused tests and static replay, but remains undeployed. The queued final budget/sandbox checks did not execute because the watcher agents hit a usage limit; validation was resumed on1 November. The pre-trim isolated tokenizer proof exceeded the input budget, so the final prose trim still needs a fresh proof. Earlier checkpoints below describe the experiment history.

## Completed frozen Bank evaluation

The `febd6411` method's seed42 continuation finished and the original host-side combiner verified all ten scheduled slots, including evaluator ledgers and library lineage.

| Method | Retrieval | Through adaptation | Through evolution | Scheduled incidents |
|---|---:|---:|---:|---:|
| Frozen E4 `febd6411`, seed42 | 40% | 40% | 40% | 10 |
| Historical T2, seed42 | — | — | 40% | 10 |

Nine incidents completed; one was interrupted by the GPU-monitoring failure and remains counted as failed infrastructure. No completed or interrupted diagnosis was rerun within this trajectory. Total E4 token cost remains unknown because an in-flight call was interrupted. This is an exploratory reused-case result, not evidence of statistical equivalence or a successful escalation trend.

Combined result:
`artifacts/m3/model_runs/e4r42-6e0cb553/audits/20261027T211452Z-6f705b36/combined_bank10_result.json`.

## Parallel authoring validation

The separately frozen gap-draft variant passed full static evolution-authoring checks on three fixed previously failing public contexts. These checks include strict operator compilation, canonical gap validation, exact prior-plan references, duplicate-body checks, identifier guards and candidate-registry construction. They do not execute a serving pipeline or evaluate an answer.

| Context | Model host | Calls | Result |
|---|---|---:|---|
| local case01 | Local V100/P100 pair | 2 | Static authoring validation passed |
| Wingtip case01 | Wingtip RTX 8000 | 2 | Static authoring validation passed |
| Wingtip case05 | Wingtip RTX 8000 | 1 | Static authoring validation passed |

All five API calls completed normally. Two initial proposals needed a missing runtime binding repaired. The gap-record object-shape failures were cleared. These are three contexts, not three correctly diagnosed incidents.

Frozen bundle and first trajectories are under
`artifacts/escalation_fixes/gap_draft_20261030/prepared_r2/` and `runs/`.

## Additional trusted-host bug fixed

In the completed Bank run, case04 created and executed a V2 operator successfully, but its final combiner raised `scorer 'typed-operator-ir-v2' has no trusted semantic consensus group`. The registered host wrapper and group classification still handled V1 only.

The correction stamps the validated operator identity for V2 and keeps V1/V2 in the same capped generated-IR group. It does not add a global generic-scorer allowlist, change scores or raise weights. Tests cover positive V2-to-answer flow, duplicate-body deduplication, unregistered-tag rejection and V1 compatibility. An actual bubblewrap worker-to-host-to-consensus smoke passed on DGX. The full DGX regression passed **571 tests**.

## Next scored experiment

Prepare two independently frozen seed42/43 evaluations on the same ordered ten Bank incidents. Each seed retains separate five-case chains and fresh initial libraries. The fixed schedule starts seed42/local on DGX, seed42/Wingtip on Wingtip, and seed43/local on the coordinator's V100/P100 pair concurrently; seed43/Wingtip uses DGX after seed42/local finishes. Scored CPU/operator/evaluator execution stays in the existing DGX isolation; external GPUs serve the model only.

All model services are loaded with the same checkpoint and context/generation settings. The final maximum-library tokenizer overflow was corrected by shortening redundant evolution wording. The repeated full suite passed **571 tests**, and all eight phase/repair prompts fit without changing the reserve.

The first new seed42/43 launch started on DGX at **2026-10-30 22:03 UTC**, under `artifacts/escalation_fixes/parallel_scored_20261030/runs/paired_r1/`. It stopped before any diagnoses because the routing adapter omitted the validated service-map role from its returned metadata. All three task processes reported `KeyError: 'service_role'`; no incident/model attempt began. Its artifacts remain preserved. A separate versioned adapter and fresh run metadata are being prepared; numerical method and model settings are unchanged.

Final tested DGX method fingerprint remains `39842e19010a0307c8844aae66f866d339506f5cf83a4a4bf9b1e0001d26631c`. Its release gate passed 571 tests and all phase/tokenizer checks; the routing failure is an orchestration metadata issue, not a measured method outcome.

**Update, 31 October:** the versioned paired_r2 run also closed operationally: all 60 recorded requests failed during template preflight with connection refused, before any model response or generation usage. Its 0/10 aggregates are not a valid model-accuracy measurement. Scored execution omitted the inner namespace relay startup even though the smoke-check path included it. A v3 adapter is being tested through the exact scored execution path before a fresh launch. Wingtip's expired model service was replaced on fresh port19244; local and DGX endpoints remain healthy.

**Update, 31 October 16:23 UTC:** fresh `paired_r3` is launched on DGX (orchestrator PID1765491). The corrected v3 runner passed ten ordinary tests and both opt-in live tests. All four service/logical-shard routes and the direct `execute_task` no-case path reached the model through the restricted M4 worker, with successful template/tokenizer responses and zero generation calls during validation. The scientific source, checkpoint and budgets remain unchanged. Three initial chains use DGX, Wingtip and the local GPU pair concurrently; the fourth waits for DGX's first chain. Launch and release receipts are under `artifacts/escalation_fixes/parallel_scored_20261030/runs/paired_r3/`. Actual diagnosis responses from this launch are not yet verified at this checkpoint.

No new scored-method result is available yet. Old trajectories, source archives and failed proofs remain preserved. Seeds repeat the same ten incidents and are not independent holdout samples.

The first scored response on each of the three initial chains has now been verified: HTTP200, no transport error, with token usage recorded. GPU activity was observed on all three hosts. No incident had closed at that check; accuracy remains pending.

Later in this run, seed42/local case03 (`bank-20210306T0330-utc8`) produced the first host-verified evolution rescue: retrieval and adaptation missed; the separate serving pipeline using a newly validated component-metric operator placed the ground truth at rank2. This is a successful diagnosis, distinct from static authoring validation. Seed43/local case02 also succeeded through adaptation after retrieval missed. Both seed evaluations remain incomplete.

Two earlier seed42 cases were blocked after successful operator authoring by an incorrectly derived mixed-table grouping contract. The diagnosis and isolated correction are documented in `docs/TABLE_GROUPING_CONTRACT_DIAGNOSIS_20261031.md`; active experiment code remains frozen.
