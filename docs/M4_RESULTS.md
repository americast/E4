# M4 Bank results

The v4 source release passed. The single schema-only bootstrap v4 completed seed-only; its first resulting library initialized case one, which exhausted its stages without an accepted diagnosis. Case two continued from case one's resulting library and was accepted at stage 1. Across these two fixed development engineering cases, the top-three-within-three acceptance fraction was 1/2, descriptive only; this is not scored-set accuracy or a paper trend. Earlier bootstrap and development captures remain preserved as engineering history. No scored gold has been inspected, and no scored-set Accuracy@3,3 result is available.

## Verification and bounded projection

The final v4 full release suite passed **220/220 tests in 77.899 seconds**, including the release checks; `pip check` also passed. The frozen source implementation fingerprint is `86ab15ebb7af8bc7b9067a48c2477549da4b65b23a1e0de216f743f0fbf2f529`, and its source archive SHA-256 is `5759a0a9f50adcb20bf83cbbd969e4866941661e673ee0bf811e48e67802bfdf` (`artifacts/m4/verification/source_before_model_v4.json`). Policy, profile, and data hashes are unchanged. The bounded Aggregate projection suite passed **7/7** under bubblewrap (`artifacts/m4/verification/projection_release_v1.log`). A separate read-only audit used existing public cached aggregates, without gold or model calls: it projected 28 CPU summary rows to approximately 32 KB and 36 trace summary rows to approximately 28 KB from source aggregates between 7 and 35 MB. Selected values, support counts, and windows were unchanged (`artifacts/m4/verification/real_aggregate_projection.json`).

Projection is a bounded view of already-computed full-window summaries. It filters and pages rows, preserves scalar identity/unit/support fields, records source digests and omitted detail keys, and fails when the selected projection still exceeds 512 KiB. It does not sample rows or recompute statistics. Raw query pages remain available for analyses that need raw rows or log text.

## Schema-only bootstrap v1 engineering capture

Bootstrap v1 completed with status `complete_seed_only`. Its immutable snapshot is `0ae0ef653373cf616f89e30d372973cf009cf839ecd36bbeedc963e0eef786be`, with 10 trusted built-ins, four fixed seed playbooks, zero generated additions, two model calls, and one static repair. The calls used 17,796 prompt tokens and 6,683 completion tokens; measured model time was 227.2273915 seconds and bootstrap wall time was 234.9992702 seconds. Peak GPU memory was 21,403 MiB with 11,090 MiB minimum free; the inference server exited cleanly.

A proposed `log_keyword_alert_analysis` / `evolved.log_keyword_scorer` operator was not admitted: its recorded synthetic-control execution had a `tuple object has no attribute get` error, and the operator was not reachable from a verified generated playbook. This was a synthetic bootstrap check only; it did not involve diagnosis labels or real-case feedback.

The v1 source archive SHA-256 is `c8d3008598091b285d27ed64d5ef05228e498c04b25ab97eb294a41df7baabdc`; its implementation fingerprint is `ac75780c56e78135ecf5072c0aef8de34ca09f4fe5c8c8f678c0c200eddb88e9`. This capture is retained as engineering evidence and did not seed the v2 diagnosis attempt.

## Independent interface audit before v2

Before any development-gold inspection, an independent public-API audit found that the bootstrap/controller prompts did not describe the actual `bank.public.query` metadata layout (`Aggregate.metadata.public_query.rows_data`) and the synthetic logging fixtures lacked a lexical positive case because their log text was identical. The v2 source incorporated these interface and fixture updates; the audit does not establish that either issue caused the v1 synthetic error above.

## Schema-only bootstrap v2 and first development engineering capture

Bootstrap v2 completed with status `complete_seed_only`, two model calls, one static repair, ten trusted built-ins, four fixed seed playbooks, and zero generated additions. Its snapshot ID is `3e194f3e816a9446b70fe59042ce688a5afdb4e860d709117d0604091dcc8c45`, with implementation fingerprint `a0f610ac51852534855de286949d4a3f64d4249c988600353e2b6cae6d93dbad`. Bootstrap recorded 19,725 prompt tokens, 7,100 completion tokens, and 244.204 seconds of model-call time, including HTTP prefill and preflight. Its artifact is under `artifacts/m3/model_runs/m4_schema_bootstrap_v2/m4_bootstrap/bootstrap/`.

The first development case, `bank-20210323T0630-utc8`, ran against that seed-only library and ended `exhausted_or_failed`. Stage 1 was rejected. Stage 2 was rejected after one static repair. Stage 3 produced a static error; the subsequent repair prompt exceeded the 32,768-token context budget (`24,884` prompt tokens plus `8,192` reserved completion tokens), so no repair inference ran and the attempt ended `model_call_failed`. This is a controller/serving failure, not an accepted diagnosis; process completion does not mean the evaluator accepted an answer.

The four usage-bearing HTTP generations reported a subtotal of 78,383 prompt tokens and 8,536 completion tokens. The aggregate usage field is null because the additional preflight-only invocation had no usage record; these subtotals do not represent a complete aggregate. Case wall time was 453.782691 seconds and model-call time was 362.807697 seconds. Full stage records and requests are preserved under `artifacts/m3/model_runs/m4_firstdev_v2/m4/firstdev/diagnostic/`.

The serving-only v3 release passed 218/218 tests in 66.940 seconds with `pip check` passing. Its fingerprint was `8d8a7b7b3f64c4ad12f464295409869f1576b0271eab83a69dc680678b78530d` and source archive SHA-256 `2064fc91b4b516715a0c4492f191a5d39454a4d9bd57bf6cc1cae3540e2e4c0e` (`artifacts/m4/verification/source_before_model_v3.json`). As a historical serving-only correction, the original seed-only v2 library was revalidated against the v3 implementation. All 20 fixed synthetic seed fixtures passed; metadata-only child snapshot `82d88893533f6ad021aa890490e4f8aab97fadbddbdb7674f57b144e7ca03e9a` is at `artifacts/m4/verification/revalidated_initial_v3/library/`. The original v2 snapshot `3e194f3e816a9446b70fe59042ce688a5afdb4e860d709117d0604091dcc8c45` was unchanged. The final v4 bootstrap then initialized case one, and case two inherited case one's resulting library.

The first corrected development case, `bank-20210323T0630-utc8`, ran with the v3 source and ended `exhausted_or_failed`, without queries, new operators, or an accepted diagnosis. Stages 1 and 2 were rejected (stage 2 used one static repair). Stage 3 failed static validation after two repairs because each proposal used an `input_ports` map where the generated-operator schema required a list. The schema was missing from both prompts and the compiler error was ambiguous. These are interface-contract findings; v4 changes the nested generated-spec schema guidance and error wording only, with no algorithm, grammar, profile, policy, or seed changes.

All three stages' first request prompts and initial model responses were byte-identical between v2 and v3. The v3 repair prompts fit context: stage 3 used prompt plus reserved completion budgets of 8,171 + 8,192 and 8,311 + 8,192 tokens for its two repair calls. The retained v3 run made six actual HTTP calls, including three static-repair calls (one in stage 2 and two in stage 3). Usage totals across those calls were 80,749 prompt and 16,762 completion tokens; model-call time was 665.1073027011007 seconds, HTTP inference time 664.8546872567385 seconds, and case wall time 755.9239724064246 seconds. Its usage-only library child was `31300a215bf1ca0ffd1cdd0923656052e89ecc55f0cd51c7f952c2c211a21621`. Peak GPU memory was 21,403 MiB and minimum free memory was 11,090 MiB. Preserve this capture as an engineering failure; it is not an accepted diagnosis.

Bootstrap v4 launched at 19:00 UTC using the frozen source. Its output is under `artifacts/m3/model_runs/m4_schema_bootstrap_v4/m4_bootstrap/bootstrap/`. The bootstrap snapshot initializes case one, then case two receives case one's resulting library so accepted additions and usage counts evolve sequentially.

The sole v4 bootstrap completed `complete_seed_only` after one model call and no static repairs. Its immutable snapshot is `1862834dc9bc244f81aed5fa3aea6e531beaa54a9453f0def20d19fd9d8e58f4`: 10 built-ins, four seeds, and zero generated additions. The call used 8,904 prompt and 3,316 completion tokens; model-call time was 119.62815730273724 seconds, HTTP inference time 119.5821047918871 seconds, and bootstrap wall time 127.30762542132288 seconds. Peak GPU memory was 21,403 MiB and minimum free memory was 11,090 MiB; the server exited cleanly.

The proposed `log_pattern_analysis` / `evolved.log_pattern_scorer@1+d27f01ca37d4316a` passed static validation but failed all five synthetic executions with `expected metadata cache wire envelope`; it was not admitted. There were two rejections total: the playbook proposal and the unreachable generated operator. This was the only v4 bootstrap sample; no regeneration or changes based on this synthetic failure were made. The first final development run, `m4_firstdev_v4`, launched at 19:04 UTC using the v4 snapshot. It ended `exhausted_or_failed` with `accepted=false`; case two launched at 19:19 UTC using its resulting library.

Case one was rejected at stages 1 and 2 (stage 2 used one static repair). Stage 3 failed static validation after two repairs. Its errors were, in order: a sandbox-restricted Python construct (`Lambda`), malformed DAG node objects (`inputs`/`params`/`bindings`), and a returned value that was not `RankedAnswer`. The generated `input_ports` list was correctly shaped in this run. No query ran and no generated operator was executed or admitted. The library changed from bootstrap snapshot `1862834dc9bc244f81aed5fa3aea6e531beaa54a9453f0def20d19fd9d8e58f4` to `7323078280413efaec51bccba2247b8d9f1886b3cdc6a223683942d7024aec26` through usage accounting only.

The run made six actual model calls, including three repair calls. It used 75,656 prompt and 17,716 completion tokens; model-call time was 692.797699724324 seconds, HTTP inference time was 692.537357098423 seconds, and case wall time was 785.0135350776836 seconds. Peak GPU memory was 21,403 MiB and minimum free memory was 11,090 MiB; the server exited cleanly. Full artifacts are under `artifacts/m3/model_runs/m4_firstdev_v4/m4/firstdev/diagnostic/`.

This case does not establish real generated-operator execution, admission, or retention after trusted feedback. No Gemma-proposed operator ran against a real Bank incident or was admitted. The bootstrap proposal ran only on synthetic fixtures. The final-protocol proposals remain unmodified; this run received no extra repairs or regeneration. General harness defects can be fixed under a new recorded source version.

The second case, `bank-20210325T1530-utc8`, was accepted at `stage1_select` without repairs and early-stopped. It made one HTTP call (16,105 prompt and 1,085 completion tokens); model-call time was 63.74290231242776 seconds, HTTP inference time 63.69229434709996 seconds, and case wall time 311.10181173402816 seconds. No query ran and no generated operator was admitted. Its library advanced from `7323078280413efaec51bccba2247b8d9f1886b3cdc6a223683942d7024aec26` to `1978c53aa410f209a3715a2e9e6ed9e3fddf7205a71d188c73e380e4457a9f2c` through usage accounting. Full artifacts are under `artifacts/m3/model_runs/m4_seconddev_v4/m4/seconddev/diagnostic/`.

Together, the two final cases made seven HTTP calls with three repair calls, 91,761 prompt and 18,801 completion tokens, 756.5406020367518 seconds of model-call time, and 1,096.1153468117118 seconds of case wall time. Bootstrap cost is reported separately. All three executed DAGs had 10 nodes: one non-cacheable node and nine stored misses, with zero hits. Their elapsed node-time sums were 43,039.721 ms (case-one stage 1), 43,216.976 ms (case-one stage 2), and 184,688.754 ms (case-two stage 1). Built-in `bytes_processed` values are zero placeholders and do not measure physical I/O. Query cost was zero because no query ran. Peak GPU memory was 21,403 MiB, minimum free memory was 11,090 MiB, and the server exited cleanly.

The final top-three-within-three acceptance fraction is 1/2 on these two development engineering cases only. Both cases made zero queries; there were zero real generated-operator admissions, and no generated operator reached Stage 3 against a real Bank incident. Admission, retention, eviction, and rollback tests use scripted synthetic clients and fixture operator source; they are not evidence that Gemma discovered or admitted an operator. All source hashes were reverified unchanged after both runs.

## Development and scored outcomes

| Measure | Current result |
| --- | --- |
| Bootstrap v1 | Complete seed-only engineering capture; 0 generated additions |
| Bootstrap v2 | Complete seed-only engineering capture; 0 generated additions |
| First development v2 case | Exhausted stages; no accepted diagnosis; serving repair prompt exceeded context budget |
| First development v3 case | Exhausted stages; no accepted diagnosis; generated-operator schema contract was missing from prompts |
| Final v4 release | Passed 220/220; `pip check` passed |
| Serving-only v3 release | Passed 218/218; `pip check` passed |
| Historical v3 seed-library revalidation | Passed 20/20; child snapshot `82d88893…7ca03e9a` |
| Bootstrap v4 | Complete seed-only; one call, zero repairs, zero generated additions |
| Final first development case | Exhausted stages; accepted=false; no queries or generated-operator execution/admission |
| Final second development case | Accepted at stage1_select; early-stopped from first case's usage-only child |
| Final development acceptance | 1/2 descriptive engineering cases; not scored accuracy |
| Real Gemma-generated operator execution/admission/retention | Not demonstrated; real bootstrap proposals were rejected and case-one synthesis never executed |
| Query requests | Zero in both final cases |
| Cache results | Three 10-node DAG executions; one non-cacheable node and nine misses per DAG; zero hits |
| Development feedback outcomes | v2/v3 engineering attempts and final case one ended without acceptance; final case two accepted at stage 1 |
| Scored Bank cases and Accuracy@3,3 | Not run |

The two frozen development cases are engineering checks, not a scored-set estimate. No bootstrap or diagnostic output will be selected by diagnosis performance, and all future stage failures, rejected proposals, model calls, resource use, and library lineage must be retained.

Cache note for future efficiency reporting: the executor cache key includes the complete runtime JSON, including measured DAG node count and depth. A different graph shape can therefore produce conservative cache misses even when shared-node input values and declared bindings match; this does not change execution correctness. No M5 efficiency result is available yet.
