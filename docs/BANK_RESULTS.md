# Bank adaptation results — development evaluation complete

The Bank adaptation and trusted development evaluation are complete. The project Python 3.10 release suite passed 141/141 tests in 26.177 seconds (`artifacts/data_audit/bank_determinism_release_tests.log`). All 32 source files and 48 published clips passed independent hash, header, window and unit checks; both development cases' per-window counts match the independent audit. The trusted result file is `artifacts/bank_evaluation/trusted_dev_results.json`, SHA-256 `669d3757903c58fd4c1ab2927006fe0d023cea182dc6e361867a4adeca4387cb`. These two development cases are limited evidence, not scored accuracy or a harness trend. The ten scored cases remain untouched; there was no post-gold tuning or starter-library weakening.

## Source and sample status

The official schema reference is OpenRCA commit `c121a483a5a91dbae50708e5942c42a6609098b2`, Bank prompt SHA-256 `a702127cd22cde022f7f3975c0ab3203c15d8ceb0e74b3357f2615d1fafef987`. The working telemetry mirror is `tracer-cloud/opensre` revision `07714872ea2cec77c13f9dec17a688e9df9621d1`, path prefix `Bank`. Its metadata file hashes are pinned, but byte-for-byte identity with the official Google Drive archive is not verified.

The current sample configuration is `config/bank_sample.json`, SHA-256 `329acdc980afc0faa7bdeaf3c6a5ff6d1fca3c1b2e167b0d391674fdb28b7d52`. It freezes ten scored and two disjoint development windows with seed 42. The public scored/dev manifest hashes are `62bfb45bcb644e1530d43582c845dd7e09712b704c9310646cb1f1eb9fcc6de6` and `a63bdb5eeaad5f1acf614eb1984db08f1af5cd5f131994d6153613f2b82d9f3d`. The verified Bank input manifest SHA-256 is `becce73434b7d2eeea0ed6842e377916f86f3c2a4ce1fc815a91f56de631b310`; its 48-clip manifest SHA-256 is `49ca7f43cc8bab322b6df86826653983b9489a73985ccd2c068643cca4fe8225`. The clips contain 11,265,564 rows (9,641,785 trace, 622,803 container metric, 6,269 app metric, 994,707 logs) and total 1,190,280,745 bytes. Independent source/clip recovery checks are in `artifacts/data_audit/bank_selected_clip_audit.json`; independent per-window development counts are in `artifacts/data_audit/bank_independent_dev_counts.json`. See [BANK_RUNBOOK.md](BANK_RUNBOOK.md) for IDs, local windows and selection policy. Trusted preparation uses private fault-reason strata to balance sample allocation; reasons remain in the evaluator and are not copied into public manifests or prompts. This is metadata-stratified sampling, not label-blind random sampling.

## Source timestamp and coverage audit

The upstream schema declares trace timestamps in milliseconds, but observed source files have per-date units. A first-row preview initially appeared to show no trace rows in the first development interval when the `2021_03_23` values were interpreted as milliseconds; this was a unit-interpretation error, not missing traces. An independent raw count using the observed per-date units found:

| Development case | App metric rows | Container metric rows | Trace rows in reference + alert | Raw trace-source rows scanned | Log rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| `bank-20210323T0630-utc8` | 609 (287 reference, 322 alert) | 57,178 (28,713; 28,465) | 136,736 (51,340; 85,396) | 24,615,934 | 13,777 (5,113; 8,664) |
| `bank-20210325T1530-utc8` | 644 | 54,848 | 2,337,139 | Not included in the earlier independent window-count scan | 241,653 |

These are independent raw-source window counts, not the full public clip inventory. In the eight selected source dates, trace timestamps are milliseconds for `2021_03_04`, `2021_03_06`, `2021_03_07`, `2021_03_09`, and `2021_03_12`, and seconds for `2021_03_23`, `2021_03_24`, and `2021_03_25`. A separate `2021_03_10` source preview also showed milliseconds, but that date is not among the eight selected dates. Full per-file row validation, unit homogeneity checks and source-year checks completed for all 32 selected native files; the progress/audit log is `artifacts/data_audit/bank_clip_progress_20261015T200026Z.jsonl`. Metric and log timestamps are seconds. `duration` units remain undeclared. A `2021_03_09` trace scan found 1,028 prior-local-date records; the earliest was 2021-03-08 19:44:31.709 UTC+8. The record at CSV line 308,680 (`1615204174572` ms = 2021-03-08 19:49:34.572 UTC+8) was the first found more than ten minutes before the folder date, not the first prior-date record. The file contains 22,973,987 records within March 9 and no later-date rows. Raw rows are unchanged; clipping follows actual frozen windows without a fixed spill allowance. The published 48-clip manifest and independent recovery audits are recorded above.

## Current operator applicability

The current Bank-specific registry is conservative:

- Metric aggregation preserves Bank fields. The current policy allowlists 30 exact `metric_container.kpi_name` values as working gauge/rate interpretations, compared only with the same exact field across windows; units and absolute scales remain unverified. App `mrt` is context only with opaque `tc` identity and no component mapping; `rr/sr/cnt` remain unclassified. No resource fallback selects a primary KPI.
- Trace duration aggregation and within-`duration` contrast are implemented, but they must use per-date-corrected windows. The source has no success/error field, no `callType`, and no logical-service/database-target fields. Trace error rates, OSB root-KPI attribution, conditional flow, and failure-propagation scoring remain unsupported.
- Log volume can be contrasted by exact `cmdb_id`/`log_name`/minute as event-count evidence. Raw `value` text is not parsed; this is not a log error rate, GC diagnosis, or OOM classifier.
- Candidates retain exact observed identifiers. There is no verified `dockerA2` alias to the upstream 14-pod list and no vertical topology mapping. Bank-native cause names also do not fit the current Telecom category enum, so predicted category remains null.

## Development generation and replay

The four seed controls for each of the two development cases completed and passed. First-case controls had cold/warm/cache-disabled parity, five warm-cache hits per seed, and nine combined hits; their manifest is `artifacts/bank_controls/firstdev_v1/development/bank-20210323T0630-utc8/controls/controls_manifest.json`.

The first model-launcher preflight failed before an LLM call because the Bank runner checked `E4_BWRAPPED`, which the approved launcher strips from the child environment. The guard was corrected and verified against the actual sanitized environment. After both model attempts and deterministic replay, but before trusted gold evaluation, the final source/config snapshot was archived at `artifacts/data_audit/bank_source_final_v3.tar.gz`, SHA-256 `9af51ebf941d46a14da1372bf024e52f740a33a5eff2ae0796120f2694a2b892`.

Run `bank_firstdev_v2` completed one LLM call with zero repairs: 14,503 prompt tokens, 2,430 completion tokens, 102.952 seconds in the model call, and 190.605 seconds total. Its ten-node DAG SHA-256 is `0a586f4abd03c4d2e32daec0a31e1a976b20bdd7b0b25c59d42e9e6f6d7f77f5`. The graph is isomorphic to the supplied `combined_shared_candidates` seed with the same operators, parameters, bindings and edges after node-ID renaming. Literal-ID comparison reported no exact seed match, but the structural result reproduces the supplied example; this run does not demonstrate novel analysis or benefit.

Canonical combiner ordering was fixed after the pre-fix model output and replay had identical ranks and scores but evidence in different orders across Python hash seeds. After the fix, the six full-answer outputs matched exactly across two processes and three cache modes. Compared with the pre-fix model output, ranks and scores stayed exact and the evidence multiset stayed unchanged; only its evidence ordering differed. The replay manifests are `artifacts/bank_replays/fixed_seed1/development/bank-20210323T0630-utc8/saved_dag_replay/replay_manifest.json` (SHA-256 `729ad804774313f16d15c7fa9731b022f7f04520abd7a153dcb2c6e98cd2eb73`) and `artifacts/bank_replays/fixed_seed987/development/bank-20210323T0630-utc8/saved_dag_replay/replay_manifest.json` (SHA-256 `1806d224d0e01d16d9ffc145f46580ef46d1d143038f675a9b6b7bf95361c66e`). Both recorded cache hits 0/9/0; execution times were 47.120/4.120/39.001 seconds and 42.792/4.050/34.824 seconds. The implementation fingerprint is `ecfb01eb01b1855e56bf3974386f410fab16a5632e11eaa27a165af43607aa03`.

The canonical scored-candidate recombination audit preserved rank and score for all eight cached-control outputs and preserved their evidence multisets; see `artifacts/bank_evaluation/combiner_replay_audit/audit.json`. The trusted development outcomes are reported separately below; no scored-set accuracy is reported.

## Trusted development evaluation

The two frozen development cases had these gold causes and component-ranking results:

| Case | Gold | Metric control | Trace / log controls | Combined control | Gemma model |
| --- | --- | --- | --- | --- | --- |
| `bank-20210323T0630-utc8` | `MG01` / network packet loss | Rank 4; missed top 3 | Both missed | Missed | Missed; model DAG reproduces the combined seed topology |
| `bank-20210325T1530-utc8` | `Mysql02` / high CPU usage | Rank 1; top 3 hit | Both abstained | Rank 1; top 3 hit | Not run |

Metric and combined controls each ranked the gold component in the top three on 1/2 development cases. The model ranked the gold component in the top three on 0/1 case it ran. All category outputs were null, so none produced a category success. The second-case result is control-only; there was no model run for that case. These development outcomes do not estimate scored-set accuracy, and the single model case provides no accuracy trend. No scored gold was inspected, and no operator or prompt was tuned after viewing development gold.

The Bank dataset adaptation, development-only model run, trusted two-case evaluation, controls and exact cross-process replay are complete. The model copied the supplied combined seed structure, so no novel analysis or benefit is established. The ten scored cases remain untouched. M3's Telecom comparison remains complete, but its result does not transfer to Bank. M4's three-stage controller has not started; there is no scored Bank run or harness accuracy trend.
