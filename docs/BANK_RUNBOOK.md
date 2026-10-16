# Bank dataset runbook — adaptation complete

This runbook records the Bank schema facts and the checks required before using it as the primary dataset. The official OpenRCA schema source is commit `c121a483a5a91dbae50708e5942c42a6609098b2`, file `third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py`, SHA-256 `a702127cd22cde022f7f3975c0ab3203c15d8ceb0e74b3357f2615d1fafef987`. The working mirror is `tracer-cloud/opensre` revision `07714872ea2cec77c13f9dec17a688e9df9621d1`, path prefix `Bank`; byte-for-byte equality with the official Google Drive archive has not been established. The 10-day mirror identity and per-file telemetry hashes are retained in `config/openrca_bank_source.json` and data-audit manifests.

## Declared upstream schema

The Bank schema prompt declares these inputs:

| File | Header | Declared timestamp unit | Notes |
| --- | --- | --- | --- |
| `metric/metric_app.csv` | `timestamp,rr,sr,cnt,mrt,tc` | seconds | `tc` is the opaque `app_kpi` entity identity. `mrt` is permitted as development KPI context only; its units remain undeclared. `rr`, `sr`, and `cnt` remain unclassified. |
| `metric/metric_container.csv` | `timestamp,cmdb_id,kpi_name,value` | seconds | KPI identity must include `kpi_name`; the prompt gives example names but does not establish full KPI units or kinds. |
| `trace/trace_span.csv` | `timestamp,cmdb_id,parent_id,span_id,trace_id,duration` | milliseconds | Supports trace-local structural parent edges and within-field duration contrasts after identity/support checks. Duration units are undeclared. |
| `log/log_service.csv` | `log_id,timestamp,cmdb_id,log_name,value` | seconds | `value` is free text in the example (a JVM GC message). No Bank log parser/operator is established yet. |

The prompt names 14 possible pods/components: `apache01`, `apache02`, `Tomcat01`, `Tomcat02`, `Tomcat03`, `Tomcat04`, `MG01`, `MG02`, `IG01`, `IG02`, `Mysql01`, `Mysql02`, `Redis01`, and `Redis02`. The trace example uses `dockerA2`, which is not in that list. Do not alias it to a pod without a verified deployment mapping. The prompt describes pod-level entities and horizontal traces, not a verified host/container/pod hierarchy.

The source says all incident-time analysis uses UTC+8. It declares metric and log timestamps in seconds and trace timestamps in milliseconds. Preserve raw timestamps and per-source-file unit metadata. Normalize only after validating each file's actual unit, using a documented conversion into epoch milliseconds. Do not infer a duration unit from the timestamp unit, or mix `duration` with timestamp differences or `mrt`, until a source declaration establishes compatibility. Window boundaries, timezone conversion, half-open behavior, and timestamp coverage must be frozen in the data-preparation decision record before clipping.

### Observed timestamp/schema conflict

The upstream prompt's trace-millisecond declaration conflicts with observed source files. The eight selected dates record milliseconds for `2021_03_04`, `2021_03_06`, `2021_03_07`, `2021_03_09`, and `2021_03_12`, and epoch seconds for `2021_03_23`, `2021_03_24`, and `2021_03_25`. A separate `2021_03_10` source preview also showed milliseconds, but that date is not selected. The first-row probe that filtered `2021_03_23` traces as milliseconds and appeared to find zero rows was a unit-interpretation error; its first timestamp `1616428799` is seconds. Full-stream homogeneity and source-year checks passed for all 32 selected native files before clip hashes were frozen. An independent raw scan for the first development case counted 24,615,934 source trace rows and 136,736 rows inside its reference-plus-alert windows. Independent counts for the second development case are 644 app rows, 54,848 container rows, 2,337,139 trace rows and 241,653 log rows. These are raw-window counts, not final clip counts; the verified 48-clip totals are recorded below.

The `2021_03_09` trace source contains 1,028 records whose local timestamp falls on `2021_03_08`; the earliest is 2021-03-08 19:44:31.709 UTC+8. CSV line 308,680 (`1615204174572` ms = 2021-03-08 19:49:34.572 UTC+8) was the first found more than ten minutes before the folder date, not the first prior-date record. The source file contains 22,973,987 records within March 9 and no later-date rows. This is a source-date spill, not a reason to shift or resample data. Parse each source timestamp with that file's validated native unit, convert to the actual instant, and clip only records in the frozen UTC windows; do not assume a fixed spill duration or equate the folder date with every row's local date. The approved public layout preserves native files by source date at `public/cases/<case_id>/source_dates/<date>/<table>`, with per-source units in the alert and clip metadata. Files recorded in seconds do not establish subsecond timestamp precision.

## Telecom-to-Bank semantic boundary

The present core and Telecom adapter cannot be treated as a Bank adapter. The Bank prompt schema has no trace `success`/error field, no `callType`, no logical `serviceName`, and no database target. Its trace rows therefore cannot support Telecom-style per-trace failure denominators, OSB-root KPI alignment, JDBC/database role attribution, or conditional root-to-backend flow. Structural parent links can be inspected only within a `trace_id`; they do not by themselves identify failure propagation or causality.

Candidate Bank analyses must be gated by what the audited files actually establish:

| Family | Bank support status | Validity gate |
| --- | --- | --- |
| Metric contrasts | Thirty exact `metric_container.kpi_name` values are allowlisted; app values are context only | The current policy marks `rr/sr/cnt` unknown and allows `mrt` only for app KPI context; no resource fallback is permitted. The container allowlist's working interpretations are source-name-based, two-sided, and compared only within each exact field; absolute units/scales remain unverified. Keep series distinct by source table, exact entity, and KPI. |
| Trace duration contrasts | Supported only as a within-`duration` comparison, subject to units audit | Validate finite duration values, trace/span uniqueness, per-file timestamp normalization, stable exact entity IDs, and independent support. Duration units remain undeclared. Do not label a duration shift as root cause solely from association. |
| Trace error contrasts | Unsupported by the declared schema | `trace_span.csv` declares no success or error field, so a valid success/failure rate and denominator cannot be constructed. Abstain; do not infer success from span presence or logs. |
| Conditional flow / root KPI attribution | Unsupported by the declared schema | No `callType`, root OSB role, logical service, database target, or explicit request outcome is declared. Do not reuse the Telecom flow operator or infer equivalent roles. |
| Trace parent-edge audit | Structural links only | `parent_id` can describe a trace-local edge when resolved against `(trace_id, span_id)`. Keep duplicate span identities, unresolved parents, and out-of-window references as audit outcomes; do not infer cross-trace edges. |
| Failure-propagation / dependency scoring | Unsupported from the declared Bank schema | Without a failure outcome field, do not assign direct/propagated failure weights or rank a component as the source of a propagated error. |
| Log evidence | Source-row volume contrast implemented; text interpretation unsupported | `bank.logs.volume_contrast` counts log rows per exact component/log name/minute. This can detect volume changes, not error rates or causes. The raw `value` text is not parsed; no GC/OOM classifier is established. |
| Dependency edges | Structural edges only; failure propagation unsupported | Same-trace parent references can be audited. No success/error field exists, so direct/propagated-failure scoring remains ineligible. No vertical topology is inferred. |
| Combined evidence | Conditional | Combine only eligible metric, trace-duration, and log-volume branches. Unknown KPI fields do not rank; unsupported trace-error/flow/failure-propagation branches must abstain. |

Bank's declared cause vocabulary differs from the core's current Telecom category enum: the Bank prompt lists high CPU, high memory, network latency, packet loss, high disk-I/O read usage, high disk-space usage, high JVM CPU load, and JVM heap OOM. Do not map these to Telecom categories such as `CPU fault`, `network delay`, or database failures. Leave category unknown/null until an explicit Bank-compatible category contract and evidence-backed classifier are implemented. Changes to starter-library strength are deferred; these limits are data applicability constraints, not reasons to silently substitute a different operator.

Four source-neutral Bank seed DAG structures live in `config/bank_seed_library.json`: `native_metric_contrast`, `per_trace_native_duration`, `per_minute_log_volume`, and `combined_shared_candidates`. Their operator versions are materialized from the current registry for prompts. They are structural examples, not evidence of cause accuracy. Unsupported families remain abstentions; M4/bootstrap or starter-library strength changes are not part of this dataset switch.

## Data boundary, provenance, and sample freeze

The metadata sources are `data/raw/openrca/Bank/record.csv` and `data/raw/openrca/Bank/query.csv` (136 rows each; their audited headers are recorded in `config/openrca_bank_source.json`). They contain task/evaluator metadata and must stay outside any model-readable case mount. Trusted preparation uses private fault-reason strata only to balance sample selection; the exact reason labels remain in the evaluator split and never enter public manifests or model prompts. This is metadata-stratified sampling, not label-blind random sampling. The source population has 127 physical query windows, with 114 single-record windows meeting the metric_app coverage criterion. Coverage uses 80% of unique `metric_app.timestamp` buckets in each of the adjacent 30-minute half-open windows; it indicates data availability, not a healthy reference. The sample uses seed 42 and contains ten scored and two temporally disjoint development windows:

| Split | Case ID | Alert window, Asia/Shanghai (UTC+8) | Source date |
| --- | --- | --- | --- |
| Scored | `bank-20210304T0100-utc8` | 2021-03-04 01:00–01:30 | `2021_03_04` |
| Scored | `bank-20210304T1800-utc8` | 2021-03-04 18:00–18:30 | `2021_03_04` |
| Scored | `bank-20210306T0330-utc8` | 2021-03-06 03:30–04:00 | `2021_03_06` |
| Scored | `bank-20210307T0300-utc8` | 2021-03-07 03:00–03:30 | `2021_03_07` |
| Scored | `bank-20210307T0330-utc8` | 2021-03-07 03:30–04:00 | `2021_03_07` |
| Scored | `bank-20210309T0800-utc8` | 2021-03-09 08:00–08:30 | `2021_03_09` |
| Scored | `bank-20210309T1730-utc8` | 2021-03-09 17:30–18:00 | `2021_03_09` |
| Scored | `bank-20210312T2030-utc8` | 2021-03-12 20:30–21:00 | `2021_03_12` |
| Scored | `bank-20210324T1100-utc8` | 2021-03-24 11:00–11:30 | `2021_03_24` |
| Scored | `bank-20210325T2000-utc8` | 2021-03-25 20:00–20:30 | `2021_03_25` |
| Development | `bank-20210323T0630-utc8` | 2021-03-23 06:30–07:00 | `2021_03_23` |
| Development | `bank-20210325T1530-utc8` | 2021-03-25 15:30–16:00 | `2021_03_25` |

Current selection identities: `config/bank_sample.json` SHA-256 `329acdc980afc0faa7bdeaf3c6a5ff6d1fca3c1b2e167b0d391674fdb28b7d52`; scored public manifest SHA-256 `62bfb45bcb644e1530d43582c845dd7e09712b704c9310646cb1f1eb9fcc6de6`; development manifest SHA-256 `a63bdb5eeaad5f1acf614eb1984db08f1af5cd5f131994d6153613f2b82d9f3d`. The refreshed sample records observed per-date units without changing the twelve IDs/windows. The metadata audit SHA-256 is `5100e695ec49eda2c86de9844610e5e4ee425042296ed4ddc50c6e1e0451ed55`; metric-app coverage audit SHA-256 is `245304c5c91eded7b322989f9a93f3c2c3c8322f05affba34965298e00949e72`. Metadata file SHA-256 values are `record.csv` `eea788a8c6d07d5d9567067d1e740fe71f3221d8357f0e12e1c643c42cbd15f7` and `query.csv` `605d3b5c863928caf1611d97858379765858595e150efad2f77d2f733774f588`. These are preparation metadata hashes, not telemetry clip hashes. The frozen Bank input manifest SHA-256 is `becce73434b7d2eeea0ed6842e377916f86f3c2a4ce1fc815a91f56de631b310`; the 48-clip manifest SHA-256 is `49ca7f43cc8bab322b6df86826653983b9489a73985ccd2c068643cca4fe8225`.

All 32 selected native telemetry files (22,604,535,431 bytes) passed source hash, row-level timestamp-unit and source-year checks. All twelve cases are now published as 48 verified clips containing 11,265,564 rows (9,641,785 trace; 622,803 container metric; 6,269 app metric; 994,707 logs), totaling 1,190,280,745 bytes. The independent recovery audit verified all 32 source hashes and all 48 clip headers, windows and units; both development cases' counts match the per-window independent audit in `artifacts/data_audit/bank_independent_dev_counts.json`. See `artifacts/data_audit/bank_selected_clip_audit.json`. The earlier finalizer `NameError` occurred before publication and has been recovered; the frozen manifests above identify the published inputs.

Before model runs, verify:

1. The exact upstream repository revision, mirror/file hashes, license/provenance, and actual CSV headers/row counts.
2. Bank-specific timestamp, timezone, window, duplicate-row, missing-value, KPI identity/unit, and entity-mapping policies from public metadata/schema and blind telemetry inspection.
3. A deterministic set of ten scored and two disjoint development incidents with the public IDs/windows listed above, per-file hashes, sufficient telemetry coverage, and exact source-to-public-clip provenance. The trusted sampler uses hidden fault-reason strata; keep labels private and do not present the sample as purely random.
4. A public adapter that loads only the selected case clips and permitted static catalog; keep raw full days, other cases, `record.csv`, `query.csv`, and audit outputs outside the diagnostic/model worker mount.
5. Synthetic fixtures for seconds-vs-milliseconds conversion, UTC+8 boundaries, half-open windows, repeated metric keys, missing/nonfinite values, duplicate span IDs across traces, unresolved parent IDs, mixed `cmdb_id` aliases, and absent-success abstention.

The fixed ten-scored/two-development Bank IDs/windows are listed above. Do not inspect scored gold to adapt thresholds, write prompts, or repair outputs. The trusted preparation code uses private reason strata only to balance the predeclared sample; diagnostic/model workers receive only public windows and telemetry. The first development case may be evaluated only after implementation/source is frozen, using a separate trusted evaluator; that result must remain separate from the ten-scored run.

## Commands and current execution status

The upstream schema is in the checked-out repository at the pinned commit. Verify that checkout with:

```sh
git -C third_party/OpenRCA rev-parse HEAD
sed -n '1,180p' third_party/OpenRCA/rca/baseline/rca_agent/prompt/basic_prompt_Bank.py
```

Existing trusted preparation entrypoints are:

```sh
.venv/bin/python scripts/fetch_bank.py bootstrap
.venv/bin/python scripts/prepare_bank.py metadata
.venv/bin/python scripts/fetch_bank.py metric-app
.venv/bin/python scripts/prepare_bank.py audit-metric-app --coverage-fraction 0.8
.venv/bin/python scripts/prepare_bank.py sample --seed 42
.venv/bin/python scripts/fetch_bank.py selected-telemetry --workers 3
.venv/bin/python scripts/prepare_bank.py clip-selected
```

These steps describe a clean preparation workspace. `metadata` and `sample` write private evaluator files; the Bank sampler intentionally reads private reason strata for trusted sample balance, but those labels must never enter public manifests or model prompts. The existing sample is guarded against silent replacement; do not use `--overwrite-freeze` or `--overwrite-public` without a reviewed diff showing the twelve IDs/windows unchanged. For the current verified freeze, use the input and clip manifest hashes above and the recovery audit artifacts; do not repeat the fetch/scan or overwrite the public sample. Never use a legacy single-unit clipper for the mixed-unit source dates.

The Bank-specific runner is `scripts/run_bank_development.py`. It supports four CPU-only seed controls, saved-DAG CPU replay, and a model-generated development DAG. CPU-only controls require a writable existing `E4_RUN_DIR` but make zero model requests:

```sh
mkdir -p artifacts/m3/bank_controls_cpu
E4_RUN_DIR=artifacts/m3/bank_controls_cpu .venv/bin/python scripts/run_bank_development.py \
  --case-id bank-20210323T0630-utc8 --controls
```

The recorded first-development model invocation was:

```sh
.venv/bin/python scripts/run_m3_model.py --profile config/gemma_m3_inference.json \
  --run-id bank_firstdev_v2 -- \
  .venv/bin/python scripts/run_bank_development.py --case-id bank-20210323T0630-utc8
```

The launcher supplies the offline namespace and existing `E4_RUN_DIR`; the Bank runner writes under `E4_RUN_DIR/development/<case-id>/`. A saved DAG can be replayed without an LLM request by setting a fresh existing run directory in `E4_RUN_DIR` and invoking `scripts/run_bank_development.py --case-id <dev-id> --saved-dag <project-relative-dag-path>`.

The data freeze and independent clip audit are complete. All four CPU seed controls for both development cases have completed and passed. First-case controls had cold/warm/cache-disabled parity; each seed recorded five warm-cache hits and nine combined hits. The first-case controls manifest is `artifacts/bank_controls/firstdev_v1/development/bank-20210323T0630-utc8/controls/controls_manifest.json`.

The first launcher preflight (`bank_firstdev_v1`) failed before any LLM call because the runner checked `E4_BWRAPPED`, which the approved launcher strips from the child environment. The runner guard was fixed and tested with the actual sanitized environment. The project Python 3.10 release suite passed 141/141 in 26.177 seconds (`artifacts/data_audit/bank_determinism_release_tests.log`). After both model attempts and deterministic replay, but before trusted gold evaluation, the final source/config snapshot was archived at `artifacts/data_audit/bank_source_final_v3.tar.gz`, SHA-256 `9af51ebf941d46a14da1372bf024e52f740a33a5eff2ae0796120f2694a2b892`.

Model run `bank_firstdev_v2` completed one LLM call, with zero repairs, 14,503 prompt tokens, 2,430 completion tokens, 102.952 seconds in the model call and 190.605 seconds total. Cold, warm and cache-disabled execution took 43.612, 4.078 and 35.950 seconds. Its ten-node DAG SHA-256 is `0a586f4abd03c4d2e32daec0a31e1a976b20bdd7b0b25c59d42e9e6f6d7f77f5`. The graph is isomorphic to the provided `combined_shared_candidates` seed with identical operators, parameters, bindings and edges after node-ID renaming. Literal-ID comparison reports no exact seed match; structurally, the model reproduced the supplied example, so this run does not establish novel analysis or benefit.

After fixing canonical combiner ordering, the six full-answer outputs matched exactly across two processes and three cache modes. Compared with the pre-fix model output, ranks/scores remained exact and the evidence multiset remained unchanged; only evidence ordering differed. Replay manifests are `artifacts/bank_replays/fixed_seed1/development/bank-20210323T0630-utc8/saved_dag_replay/replay_manifest.json` (SHA-256 `729ad804774313f16d15c7fa9731b022f7f04520abd7a153dcb2c6e98cd2eb73`) and `artifacts/bank_replays/fixed_seed987/development/bank-20210323T0630-utc8/saved_dag_replay/replay_manifest.json` (SHA-256 `1806d224d0e01d16d9ffc145f46580ef46d1d143038f675a9b6b7bf95361c66e`). Both runs recorded cache hits 0/9/0; execution times were 47.120/4.120/39.001 seconds and 42.792/4.050/34.824 seconds. The implementation fingerprint was `ecfb01eb01b1855e56bf3974386f410fab16a5632e11eaa27a165af43607aa03`. The trusted development evaluation is complete (see [BANK_RESULTS.md](BANK_RESULTS.md)); the canonical scored-candidate recombination audit preserved all eight cached-control rank/score outputs and evidence multisets, at `artifacts/bank_evaluation/combiner_replay_audit/audit.json`.

## Completion and scope limits

The data freeze, 141-test release suite, all eight controls, the first-development model run, deterministic replay, and trusted two-case development evaluation are complete. The trusted result file is `artifacts/bank_evaluation/trusted_dev_results.json`, SHA-256 `669d3757903c58fd4c1ab2927006fe0d023cea182dc6e361867a4adeca4387cb`; findings are in [BANK_RESULTS.md](BANK_RESULTS.md). The ten scored cases remain untouched, with no post-gold tuning. Bank adaptation is complete. M4's serving controller, bootstrap/admission, scored Bank evaluation, and any 10-case accuracy trend have not started. Do not infer Telecom-to-Bank portability or paper-level fidelity from this adaptation.
