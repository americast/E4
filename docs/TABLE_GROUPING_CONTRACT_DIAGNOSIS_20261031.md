# Host-generated grouping contract blocks mixed metric operators

**Completed evaluation:** the clean `paired_r6_final` run finished on 1 November at 21:15 UTC. Its mean accuracy was 30% after retrieval, 55% through adaptation and 70% through evolution, versus historical T2 at 40%. Both result hashes verify. See [the results report](BANK_CURRENT_RESULTS.md). The earlier R4 startup failure and R5 hardware interruption remain preserved separately.

**Current evaluation,1 November07:35UTC:** corrected paired_r5 is running with the official source-bound starter library. Its19 operators and11 playbooks match the previous semantic content. A failed paired_r4 launch had reused old source-binding metadata and produced zero model calls; that attempt is preserved as an operational failure. The new restricted startup test verifies old metadata is rejected and rebuilt metadata passes. R5 has now returned its first actual HTTP200 model response; accuracy results are pending. All577 tests, eight prompt-budget contexts and the mixed-table execution/consensus smoke passed before deployment.

**Update,1 November:** the correction passed577 tests with zero skips, all eight tokenizer stress contexts, and an actual mixed component/KPI restricted-worker-to-consensus smoke without model calls. It was deployed with backups and a fresh source freeze. The new `paired_r4` evaluation launched at06:59UTC; no new accuracy result is available yet. The Docker asset visibility adapter used for isolated validation was excluded from deployment. Earlier failed proofs/logs and paired_r3 outcomes remain preserved.

The live `paired_r3` experiment exposed a host-contract bug after evolution successfully authored an operator. This is separate from the repaired transport failure. The running experiment remains frozen; a correction is being prepared in an isolated workspace for a subsequent variant.

## Observed failure

Seed42, local chain, case01 completed unsuccessfully. Evolution validated one new typed V2 operator without a static repair. The separate serving call and its repairs failed admission with:

```json
{
  "code": "input_grouping_unavailable",
  "details": {
    "node_id": "score_correlation",
    "port": "reference",
    "producer": "bank.metrics.aggregate@1+e63c4b7bbaace9a0",
    "required": ["component"],
    "available": ["bomc_id", "item_id", "source_field"]
  }
}
```

The evidence is in the DGX run's `seed42/audit/20261031T162140Z-211ea4ee/bank10_local/case01/diagnostic/attempts/stage3_synthesize/{evolution_record.json,admission_checks.jsonl}` beneath `artifacts/escalation_fixes/parallel_scored_20261030/runs/paired_r3/`.

## Why repair calls could not resolve it

The model authors a kernel program. The host derives its input requirements in `e4core/m4_operator_ir.py::_requirements_for_v2_program`; the evolution prompt explicitly tells the model not to author outer input requirements.

A program using component metric series and application KPI series receives both source-table requirements, but one flat grouping requirement:

```python
source_columns = {
    "metric/metric_container.csv": [...],
    "metric/metric_app.csv": [...],
}
group_by = ["component"]  # host-derived when component series are used
```

The compiler correctly preserves the existing meaning of a flat grouping requirement: each required table must provide it. The trusted aggregate producer declares:

| Table | Grouping dimensions |
|---|---|
| Component metrics | `component`, `source_field`, `bomc_id`, `item_id` |
| Application metrics | `app_kpi`, `source_field`, `bomc_id`, `item_id` |

Their intersection lacks `component`. The host consequently generates a requirement that its own aggregate producer cannot satisfy. Changing only the serving DAG cannot make application KPI rows become component-grouped rows. This is a host typing issue; HTTP requests and operator authoring succeeded.

## Isolated correction

Luna is preparing the correction under `artifacts/escalation_fixes/table_grouping_20261031/workspace/`. No running method, prompt, runner, service map or scored artifact is being changed.

- Represent required grouping dimensions per source table for mixed-table V2 programs: component metrics require `component`; application metrics require `app_kpi`.
- Validate each requirement against that table's trusted producer capabilities. Preserve the old flat requirement semantics and reject missing or incompatible grouping.
- Keep V1 behavior, numerical kernels, evidence thresholds, metric policies and evaluator unchanged.
- Add positive and negative regression cases, followed by a no-generation static replay of the captured public candidate and serving pipeline. Such a replay checks compilation, not diagnosis accuracy.

Do not deploy the correction into the active workspace until both frozen seed evaluations close. Any later evaluation must have a fresh source fingerprint and separate artifacts. Current results must retain the failed cases.
