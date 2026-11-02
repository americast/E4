#!/usr/bin/env python3
"""Label-blind inventory of native Bank metrics in the frozen development clips.

Reads only the public development manifest and its hash-pinned public telemetry.
It does not open any evaluator file or use incident outcome labels.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from e4core.bank_metric_policy import load_bank_metric_policy


# Documentary candidates for audit only. These are not active ranking rules.
# Exact keys are matched after an exact source-family envelope is validated.
_REDIS_INFO_SEMANTICS: Mapping[str, Mapping[str, str]] = {
    "used_memory": {"kind": "gauge", "unit": "bytes", "meaning": "Redis allocator bytes currently allocated"},
    "used_memory_rss": {"kind": "gauge", "unit": "bytes", "meaning": "Redis RSS bytes"},
    "used_memory_peak": {"kind": "gauge", "unit": "bytes", "meaning": "peak memory in bytes; high-water mark may persist after usage falls", "candidate_ranking_eligible": False},
    "total_system_memory": {"kind": "gauge", "unit": "bytes", "meaning": "host total system memory"},
    "mem_fragmentation_ratio": {"kind": "gauge", "unit": "dimensionless ratio", "meaning": "RSS/used-memory ratio; includes overhead beyond fragmentation"},
    "mem_fragmentation_bytes": {"kind": "gauge", "unit": "bytes", "meaning": "RSS minus allocator-used memory"},
    "maxmemory": {"kind": "configuration", "unit": "undeclared in field description", "meaning": "configured maxmemory directive"},
    "lru_clock": {"kind": "internal_clock", "unit": "ticks (per-minute increment; not a duration)", "meaning": "internal Redis LRU clock", "candidate_ranking_eligible": False},
    "instantaneous_ops_per_sec": {"kind": "rate", "unit": "operations/second", "meaning": "instantaneous command rate"},
    "total_commands_processed": {"kind": "counter", "unit": "commands", "meaning": "cumulative commands processed"},
    "total_connections_received": {"kind": "counter", "unit": "connections", "meaning": "cumulative accepted connections"},
    "total_net_input_bytes": {"kind": "counter", "unit": "bytes", "meaning": "cumulative bytes read from network"},
    "total_net_output_bytes": {"kind": "counter", "unit": "bytes", "meaning": "cumulative bytes written to network"},
    "instantaneous_input_kbps": {"kind": "rate", "unit": "KB/second", "meaning": "instantaneous network read rate"},
    "instantaneous_output_kbps": {"kind": "rate", "unit": "KB/second", "meaning": "instantaneous network write rate"},
    "evicted_keys": {"kind": "counter", "unit": "keys", "meaning": "cumulative keys evicted due to maxmemory"},
    "expired_keys": {"kind": "counter", "unit": "keys", "meaning": "cumulative key expiration events"},
    "keyspace_hits": {"kind": "counter", "unit": "lookups", "meaning": "successful lookups"},
    "keyspace_misses": {"kind": "counter", "unit": "lookups", "meaning": "failed lookups"},
    "rejected_connections": {"kind": "counter", "unit": "connections", "meaning": "cumulative connections rejected by maxclients"},
    "rdb_changes_since_last_save": {"kind": "resettable_counter", "unit": "changes", "meaning": "changes since last RDB save; reset boundary is save"},
    "used_cpu_sys": {"kind": "counter", "unit": "not declared by field description", "meaning": "cumulative Redis system CPU consumed"},
    "used_cpu_user": {"kind": "counter", "unit": "not declared by field description", "meaning": "cumulative Redis user CPU consumed"},
    "used_cpu_sys_children": {"kind": "counter", "unit": "not declared by field description", "meaning": "cumulative child system CPU consumed"},
    "used_cpu_user_children": {"kind": "counter", "unit": "not declared by field description", "meaning": "cumulative child user CPU consumed"},
    "uptime_in_seconds": {"kind": "elapsed_since_start", "unit": "seconds", "meaning": "seconds since Redis server start; resets on restart"},
    "latest_fork_usec": {"kind": "event_measurement", "unit": "microseconds", "meaning": "duration of latest fork; event-specific retained value", "candidate_ranking_eligible": False},
    "blocked_clients": {"kind": "gauge", "unit": "clients", "meaning": "current clients waiting on blocking calls"},
    "connected_clients": {"kind": "gauge", "unit": "clients", "meaning": "current client connections excluding replicas"},
    "connected_slaves": {"kind": "gauge", "unit": "replicas", "meaning": "current connected replicas"},
    "pubsub_channels": {"kind": "gauge", "unit": "channels", "meaning": "current subscribed pub/sub channels"},
    "aof_enabled": {"kind": "state", "unit": "boolean-like", "meaning": "AOF enablement flag"},
    "aof_rewrite_in_progress": {"kind": "state", "unit": "boolean-like", "meaning": "AOF rewrite state flag"},
    "aof_rewrite_scheduled": {"kind": "state", "unit": "boolean-like", "meaning": "AOF rewrite scheduled flag"},
    "rdb_bgsave_in_progress": {"kind": "state", "unit": "boolean-like", "meaning": "RDB save state flag"},
    "loading": {"kind": "state", "unit": "boolean-like", "meaning": "loading state flag"},
    "redis_git_dirty": {"kind": "build_metadata", "unit": "boolean-like", "meaning": "build dirty flag; not a runtime fault metric"},
}

_BMC_EXACT_PARAMETER_HINTS: Mapping[str, Mapping[str, str]] = {
    "CPULoad": {"kind": "gauge", "unit": "load factor (1-minute run-queue average)",
        "meaning": "run-queue load, not CPU utilization", "direction": "two_sided"},
    "CPUCpuUtil": {"kind": "gauge", "unit": "percent (0-100)", "meaning": "CPU time utilization", "direction": "two_sided"},
    "CPUSysTime": {"kind": "gauge", "unit": "percent (0-100)", "meaning": "CPU time in system mode", "direction": "two_sided"},
    "CPUUserTime": {"kind": "gauge", "unit": "percent (0-100)", "meaning": "CPU time in user mode", "direction": "two_sided"},
    "CPUWio": {"kind": "gauge", "unit": "undeclared for this source key", "meaning": "working-I/O CPU parameter; exact scale not confirmed", "direction": "two_sided"},
    "MEMUsedMemPerc": {"kind": "gauge", "unit": "percent (0-100)", "meaning": "percentage of available memory used", "direction": "two_sided"},
    "MEMFreeMem": {"kind": "gauge", "unit": "platform/source variant dependent", "meaning": "free/available memory; exact Linux collector unit unpinned", "direction": "two_sided"},
    "MEMUsedMem": {"kind": "gauge", "unit": "platform/source variant dependent", "meaning": "used memory; exact Linux collector unit unpinned", "direction": "two_sided"},
    "MEMTotalMem": {"kind": "capacity", "unit": "platform/source variant dependent", "meaning": "total assigned memory", "direction": "not a default fault direction"},
    "DSKPercentBusy": {"kind": "gauge", "unit": "percent-like; source mapping unpinned", "meaning": "disk busy time", "direction": "two_sided"},
    "DSKBps": {"kind": "ambiguous_rate_or_interval_quantity", "unit": "not confirmed", "meaning": "BMC parameter exists; exact quantity/time aggregation unavailable in retrieved doc", "direction": "unknown"},
    "FSCapacity": {"kind": "gauge", "unit": "percent (0-100)", "meaning": "filesystem storage percentage in use", "direction": "higher_is_worse"},
    "FSInodeUsedPercent": {"kind": "gauge", "unit": "percent-like; Bank scale unpinned", "meaning": "percentage of filesystem inodes used", "direction": "higher_is_worse"},
    "FSAvailableSpace": {"kind": "gauge", "unit": "collector/platform-dependent", "meaning": "filesystem space available", "direction": "lower_is_worse"},
    "FSUsedSpace": {"kind": "gauge", "unit": "collector/platform-dependent", "meaning": "filesystem space in use", "direction": "two_sided"},
    "NETInErrPrc": {"kind": "gauge_candidate", "unit": "percent-like; denominator unpinned", "meaning": "network-interface input error percentage candidate; not request latency or packet-loss rate", "direction": "unknown"},
    "NETOutErrPrc": {"kind": "gauge_candidate", "unit": "percent-like; denominator unpinned", "meaning": "network-interface output error percentage candidate; not request latency or packet-loss rate", "direction": "unknown"},
    "NETPacketsIn": {"kind": "counter_candidate", "unit": "packets candidate", "meaning": "network interface input packet count; cumulative/reset semantics need source confirmation", "direction": "unknown"},
    "NETPacketsOut": {"kind": "counter_candidate", "unit": "packets candidate", "meaning": "network interface output packet count; cumulative/reset semantics need source confirmation", "direction": "unknown"},
    "NETBytesTotalPerSec": {"kind": "rate_candidate", "unit": "bytes/second candidate", "meaning": "BMC parameter name declares rate; exact producer/version mapping is not pinned", "direction": "two_sided"},
}

_DOCKER_STATS_NAME_HINTS: Mapping[str, Mapping[str, str]] = {
    "CpuPercent": {"kind": "gauge", "unit": "percent-like", "meaning": "Docker stats CPU percentage"},
    "MemPercent": {"kind": "gauge", "unit": "percent-like", "meaning": "Docker stats memory percentage"},
    "MemUsage": {"kind": "gauge", "unit": "ambiguous in scalar Bank value", "meaning": "Docker stats column is used/limit pair; scalar projection not documented"},
    "MemLimit": {"kind": "capacity", "unit": "likely bytes but not established by Bank metadata", "meaning": "Docker memory limit candidate"},
    "NetworkRxBytes": {"kind": "counter_candidate", "unit": "bytes candidate", "meaning": "resembles Docker Engine network rx_bytes field; Bank adapter/collector mapping not pinned"},
    "NetworkTxBytes": {"kind": "counter_candidate", "unit": "bytes candidate", "meaning": "resembles Docker Engine network tx_bytes field; Bank adapter/collector mapping not pinned"},
}


def _semantic_name_evidence(field_name: str) -> dict[str, Any]:
    import re
    redis = re.fullmatch(r"redis-Redis_[0-9]+_Redis\s+\(([^()]*)\)", field_name)
    if redis:
        info_key = redis.group(1)
        info = _REDIS_INFO_SEMANTICS.get(info_key)
        return {"family": "redis_info_exact_key_candidate", "producer_match": "exact field-key match; producer/version linkage is absent from input manifest",
            "key": info_key, "documented_candidate": dict(info) if info else None,
            "candidate_ranking_eligible": False if info is None else info.get(
                "candidate_ranking_eligible", info["kind"] in {"gauge", "rate"}),
            "direction_policy": "two_sided" if info and info["kind"] in {"gauge", "rate"} else "unknown; counters require validated interval transform",
            "required_transform": ("counter_rate_with_declared_reset_policy" if info and info["kind"] in {"counter", "resettable_counter"} else "none"),
            "default_status": "working_match_only; requires review before policy promotion"}
    bmc_suffix = field_name.rsplit("_", 1)[-1]
    if field_name.startswith("OSLinux-") and bmc_suffix in _BMC_EXACT_PARAMETER_HINTS:
        return {"family": "bmc_patrol_parameter_name_candidate", "producer_match": "exact parameter-name suffix; OpenRCA input does not pin BMC producer/version",
            "key": bmc_suffix, "documented_candidate": dict(_BMC_EXACT_PARAMETER_HINTS[bmc_suffix]),
            "candidate_ranking_eligible": bmc_suffix in {"CPULoad", "CPUCpuUtil", "CPUSysTime", "CPUUserTime", "CPUWio", "MEMUsedMemPerc", "MEMFreeMem", "MEMUsedMem", "DSKPercentBusy", "FSCapacity", "FSInodeUsedPercent", "FSAvailableSpace", "FSUsedSpace", "NETOutErrPrc"},
            "direction_policy": _BMC_EXACT_PARAMETER_HINTS[bmc_suffix].get("direction", "unknown"),
            "default_status": "working_match_only; source mapping and per-field unit must remain explicit"}
    docker_match = re.fullmatch(r"Container-DOCKER_CONTAINER_[0-9a-fA-F]+--.+_([^_]+)", field_name)
    if docker_match and docker_match.group(1) in _DOCKER_STATS_NAME_HINTS:
        suffix = docker_match.group(1)
        return {"family": "docker_stats_name_candidate", "producer_match": "documented concept/name resemblance; exact collector mapping is not pinned",
            "key": suffix, "documented_candidate": dict(_DOCKER_STATS_NAME_HINTS[suffix]),
            "candidate_ranking_eligible": False,
            "direction_policy": "two_sided or unknown until data semantics are pinned",
            "default_status": "working hint only; especially do not rate-transform network bytes until counter lineage is confirmed"}
    if field_name.startswith("JVM-") and field_name.rsplit("_", 1)[-1] in {"HeapMemoryUsage", "HeapMemoryUsed", "NoHeapMemoryUsed"}:
        key = field_name.rsplit("_", 1)[-1]
        return {"family": "java_mxbean_name_candidate", "producer_match": "exact or similar property label; exporter flattening and units are not pinned",
            "key": key, "documented_candidate": {"kind": "gauge" if key != "HeapMemoryUsage" else "ambiguous_composite",
                "unit": "undeclared", "meaning": "current JVM heap/nonheap memory usage candidate",
                "direction": "two_sided"},
            "candidate_ranking_eligible": False,
            "direction_policy": "two_sided only if scalar transform is verified; otherwise unknown",
            "default_status": "working hint only; CompositeData flattening must be documented before promotion"}
    return {"family": "no exact documented match", "producer_match": "none",
        "key": None, "documented_candidate": None, "candidate_ranking_eligible": False,
        "direction_policy": "unknown",
        "default_status": "unknown; no substring fallback"}


class CoverageAuditError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _seq_shape(values: Sequence[tuple[int, float]]) -> str:
    if len(values) < 2:
        return "insufficient_points"
    ordered = [value for _time, value in sorted(values)]
    deltas = [right - left for left, right in zip(ordered, ordered[1:])]
    if all(delta == 0 for delta in deltas):
        return "constant"
    if all(delta >= 0 for delta in deltas):
        return "nondecreasing"
    if all(delta <= 0 for delta in deltas):
        return "nonincreasing"
    return "mixed"


def _summary(values: Sequence[int]) -> dict[str, int | float | None]:
    if not values:
        return {"minimum": None, "median": None, "maximum": None}
    return {"minimum": min(values), "median": float(median(values)), "maximum": max(values)}


def _field_inventory(rows: Mapping[str, Mapping[str, Any]], metric_policy,
                     expected_cases: Sequence[str]) -> dict[str, Any]:
    result = []
    total_pairs = 0
    for field_name in sorted(rows):
        field = rows[field_name]
        entities = field["entities"]
        entity_rows = []
        full_pairs = 0
        window_supports: dict[str, list[int]] = {"reference": [], "alert": []}
        for entity_id, cases in sorted(entities.items(), key=lambda item: (item[0].casefold(), item[0])):
            pair_case_rows = []
            for case_id, windows in sorted(cases.items()):
                ref = windows.get("reference", {"rows": 0, "valid": 0, "timestamps": {}, "values": []})
                alert = windows.get("alert", {"rows": 0, "valid": 0, "timestamps": {}, "values": []})
                ref_support, alert_support = len(ref["timestamps"]), len(alert["timestamps"])
                window_supports["reference"].append(ref_support)
                window_supports["alert"].append(alert_support)
                paired = ref_support > 0 and alert_support > 0
                full_pairs += int(paired)
                pair_case_rows.append({"development_case_id": case_id,
                    "reference": {"rows": ref["rows"], "finite_values": ref["valid"],
                        "unique_timestamps": ref_support,
                        "nonempty_independent_blocks_by_minutes": {str(k): len(v) for k, v in ref.get("block_ids", {}).items()},
                        "temporal_shape": _seq_shape([(t, float(median(v))) for t, v in ref["timestamps"].items()])},
                    "alert": {"rows": alert["rows"], "finite_values": alert["valid"],
                        "unique_timestamps": alert_support,
                        "nonempty_independent_blocks_by_minutes": {str(k): len(v) for k, v in alert.get("block_ids", {}).items()},
                        "temporal_shape": _seq_shape([(t, float(median(v))) for t, v in alert["timestamps"].items()])},
                    "both_windows_present": paired})
            entity_rows.append({"component_id": entity_id, "development_cases": pair_case_rows})
            total_pairs += len(cases)
        if callable(getattr(metric_policy, "classify", None)):
            semantics = metric_policy.classify("bank/metric_container.csv", field_name).to_dict()
        else:
            semantics = dict(metric_policy.get(field_name, {})) or {
                "kind": "unknown", "unit": "undeclared", "ranking_eligible": False,
                "provenance": "no exact source-field override"}
        result.append({"source_table": "bank/metric_container.csv", "field_name": field_name,
            "development_case_count": len(field["cases"]),
            "components_exposed": len(entities), "rows": field["rows"],
            "finite_numeric_rows": field["finite"],
            "missing_value_rows": field["missing"],
            "nonnumeric_or_nonfinite_value_rows": field["invalid"],
            "observed_native_value_range": {"minimum": field["min"], "maximum": field["max"]},
            "reference_unique_timestamp_support_across_observed_component_case_pairs": _summary(window_supports["reference"]),
            "alert_unique_timestamp_support_across_observed_component_case_pairs": _summary(window_supports["alert"]),
            "component_case_pairs_with_both_windows": full_pairs,
            "component_case_pairs_observed": sum(len(cases) for cases in entities.values()),
            "current_policy": semantics,
            "label_blind_external_semantic_candidate": _semantic_name_evidence(field_name),
            "observed_temporal_shape_counts": dict(sorted(field["shape_counts"].items())),
            "per_component_coverage": entity_rows})
    return {"fields": result, "component_case_pair_count": total_pairs,
        "distinct_field_count": len(result),
        "current_rankable_fields_observed": sum(1 for field in result
            if field["current_policy"].get("ranking_eligible") is True),
        "current_allowlist_fields_observed": sum(1 for field in result
            if field["current_policy"].get("ranking_eligible") is True),
        "observed_unrankable_fields": sum(1 for field in result
            if field["current_policy"].get("ranking_eligible") is not True),
        "exact_allowlisted_fields_not_observed_in_development": sorted(
            set(metric_policy.exact_rankable_fields("bank/metric_container.csv")
                if callable(getattr(metric_policy, "exact_rankable_fields", None))
                else metric_policy) - set(rows)),
        "scope_note": "The fixed development clips define this denominator; it is not the full-source Bank field universe."}


def _hypothesis_resolution(coverage: Mapping[str, Any], *,
                           minimum_timestamps: int, minimum_blocks: int,
                           block_minutes: int, resamples: int,
                           family_alpha: float = 0.05,
                           scope: str = "per-case global native family") -> dict[str, Any]:
    per_case = {}
    case_ids = sorted({row["development_case_id"]
        for field in coverage["fields"] for component in field["per_component_coverage"]
        for row in component["development_cases"]})
    for case_id in case_ids:
        eligible_pairs = all_pairs = 0
        exact_minima = []
        support_pairs = []
        for field in coverage["fields"]:
            current = field["current_policy"].get("ranking_eligible") is True and \
                field["current_policy"].get("kind") in {"gauge", "rate"}
            for component in field["per_component_coverage"]:
                row = next((item for item in component["development_cases"]
                    if item["development_case_id"] == case_id), None)
                if row is None:
                    continue
                n_ref = row["reference"]["nonempty_independent_blocks_by_minutes"].get(str(block_minutes), 0)
                n_alert = row["alert"]["nonempty_independent_blocks_by_minutes"].get(str(block_minutes), 0)
                support_ok = (min(row["reference"]["unique_timestamps"],
                    row["alert"]["unique_timestamps"]) >= minimum_timestamps and
                    min(n_ref, n_alert) >= minimum_blocks)
                if not support_ok:
                    continue
                all_pairs += 1
                if current:
                    eligible_pairs += 1
                support_pairs.append((n_ref, n_alert))
                if n_ref and n_alert:
                    # For two-sided exhaustive label permutations, both the
                    # observed allocation and its complement are extreme.
                    from math import comb
                    exact_minima.append(min(1.0, 2.0 / comb(n_ref + n_alert, n_ref)))
        current_m = eligible_pairs * 2  # median and tail-quantile hypotheses
        upper_m = all_pairs * 2
        p_floor = 1.0 / (resamples + 1)
        def correction(m: int) -> dict[str, Any]:
            cutoff = family_alpha / m if m else None
            required_resamples = (math.ceil(m / family_alpha) - 1) if m else None
            max_blocks = max((a + b for a, b in support_pairs), default=0)
            return {"hypotheses": m, "bonferroni_cutoff_at_family_alpha": cutoff,
                "monte_carlo_resamples": resamples,
                "monte_carlo_minimum_p_value": p_floor,
                "bonferroni_attainable_at_current_resamples": bool(cutoff is not None and p_floor <= cutoff),
                "minimum_add_one_resamples_for_bonferroni_resolution": required_resamples,
                "max_block_samples_in_any_test": max_blocks,
                "rough_center_evaluations_per_monte_carlo_run": m * resamples * max_blocks}
        per_case[case_id] = {"eligible_component_field_pairs_before_effect_or_p_selection": eligible_pairs,
            "upper_bound_component_field_pairs_if_every_dev_field_were_classified": all_pairs,
            "native_statistics_per_pair": ["median", "tail_quantile"],
            "current_policy_family_resolution": correction(current_m),
            "all_dev_fields_upper_bound_resolution": correction(upper_m),
            "exact_two_sample_permutation_minimum_p_range_if_exhaustively_enumerated":
                {"best_resolution": min(exact_minima) if exact_minima else None,
                 "worst_resolution": max(exact_minima) if exact_minima else None,
                 "interpretation": "grid resolution varies with support; this does not validate exchangeability"}}
    return {"scope": f"{scope} across eligible component x exact field x {{median, tail_quantile}}, counted after source-kind and support gates but before observed-effect/p-value gates",
        "family_alpha_for_resolution_audit": family_alpha, "block_minutes": block_minutes,
        "minimum_timestamps_per_window": minimum_timestamps,
        "minimum_independent_blocks_per_window": minimum_blocks,
        "resamples": resamples, "cases": per_case,
        "correction_design_status": "planning comparison only; no correction behavior is enabled by this audit",
        "candidate_correction": "joint block-label permutation max-statistic across the declared family may retain Monte-Carlo resolution if shared exchangeability is justified; otherwise Holm/Bonferroni cutoffs require the listed much larger resample counts. A runtime p-value floor below 1/(R+1) is unreachable."}


def _paper_backend_family_resolution(coverage: Mapping[str, Any], *,
                                     block_minutes: int, minimum_samples: int,
                                     resamples: int, alpha: float = 0.05) -> dict[str, Any]:
    """Audit the reconstructed paper metric scorer's per-candidate field family."""
    from math import comb
    per_case: dict[str, Any] = {}
    case_ids = sorted({row["development_case_id"]
        for field in coverage["fields"] for component in field["per_component_coverage"]
        for row in component["development_cases"]})
    max_family_size = 0
    all_exact_floors: list[float] = []
    for case_id in case_ids:
        candidate_rows: dict[str, list[tuple[int, int]]] = defaultdict(list)
        for field in coverage["fields"]:
            semantics = field["current_policy"]
            if semantics.get("ranking_eligible") is not True or semantics.get("kind") not in {"gauge", "rate"}:
                continue
            for component in field["per_component_coverage"]:
                observed = next((row for row in component["development_cases"]
                    if row["development_case_id"] == case_id), None)
                if observed is None:
                    continue
                n_ref = observed["reference"]["nonempty_independent_blocks_by_minutes"].get(str(block_minutes), 0)
                n_alert = observed["alert"]["nonempty_independent_blocks_by_minutes"].get(str(block_minutes), 0)
                if min(n_ref, n_alert) >= minimum_samples:
                    candidate_rows[component["component_id"]].append((n_ref, n_alert))
        family_sizes = {component: len(rows) for component, rows in candidate_rows.items()}
        case_max = max(family_sizes.values(), default=0)
        max_family_size = max(max_family_size, case_max)
        exact_counts = [comb(n_ref + n_alert, n_ref)
            for rows in candidate_rows.values() for n_ref, n_alert in rows]
        exact_floors = [min(1.0, 2.0 / count) for count in exact_counts]
        actual_p_floors = [floor if count <= resamples else 1.0 / (resamples + 1)
            for count, floor in zip(exact_counts, exact_floors)]
        all_exact_floors.extend(exact_floors)
        cutoff = alpha / case_max if case_max else None
        per_case[case_id] = {"support_qualified_fields_by_component": family_sizes,
            "maximum_per_candidate_field_family": case_max,
            "support_qualified_field_test_count": len(actual_p_floors),
            "bonferroni_cutoff_at_default_alpha": cutoff,
            "default_monte_carlo_resamples": resamples,
            "default_add_one_p_floor": 1.0 / (resamples + 1),
            "actual_test_resolution_p_floor_range": {"minimum": min(actual_p_floors) if actual_p_floors else None,
                "maximum": max(actual_p_floors) if actual_p_floors else None},
            "support_qualified_tests_reaching_cutoff": (sum(floor <= cutoff for floor in actual_p_floors)
                if cutoff is not None else 0),
            "all_support_qualified_tests_can_reach_cutoff": bool(actual_p_floors) and
                cutoff is not None and all(floor <= cutoff for floor in actual_p_floors),
            "exact_two_sided_p_floor_range": {"minimum": min(exact_floors) if exact_floors else None,
                "maximum": max(exact_floors) if exact_floors else None,
                "interpretation": "finite exchangeability grid diagnostic only; not proof of exchangeability"}}
    return {"scope": "paper metric scorer, one selected scorer per DAG node; Bonferroni among support-qualified fields for each candidate, no across-candidate family correction",
        "analysis_block_minutes": block_minutes, "default_minimum_samples_per_window": minimum_samples,
        "default_resamples": resamples, "default_alpha": alpha,
        "maximum_observed_per_candidate_family": max_family_size,
        "exact_two_sided_p_floor_range_across_support_qualified_tests": {
            "minimum": min(all_exact_floors) if all_exact_floors else None,
            "maximum": max(all_exact_floors) if all_exact_floors else None},
        "cases": per_case,
        "note": "PaperStarter is a reconstructed method with runtime parameters that may override these registry defaults. Families are sized before effect and p-value selection."}


def _benchmark_native_permutation_cost(resolution: Mapping[str, Any], *,
                                      resamples: int = 499, repeats: int = 25) -> dict[str, Any]:
    """Small label-free call-cost estimate; not a diagnosis or evidence test."""
    from e4core.numerics import permutation_contrast
    before = [1., 2., 3., 4., 5., 6.]
    after = [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]
    started = time.perf_counter()
    for index in range(repeats):
        permutation_contrast(before, after, seed=index, resamples=resamples,
            statistic="median", minimum_blocks=4, relative_scale_floor=0.05,
            absolute_scale_floor=1e-9, quantile=0.9)
    elapsed = time.perf_counter() - started
    per_test = elapsed / repeats
    case_rows = list(resolution["cases"].values())
    current_m = max(row["current_policy_family_resolution"]["hypotheses"] for row in case_rows)
    upper_m = max(row["all_dev_fields_upper_bound_resolution"]["hypotheses"] for row in case_rows)
    return {"fixture": "synthetic six-block before/after arrays; no telemetry or labels",
        "resamples": resamples, "repeats": repeats,
        "wall_seconds_total": elapsed, "wall_seconds_per_test": per_test,
        "current_policy_max_family_hypotheses": current_m,
        "projected_seconds_current_policy_max_family": per_test * current_m,
        "all_dev_fields_upper_bound_hypotheses": upper_m,
        "projected_seconds_all_dev_fields_upper_bound": per_test * upper_m,
        "projection_note": "linear extrapolation of the existing per-series implementation; joint vectorized max-stat code may differ and needs sandbox benchmarking"}


def build_inventory(root: Path = ROOT) -> dict[str, Any]:
    from e4core.bank_adapter import load_bank_case
    from e4core.bank_ops import bank_registry
    from e4core.m4_operator_ir_worker import KERNELS
    from e4core.paper_starter import make_paper_starter_registry

    root = Path(root).resolve(strict=True)
    sample_path = root / "config/bank_sample.json"
    policy_path = root / "config/bank_metric_kind_policy.json"
    input_manifest_path = root / "config/bank_input_manifest.json"
    dev_manifest_path = root / "data/prepared/bank/dev_incidents.jsonl"
    m4_runtime_path = root / "config/m4_runtime.json"
    core_runtime_path = root / "config/core_runtime.json"
    paper_runtime_path = root / "config/paper_starter_runtime.json"
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    m4_runtime = json.loads(m4_runtime_path.read_text(encoding="utf-8"))
    core_runtime = json.loads(core_runtime_path.read_text(encoding="utf-8"))
    paper_runtime = json.loads(paper_runtime_path.read_text(encoding="utf-8"))
    dev_cases = [row["incident_id"] for row in sample.get("development_cases", [])]
    if not dev_cases:
        raise CoverageAuditError("frozen development sample has no cases")
    if policy.get("schema_version") != "e4-bank-metric-kind-policy/1":
        raise CoverageAuditError("unsupported Bank metric policy schema")
    allowlist = policy.get("source_semantics", {}).get("metric_container", {}).get("ranking_allowlist", {})
    if not isinstance(allowlist, Mapping):
        raise CoverageAuditError("metric container allowlist is malformed")
    metric_policy = load_bank_metric_policy(policy_path)

    block_minutes_choices = (1, 2, 3, 5, 6, 10)
    window_stats = lambda: {"rows": 0, "valid": 0, "timestamps": defaultdict(list),
        "block_ids": {k: set() for k in block_minutes_choices}}
    container: dict[str, Any] = defaultdict(lambda: {"entities": defaultdict(lambda: defaultdict(
        lambda: defaultdict(window_stats))),
        "cases": set(), "rows": 0, "finite": 0, "min": None, "max": None,
        "missing": 0, "invalid": 0, "shape_counts": Counter()})
    app: dict[str, Any] = defaultdict(lambda: {"entities": defaultdict(lambda: defaultdict(
        lambda: defaultdict(window_stats))),
        "rows": 0, "finite": 0})
    log_derived: dict[str, Any] = {"case_summaries": [], "derived_minute_points_by_log_name": Counter(),
        "component_log_coverage": defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(
            lambda: {"minute_points": 0, "nonzero_minutes": 0, "zero_minutes": 0,
                "source_rows_counted": 0})))),
        "timestamp_unit": "seconds (source timestamp); derived buckets are fixed one-minute intervals",
        "kind": "event_count", "value_semantics": "raw log value is not parsed; counts include all log rows"}
    trace: dict[str, Any] = {"rows": 0, "finite_nonnegative_durations": 0,
        "invalid_or_negative_duration_rows": 0, "distinct_trace_ids": set(),
        "distinct_components": set(), "resolved_parent_rows": 0,
        "per_component_case_window_coverage": defaultdict(lambda: defaultdict(lambda: defaultdict(
            lambda: {"span_rows": 0, "finite_nonnegative_duration_rows": 0,
                "invalid_or_negative_duration_rows": 0, "trace_ids": set(),
                "resolved_parent_rows": 0}))),
        "success_field": "not present in schema", "duration_unit": "undeclared",
        "source_timestamp_unit": "milliseconds"}
    case_records = []

    for case_id in dev_cases:
        case = load_bank_case(case_id, split="dev", root=root)
        telemetry = case.telemetry
        case_records.append({"development_case_id": case_id,
            "public_data_sha256": telemetry.data_sha256,
            "source_file_sha256": dict(sorted(telemetry.file_sha256.items())),
            "upstream_timestamp_units": dict(case.provenance["upstream_declared_timestamp_units"]),
            "empirical_source_file_timestamp_units": dict(case.provenance["empirical_timestamp_units_by_source_file"]),
            "quality_counts": dict(sorted(telemetry.quality_counts.items())),
            "duplicate_counts": dict(sorted(telemetry.duplicate_counts.items()))})
        for point in telemetry.metrics:
            if point.table == "bank/metric_container.csv":
                field = container[point.metric_name]
                entity = point.entity.identifier
                field["cases"].add(case_id)
                field["rows"] += 1
                pair = field["entities"][entity][case_id]
                window = "reference" if telemetry.reference_window.contains(point.timestamp_ms_utc) else "alert"
                window_obj = telemetry.reference_window if window == "reference" else telemetry.alert_window
                pair[window]["rows"] += 1
                if point.value is not None and math.isfinite(float(point.value)):
                    value = float(point.value)
                    field["finite"] += 1
                    pair[window]["valid"] += 1
                    pair[window]["timestamps"][point.timestamp_ms_utc].append(value)
                    for minutes in block_minutes_choices:
                        block_index = (point.timestamp_ms_utc - window_obj.start_ms_utc) // (minutes * 60_000)
                        pair[window]["block_ids"][minutes].add(int(block_index))
                    field["min"] = value if field["min"] is None else min(field["min"], value)
                    field["max"] = value if field["max"] is None else max(field["max"], value)
                elif not point.raw_value.strip():
                    field["missing"] += 1
                else:
                    field["invalid"] += 1
            elif point.table == "bank/metric_app.csv":
                field = app[point.metric_name]
                entity = point.entity.identifier
                field["rows"] += 1
                pair = field["entities"][entity][case_id]
                window = "reference" if telemetry.reference_window.contains(point.timestamp_ms_utc) else "alert"
                pair[window]["rows"] += 1
                if point.value is not None and math.isfinite(float(point.value)):
                    field["finite"] += 1
                    pair[window]["valid"] += 1
                    pair[window]["timestamps"][point.timestamp_ms_utc].append(float(point.value))
            elif point.table == "bank/log/log_service.csv":
                log_derived["derived_minute_points_by_log_name"][point.item_id or ""] += 1
                component = point.entity.identifier
                log_name = point.item_id or ""
                window = "reference" if telemetry.reference_window.contains(point.timestamp_ms_utc) else "alert"
                log_pair = log_derived["component_log_coverage"][component][log_name][case_id][window]
                log_pair["minute_points"] += 1
                count = float(point.value) if point.value is not None else 0.0
                log_pair["source_rows_counted"] += int(count)
                if count > 0:
                    log_pair["nonzero_minutes"] += 1
                else:
                    log_pair["zero_minutes"] += 1
        for span in telemetry.spans:
            trace["rows"] += 1
            window = "reference" if telemetry.reference_window.contains(span.start_ms_utc) else "alert"
            component_id = span.executing_component.identifier if span.executing_component else ""
            span_pair = trace["per_component_case_window_coverage"][component_id][case_id][window]
            span_pair["span_rows"] += 1
            if span.trace_id:
                trace["distinct_trace_ids"].add(span.trace_id)
                span_pair["trace_ids"].add(span.trace_id)
            if span.executing_component is not None:
                trace["distinct_components"].add(span.executing_component.identifier)
            if span.parent_resolved_in_clip:
                trace["resolved_parent_rows"] += 1
                span_pair["resolved_parent_rows"] += 1
            try:
                duration = float(span.elapsed_raw)
                if math.isfinite(duration) and duration >= 0:
                    trace["finite_nonnegative_durations"] += 1
                    span_pair["finite_nonnegative_duration_rows"] += 1
                else:
                    trace["invalid_or_negative_duration_rows"] += 1
                    span_pair["invalid_or_negative_duration_rows"] += 1
            except (TypeError, ValueError, OverflowError):
                trace["invalid_or_negative_duration_rows"] += 1
                span_pair["invalid_or_negative_duration_rows"] += 1
        summary = dict(case.log_summary)
        log_derived["case_summaries"].append({"development_case_id": case_id,
            "raw_rows": summary.get("raw_rows"),
            "observed_nonzero_minute_groups": summary.get("observed_nonzero_minute_groups"),
            "projected_groups": summary.get("projected_groups"),
            "summary_truncated": summary.get("summary_truncated"),
            "summary_omitted_groups": summary.get("summary_omitted_groups")})

    for field in container.values():
        for cases in field["entities"].values():
            for windows in cases.values():
                for window, stats in windows.items():
                    series = [(timestamp, float(median(values)))
                        for timestamp, values in stats["timestamps"].items() if values]
                    field["shape_counts"][f"{window}:{_seq_shape(series)}"] += 1
    container_inventory = _field_inventory(container, metric_policy, dev_cases)
    m4_protected = m4_runtime.get("protected_runtime_values", {})
    m4_limits = m4_runtime.get("model_runtime_overrides", {})
    code_default_resamples = 999
    active_resamples = int(core_runtime.get("permutation_resamples", code_default_resamples))
    native_minimum_support = int(core_runtime.get("minimum_support", 12))
    native_minimum_blocks = int(core_runtime.get("minimum_independent_blocks", 4))
    native_block_minutes = int(core_runtime.get("block_minutes", 5))
    resolution = _hypothesis_resolution(container_inventory,
        minimum_timestamps=native_minimum_support,
        minimum_blocks=native_minimum_blocks, block_minutes=native_block_minutes,
        resamples=active_resamples,
        scope="native Bank scorer active core-runtime family")
    resolution["existing_implementation_cost_benchmark"] = _benchmark_native_permutation_cost(
        resolution, resamples=active_resamples)
    paper_minimum_samples = 3
    paper_resamples = 999
    paper_resolution = _paper_backend_family_resolution(container_inventory,
        block_minutes=int(paper_runtime.get("analysis_block_minutes", 1)),
        minimum_samples=paper_minimum_samples, resamples=paper_resamples)
    evidence_families = Counter(field["label_blind_external_semantic_candidate"]["family"]
        for field in container_inventory["fields"])
    app_fields = []
    for name, field in sorted(app.items()):
        app_fields.append({"source_table": "bank/metric_app.csv", "field_name": name,
            "kind": "unknown", "unit": "undeclared", "ranking_eligible": False,
            "working_interpretation": "mrt suggests a response-time-style KPI; rr/sr/cnt meanings are not specified by the native schema",
            "rows": field["rows"], "finite_numeric_rows": field["finite"],
            "observed_component_role": "app_kpi", "entities": sorted(field["entities"]),
            "per_entity_case_window_support": {entity: {case_id: {
                window: {"rows": bucket["rows"], "finite": bucket["valid"],
                    "unique_timestamps": len(bucket["timestamps"])}
                for window, bucket in windows.items()} for case_id, windows in cases.items()}
                for entity, cases in field["entities"].items()}})
    log_derived["derived_minute_points_by_log_name"] = dict(
        sorted(log_derived["derived_minute_points_by_log_name"].items()))
    log_derived["component_log_coverage"] = [
        {"component_id": component, "log_name": log_name,
         "development_cases": [{"development_case_id": case_id,
            "windows": {window: dict(stats) for window, stats in sorted(windows.items())}}
            for case_id, windows in sorted(cases.items())]}
        for component, names in sorted(log_derived["component_log_coverage"].items())
        for log_name, cases in sorted(names.items())]
    trace["per_component_case_window_coverage"] = [
        {"component_id": component,
         "development_cases": [{"development_case_id": case_id,
            **{window: {**{k: v for k, v in stats.items() if k != "trace_ids"},
                "unique_trace_ids": len(stats["trace_ids"])}
               for window, stats in sorted(windows.items())}}
            for case_id, windows in sorted(cases.items())]}
        for component, cases in sorted(trace["per_component_case_window_coverage"].items())]
    trace["distinct_trace_ids"] = len(trace["distinct_trace_ids"])
    trace["distinct_components"] = sorted(trace["distinct_components"])

    from e4core.bank_adapter import BANK_SOURCE_REVISION
    bank_ops = bank_registry()
    paper_ops = make_paper_starter_registry(bank_only=True)
    operators = {"bank_registry": [row["name"] for row in bank_ops.signatures()],
        "bank_only_paper_starter_registry": [row["name"] for row in paper_ops.signatures()],
        "typed_ir_kernel_ids": sorted(KERNELS)}
    field_union = set(container)
    current_allowlist = set(allowlist)
    selector_catalog = metric_policy.selector_catalog("bank/metric_container.csv")
    current_rule_refs = [entry.selector_ref for entry in selector_catalog
        if entry.selector_kind == "pattern"]
    return {"schema_version": "e4-bank-dev-metric-coverage-audit/2",
        "label_blind": True,
        "split": "dev",
        "development_case_count": len(dev_cases),
        "development_case_ids": list(dev_cases),
        "provenance": {"bank_source_revision": BANK_SOURCE_REVISION,
            "bank_input_manifest_sha256": _sha(input_manifest_path),
            "dev_manifest_sha256": _sha(dev_manifest_path),
            "metric_policy_sha256": _sha(policy_path),
            "sample_config_sha256": _sha(sample_path),
            "native_core_runtime_sha256": _sha(core_runtime_path),
            "m4_runtime_policy_sha256": _sha(m4_runtime_path),
            "paper_starter_runtime_sha256": _sha(paper_runtime_path)},
        "schemas": {"metric_container": ["timestamp", "cmdb_id", "kpi_name", "value"],
            "metric_app": ["timestamp", "rr", "sr", "cnt", "mrt", "tc"],
            "trace_span": ["timestamp", "cmdb_id", "parent_id", "span_id", "trace_id", "duration"],
            "log_service": ["log_id", "timestamp", "cmdb_id", "log_name", "value"]},
        "metric_container_coverage": {**container_inventory,
            "current_allowlist_count": len(current_allowlist),
            "current_rule_selector_count": len(current_rule_refs),
            "current_rule_selector_refs": current_rule_refs,
            "dev_union_count": len(field_union),
            "dev_allowlist_intersection_count": len(field_union & current_allowlist),
            "dev_rankable_field_count": container_inventory["current_rankable_fields_observed"],
            "claimed_full_source_universe_count": 465,
            "claim_status": "unverified_by_dev_only_inventory; a fixed two-case development union is not the full-source universe",
            "claimed_exclusions_count": 435,
            "claimed_exclusions_status": "not equal to observed dev-unlisted count; do not infer source-wide exclusion from this dev sample"},
        "external_document_name_match_counts": dict(sorted(evidence_families.items())),
        "scorer_runtime_and_multiple_testing": {"bank_native_scorer_code_defaults": {
                "permutation_resamples": code_default_resamples,
                "maximum_change_p_value": 0.1,
                "minimum_support": 4,
                "minimum_independent_blocks": 4,
                "block_minutes": 5},
            "native_core_runtime_active_values": {k: core_runtime.get(k) for k in
                ("minimum_support", "minimum_independent_blocks", "block_minutes",
                 "permutation_resamples", "maximum_change_p_value")},
            "m4_protected_runtime_values": {k: m4_protected.get(k) for k in
                ("permutation_resamples", "seed", "minimum_support", "minimum_independent_blocks",
                 "block_minutes", "maximum_change_p_value") if k in m4_protected},
            "m4_override_bounds": {k: m4_limits.get(k) for k in
                ("minimum_support", "minimum_independent_blocks", "block_minutes",
                 "maximum_change_p_value") if k in m4_limits},
            "paper_backend_parameter_defaults_from_registry_source": {
                "analysis_block_minutes": int(paper_runtime.get("analysis_block_minutes", 1)),
                "minimum_samples": paper_minimum_samples,
                "resamples": paper_resamples, "alpha": 0.05,
                "note": "paper_starter scorer parameters are separate from the Bank M4 protected runtime and may be explicitly overridden per analysis"},
            "execution_scope": "Trusted bank operators are invoked directly by SerialExecutor; the generated-IR worker CPU rlimit is not their per-operator cap.",
            "resolution_analysis": resolution,
            "paper_backend_family_resolution": paper_resolution},
        "other_sources": {"metric_app_fields": app_fields,
            "trace_span": trace, "log_service_row_count_derivation": log_derived},
        "case_provenance": case_records,
        "operator_and_kernel_inventory": operators,
        "semantic_rules_under_review": {
            "native_metric_container_default": "unknown/undeclared/not ranking-eligible until exact producer semantics are documented or exact field override carries provenance",
            "name_suffix": "may be recorded as a working name hint, but does not establish units, metric kind, reset behavior, or direction",
            "timestamp_unit": "source timestamp units are separate from metric value units",
            "counter_transform": "only after exact counter evidence; sort by timestamp, detect reset/wrap per declared reset rule, divide valid deltas by actual elapsed interval, and abstain across missing samples unless explicit policy allows a known interval",
            "log_values": "raw CSV value is unparsed and not a request-failure/error indicator; only bounded row-count-derived event measures may be classified as event_count"},
        "documentation_sources": {
            "prometheus_metric_types": "https://prometheus.io/docs/concepts/metric_types/",
            "prometheus_instrumentation": "https://prometheus.io/docs/practices/instrumentation/",
            "linux_procfs": "https://docs.kernel.org/filesystems/proc.html",
            "redis_info": "https://redis.io/docs/latest/commands/info/",
            "prometheus_node_exporter_examples": "https://prometheus.io/docs/guides/node-exporter/"}}


def markdown(inventory: Mapping[str, Any]) -> str:
    coverage = inventory["metric_container_coverage"]
    lines = ["# Bank metric coverage audit — development data only", "",
        "This audit reads only the frozen public development telemetry and its manifests. It does not read evaluator files or outcome labels. The current classifier policy is pinned in the JSON provenance. Two development incidents expose the inventory below; these results do not claim full-source coverage.", "",
        f"- Native container fields observed in development clips: **{coverage['dev_union_count']}**.",
        f"- Current exact allowlist entries: **{coverage['current_allowlist_count']}**; versioned family selectors: **{coverage['current_rule_selector_count']}**.",
        f"- Exact allowlist fields present in development clips: **{coverage['dev_allowlist_intersection_count']}**.",
        f"- Current rule-resolved rankable fields present in development clips: **{coverage['dev_rankable_field_count']}**.",
        f"- Unrankable fields observed in development clips: **{coverage['observed_unrankable_fields']}**.",
        f"- Exact allowlist fields absent from development clips: **{len(coverage['exact_allowlisted_fields_not_observed_in_development'])}**.",
        "- The proposal's 465/435 source-wide counts remain unverified by this development-only inventory.", "",
        "Label-blind documentation/name-match counts (including fields still unrankable): " + ", ".join(
            f"`{name}` {count}" for name, count in sorted(inventory["external_document_name_match_counts"].items())) + ".", "",
        "## Native metric field coverage", "",
        "| Exact `kpi_name` | Current kind / eligibility / direction | Candidate evidence / candidate direction | Entities | Dev cases | Rows | Finite / missing | Both windows (component-case pairs) | Value-unit status |",
        "|---|---|---|---:|---:|---:|---:|---:|---|"]
    for field in coverage["fields"]:
        current = field["current_policy"]
        candidate = field["label_blind_external_semantic_candidate"]
        evidence = (f"{candidate['family']} / {candidate['key']} / {candidate['direction_policy']}"
            if candidate["key"] is not None else f"{candidate['family']} / unknown")
        both = f"{field['component_case_pairs_with_both_windows']}/{field['component_case_pairs_observed']}"
        finite = f"{field['finite_numeric_rows']}/{field['rows']}"
        lines.append(f"| `{field['field_name']}` | {current.get('kind', 'unknown')} / {current.get('ranking_eligible', False)} / {current.get('direction', 'unknown')} | {evidence} | {field['components_exposed']} | {field['development_case_count']} | {field['rows']} | {finite} | {both} | {current.get('unit', 'undeclared')} |")
    resolution = inventory["scorer_runtime_and_multiple_testing"]["resolution_analysis"]
    lines.extend(["", "## Hypothesis family and attainable correction", "",
        resolution["scope"] + ". Support/source eligibility is counted first; no observed effect or p-value is used to size the family.", "",
        "| Development case | Eligible hypotheses | Bonferroni .05/M cutoff | Native core resamples / p floor | Exact two-sided grid floor range | Holm/Bonferroni resolution? | Resamples needed | All-dev-fields upper bound M |",
        "|---|---:|---:|---:|---:|---|---:|---:|"])
    for case_id, row in resolution["cases"].items():
        current = row["current_policy_family_resolution"]
        upper = row["all_dev_fields_upper_bound_resolution"]
        cutoff = current["bonferroni_cutoff_at_family_alpha"]
        exact_range = row["exact_two_sample_permutation_minimum_p_range_if_exhaustively_enumerated"]
        exact_pretty = ("unknown" if exact_range["best_resolution"] is None else
            f"{exact_range['best_resolution']:.6g}–{exact_range['worst_resolution']:.6g}")
        lines.append(f"| {case_id} | {current['hypotheses']} | {cutoff:.6g} | {current['monte_carlo_resamples']} / {current['monte_carlo_minimum_p_value']:.6g} | {exact_pretty} | {'yes' if current['bonferroni_attainable_at_current_resamples'] else 'no'} | {current['minimum_add_one_resamples_for_bonferroni_resolution']:,} | {upper['hypotheses']} |")
    benchmark = resolution["existing_implementation_cost_benchmark"]
    lines.extend(["", f"A label-free synthetic microbenchmark of the existing native permutation function took {benchmark['wall_seconds_per_test']:.4f}s per 6+6-block, {benchmark['resamples']}-resample test ({benchmark['repeats']} repeats). Linear projection is {benchmark['projected_seconds_current_policy_max_family']:.1f}s for the current max-M family and {benchmark['projected_seconds_all_dev_fields_upper_bound']:.1f}s for the all-dev-fields upper bound. This is a simple per-series extrapolation; a joint/vectorized correction may cost differently.", "",
        "For the native scorer, the family is global across currently eligible component × field × {median, tail-quantile} tests in each case; it is counted before effect or p-value filtering. The active core runtime has minimum timestamp support 12, four independent 5-minute blocks, 499 resamples, and a per-test p gate of 0.1. The helper fallback uses 999 draws, while M4 schema minimum 4 is only a legal lower bound and its runtime overrides vary; these values are not interchangeable. At 499 draws the add-one p floor is 0.002. Trusted native operators run directly inside SerialExecutor, so the generated-IR worker's 10-second CPU limit is not their per-operator limit; measured cost still needs reconciliation with case/runtime budgets. A joint block-label max-statistic is a candidate correction, not an approved change: it must define missingness and standardized cross-field statistics, establish temporal exchangeability, pass independent null controls, and fit measured runtime bounds. No correction behavior is changed by this audit.", ""])
    paper = inventory["scorer_runtime_and_multiple_testing"]["paper_backend_family_resolution"]
    lines.extend(["", "## PaperStarter metric-family resolution", "",
        paper["scope"] + ". These are registry defaults; each DAG node may override thresholds and selector refs.", "",
        f"The paper metric scorer uses {paper['analysis_block_minutes']}-minute blocks, default minimum samples {paper['default_minimum_samples_per_window']}, default resamples {paper['default_resamples']}, and alpha {paper['default_alpha']}. The largest support-qualified per-candidate field family in these dev clips is {paper['maximum_observed_per_candidate_family']}; exact two-sided permutation resolution varies with the number of blocks.", "",
        "| Development case | Maximum fields for one candidate | Per-field cutoff | Support-qualified field tests | Actual permutation p-floor range | Tests whose default resolution reaches cutoff |",
        "|---|---:|---:|---:|---:|---:|"])
    for case_id, row in paper["cases"].items():
        cutoff = row["bonferroni_cutoff_at_default_alpha"]
        floor_range = row["actual_test_resolution_p_floor_range"]
        pretty_range = ("unknown" if floor_range["minimum"] is None else
            f"{floor_range['minimum']:.6g}–{floor_range['maximum']:.6g}")
        lines.append(f"| {case_id} | {row['maximum_per_candidate_field_family']} | {cutoff:.6g} | {row['support_qualified_field_test_count']} | {pretty_range} | {row['support_qualified_tests_reaching_cutoff']} |")
    lines.append("")
    lines.extend(["", "## Interpretation limits", "",
        "The table reports observed coverage and the policy's current working interpretations. The Bank CSV schema declares field names and timestamps, but it does not provide metric-type, value-unit, counter-reset, wrap, or direction metadata. Two-window behavior is diagnostic only and cannot prove a counter or gauge. The 465-field claim is not estimated from the two-case development subset.", "",
        "Prometheus describes counters as cumulative values that can reset and gauges as values that may move either way; raw counters usually need a rate transform. That documentation does not establish that the Bank fields originate from Prometheus. Linux `/proc/meminfo` documents exact keys, but the Bank names do not establish exact source-field mappings. The dev clips include 33 exact Redis INFO property-key matches. Redis INFO describes current memory gauges, configuration fields, cumulative event/byte counters, elapsed-time values, instantaneous rates, and state flags. The input manifest does not pin the Redis exporter/version or transform, so these are working exact-key matches pending source linkage. Dev fields also match BMC PATROL parameter names: its docs identify CPULoad as run-queue load (not CPU utilization) and MEMUsedMemPerc as percent-used (0–100); no BMC producer/version link is present in the input manifest. Docker-shaped names are candidates only, since the source-to-field projection is unpinned. Raw log `value` is unparsed; a `NETOutErr`-style field is not response latency.", "",
        "## Reuse map", "",
        "- Starter operators include native Bank aggregation/contrast, paper backend mean/Welch and median/tail permutation contrasts, and trace-duration comparisons.",
        "- Typed IR already contains timestamped component series, reductions (mean/median/variance/q90), and window difference/relative-change/ratio. A basic level contrast is expressible; do not add a duplicate kernel.",
        "- No trusted native counter-difference/rate transform or threshold/exceedance primitive appears in the catalog. Add counter handling only for fields with proven counter semantics; otherwise classify as unknown.",
        "- Trace duration has timestamp support but the duration unit is undeclared. Trace outcome/success is absent. Raw log value is unparsed; `NETOutErr` alone does not define latency.", "",
        "## Preliminary classifier proposal — no behavior changed", "",
        "1. Resolve exact `(source_table, field_name)` and anchored producer grammars with rule IDs and source provenance. Do not match substrings across arbitrary names or use the component identifier as a semantic key.",
        "2. Keep the existing 30 exact allowlist entries behavior-identical initially. Evaluate separately reviewed candidates such as the Redis INFO envelope `redis-Redis_<port>_Redis  (<exact INFO key>)`, BMC PATROL's exact `OSLinux` parameter suffixes across disk/CPU instances, and the Docker container metric suffix grammar. Those are working name-to-document matches because the input manifest does not pin the telemetry exporter/version; they are not automatically verified producer lineage.",
        "3. Documented examples: exact Redis `used_memory_rss` is an allocator/RSS byte gauge; `total_commands_processed` is cumulative count; `instantaneous_ops_per_sec` is already a rate. BMC `CPULoad` is a 1-minute run-queue load factor (not CPU utilization), `CPUCpuUtil`/`CPUSysTime`/`CPUUserTime` are percent gauges, and `MEMUsedMemPerc` is 0–100 percent. A flattened JVM `HeapMemoryUsage` field remains ambiguous because the standard MemoryMXBean attribute is composite. Docker `NetworkRxBytes`/`NetworkTxBytes` are only a counter candidate until the Bank projection and reset behavior are pinned.",
        "4. Separate `kind`, `unit_status`, `direction`, transform, and ranking eligibility. Unknown absolute units need not block dimensionless within-field comparisons when gauge/counter semantics are justified, but native values from unrelated fields or units must not be combined as raw magnitude scores.",
        "5. Apply timestamp unit independently of value unit. `DSKBps`/`DSKRTps` suffixes do not establish rate semantics. Exact Redis counters and any later verified counters require ordered timestamp differencing, actual elapsed-interval normalization for rates, explicit source-reset/wrap policy, and abstention on missing or ambiguous intervals; never replace missing samples with zero.",
        "6. Keep default direction two-sided unless exact source documentation justifies a direction; still treat memory/CPU/disk values as symptoms, not standalone cause labels. A `NETOutErr` or `NETOutErrPrc` field is interface errors, not latency.",
        "7. Represent the one-minute log row count as an explicit derived `event_count` (`log_volume_count`, rows per one-minute bucket). Raw `log_service.value` and arbitrary log-name patterns stay unclassified; event-pattern summaries must carry denominators, coverage, and source hashes.",
        "8. Count the complete predeclared component×field×statistic family after documented eligibility/support gates but before observed effects/p-values. The current 499-draw floor cannot satisfy simple Holm/Bonferroni cutoffs at either observed family size; any max-stat or other correction must specify shared-block/missingness semantics, pass null controls, and meet measured runtime bounds before policy behavior changes.", "",
        "## Documentation used", "",
        "- [Prometheus metric types](https://prometheus.io/docs/concepts/metric_types/) and [instrumentation practice](https://prometheus.io/docs/practices/instrumentation/)",
        "- [Linux kernel `/proc` documentation](https://docs.kernel.org/filesystems/proc.html)",
        "- [Redis `INFO` fields](https://redis.io/docs/latest/commands/info/)",
        "- [Prometheus Node Exporter examples](https://prometheus.io/docs/guides/node-exporter/)",
        "- BMC PATROL [`CPULoad`](https://docs.helixops.ai/bin/IT-Operations-Management/Operations-Management/BMC-PATROL-for-UNIX-and-Linux/unixlinux913/Monitor-types-and-attributes/CPU-CPU/CPU-Load-CPULoad/), [`CPUCpuUtil`](https://docs.helixops.ai/bin/IT-Operations-Management/Operations-Management/BMC-PATROL-for-UNIX-and-Linux/unixlinux913/Monitor-types-and-attributes/CPU-CPU/CPU-Utilization-CPUCpuUtil/), [`CPUSysTime`](https://docs.helixops.ai/bin/IT-Operations-Management/Operations-Management/BMC-PATROL-for-UNIX-and-Linux/unixlinux913/Monitor-types-and-attributes/CPU-CPU/CPU-Utilization-In-System-Mode-CPUSysTime/), and [`MEMUsedMemPerc`](https://docs.helixops.ai/bin/IT-Operations-Management/Operations-Management/BMC-PATROL-for-UNIX-and-Linux/unixlinux913/Monitor-types-and-attributes/Memory-MEMORY/Percent-of-Memory-Pages-Used-MEMUsedMemPerc/)",
        "- [Docker container stats](https://docs.docker.com/reference/cli/docker/container/stats/) and [Docker Engine stats API](https://docs.docker.com/reference/api/engine/version/v1.40/)",
        "- [Oracle Java `MemoryMXBean`](https://docs.oracle.com/javase/8/docs/api/java/lang/management/MemoryMXBean.html)" ])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path, default=ROOT / "artifacts/bank_metric_coverage/dev_inventory.json")
    parser.add_argument("--output-markdown", type=Path, default=ROOT / "docs/BANK_METRIC_COVERAGE_AUDIT.md")
    args = parser.parse_args()
    inventory = build_inventory(ROOT)
    json_path = Path(args.output_json)
    markdown_path = Path(args.output_markdown)
    if not json_path.is_absolute():
        json_path = ROOT / json_path
    if not markdown_path.is_absolute():
        markdown_path = ROOT / markdown_path
    for path in (json_path, markdown_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(inventory, ensure_ascii=False, sort_keys=True,
        indent=2, allow_nan=False) + "\n", encoding="utf-8")
    markdown_path.write_text(markdown(inventory), encoding="utf-8")
    def display(path: Path) -> str:
        try:
            return path.resolve().relative_to(ROOT).as_posix()
        except ValueError:
            return str(path.resolve())
    print(json.dumps({"json_path": display(json_path),
        "markdown_path": display(markdown_path),
        "dev_cases": inventory["development_case_count"],
        "container_fields": inventory["metric_container_coverage"]["dev_union_count"],
        "allowlisted": inventory["metric_container_coverage"]["current_allowlist_count"],
        "eligible_observed": inventory["metric_container_coverage"]["dev_rankable_field_count"],
        "exact_allowlist_intersection": inventory["metric_container_coverage"]["dev_allowlist_intersection_count"]},
        sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
