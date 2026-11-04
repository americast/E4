"""Opt-in reconstructed paper-family starter operators and canonical DAGs.

The paper reports operator families and inventory counts, not its exact operator
identities or equations. This module implements a small, typed reconstruction
and keeps optional client/session/URL capabilities explicit.
"""

from __future__ import annotations

import csv
import hashlib
import itertools
import json
import math
import re
from dataclasses import replace
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .bank_adapter import _timestamp_ms, sha256_file
from .bank_metric_policy import load_bank_metric_policy
from .cache import canonical_json, digest_value
from .contracts import (Candidate, CandidateSet, EntityRef, Evidence, PaperCandidate,
    PaperCandidateSet, PaperLogPatternSummary, PaperLogSummary, PaperSessionObservation, PaperStarterInput,
    PaperURLObservation, RankedAnswer, ScoredCandidate, ScoredCandidates, Telemetry)
from .executor import NodeContext, OperatorResult
from .paper_starter_math import (FlowFitResult, MathResult, ScoringResult, VoteRecord,
    adjust_dependency_scores, combine_votes, fit_flow_contributions, permutation_test,
    stable_seed, token_sequence_matches, tree_attribution, tree_prediction_contributions, welch_test)
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec, RuntimeResolutionError, _validate_parameter


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/paper_starter_catalog.json"
PROFILE = "reconstructed-paper-starter-v1"
TABLE = "log/log_service.csv"
LOG_HEADER = ("log_id", "timestamp", "cmdb_id", "log_name", "value")

# One trusted weight group is one scoring budget. Historical Bank family names
# are aliases into these groups; they never create a separate vote budget.
PAPER_SCORER_GROUPS: Mapping[str, str] = MappingProxyType({
    "paper.cohorts.welch": "client_cohort",
    "paper.cohorts.permutation": "client_cohort",
    "paper.cohorts.tree_likelihood": "client_cohort_tree",
    "paper.backend.mean_welch": "backend_metric",
    "paper.backend.location_permutation": "backend_metric",
    "paper.backend.tail_permutation": "backend_metric",
    "paper.urls.welch": "url_analysis",
    "paper.urls.permutation": "url_analysis",
    "paper.urls.flow_contribution": "url_flow",
    "paper.events.outcome_support": "event_pattern",
    "paper.bank.metric_tree_likelihood": "backend_kpi_tree",
    "paper.bank.trace_root_flow": "trace_root_flow",
    "paper.bank.log_pattern_contrast": "bank_log_pattern",
    "paper.dependency_adjust": "dependency_adjustment",
    "bank.metrics.contrast": "backend_metric",
    "bank.traces.latency_contrast": "trace_latency",
    "bank.logs.volume_contrast": "log_volume",
})
PAPER_CONSENSUS_GROUPS = tuple(sorted(set(PAPER_SCORER_GROUPS.values()) |
    {"generated_metric_ir", "generated_operator"}))
PAPER_CONSENSUS_WEIGHT_ALIASES: Mapping[str, str] = MappingProxyType({
    "metric": "backend_metric",
    "bank_metric": "backend_metric",
    "bank_trace_latency": "trace_latency",
    "bank_log_volume": "log_volume",
})
PAPER_CONSENSUS_MAX_WEIGHT = 5.0


def consensus_weight_catalog() -> dict[str, Any]:
    """Publish the exact trusted consensus groups, legacy aliases, and bounds."""
    return {"canonical_groups": list(PAPER_CONSENSUS_GROUPS),
        "legacy_aliases": dict(PAPER_CONSENSUS_WEIGHT_ALIASES),
        "maximum_weight": PAPER_CONSENSUS_MAX_WEIGHT,
        "semantics": "Aliases normalize to one canonical group; supplying an alias and its canonical key together is invalid and cannot create a second vote budget."}


def normalize_consensus_weights(value: Any) -> dict[str, float]:
    """Validate and canonicalize caller-supplied group weights."""
    if not isinstance(value, Mapping):
        raise ValueError("family_weights must be an object mapping trusted groups to numbers")
    normalized: dict[str, float] = {}
    allowed = set(PAPER_CONSENSUS_GROUPS)
    for key, weight in value.items():
        if type(key) is not str:
            raise ValueError("family_weights keys must be trusted group strings")
        group = PAPER_CONSENSUS_WEIGHT_ALIASES.get(key, key)
        if group not in allowed:
            raise ValueError(f"family_weights contains unknown trusted group {key!r}")
        if group in normalized:
            raise ValueError(f"family_weights supplies duplicate aliases for canonical group {group!r}")
        if (type(weight) not in (int, float) or not math.isfinite(float(weight)) or
                float(weight) < 0.0 or float(weight) > PAPER_CONSENSUS_MAX_WEIGHT):
            raise ValueError(f"family_weights.{key} must be finite and between 0 and {PAPER_CONSENSUS_MAX_WEIGHT}")
        normalized[group] = float(weight)
    return normalized


class PaperStarterError(ValueError):
    pass


def _params(**values: tuple[Any, ...]) -> dict[str, ParameterSpec]:
    result: dict[str, ParameterSpec] = {}
    for name, row in values.items():
        if len(row) not in {2, 3}:
            raise ValueError(f"parameter {name} must declare type, default, and optional bounds")
        kind, default = row[0], row[1]
        constraints = row[2] if len(row) == 3 else {}
        result[name] = ParameterSpec(name, kind, default=default, has_default=True,
                                     constraints=constraints)
    return result


def _parameter_contract_error(spec: ParameterSpec, value: Any) -> None:
    try:
        _validate_parameter(spec, value)
    except RuntimeResolutionError as exc:
        raise PaperStarterError(str(exc)) from exc


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _normalized_log_tokens(text: str, *, maximum_tokens: int) -> tuple[str, ...]:
    """Normalize row-local lexical tokens; raw text and high-cardinality IDs stay local."""
    normalized = text.casefold()
    normalized = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{27,}\b", " <uuid> ", normalized)
    normalized = re.sub(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", " <ip> ", normalized)
    normalized = re.sub(r"\b[0-9a-f]{16,}\b", " <hex> ", normalized)
    normalized = re.sub(r"\b\d{6,}\b", " <number> ", normalized)
    tokens = re.findall(r"<uuid>|<ip>|<hex>|<number>|[a-z][a-z0-9_]{1,31}|\d{3}", normalized)
    if len(tokens) > maximum_tokens:
        raise PaperStarterError("native log line exceeds the declared token bound")
    return tuple(tokens)


def _collect_native_log_summaries(telemetry: Telemetry, root: Path, *, block_duration_ms: int,
        maximum_pattern_groups: int, maximum_tokens_per_line: int) -> tuple[
        tuple[PaperLogSummary, ...], tuple[PaperLogPatternSummary, ...], bool]:
    """Read/hash-check all log clips; return bounded counts and token hashes only."""
    case_dir = (root / "data/prepared/bank/public/cases" / telemetry.case_id).resolve(strict=True)
    cases_root = (root / "data/prepared/bank/public/cases").resolve(strict=True)
    if cases_root not in case_dir.parents:
        raise PaperStarterError("Bank case directory escapes public input root")
    alert_path = (case_dir / "alert.json").resolve(strict=True)
    if case_dir not in alert_path.parents or sha256_file(alert_path) != telemetry.file_sha256.get("alert.json"):
        raise PaperStarterError("Bank log summary requires hash-verified alert metadata")
    alert = json.loads(alert_path.read_text(encoding="utf-8"))
    dates, units_by_date = alert.get("telemetry_dates"), alert.get("source_date_timestamp_units")
    if not isinstance(dates, list) or not isinstance(units_by_date, Mapping):
        raise PaperStarterError("Bank log summary requires declared dates and timestamp units")
    counts: Counter[tuple[str, str, str]] = Counter()
    pattern_counts: Counter[tuple[str, str, str, int, tuple[str, ...]]] = Counter()
    block_counts: Counter[tuple[str, str, str, int]] = Counter()
    pattern_capacity_ok = True
    token_bound_ok = True
    source_hashes: list[str] = []
    scanned = 0
    csv.field_size_limit(4 * 1024 * 1024)
    for date in sorted(dates):
        relative = f"source_dates/{date}/{TABLE}"
        expected_hash = telemetry.file_sha256.get(relative)
        unit = units_by_date.get(date, {}).get(TABLE)
        if not expected_hash or unit not in {"seconds", "milliseconds"}:
            raise PaperStarterError(f"Bank log source metadata missing for {relative}")
        path = (case_dir / relative).resolve(strict=True)
        if case_dir not in path.parents or not path.is_file() or sha256_file(path) != expected_hash:
            raise PaperStarterError(f"Bank log clip hash/path check failed for {relative}")
        source_hashes.append(expected_hash)
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != LOG_HEADER:
                raise PaperStarterError(f"Bank native log schema mismatch for {relative}")
            for line_number, row in enumerate(reader, 2):
                if None in row or any(value is None for value in row.values()):
                    raise PaperStarterError(f"malformed Bank log row at {relative}:{line_number}")
                scanned += 1
                timestamp = _timestamp_ms(row["timestamp"], unit, path, line_number)
                window = telemetry.reference_window if telemetry.reference_window.contains(timestamp) else (
                    telemetry.alert_window if telemetry.alert_window.contains(timestamp) else None)
                component, log_name = row["cmdb_id"], row["log_name"]
                if window is not None and component and log_name:
                    counts[(window.name, component, log_name)] += 1
                    block_start = window.start_ms_utc + ((timestamp - window.start_ms_utc) // block_duration_ms) * block_duration_ms
                    block_counts[(window.name, component, log_name, block_start)] += 1
                    if pattern_capacity_ok and token_bound_ok:
                        try:
                            normalized_tokens = _normalized_log_tokens(row["value"], maximum_tokens=maximum_tokens_per_line)
                        except PaperStarterError:
                            token_bound_ok = False
                            pattern_counts.clear()
                        else:
                            if 2 <= len(normalized_tokens) <= maximum_tokens_per_line:
                                pattern_counts[(window.name, component, log_name, block_start, normalized_tokens)] += 1
                            if len(pattern_counts) > maximum_pattern_groups:
                                pattern_capacity_ok = False
                                pattern_counts.clear()
        if sha256_file(path) != expected_hash:
            raise PaperStarterError(f"Bank log clip changed while being scanned: {relative}")
    result = []
    hashes = tuple(sorted(source_hashes))
    for (window_name, component, log_name), count in sorted(counts.items()):
        window = telemetry.reference_window if window_name == "reference" else telemetry.alert_window
        result.append(PaperLogSummary(component, log_name, window, count, scanned, hashes))
    patterns = []
    for (window_name, component, log_name, block_start, tokens), count in sorted(pattern_counts.items()):
        window = telemetry.reference_window if window_name == "reference" else telemetry.alert_window
        denominator = block_counts[(window_name, component, log_name, block_start)]
        pattern_hash = hashlib.sha256("\0".join(tokens).encode("utf-8")).hexdigest()
        patterns.append(PaperLogPatternSummary(component, log_name, window, block_start,
            block_duration_ms, tokens, pattern_hash, count, denominator, scanned, hashes))
    if not pattern_capacity_ok or not token_bound_ok:
        patterns = []
    return tuple(result), tuple(patterns), pattern_capacity_ok and token_bound_ok


def make_paper_starter_input(telemetry: Telemetry, *,
        sessions: Sequence[PaperSessionObservation] | None = None,
        urls: Sequence[PaperURLObservation] | None = None,
        block_minutes: int | None = None,
        root: Path = ROOT) -> PaperStarterInput:
    """Build the profile input. None means unsupported; () means supported-empty."""
    logs: tuple[PaperLogSummary, ...] = ()
    log_patterns: tuple[PaperLogPatternSummary, ...] = ()
    log_patterns_complete = False
    native_logs = "unsupported"
    policy = json.loads((root / "config/paper_starter_runtime.json").read_text(encoding="utf-8"))
    if policy.get("schema_version") != "e4-paper-starter-runtime/1":
        raise PaperStarterError("unsupported paper starter runtime policy")
    block_minutes = int(policy["analysis_block_minutes"] if block_minutes is None else block_minutes)
    if block_minutes < 1 or block_minutes > 60:
        raise PaperStarterError("block_minutes must be between 1 and 60")
    block_duration_ms = block_minutes * 60_000
    pattern_policy = policy["log_token_normalization"]
    try:
        logs, log_patterns, log_patterns_complete = _collect_native_log_summaries(telemetry, root.resolve(strict=True),
            block_duration_ms=block_duration_ms,
            maximum_pattern_groups=int(policy["maximum_native_log_pattern_groups"]),
            maximum_tokens_per_line=int(pattern_policy["maximum_tokens_per_line"]))
        native_logs = "available"
    except FileNotFoundError:
        # Synthetic tests may intentionally provide only in-memory telemetry.
        if not telemetry.case_id.startswith(("synthetic-", "fixture-", "test-")):
            raise
    capabilities = {"sessions": "available" if sessions is not None else "unsupported",
        "urls": "available" if urls is not None else "unsupported",
        "native_logs": native_logs,
        "native_log_patterns": "available" if native_logs == "available" and log_patterns_complete else "unsupported",
        "component_metrics": "available" if any(point.table == "bank/metric_container.csv"
            for point in telemetry.metrics) else "unsupported",
        "app_mrt": "available" if any(point.table == "bank/metric_app.csv" and
            point.metric_name == "mrt" and point.value is not None for point in telemetry.metrics) else "unsupported",
        "trace_dependencies": "available" if any(span.parent_resolved_in_clip for span in telemetry.spans)
                              else "unsupported",
        "trace_root_duration": "available" if any(span.parent_id is None for span in telemetry.spans)
                              else "unsupported"}
    return PaperStarterInput(telemetry, None if sessions is None else tuple(sessions),
        None if urls is None else tuple(urls), capabilities, block_duration_ms, logs, log_patterns)


def _starter(value: Any) -> PaperStarterInput:
    if not isinstance(value, PaperStarterInput):
        raise PaperStarterError("operator requires PaperStarterInput")
    return value


def _candidate_descriptor(value: Candidate | PaperCandidate) -> tuple[EntityRef, Mapping[str, Any]]:
    if isinstance(value, Candidate):
        return value.entity, {"kind": "component", "rationale": value.rationale}
    return EntityRef(value.kind, value.identifier), {"kind": value.kind,
        "attributes": dict(value.attributes), "tokens": value.tokens, "url": value.url}


def _score_outputs(items: Sequence[Candidate | PaperCandidate], *, scores: Mapping[str, float],
        scorer: str, window: Any, statistic: str, support: Mapping[str, int],
        details: Mapping[str, Mapping[str, Any]] | None = None,
        family: str | None = None, status: str = "ready", status_details: Mapping[str, Any] | None = None
        ) -> ScoredCandidates:
    detail_map = details or {}
    output = []
    for item in items:
        entity, descriptor = _candidate_descriptor(item)
        identity = entity.identifier
        score = float(scores.get(identity, 0.0))
        support_count = int(support.get(identity, 0))
        evidence_details = {"candidate_descriptor": descriptor,
            **dict(detail_map.get(identity, {}))}
        sources = evidence_details.get("source_tables", ())
        if isinstance(sources, str): sources = (sources,)
        if not isinstance(sources, (tuple, list)): sources = ()
        eligible = bool(evidence_details.get("ranking_eligible", score > 0 and support_count > 0))
        evidence_details["ranking_eligible"] = eligible
        evidence_details["computation"] = scorer
        if not sources:
            sources = (scorer,)
        evidence = () if identity not in support else tuple(Evidence(str(source), window, statistic,
            score, (entity,), support_count, {**evidence_details, "source_tables": (str(source),)})
            for source in sources)
        output.append(ScoredCandidate(entity, score, None,
            evidence, family))
    return ScoredCandidates(tuple(output), scorer, status, status_details or {})


def _samples(telemetry: Telemetry, component: str, field_name: str,
             *, block_duration_ms: int = 300_000,
             metric_policy=None) -> tuple[list[float], list[float]]:
    classifier = metric_policy or load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
    semantics = classifier.classify("bank/metric_container.csv", field_name)
    if not semantics.ranking_eligible:
        raise PaperStarterError("native component samples require one frozen allowlisted field")
    buckets: dict[tuple[str, int], list[float]] = defaultdict(list)
    for point in telemetry.metrics:
        if point.entity.role != "component" or point.entity.identifier != component or point.value is None:
            continue
        if point.table != "bank/metric_container.csv" or point.metric_name != field_name:
            continue
        window = telemetry.reference_window if telemetry.reference_window.contains(point.timestamp_ms_utc) else (
            telemetry.alert_window if telemetry.alert_window.contains(point.timestamp_ms_utc) else None)
        if window is None: continue
        bucket_start = window.start_ms_utc + ((point.timestamp_ms_utc - window.start_ms_utc) // block_duration_ms) * block_duration_ms
        buckets[(window.name, bucket_start)].append(float(point.value))
    before = [median(values) for (name, _start), values in sorted(buckets.items()) if name == "reference"]
    after = [median(values) for (name, _start), values in sorted(buckets.items()) if name == "alert"]
    return before, after


def _p_score(result: MathResult, *, alpha: float, directional: bool = False) -> float:
    if result.status != "ok" or result.effect is None or result.p_value is None:
        return 0.0
    p = max(float(result.p_value), 1e-15)
    effect = abs(result.effect) if not directional else max(0.0, result.effect)
    return float(effect * -math.log10(p)) if p <= alpha else 0.0


def _candidate_pairs(value: Any) -> tuple[Candidate | PaperCandidate, ...]:
    if isinstance(value, CandidateSet):
        return tuple(value.candidates)
    if isinstance(value, PaperCandidateSet):
        return tuple(value.candidates)
    raise PaperStarterError("scorer requires a typed candidate set")


def _input_from(inputs: Mapping[str, Any]) -> PaperStarterInput:
    return _starter(inputs.get("data"))


def _rankable_component_metric_fields() -> Mapping[str, Mapping[str, Any]]:
    classifier = load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
    return {name: classifier.classify("bank/metric_container.csv", name).to_dict()
        for name in classifier.exact_rankable_fields("bank/metric_container.csv")}


def _rankable_component_metric_rule_refs() -> tuple[str, ...]:
    """Stable source-rule selectors, independent of the current case field set."""
    classifier = load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
    return tuple(entry.selector_ref for entry in
        classifier.selector_catalog("bank/metric_container.csv")
        if entry.selector_kind == "pattern")


def _selected_metric_fields(telemetry: Telemetry, *, metric: str,
                            metric_rule_ref: str,
                            metric_policy=None) -> tuple[list[str], dict[str, Any] | None]:
    if metric and metric_rule_ref:
        raise PaperStarterError("choose either the legacy exact metric selector or a metric rule ref")
    if metric:
        allowed = _rankable_component_metric_fields()
        if metric not in allowed:
            raise PaperStarterError("metric selector is not in the frozen Bank ranking allowlist")
        return [metric], None
    if metric_rule_ref:
        classifier = metric_policy or load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
        observed = tuple(sorted({point.metric_name for point in telemetry.metrics
            if point.table == "bank/metric_container.csv"}))
        resolution = classifier.resolve_selector(metric_rule_ref,
            "bank/metric_container.csv", observed,
            case_data_sha256=telemetry.data_sha256)
        return [item.field_name for item in resolution.matches], resolution.to_dict()
    return sorted(_rankable_component_metric_fields()), None


def paper_parameter_catalog() -> dict[str, Any]:
    """Intern repeated native enums once for model display; registry specs stay complete."""
    classifier = load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
    selector_rules = tuple(entry for entry in
        classifier.selector_catalog("bank/metric_container.csv")
        if entry.selector_kind == "pattern")
    return {"schema_version": "e4-paper-parameter-catalog/1",
        "enums": {
            "paper.bank_component_metric_fields": {
                "values": ["", *sorted(_rankable_component_metric_fields())],
                "empty_value_semantics": "analyze each supported allowlisted component metric separately",
                "meaning": "exact component metric_container.kpi_name values permitted by the frozen native ranking allowlist"},
            "paper.bank_component_metric_rule_refs": {
                "values": ["", *[entry.selector_ref for entry in selector_rules]],
                "empty_value_semantics": "keep legacy exact-field selector/default behavior",
                "meaning": "stable versioned source-semantic rule refs resolved by the trusted host to exact case fields",
                "descriptions": {entry.selector_ref: entry.description for entry in selector_rules}},
            "paper.app_kpi_fields": {"values": ["mrt"],
                "meaning": "opaque app KPI context field; not a component metric"},
            "paper.flow_fit_modes": {"values": ["historical", "pooled"],
                "meaning": "trusted flow-kernel fit modes"}},
        "numeric_contracts": {
            "quantile": "URL delay quantiles permit [0,1]; permutation tail quantiles require (0,1)",
            "validation_fraction": "0 < value < 1",
            "scale_floor": "value > 0",
            "nonnegative_effect_or_tolerance": "value >= 0",
            "maximum_correlation_or_fraction": "0 <= value <= 1",
            "consensus_tolerance": "tie_tolerance >= 0",
            "dependency_penalty": "0 <= downstream_penalty <= 1"}}


def intern_paper_parameter_choices(signatures: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Compress repeated long enum arrays for prompts without changing registry signatures."""
    metric_choices = ["", *sorted(_rankable_component_metric_fields())]
    rule_choices = list(paper_parameter_catalog()["enums"][
        "paper.bank_component_metric_rule_refs"]["values"])
    result = _plain(signatures)
    for operator in result:
        for name, parameter in operator.get("parameters", {}).items():
            if (name in {"metric", "component_metric"} and
                    parameter.get("choices") == metric_choices):
                parameter["choices"] = []
                parameter["choices_ref"] = "paper.bank_component_metric_fields"
            elif (name == "metric_rule_ref" and parameter.get("choices") == rule_choices):
                parameter["choices"] = []
                parameter["choices_ref"] = "paper.bank_component_metric_rule_refs"
    return result


def _metric_conditions(inputs: Mapping[str, Any], params: Mapping[str, Any],
                       context: NodeContext) -> OperatorResult:
    starter = _input_from(inputs)
    telemetry = starter.telemetry
    min_support, min_effect = int(params["minimum_samples"]), float(params["minimum_effect"])
    classifier = load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
    selected_fields, selector_resolution = _selected_metric_fields(telemetry,
        metric=params.get("metric") or "",
        metric_rule_ref=params.get("metric_rule_ref") or "", metric_policy=classifier)
    candidates = []
    for entity in sorted({point.entity.identifier for point in telemetry.metrics
                          if point.entity.role == "component"}, key=lambda value: (value.casefold(), value)):
        field_changes = []
        for field_name in selected_fields:
            before, after = _samples(telemetry, entity, field_name,
                block_duration_ms=starter.analysis_block_duration_ms, metric_policy=classifier)
            if min(len(before), len(after)) < min_support: continue
            change = median(after) - median(before)
            if abs(change) >= min_effect:
                field_changes.append((field_name, change, len(before), len(after)))
        if field_changes:
            candidates.append(Candidate(EntityRef("component", entity),
                "supported native metric field excursion(s); fields retain separate native units" +
                (f"; selector={params.get('metric_rule_ref')}" if params.get("metric_rule_ref") else "")))
    return OperatorResult(CandidateSet(tuple(candidates), "paper.backend.metric_conditions"),
                          len(candidates), 0)


def _rich_candidates(inputs: Mapping[str, Any], params: Mapping[str, Any],
                     context: NodeContext, kind: str) -> OperatorResult:
    starter = _input_from(inputs)
    minimum = int(params["minimum_support"])
    support_sets: dict[tuple[str, tuple[tuple[str, str], ...], tuple[str, ...], str | None], set[str]] = defaultdict(set)
    found: dict[tuple[str, tuple[tuple[str, str], ...], tuple[str, ...], str | None], int] = {}
    sessions = starter.sessions
    urls = starter.urls
    if kind in {"cohort_attribute", "cohort_conjunction", "cohort_activity", "event_pattern"}:
        if sessions is None:
            raise PaperStarterError(f"{kind} capability is unsupported for this input")
        reference = starter.telemetry.reference_window
        if kind == "cohort_attribute":
            for row in sessions:
                if not reference.contains(row.timestamp_ms_utc): continue
                for key, value in row.attributes.items():
                    support_sets[("cohort", ((key, value),), (), None)].add(row.session_id)
        elif kind == "cohort_conjunction":
            order = int(params["maximum_order"])
            for row in sessions:
                if not reference.contains(row.timestamp_ms_utc): continue
                attrs = tuple(sorted(row.attributes.items()))
                for size in range(2, min(order, len(attrs)) + 1):
                    for combo in itertools.combinations(attrs, size):
                        item = ("cohort", tuple(combo), (), None)
                        support_sets[item].add(row.session_id)
        elif kind == "cohort_activity":
            history: dict[str, int] = Counter()
            for row in sessions:
                if reference.contains(row.timestamp_ms_utc):
                    history[row.session_id] += len(row.events)
            values = sorted(history.values())
            if values:
                cutoff = values[len(values) // 2]
                for session_id, count in history.items():
                    label = "high_activity" if count > cutoff else "low_activity"
                    item = ("cohort", (("activity", label),), (), None)
                    support_sets[item].add(session_id)
        else:
            max_length = int(params["maximum_length"])
            max_candidates = int(params["maximum_candidates"])
            for row in sessions:
                if not row.events: continue
                tokens = row.events
                for length in range(1, min(max_length, len(tokens)) + 1):
                    for start in range(len(tokens) - length + 1):
                        pattern = tuple(tokens[start:start + length])
                        item = ("event_pattern", (), pattern, None)
                        support_sets[item].add(row.session_id)
            support_sets = dict(sorted(support_sets.items(),
                key=lambda pair: (-len(pair[1]), pair[0]))[:max_candidates])
    elif kind in {"url_failure", "url_delay"}:
        if urls is None:
            raise PaperStarterError(f"{kind} capability is unsupported for this input")
        by_url: dict[str, list[PaperURLObservation]] = defaultdict(list)
        for row in urls: by_url[row.url].append(row)
        for url, rows in by_url.items():
            before = [r for r in rows if starter.telemetry.reference_window.contains(r.timestamp_ms_utc)]
            after = [r for r in rows if starter.telemetry.alert_window.contains(r.timestamp_ms_utc)]
            if kind == "url_failure":
                b_groups: dict[str, list[bool]] = defaultdict(list)
                a_groups: dict[str, list[bool]] = defaultdict(list)
                for row in before:
                    if row.success is not None: b_groups[row.session_id].append(bool(row.success))
                for row in after:
                    if row.success is not None: a_groups[row.session_id].append(bool(row.success))
                b = [sum(not value for value in values) / len(values) for values in b_groups.values()]
                a = [sum(not value for value in values) / len(values) for values in a_groups.values()]
                if min(len(b), len(a)) < minimum: continue
                change = sum(a) / len(a) - sum(b) / len(b)
                if change >= float(params["minimum_change"]):
                    found[("url", (("url_signal", "failure"),), (), url)] = len(a)
            else:
                b_groups: dict[str, list[float]] = defaultdict(list)
                a_groups: dict[str, list[float]] = defaultdict(list)
                for row in before:
                    if row.duration is not None: b_groups[row.session_id].append(float(row.duration))
                for row in after:
                    if row.duration is not None: a_groups[row.session_id].append(float(row.duration))
                b = [median(values) for values in b_groups.values()]
                a = [median(values) for values in a_groups.values()]
                if min(len(b), len(a)) < minimum: continue
                qb, qa = float(params["quantile"]), float(params["quantile"])
                change = _quantile(a, qa) - _quantile(b, qb)
                if change >= float(params["minimum_change"]):
                    found[("url", (("url_signal", "delay"),), (), url)] = len(a)
    found.update({key: len(session_ids) for key, session_ids in support_sets.items()})
    rows = [PaperCandidate(kind_key, _candidate_id(kind_key, attrs, tokens, url),
                dict(attrs), tokens, url)
            for (kind_key, attrs, tokens, url), count in sorted(found.items()) if count >= minimum]
    return OperatorResult(PaperCandidateSet(tuple(rows), f"paper.{kind}"), len(rows), 0)


def _candidate_id(kind: str, attributes: tuple[tuple[str, str], ...], tokens: tuple[str, ...], url: str | None) -> str:
    return hashlib.sha256(canonical_json((kind, attributes, tokens, url))).hexdigest()[:24]


def _quantile(values: Sequence[float], q: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered: return 0.0
    position = max(0.0, min(1.0, q)) * (len(ordered) - 1)
    lo, hi = int(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def _score_backend(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext,
                   scorer: str, statistic: str) -> OperatorResult:
    starter = _input_from(inputs)
    telemetry = starter.telemetry
    candidates = _candidate_pairs(inputs.get("candidates"))
    classifier = load_bank_metric_policy(ROOT / "config/bank_metric_kind_policy.json")
    fields, selector_resolution = _selected_metric_fields(telemetry,
        metric=params.get("metric") or "",
        metric_rule_ref=params.get("metric_rule_ref") or "", metric_policy=classifier)
    field_semantics = {name: classifier.classify("bank/metric_container.csv", name).to_dict()
        for name in fields}
    selector_report = ({"selector_ref": selector_resolution["selector_ref"],
        "status": selector_resolution["status"],
        "policy_sha256": selector_resolution["policy_sha256"],
        "case_data_sha256": selector_resolution["case_data_sha256"],
        "match_count": selector_resolution["match_count"],
        "resolved_exact_fields": tuple({"field_name": row["field_name"],
            "rule_id": row["rule_id"], "rule_version": row["rule_version"]}
            for row in selector_resolution["matches"])}
        if selector_resolution else None)
    scores, supports, details = {}, {}, {}
    for candidate in candidates:
        entity, _ = _candidate_descriptor(candidate)
        field_results = []
        for metric_name in fields:
            before, after = _samples(telemetry, entity.identifier, metric_name,
                block_duration_ms=starter.analysis_block_duration_ms, metric_policy=classifier)
            if min(len(before), len(after)) < int(params["minimum_samples"]):
                continue
            if scorer == "welch":
                stat_result = welch_test(before, after, minimum_samples=int(params["minimum_samples"]))
            else:
                stat_result = permutation_test(before, after, seed=stable_seed(
                    telemetry.case_id, entity.identifier, metric_name, scorer),
                    resamples=int(params["resamples"]), minimum_blocks=int(params["minimum_samples"]),
                    relative_scale_floor=float(params["relative_scale_floor"]),
                    absolute_scale_floor=float(params["absolute_scale_floor"]),
                    statistic=statistic, quantile=float(params["quantile"]))
            field_results.append((metric_name, stat_result, before, after))
        scored_fields = [(_p_score(row, alpha=float(params["alpha"]) / max(1, len(field_results))),
                          metric_name, row, before, after)
                         for metric_name, row, before, after in field_results]
        score, metric_name, result, before, after = max(scored_fields,
            key=lambda value: (value[0], value[1])) if scored_fields else (0.0, "", None, [], [])
        scores[entity.identifier] = score
        supports[entity.identifier] = min(result.support_before, result.support_after) if result else 0
        details[entity.identifier] = {"status": result.status if result else "abstain",
            "reason": result.reason if result else "no_allowlisted_metric_field",
            "effect": result.effect if result else None,
            "p_value": result.p_value if result else None,
            **(dict(result.details) if result else {}),
            "method": "paper-family reconstruction; not an exact unpublished equation",
            "metric_field": metric_name,
            "metric_field_test_count": len(field_results),
            "metric_rule_ref": params.get("metric_rule_ref") or None,
            "metric_field_semantics": field_semantics.get(metric_name),
            "multiple_testing_scope": "Bonferroni within this candidate's support-qualified selected fields only; no global component-family significance claim",
            "source_tables": ("bank/metric_container.csv",),
            "ranking_eligible": score > 0 and supports[entity.identifier] > 0}
    scorer_name = ("paper.backend.mean_welch" if scorer == "welch" else
        "paper.backend.location_permutation" if statistic == "median" else
        "paper.backend.tail_permutation")
    status_details = {"metric_selector_resolution": selector_report,
        "resolved_field_semantics": tuple(field_semantics[name] for name in fields),
        "policy_sha256": classifier.sha256,
        "case_data_sha256": telemetry.data_sha256,
        "multiple_testing_scope": "per-candidate selected-field tests only; no global component-family significance claim"}
    output = _score_outputs(candidates, scores=scores, scorer=scorer_name,
        window=telemetry.alert_window, statistic=statistic, support=supports, details=details,
        family="backend_metric", status_details=status_details)
    return OperatorResult(output, sum(supports.values()), 0)


def _score_rich(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext,
                scorer: str) -> OperatorResult:
    starter = _input_from(inputs)
    candidates = _candidate_pairs(inputs.get("candidates"))
    sessions, urls = starter.sessions, starter.urls
    scores, supports, details = {}, {}, {}
    for candidate in candidates:
        entity, descriptor = _candidate_descriptor(candidate)
        result = None
        if scorer in {"cohort_welch", "cohort_permutation"}:
            if sessions is None: raise PaperStarterError("session capability is unsupported")
            attrs = descriptor.get("attributes", {})
            if "activity" in attrs:
                activity = Counter()
                for row in sessions:
                    if starter.telemetry.reference_window.contains(row.timestamp_ms_utc):
                        activity[row.session_id] += len(row.events)
                cutoff = sorted(activity.values())[len(activity) // 2] if activity else 0
                eligible_sessions = {sid for sid, count in activity.items()
                    if ("high_activity" if count > cutoff else "low_activity") == attrs["activity"]}
            else:
                eligible_sessions = None
            rows = [row for row in sessions if all(row.attributes.get(k) == v for k, v in attrs.items()
                    if k != "activity")
                    and (eligible_sessions is None or row.session_id in eligible_sessions)
                    and row.kpi_value is not None]
            window_kpi: dict[tuple[str, str], list[float]] = defaultdict(list)
            for row in rows:
                window = "reference" if starter.telemetry.reference_window.contains(row.timestamp_ms_utc) else (
                    "alert" if starter.telemetry.alert_window.contains(row.timestamp_ms_utc) else None)
                if window is not None: window_kpi[(row.session_id, window)].append(float(row.kpi_value))
            before = [median(values) for (sid, window), values in sorted(window_kpi.items())
                      if window == "reference"]
            after = [median(values) for (sid, window), values in sorted(window_kpi.items())
                     if window == "alert"]
            if scorer == "cohort_welch":
                result = welch_test(before, after, minimum_samples=int(params["minimum_samples"]))
            else:
                result = permutation_test(before, after, seed=stable_seed(entity.identifier),
                    resamples=int(params["resamples"]), minimum_blocks=int(params["minimum_samples"]),
                    relative_scale_floor=float(params["relative_scale_floor"]),
                absolute_scale_floor=float(params["absolute_scale_floor"]), statistic="median", quantile=0.9)
            score = _p_score(result, alpha=float(params["alpha"]))
            supports[entity.identifier] = min(result.support_before, result.support_after)
            details[entity.identifier] = {"status": result.status, "reason": result.reason,
                "effect": result.effect, "p_value": result.p_value, **dict(result.details)}
        elif scorer == "tree_likelihood":
            if sessions is None: raise PaperStarterError("session capability is unsupported")
            attrs = descriptor.get("attributes", {})
            before_rows = sorted((r for r in sessions
                if starter.telemetry.reference_window.contains(r.timestamp_ms_utc) and r.kpi_value is not None),
                key=lambda row: (row.timestamp_ms_utc, row.session_id))
            alert_rows = sorted((r for r in sessions
                if starter.telemetry.alert_window.contains(r.timestamp_ms_utc) and r.kpi_value is not None),
                key=lambda row: (row.timestamp_ms_utc, row.session_id))
            baseline_values: dict[str, set[str]] = defaultdict(set)
            for row in before_rows:
                for key, value in row.attributes.items(): baseline_values[key].add(value)
            feature_names = tuple(sorted(f"{key}={value}" for key, values in baseline_values.items()
                                         for value in values))
            unknown_alert_category = any(value not in baseline_values.get(key, set())
                for row in alert_rows for key, value in row.attributes.items())
            if unknown_alert_category:
                score, supports[entity.identifier] = 0.0, 0
                details[entity.identifier] = {"status": "abstain",
                    "reason": "alert_contains_category_absent_from_baseline",
                    "baseline_feature_values": tuple((key, tuple(sorted(values)))
                        for key, values in sorted(baseline_values.items()))}
                continue
            def vec(row): return [1.0 if row.attributes.get(name.split("=", 1)[0]) == name.split("=", 1)[1]
                                  else 0.0 for name in feature_names]
            if not feature_names:
                score, supports[entity.identifier] = 0.0, 0
                details[entity.identifier] = {"status": "abstain", "reason": "no_client_features"}
                continue
            result = tree_attribution([vec(r) for r in before_rows],
                [float(r.kpi_value) for r in before_rows], [vec(r) for r in alert_rows],
                [float(r.kpi_value) for r in alert_rows], feature_names=feature_names,
                max_depth=int(params["max_depth"]), min_samples_leaf=int(params["min_samples_leaf"]),
                minimum_samples=int(params["minimum_samples"]),
                validation_fraction=float(params["validation_fraction"]),
                minimum_validation_samples=int(params["minimum_validation_samples"]),
                minimum_validation_r2=float(params["minimum_validation_r2"]),
                maximum_validation_normalized_error=float(params["maximum_validation_normalized_error"]),
                residual_scale_floor=float(params["residual_scale_floor"]),
                minimum_excess_nll_per_observation=float(params["minimum_excess_nll_per_observation"]),
                seed=int(params["seed"]))
            result_details = dict(result.details)
            validation_rows = {index: before_rows[index]
                for index in result_details.get("validation_row_indices", ())
                if 0 <= index < len(before_rows)}
            alert_scored_rows = {index: alert_rows[index]
                for index in result_details.get("alert_row_indices", ())
                if 0 <= index < len(alert_rows)}
            validation_nll = {index: -float(ll) for index, ll in zip(
                result_details.get("validation_row_indices", ()),
                result_details.get("validation_log_likelihoods", ()))}
            alert_nll = {index: -float(ll) for index, ll in zip(
                result_details.get("alert_row_indices", ()),
                result_details.get("alert_log_likelihoods", ()))}
            if "activity" in attrs:
                activity = Counter()
                for row in sessions:
                    if starter.telemetry.reference_window.contains(row.timestamp_ms_utc):
                        activity[row.session_id] += len(row.events)
                cutoff = sorted(activity.values())[len(activity) // 2] if activity else 0
                eligible_sessions = {sid for sid, count in activity.items()
                    if ("high_activity" if count > cutoff else "low_activity") == attrs["activity"]}
            else:
                eligible_sessions = None
            def matches(row):
                return (all(row.attributes.get(key) == value for key, value in attrs.items()
                    if key != "activity") and
                    (eligible_sessions is None or row.session_id in eligible_sessions))
            base_values = [validation_nll[index] for index, row in validation_rows.items() if matches(row)]
            alert_values = [alert_nll[index] for index, row in alert_scored_rows.items() if matches(row)]
            group_excess = (sum(alert_values) / len(alert_values) - sum(base_values) / len(base_values)
                            if base_values and alert_values else 0.0)
            threshold = float(params["minimum_excess_nll_per_observation"])
            score = max(0.0, group_excess) if result.status != "abstain" and group_excess >= threshold else 0.0
            supports[entity.identifier] = len(alert_values)
            details[entity.identifier] = {"status": result.status, "reason": result.reason,
                "cohort_heldout_support": len(base_values), "cohort_alert_support": len(alert_values),
                "cohort_excess_nll_per_observation": group_excess,
                "minimum_excess_nll_per_observation": threshold,
                **result_details}
        elif scorer in {"url_welch", "url_failure"} and (
                scorer == "url_failure" or descriptor.get("attributes", {}).get("url_signal") == "failure"):
            if urls is None: raise PaperStarterError("URL capability is unsupported")
            url = descriptor.get("url")
            by_session_window: dict[tuple[str, str], list[bool]] = defaultdict(list)
            for row in urls:
                if row.url != url or row.success is None: continue
                window = "reference" if starter.telemetry.reference_window.contains(row.timestamp_ms_utc) else (
                    "alert" if starter.telemetry.alert_window.contains(row.timestamp_ms_utc) else None)
                if window:
                    by_session_window[(row.session_id, window)].append(bool(row.success))
            before = [1.0 - sum(values) / len(values) for (sid, window), values in by_session_window.items()
                      if window == "reference"]
            after = [1.0 - sum(values) / len(values) for (sid, window), values in by_session_window.items()
                     if window == "alert"]
            result = welch_test(before, after, minimum_samples=int(params["minimum_samples"]))
            score = _p_score(result, alpha=float(params["alpha"]), directional=True)
            supports[entity.identifier] = min(result.support_before, result.support_after)
            details[entity.identifier] = {"failure_rate_effect": result.effect,
                "p_value": result.p_value, "status": result.status, "reason": result.reason,
                "valid_request_observations": min(result.support_before, result.support_after),
                "outcome_denominator": "distinct session-window valid outcome observations",
                "source_tables": ("paper/session_outcomes",),
                "ranking_eligible": score > 0 and supports[entity.identifier] > 0}
        elif scorer in {"url_welch", "url_permutation"}:
            if urls is None: raise PaperStarterError("URL capability is unsupported")
            url = descriptor.get("url")
            rows = [row for row in urls if row.url == url and row.duration is not None]
            by_session_window: dict[tuple[str, str], list[float]] = defaultdict(list)
            for row in rows:
                window = "reference" if starter.telemetry.reference_window.contains(row.timestamp_ms_utc) else (
                    "alert" if starter.telemetry.alert_window.contains(row.timestamp_ms_utc) else None)
                if window:
                    by_session_window[(row.session_id, window)].append(float(row.duration))
            before = [median(values) for (sid, window), values in by_session_window.items() if window == "reference"]
            after = [median(values) for (sid, window), values in by_session_window.items() if window == "alert"]
            if scorer == "url_welch":
                result = welch_test(before, after, minimum_samples=int(params["minimum_samples"]))
            else:
                result = permutation_test(before, after, seed=stable_seed(entity.identifier),
                    resamples=int(params["resamples"]), minimum_blocks=int(params["minimum_samples"]),
                    relative_scale_floor=float(params["relative_scale_floor"]),
                    absolute_scale_floor=float(params["absolute_scale_floor"]), statistic="median", quantile=0.9)
            score = _p_score(result, alpha=float(params["alpha"]))
            supports[entity.identifier] = min(result.support_before, result.support_after)
            details[entity.identifier] = {"status": result.status, "reason": result.reason,
                "effect": result.effect, "p_value": result.p_value, **dict(result.details)}
        elif scorer == "event_support":
            if sessions is None: raise PaperStarterError("session capability is unsupported")
            pattern = tuple(descriptor.get("tokens", ()))
            outcome_by_identity: dict[tuple[str, str], set[bool]] = defaultdict(set)
            matched_by_identity: set[tuple[str, str]] = set()
            for row in sessions:
                if row.outcome is None: continue
                window = "reference" if starter.telemetry.reference_window.contains(row.timestamp_ms_utc) else (
                    "alert" if starter.telemetry.alert_window.contains(row.timestamp_ms_utc) else None)
                if window is None: continue
                observation_id = (row.session_id, window)
                outcome_by_identity[observation_id].add(bool(row.outcome))
                matches = token_sequence_matches(row.events, [pattern], sequence_length=len(row.events),
                    max_patterns=int(params["maximum_patterns"]),
                    max_pattern_length=int(params["maximum_pattern_length"])) if pattern else ()
                if matches:
                    matched_by_identity.add(observation_id)
            consistent_outcomes = {identity: next(iter(outcomes)) for identity, outcomes in outcome_by_identity.items()
                                   if len(outcomes) == 1}
            conflict_count = len(outcome_by_identity) - len(consistent_outcomes)
            failed = {identity for identity, outcome in consistent_outcomes.items()
                      if outcome is False and identity in matched_by_identity}
            successful = {identity for identity, outcome in consistent_outcomes.items()
                          if outcome is True and identity in matched_by_identity}
            support_failed, support_success = len(failed), len(successful)
            total_failed = sum(outcome is False for outcome in consistent_outcomes.values())
            total_success = sum(outcome is True for outcome in consistent_outcomes.values())
            if min(total_failed, total_success) < int(params["minimum_samples"]):
                score = 0.0
            else:
                effect = support_failed / total_failed - support_success / total_success
                score = max(0.0, effect - float(params["minimum_effect"]))
            supports[entity.identifier] = support_failed + support_success
            details[entity.identifier] = {"failed_session_support": support_failed,
                "successful_session_support": support_success,
                "valid_failed_outcome_denominator": total_failed,
                "valid_successful_outcome_denominator": total_success,
                "conflicting_session_window_outcomes_excluded": conflict_count,
                "observation_identity": "distinct session_id × half-open window; conflicting outcomes excluded",
                "causal": False}
        else:
            raise PaperStarterError(f"unknown rich scorer {scorer}")
        scores[entity.identifier] = float(score)
    scorer_names = {"cohort_welch": "paper.cohorts.welch",
        "cohort_permutation": "paper.cohorts.permutation",
        "tree_likelihood": "paper.cohorts.tree_likelihood",
        "url_welch": "paper.urls.welch", "url_permutation": "paper.urls.permutation",
        "url_failure": "paper.urls.welch",
        "event_support": "paper.events.outcome_support"}
    scorer_name = scorer_names[scorer]
    output = _score_outputs(candidates, scores=scores, scorer=scorer_name,
        window=starter.telemetry.alert_window, statistic=scorer, support=supports, details=details,
        family={"cohort_welch": "client_cohort", "cohort_permutation": "client_cohort",
            "tree_likelihood": "client_cohort_tree", "url_welch": "url_analysis",
            "url_failure": "url_failure_rate",
            "url_permutation": "url_analysis", "event_support": "event_pattern"}[scorer])
    return OperatorResult(output, sum(supports.values()), 0)


def _flow_scorer(inputs: Mapping[str, Any], params: Mapping[str, Any], context: NodeContext) -> OperatorResult:
    starter = _input_from(inputs)
    urls = starter.urls
    if urls is None: raise PaperStarterError("URL capability is unsupported")
    candidates = _candidate_pairs(inputs.get("candidates"))
    url_values = sorted({row.url for row in urls})
    if not url_values:
        return OperatorResult(ScoredCandidates((), "paper.url_flow", "abstained",
            {"reason": "no_url_observations"}), 0, 0)
    before, after = [], []
    kpi_before, kpi_after = [], []
    # Rows are aligned by session and timestamp; undeclared targets are excluded.
    sessions = starter.sessions or ()
    session_kpi_rows: dict[tuple[str, str], list[float]] = defaultdict(list)
    session_kpi_timestamps: dict[tuple[str, str], int] = {}
    for row in sessions:
        if row.kpi_value is None: continue
        window = "reference" if starter.telemetry.reference_window.contains(row.timestamp_ms_utc) else (
            "alert" if starter.telemetry.alert_window.contains(row.timestamp_ms_utc) else None)
        if window is not None:
            identity = (row.session_id, window)
            session_kpi_rows[identity].append(float(row.kpi_value))
            session_kpi_timestamps[identity] = min(
                row.timestamp_ms_utc, session_kpi_timestamps.get(identity, row.timestamp_ms_utc))
    session_kpi = {key: median(values) for key, values in session_kpi_rows.items()}
    grouped: dict[tuple[str, str], list[PaperURLObservation]] = defaultdict(list)
    for row in urls: grouped[(row.session_id, row.url)].append(row)
    session_ids = sorted({sid for sid, _ in grouped} & {sid for sid, _window in session_kpi})
    for window, matrix, target in ((starter.telemetry.reference_window, before, kpi_before),
                                   (starter.telemetry.alert_window, after, kpi_after)):
        window_sessions = sorted((sid for sid in session_ids if (sid, window.name) in session_kpi),
            key=lambda sid: (session_kpi_timestamps[(sid, window.name)], sid))
        for sid in window_sessions:
            rows = [row for (session, _url), observations in grouped.items() if session == sid
                    for row in observations if window.contains(row.timestamp_ms_utc) and row.duration is not None]
            by_url = {url: median([r.duration for r in rows if r.url == url])
                      for url in url_values if any(r.url == url for r in rows)}
            if len(by_url) != len(url_values): continue
            matrix.append([float(by_url[url]) for url in url_values])
            target.append(float(session_kpi[(sid, window.name)]))
    fit = fit_flow_contributions(before, kpi_before, after, kpi_after,
        component_names=url_values, fit_mode=str(params["fit_mode"]),
        validation_fraction=float(params["validation_fraction"]),
        minimum_validation_samples=int(params["minimum_validation_samples"]),
        minimum_validation_skill=float(params["minimum_validation_skill"]),
        maximum_validation_normalized_error=float(params["maximum_validation_normalized_error"]),
        residual_scale_floor=float(params["residual_scale_floor"]), ridge=float(params["ridge"]),
        max_condition=float(params["max_condition"]),
        maximum_relative_residual=float(params["maximum_relative_residual"]),
        minimum_rows=int(params["minimum_rows"]))
    values = dict(fit.changes)
    score_map = {item.identifier: max(0.0, values.get(item.url or "", 0.0)) for item in candidates}
    output = _score_outputs(candidates, scores=score_map, scorer="paper.urls.flow_contribution",
        window=starter.telemetry.alert_window, statistic="flow_contribution_change",
        support={item.identifier: fit.support for item in candidates},
        details={item.identifier: {"fit_status": fit.status, "fit_reason": fit.reason,
            "contribution_change": values.get(item.url or item.identifier, 0.0), **dict(fit.details)}
            for item in candidates}, family="url_flow")
    return OperatorResult(output, fit.support, 0)


def _dependency_adjust(inputs: Mapping[str, Any], params: Mapping[str, Any],
                       context: NodeContext) -> OperatorResult:
    starter = _input_from(inputs)
    source: ScoredCandidates = inputs["scores"]
    scores = {item.entity.identifier: item.score for item in source.candidates}
    id_to_component = {identifier: identifier for identifier in scores
                       if identifier in starter.telemetry.deployment_catalog}
    edges = set()
    spans_by_trace: dict[str, list[Any]] = defaultdict(list)
    for span in starter.telemetry.spans: spans_by_trace[span.trace_id].append(span)
    skipped = Counter()
    valid_trace_count = 0
    for _trace_id, spans in spans_by_trace.items():
        ids = [span.span_id for span in spans]
        if len(set(ids)) != len(ids):
            skipped["duplicate_span_ids"] += 1
            continue
        by_id = {span.span_id: span for span in spans}
        skipped["unresolved_external_parent_edges"] += sum(
            span.parent_id is not None and span.parent_id not in by_id for span in spans)
        color: dict[str, int] = {}
        def visit(span_id: str) -> bool:
            if color.get(span_id) == 1: return False
            if color.get(span_id) == 2: return True
            color[span_id] = 1
            parent_id = by_id[span_id].parent_id
            if parent_id in by_id and not visit(parent_id): return False
            color[span_id] = 2
            return True
        if not all(visit(span_id) for span_id in ids):
            skipped["cyclic_trace_graph"] += 1
            continue
        valid_trace_count += 1
        for span in spans:
            if not span.parent_resolved_in_clip or span.parent_id is None or span.parent_id not in by_id: continue
            parent = by_id[span.parent_id]
            if parent.executing_component is None or span.executing_component is None: continue
            caller, callee = parent.executing_component.identifier, span.executing_component.identifier
            if caller != callee and caller in id_to_component and callee in id_to_component:
                edges.add((caller, callee))
    result = adjust_dependency_scores(scores, sorted(edges),
        downstream_penalty=float(params["downstream_penalty"]))
    adjusted = dict(result.scores) if result.status == "ok" else scores
    candidates = tuple(ScoredCandidate(item.entity, adjusted.get(item.entity.identifier, item.score),
        item.category, item.evidence + (Evidence("bank/trace_span.csv", starter.telemetry.alert_window,
            "one_hop_caller_penalty", adjusted.get(item.entity.identifier, item.score),
            (item.entity,), sum(e.support for e in item.evidence),
            {"status": result.status, "reason": result.reason,
             "edges_used": tuple(result.details.get("edges_used", ())),
             "source_tables": ("bank/trace_span.csv",),
             "ranking_eligible": any(e.details.get("ranking_eligible") is True for e in item.evidence),
             "causal": False}),),
        item.analysis_family) for item in source.candidates)
    value = ScoredCandidates(candidates, "paper.dependency_adjust", source.status,
        {"source_scorer": (source.status_details.get("source_scorer")
            if source.scorer == "paper.dependency_adjust" else source.scorer),
         "trusted_source_scorer": (source.status_details.get("trusted_source_scorer",
            source.status_details.get("source_scorer")) if source.scorer == "paper.dependency_adjust"
            else source.scorer),
         "status": result.status,
         "reason": result.reason, "valid_trace_graph_count": valid_trace_count,
         "skipped_trace_counts": dict(skipped), **dict(result.details)})
    return OperatorResult(value, len(edges), 0)


def _bank_metric_tree(inputs: Mapping[str, Any], params: Mapping[str, Any],
                      context: NodeContext) -> OperatorResult:
    """Per-component historical metric model aligned with app-MRT contribution shift."""
    starter = _input_from(inputs)
    telemetry = starter.telemetry
    candidates = _candidate_pairs(inputs.get("candidates"))
    if not all(isinstance(item, Candidate) and item.entity.role == "component" for item in candidates):
        raise PaperStarterError("Bank component tree scorer requires exact component CandidateSet")
    app_field = str(params["app_kpi_field"])
    target_rows: dict[tuple[str, int], list[float]] = defaultdict(list)
    for point in telemetry.metrics:
        if point.table == "bank/metric_app.csv" and point.metric_name == app_field and point.value is not None:
            target_rows[(point.entity.identifier, point.timestamp_ms_utc)].append(float(point.value))
    metric_rows: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    for point in telemetry.metrics:
        if (point.table == "bank/metric_container.csv" and point.value is not None and
                point.metric_name in _rankable_component_metric_fields() and
                (not params["component_metric"] or point.metric_name == params["component_metric"])):
            metric_rows[(point.entity.identifier, point.metric_name, point.timestamp_ms_utc)].append(float(point.value))
    allowed_fields = _rankable_component_metric_fields()
    metric_names = sorted(set(metric for _component, metric, _timestamp in metric_rows) & set(allowed_fields))
    if params["component_metric"]:
        if params["component_metric"] not in allowed_fields:
            raise PaperStarterError("component metric is not in the frozen Bank ranking allowlist")
        metric_names = [params["component_metric"]]
    scores: dict[str, float] = defaultdict(float)
    supports: dict[str, int] = defaultdict(int)
    reports: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for component in (item.entity.identifier for item in candidates):
        for metric_name in metric_names:
            feature_name = f"{component}|{metric_name}"
            for target in sorted({key[0] for key in target_rows}):
                aligned = []
                for (target_id, timestamp), target_values in target_rows.items():
                    if target_id != target: continue
                    values = metric_rows.get((component, metric_name, timestamp))
                    if not values: continue
                    window = "reference" if telemetry.reference_window.contains(timestamp) else (
                        "alert" if telemetry.alert_window.contains(timestamp) else None)
                    if window:
                        aligned.append((timestamp, window, median(target_values), median(values)))
                before = sorted((row for row in aligned if row[1] == "reference"), key=lambda row: row[0])
                after = sorted((row for row in aligned if row[1] == "alert"), key=lambda row: row[0])
                if min(len(before), len(after)) < int(params["minimum_samples"]):
                    reports[component].append({"app_kpi_identity": target, "metric_field": metric_name,
                        "status": "abstain", "reason": "insufficient_component_metric_kpi_timestamp_alignment",
                        "aligned_rows_before": len(before), "aligned_rows_after": len(after)})
                    continue
                result = tree_prediction_contributions(
                    [[row[3]] for row in before], [row[2] for row in before],
                    [[row[3]] for row in after], [row[2] for row in after],
                    feature_names=[feature_name], feature_groups={component: [feature_name]},
                    max_depth=int(params["max_depth"]),
                    min_samples_leaf=int(params["min_samples_leaf"]),
                    minimum_samples=int(params["minimum_samples"]),
                    validation_fraction=float(params["validation_fraction"]),
                    minimum_validation_samples=int(params["minimum_validation_samples"]),
                    minimum_validation_r2=float(params["minimum_validation_r2"]),
                    maximum_validation_normalized_error=float(params["maximum_validation_normalized_error"]),
                    residual_scale_floor=float(params["residual_scale_floor"]),
                    minimum_observed_kpi_change=float(params["minimum_observed_kpi_change"]),
                    minimum_contribution_fraction=float(params["minimum_contribution_fraction"]),
                    seed=int(params["seed"]))
                report_map = dict(result.details.get("feature_group_models", ()))
                value = dict(result.scores).get(component, 0.0)
                scores[component] += float(value)
                supports[component] += result.support
                reports[component].append({"app_kpi_identity": target, "status": result.status,
                    "reason": result.reason, "group_result": report_map.get(component, {}),
                    "aligned_rows_before": len(before), "aligned_rows_after": len(after),
                    "feature": feature_name, **dict(result.details)})
    output_scores = dict(scores)
    output_supports = dict(supports)
    details = {item.entity.identifier: {"models": tuple(reports.get(item.entity.identifier, ())),
        "eligible_model_count": len(reports.get(item.entity.identifier, ())),
        "interpretation": "component-specific heldout tree prediction shift aligned to observed app MRT shift; correlated groups may overlap, not causal or summable",
        "source_tables": ("bank/metric_container.csv", "bank/metric_app.csv"),
        "ranking_eligible": output_scores.get(item.entity.identifier, 0.0) > 0 and output_supports.get(item.entity.identifier, 0) > 0}
        for item in candidates}
    any_usable = any(value > 0 for value in output_scores.values())
    output = _score_outputs(candidates, scores=scores, scorer="paper.bank.metric_tree_likelihood",
        window=telemetry.alert_window, statistic="predicted_kpi_contribution_shift", support=output_supports,
        details=details, family="paper.bank.metric_tree_likelihood",
        status="ready" if any_usable else "abstained",
        status_details={} if any_usable else {"reason": "no_component_metric_prediction_passed_baseline_validation_and_alignment_gates"})
    return OperatorResult(output, sum(output_supports.values()), 0)


def _trace_flow_adaptation(inputs: Mapping[str, Any], params: Mapping[str, Any],
                           context: NodeContext) -> OperatorResult:
    """Associate non-root inclusive span durations with explicit root durations."""
    starter = _input_from(inputs)
    telemetry = starter.telemetry
    candidates = _candidate_pairs(inputs.get("candidates"))
    spans_by_trace: dict[str, list[Any]] = defaultdict(list)
    for span in telemetry.spans: spans_by_trace[span.trace_id].append(span)
    invalid: Counter[str] = Counter()
    trace_rows: dict[str, list[tuple[str, int, float, dict[str, float]]]] = defaultdict(list)
    for trace_id, spans in spans_by_trace.items():
        ids = [span.span_id for span in spans]
        if len(set(ids)) != len(ids): invalid["duplicate_span_ids"] += 1; continue
        by_id = dict(zip(ids, spans))
        roots = [span for span in spans if span.parent_id is None]
        if len(roots) != 1: invalid["nonunique_explicit_root"] += 1; continue
        root_span = roots[0]
        if root_span.executing_component is None: invalid["root_component_missing"] += 1; continue
        if not telemetry.reference_window.contains(root_span.start_ms_utc) and not telemetry.alert_window.contains(root_span.start_ms_utc):
            invalid["root_outside_windows"] += 1; continue
        if any(span.parent_id is not None and span.parent_id not in by_id for span in spans):
            invalid["unresolved_parent"] += 1; continue
        color: dict[str, int] = {}
        def visit(span_id: str) -> bool:
            if color.get(span_id) == 1: return False
            if color.get(span_id) == 2: return True
            color[span_id] = 1
            child_ids = [span.span_id for span in spans if span.parent_id == span_id]
            if any(not visit(child) for child in child_ids): return False
            color[span_id] = 2
            return True
        if not visit(root_span.span_id) or len(color) != len(spans):
            invalid["cycle_or_disconnected_parent_graph"] += 1; continue
        try: root_duration = float(root_span.elapsed_raw)
        except (TypeError, ValueError): invalid["invalid_root_duration"] += 1; continue
        if not math.isfinite(root_duration) or root_duration < 0:
            invalid["invalid_root_duration"] += 1; continue
        root_component = root_span.executing_component.identifier
        component_durations: dict[str, float] = defaultdict(float)
        bad = False
        for span in spans:
            if span is root_span or span.executing_component is None: continue
            try: duration = float(span.elapsed_raw)
            except (TypeError, ValueError): bad = True; break
            if not math.isfinite(duration) or duration < 0: bad = True; break
            component_durations[span.executing_component.identifier] += duration
        if bad: invalid["invalid_component_duration"] += 1; continue
        trace_rows[root_component].append((trace_id, root_span.start_ms_utc, root_duration, component_durations))

    scores: dict[str, float] = {}
    supports: dict[str, int] = {}
    details: dict[str, dict[str, Any]] = {}
    candidate_ids = {item.entity.identifier for item in candidates}
    model_reports: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for root_component, rows in trace_rows.items():
        available = sorted({component for _tid, _timestamp, _duration, mapping in rows
                            for component in mapping if component in candidate_ids and component != root_component})
        if not available: continue
        coverage = {component: sum(component in row[3] for row in rows) / len(rows)
                    for component in available}
        selected = [component for component in available
                    if coverage[component] >= float(params["minimum_component_coverage"])]
        # Joint complete cases preserve a shared multicomponent design; missing
        # components are never imputed as zero.
        complete = [row for row in rows if selected and all(name in row[3] for name in selected)]
        before = [row for row in complete if telemetry.reference_window.contains(row[1])]
        after = [row for row in complete if telemetry.alert_window.contains(row[1])]
        if not selected or min(len(before), len(after)) < int(params["minimum_rows"]):
            for component in selected:
                model_reports[component].append({"root_component": root_component,
                    "status": "abstain", "reason": "insufficient_joint_complete_rows",
                    "component_coverage": coverage[component], "feature_count": len(selected)})
            continue
        result = fit_flow_contributions(
            [[row[3][name] for name in selected] for row in before], [row[2] for row in before],
            [[row[3][name] for name in selected] for row in after], [row[2] for row in after],
            component_names=selected, fit_mode=str(params["fit_mode"]),
            validation_fraction=float(params["validation_fraction"]),
            minimum_validation_samples=int(params["minimum_validation_samples"]),
            minimum_validation_skill=float(params["minimum_validation_skill"]),
            maximum_validation_normalized_error=float(params["maximum_validation_normalized_error"]),
            residual_scale_floor=float(params["residual_scale_floor"]),
            ridge=float(params["ridge"]), max_condition=float(params["max_condition"]),
            maximum_relative_residual=float(params["maximum_relative_residual"]),
            minimum_rows=int(params["minimum_rows"]))
        changes = dict(result.changes)
        for component in selected:
            model_reports[component].append({"root_component": root_component,
                "status": result.status, "reason": result.reason, "support": result.support,
                "contribution_change": changes.get(component),
                "component_coverage": coverage[component], "joint_features": tuple(selected),
                **dict(result.details)})
    for item in candidates:
        component = item.entity.identifier
        reports = model_reports.get(component, [])
        deltas = [float(row["contribution_change"]) for row in reports
                  if row.get("status") == "ok" and row.get("contribution_change") is not None]
        scores[component] = max(0.0, sum(deltas) / len(deltas)) if deltas else 0.0
        supports[component] = sum(int(row.get("support", 0)) for row in reports)
        details[component] = {"models": tuple(reports),
            "duration_unit": "undeclared native source unit; within-field comparisons only",
            "root_features_excluded": True, "explicit_unique_root_required": True,
            "parent_closure_does_not_establish_trace_completeness": True,
            "missing_features": "joint complete cases only; never imputed as zero",
            "inclusive_duration_association": True, "exclusive_time_or_causality_claimed": False,
            "source_tables": ("bank/trace_span.csv",),
            "ranking_eligible": scores.get(component, 0.0) > 0 and supports.get(component, 0) > 0}
    usable = any(details[key]["models"] for key in details)
    output = _score_outputs(candidates, scores=scores, scorer="paper.bank.trace_root_flow",
        window=telemetry.alert_window, statistic="inclusive_duration_contribution_change",
        support=supports, details=details, family="paper.bank.trace_root_flow",
        status="ready" if usable else "abstained",
        status_details={"invalid_trace_counts": dict(invalid),
            "reason": None if usable else "no_trace_model_passed_support_and_validation"})
    return OperatorResult(output, sum(supports.values()), 0)


def validate_log_pattern_configuration(params: Mapping[str, Any]) -> dict[str, Any]:
    """Validate that Monte Carlo resolution can reach the Bonferroni cutoff.

    The per-hypothesis cutoff is alpha / maximum_patterns. A plus-one Monte
    Carlo p-value has minimum 1 / (resamples + 1); reject configurations whose
    declared maximum family cannot reach that cutoff. Controller DAG validation
    and direct operator execution share this trusted check.
    """
    alpha = params.get("alpha")
    maximum_patterns = params.get("maximum_patterns")
    resamples = params.get("resamples")
    if (type(alpha) not in (int, float) or not math.isfinite(float(alpha)) or
            not 0.0 < float(alpha) <= 1.0):
        raise PaperStarterError("log-pattern alpha must be finite and in (0,1]")
    if type(maximum_patterns) is not int or maximum_patterns < 1:
        raise PaperStarterError("log-pattern maximum_patterns must be a positive integer")
    if type(resamples) is not int or resamples < 1:
        raise PaperStarterError("log-pattern resamples must be a positive integer")
    local_alpha = float(alpha) / maximum_patterns
    minimum_p = 1.0 / (resamples + 1)
    minimum_resamples = math.ceil(maximum_patterns / float(alpha)) - 1
    if minimum_p > local_alpha:
        raise PaperStarterError(
            "log-pattern Monte Carlo p-value resolution cannot reach the declared Bonferroni cutoff; "
            f"resamples must be at least {minimum_resamples} for maximum_patterns={maximum_patterns} "
            f"and alpha={float(alpha):g}")
    return {"local_alpha": local_alpha, "minimum_monte_carlo_p": minimum_p,
            "minimum_resamples": minimum_resamples}


def _bank_log_pattern_contrast(inputs: Mapping[str, Any], params: Mapping[str, Any],
                               context: NodeContext) -> OperatorResult:
    """Compare label-blind discovered hashed templates by per-block composition."""
    resolution = validate_log_pattern_configuration(params)
    starter = _input_from(inputs)
    telemetry = starter.telemetry
    candidates = _candidate_pairs(inputs.get("candidates"))
    patterns = starter.native_log_patterns
    if starter.capabilities.get("native_log_patterns") != "available":
        return OperatorResult(ScoredCandidates((), "paper.bank.log_pattern_contrast", "abstained",
            {"reason": "native_log_pattern_summary_unsupported_or_capacity_exceeded"}), 0, 0)
    if any(row.block_duration_ms != starter.analysis_block_duration_ms for row in patterns):
        raise PaperStarterError("native log summaries use a different configured block duration")
    pooled: Counter[tuple[str, str, tuple[str, ...]]] = Counter()
    for row in patterns:
        for size in range(2, min(int(params["maximum_pattern_length"]), len(row.tokens)) + 1):
            for start in range(len(row.tokens) - size + 1):
                pooled[(row.component_id, row.log_name, row.tokens[start:start + size])] += row.row_support
    # Discovery is pooled across both windows; alert/reference labels do not select patterns.
    distinct = sorted({(name, pattern) for _component, name, pattern in pooled})
    group_rows: dict[tuple[str, str, str, int], list[PaperLogPatternSummary]] = defaultdict(list)
    for row in patterns:
        group_rows[(row.component_id, row.log_name, row.window.name, row.block_start_ms_utc)].append(row)
    group_denominators: dict[tuple[str, str, str, int], int] = {}
    for key, rows in group_rows.items():
        group_denominators[key] = max((row.block_row_count for row in rows), default=0)
    scores, supports, details = {}, {}, {}
    alpha = float(params["alpha"])
    total_resolution_limited = 0
    total_tested_patterns = 0
    for candidate in candidates:
        entity, _ = _candidate_descriptor(candidate)
        component = entity.identifier
        eligible_results = []
        examples = []
        support_total = 0
        resolution_limited = 0
        exact_tests = 0
        monte_carlo_tests = 0
        tested_patterns = 0
        for log_name in sorted({name for comp, name, _pattern in pooled if comp == component}):
            local_patterns = [pattern for comp, name, pattern in sorted(pooled,
                key=lambda key: (-pooled[key], key[2])) if comp == component and name == log_name
                and pooled[(comp, name, pattern)] >= int(params["minimum_pattern_support"])]
            local_patterns = local_patterns[:int(params["maximum_patterns"])]
            if not local_patterns: continue
            numerator: dict[tuple[tuple[str, ...], str, int], int] = defaultdict(int)
            for row in patterns:
                if row.component_id != component or row.log_name != log_name: continue
                found = token_sequence_matches(row.tokens, local_patterns,
                    sequence_length=len(row.tokens), max_patterns=int(params["maximum_patterns"]),
                    max_pattern_length=int(params["maximum_pattern_length"]))
                matched = {local_patterns[pattern_index] for pattern_index, _start, _end in found}
                for pattern in matched:
                    numerator[(pattern, row.window.name, row.block_start_ms_utc)] += row.row_support
            block_ms = starter.analysis_block_duration_ms
            before_blocks = [telemetry.reference_window.start_ms_utc + i * block_ms
                for i in range((telemetry.reference_window.end_ms_utc - telemetry.reference_window.start_ms_utc) // block_ms)]
            after_blocks = [telemetry.alert_window.start_ms_utc + i * block_ms
                for i in range((telemetry.alert_window.end_ms_utc - telemetry.alert_window.start_ms_utc) // block_ms)]
            local_alpha = alpha / max(1, len(local_patterns))
            for pattern in local_patterns:
                before_rates, after_rates = [], []
                for before_block, after_block in zip(before_blocks, after_blocks):
                    before_key = (component, log_name, "reference", before_block)
                    after_key = (component, log_name, "alert", after_block)
                    d0, d1 = group_denominators.get(before_key, 0), group_denominators.get(after_key, 0)
                    if d0 <= 0 or d1 <= 0: continue
                    before_rates.append(numerator.get((pattern, "reference", before_block), 0) / d0)
                    after_rates.append(numerator.get((pattern, "alert", after_block), 0) / d1)
                if min(len(before_rates), len(after_rates)) < int(params["minimum_blocks"]): continue
                support_total += len(before_rates) + len(after_rates)
                exact_count = math.comb(len(before_rates) + len(after_rates), len(before_rates))
                if exact_count <= int(params["resamples"]):
                    exact_tests += 1
                    # Paired block windows have equal support. Every nonzero
                    # two-sided exact contrast includes the observed assignment
                    # and its complementary assignment among the extremes.
                    exact_p_floor = (2.0 / exact_count) if exact_count > 1 else 1.0
                    if exact_p_floor > local_alpha:
                        resolution_limited += 1
                        total_resolution_limited += 1
                        continue
                else:
                    monte_carlo_tests += 1
                tested_patterns += 1
                total_tested_patterns += 1
                result = permutation_test(before_rates, after_rates,
                    seed=stable_seed(telemetry.case_id, component, log_name,
                        hashlib.sha256("\0".join(pattern).encode("utf-8")).hexdigest()),
                    resamples=int(params["resamples"]), minimum_blocks=int(params["minimum_blocks"]),
                    relative_scale_floor=float(params["relative_scale_floor"]),
                    absolute_scale_floor=float(params["absolute_scale_floor"]), statistic="mean", quantile=0.9)
                value = max(0.0, float(result.effect or 0.0)) if result.status == "ok" and (result.p_value or 1.0) <= local_alpha else 0.0
                eligible_results.append(value)
                if value > 0:
                    examples.append({"tokens": pattern,
                        "pattern_sha256": hashlib.sha256("\0".join(pattern).encode("utf-8")).hexdigest(),
                        "effect": result.effect, "p_value": result.p_value,
                        "adjusted_alpha": local_alpha,
                        "before_block_rates": tuple(before_rates), "alert_block_rates": tuple(after_rates),
                        "support_blocks": min(result.support_before, result.support_after)})
        scores[component] = max(eligible_results, default=0.0)
        supports[component] = support_total
        details[component] = {"patterns_discovered_globally": len(distinct),
            "patterns_tested": tested_patterns,
            "patterns_limited_by_exact_p_resolution": resolution_limited,
            "exact_permutation_tests": exact_tests,
            "monte_carlo_tests": monte_carlo_tests,
            "permutation_resolution": {**resolution, "maximum_patterns": int(params["maximum_patterns"])},
            "block_minutes": starter.analysis_block_duration_ms // 60_000,
            "denominator": "all rows in the same component/log_name/time block",
            "multiplicity": "Bonferroni within each component/log_name pattern family",
            "discovery": "pooled across both windows without labels",
            "matching": "Aho-Corasick inside one normalized log-line template; no cross-line/cross-window matching",
            "normalized_template_examples": tuple(sorted(examples,
                key=lambda row: (-float(row.get("effect") or 0.0), row["pattern_sha256"]))[:3]),
            "raw_log_text_returned": False,
            "source_tables": ("bank/log/log_service.csv",),
            "ranking_eligible": scores.get(component, 0.0) > 0 and supports.get(component, 0) > 0}
    usable = any(value > 0 for value in scores.values())
    reason = ("exact_permutation_resolution_coarser_than_local_alpha"
              if not usable and total_resolution_limited and not total_tested_patterns else
              None if usable else "no_pattern_passed_support_and_multiple_test_threshold")
    output = _score_outputs(candidates, scores=scores, scorer="paper.bank.log_pattern_contrast",
        window=telemetry.alert_window, statistic="block_pattern_rate", support=supports,
        details=details, family="paper.bank.log_pattern_contrast",
        status="ready" if usable else "abstained",
        status_details={"reason": reason,
            "exact_resolution_limited_pattern_count": total_resolution_limited})
    return OperatorResult(output, sum(supports.values()), 0)


def _weighted_consensus(inputs: Mapping[str, Any], params: Mapping[str, Any],
                        context: NodeContext, *,
                        generated_semantics: Mapping[str, tuple[str, str]] | None = None) -> OperatorResult:
    score_sets = inputs.get("scores", ())
    if isinstance(score_sets, ScoredCandidates): score_sets = (score_sets,)
    score_sets = tuple(score_sets)
    weights_raw = params.get("family_weights", {})
    family_weights = normalize_consensus_weights(weights_raw)
    semantic_groups = PAPER_SCORER_GROUPS
    generated_semantics = generated_semantics or {}
    if not family_weights:
        family_weights = {group: 1.0 for group in PAPER_CONSENSUS_GROUPS}
    votes = []
    evidence_by_key: dict[str, list[Evidence]] = defaultdict(list)
    entities: dict[str, EntityRef] = {}
    vote_metadata: dict[str, Any] = {}
    for scores in score_sets:
        if not isinstance(scores, ScoredCandidates) or scores.status == "abstained": continue
        group = semantic_groups.get(scores.scorer)
        computation_fingerprint = scores.scorer
        if scores.scorer == "paper.dependency_adjust":
            source_scorer = scores.status_details.get("trusted_source_scorer",
                scores.status_details.get("source_scorer"))
            group = semantic_groups.get(source_scorer) if isinstance(source_scorer, str) else None
            source_computation = source_scorer
            if group is None and isinstance(source_scorer, str) and source_scorer in generated_semantics:
                group, source_computation = generated_semantics[source_scorer]
            if group is None:
                raise PaperStarterError("dependency adjustment lacks a trusted registered source scorer")
            computation_fingerprint = digest_value({"source_computation": source_computation,
                "edges_used": scores.status_details.get("edges_used", ()),
                "downstream_penalty": scores.status_details.get("downstream_penalty")})
        if group is None and scores.scorer in generated_semantics:
            group, computation_fingerprint = generated_semantics[scores.scorer]
        if group is None:
            raise PaperStarterError(f"scorer {scores.scorer!r} has no trusted semantic consensus group")
        raw_values = {}
        metadata = []
        values = []
        for item in scores.candidates:
            key = f"{item.entity.role}:{item.entity.identifier}"
            entities[key] = item.entity
            raw_values[key] = max(0.0, item.score)
            evidence_by_key[key].extend(item.evidence)
            metadata.extend((key, evidence.window.name, evidence.support,
                tuple(evidence.details.get("source_tables", ()))) for evidence in item.evidence)
        maximum = max(raw_values.values(), default=0.0)
        values = [(key, (value / maximum if maximum > 0 else 0.0))
                  for key, value in sorted(raw_values.items())]
        fingerprint = digest_value({"semantic_group": group, "trusted_computation": computation_fingerprint,
            "input_data_sha256": context.data_sha256, "normalized_scores": values})
        metadata_digest = digest_value(sorted(metadata))
        previous = vote_metadata.get(fingerprint)
        if previous is not None and previous != metadata_digest:
            raise PaperStarterError(f"duplicate normalized consensus votes have conflicting evidence metadata for {scores.scorer}/{group}")
        vote_metadata[fingerprint] = metadata_digest
        votes.append(VoteRecord(group, fingerprint, tuple(values)))
    result = combine_votes(votes, family_weights=family_weights,
        tie_tolerance=float(params["tie_tolerance"]))
    scores_by_key = dict(result.scores)
    leads = []
    ordered_keys = sorted(scores_by_key, key=lambda key: (-scores_by_key[key],
        entities[key].canonical_id, entities[key].role))
    for key in ordered_keys[:int(params["limit"])]:
        if scores_by_key[key] <= 0:
            continue
        entity = entities[key]
        evidences = tuple(sorted((Evidence(e.source, e.window, e.statistic, e.value,
            e.contributing_entities, e.support, {**dict(e.details),
                "consensus_uncertainty": "top scores are tied within declared tolerance"
                    if result.status == "ambiguous" else None})
            for e in evidence_by_key[key]), key=lambda e: (e.source, e.window.name, e.statistic)))
        leads.append(ScoredCandidate(entity, scores_by_key[key], None, evidences,
                                     "paper.weighted_consensus"))
    answer = RankedAnswer(tuple(leads))
    return OperatorResult(answer, len(leads), 0)


def _register_op(registry: OperatorRegistry, name: str, output: str,
                 inputs: tuple[PortSpec, ...], params: Mapping[str, ParameterSpec],
                 impl, description: str) -> None:
    def checked_implementation(values, resolved_params, context):
        for parameter_name, value in resolved_params.items():
            spec = params.get(parameter_name)
            if spec is not None:
                _parameter_contract_error(spec, value)
        return impl(values, resolved_params, context)

    registry.register(OperatorSpec(name, "1", inputs, output, params,
        family=name.removeprefix("paper."), implementation=checked_implementation,
        description=description))


def register_paper_starter(registry: OperatorRegistry, *,
        bank_only: bool = False, include_conditional_dependencies: bool = True,
        input_kwargs: Mapping[str, Any] | None = None,
        starter_input: PaperStarterInput | None = None,
        generated_specs: Sequence[Mapping[str, Any]] = (),
        audit_dir: Path | None = None,
        root: Path = ROOT) -> OperatorRegistry:
    """Register the reconstructed 19-row catalog, optionally filtered for Bank."""
    input_kwargs = dict(input_kwargs or {})
    runtime_policy = json.loads((root / "config/paper_starter_runtime.json").read_text(encoding="utf-8"))
    metric_policy_digest = load_bank_metric_policy(
        root / "config/bank_metric_kind_policy.json").sha256
    input_spec = OperatorSpec("paper.input.prepare", "1", (PortSpec("data", "Telemetry"),),
        "PaperStarterInput", {"analysis_block_minutes": ParameterSpec("analysis_block_minutes", "positive_integer",
            default=int(runtime_policy["analysis_block_minutes"]), has_default=True,
            constraints={"minimum": 1, "maximum": 60})},
        implementation=lambda i, p, c: _prepare_paper_input(i["data"], p,
            input_kwargs=input_kwargs, starter_input=starter_input), family="paper_data_access",
        description="Build typed optional capabilities and hash-verified local log counts; no raw log text is returned.")
    registry.register(input_spec)

    names = []
    native_metric_selector_choices = ("", *tuple(sorted(_rankable_component_metric_fields())))
    native_metric_rule_choices = ("", *tuple(_rankable_component_metric_rule_refs()))
    candidate_params = {
        "paper.cohorts.attribute": _params(minimum_support=("positive_integer", 2)),
        "paper.cohorts.conjunction": _params(minimum_support=("positive_integer", 2),
            maximum_order=("positive_integer", 3, {"minimum": 2})),
        "paper.cohorts.activity": _params(minimum_support=("positive_integer", 2)),
        "paper.backend.metric_conditions": _params(minimum_samples=("positive_integer", 2),
            minimum_effect=("number", 0.0, {"minimum": 0.0}),
            metric=("string", "")),
        "paper.events.subsequences": _params(minimum_support=("positive_integer", 2), maximum_length=("positive_integer", 4), maximum_candidates=("positive_integer", 64)),
        "paper.urls.failure_conditions": _params(minimum_support=("positive_integer", 2), minimum_change=("number", 0.05, {"minimum": 0.0})),
        "paper.urls.delay_conditions": _params(minimum_support=("positive_integer", 2), minimum_change=("number", 1.0, {"minimum": 0.0}),
            quantile=("number", 0.9, {"minimum": 0.0, "maximum": 1.0})),
    }
    candidate_params["paper.backend.metric_conditions"]["metric"] = ParameterSpec(
        "metric", "string", default="", has_default=True,
        choices=native_metric_selector_choices,
        description=("Exact component metric_container.kpi_name from the Bank metric catalog; empty means test allowlisted fields separately. "
            f"Catalog policy SHA-256: {metric_policy_digest}."))
    candidate_params["paper.backend.metric_conditions"]["metric_rule_ref"] = ParameterSpec(
        "metric_rule_ref", "string", default="", has_default=True,
        choices=native_metric_rule_choices,
        description=("Stable versioned source-rule reference; trusted host resolves every exact observed field in the selected case. "
            f"Catalog policy SHA-256: {metric_policy_digest}."))
    rich_candidates = {
        "paper.cohorts.attribute": ("cohort_attribute",),
        "paper.cohorts.conjunction": ("cohort_conjunction",),
        "paper.cohorts.activity": ("cohort_activity",),
        "paper.events.subsequences": ("event_pattern",),
        "paper.urls.failure_conditions": ("url_failure",),
        "paper.urls.delay_conditions": ("url_delay",),
    }
    if not bank_only:
        for name, (kind,) in rich_candidates.items():
            _register_op(registry, name, "PaperCandidateSet",
                (PortSpec("data", "PaperStarterInput"),), candidate_params[name],
                lambda i, p, c, kind=kind: _rich_candidates(i, p, c, kind),
                "Typed candidate discovery over declared client/session/event/URL fields.")
            names.append(name)
    _register_op(registry, "paper.backend.metric_conditions", "CandidateSet",
        (PortSpec("data", "PaperStarterInput"),), candidate_params["paper.backend.metric_conditions"],
        _metric_conditions, "Supported exact component IDs with native metric excursions.")
    names.append("paper.backend.metric_conditions")

    score_defaults = {"minimum_samples": ("positive_integer", 3),
        "alpha": ("number", 0.05, {"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True}),
        "resamples": ("positive_integer", 999),
        "absolute_scale_floor": ("number", 1e-9, {"minimum": 0.0, "exclusive_minimum": True}),
        "relative_scale_floor": ("number", 0.05, {"minimum": 0.0}),
        "quantile": ("number", 0.9, {"minimum": 0.0, "maximum": 1.0,
                                     "exclusive_minimum": True, "exclusive_maximum": True}),
        "metric": ("string", ""),
        "metric_rule_ref": ("string", "")}
    score_defaults["resamples"] = ("positive_integer", 999, {"minimum": 1})
    candidate_port = PortSpec("candidates", "CandidateSet")
    for name, method, statistic in (
            ("paper.backend.mean_welch", "welch", "mean"),
            ("paper.backend.location_permutation", "permutation", "median"),
            ("paper.backend.tail_permutation", "permutation", "quantile")):
        params = _params(**score_defaults)
        params["minimum_samples"] = ParameterSpec("minimum_samples", "positive_integer",
            default=3, has_default=True,
            constraints={"minimum": 2 if method == "welch" else 1})
        params["metric"] = ParameterSpec("metric", "string", default="", has_default=True,
            choices=native_metric_selector_choices,
            description=("Exact component metric field in the Bank metric catalog; empty means all allowlisted fields tested separately. "
                f"Catalog policy SHA-256: {metric_policy_digest}."))
        params["metric_rule_ref"] = ParameterSpec("metric_rule_ref", "string", default="", has_default=True,
            choices=native_metric_rule_choices,
            description=("Stable versioned source-rule reference; the trusted host resolves all exact field matches for this case. "
                f"Catalog policy SHA-256: {metric_policy_digest}."))
        _register_op(registry, name, "ScoredCandidates",
            (PortSpec("data", "PaperStarterInput"), candidate_port), params,
            lambda i, p, c, method=method, statistic=statistic: _score_backend(i, p, c, method, statistic),
            "Backend metric contrast with explicit support, scale, and statistic policy.")
        names.append(name)

    if not bank_only:
        rich_scores = [
            ("paper.cohorts.welch", "cohort_welch", _params(minimum_samples=("positive_integer", 3, {"minimum": 2}), alpha=("number", 0.05, {"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True}))),
            ("paper.cohorts.permutation", "cohort_permutation", _params(minimum_samples=("positive_integer", 3), alpha=("number", 0.05, {"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True}), resamples=("positive_integer", 999, {"minimum": 1}), relative_scale_floor=("number", 0.05, {"minimum": 0.0}), absolute_scale_floor=("number", 1e-9, {"minimum": 0.0, "exclusive_minimum": True}))),
            ("paper.cohorts.tree_likelihood", "tree_likelihood", _params(minimum_samples=("positive_integer", 8, {"minimum": 2}),
                max_depth=("positive_integer", 4), min_samples_leaf=("positive_integer", 3),
                validation_fraction=("number", 0.25, {"minimum": 0.0, "maximum": 1.0,
                    "exclusive_minimum": True, "exclusive_maximum": True}),
                minimum_validation_samples=("positive_integer", 3, {"minimum": 2}),
                minimum_validation_r2=("number", 0.0), maximum_validation_normalized_error=("number", 1.0, {"minimum": 0.0}),
                residual_scale_floor=("number", 1e-6, {"minimum": 0.0, "exclusive_minimum": True}),
                minimum_excess_nll_per_observation=("number", 0.05, {"minimum": 0.0}),
                seed=("integer", 0))),
            ("paper.urls.welch", "url_welch", _params(minimum_samples=("positive_integer", 3, {"minimum": 2}), alpha=("number", 0.05, {"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True}))),
            ("paper.urls.permutation", "url_permutation", _params(minimum_samples=("positive_integer", 3), alpha=("number", 0.05, {"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True}), resamples=("positive_integer", 999, {"minimum": 1}), relative_scale_floor=("number", 0.05, {"minimum": 0.0}), absolute_scale_floor=("number", 1e-9, {"minimum": 0.0, "exclusive_minimum": True}))),
            ("paper.events.outcome_support", "event_support", _params(minimum_samples=("positive_integer", 2), minimum_effect=("number", 0.0, {"minimum": 0.0}), maximum_patterns=("positive_integer", 64), maximum_pattern_length=("positive_integer", 16))),
        ]
        for name, scorer, params in rich_scores:
            _register_op(registry, name, "ScoredCandidates",
                (PortSpec("data", "PaperStarterInput"), PortSpec("candidates", "PaperCandidateSet")),
                params, lambda i, p, c, scorer=scorer: _score_rich(i, p, c, scorer),
                "Generic typed client/session/event/URL evidence scorer.")
            names.append(name)
        _register_op(registry, "paper.urls.flow_contribution", "ScoredCandidates",
            (PortSpec("data", "PaperStarterInput"), PortSpec("candidates", "PaperCandidateSet")),
            {"fit_mode": ParameterSpec("fit_mode", "string", default="historical", has_default=True,
                    choices=("historical", "pooled")),
             "validation_fraction": ParameterSpec("validation_fraction", "number", default=0.25, has_default=True,
                    constraints={"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True, "exclusive_maximum": True}),
             "minimum_validation_samples": ParameterSpec("minimum_validation_samples", "positive_integer", default=3,
                    has_default=True, constraints={"minimum": 2}),
             "minimum_validation_skill": ParameterSpec("minimum_validation_skill", "number", default=0.0, has_default=True),
             "maximum_validation_normalized_error": ParameterSpec("maximum_validation_normalized_error", "number", default=1.0,
                    has_default=True, constraints={"minimum": 0.0}),
             "residual_scale_floor": ParameterSpec("residual_scale_floor", "number", default=1e-6, has_default=True,
                    constraints={"minimum": 0.0, "exclusive_minimum": True}),
             "ridge": ParameterSpec("ridge", "number", default=1e-6, has_default=True,
                    constraints={"minimum": 0.0}),
             "max_condition": ParameterSpec("max_condition", "number", default=1e6, has_default=True,
                    constraints={"minimum": 0.0, "exclusive_minimum": True}),
             "maximum_relative_residual": ParameterSpec("maximum_relative_residual", "number", default=0.5,
                    has_default=True, constraints={"minimum": 0.0}),
             "minimum_rows": ParameterSpec("minimum_rows", "positive_integer", default=4, has_default=True)},
            _flow_scorer, "Shared additive URL flow model with separately checked fit residuals.")
        names.append("paper.urls.flow_contribution")

    _register_op(registry, "paper.scores.dependency_adjust", "ScoredCandidates",
        (PortSpec("data", "PaperStarterInput"), PortSpec("scores", "ScoredCandidates")),
        _params(downstream_penalty=("number", 0.5, {"minimum": 0.0, "maximum": 1.0})), _dependency_adjust,
        "Adjust an existing score set from observed trace-local caller/callee edges only; missing edges cause abstention.")
    generated_semantics = {}
    for spec in generated_specs:
        if spec.get("output_type") != "ScoredCandidates":
            continue
        name = spec.get("name")
        if not isinstance(name, str): continue
        profile_name = spec.get("authoring_profile")
        digest = (spec.get("program_sha256") or
            (spec.get("contract_hash") if profile_name == "typed-operator-code-v1" else None) or
            spec.get("source_sha256") or spec.get("signature_sha256"))
        if not isinstance(digest, str):
            # Normalized snapshot specs carry source-bound signatures; if no
            # fingerprint exists, the proposed scorer is not admitted here.
            continue
        group = ("generated_metric_ir" if profile_name in
            ("typed-operator-ir-v1", "typed-operator-ir-v2", "typed-operator-code-v1")
            else "generated_operator")
        generated_semantics[name] = (group, digest)
    _register_op(registry, "paper.answers.weighted_consensus", "RankedAnswer",
        (PortSpec("scores", "ScoredCandidates", variadic=True, minimum_items=1),),
        {"family_weights": ParameterSpec("family_weights", "number_map", default={},
            has_default=True, description=("Trusted consensus weight groups: " +
                ", ".join(PAPER_CONSENSUS_GROUPS) + "; accepted legacy aliases " +
                ", ".join(f"{alias}→{canonical}" for alias, canonical in PAPER_CONSENSUS_WEIGHT_ALIASES.items()) +
                f"; each weight must be between 0 and {PAPER_CONSENSUS_MAX_WEIGHT}, and aliases/canonical names may not duplicate a group.")),
         "tie_tolerance": ParameterSpec("tie_tolerance", "number", default=1e-9, has_default=True,
            constraints={"minimum": 0.0}),
         "limit": ParameterSpec("limit", "positive_integer", default=5, has_default=True)},
        _weighted_consensus,
        "Combine normalized independent scorer votes with explicit canonical group weights; historical Bank family names are aliases.")
    if generated_semantics:
        current = registry.require("paper.answers.weighted_consensus", "1")
        registry._operators[(current.name, current.version)] = replace(current,
            implementation=lambda i, p, c, generated=MappingProxyType(dict(generated_semantics)):
                _weighted_consensus(i, p, c, generated_semantics=generated))
    if bank_only:
        tree_params = _params(app_kpi_field=("string", "mrt"), component_metric=("string", ""),
            minimum_samples=("positive_integer", 8, {"minimum": 2}), max_depth=("positive_integer", 3),
            min_samples_leaf=("positive_integer", 3),
            validation_fraction=("number", 0.25, {"minimum": 0.0, "maximum": 1.0,
                "exclusive_minimum": True, "exclusive_maximum": True}),
            minimum_validation_samples=("positive_integer", 3, {"minimum": 2}),
            minimum_validation_r2=("number", 0.0),
            maximum_validation_normalized_error=("number", 1.0, {"minimum": 0.0}),
            residual_scale_floor=("number", 1e-6, {"minimum": 0.0, "exclusive_minimum": True}),
            minimum_observed_kpi_change=("number", 1.0, {"minimum": 0.0}),
            minimum_contribution_fraction=("number", 0.1, {"minimum": 0.0, "maximum": 1.0}),
            seed=("integer", 0))
        tree_params["app_kpi_field"] = ParameterSpec("app_kpi_field", "string",
            default="mrt", has_default=True, choices=("mrt",),
            description="Application KPI field on opaque tc identity; not a component metric.")
        tree_params["component_metric"] = ParameterSpec("component_metric", "string",
            default="", has_default=True, choices=native_metric_selector_choices,
            description="Exact allowlisted component metric field; empty tests allowlisted fields separately.")
        _register_op(registry, "paper.bank.metric_tree_likelihood", "ScoredCandidates",
            (PortSpec("data", "PaperStarterInput"), PortSpec("candidates", "CandidateSet")),
            tree_params, _bank_metric_tree,
            "Bank adaptation: per-component metric-to-app-MRT regression contribution with heldout validation; case eligibility is explicit and runtime abstains if unsupported.")
        trace_params = _params(fit_mode=("string", "historical"), minimum_rows=("positive_integer", 8),
            minimum_component_coverage=("number", 0.8, {"minimum": 0.0, "maximum": 1.0}),
            validation_fraction=("number", 0.25, {"minimum": 0.0, "maximum": 1.0,
                "exclusive_minimum": True, "exclusive_maximum": True}),
            minimum_validation_samples=("positive_integer", 3, {"minimum": 2}),
            minimum_validation_skill=("number", 0.0),
            maximum_validation_normalized_error=("number", 1.0, {"minimum": 0.0}),
            residual_scale_floor=("number", 1e-6, {"minimum": 0.0, "exclusive_minimum": True}),
            ridge=("number", 1e-6, {"minimum": 0.0}),
            max_condition=("number", 1e6, {"minimum": 0.0, "exclusive_minimum": True}),
            maximum_relative_residual=("number", 0.5, {"minimum": 0.0}))
        trace_params["fit_mode"] = ParameterSpec("fit_mode", "string", default="historical",
            has_default=True, choices=("historical", "pooled"))
        _register_op(registry, "paper.bank.trace_root_flow", "ScoredCandidates",
            (PortSpec("data", "PaperStarterInput"), PortSpec("candidates", "CandidateSet")),
            trace_params, _trace_flow_adaptation,
            "Bank adaptation: non-root inclusive span duration association to unique trace root durations; case eligibility is explicit and runtime abstains if unsupported.")
        log_params = _params(minimum_pattern_support=("positive_integer", 2), minimum_blocks=("positive_integer", 3),
            maximum_patterns=("positive_integer", 64), maximum_pattern_length=("positive_integer", 4, {"maximum": 256}),
            alpha=("number", 0.05, {"minimum": 0.0, "maximum": 1.0, "exclusive_minimum": True}),
            resamples=("positive_integer", 1999),
            relative_scale_floor=("number", 0.05, {"minimum": 0.0}),
            absolute_scale_floor=("number", 1e-9, {"minimum": 0.0, "exclusive_minimum": True}))
        log_params["alpha"] = ParameterSpec("alpha", "number", default=0.05,
            has_default=True, description="Family-wise error rate; per-pattern cutoff is alpha / maximum_patterns.")
        log_params["maximum_patterns"] = ParameterSpec("maximum_patterns", "positive_integer",
            default=64, has_default=True,
            description="Maximum distinct motifs in each component/log-name Bonferroni family; larger valid families require sufficient resamples to resolve alpha/family-size.")
        log_params["resamples"] = ParameterSpec("resamples", "positive_integer", default=1999,
            has_default=True, description="Permutation resamples. Must satisfy 1/(resamples+1) <= alpha/maximum_patterns.")
        _register_op(registry, "paper.bank.log_pattern_contrast", "ScoredCandidates",
            (PortSpec("data", "PaperStarterInput"), PortSpec("candidates", "CandidateSet")),
            log_params, _bank_log_pattern_contrast,
            "Bank adaptation: row-local normalized token motifs scored by full-window block composition rates; case eligibility is explicit and runtime abstains if unsupported.")
    if generated_specs:
        from .m4_data import generated_sandbox_limits
        from .m4_generated import register_generated_specs
        normalized_generated = register_generated_specs(registry, list(generated_specs), audit_dir=audit_dir,
            sandbox_limits=generated_sandbox_limits(root),
            public_data_root=root / "data/prepared/bank/public")
        # Generated workers return a generic profile tag. Stamp the validated
        # registered identity at this host boundary before consensus sees it.
        for spec in normalized_generated:
            if spec.get("output_type") != "ScoredCandidates": continue
            registered = registry.require(spec["name"], spec["version"])
            original_implementation = registered.implementation
            if original_implementation is None: continue
            expected_worker_scorer = ("typed-operator-ir-v2" if
                spec.get("authoring_profile") == "typed-operator-ir-v2" else
                "typed-operator-code-v1" if spec.get("authoring_profile") == "typed-operator-code-v1"
                else "typed-operator-ir-v1")
            def named_scoring_implementation(inputs, params, context,
                    implementation=original_implementation, scorer_name=spec["name"],
                    expected_scorer=expected_worker_scorer,
                    authoring_profile=spec.get("authoring_profile")):
                result = implementation(inputs, params, context)
                if isinstance(result, ScoredCandidates):
                    if (authoring_profile == "typed-operator-code-v1" and
                            result.scorer != expected_scorer):
                        raise PaperStarterError("typed operator code returned an untrusted scorer identity")
                    if result.scorer == expected_scorer:
                        return replace(result, scorer=scorer_name)
                return result
            registry._operators[(registered.name, registered.version)] = replace(
                registered, implementation=named_scoring_implementation)
    return registry


def make_paper_starter_registry(*, bank_only: bool = False,
        input_kwargs: Mapping[str, Any] | None = None,
        include_conditional_dependencies: bool | None = None,
        starter_input: PaperStarterInput | None = None,
        generated_specs: Sequence[Mapping[str, Any]] = (),
        audit_dir: Path | None = None,
        active_operator_keys: set[str] | None = None,
        root: Path = ROOT) -> OperatorRegistry:
    """Create a separate opt-in registry; legacy Bank builders stay unchanged."""
    registry = OperatorRegistry()
    if bank_only:
        from .m4_data import make_m4_registry
        base = make_m4_registry(None, [], root=root)
        for signature in base.signatures():
            if signature["name"] == "bank.answers.combine":
                continue
            registry.register(base.require(signature["name"], signature["version"]))
        for name, estimator in base.estimators.items():
            registry.register_estimator(name, estimator)
    else:
        registry.register(OperatorSpec("case.telemetry", "1", (), "Telemetry",
            cacheable=False, external_input=True,
            description="Executor-injected immutable telemetry for the opt-in paper starter."))
    if include_conditional_dependencies is None:
        include_conditional_dependencies = True
    registry = register_paper_starter(registry, bank_only=bank_only,
        include_conditional_dependencies=include_conditional_dependencies,
        input_kwargs=input_kwargs, starter_input=starter_input,
        generated_specs=generated_specs, audit_dir=audit_dir, root=root)
    if active_operator_keys is None:
        return registry
    selected = OperatorRegistry()
    available = {f"{row['name']}@{row['version']}" for row in registry.signatures()}
    missing = set(active_operator_keys) - available
    if missing:
        raise PaperStarterError(f"snapshot operator identities are unavailable in paper registry: {sorted(missing)}")
    for signature in registry.signatures():
        key = f"{signature['name']}@{signature['version']}"
        if key in active_operator_keys:
            selected.register(registry.require(signature["name"], signature["version"]))
    for name, estimator in registry.estimators.items():
        selected.register_estimator(name, estimator)
    return selected


def _prepare_paper_input(telemetry: Telemetry, params: Mapping[str, Any], *,
        input_kwargs: Mapping[str, Any], starter_input: PaperStarterInput | None) -> PaperStarterInput:
    if starter_input is None:
        return make_paper_starter_input(telemetry,
            block_minutes=int(params["analysis_block_minutes"]), **dict(input_kwargs))
    if starter_input.telemetry.case_id != telemetry.case_id or starter_input.telemetry.data_sha256 != telemetry.data_sha256:
        raise PaperStarterError("prepared PaperStarterInput belongs to different telemetry")
    if starter_input.analysis_block_duration_ms != int(params["analysis_block_minutes"]) * 60_000:
        return make_paper_starter_input(telemetry, sessions=starter_input.sessions,
            urls=starter_input.urls, block_minutes=int(params["analysis_block_minutes"]),
            root=Path(input_kwargs.get("root", ROOT)))
    return starter_input


def bank_active_operator_names(*, has_trace_dependencies: bool) -> tuple[str, ...]:
    names = ["paper.backend.metric_conditions", "paper.backend.mean_welch",
        "paper.backend.location_permutation", "paper.backend.tail_permutation",
        "paper.answers.weighted_consensus"]
    if has_trace_dependencies: names.append("paper.scores.dependency_adjust")
    return tuple(names)


def active_bank_diagnostic_inventory(starter: PaperStarterInput, *, root: Path = ROOT) -> dict[str, Any]:
    """Report actual active diagnostic counts; helpers remain in a separate utility list."""
    canonical = list(bank_active_operator_names(has_trace_dependencies=True))
    adaptation_doc = json.loads((root / "config/paper_bank_adaptations.json").read_text(encoding="utf-8"))
    adaptations = [row["name"] for row in adaptation_doc["operators"]]
    case_adaptation_names = {name for book in available_bank_adaptation_books(starter, root=root)
        for name in book["operators"] if name.startswith("paper.bank.")}
    deployment = ["bank.candidates.generate", "bank.metrics.contrast",
                  "bank.traces.latency_contrast", "bank.logs.volume_contrast"]
    diagnostics = tuple(dict.fromkeys(canonical + adaptations + deployment))
    if len(diagnostics) > 19:
        raise PaperStarterError("active Bank diagnostic inventory exceeds the profile cap of 19")
    utilities = ("case.telemetry", "paper.input.prepare", "bank.metrics.aggregate",
        "bank.traces.aggregate", "bank.public.query", "bank.public.project")
    return {"profile": PROFILE, "diagnostic_operator_names": diagnostics,
        "diagnostic_operator_count": len(diagnostics), "diagnostic_cap": 19,
        "utility_operator_names": utilities, "utility_count_excluded": len(utilities),
        "canonical_origin_count": 19, "canonical_origin_active_count": len(canonical),
        "bank_adaptation_count": len(adaptations), "legacy_deployment_diagnostic_count": len(deployment),
        "case_available_adaptation_operator_names": tuple(name for name in adaptations if name in case_adaptation_names),
        "case_capabilities": dict(starter.capabilities)}


def make_paper_bank_registry(telemetry: Telemetry, *, root: Path = ROOT,
        sessions: Sequence[PaperSessionObservation] | None = None,
        urls: Sequence[PaperURLObservation] | None = None) -> tuple[OperatorRegistry, PaperStarterInput]:
    """Create the opt-in Bank registry and one fully prepared typed case input."""
    starter = make_paper_starter_input(telemetry, sessions=sessions, urls=urls, root=root)
    registry = make_paper_starter_registry(bank_only=True, starter_input=starter,
        input_kwargs={"root": root})
    return registry, starter


_OPERATOR_CAPABILITIES = {
    "paper.cohorts.attribute": ("sessions",),
    "paper.cohorts.conjunction": ("sessions",),
    "paper.cohorts.activity": ("sessions",),
    "paper.backend.metric_conditions": ("component_metrics",),
    "paper.events.subsequences": ("sessions",),
    "paper.urls.failure_conditions": ("urls",),
    "paper.urls.delay_conditions": ("urls",),
    "paper.cohorts.welch": ("sessions",),
    "paper.cohorts.permutation": ("sessions",),
    "paper.cohorts.tree_likelihood": ("sessions",),
    "paper.backend.mean_welch": ("component_metrics",),
    "paper.backend.location_permutation": ("component_metrics",),
    "paper.backend.tail_permutation": ("component_metrics",),
    "paper.urls.welch": ("urls",),
    "paper.urls.permutation": ("urls",),
    "paper.urls.flow_contribution": ("urls", "sessions"),
    "paper.events.outcome_support": ("sessions",),
    "paper.scores.dependency_adjust": ("trace_dependencies",),
    "paper.answers.weighted_consensus": (),
}
def available_paper_books(starter: PaperStarterInput, *,
                          catalog: Mapping[str, Any] | None = None) -> tuple[Mapping[str, Any], ...]:
    """Return only books whose declared typed capabilities are available."""
    source = catalog or paper_catalog()
    available = {name for name, status in starter.capabilities.items() if status == "available"}
    result = []
    for book in source["books"]:
        required = {capability for name in book["operators"]
                    for capability in _OPERATOR_CAPABILITIES.get(name, ())}
        if required <= available:
            result.append(book)
    return tuple(result)


def available_bank_adaptation_books(starter: PaperStarterInput, *,
        root: Path = ROOT) -> tuple[Mapping[str, Any], ...]:
    """Filter Bank deployment-specific books by exact case input capabilities."""
    source = json.loads((root / "config/paper_bank_adaptations.json").read_text(encoding="utf-8"))
    available = {name for name, status in starter.capabilities.items() if status == "available"}
    return tuple(book for book in source["books"] if set(book["requires"]) <= available)


def canonical_paper_playbooks(registry: OperatorRegistry) -> tuple[dict[str, Any], ...]:
    """Build all eleven canonical e4-dag/1 templates from the frozen inventory."""
    source = paper_catalog()
    version_by_name = {}
    output_by_name = {}
    inputs_by_name = {}
    for signature in registry.signatures():
        name = signature["name"]
        if name.startswith("paper."):
            version_by_name[name] = signature["version"]
            output_by_name[name] = signature["output_type"]
            inputs_by_name[name] = signature["inputs"]
    definitions = {row["name"]: row for row in source["inventory"]["candidate_generators"] +
                   source["inventory"]["scorers"] + source["inventory"]["summarizers"]}
    books = []
    for book in source["books"]:
        required_names = book["operators"]
        missing = set(required_names) - set(version_by_name)
        if missing:
            # The canonical artifact is the full catalog; do not silently write
            # a partial book from a capability-filtered deployment registry.
            continue
        nodes = [{"id": "telemetry", "operator": "case.telemetry", "version": "1",
            "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter_input", "operator": "paper.input.prepare",
             "version": version_by_name["paper.input.prepare"],
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}}]
        candidate_nodes: dict[str, str] = {}
        scorer_nodes: list[str] = []
        for index, name in enumerate(required_names):
            if name not in definitions: continue
            role = definitions[name]["id"]
            if role.startswith("C"):
                node_id = f"candidate_{index}"
                nodes.append({"id": node_id, "operator": name,
                    "version": version_by_name[name], "inputs": {"data": "starter_input"},
                    "params": {}, "bindings": {}})
                candidate_nodes[output_by_name[name]] = node_id
            elif role.startswith("S"):
                node_id = f"score_{index}"
                scorer_candidate_type = next(row["type"] for row in inputs_by_name[name]
                    if row["name"] == "candidates")
                candidate_output = scorer_candidate_type
                if candidate_output not in candidate_nodes:
                    raise PaperStarterError(f"canonical book {book['id']} lacks {candidate_output} producer")
                candidate_ref = candidate_nodes[candidate_output]
                nodes.append({"id": node_id, "operator": name,
                    "version": version_by_name[name], "inputs": {
                        "data": "starter_input", "candidates": candidate_ref},
                    "params": {}, "bindings": {}})
                scorer_nodes.append(node_id)
            elif role == "U1":
                adjusted_nodes = []
                for branch_index, prior in enumerate(scorer_nodes):
                    node_id = f"adjust_{index}_{branch_index}"
                    nodes.append({"id": node_id, "operator": name,
                        "version": version_by_name[name], "inputs": {
                            "data": "starter_input", "scores": prior},
                        "params": {}, "bindings": {}})
                    adjusted_nodes.append(node_id)
                scorer_nodes = adjusted_nodes
        if not scorer_nodes:
            continue
        nodes.append({"id": "answer", "operator": "paper.answers.weighted_consensus",
            "version": version_by_name["paper.answers.weighted_consensus"],
            "inputs": {"scores": scorer_nodes}, "params": {}, "bindings": {}})
        books.append({"name": book["name"], "document": {"schema_version": "e4-dag/1",
            "nodes": nodes, "return": {"node": "answer"}},
            "provenance": {"source": PROFILE, "reconstruction_id": book["id"]}})
    if len(books) != 11:
        raise PaperStarterError(f"full registry produced {len(books)} of 11 canonical books")
    return tuple(books)


def canonical_paper_library_source(registry: OperatorRegistry) -> dict[str, Any]:
    """Return exactly the 19 reconstructed diagnostic signatures and 11 books."""
    source = paper_catalog()
    names = {row["name"] for group in source["inventory"].values() for row in group
             if isinstance(group, list) and isinstance(row, Mapping) and "name" in row}
    signatures = [row for row in registry.signatures() if row["name"] in names]
    signatures.sort(key=lambda row: row["name"])
    books = list(canonical_paper_playbooks(registry))
    if len(signatures) != 19 or len(books) != 11:
        raise PaperStarterError("canonical starter source must contain exactly 19 diagnostics and 11 DAGs")
    from .dag import verify_dag
    for book in books:
        verify_dag(book["document"], registry, runtime=None, max_nodes=40, max_depth=12)
    return {"profile": PROFILE, "builtin_specs": signatures,
        "playbooks": books, "operator_count": len(signatures), "playbook_count": len(books),
        "inventory_sha256": hashlib.sha256(CATALOG_PATH.read_bytes()).hexdigest(),
        "runtime_policy_sha256": hashlib.sha256((ROOT / "config/paper_starter_runtime.json").read_bytes()).hexdigest()}


def bank_profile_playbooks(registry: OperatorRegistry, starter: PaperStarterInput | None = None) -> tuple[dict[str, Any], ...]:
    """Build the case-independent Bank profile books for its frozen active registry."""
    from .m4_library import playbook_graph_hash
    signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
    active_keys = {f"{name}@{version}" for name, version in signatures}
    full_registry = make_paper_starter_registry()
    canonical = [book for book in canonical_paper_playbooks(full_registry)
                 if _playbook_refs(book["document"]) <= active_keys]
    books = list(canonical)
    adaptation_source = json.loads((ROOT / "config/paper_bank_adaptations.json").read_text(encoding="utf-8"))
    for book in adaptation_source["books"]:
        names = book["operators"]
        if any(not any(name == op for op, _version in signatures) for name in names):
            continue
        nodes = [{"id": "telemetry", "operator": "case.telemetry", "version": "1",
            "inputs": {}, "params": {}, "bindings": {}},
            {"id": "starter_input", "operator": "paper.input.prepare",
             "version": signatures[("paper.input.prepare", "1")]["version"],
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "candidates", "operator": "bank.candidates.generate",
             "version": signatures[("bank.candidates.generate", "1")]["version"],
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}},
            {"id": "scores", "operator": names[1],
             "version": next(version for op, version in signatures if op == names[1]),
             "inputs": {"data": "starter_input", "candidates": "candidates"},
             "params": {}, "bindings": {}},
            {"id": "answer", "operator": "paper.answers.weighted_consensus",
             "version": "1", "inputs": {"scores": ["scores"]}, "params": {}, "bindings": {}}]
        doc = {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "answer"}}
        books.append({"name": book["name"], "document": doc,
            "provenance": {"source": PROFILE, "reconstruction_id": book["id"],
                           "kind": "bank_deployment_adaptation"}})

    legacy_definitions = [
        ("bank_native_metric_contrast", ("bank.metrics.contrast",)),
        ("bank_per_trace_native_duration", ("bank.traces.latency_contrast",)),
        ("bank_per_minute_log_volume", ("bank.logs.volume_contrast",)),
        ("bank_combined_shared_candidates", ("bank.metrics.contrast", "bank.traces.latency_contrast",
                                               "bank.logs.volume_contrast")),
    ]
    for name, scorers in legacy_definitions:
        if any(not any(op == scorer for op, _version in signatures) for scorer in scorers):
            continue
        nodes = [{"id": "telemetry", "operator": "case.telemetry", "version": "1",
            "inputs": {}, "params": {}, "bindings": {}},
            {"id": "candidate", "operator": "bank.candidates.generate",
             "version": signatures[("bank.candidates.generate", "1")]["version"],
             "inputs": {"data": "telemetry"}, "params": {}, "bindings": {}}]
        metric = {"candidates": "candidate"}
        if any(scorer in {"bank.metrics.contrast", "bank.logs.volume_contrast"} for scorer in scorers):
            for window in ("reference", "alert"):
                nodes.append({"id": f"metric_{window}", "operator": "bank.metrics.aggregate",
                    "version": next(v for op, v in signatures if op == "bank.metrics.aggregate"),
                    "inputs": {"data": "telemetry"}, "params": {"window": window}, "bindings": {}})
            metric.update({"reference": "metric_reference", "alert": "metric_alert"})
        score_nodes = []
        for index, scorer in enumerate(scorers):
            node_id = f"score_{index}"
            if scorer == "bank.metrics.contrast":
                input_map = metric
            elif scorer == "bank.traces.latency_contrast":
                for window in ("reference", "alert"):
                    trace_id = f"trace_{window}"
                    if not any(node["id"] == trace_id for node in nodes):
                        nodes.append({"id": trace_id, "operator": "bank.traces.aggregate",
                            "version": next(v for op, v in signatures if op == "bank.traces.aggregate"),
                            "inputs": {"data": "telemetry"}, "params": {"window": window}, "bindings": {}})
                input_map = {"reference": "trace_reference", "alert": "trace_alert", "candidates": "candidate"}
            else:
                input_map = metric
            nodes.append({"id": node_id, "operator": scorer,
                "version": next(v for op, v in signatures if op == scorer),
                "inputs": input_map, "params": {}, "bindings": {}})
            score_nodes.append(node_id)
        nodes.append({"id": "answer", "operator": "paper.answers.weighted_consensus",
            "version": "1", "inputs": {"scores": score_nodes}, "params": {}, "bindings": {}})
        doc = {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "answer"}}
        books.append({"name": name, "document": doc,
            "provenance": {"source": PROFILE, "kind": "existing_bank_deployment_diagnostic"}})
    seen = set()
    from .dag import verify_dag
    for book in books:
        digest = playbook_graph_hash(book["document"])
        if digest in seen:
            raise PaperStarterError(f"Bank profile books contain duplicate DAG identity: {book['name']}")
        seen.add(digest)
        verify_dag(book["document"], registry, runtime=None, max_nodes=40, max_depth=12)
    return tuple(books)


def available_bank_profile_playbooks(books: Sequence[Mapping[str, Any]],
        starter: PaperStarterInput) -> tuple[Mapping[str, Any], ...]:
    """Filter a frozen case-independent inventory by this case's capabilities."""
    available = {name for name, status in starter.capabilities.items() if status == "available"}
    selected = []
    adaptation_doc = json.loads((ROOT / "config/paper_bank_adaptations.json").read_text(encoding="utf-8"))
    adaptation_requirements = {row["name"]: set(row["capabilities"])
        for row in adaptation_doc["operators"]}
    for book in books:
        name = str(book.get("name", ""))
        if name in {"bank_component_metric_to_app_mrt"}:
            needed = {"component_metrics", "app_mrt"}
        elif name == "bank_trace_root_duration_flow":
            needed = {"trace_root_duration"}
        elif name == "bank_log_template_composition":
            needed = {"native_log_patterns"}
        elif name in {"per_trace_native_duration", "bank_per_trace_native_duration"}:
            needed = {"trace_dependencies"}
        elif name in {"per_minute_log_volume", "bank_per_minute_log_volume"}:
            needed = {"native_logs"}
        elif name == "bank_combined_shared_candidates":
            needed = {"component_metrics", "trace_dependencies", "native_logs"}
        elif name in {"backend_mean_shift", "backend_location_shift", "backend_tail_degradation",
                      "bank_native_metric_contrast", "bank_combined_shared_candidates"}:
            needed = {"component_metrics"}
        elif name == "dependency_adjusted_backend_consensus":
            needed = {"component_metrics", "trace_dependencies"}
        else:
            needed = set()
        if needed <= available:
            selected.append(book)
    return tuple(selected)


def _playbook_refs(document: Mapping[str, Any]) -> set[str]:
    return {f"{node['operator']}@{node['version']}" for node in document.get("nodes", [])
            if isinstance(node, Mapping) and isinstance(node.get("operator"), str)
            and isinstance(node.get("version"), str)}


def paper_catalog() -> dict[str, Any]:
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


__all__ = ["CATALOG_PATH", "PROFILE", "PaperStarterError", "available_bank_adaptation_books",
    "active_bank_diagnostic_inventory", "available_paper_books", "bank_active_operator_names",
    "canonical_paper_library_source", "canonical_paper_playbooks", "make_paper_bank_registry",
    "available_bank_profile_playbooks", "bank_profile_playbooks", "make_paper_starter_input",
    "make_paper_starter_registry", "PAPER_CONSENSUS_GROUPS", "PAPER_CONSENSUS_WEIGHT_ALIASES",
    "consensus_weight_catalog", "normalize_consensus_weights",
    "paper_catalog", "register_paper_starter"]
